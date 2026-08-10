"""
Background job enforcing the time-based escalation ladder for pending
Slack approvals:

  P1: unanswered for SLACK_TIMEOUT_MINUTES -> auto-apply (retry the
      task via Airflow) even without a human click.
  P0: unanswered for SLACK_TIMEOUT_MINUTES -> urgent email to
      ESCALATION_EMAIL. Still unanswered at 2x SLACK_TIMEOUT_MINUTES ->
      confirm the DAG is paused (it should already be paused immediately
      on P0 detection in graph.py — this is a safety-net re-check, not
      the primary pause trigger).

Runs inside the same FastAPI process via APScheduler rather than a
separate worker/queue — this is a single small agent instance, and a
whole task-queue system would be a lot of infrastructure for "poll a
table every few minutes."
"""

import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, text

from mcp_servers.slack_server import post_notification, resolve_approved_fix
from tools import airflow_client, email_notifier

logger = logging.getLogger("agent.escalation_scheduler")

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)
SLACK_TIMEOUT_MINUTES = int(os.environ.get("SLACK_TIMEOUT_MINUTES", "30"))

# How often to poll — a fraction of the smallest configured timeout so
# the worst-case lateness on a deadline stays small without polling so
# often it's wasted work on a small instance.
_CHECK_INTERVAL_MINUTES = 5


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _handle_p1_timeout(conn, row) -> None:
    logger.info(
        "Auto-applying P1 fix for incident %s after %.0f min with no response",
        row.incident_id, row.age_minutes,
    )
    # Same execute-then-retry path a human triggers by clicking Approve
    # in Slack — see resolve_approved_fix()'s docstring. It re-validates
    # row.proposed_sql via sql_validator itself rather than trusting
    # whatever tier it was classified as when the approval was created.
    detail = resolve_approved_fix(row.proposed_sql, row.dag_id, row.run_id, row.task_id)

    conn.execute(text("""
        UPDATE monitoring.agent_approvals
        SET status = 'auto_approved', resolved_at = NOW(), resolved_by = 'escalation_timeout'
        WHERE incident_id = :id
    """), {"id": row.incident_id})

    try:
        post_notification(
            f"⏱️ No response in {SLACK_TIMEOUT_MINUTES} min — auto-applied P1 fix: "
            f"{row.summary}\n{detail}"
        )
    except Exception as exc:
        logger.warning("Slack notification failed for incident %s: %s", row.incident_id, exc)


def _handle_p0_timeout(conn, row) -> None:
    if row.escalation_email_sent_at is None and row.age_minutes >= SLACK_TIMEOUT_MINUTES:
        try:
            email_notifier.send_escalation_email(
                subject=f"URGENT: P0 incident unacknowledged — {row.dag_id}.{row.task_id}",
                body=(
                    f"{row.summary}\n\n"
                    f"No response in Slack after {SLACK_TIMEOUT_MINUTES} minutes.\n"
                    f"Incident: {row.incident_id}\n"
                    f"DAG: {row.dag_id}  Run: {row.run_id}  Task: {row.task_id}"
                ),
            )
            conn.execute(text("""
                UPDATE monitoring.agent_approvals
                SET escalation_email_sent_at = NOW()
                WHERE incident_id = :id
            """), {"id": row.incident_id})
            logger.info("Sent P0 escalation email for incident %s", row.incident_id)
        except Exception as exc:
            logger.error("Failed to send P0 escalation email for %s: %s", row.incident_id, exc)

    if row.dag_paused_at is None and row.age_minutes >= SLACK_TIMEOUT_MINUTES * 2:
        try:
            airflow_client.pause_dag(row.dag_id)
            conn.execute(text("""
                UPDATE monitoring.agent_approvals
                SET dag_paused_at = NOW()
                WHERE incident_id = :id
            """), {"id": row.incident_id})
            logger.info("Confirmed DAG %s paused for incident %s", row.dag_id, row.incident_id)
        except Exception as exc:
            logger.error("Failed to pause DAG for P0 incident %s: %s", row.incident_id, exc)


def check_escalations() -> None:
    try:
        with _engine().begin() as conn:
            pending = conn.execute(text("""
                SELECT incident_id, dag_id, run_id, task_id, summary, severity,
                       proposed_sql, escalation_email_sent_at, dag_paused_at,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) / 60 AS age_minutes
                FROM monitoring.agent_approvals
                WHERE status = 'pending'
            """)).fetchall()

            for row in pending:
                if row.severity == "P1" and row.age_minutes >= SLACK_TIMEOUT_MINUTES:
                    _handle_p1_timeout(conn, row)
                elif row.severity == "P0":
                    _handle_p0_timeout(conn, row)
    except Exception:
        logger.exception("escalation check failed")


def start() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_escalations, "interval", minutes=_CHECK_INTERVAL_MINUTES, id="escalation_check")
    scheduler.start()
    logger.info(
        "Escalation scheduler started: checking every %d min, timeout=%d min",
        _CHECK_INTERVAL_MINUTES, SLACK_TIMEOUT_MINUTES,
    )
    return scheduler
