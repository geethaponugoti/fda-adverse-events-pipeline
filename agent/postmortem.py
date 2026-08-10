"""
Writes a structured postmortem for a resolved (or escalated) incident.
Dual-writes it to two places:
  - Qdrant, embedded, so tools/memory_lookup.py can surface it as prior
    art for future similar failures (semantic search).
  - monitoring.incident_reports in Postgres, as a plain relational audit
    trail (exact-match queries, joins with monitoring.pipeline_runs,
    dashboards) — the two stores serve different query patterns, neither
    replaces the other.

The Postgres insert is best-effort: a failure there is logged and
swallowed rather than raised, so a DB hiccup can't turn a successfully
diagnosed incident into a 500 back to the /alert caller.
"""

import datetime as dt
import logging
import os
import uuid

from qdrant_client.http.models import PointStruct
from sqlalchemy import create_engine, text

from tools import sql_validator
from tools.memory_lookup import (
    COLLECTION_NAME,
    embed_text,
    ensure_collection,
    get_qdrant_client,
)

logger = logging.getLogger("agent.postmortem")

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _build_summary(state: dict) -> str:
    alert = state.get("alert", {})
    error_type = state.get("error_type", "unknown_error")
    return (
        f"{error_type} on {alert.get('dag_id')}.{alert.get('task_id')} "
        f"(run {alert.get('run_id')})"
    )


def _insert_incident_report(state: dict, postmortem: dict) -> None:
    """monitoring.incident_reports' actual live schema is
    (dag_id, run_id, task_id, error_type, error_message, fix_attempted,
    fix_result, approved, created_at, fix_sql) — no incident_id/
    failure_type/root_cause/fix_applied/fix_status/duration_seconds/
    postmortem_text. Column names here must match that, not the
    postmortem dict's own (differently-named) keys.

    fix_sql is nullable and only added via ALTER TABLE IF NOT EXISTS in
    scripts/init_db.sql / applied directly to the live DB — this insert
    doesn't try to create it itself, unlike agent_approvals' pattern
    elsewhere, since this table's column additions have historically
    been done as a one-time migration rather than an ensure-on-every-call."""
    approval_status = postmortem["approval_status"]
    approved = (
        True if approval_status in ("auto_approved", "approved")
        else False if approval_status == "rejected"
        else None  # pending / escalated: not yet decided
    )
    parsed_log = state.get("parsed_log") or {}
    proposed_fix = state.get("proposed_fix") or {}

    with _engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.incident_reports
                (dag_id, run_id, task_id, error_type, error_message,
                 fix_attempted, fix_result, approved, fix_sql, created_at)
            VALUES (:dag_id, :run_id, :task_id, :error_type, :error_message,
                    :fix_attempted, :fix_result, :approved, :fix_sql, NOW())
        """), {
            "dag_id": postmortem["dag_id"],
            "run_id": postmortem["run_id"],
            "task_id": postmortem["task_id"],
            "error_type": postmortem["error_type"],
            "error_message": parsed_log.get("error_message") or postmortem["root_cause"],
            "fix_attempted": postmortem["resolution"],
            "fix_result": approval_status,
            "approved": approved,
            "fix_sql": proposed_fix.get("sql"),
        })


def write_postmortem(state: dict) -> dict:
    """Builds the postmortem record from the final agent state, stores
    its embedding + payload in Qdrant, and returns the record so it can
    also be returned to the caller of /alert."""
    alert = state.get("alert", {})
    investigation = state.get("investigation", {}) or {}
    proposed_fix = state.get("proposed_fix", {}) or {}

    incident_id = str(uuid.uuid4())
    created_at = dt.datetime.utcnow().isoformat() + "Z"
    summary = _build_summary(state)
    root_cause = investigation.get("summary") or "No investigation summary available."
    resolution = proposed_fix.get("description") or "No fix proposed."

    # has_sql_fix gates Qdrant reuse (see memory_lookup.find_reusable_fix)
    # — deliberately keyed on fix_executed, not approval_status. A P1
    # auto-fix that fell back to a plain retry still resolves as
    # "auto_approved" but never actually ran any SQL; caching that as
    # a reusable fix would make future incidents worse, not better.
    fix_executed = bool(state.get("fix_executed"))
    sql = proposed_fix.get("sql")
    has_sql_fix = fix_executed and bool(sql)
    # Re-classified fresh rather than trusting whatever tier applied at
    # execution time — matches memory_lookup.find_reusable_fix's payload
    # contract (it reads "sql_tier" back out for a reused fix's metadata).
    sql_tier = sql_validator.classify_sql(sql).tier.value if sql else None

    postmortem = {
        "incident_id": incident_id,
        "created_at": created_at,
        "dag_id": alert.get("dag_id"),
        "run_id": alert.get("run_id"),
        "task_id": alert.get("task_id"),
        "error_type": state.get("error_type"),
        "summary": summary,
        "root_cause": root_cause,
        "resolution": resolution,
        "risk_level": proposed_fix.get("risk_level"),
        "approval_status": state.get("approval_status", "pending"),
        "sql": sql,
        "sql_tier": sql_tier,
        "has_sql_fix": has_sql_fix,
    }

    ensure_collection()
    embedding_text = f"{summary}\n{root_cause}\n{resolution}"
    vector = embed_text(embedding_text)

    get_qdrant_client().upsert(
        collection_name=COLLECTION_NAME,
        points=[PointStruct(id=incident_id, vector=vector, payload=postmortem)],
    )

    try:
        _insert_incident_report(state, postmortem)
    except Exception as exc:
        logger.warning(
            "Failed to write monitoring.incident_reports row for incident %s "
            "(Qdrant write already succeeded, continuing): %s", incident_id, exc
        )

    return postmortem
