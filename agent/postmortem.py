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
import time
import uuid

from qdrant_client.http.models import PointStruct
from sqlalchemy import create_engine, text

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


def _build_postmortem_text(postmortem: dict) -> str:
    return (
        f"Incident {postmortem['incident_id']}\n"
        f"DAG: {postmortem['dag_id']}  Task: {postmortem['task_id']}  "
        f"Run: {postmortem['run_id']}\n"
        f"Failure type: {postmortem['error_type']}\n"
        f"Risk level: {postmortem['risk_level']}\n"
        f"Approval status: {postmortem['approval_status']}\n\n"
        f"Root cause:\n{postmortem['root_cause']}\n\n"
        f"Fix applied:\n{postmortem['resolution']}"
    )


def _insert_incident_report(
    postmortem: dict, duration_seconds, postmortem_text: str
) -> None:
    with _engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.incident_reports
                (incident_id, dag_id, task_id, failure_type, root_cause,
                 fix_applied, fix_status, duration_seconds, postmortem_text)
            VALUES (:incident_id, :dag_id, :task_id, :failure_type,
                    :root_cause, :fix_applied, :fix_status,
                    :duration_seconds, :postmortem_text)
        """), {
            "incident_id": postmortem["incident_id"],
            "dag_id": postmortem["dag_id"],
            "task_id": postmortem["task_id"],
            "failure_type": postmortem["error_type"],
            "root_cause": postmortem["root_cause"],
            "fix_applied": postmortem["resolution"],
            "fix_status": postmortem["approval_status"],
            "duration_seconds": duration_seconds,
            "postmortem_text": postmortem_text,
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
    }

    ensure_collection()
    embedding_text = f"{summary}\n{root_cause}\n{resolution}"
    vector = embed_text(embedding_text)

    get_qdrant_client().upsert(
        collection_name=COLLECTION_NAME,
        points=[PointStruct(id=incident_id, vector=vector, payload=postmortem)],
    )

    started_at = state.get("started_at")
    duration_seconds = int(round(time.time() - started_at)) if started_at else None

    try:
        _insert_incident_report(
            postmortem, duration_seconds, _build_postmortem_text(postmortem)
        )
    except Exception as exc:
        logger.warning(
            "Failed to write monitoring.incident_reports row for incident %s "
            "(Qdrant write already succeeded, continuing): %s", incident_id, exc
        )

    return postmortem
