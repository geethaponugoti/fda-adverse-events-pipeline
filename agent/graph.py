"""
LangGraph graph definition for the FDA pipeline self-healing agent.

Six nodes, run in a fixed sequence (no branching in the graph itself —
the branching happens inside approve_or_escalate, based on severity,
P0-P3, computed in classify_node from error_type via SEVERITY_MAP):

    ingest -> classify -> investigate -> fix -> approve_or_escalate -> postmortem

Safety note: the `fix` node asks the LLM to propose SQL, but that SQL
is advisory only — it's surfaced to a human in Slack / the postmortem,
never executed, regardless of severity. The only DB write
approve_or_escalate ever makes automatically is a single,
narrowly-scoped case: renaming a column back to its pre-drift name,
and only when schema_inspector found exactly one column added and
exactly one removed (an unambiguous rename), with the identifiers
coming from information_schema / monitoring.schema_snapshots — not
from LLM output. Every other "auto-fix" action is a retry of the
Airflow task via airflow_client, which is safe regardless of what
caused the original failure.
"""

import json
import logging
import os
import uuid
from typing import Literal, Optional

from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from mcp_servers.slack_server import post_notification, request_approval
from postmortem import write_postmortem
from state import AgentState
from tools import airflow_client, lineage_tracer, log_parser, memory_lookup, schema_inspector

logger = logging.getLogger("agent.graph")

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
SLACK_TIMEOUT_MINUTES = int(os.environ.get("SLACK_TIMEOUT_MINUTES", "30"))

# error_type -> severity. Drives approve_or_escalate_node's response
# strategy (see its docstring) — a separate dimension from risk_level,
# which only judges whether a specific proposed fix is safe to apply.
SEVERITY_MAP = {
    "data_corruption": "P0",
    "schema_drift": "P1",
    "volume_anomaly": "P1",
    "data_quality": "P2",
    "freshness": "P2",
    "upstream_failure": "P3",
}


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _llm() -> ChatOpenAI:
    return ChatOpenAI(model=OPENAI_MODEL, temperature=0)


# --------------------------------------------------------------------------
# ingest
# --------------------------------------------------------------------------

def ingest_node(state: AgentState) -> dict:
    alert = state["alert"]
    parsed_log = log_parser.parse_alert(alert)
    logger.info("ingest: task_id=%s error_message=%s error_type_hint=%s",
                parsed_log["task_id"], parsed_log["error_message"],
                parsed_log["error_type_hint"])
    return {"parsed_log": parsed_log}


# --------------------------------------------------------------------------
# classify
# --------------------------------------------------------------------------

class _ErrorClassification(BaseModel):
    error_type: Literal[
        "data_corruption", "schema_drift", "volume_anomaly",
        "data_quality", "freshness", "upstream_failure",
    ] = Field(description="Best-fit category for this pipeline failure.")
    confidence: float = Field(description="0.0-1.0 confidence in this classification.")
    reasoning: str = Field(description="One or two sentences explaining why.")


def classify_node(state: AgentState) -> dict:
    parsed_log = state["parsed_log"]

    prompt = (
        "You are triaging a failure in an Airflow data pipeline that extracts "
        "FDA adverse event reports from the openFDA API, stages them in S3, "
        "loads them into PostgreSQL, and transforms them with dbt.\n\n"
        f"Failed task: {parsed_log['task_id']}\n"
        f"Error message: {parsed_log['error_message']}\n"
        f"Heuristic keyword-match hint (not authoritative, may be wrong): "
        f"{parsed_log.get('error_type_hint')}\n\n"
        "Classify this into exactly one of:\n"
        "- data_corruption: existing rows are structurally broken — encoding "
        "corruption, referential/integrity violations, widespread garbled "
        "values — as opposed to merely missing/out-of-range ones. This is the "
        "most severe category: it pauses the pipeline, so only use it when "
        "the data itself looks damaged, not just imperfect.\n"
        "- schema_drift: a column was renamed, added, or removed\n"
        "- volume_anomaly: row count is unexpectedly high or low\n"
        "- data_quality: existing rows have bad or missing values (moderate "
        "null rates, out-of-range values) short of outright corruption\n"
        "- freshness: data is stale — the pipeline ran but the underlying "
        "source data didn't actually update\n"
        "- upstream_failure: infra/network/API/storage issue unrelated to "
        "the shape or content of the data (timeouts, missing files, OOM, "
        "connection errors)"
    )

    result = _llm().with_structured_output(_ErrorClassification).invoke(prompt)
    severity = SEVERITY_MAP.get(result.error_type, "P3")
    logger.info("classify: error_type=%s severity=%s confidence=%.2f reasoning=%s",
                result.error_type, severity, result.confidence, result.reasoning)

    return {"error_type": result.error_type, "severity": severity}


# --------------------------------------------------------------------------
# investigate
# --------------------------------------------------------------------------

def _check_row_count() -> dict:
    with _engine().connect() as conn:
        current_count = conn.execute(text(
            "SELECT COUNT(*) FROM raw.fda_adverse_events"
        )).scalar_one()
        baseline_row = conn.execute(text("""
            SELECT baseline_count FROM monitoring.row_count_baselines
            WHERE table_name = 'fda_adverse_events' AND schema_name = 'raw'
            ORDER BY recorded_at DESC LIMIT 1
        """)).fetchone()

    baseline_count = baseline_row[0] if baseline_row else None
    deviation_pct = None
    if baseline_count:
        deviation_pct = round(abs(current_count - baseline_count) / baseline_count * 100, 2)

    return {
        "current_count": current_count,
        "baseline_count": baseline_count,
        "deviation_pct": deviation_pct,
    }


def _check_null_rate(column: str = "drug_name") -> dict:
    with _engine().connect() as conn:
        total = conn.execute(text(
            "SELECT COUNT(*) FROM raw.fda_adverse_events"
        )).scalar_one()
        nulls = conn.execute(text(
            f"SELECT COUNT(*) FROM raw.fda_adverse_events WHERE {column} IS NULL"
        )).scalar_one()

    null_pct = round(nulls / total * 100, 2) if total else 0.0
    return {"column": column, "total_rows": total, "null_rows": nulls, "null_pct": null_pct}


def investigate_node(state: AgentState) -> dict:
    parsed_log = state["parsed_log"]
    error_type = state.get("error_type")
    error_message = parsed_log["error_message"]

    try:
        similar_incidents = memory_lookup.search_similar_incidents(error_message, top_k=3)
    except Exception as exc:
        logger.warning("memory_lookup failed, continuing without prior incidents: %s", exc)
        similar_incidents = []

    details = {"similar_incidents": similar_incidents}
    tool_used = "memory_lookup"
    summary_parts = []

    if similar_incidents:
        top = similar_incidents[0]
        summary_parts.append(
            f"Found {len(similar_incidents)} similar past incident(s); most "
            f"similar (score {top['score']:.2f}): {top['summary']}."
        )
    else:
        summary_parts.append("No similar past incidents found in memory.")

    if error_type == "schema_drift":
        tool_used = "schema_inspector+lineage_tracer"
        diff = schema_inspector.get_schema_diff()
        details["schema_diff"] = diff

        changed_column = None
        if len(diff.get("added", [])) == 1 and len(diff.get("removed", [])) == 1:
            changed_column = diff["removed"][0]

        try:
            lineage = lineage_tracer.get_impacted_models(column_name=changed_column)
        except FileNotFoundError as exc:
            logger.warning("lineage_tracer unavailable, continuing without it: %s", exc)
            lineage = {"source_found": False, "impacted_models": [], "models_referencing_column": []}
        details["lineage"] = lineage

        summary_parts.append(
            f"Schema diff vs. last snapshot: added={diff.get('added')} "
            f"removed={diff.get('removed')} changed_types={diff.get('changed_types')}. "
            f"dbt models impacted: {lineage.get('impacted_models')}"
            + (f"; models referencing the changed column: {lineage.get('models_referencing_column')}."
               if changed_column else ".")
        )

    elif error_type == "volume_anomaly":
        tool_used = "row_count_check"
        row_check = _check_row_count()
        details["row_count_check"] = row_check
        summary_parts.append(
            f"Row count check: current={row_check['current_count']} "
            f"baseline={row_check['baseline_count']} "
            f"deviation={row_check['deviation_pct']}%."
        )

    elif error_type == "data_quality":
        tool_used = "null_rate_check"
        null_check = _check_null_rate()
        details["null_check"] = null_check
        summary_parts.append(
            f"Null-rate check on {null_check['column']}: "
            f"{null_check['null_rows']}/{null_check['total_rows']} rows "
            f"({null_check['null_pct']}%) are NULL."
        )

    elif error_type == "data_corruption":
        # Most severe category — run both checks available rather than
        # picking one, since corruption can show up as either pattern
        # (or neither, in which case a human needs to look regardless).
        tool_used = "null_rate_check+row_count_check"
        null_check = _check_null_rate()
        row_check = _check_row_count()
        details["null_check"] = null_check
        details["row_count_check"] = row_check
        summary_parts.append(
            f"Null-rate check on {null_check['column']}: "
            f"{null_check['null_rows']}/{null_check['total_rows']} rows "
            f"({null_check['null_pct']}%) are NULL. Row count check: "
            f"current={row_check['current_count']} baseline={row_check['baseline_count']} "
            f"deviation={row_check['deviation_pct']}%."
        )

    elif error_type == "freshness":
        tool_used = "freshness_check"
        with _engine().connect() as conn:
            freshness_row = conn.execute(text("""
                SELECT table_name, last_loaded, is_stale
                FROM monitoring.freshness_checks
                ORDER BY checked_at DESC LIMIT 1
            """)).fetchone()
        freshness = (
            {"table_name": freshness_row[0], "last_loaded": str(freshness_row[1]),
             "is_stale": freshness_row[2]}
            if freshness_row else None
        )
        details["freshness"] = freshness
        summary_parts.append(
            f"Latest freshness check: {freshness}." if freshness
            else "No freshness check recorded yet."
        )

    else:
        summary_parts.append(
            "No specialized investigation tool for upstream_failure beyond "
            "prior-incident lookup — likely an infra issue outside the data itself."
        )

    return {
        "investigation": {
            "tool_used": tool_used,
            "summary": " ".join(summary_parts),
            "details": details,
        }
    }


# --------------------------------------------------------------------------
# fix
# --------------------------------------------------------------------------

class _ProposedFixLLM(BaseModel):
    description: str = Field(description="Human-readable description of the recommended fix.")
    risk_level: Literal["low", "medium", "high"] = Field(
        description=(
            "low = purely mechanical and reversible (e.g. renaming a column "
            "back to its known-correct name); medium = involves a judgment "
            "call a human should quickly confirm; high = could affect data "
            "integrity or needs domain knowledge you don't have."
        )
    )
    sql: Optional[str] = Field(
        default=None,
        description=(
            "Suggested corrective SQL, for a human to review only. This is "
            "never executed automatically, regardless of risk_level."
        ),
    )
    auto_applicable: bool = Field(
        description="True only if you are confident this is safe to apply without human review."
    )


def fix_node(state: AgentState) -> dict:
    error_type = state.get("error_type")
    investigation = state.get("investigation", {})

    prompt = (
        f"A '{error_type}' failure was investigated in the FDA adverse events "
        f"pipeline.\n\nInvestigation summary: {investigation.get('summary')}\n"
        f"Investigation details: {json.dumps(investigation.get('details', {}), default=str)[:4000]}\n\n"
        "Propose a fix for a human on-call engineer to review. Be honest about "
        "risk_level — most fixes should be medium or high; only mark low if "
        "this is a purely mechanical, reversible correction."
    )

    result = _llm().with_structured_output(_ProposedFixLLM).invoke(prompt)
    logger.info("fix: risk_level=%s auto_applicable=%s description=%s",
                result.risk_level, result.auto_applicable, result.description)

    return {
        "proposed_fix": {
            "description": result.description,
            "risk_level": result.risk_level,
            "sql": result.sql,
            "auto_applicable": result.auto_applicable,
        }
    }


# --------------------------------------------------------------------------
# approve_or_escalate
# --------------------------------------------------------------------------

def _retry_airflow_task(alert: dict) -> Optional[str]:
    """Best-effort: never lets an Airflow API failure break the graph."""
    try:
        airflow_client.retry_task(alert["dag_id"], alert["run_id"], alert["task_id"])
        return f"Retried task '{alert['task_id']}' via the Airflow API."
    except Exception as exc:
        logger.warning("Airflow retry_task failed for %s.%s: %s",
                       alert["dag_id"], alert["task_id"], exc)
        return None


def approve_or_escalate_node(state: AgentState) -> dict:
    """Severity (P0-P3, from classify_node) drives the response strategy;
    risk_level (from fix_node) only ever affects message framing here —
    it no longer gates whether an action is taken, since none of the
    actions this node takes are LLM-proposed SQL (see module docstring).

      P0 (data corruption): pause the DAG immediately, escalate via
      Slack now, then via email/re-confirmed-pause on the timeout
      ladder enforced by escalation_scheduler.py if unacknowledged.

      P1 (schema drift / volume anomaly): apply the narrow safe schema
      fix immediately if it applies; otherwise send a Slack approval
      request that auto-applies (retries the task) after
      SLACK_TIMEOUT_MINUTES if nobody responds.

      P2 (data quality / freshness): retry the task silently, log only
      — no Slack message at all.

      P3 (upstream/infra): log only, no fix attempted, matching the
      existing behavior for this category.
    """
    alert = state["alert"]
    error_type = state.get("error_type", "upstream_failure")
    severity = state.get("severity") or SEVERITY_MAP.get(error_type, "P3")
    investigation = state.get("investigation", {})
    proposed_fix = state.get("proposed_fix", {})
    risk_level = proposed_fix.get("risk_level", "high")

    incident_id = str(uuid.uuid4())
    summary = (
        f"[{severity}/{error_type}] {alert['dag_id']}.{alert['task_id']} "
        f"(run {alert['run_id']}): {proposed_fix.get('description', '')}"
    )

    # ---------------- P0: pause now, escalate now, ladder for the rest ----
    if severity == "P0":
        try:
            airflow_client.pause_dag(alert["dag_id"])
            pause_detail = f"DAG '{alert['dag_id']}' paused immediately."
            logger.info("approve_or_escalate: P0 — %s", pause_detail)
        except Exception as exc:
            pause_detail = f"Attempted to pause the DAG but the Airflow API call failed: {exc}"
            logger.error("approve_or_escalate: P0 pause failed: %s", exc)

        try:
            post_notification(f"🚨 P0 incident: {summary}\n{pause_detail}")
        except Exception as exc:
            logger.warning("Slack notification failed (non-fatal): %s", exc)

        try:
            request_approval(
                incident_id=incident_id, summary=summary, risk_level=risk_level,
                dag_id=alert["dag_id"], run_id=alert["run_id"], task_id=alert["task_id"],
                severity="P0",
            )
        except Exception as exc:
            logger.warning("Slack approval request failed (non-fatal): %s", exc)

        return {"approval_status": "escalated"}

    # ---------------- P1: safe fix now, else timeout-gated approval -------
    if severity == "P1":
        applied_detail = None
        if error_type == "schema_drift":
            try:
                applied_detail = schema_inspector.apply_safe_rename_fix()
            except Exception as exc:
                logger.error("auto-fix attempt failed: %s", exc)

        if applied_detail:
            retry_detail = _retry_airflow_task(alert)
            detail = applied_detail + (f" {retry_detail}" if retry_detail else "")
            try:
                post_notification(f"✅ P1 auto-fixed: {summary}\n{detail}")
            except Exception as exc:
                logger.warning("Slack notification failed (non-fatal): %s", exc)
            logger.info("approve_or_escalate: P1 auto_approved. %s", detail)
            return {"approval_status": "auto_approved"}

        # No mechanical fix applied yet (e.g. volume_anomaly with nothing
        # to rename, or the rename attempt above raised). For schema_drift
        # specifically, still check read-only whether a safe rename is
        # identifiable — if so, preview it in the Slack message so a human
        # (or escalation_scheduler on timeout) knows exactly what approving
        # will do; handle_slack_action re-attempts the same fix on approval.
        approval_summary = summary
        if error_type == "schema_drift":
            try:
                rename = schema_inspector.find_safe_rename()
            except Exception as exc:
                logger.error("find_safe_rename failed: %s", exc)
                rename = None
            if rename:
                current_name, canonical_name = rename
                approval_summary += (
                    f"\nWill rename column '{current_name}' back to "
                    f"'{canonical_name}' if approved."
                )

        # Ask, but don't block on an answer: escalation_scheduler retries
        # the task on our behalf after SLACK_TIMEOUT_MINUTES if nobody
        # responds.
        try:
            request_approval(
                incident_id=incident_id, summary=approval_summary, risk_level=risk_level,
                dag_id=alert["dag_id"], run_id=alert["run_id"], task_id=alert["task_id"],
                severity="P1", timeout_minutes=SLACK_TIMEOUT_MINUTES,
            )
        except Exception as exc:
            logger.warning("Slack approval request failed (non-fatal): %s", exc)

        logger.info("approve_or_escalate: P1 pending (incident_id=%s)", incident_id)
        return {"approval_status": "pending"}

    # ---------------- P2: silent auto-fix, no Slack ------------------------
    if severity == "P2":
        retry_detail = _retry_airflow_task(alert)
        logger.info("approve_or_escalate: P2 auto_approved (silent). %s", retry_detail)
        return {"approval_status": "auto_approved"}

    # ---------------- P3: log only ------------------------------------------
    logger.info("approve_or_escalate: P3 — logged only, no fix attempted.")
    return {"approval_status": "auto_approved"}


# --------------------------------------------------------------------------
# postmortem
# --------------------------------------------------------------------------

def postmortem_node(state: AgentState) -> dict:
    record = write_postmortem(state)
    logger.info("postmortem: incident_id=%s stored in Qdrant", record["incident_id"])
    return {"postmortem": record}


# --------------------------------------------------------------------------
# graph assembly
# --------------------------------------------------------------------------

def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("ingest", ingest_node)
    graph.add_node("classify", classify_node)
    graph.add_node("investigate", investigate_node)
    graph.add_node("fix", fix_node)
    graph.add_node("approve_or_escalate", approve_or_escalate_node)
    graph.add_node("postmortem", postmortem_node)

    graph.add_edge(START, "ingest")
    graph.add_edge("ingest", "classify")
    graph.add_edge("classify", "investigate")
    graph.add_edge("investigate", "fix")
    graph.add_edge("fix", "approve_or_escalate")
    graph.add_edge("approve_or_escalate", "postmortem")
    graph.add_edge("postmortem", END)

    return graph.compile()


compiled_graph = build_graph()
