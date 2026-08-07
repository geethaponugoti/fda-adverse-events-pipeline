"""
LangGraph graph definition for the FDA pipeline self-healing agent.

Six nodes, run in a fixed sequence (no branching in the graph itself —
the branching happens inside approve_or_escalate based on risk_level):

    ingest -> classify -> investigate -> fix -> approve_or_escalate -> postmortem

Safety note: the `fix` node asks the LLM to propose SQL, but that SQL
is advisory only — it's surfaced to a human in Slack / the postmortem,
never executed. The only DB write approve_or_escalate ever makes
automatically is a single, narrowly-scoped case: renaming a column
back to its pre-drift name, and only when schema_inspector found
exactly one column added and exactly one removed (an unambiguous
rename), with the identifiers coming from information_schema /
monitoring.schema_snapshots — not from LLM output.
"""

import json
import logging
import os
import time
import uuid
from typing import Literal, Optional

from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from mcp_servers.slack_server import post_notification, request_approval
from postmortem import write_postmortem
from state import AgentState
from tools import lineage_tracer, log_parser, memory_lookup, schema_inspector

logger = logging.getLogger("agent.graph")

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")


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
    return {"parsed_log": parsed_log, "started_at": time.time()}


# --------------------------------------------------------------------------
# classify
# --------------------------------------------------------------------------

class _ErrorClassification(BaseModel):
    error_type: Literal[
        "schema_drift", "volume_anomaly", "data_quality", "upstream_failure"
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
        "- schema_drift: a column was renamed, added, or removed\n"
        "- volume_anomaly: row count is unexpectedly high or low\n"
        "- data_quality: existing rows have bad or missing values\n"
        "- upstream_failure: infra/network/API/storage issue unrelated to "
        "the shape or content of the data (timeouts, missing files, OOM, "
        "connection errors)"
    )

    result = _llm().with_structured_output(_ErrorClassification).invoke(prompt)
    logger.info("classify: error_type=%s confidence=%.2f reasoning=%s",
                result.error_type, result.confidence, result.reasoning)

    return {"error_type": result.error_type}


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

        lineage = lineage_tracer.get_impacted_models(column_name=changed_column)
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

def _try_auto_fix_schema_drift(investigation: dict) -> Optional[str]:
    """The only auto-applied DB change in this whole agent: if exactly one
    column was added and exactly one was removed since the last schema
    snapshot, rename the current (added) column back to the original
    (removed) name. Identifiers come from information_schema /
    monitoring.schema_snapshots, never from LLM output."""
    diff = investigation.get("details", {}).get("schema_diff", {})
    added = diff.get("added", [])
    removed = diff.get("removed", [])

    if len(added) != 1 or len(removed) != 1:
        return None

    new_name, original_name = added[0], removed[0]
    with _engine().begin() as conn:
        conn.execute(text(
            f'ALTER TABLE raw.fda_adverse_events '
            f'RENAME COLUMN "{new_name}" TO "{original_name}"'
        ))

    return f"Auto-renamed column '{new_name}' back to '{original_name}'."


def approve_or_escalate_node(state: AgentState) -> dict:
    alert = state["alert"]
    error_type = state.get("error_type", "upstream_failure")
    investigation = state.get("investigation", {})
    proposed_fix = state.get("proposed_fix", {})
    risk_level = proposed_fix.get("risk_level", "high")

    incident_id = str(uuid.uuid4())
    summary = (
        f"[{error_type}] {alert['dag_id']}.{alert['task_id']} "
        f"(run {alert['run_id']}): {proposed_fix.get('description', '')}"
    )

    if risk_level == "low":
        detail = "No safe automatic action available for this error type; approved without a DB change."

        if error_type == "schema_drift":
            try:
                applied_detail = _try_auto_fix_schema_drift(investigation)
                if applied_detail:
                    detail = applied_detail
            except Exception as exc:
                logger.error("auto-fix attempt failed: %s", exc)
                detail = f"Attempted auto-fix failed, needs manual review: {exc}"

        try:
            post_notification(f"✅ Auto-resolved: {summary}\n{detail}")
        except Exception as exc:
            logger.warning("Slack notification failed (non-fatal): %s", exc)

        logger.info("approve_or_escalate: auto_approved. %s", detail)
        return {"approval_status": "auto_approved"}

    # medium and high risk always go to a human via Slack; the agent never
    # applies the fix itself in either case.
    try:
        request_approval(
            incident_id=incident_id,
            summary=summary,
            risk_level=risk_level,
            dag_id=alert["dag_id"],
            task_id=alert["task_id"],
        )
    except Exception as exc:
        logger.warning("Slack approval request failed (non-fatal): %s", exc)

    status = "escalated" if risk_level == "high" else "pending"
    logger.info("approve_or_escalate: %s (incident_id=%s)", status, incident_id)
    return {"approval_status": status}


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
