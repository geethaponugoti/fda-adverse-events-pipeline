"""
LangGraph graph definition for the FDA pipeline self-healing agent.

Six nodes, run in a fixed sequence (no branching in the graph itself —
the branching happens inside approve_or_escalate, based on severity,
P0-P3, computed in classify_node from error_type via SEVERITY_MAP):

    ingest -> classify -> investigate -> fix -> approve_or_escalate -> postmortem

Safety note — read this before changing fix_node or
approve_or_escalate_node: fix_node can now propose LLM-generated SQL
(not just advisory text), but no proposed SQL is ever trusted just
because an LLM wrote it. tools/sql_validator.py structurally classifies
every proposal — statement type, single-vs-multi-statement, WHERE
clause presence, table allowlist — into three tiers, and that
classification (not the LLM's own risk_level self-assessment) is what
approve_or_escalate_node actually acts on:

  REJECTED       — never executed, never even offered for Slack
                   approval (DROP/TRUNCATE/DELETE-without-WHERE/
                   multi-statement/out-of-scope-table).
  NEEDS_APPROVAL — always goes to a human in Slack with the exact SQL
                   shown (ALTER TABLE beyond a rename, UPDATE, INSERT,
                   DELETE with WHERE) — even for P2, which otherwise
                   handles things silently; a real data mutation isn't
                   "no real risk" just because its error category is.
  AUTO_EXECUTABLE — the one form this can ever be: ALTER TABLE ...
                   RENAME COLUMN. Even then, the literal proposed SQL
                   text still isn't what runs — approve_or_escalate_node
                   re-derives and verifies the rename against live
                   information_schema via schema_inspector before
                   executing anything, same invariant this project has
                   held since the very first version of this fix.
                   Qdrant-reused or LLM-authored identifiers are never
                   trusted directly, only used as a signal that a
                   rename might be needed.

Every other "auto-fix" action is a retry of the Airflow task via
airflow_client, which is safe regardless of what caused the original
failure.
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
from tools import airflow_client, lineage_tracer, log_parser, memory_lookup, schema_inspector, sql_validator

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
            "A single corrective SQL statement, if one is genuinely "
            "warranted — otherwise leave this empty and explain why in the "
            "description. This is NOT trusted or executed as-is: "
            "tools/sql_validator.py structurally classifies it afterward "
            "(safe to auto-run / needs a human's approval in Slack / "
            "rejected outright) regardless of what you say here. Propose "
            "exactly one statement — no semicolon-separated chains, those "
            "are rejected automatically — touching only "
            "raw.fda_adverse_events or a monitoring.* table; anything else "
            "is rejected too."
        ),
    )


def _try_reuse_prior_fix(error_message: str) -> Optional[dict]:
    try:
        return memory_lookup.find_reusable_fix(error_message)
    except Exception as exc:
        logger.warning("find_reusable_fix failed, continuing without reuse: %s", exc)
        return None


def fix_node(state: AgentState) -> dict:
    """Three ways a fix gets proposed here, tried in order — each one
    is free/safer than the one behind it, so later options only run
    when the earlier ones don't apply:

      1. Mechanical (schema_drift only): schema_inspector's
         information_schema-verified rename, when the drift is
         unambiguous. Instant, free, and the only source whose exact
         SQL text is ever trusted enough to auto-execute — see the
         module docstring for why.
      2. Qdrant reuse: a semantically similar prior incident whose fix
         actually worked. Saves an OpenAI call.
      3. Fresh OpenAI generation, given the live schema, the last
         snapshot, and the investigation findings as context.

    Whatever comes out of this node is still just a *proposal* —
    approve_or_escalate_node runs it through sql_validator before
    anything executes."""
    error_type = state.get("error_type")
    severity = state.get("severity")
    investigation = state.get("investigation", {})
    parsed_log = state.get("parsed_log", {})
    error_message = parsed_log.get("error_message", "")

    if severity == "P3":
        return {"proposed_fix": {
            "description": "P3 (upstream/infra) — no fix attempted, logged only.",
            "risk_level": "low", "sql": None, "auto_applicable": False, "source": "none",
        }}

    # 1. Mechanical.
    if error_type == "schema_drift":
        try:
            rename = schema_inspector.find_safe_rename()
        except Exception as exc:
            logger.error("find_safe_rename failed: %s", exc)
            rename = None
        if rename:
            current_name, canonical_name = rename
            sql = (
                f'ALTER TABLE raw.fda_adverse_events '
                f'RENAME COLUMN "{current_name}" TO "{canonical_name}"'
            )
            logger.info("fix: mechanical rename available: %s", sql)
            return {"proposed_fix": {
                "description": f"Rename column '{current_name}' back to '{canonical_name}'.",
                "risk_level": "low", "sql": sql, "auto_applicable": True, "source": "mechanical",
            }}

    # 2. Qdrant reuse.
    reused = _try_reuse_prior_fix(error_message)
    if reused:
        logger.info("fix: reusing prior fix from Qdrant (score=%.2f, incident_id=%s)",
                    reused["score"], reused.get("incident_id"))
        return {"proposed_fix": {
            "description": (
                f"Reusing a previously successful fix for a similar incident "
                f"(similarity {reused['score']:.2f}): {reused['description']}"
            ),
            "risk_level": reused.get("risk_level") or "medium",
            "sql": reused.get("sql"),
            "auto_applicable": False,
            "source": "qdrant_reuse",
        }}

    # 3. Fresh OpenAI generation, with real schema context — not just
    # investigation.details.schema_diff, which can itself already be
    # stale (see schema_inspector.find_safe_rename()'s docstring).
    try:
        current_schema = schema_inspector.get_current_columns()
    except Exception as exc:
        logger.warning("get_current_columns failed: %s", exc)
        current_schema = {}
    try:
        previous_schema = schema_inspector.get_previous_snapshot()
    except Exception as exc:
        logger.warning("get_previous_snapshot failed: %s", exc)
        previous_schema = None

    prompt = (
        f"A '{error_type}' failure was investigated in the FDA adverse events "
        f"pipeline.\n\n"
        f"Error message: {error_message}\n"
        f"Investigation summary: {investigation.get('summary')}\n"
        f"Investigation details: {json.dumps(investigation.get('details', {}), default=str)[:3000]}\n\n"
        f"Current live columns of raw.fda_adverse_events: {sorted(current_schema)}\n"
        f"Most recent schema snapshot (this can itself already reflect a "
        f"past drift rather than the true original schema — treat it as "
        f"context, not ground truth): "
        f"{sorted(previous_schema) if previous_schema else 'none recorded'}\n\n"
        "Propose a fix. If a single SQL statement would genuinely resolve "
        "this, propose exactly one. If no SQL fix makes sense — this needs "
        "a human's domain judgment, or nothing in the database is actually "
        "wrong — leave sql empty and say so in the description. Be honest "
        "about risk_level."
    )

    result = _llm().with_structured_output(_ProposedFixLLM).invoke(prompt)
    logger.info("fix: source=llm risk_level=%s sql=%r description=%s",
                result.risk_level, result.sql, result.description)

    return {
        "proposed_fix": {
            "description": result.description,
            "risk_level": result.risk_level,
            "sql": result.sql,
            "auto_applicable": False,
            "source": "llm",
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
    """Severity (P0-P3, from classify_node) still drives the overall
    response strategy. sql_validator.classify_sql() is a second,
    orthogonal gate on top of it that decides whether *this specific
    proposed fix* is safe to run — see the module docstring for the
    three tiers. The two combine as:

      P0 (data corruption): pause the DAG immediately, escalate via
      Slack now, then via email/re-confirmed-pause on the timeout
      ladder if unacknowledged. Never attempts a fix — unchanged.

      P3 (upstream/infra): log only, no fix attempted — unchanged.

      P1 / P2, no SQL proposed: just retry the task (existing
      behavior for e.g. volume_anomaly with nothing mechanical to fix).

      P1 / P2, SQL classified AUTO_EXECUTABLE (always a verified
      rename): apply it, retry, notify on P1 / log only on P2.

      P1 / P2, SQL classified NEEDS_APPROVAL: always goes to Slack
      with the SQL shown, regardless of severity — a real data
      mutation isn't "no real risk" just because its error category
      is. P1 auto-applies (executes the shown SQL, then retries) after
      SLACK_TIMEOUT_MINUTES if nobody responds; P2 waits for an
      explicit human decision rather than defaulting into running a
      mutation nobody confirmed.

      P1 / P2, SQL classified REJECTED: never executed, never offered
      for approval, but always surfaced to Slack even for P2 —
      silently dropping something flagged dangerous would hide a real
      problem instead of handling it quietly.
    """
    alert = state["alert"]
    error_type = state.get("error_type", "upstream_failure")
    severity = state.get("severity") or SEVERITY_MAP.get(error_type, "P3")
    proposed_fix = state.get("proposed_fix", {})
    risk_level = proposed_fix.get("risk_level", "high")
    source = proposed_fix.get("source", "none")
    sql = proposed_fix.get("sql")

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

        return {"approval_status": "escalated", "fix_executed": False}

    # ---------------- P3: log only ------------------------------------------
    if severity == "P3":
        logger.info("approve_or_escalate: P3 — logged only, no fix attempted.")
        return {"approval_status": "auto_approved", "fix_executed": False}

    # ---------------- P1 / P2: classify the proposed SQL and route --------
    classification = sql_validator.classify_sql(sql) if sql else None

    if classification is None:
        retry_detail = _retry_airflow_task(alert)
        if severity == "P1":
            try:
                post_notification(f"↻ P1: {summary}\n{retry_detail or 'Retry attempted.'}")
            except Exception as exc:
                logger.warning("Slack notification failed (non-fatal): %s", exc)
        logger.info("approve_or_escalate: %s, no SQL proposed, retried. %s", severity, retry_detail)
        return {"approval_status": "auto_approved", "fix_executed": False}

    if classification.tier == sql_validator.SqlTier.REJECTED:
        try:
            post_notification(
                f"⛔ Proposed fix rejected as unsafe ({source}): {summary}\n"
                f"Reason: {classification.reason}\nProposed SQL: {sql}"
            )
        except Exception as exc:
            logger.warning("Slack notification failed (non-fatal): %s", exc)
        logger.warning("approve_or_escalate: fix REJECTED — %s", classification.reason)
        return {"approval_status": "escalated", "fix_executed": False}

    if classification.tier == sql_validator.SqlTier.AUTO_EXECUTABLE:
        # Never trust the proposed text directly, even here — re-derive
        # and verify against live information_schema (see module
        # docstring). If it no longer applies (someone already fixed
        # it, or the proposal was simply wrong), fall back to a plain
        # retry rather than running unverified text.
        applied_detail = None
        try:
            applied_detail = schema_inspector.apply_safe_rename_fix()
        except Exception as exc:
            logger.error("apply_safe_rename_fix failed: %s", exc)

        retry_detail = _retry_airflow_task(alert)
        if applied_detail:
            detail = applied_detail + (f" {retry_detail}" if retry_detail else "")
            if severity == "P1":
                try:
                    post_notification(f"✅ Auto-fixed ({source}): {summary}\n{detail}")
                except Exception as exc:
                    logger.warning("Slack notification failed (non-fatal): %s", exc)
            logger.info("approve_or_escalate: %s auto_approved. %s", severity, detail)
        else:
            logger.info("approve_or_escalate: proposed rename (%s) no longer applies, retried instead. %s",
                        source, retry_detail)
        return {"approval_status": "auto_approved", "fix_executed": bool(applied_detail)}

    # NEEDS_APPROVAL — always to Slack with the SQL shown, whether P1
    # or P2. P1 auto-applies on timeout (escalation_scheduler executes
    # the shown SQL, then retries); P2 has no timeout — it waits for
    # an explicit human decision rather than defaulting a data
    # mutation into running because nobody happened to respond.
    timeout = SLACK_TIMEOUT_MINUTES if severity == "P1" else 0
    try:
        request_approval(
            incident_id=incident_id, summary=f"{summary}\n({classification.reason})",
            risk_level=risk_level,
            dag_id=alert["dag_id"], run_id=alert["run_id"], task_id=alert["task_id"],
            severity=severity, timeout_minutes=timeout, proposed_sql=sql,
        )
    except Exception as exc:
        logger.warning("Slack approval request failed (non-fatal): %s", exc)

    logger.info("approve_or_escalate: %s pending approval (incident_id=%s, sql_tier=%s)",
                severity, incident_id, classification.tier.value)
    # Not yet known — resolved later, asynchronously, by a human's
    # Slack click or escalation_scheduler's timeout. postmortem_node
    # runs once, synchronously, right after this — it can't see that
    # future outcome, so this incident's Qdrant entry won't be
    # eligible for reuse until/unless it's written again later (it
    # currently isn't — same known limitation as monitoring.
    # incident_reports going stale for anything that doesn't resolve
    # immediately).
    return {"approval_status": "pending", "fix_executed": False}


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
