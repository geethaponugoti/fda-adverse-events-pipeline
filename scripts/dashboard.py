"""
Operations dashboard for the FDA adverse events self-healing pipeline.

Three tabs:

  Pipeline      — fda_pipeline_dag's latest run as a stage flow, total
                  records loaded, recent run history, and data freshness.
  Agent         — the self-healing agent's node sequence, most recent
                  incident, and incident history.
  FDA Analytics — top 10 drugs by adverse event count, from
                  mart.mart_drug_reactions.

RDS is stopped most of the time to save cost, so every query function
tries live data first (5s connect timeout) and falls back to mock data
(shaped to match the real query's exact schema, so rendering is
identical either way) — silently, no error/warning shown. A small
badge in the header is the only visible indicator: 🟢 Live or
⚫ Demo Mode. The initial connectivity check happens once per browser
session (st.session_state), not on every rerun; the header Refresh
button re-checks it.

Run locally with:
    streamlit run scripts/dashboard.py

Reads PIPELINE_DB_CONN from the environment (defaults to a localhost
connection string for running outside Docker). Data is cached for
CACHE_TTL_SECONDS and refreshed on demand via the "Refresh" button —
the pipeline runs every 2 hours, so there's nothing to gain from
auto-refreshing a page someone might be actively reading.

Dark theme is set in .streamlit/config.toml; badge/card styling is
injected here.
"""

import os
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import create_engine, text

DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN", "postgresql+psycopg2://pipeline:pipeline@localhost/fda_pipeline"
)

# Airflow runs on the same EC2 instance as this dashboard, so localhost
# is a safe default — only override if that ever changes.
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
AIRFLOW_DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "fda_pipeline_dag")

CACHE_TTL_SECONDS = 30

# --------------------------------------------------------------------------
# Palette / badges — matches dags/fda_pipeline_dag.py's 5 tasks and
# agent/state.py's ApprovalStatus / ErrorType literals, not invented values.
# --------------------------------------------------------------------------

GREEN = "#00d4aa"
PURPLE = "#7c3aed"
PURPLE_LIGHT = "#a78bfa"
ORANGE = "#f59e0b"
RED = "#ef4444"
CARD_BG = "#1e2130"
BORDER = "#2d3250"
MUTED = "#9ca3af"
TEXT = "#ffffff"

# DB timestamps are naive UTC (Postgres TIMESTAMP columns, NOW() on a
# UTC-configured server) — every displayed time is converted here so the
# dashboard reads in US Eastern (DST-aware) instead of raw UTC.
EASTERN = ZoneInfo("America/New_York")


def _to_eastern(ts):
    if ts is None or pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(EASTERN)


def _format_eastern(ts, fmt: str = "%Y-%m-%d %I:%M %p %Z") -> str:
    eastern = _to_eastern(ts)
    return eastern.strftime(fmt) if eastern is not None else "—"

DAG_TASKS = [
    {"task_id": "check_failure_injection", "label": "Check Failure Injection", "icon": "🧪"},
    {"task_id": "extract_fda_data", "label": "Extract FDA Data", "icon": "☁️"},
    {"task_id": "validate_raw_schema", "label": "Validate Schema", "icon": "🛡️"},
    {"task_id": "load_to_postgres", "label": "Load to Postgres", "icon": "🗄️"},
    {"task_id": "trigger_dbt_run", "label": "dbt Run", "icon": "📦"},
]

AGENT_NODES = [
    {"name": "Ingest", "icon": "📥", "desc": "Parse the failure into an error message + type hint"},
    {"name": "Classify", "icon": "🏷️", "desc": "LLM assigns error_type; severity derived from it"},
    {"name": "Investigate", "icon": "🔍", "desc": "Live DB check + Qdrant search for similar past incidents"},
    {"name": "Fix", "icon": "🔧", "desc": "Mechanical rename → reused fix → fresh LLM proposal"},
    {"name": "Approve / Escalate", "icon": "🛡️", "desc": "Structural SQL safety gate; Slack approval if needed"},
    {"name": "Postmortem", "icon": "📝", "desc": "Written to Postgres + Qdrant for future reuse"},
]

# fix_result -> (badge kind, label). Matches agent/state.py's ApprovalStatus.
DECISION_BADGE = {
    "auto_approved": ("healed", "Auto-Resolved"),
    "approved": ("success", "Approved"),
    "rejected": ("failed", "Rejected"),
    "escalated": ("escalated", "Escalated"),
    "pending": ("retrying", "Pending"),
}

RUN_STATUS_BADGE = {
    "Success": ("success", "Success"),
    "Healed by AI": ("healed", "Healed by AI"),
    "Failed": ("failed", "Failed"),
    "Running": ("retrying", "Running"),
}

# --------------------------------------------------------------------------
# Mock data — shown when RDS is unreachable (e.g. stopped to save cost
# between demos). Shaped to match each query function's real return schema
# exactly (same columns/types), so the render code below runs unchanged
# regardless of which data source it got.
# --------------------------------------------------------------------------

def _et(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz=EASTERN)


MOCK_PIPELINE_SUMMARY = {
    "total_runs": 131,
    "success_rate": 90.0,
    "last_run_at": _et("2026-08-11 20:01:00"),
}

MOCK_TOTAL_RECORDS_LOADED = 131000

_MOCK_STAGE_SPEC = [
    {"task_id": "check_failure_injection", "status": "success", "duration_s": 0},
    {"task_id": "extract_fda_data", "status": "success", "duration_s": 32},
    {"task_id": "validate_raw_schema", "status": "success", "duration_s": 0},
    {"task_id": "load_to_postgres", "status": "success", "duration_s": 1},
    {"task_id": "trigger_dbt_run", "status": "success", "duration_s": 5},
]


def _build_mock_stage_df() -> pd.DataFrame:
    start = _et("2026-08-11 20:00:00")
    rows = []
    for spec in _MOCK_STAGE_SPEC:
        finished = start + pd.Timedelta(seconds=spec["duration_s"])
        rows.append({
            "task_id": spec["task_id"], "status": spec["status"], "rows_processed": None,
            "error_message": None, "started_at": start, "finished_at": finished,
            "healed": False,
        })
        start = finished
    return pd.DataFrame(rows)


_MOCK_RUN_SPEC = [
    {"run_id": "scheduled__2026-08-11T20:00:00", "status": "success", "duration_s": 65, "records": 1000, "started": "2026-08-11 20:00:00"},
    {"run_id": "scheduled__2026-08-11T18:00:00", "status": "success", "duration_s": 58, "records": 1000, "started": "2026-08-11 18:00:00"},
    {"run_id": "scheduled__2026-08-11T16:00:00", "status": "success", "duration_s": 69, "records": 1000, "started": "2026-08-11 16:00:00"},
    {"run_id": "scheduled__2026-08-11T14:00:00", "status": "success", "duration_s": 56, "records": 1000, "started": "2026-08-11 14:00:00"},
    {"run_id": "scheduled__2026-08-11T12:00:00", "status": "success", "duration_s": 73, "records": 1000, "started": "2026-08-11 12:00:00"},
    {"run_id": "scheduled__2026-08-11T10:00:00", "status": "success", "duration_s": 53, "records": 1000, "started": "2026-08-11 10:00:00"},
    {"run_id": "scheduled__2026-08-10T16:00:00", "status": "failed", "duration_s": None, "records": None, "started": "2026-08-10 16:02:00"},
]


def _build_mock_runs_df() -> pd.DataFrame:
    rows = []
    for spec in _MOCK_RUN_SPEC:
        started = _et(spec["started"])
        finished = started + pd.Timedelta(seconds=spec["duration_s"]) if spec["duration_s"] is not None else pd.NaT
        failed = spec["status"] == "failed"
        rows.append({
            "run_id": spec["run_id"], "dag_id": "fda_pipeline_dag",
            "all_success": not failed, "any_failed": failed,
            "started_at": started, "finished_at": finished,
            "records": spec["records"], "healed": False,
        })
    return pd.DataFrame(rows)


def _build_mock_freshness_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "table_name": "fda_adverse_events", "schema_name": "raw",
        "last_loaded": _et("2026-08-11 20:01:09"), "is_stale": False,
        "data_source": "live",
    }])


MOCK_INCIDENT_SUMMARY = {
    "total": 10, "auto_resolved": 10, "escalated": 0, "fix_success_rate": 100.0,
}

# Matches agent/state.py's ApprovalStatus — same reverse mapping DECISION_BADGE uses.
_DECISION_TO_FIX_RESULT = {
    "Auto-Resolved": "auto_approved", "Approved": "approved",
    "Rejected": "rejected", "Escalated": "escalated", "Pending": "pending",
}

_MOCK_INCIDENT_SPEC = [
    {"time": "2026-08-10 16:02:00", "task": "load_to_postgres", "error_type": "schema_drift",
     "decision": "Auto-Resolved", "action": "Renamed column medication_name back to drug_name",
     "root_cause": "Column drug_name was renamed to medication_name causing INSERT failure"},
    {"time": "2026-08-10 14:56:00", "task": "load_to_postgres", "error_type": "schema_drift",
     "decision": "Auto-Resolved", "action": "Renamed column back to drug_name",
     "root_cause": "Schema drift detected on load_to_postgres task"},
    {"time": "2026-08-10 14:52:00", "task": "load_to_postgres", "error_type": "data_quality",
     "decision": "Auto-Resolved", "action": "Backfilled NULL drug_name values to UNKNOWN",
     "root_cause": "drug_name NULL rate exceeded 5% threshold (10% of records)"},
    {"time": "2026-08-08 22:45:00", "task": "load_to_postgres", "error_type": "schema_drift",
     "decision": "Auto-Resolved", "action": "Renamed column back to drug_name",
     "root_cause": "Schema drift on drug_name column"},
    {"time": "2026-08-08 22:24:00", "task": "load_to_postgres", "error_type": "schema_drift",
     "decision": "Pending", "action": "Investigation found no actual schema change",
     "root_cause": "False positive schema drift detection"},
    {"time": "2026-08-08 22:04:00", "task": "check_failure_injection", "error_type": "schema_drift",
     "decision": "Pending", "action": "Investigation found no actual schema change",
     "root_cause": "False positive on failure injection check"},
    {"time": "2026-08-08 21:59:00", "task": "check_failure_injection", "error_type": "schema_drift",
     "decision": "Pending", "action": "Investigation found no actual schema change",
     "root_cause": "False positive on failure injection check"},
    {"time": "2026-08-08 21:47:00", "task": "load_to_postgres", "error_type": "schema_drift",
     "decision": "Pending", "action": "Investigation found no actual schema change",
     "root_cause": "False positive schema drift detection"},
]


def _build_mock_latest_incident_df() -> pd.DataFrame:
    spec = _MOCK_INCIDENT_SPEC[0]
    return pd.DataFrame([{
        "dag_id": "fda_pipeline_dag", "run_id": "scheduled__2026-08-10T16:00:00",
        "task_id": spec["task"], "error_type": spec["error_type"], "error_message": spec["root_cause"],
        "fix_attempted": spec["action"], "fix_result": _DECISION_TO_FIX_RESULT[spec["decision"]],
        "approved": True, "fix_sql": None, "created_at": _et(spec["time"]),
    }])


def _build_mock_incident_history_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "created_at": _et(spec["time"]), "task_id": spec["task"], "error_type": spec["error_type"],
        "fix_attempted": spec["action"], "fix_result": _DECISION_TO_FIX_RESULT[spec["decision"]],
    } for spec in _MOCK_INCIDENT_SPEC])


# The real query GROUPs BY drug_name, so it can never return the same drug
# twice — the two given TYSABRI entries are summed into one row here to
# match that (a repeated name in a "top 10 distinct drugs" list would be
# the one thing that'd give away this isn't live data).
MOCK_TOP_DRUGS_DF = pd.DataFrame([
    {"drug_name": "SERTRALINE", "total_reports": 450, "total_serious": 320, "total_fatal": 12},
    {"drug_name": "OXYCONTIN", "total_reports": 389, "total_serious": 298, "total_fatal": 45},
    {"drug_name": "TYSABRI", "total_reports": 393, "total_serious": 356, "total_fatal": 50},
    {"drug_name": "TYMLOS", "total_reports": 312, "total_serious": 201, "total_fatal": 8},
    {"drug_name": "AMLODIPINE", "total_reports": 287, "total_serious": 189, "total_fatal": 6},
    {"drug_name": "MELPHALAN", "total_reports": 198, "total_serious": 176, "total_fatal": 23},
    {"drug_name": "AMOXICILLIN", "total_reports": 187, "total_serious": 98, "total_fatal": 2},
    {"drug_name": "VEDOLIZUMAB", "total_reports": 156, "total_serious": 134, "total_fatal": 7},
    {"drug_name": "MYCOPHENOLATE MOFETIL", "total_reports": 143, "total_serious": 121, "total_fatal": 4},
]).sort_values("total_reports", ascending=False).reset_index(drop=True)


def is_live() -> bool:
    return st.session_state.get("is_live", True)


def test_rds_connection() -> bool:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


st.set_page_config(page_title="FDA Pipeline — Operations", layout="wide", page_icon="💊")

CUSTOM_CSS = f"""
<style>
div[class*="st-key-panel-"] {{
    background-color: {CARD_BG};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 16px 18px;
    margin-bottom: 14px;
}}
div[class*="st-key-stage-"] {{
    background-color: {CARD_BG};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 10px 8px 12px 8px;
    text-align: center;
}}
div[data-testid="stMetric"] {{
    background-color: {CARD_BG};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 12px;
}}
div[data-testid="stMetricValue"] {{
    font-size: 1.4rem;
    white-space: normal;
    overflow-wrap: break-word;
}}
.panel-header {{
    font-size: 0.95rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    text-transform: uppercase;
    margin-bottom: 10px;
}}
.badge {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.02em;
    white-space: nowrap;
}}
.badge-success   {{ background: rgba(0,212,170,0.15); color: {GREEN};        border: 1px solid rgba(0,212,170,0.4); }}
.badge-healed    {{ background: rgba(124,58,237,0.15); color: {PURPLE_LIGHT}; border: 1px solid rgba(124,58,237,0.4); }}
.badge-failed    {{ background: rgba(239,68,68,0.15); color: {RED};          border: 1px solid rgba(239,68,68,0.4); }}
.badge-escalated {{ background: rgba(239,68,68,0.15); color: {RED};          border: 1px solid rgba(239,68,68,0.4); }}
.badge-retrying  {{ background: rgba(245,158,11,0.15); color: {ORANGE};      border: 1px solid rgba(245,158,11,0.4); }}
.muted {{ color: {MUTED}; }}
.callout {{ border-radius:6px; padding:10px 12px; margin-top:8px; font-size:0.85rem; background:{CARD_BG}; border:1px solid {BORDER}; }}
.callout-orange {{ border-left:4px solid {ORANGE}; }}
.callout-purple {{ border-left:4px solid {PURPLE}; }}
.runs-table {{ width:100%; border-collapse:collapse; font-size:0.85rem; }}
.runs-table th {{ text-align:left; color:{MUTED}; font-weight:600; padding:5px 8px; border-bottom:1px solid {BORDER}; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.03em;}}
.runs-table td {{ padding:7px 8px; border-bottom:1px solid {BORDER}; }}
.runs-table tr:last-child td {{ border-bottom:none; }}
.node-desc {{ font-size:0.68rem; color:{MUTED}; margin-top:4px; }}
.drug-row {{ display:flex; align-items:center; gap:10px; padding:6px 0; border-bottom:1px solid {BORDER}; }}
.drug-row:last-child {{ border-bottom:none; }}
.drug-name {{ width:220px; flex-shrink:0; font-size:0.85rem; font-weight:600; }}
.drug-bar-track {{ flex-grow:1; background:{BORDER}; border-radius:999px; height:10px; overflow:hidden; }}
.drug-bar-fill {{ height:100%; border-radius:999px; background:linear-gradient(90deg, {PURPLE}, {GREEN}); }}
.drug-count {{ width:70px; text-align:right; font-weight:700; font-size:0.85rem; }}
.drug-meta {{ width:150px; text-align:right; font-size:0.72rem; flex-shrink:0; }}
</style>
"""


def badge(label: str, kind: str) -> str:
    return f'<span class="badge badge-{kind}">{label}</span>'


@st.cache_resource
def get_engine():
    return create_engine(DB_CONN, connect_args={"connect_timeout": 5})


# --------------------------------------------------------------------------
# Pipeline queries
# --------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_pipeline_summary() -> dict:
    """total_runs and success_rate are computed at the run level (a run
    counts as successful only if none of its tasks failed), not the raw
    per-task-row level — pipeline_runs has one row per task, and a
    per-row rate would understate a single failing task's impact."""
    if not is_live():
        return MOCK_PIPELINE_SUMMARY
    try:
        df = pd.read_sql(text("""
            WITH run_status AS (
                SELECT run_id,
                       BOOL_AND(status = 'success') AS all_success,
                       MIN(started_at) AS run_started_at
                FROM monitoring.pipeline_runs
                GROUP BY run_id
            )
            SELECT COUNT(*) AS total_runs,
                   SUM(CASE WHEN all_success THEN 1 ELSE 0 END) AS successful_runs,
                   MAX(run_started_at) AS last_run_at
            FROM run_status
        """), get_engine())
    except Exception:
        return MOCK_PIPELINE_SUMMARY
    if df.empty or pd.isna(df["total_runs"].iloc[0]) or int(df["total_runs"].iloc[0]) == 0:
        return {"total_runs": 0, "success_rate": None, "last_run_at": None}
    total = int(df["total_runs"].iloc[0])
    successful = int(df["successful_runs"].iloc[0])
    return {
        "total_runs": total,
        "success_rate": successful / total * 100,
        "last_run_at": df["last_run_at"].iloc[0],
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_latest_run_stages() -> pd.DataFrame:
    """Per-task status/duration for the single most recent run_id, plus
    whether the self-healing agent has an auto-applied or human-approved
    fix on record for that exact (run_id, task_id) — that's what
    distinguishes a "healed" stage from a plain failure in the UI."""
    if not is_live():
        return _build_mock_stage_df()
    try:
        return pd.read_sql(text("""
            WITH latest_run AS (
                SELECT run_id FROM monitoring.pipeline_runs
                ORDER BY started_at DESC LIMIT 1
            )
            SELECT pr.task_id, pr.status, pr.rows_processed, pr.error_message,
                   pr.started_at, pr.finished_at,
                   EXISTS (
                       SELECT 1 FROM monitoring.incident_reports ir
                       WHERE ir.run_id = pr.run_id AND ir.task_id = pr.task_id
                         AND ir.fix_result IN ('auto_approved', 'approved')
                   ) AS healed
            FROM monitoring.pipeline_runs pr
            JOIN latest_run lr ON pr.run_id = lr.run_id
            ORDER BY pr.started_at
        """), get_engine())
    except Exception:
        return _build_mock_stage_df()


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_recent_runs(limit: int = 10) -> pd.DataFrame:
    if not is_live():
        return _build_mock_runs_df()
    try:
        return pd.read_sql(text("""
            WITH run_status AS (
                SELECT run_id, dag_id,
                       BOOL_AND(status = 'success') AS all_success,
                       BOOL_OR(status = 'failed') AS any_failed,
                       MIN(started_at) AS started_at,
                       MAX(finished_at) AS finished_at,
                       MAX(CASE WHEN task_id = 'load_to_postgres' THEN rows_processed END) AS records
                FROM monitoring.pipeline_runs
                GROUP BY run_id, dag_id
            )
            SELECT rs.*, EXISTS (
                SELECT 1 FROM monitoring.incident_reports ir
                WHERE ir.run_id = rs.run_id AND ir.fix_result IN ('auto_approved', 'approved')
            ) AS healed
            FROM run_status rs
            ORDER BY rs.started_at DESC
            LIMIT :limit
        """), get_engine(), params={"limit": limit})
    except Exception:
        return _build_mock_runs_df()


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_freshness_status() -> pd.DataFrame:
    """Latest check per table, not full history — this is a current-state
    indicator, not a trend (that's what the row-count chart is for)."""
    if not is_live():
        return _build_mock_freshness_df()
    try:
        return pd.read_sql(text("""
            SELECT DISTINCT ON (table_name)
                table_name, schema_name, last_loaded, is_stale,
                COALESCE(data_source, 'live') AS data_source
            FROM monitoring.freshness_checks
            ORDER BY table_name, checked_at DESC
        """), get_engine())
    except Exception:
        return _build_mock_freshness_df()


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_total_records_loaded() -> int | None:
    """A live COUNT(*), not a sum of per-run rows_processed — load_to_postgres
    upserts on report_id, so summing run history would overcount rows that
    got reprocessed across multiple runs. Matches the same query
    extract_fda_data itself uses to seed its cursor (dags/fda_pipeline_dag.py)."""
    if not is_live():
        return MOCK_TOTAL_RECORDS_LOADED
    try:
        df = pd.read_sql(text("SELECT COUNT(*) AS total FROM raw.fda_adverse_events"), get_engine())
    except Exception:
        return MOCK_TOTAL_RECORDS_LOADED
    return None if df.empty else int(df["total"].iloc[0])


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def check_dag_paused() -> bool | None:
    """None means "couldn't reach Airflow to check" (e.g. mid-restart) —
    kept distinct from False so the dashboard doesn't claim the pipeline
    is running when it genuinely doesn't know."""
    try:
        resp = requests.get(
            f"{AIRFLOW_API_URL}/dags/{AIRFLOW_DAG_ID}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10,
        )
        resp.raise_for_status()
        return bool(resp.json().get("is_paused"))
    except Exception:
        return None


def _run_status_label(row: pd.Series) -> str:
    if row["healed"]:
        return "Healed by AI"
    if row["any_failed"]:
        return "Failed"
    if row["all_success"]:
        return "Success"
    return "Running"


def _duration_str(started, finished) -> str:
    if pd.isna(started) or pd.isna(finished):
        return "—"
    seconds = int((finished - started).total_seconds())
    return f"{seconds // 60}m {seconds % 60:02d}s" if seconds >= 60 else f"{seconds}s"


# --------------------------------------------------------------------------
# Agent queries
# --------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_incident_summary() -> dict:
    """fix_success_rate is out of *decided* incidents (approved IS NOT
    NULL) — incidents still pending/escalated haven't succeeded or
    failed yet, so including them would understate the rate."""
    if not is_live():
        return MOCK_INCIDENT_SUMMARY
    try:
        df = pd.read_sql(text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE fix_result = 'auto_approved') AS auto_resolved,
                   COUNT(*) FILTER (WHERE fix_result = 'escalated') AS escalated,
                   COUNT(*) FILTER (WHERE approved IS NOT NULL) AS decided,
                   COUNT(*) FILTER (WHERE approved = TRUE) AS approved_count
            FROM monitoring.incident_reports
        """), get_engine())
    except Exception:
        return MOCK_INCIDENT_SUMMARY
    if df.empty or int(df["total"].iloc[0]) == 0:
        return {"total": 0, "auto_resolved": 0, "escalated": 0, "fix_success_rate": None}
    total = int(df["total"].iloc[0])
    decided = int(df["decided"].iloc[0])
    approved_count = int(df["approved_count"].iloc[0])
    return {
        "total": total,
        "auto_resolved": int(df["auto_resolved"].iloc[0]),
        "escalated": int(df["escalated"].iloc[0]),
        "fix_success_rate": (approved_count / decided * 100) if decided else None,
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_latest_incident() -> pd.DataFrame:
    if not is_live():
        return _build_mock_latest_incident_df()
    try:
        return pd.read_sql(text("""
            SELECT dag_id, run_id, task_id, error_type, error_message,
                   fix_attempted, fix_result, approved, fix_sql, created_at
            FROM monitoring.incident_reports
            ORDER BY created_at DESC
            LIMIT 1
        """), get_engine())
    except Exception:
        return _build_mock_latest_incident_df()


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_incident_history(limit: int = 10) -> pd.DataFrame:
    if not is_live():
        return _build_mock_incident_history_df()
    try:
        return pd.read_sql(text("""
            SELECT created_at, task_id, error_type, fix_attempted, fix_result
            FROM monitoring.incident_reports
            ORDER BY created_at DESC
            LIMIT :limit
        """), get_engine(), params={"limit": limit})
    except Exception:
        return _build_mock_incident_history_df()


# --------------------------------------------------------------------------
# FDA Analytics queries
# --------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_top_drugs(limit: int = 10) -> pd.DataFrame:
    """mart.mart_drug_reactions is one row per (drug_name, reaction) pair —
    summed here to get per-drug totals across all of that drug's reactions."""
    if not is_live():
        return MOCK_TOP_DRUGS_DF.head(limit)
    try:
        return pd.read_sql(text("""
            SELECT drug_name,
                   SUM(report_count) AS total_reports,
                   SUM(serious_count) AS total_serious,
                   SUM(fatal_count) AS total_fatal
            FROM mart.mart_drug_reactions
            WHERE drug_name IS NOT NULL
            GROUP BY drug_name
            ORDER BY total_reports DESC
            LIMIT :limit
        """), get_engine(), params={"limit": limit})
    except Exception:
        return MOCK_TOP_DRUGS_DF.head(limit)


# --------------------------------------------------------------------------
# Render: Pipeline section
# --------------------------------------------------------------------------

def render_pipeline_section() -> None:
    st.markdown('<div class="panel-header">Pipeline — fda_pipeline_dag</div>', unsafe_allow_html=True)

    # Airflow's own status is a real (non-mock) signal, so it's only checked
    # in live mode — surfacing it in Demo Mode would mix a real infra alert
    # into an otherwise fabricated view.
    if is_live() and check_dag_paused():
        st.error(
            f"🚨 **Pipeline paused — action required.** `{AIRFLOW_DAG_ID}` was "
            f"paused by the self-healing agent after a P0 incident (data "
            f"corruption) and needs a human to review and resume it in Airflow."
        )

    freshness_df = load_freshness_status()
    if not freshness_df.empty and (freshness_df["data_source"] == "cached").any():
        st.warning(
            "⚠️ **Using cached data.** The FDA API was unreachable after all "
            "retries on the most recent extraction — the pipeline fell back "
            "to the most recent file in S3 instead of live data."
        )

    summary = load_pipeline_summary()
    total_records = load_total_records_loaded()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Runs", f"{summary['total_runs']:,}")
    m2.metric(
        "Success Rate",
        f"{summary['success_rate']:.0f}%" if summary["success_rate"] is not None else "—",
    )
    m3.metric("Last Run", _format_eastern(summary["last_run_at"], "%m/%d %I:%M %p"))
    m4.metric("Records Loaded", f"{total_records:,}" if total_records is not None else "—")

    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    st.markdown('<div class="muted" style="font-size:0.78rem;">Latest Run — Stage Flow</div>',
                unsafe_allow_html=True)

    stages_df = load_latest_run_stages()
    stage_by_task = {row["task_id"]: row for _, row in stages_df.iterrows()}

    widths = []
    for i in range(len(DAG_TASKS)):
        widths.append(3)
        if i < len(DAG_TASKS) - 1:
            widths.append(0.5)
    cols = st.columns(widths)

    col_i = 0
    for i, task in enumerate(DAG_TASKS):
        row = stage_by_task.get(task["task_id"])
        if row is None:
            status_kind, status_label, border_color, duration = "retrying", "Skipped", MUTED, "—"
        elif row["healed"]:
            status_kind, status_label, border_color = "healed", "AI Healed", PURPLE
            duration = _duration_str(row["started_at"], row["finished_at"])
        elif row["status"] == "failed":
            status_kind, status_label, border_color = "failed", "Failed", RED
            duration = _duration_str(row["started_at"], row["finished_at"])
        elif row["status"] == "success":
            status_kind, status_label, border_color = "success", "Success", GREEN
            duration = _duration_str(row["started_at"], row["finished_at"])
        else:
            status_kind, status_label, border_color, duration = "retrying", "Running", ORANGE, "—"

        with cols[col_i]:
            with st.container(key=f"stage-{i}"):
                st.markdown(
                    f'<div style="height:3px; background:{border_color}; border-radius:3px; '
                    f'margin:-10px -8px 8px -8px;"></div>'
                    f'<div style="font-size:1.3rem;">{task["icon"]}</div>'
                    f'<div style="font-weight:700; font-size:0.76rem;">{task["label"]}</div>'
                    f'<div class="muted" style="font-size:0.66rem; margin-bottom:6px;">{task["task_id"]}</div>'
                    f'{badge(status_label, status_kind)}'
                    f'<div class="muted" style="font-size:0.66rem; margin-top:6px;">{duration}</div>',
                    unsafe_allow_html=True,
                )
        col_i += 1
        if i < len(DAG_TASKS) - 1:
            with cols[col_i]:
                st.markdown(
                    f'<div style="text-align:center; color:{BORDER}; font-size:1.1rem; padding-top:30px;">→</div>',
                    unsafe_allow_html=True,
                )
            col_i += 1

    failed_rows = stages_df[(stages_df["status"] == "failed") & (~stages_df["healed"])]
    if not failed_rows.empty:
        with st.expander(f"Failure details ({len(failed_rows)})"):
            for _, row in failed_rows.iterrows():
                st.markdown(f"**{row['task_id']}** — {row['finished_at']}")
                st.code(row["error_message"] or "(no error message recorded)")

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
    runs_col, fresh_col = st.columns([3, 2])

    with runs_col:
        st.markdown('<div class="muted" style="font-size:0.78rem;">Recent Runs</div>', unsafe_allow_html=True)
        runs_df = load_recent_runs()
        if runs_df.empty:
            st.info("No pipeline runs recorded yet.")
        else:
            rows_html = ""
            for _, run in runs_df.iterrows():
                status_label = _run_status_label(run)
                kind, label = RUN_STATUS_BADGE[status_label]
                records = "—" if pd.isna(run["records"]) else f'{int(run["records"]):,}'
                started = _format_eastern(run["started_at"], "%m/%d %I:%M %p")
                rows_html += (
                    f'<tr><td>{run["run_id"]}</td><td>{badge(label, kind)}</td>'
                    f'<td>{_duration_str(run["started_at"], run["finished_at"])}</td>'
                    f'<td>{records}</td><td>{started}</td></tr>'
                )
            st.markdown(
                '<div style="overflow-x:auto;"><table class="runs-table"><thead><tr>'
                '<th>Run</th><th>Status</th><th>Duration</th><th>Records</th><th>Started (ET)</th>'
                f'</tr></thead><tbody>{rows_html}</tbody></table></div>',
                unsafe_allow_html=True,
            )

    with fresh_col:
        st.markdown('<div class="muted" style="font-size:0.78rem;">Data Freshness</div>', unsafe_allow_html=True)
        if freshness_df.empty:
            st.info("No freshness checks recorded yet.")
        else:
            for _, row in freshness_df.iterrows():
                label = f"{row['schema_name']}.{row['table_name']}"
                source_tag = " (cached)" if row["data_source"] == "cached" else ""
                last_loaded = _format_eastern(row["last_loaded"])
                if row["is_stale"]:
                    st.error(f"**{label}**{source_tag}  \nStale — last loaded {last_loaded}")
                else:
                    st.success(f"**{label}**{source_tag}  \nFresh — last loaded {last_loaded}")


# --------------------------------------------------------------------------
# Render: Agent section
# --------------------------------------------------------------------------

def render_agent_section() -> None:
    st.markdown('<div class="panel-header">Self-Healing Agent</div>', unsafe_allow_html=True)

    summary = load_incident_summary()
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Total Incidents", f"{summary['total']:,}")
    a2.metric("Auto-Resolved", f"{summary['auto_resolved']:,}")
    a3.metric("Escalated", f"{summary['escalated']:,}")
    a4.metric(
        "Fix Success Rate",
        f"{summary['fix_success_rate']:.0f}%" if summary["fix_success_rate"] is not None else "—",
    )

    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="muted" style="font-size:0.78rem;">Agent Workflow (ingest → classify → investigate → '
        'fix → approve/escalate → postmortem)</div>',
        unsafe_allow_html=True,
    )

    widths = []
    for i in range(len(AGENT_NODES)):
        widths.append(3)
        if i < len(AGENT_NODES) - 1:
            widths.append(0.4)
    cols = st.columns(widths)

    col_i = 0
    for i, node in enumerate(AGENT_NODES):
        with cols[col_i]:
            st.markdown(
                f'<div style="text-align:center;">'
                f'<div style="font-size:1.3rem;">{node["icon"]}</div>'
                f'<div style="font-weight:700; font-size:0.74rem; color:{PURPLE_LIGHT};">{node["name"]}</div>'
                f'<div class="node-desc">{node["desc"]}</div></div>',
                unsafe_allow_html=True,
            )
        col_i += 1
        if i < len(AGENT_NODES) - 1:
            with cols[col_i]:
                st.markdown(
                    f'<div style="text-align:center; color:{BORDER}; font-size:1.1rem; padding-top:6px;">→</div>',
                    unsafe_allow_html=True,
                )
            col_i += 1

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
    latest_col, history_col = st.columns([2, 3])

    with latest_col:
        st.markdown('<div class="muted" style="font-size:0.78rem;">Most Recent Incident</div>',
                    unsafe_allow_html=True)
        latest_df = load_latest_incident()
        if latest_df.empty:
            st.info("No incidents handled by the agent yet.")
        else:
            inc = latest_df.iloc[0]
            kind, label = DECISION_BADGE.get(inc["fix_result"], ("retrying", inc["fix_result"] or "Unknown"))
            st.markdown(
                f'<div><b>{inc["task_id"]}</b> &nbsp; {badge(label, kind)}</div>'
                f'<div class="muted" style="font-size:0.78rem; margin-top:4px;">'
                f'{inc["error_type"] or "unknown_error"} · {_format_eastern(inc["created_at"])}</div>',
                unsafe_allow_html=True,
            )
            root_cause = inc["error_message"] or "—"
            if len(root_cause) > 300:
                st.markdown(
                    f'<div class="callout callout-orange"><b>Root Cause</b><br>{root_cause[:300]}…</div>',
                    unsafe_allow_html=True,
                )
                with st.expander("Show more"):
                    st.write(root_cause)
            else:
                st.markdown(
                    f'<div class="callout callout-orange"><b>Root Cause</b><br>{root_cause}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown(
                f'<div class="callout callout-purple"><b>Agent Action</b><br>{inc["fix_attempted"] or "—"}</div>',
                unsafe_allow_html=True,
            )
            if inc["fix_sql"]:
                with st.expander("Proposed SQL"):
                    st.code(inc["fix_sql"], language="sql")

    with history_col:
        st.markdown('<div class="muted" style="font-size:0.78rem;">Incident History</div>', unsafe_allow_html=True)
        history_df = load_incident_history()
        if history_df.empty:
            st.info("No incidents handled by the agent yet.")
        else:
            rows_html = ""
            for _, row in history_df.iterrows():
                kind, label = DECISION_BADGE.get(row["fix_result"], ("retrying", row["fix_result"] or "Unknown"))
                created = _format_eastern(row["created_at"], "%m/%d %I:%M %p")
                fix_text = (row["fix_attempted"] or "—")
                fix_text = fix_text if len(fix_text) <= 60 else fix_text[:57] + "…"
                rows_html += (
                    f'<tr><td>{created}</td><td>{row["task_id"]}</td>'
                    f'<td>{row["error_type"] or "—"}</td><td>{badge(label, kind)}</td>'
                    f'<td>{fix_text}</td></tr>'
                )
            st.markdown(
                '<table class="runs-table"><thead><tr>'
                '<th>Time (ET)</th><th>Task</th><th>Error Type</th><th>Decision</th><th>Action</th>'
                f'</tr></thead><tbody>{rows_html}</tbody></table>',
                unsafe_allow_html=True,
            )


# --------------------------------------------------------------------------
# Render: FDA Analytics section
# --------------------------------------------------------------------------

def render_fda_analytics_section() -> None:
    st.markdown('<div class="panel-header">FDA Analytics — Top 10 Drugs by Adverse Event Count</div>',
                unsafe_allow_html=True)

    drugs_df = load_top_drugs()
    if drugs_df.empty:
        st.info("No drug report data available yet — mart.mart_drug_reactions is empty or hasn't been built.")
        return

    max_reports = int(drugs_df["total_reports"].max())
    rows_html = ""
    for _, row in drugs_df.iterrows():
        pct = row["total_reports"] / max_reports * 100
        rows_html += (
            '<div class="drug-row">'
            f'<div class="drug-name">{row["drug_name"]}</div>'
            f'<div class="drug-bar-track"><div class="drug-bar-fill" style="width:{pct:.1f}%;"></div></div>'
            f'<div class="drug-count">{int(row["total_reports"]):,}</div>'
            f'<div class="muted drug-meta">{int(row["total_serious"]):,} serious · {int(row["total_fatal"]):,} fatal</div>'
            '</div>'
        )
    st.markdown(rows_html, unsafe_allow_html=True)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> None:
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # Tested once per browser session (not on every rerun) — RDS is stopped
    # most of the time to save cost, so the dashboard falls back to mock
    # data automatically instead of erroring, with a badge (below) as the
    # only visible indicator of which mode it's in.
    if "is_live" not in st.session_state:
        st.session_state.is_live = test_rds_connection()

    header_left, header_badge, header_refresh = st.columns([5, 1, 1])
    with header_left:
        st.title("💊🛡️ FDA Pipeline — Operations")
    with header_badge:
        st.write("")
        if is_live():
            st.markdown(
                '<div style="text-align:right;padding-top:20px">'
                '<span style="background:#1a4a2e;color:#00d4aa;'
                'padding:4px 12px;border-radius:12px;'
                'font-size:12px;font-weight:600;">'
                '🟢 Live</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="text-align:right;padding-top:20px">'
                '<span style="background:#2a2a2a;color:#888888;'
                'padding:4px 12px;border-radius:12px;'
                'font-size:12px;font-weight:600;">'
                '⚫ Demo Mode</span></div>',
                unsafe_allow_html=True,
            )
    with header_refresh:
        st.write("")
        if st.button("Refresh", use_container_width=True):
            st.session_state.is_live = test_rds_connection()
            st.cache_data.clear()
            st.rerun()

    pipeline_tab, agent_tab, analytics_tab = st.tabs(["Pipeline", "Agent", "FDA Analytics"])

    with pipeline_tab:
        with st.container(key="panel-pipeline"):
            render_pipeline_section()

    with agent_tab:
        with st.container(key="panel-agent"):
            render_agent_section()

    with analytics_tab:
        with st.container(key="panel-analytics"):
            render_fda_analytics_section()


if __name__ == "__main__":
    main()
