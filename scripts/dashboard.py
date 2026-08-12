"""
Operations dashboard for the FDA adverse events self-healing pipeline:
Pipeline Monitor (run health, freshness, row-count trend) and Agent
Activity (incidents the self-healing agent has handled).

Run locally with:
    streamlit run scripts/dashboard.py

Reads PIPELINE_DB_CONN from the environment (defaults to a localhost
connection string for running outside Docker). Data is cached for
CACHE_TTL_SECONDS and refreshed on demand via the "Refresh" button —
the pipeline runs every 2 hours, so there's nothing to gain from
auto-refreshing a page someone might be actively reading, and it would
reset scroll position and any in-progress filter selections.

Dark theme is set in .streamlit/config.toml, not injected CSS.
"""

import os

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

STATUS_STYLE = {
    "success": "color: #3fb950; background-color: rgba(63, 185, 80, 0.15); font-weight: 600;",
    "failed":  "color: #f85149; background-color: rgba(248, 81, 73, 0.15); font-weight: 600;",
    "running": "color: #d29922; background-color: rgba(210, 153, 34, 0.15); font-weight: 600;",
}

DECISION_STYLE = {
    "Approved": "color: #3fb950; background-color: rgba(63, 185, 80, 0.15); font-weight: 600;",
    "Rejected": "color: #f85149; background-color: rgba(248, 81, 73, 0.15); font-weight: 600;",
    "Pending":  "color: #d29922; background-color: rgba(210, 153, 34, 0.15); font-weight: 600;",
}

st.set_page_config(page_title="FDA Pipeline — Operations", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(DB_CONN)


def _safe_read_sql(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a query, returning an empty DataFrame instead of raising if the
    underlying table doesn't exist yet or the query otherwise fails. Keeps
    the dashboard usable at every stage of the pipeline instead of
    crashing on a fresh/partially-set-up database."""
    try:
        return pd.read_sql(text(sql), get_engine(), params=params or {})
    except Exception:
        return pd.DataFrame()


# --- Pipeline Monitor --------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_pipeline_summary(_cache_key: str) -> dict:
    """total_runs and success_rate are computed at the run level (a run
    counts as successful only if none of its tasks failed), not the raw
    per-task-row level — pipeline_runs has one row per task, and a
    per-row rate would understate a single failing task's impact."""
    df = _safe_read_sql("""
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
    """)
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
def load_pipeline_run_history(_cache_key: str, limit: int = 100) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT task_id, status, rows_processed, error_message, finished_at
        FROM monitoring.pipeline_runs
        ORDER BY finished_at DESC NULLS LAST
        LIMIT :limit
    """, {"limit": limit})


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_freshness_status(_cache_key: str) -> pd.DataFrame:
    """Latest check per table, not full history — this is a current-state
    indicator, not a trend (that's what the row-count chart is for)."""
    return _safe_read_sql("""
        SELECT DISTINCT ON (table_name)
            table_name, schema_name, last_loaded, is_stale,
            COALESCE(data_source, 'live') AS data_source
        FROM monitoring.freshness_checks
        ORDER BY table_name, checked_at DESC
    """)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def check_dag_paused(_cache_key: str) -> bool | None:
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


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_row_count_trend(_cache_key: str) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT table_name, recorded_at, baseline_count
        FROM monitoring.row_count_baselines
        ORDER BY recorded_at
    """)


def style_run_status(row: pd.Series):
    style = STATUS_STYLE.get(row["status"], "")
    return [style if col == "status" else "" for col in row.index]


# --- Agent Activity ------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_incident_summary(_cache_key: str) -> dict:
    """fix_success_rate is out of *decided* incidents (approved IS NOT
    NULL) — incidents still pending/escalated haven't succeeded or
    failed yet, so including them would understate the rate."""
    df = _safe_read_sql("""
        SELECT COUNT(*) AS total_incidents,
               COUNT(*) FILTER (WHERE approved IS NOT NULL) AS decided,
               COUNT(*) FILTER (WHERE approved = TRUE) AS approved_count
        FROM monitoring.incident_reports
    """)
    if df.empty or int(df["total_incidents"].iloc[0]) == 0:
        return {"total_incidents": 0, "fix_success_rate": None}
    total = int(df["total_incidents"].iloc[0])
    decided = int(df["decided"].iloc[0])
    approved_count = int(df["approved_count"].iloc[0])
    return {
        "total_incidents": total,
        "fix_success_rate": (approved_count / decided * 100) if decided else None,
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_incident_history(_cache_key: str, limit: int = 100) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT created_at, task_id, error_type, fix_attempted, fix_result, approved
        FROM monitoring.incident_reports
        ORDER BY created_at DESC
        LIMIT :limit
    """, {"limit": limit})


def _decision_label(approved) -> str:
    if approved is True:
        return "Approved"
    if approved is False:
        return "Rejected"
    return "Pending"


def style_incident_decision(row: pd.Series):
    style = DECISION_STYLE.get(row["decision"], "")
    return [style if col == "decision" else "" for col in row.index]


def main():
    header_left, header_right = st.columns([6, 1])
    with header_left:
        st.title("FDA Pipeline — Operations")
    with header_right:
        st.write("")
        if st.button("Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        st.error(f"Could not connect to the pipeline database: {exc}")
        st.stop()

    cache_key = "v2"

    dag_paused = check_dag_paused(cache_key)
    if dag_paused:
        st.error(
            f"🚨 **Pipeline paused — action required.** `{AIRFLOW_DAG_ID}` was "
            f"paused by the self-healing agent after a P0 incident (data "
            f"corruption) and needs a human to review and resume it in Airflow."
        )

    freshness_df = load_freshness_status(cache_key)
    if not freshness_df.empty and (freshness_df["data_source"] == "cached").any():
        st.warning(
            "⚠️ **Using cached data.** The FDA API was unreachable after all "
            "retries on the most recent extraction — the pipeline fell back "
            "to the most recent file in S3 instead of live data."
        )

    # ===================== Pipeline Monitor =====================
    st.header("Pipeline Monitor")

    summary = load_pipeline_summary(cache_key)
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Runs", f"{summary['total_runs']:,}")
    m2.metric(
        "Success Rate",
        f"{summary['success_rate']:.0f}%" if summary["success_rate"] is not None else "—",
    )
    m3.metric(
        "Last Run",
        summary["last_run_at"].strftime("%Y-%m-%d %H:%M UTC")
        if summary["last_run_at"] is not None else "—",
    )

    st.subheader("Run History")
    runs_df = load_pipeline_run_history(cache_key)
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        display_df = runs_df.drop(columns=["error_message"])
        styled = display_df.style.apply(style_run_status, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True)

        failed_df = runs_df[runs_df["status"] == "failed"]
        if not failed_df.empty:
            with st.expander(f"Failure details ({len(failed_df)})"):
                for _, row in failed_df.iterrows():
                    st.markdown(f"**{row['task_id']}** — {row['finished_at']}")
                    st.code(row["error_message"] or "(no error message recorded)")

    fresh_col, trend_col = st.columns([1, 2])
    with fresh_col:
        st.subheader("Data Freshness")
        if freshness_df.empty:
            st.info("No freshness checks recorded yet.")
        else:
            for _, row in freshness_df.iterrows():
                label = f"{row['schema_name']}.{row['table_name']}"
                loaded = row["last_loaded"]
                source_tag = " (cached)" if row["data_source"] == "cached" else ""
                if row["is_stale"]:
                    st.error(f"**{label}**{source_tag}  \nStale — last loaded {loaded}")
                else:
                    st.success(f"**{label}**{source_tag}  \nFresh — last loaded {loaded}")

    with trend_col:
        st.subheader("Row Count Trend")
        trend_df = load_row_count_trend(cache_key)
        if trend_df.empty:
            st.info("No row count history yet.")
        else:
            pivoted = trend_df.pivot(index="recorded_at", columns="table_name", values="baseline_count")
            st.line_chart(pivoted)

    st.divider()

    # ===================== Agent Activity =====================
    st.header("Agent Activity")

    incident_summary = load_incident_summary(cache_key)
    a1, a2 = st.columns(2)
    a1.metric("Total Incidents Handled", f"{incident_summary['total_incidents']:,}")
    a2.metric(
        "Fix Success Rate",
        f"{incident_summary['fix_success_rate']:.0f}%"
        if incident_summary["fix_success_rate"] is not None else "—",
    )

    st.subheader("Incident History")
    incidents_df = load_incident_history(cache_key)
    if incidents_df.empty:
        st.info("No incidents handled by the agent yet.")
    else:
        incidents_df = incidents_df.copy()
        incidents_df["decision"] = incidents_df["approved"].map(_decision_label)
        display_df = incidents_df.drop(columns=["approved"])
        styled = display_df.style.apply(style_incident_decision, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
