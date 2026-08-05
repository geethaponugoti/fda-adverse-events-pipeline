"""
Streamlit dashboard for the FDA adverse events pipeline: KPIs, top drugs,
common reactions, serious events, a filterable raw data preview, and
pipeline run history.

Run locally with:
    streamlit run scripts/dashboard.py

Reads PIPELINE_DB_CONN from the environment (defaults to a localhost
connection string for running outside Docker). Auto-refreshes every 30
seconds via a meta-refresh tag, so no background thread or extra
dependency is needed (the tradeoff: filter widgets reset on each refresh).
"""

import datetime as dt
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_CONN = os.environ.get("PIPELINE_DB_CONN", "postgresql+psycopg2://pipeline:pipeline@localhost/fda_pipeline")

REFRESH_SECONDS = 30

STATUS_COLORS = {
    "success": "#1a7f37",
    "failed": "#c62828",
    "running": "#b58105",
}
STATUS_BG = {
    "success": "#e6f4ea",
    "failed": "#fdecea",
    "running": "#fff8e1",
}

st.set_page_config(page_title="FDA Adverse Events Pipeline Monitor", layout="wide")
st.markdown(f'<meta http-equiv="refresh" content="{REFRESH_SECONDS}">', unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    return create_engine(DB_CONN)


def _safe_read_sql(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a query, returning an empty DataFrame instead of raising if the
    underlying table/view doesn't exist yet (e.g. dbt hasn't run) or the
    query otherwise fails. Keeps the dashboard usable at every stage of
    the pipeline instead of crashing."""
    try:
        return pd.read_sql(text(sql), get_engine(), params=params or {})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SECONDS)
def load_kpis(_cache_key: str) -> dict:
    reports_today = _safe_read_sql(
        "SELECT COUNT(*) AS n FROM raw.fda_adverse_events WHERE load_date = CURRENT_DATE"
    )
    serious = _safe_read_sql(
        "SELECT COUNT(*) AS n FROM staging.stg_fda_adverse_events WHERE is_serious"
    )
    fatal = _safe_read_sql(
        "SELECT COUNT(*) AS n FROM staging.stg_fda_adverse_events WHERE is_fatal"
    )
    unique_drugs = _safe_read_sql(
        "SELECT COUNT(DISTINCT drug_name) AS n FROM staging.stg_fda_adverse_events"
    )
    return {
        "reports_today": int(reports_today["n"].iloc[0]) if not reports_today.empty else 0,
        "serious": int(serious["n"].iloc[0]) if not serious.empty else 0,
        "fatal": int(fatal["n"].iloc[0]) if not fatal.empty else 0,
        "unique_drugs": int(unique_drugs["n"].iloc[0]) if not unique_drugs.empty else 0,
    }


@st.cache_data(ttl=REFRESH_SECONDS)
def load_top_drugs(_cache_key: str, limit: int = 10) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT drug_name, SUM(report_count) AS total_reports
        FROM mart.mart_drug_reactions
        GROUP BY drug_name
        ORDER BY total_reports DESC
        LIMIT :limit
    """, {"limit": limit})


@st.cache_data(ttl=REFRESH_SECONDS)
def load_common_reactions(_cache_key: str, limit: int = 20) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT
            reaction,
            SUM(report_count)  AS report_count,
            SUM(serious_count) AS serious_count,
            SUM(fatal_count)   AS fatal_count
        FROM mart.mart_drug_reactions
        GROUP BY reaction
        ORDER BY report_count DESC
        LIMIT :limit
    """, {"limit": limit})


@st.cache_data(ttl=REFRESH_SECONDS)
def load_serious_events(_cache_key: str) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT drug_name, outcome, report_count, fatal_count,
               avg_patient_age, most_recent_date, severity_rank
        FROM mart.mart_serious_events
        ORDER BY drug_name, severity_rank
        LIMIT 200
    """)


@st.cache_data(ttl=REFRESH_SECONDS)
def load_date_bounds(_cache_key: str) -> tuple[dt.date, dt.date]:
    """Min/max received_date across the raw table, converting the VARCHAR
    YYYYMMDD column to a real date. Falls back to today/today if there's
    no data yet so the date_input widget always gets valid bounds."""
    bounds = _safe_read_sql("""
        SELECT
            MIN(to_date(NULLIF(received_date, ''), 'YYYYMMDD')) AS min_date,
            MAX(to_date(NULLIF(received_date, ''), 'YYYYMMDD')) AS max_date
        FROM raw.fda_adverse_events
    """)
    if bounds.empty or pd.isna(bounds["min_date"].iloc[0]) or pd.isna(bounds["max_date"].iloc[0]):
        today = dt.date.today()
        return today, today
    return bounds["min_date"].iloc[0], bounds["max_date"].iloc[0]


@st.cache_data(ttl=REFRESH_SECONDS)
def load_raw_preview(_cache_key: str, drug_filter: str, date_from, date_to, limit: int) -> pd.DataFrame:
    sql = """
        SELECT report_id, received_date, serious, serious_death, serious_hosp,
               serious_life, patient_age, patient_age_unit, patient_sex,
               drug_name, drug_indication, reaction, outcome, country,
               loaded_at, load_date
        FROM raw.fda_adverse_events
        WHERE 1 = 1
    """
    params: dict = {}
    if drug_filter:
        sql += " AND drug_name ILIKE :drug_filter"
        params["drug_filter"] = f"%{drug_filter}%"
    if date_from:
        sql += " AND to_date(NULLIF(received_date, ''), 'YYYYMMDD') >= :date_from"
        params["date_from"] = date_from
    if date_to:
        sql += " AND to_date(NULLIF(received_date, ''), 'YYYYMMDD') <= :date_to"
        params["date_to"] = date_to
    sql += " ORDER BY loaded_at DESC NULLS LAST LIMIT :limit"
    params["limit"] = limit
    return _safe_read_sql(sql, params)


@st.cache_data(ttl=REFRESH_SECONDS)
def load_pipeline_runs(_cache_key: str) -> pd.DataFrame:
    return _safe_read_sql("""
        SELECT id, dag_id, run_id, task_id, status, rows_processed,
               error_message, started_at, finished_at
        FROM monitoring.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 200
    """)


def style_status(row: pd.Series):
    color = STATUS_COLORS.get(row["status"], "#333333")
    bg = STATUS_BG.get(row["status"], "#ffffff")
    return [f"color: {color}; background-color: {bg}; font-weight: 600"
            if col == "status" else "" for col in row.index]


def main():
    st.title("FDA Adverse Events Pipeline Monitor")

    top_left, top_right = st.columns([6, 1])
    with top_right:
        if st.button("Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        st.error(f"Could not connect to the pipeline database: {exc}")
        st.stop()

    cache_key = "v1"

    # --- KPI cards -----------------------------------------------------
    kpis = load_kpis(cache_key)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Reports Loaded Today", f"{kpis['reports_today']:,}")
    k2.metric("Serious Reports", f"{kpis['serious']:,}")
    k3.metric("Fatal Reports", f"{kpis['fatal']:,}")
    k4.metric("Unique Drugs", f"{kpis['unique_drugs']:,}")

    st.divider()

    # --- Top 10 drugs bar chart ----------------------------------------
    st.subheader("Top 10 Drugs by Adverse Report Count")
    top_drugs_df = load_top_drugs(cache_key, 10)
    if top_drugs_df.empty:
        st.info("No data yet. Load data and run dbt to populate mart.mart_drug_reactions.")
    else:
        st.bar_chart(top_drugs_df.set_index("drug_name")["total_reports"])

    st.divider()

    # --- Most common reactions ------------------------------------------
    st.subheader("Most Common Reactions")
    reactions_df = load_common_reactions(cache_key, 20)
    if reactions_df.empty:
        st.info("No data yet.")
    else:
        st.dataframe(reactions_df, use_container_width=True, hide_index=True)

    st.divider()

    # --- Serious events ---------------------------------------------------
    st.subheader("Serious Events by Drug / Outcome")
    serious_df = load_serious_events(cache_key)
    if serious_df.empty:
        st.info("No data yet, or no drug/outcome pair currently has more than 5 serious reports.")
    else:
        st.dataframe(serious_df, use_container_width=True, hide_index=True)

    st.divider()

    # --- Raw data preview with filters -------------------------------------
    st.subheader("Raw Data Preview")
    f1, f2, f3 = st.columns([2, 2, 1])
    with f1:
        drug_filter = st.text_input("Filter by drug name", value="")
    with f2:
        min_date, max_date = load_date_bounds(cache_key)
        date_range = st.date_input(
            "Received date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    with f3:
        preview_limit = st.number_input("Rows", min_value=10, max_value=1000, value=100, step=10)

    if isinstance(date_range, tuple) and len(date_range) == 2:
        date_from, date_to = date_range
    else:
        date_from, date_to = min_date, max_date

    raw_df = load_raw_preview(cache_key, drug_filter.strip(), date_from, date_to, int(preview_limit))
    if raw_df.empty:
        st.info("No data yet, or no rows match your filters.")
    else:
        st.dataframe(raw_df, use_container_width=True, hide_index=True)

    st.divider()

    # --- Pipeline run history ---------------------------------------------
    st.subheader("Pipeline Run History")
    runs_df = load_pipeline_runs(cache_key)
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        display_df = runs_df.drop(columns=["error_message"])
        styled = display_df.style.apply(style_status, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True)

        failed_df = runs_df[runs_df["status"] == "failed"]
        if not failed_df.empty:
            st.subheader("Failure Details")
            for _, row in failed_df.iterrows():
                label = f"Run {row['run_id']} - task `{row['task_id']}` - {row['started_at']}"
                with st.expander(label):
                    st.write(f"**DAG:** {row['dag_id']}")
                    st.write(f"**Started at:** {row['started_at']}")
                    st.write(f"**Finished at:** {row['finished_at']}")
                    st.write(f"**Rows processed:** {row['rows_processed']}")
                    st.code(row["error_message"] or "(no error message recorded)")


if __name__ == "__main__":
    main()
