"""
Airflow DAG: extract -> validate -> load -> transform for FDA adverse events.
Pulls real data from the openFDA API (quarterly updates, last updated April 2026).
No agent code in Part 1.
"""

# apache/airflow:2.8.1 ships Python 3.8, which doesn't support PEP 585
# subscripted builtin generics (tuple[X, Y], dict[K, V], etc.) as actual
# runtime expressions — only as strings. This defers all annotation
# evaluation so those forms are safe to use below regardless.
from __future__ import annotations

import datetime
import json
import logging
import os
import re
import subprocess
import time
from datetime import date, timedelta
from io import StringIO

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline"
)

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")

# Self-healing agent now runs on its own EC2 instance (separate from
# Airflow), not as a sibling container reachable via Docker DNS — so this
# is a real network address, not a Docker service name. Uses the agent
# host's private IP (stable across stop/start, unlike its public IP).
AGENT_ALERT_URL = os.environ.get("AGENT_ALERT_URL", "http://172.31.41.43:8000/alert")

S3_BUCKET = "pipeline-fda"

FDA_API_URL = "https://api.fda.gov/drug/event.json"

PAGE_LIMIT = 100
MAX_PAGES  = 10

EXPECTED_COLUMNS = {
    "report_id", "received_date", "serious",
    "drug_name", "reaction", "country"
}

# Seed date range, used only the first time the DAG ever runs (once
# monitoring.pipeline_runs / the fda_extraction_cursor Variable exist,
# the cursor below is the source of truth). openFDA's `skip` parameter
# has a hard ceiling of 25,000 combined with `limit` -- once a date
# range's pagination reaches that ceiling (or genuinely runs out of
# results), extract_fda_data auto-shifts the cursor to the previous
# year and keeps going, rather than treating range exhaustion as an
# API outage.
FDA_DATE_RANGE = "receiptdate:[20250401 TO 20260401]"

# openFDA has no confirmed data before FAERS' effective start; refuses
# to shift past this so a genuine, unrelated API problem can't turn
# into an infinite regression through empty years.
FDA_EARLIEST_YEAR = 2004

FDA_CURSOR_VARIABLE = "fda_extraction_cursor"

# Exponential backoff for FDA API failures: 1min, 2min, 4min between the
# 4 total attempts (1 initial + 3 retries). If all of them fail, extract
# falls back to the most recent cached file in S3.
FDA_API_BACKOFF_SECONDS = [60, 120, 240]


class FdaSkipLimitExceeded(Exception):
    """openFDA hard-caps skip+limit at 25,000 regardless of how many
    matching records actually exist for a query -- once a date range's
    pagination cursor reaches that ceiling, the API returns 400 Bad
    Request instead of results, even if the range genuinely has more
    records past that point. Distinct from a transient outage:
    retrying the same request is pointless, the only way forward is a
    narrower (earlier) date range with a fresh cursor."""


def _get_extraction_cursor(default_skip: int = 0) -> dict:
    """default_skip matters the first time this Variable is ever read:
    if the table already has rows loaded (e.g. this deployed onto an
    existing install that predates the cursor Variable), starting a
    fresh cursor at skip=0 would re-fetch everything already loaded
    from scratch. Harmless correctness-wise (load_to_postgres upserts
    on report_id), but wastes runs re-covering already-loaded ground
    instead of making progress. Callers should pass the table's
    current row count so a first-ever read picks up where the old
    row-count-as-cursor approach would have."""
    raw = Variable.get(FDA_CURSOR_VARIABLE, default_var=None)
    if raw is None:
        return {"date_range": FDA_DATE_RANGE, "skip": default_skip}
    return json.loads(raw)


def _set_extraction_cursor(date_range: str, skip: int) -> None:
    Variable.set(FDA_CURSOR_VARIABLE, json.dumps({"date_range": date_range, "skip": skip}))


def _shift_date_range_back_one_year(date_range: str) -> str:
    """'receiptdate:[20250401 TO 20260401]' -> 'receiptdate:[20240401 TO 20250401]'.
    Only the year component moves; day/month stay fixed, so this never
    has to reason about leap years."""
    match = re.match(r"receiptdate:\[(\d{4})(\d{4}) TO (\d{4})(\d{4})\]", date_range)
    if not match:
        raise ValueError(f"Can't parse FDA date range to shift it: {date_range!r}")
    start_year, start_rest, end_year, end_rest = match.groups()
    new_start_year, new_end_year = int(start_year) - 1, int(end_year) - 1
    if new_start_year < FDA_EARLIEST_YEAR:
        raise ValueError(
            f"Refusing to shift {date_range!r} back to {new_start_year} -- "
            f"below FDA_EARLIEST_YEAR ({FDA_EARLIEST_YEAR}). This means every "
            f"year back to {FDA_EARLIEST_YEAR} has already been paginated, or "
            f"something other than genuine range exhaustion is going on."
        )
    return f"receiptdate:[{new_start_year}{start_rest} TO {new_end_year}{end_rest}]"


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _log_start(dag_id: str, run_id: str, task_id: str) -> int:
    with _engine().begin() as conn:
        result = conn.execute(text("""
            INSERT INTO monitoring.pipeline_runs
                (dag_id, run_id, task_id, status)
            VALUES (:dag_id, :run_id, :task_id, 'running')
            RETURNING id
        """), {"dag_id": dag_id, "run_id": run_id, "task_id": task_id})
        return result.scalar_one()


def _log_finish(run_row_id: int, status: str,
                rows_processed: int = None, error_message: str = None):
    with _engine().begin() as conn:
        conn.execute(text("""
            UPDATE monitoring.pipeline_runs
            SET status         = :status,
                rows_processed = :rows_processed,
                error_message  = :error_message,
                finished_at    = NOW()
            WHERE id = :id
        """), {
            "status":         status,
            "rows_processed": rows_processed,
            "error_message":  error_message,
            "id":             run_row_id,
        })


def _flatten_report(report: dict, load_date: str) -> dict:
    patient   = report.get("patient", {})
    drugs     = patient.get("drug", [{}])
    reactions = patient.get("reaction", [{}])

    drug     = drugs[0]     if drugs     else {}
    reaction = reactions[0] if reactions else {}

    return {
        "report_id":        report.get("safetyreportid"),
        "received_date":    report.get("receiptdate"),
        "serious":          report.get("serious"),
        "serious_death":    report.get("seriousnessdeath"),
        "serious_hosp":     report.get("seriousnesshospitalization"),
        "serious_life":     report.get("seriousnesslifethreatening"),
        "patient_age":      patient.get("patientonsetage"),
        "patient_age_unit": patient.get("patientonsetageunit"),
        "patient_sex":      patient.get("patientsex"),
        "drug_name":        drug.get("medicinalproduct"),
        "drug_indication":  drug.get("drugindication"),
        "reaction":         reaction.get("reactionmeddrapt"),
        "outcome":          reaction.get("reactionoutcome"),
        "country":          report.get("primarysource", {}).get("reportercountry"),
        "load_date":        load_date,
    }


def _s3_object_exists(s3_key: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            return False
        raise


def _list_s3_keys(prefix: str, max_keys: int = 20):
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=max_keys)
    return [
        obj["Key"] for obj in resp.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]


def _read_csv_from_s3(s3_key: str, **read_csv_kwargs) -> pd.DataFrame:
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    except s3.exceptions.NoSuchKey:
        actual_keys = _list_s3_keys("raw/")
        actual_list = (
            "\n".join(f"  - {k}" for k in actual_keys)
            if actual_keys else "  (no files found under raw/)"
        )
        raise FileNotFoundError(
            f"Expected S3 object s3://{S3_BUCKET}/{s3_key} does not exist.\n"
            f"Files currently under s3://{S3_BUCKET}/raw/:\n{actual_list}"
        )
    return pd.read_csv(obj["Body"], **read_csv_kwargs)


def _fetch_fda_pages(date_range: str, skip_start: int, load_date: str) -> list:
    """One full paginated fetch attempt. Raises on any request failure
    (timeout, connection error, non-404/non-400 HTTP error) so the
    caller can retry with backoff — a failure partway through a page
    set leaves an incomplete/inconsistent result, not something worth
    keeping. A 400 is treated separately (FdaSkipLimitExceeded) since
    it means this date range's cursor is exhausted, not that the API
    is having a bad moment — see that exception's docstring."""
    all_rows = []
    skip = skip_start

    for page in range(MAX_PAGES):
        params = {"search": date_range, "limit": PAGE_LIMIT, "skip": skip}
        resp = requests.get(FDA_API_URL, params=params, timeout=30)

        if resp.status_code == 404:
            logger.info("No FDA reports found")
            break

        if resp.status_code == 400:
            raise FdaSkipLimitExceeded(
                f"FDA API returned 400 at skip={skip} for range {date_range!r} "
                f"(openFDA caps skip+limit at 25,000): {resp.text[:300]}"
            )

        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            logger.info("No more results at page %d", page + 1)
            break

        for report in results:
            all_rows.append(_flatten_report(report, load_date))

        skip += PAGE_LIMIT
        logger.info("Page %d: fetched %d total rows", page + 1, len(all_rows))

        total = data.get("meta", {}).get("results", {}).get("total", 0)
        if skip >= total:
            break

    return all_rows


def _fetch_fda_data_with_backoff(date_range: str, skip_start: int, load_date: str) -> list:
    """Retries the full FDA API fetch with exponential backoff
    (FDA_API_BACKOFF_SECONDS) on transient failures. Re-raises the last
    exception if every attempt fails, so the caller can fall back to S3.

    FdaSkipLimitExceeded is deliberately not caught here — it's not
    transient, so burning through 7 minutes of backoff retrying the
    exact same doomed request would just delay the caller shifting to
    a new date range, which is the only thing that actually helps."""
    last_exc = None
    for attempt, delay in enumerate([0] + FDA_API_BACKOFF_SECONDS):
        if delay:
            logger.warning(
                "FDA API attempt %d failed (%s) — retrying in %ds",
                attempt, last_exc, delay,
            )
            time.sleep(delay)
        try:
            return _fetch_fda_pages(date_range, skip_start, load_date)
        except requests.exceptions.RequestException as exc:
            last_exc = exc

    logger.error(
        "FDA API failed after %d attempts: %s",
        len(FDA_API_BACKOFF_SECONDS) + 1, last_exc,
    )
    raise last_exc


def _load_most_recent_s3_file() -> tuple[pd.DataFrame, str]:
    """Falls back to the most recently uploaded raw/ file in S3 when the
    FDA API is unreachable after all retries. Filenames are
    fda_events_YYYY-MM-DD.csv, so a lexicographic max on the key also
    gives the chronologically most recent file."""
    keys = _list_s3_keys("raw/", max_keys=1000)
    if not keys:
        raise RuntimeError(
            f"FDA API unreachable and no cached files found under "
            f"s3://{S3_BUCKET}/raw/ to fall back to."
        )
    most_recent_key = max(keys)
    df = _read_csv_from_s3(most_recent_key)
    logger.warning("Using cached S3 fallback file: s3://%s/%s", S3_BUCKET, most_recent_key)
    return df, most_recent_key


def _record_freshness_check(
    used_cache: bool, table_name: str = "fda_adverse_events", schema_name: str = "raw"
) -> None:
    """Called once per successful load_to_postgres run — records whether
    this load came from a live FDA API pull or the S3 fallback, so the
    dashboard can show a "using cached data" warning when it's stale."""
    with _engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.freshness_checks
                (table_name, schema_name, last_loaded, is_stale, data_source)
            VALUES (:table_name, :schema_name, NOW(), :is_stale, :data_source)
        """), {
            "table_name": table_name,
            "schema_name": schema_name,
            "is_stale": used_cache,
            "data_source": "cached" if used_cache else "live",
        })


def check_failure_injection(**context):
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        failure_type = Variable.get("failure_type", default_var="none")
        logger.info("failure_type Variable = %s", failure_type)

        if failure_type == "none":
            logger.info("running normally")

        elif failure_type == "schema_drift":
            with _engine().begin() as conn:
                conn.execute(text(
                    "ALTER TABLE raw.fda_adverse_events "
                    "RENAME COLUMN drug_name TO medication_name"
                ))
            logger.info(
                "Failure injection [schema_drift]: renamed "
                "raw.fda_adverse_events.drug_name -> medication_name"
            )

        elif failure_type == "volume_anomaly":
            with _engine().begin() as conn:
                result = conn.execute(text("""
                    WITH target AS (
                        SELECT report_id FROM raw.fda_adverse_events
                        ORDER BY random()
                        LIMIT (SELECT CEIL(COUNT(*) * 0.6)::int
                               FROM raw.fda_adverse_events)
                    )
                    DELETE FROM raw.fda_adverse_events
                    WHERE report_id IN (SELECT report_id FROM target)
                """))
            logger.info(
                "Failure injection [volume_anomaly]: deleted %d rows "
                "(~60%%) from raw.fda_adverse_events", result.rowcount
            )

        elif failure_type == "data_quality":
            with _engine().begin() as conn:
                result = conn.execute(text("""
                    WITH target AS (
                        SELECT report_id FROM raw.fda_adverse_events
                        ORDER BY random()
                        LIMIT (SELECT CEIL(COUNT(*) * 0.1)::int
                               FROM raw.fda_adverse_events)
                    )
                    UPDATE raw.fda_adverse_events
                    SET drug_name = NULL
                    WHERE report_id IN (SELECT report_id FROM target)
                """))
            logger.info(
                "Failure injection [data_quality]: set drug_name to NULL "
                "on %d rows (~10%%)", result.rowcount
            )

        elif failure_type == "freshness_failure":
            with _engine().begin() as conn:
                result = conn.execute(text(
                    "UPDATE raw.fda_adverse_events "
                    "SET loaded_at = NOW() - INTERVAL '3 days'"
                ))
            logger.info(
                "Failure injection [freshness_failure]: set loaded_at to "
                "3 days ago on %d rows", result.rowcount
            )

        else:
            logger.warning(
                "Unknown failure_type %r — treating as 'none'", failure_type
            )

        if failure_type != "none":
            Variable.set("failure_type", "none")
            logger.info("Reset failure_type Variable back to 'none'")

        _log_finish(run_row_id, "success")

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


def _fetch_with_auto_shift(date_range: str, skip_start: int, load_date: str) -> tuple:
    """Fetches at (date_range, skip_start). If that range turns out to
    be exhausted -- either openFDA's 400 skip-limit response, or a
    clean empty result set (the range's true total was smaller than
    25,000, so it never hit the 400 but still has nothing left) --
    shifts the cursor to the previous year and retries once at
    skip=0. Persists the new cursor as soon as the shift decision is
    made, before the retry fetch, so a crash mid-retry doesn't lose
    the shift. Returns (rows, date_range, skip_start) reflecting
    whatever range/skip was actually used, so the caller persists the
    right thing regardless of whether a shift happened."""
    try:
        rows = _fetch_fda_data_with_backoff(date_range, skip_start, load_date)
    except FdaSkipLimitExceeded as exc:
        new_range = _shift_date_range_back_one_year(date_range)
        logger.warning("%s — shifting to previous year and retrying: %s", exc, new_range)
        _set_extraction_cursor(new_range, 0)
        return _fetch_fda_data_with_backoff(new_range, 0, load_date), new_range, 0

    if not rows:
        new_range = _shift_date_range_back_one_year(date_range)
        logger.info(
            "Range %s fully paginated at skip=%d with no more results — "
            "shifting to previous year and retrying: %s",
            date_range, skip_start, new_range,
        )
        _set_extraction_cursor(new_range, 0)
        return _fetch_fda_data_with_backoff(new_range, 0, load_date), new_range, 0

    return rows, date_range, skip_start


def extract_fda_data(**context):
    dag_id    = context["dag"].dag_id
    run_id    = context["run_id"]
    task_id   = context["task"].task_id
    load_date = str(date.today())

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        with _engine().begin() as conn:
            total_loaded = conn.execute(text(
                "SELECT COUNT(*) FROM raw.fda_adverse_events"
            )).scalar_one()

        cursor = _get_extraction_cursor(default_skip=total_loaded)
        date_range, skip_start = cursor["date_range"], cursor["skip"]

        logger.info(
            "Fetching FDA adverse events for range %s, starting at skip=%d "
            "(%d total records loaded across all ranges so far)",
            date_range, skip_start, total_loaded,
        )

        used_cache = False
        try:
            all_rows, date_range, skip_start = _fetch_with_auto_shift(
                date_range, skip_start, load_date
            )
        except requests.exceptions.RequestException as exc:
            logger.error(
                "FDA API unreachable after %d attempts (%s) — falling back "
                "to the most recent cached file in S3",
                len(FDA_API_BACKOFF_SECONDS) + 1, exc,
            )
            df, s3_key = _load_most_recent_s3_file()
            used_cache = True

        if not used_cache:
            if not all_rows:
                logger.warning(
                    "No new records returned from FDA API for range %s "
                    "even after shifting back a year — nothing to load",
                    date_range,
                )
                _log_finish(run_row_id, "success", rows_processed=0)
                raise AirflowSkipException(
                    "No new FDA records to extract; skipping downstream tasks"
                )

            df = pd.DataFrame(all_rows)
            s3_key = f"raw/fda_events_{load_date}.csv"

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            s3 = boto3.client("s3")
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())

            if not _s3_object_exists(s3_key):
                raise RuntimeError(
                    f"Uploaded to s3://{S3_BUCKET}/{s3_key} but the object is not "
                    f"visible immediately after put_object — refusing to continue "
                    f"since downstream tasks would fail with NoSuchKey."
                )

            logger.info("Saved %d rows to s3://%s/%s", len(df), S3_BUCKET, s3_key)

            # Advances by however many rows this run actually pulled, within
            # whatever range _fetch_with_auto_shift settled on -- if it
            # shifted, skip_start is already 0 for the new range here.
            _set_extraction_cursor(date_range, skip_start + len(all_rows))

        context["ti"].xcom_push(key="s3_key",     value=s3_key)
        context["ti"].xcom_push(key="row_count",  value=len(df))
        context["ti"].xcom_push(key="load_date",  value=load_date)
        context["ti"].xcom_push(key="used_cache", value=used_cache)

        _log_finish(run_row_id, "success", rows_processed=len(df))

    except AirflowSkipException:
        raise

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


def validate_raw_schema(**context):
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        s3_key = context["ti"].xcom_pull(
            task_ids="extract_fda_data", key="s3_key"
        )
        df = _read_csv_from_s3(s3_key, nrows=5)

        actual_cols  = set(df.columns)
        missing_cols = EXPECTED_COLUMNS - actual_cols

        if missing_cols:
            raise ValueError(
                f"Schema validation FAILED.\n"
                f"Missing columns: {sorted(missing_cols)}\n"
                f"Actual columns:  {sorted(actual_cols)}\n"
                f"This looks like the FDA API changed its response structure."
            )

        with _engine().begin() as conn:
            live_columns = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'fda_adverse_events'
            """)).fetchall()

            conn.execute(text(
                "DELETE FROM monitoring.schema_snapshots "
                "WHERE table_name = 'fda_adverse_events'"
            ))
            for column_name, data_type, is_nullable in live_columns:
                conn.execute(text("""
                    INSERT INTO monitoring.schema_snapshots
                        (table_name, schema_name, column_name,
                         data_type, is_nullable)
                    VALUES ('fda_adverse_events', 'raw',
                            :col, :data_type, :is_nullable)
                """), {
                    "col": column_name,
                    "data_type": data_type,
                    "is_nullable": is_nullable,
                })

        logger.info(
            "Schema validation passed. CSV columns: %s. Live table schema snapshotted: %s",
            sorted(actual_cols), sorted(c[0] for c in live_columns)
        )
        _log_finish(run_row_id, "success")

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


def load_to_postgres(**context):
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        s3_key = context["ti"].xcom_pull(
            task_ids="extract_fda_data", key="s3_key"
        )
        used_cache = bool(context["ti"].xcom_pull(
            task_ids="extract_fda_data", key="used_cache"
        ))

        df = _read_csv_from_s3(s3_key)
        df.columns = [c.lower() for c in df.columns]

        engine = _engine()

        upsert_sql = text("""
            INSERT INTO raw.fda_adverse_events (
                report_id, received_date, serious, serious_death, serious_hosp,
                serious_life, patient_age, patient_age_unit, patient_sex,
                drug_name, drug_indication, reaction, outcome, country,
                load_date, loaded_at
            ) VALUES (
                :report_id, :received_date, :serious, :serious_death, :serious_hosp,
                :serious_life, :patient_age, :patient_age_unit, :patient_sex,
                :drug_name, :drug_indication, :reaction, :outcome, :country,
                :load_date, NOW()
            )
            ON CONFLICT (report_id) DO UPDATE SET
                received_date    = EXCLUDED.received_date,
                serious          = EXCLUDED.serious,
                serious_death    = EXCLUDED.serious_death,
                serious_hosp     = EXCLUDED.serious_hosp,
                serious_life     = EXCLUDED.serious_life,
                patient_age      = EXCLUDED.patient_age,
                patient_age_unit = EXCLUDED.patient_age_unit,
                patient_sex      = EXCLUDED.patient_sex,
                drug_name        = EXCLUDED.drug_name,
                drug_indication  = EXCLUDED.drug_indication,
                reaction         = EXCLUDED.reaction,
                outcome          = EXCLUDED.outcome,
                country          = EXCLUDED.country,
                load_date        = EXCLUDED.load_date,
                loaded_at        = NOW()
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, df.to_dict(orient="records"))

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO monitoring.row_count_baselines
                    (table_name, schema_name, baseline_count)
                VALUES ('fda_adverse_events', 'raw', :count)
            """), {"count": len(df)})

        _record_freshness_check(used_cache)

        logger.info("Loaded %d rows into raw.fda_adverse_events", len(df))
        _log_finish(run_row_id, "success", rows_processed=len(df))

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


def trigger_dbt_run(**context):
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    task_id = context["task"].task_id

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        packages_file = os.path.join(DBT_PROJECT_DIR, "packages.yml")
        if os.path.exists(packages_file):
            deps_result = subprocess.run(
                ["dbt", "--no-use-colors", "deps",
                 "--project-dir", DBT_PROJECT_DIR,
                 "--profiles-dir", DBT_PROJECT_DIR],
                capture_output=True,
                text=True,
                check=False,
            )
            logger.info("dbt deps stdout:\n%s", deps_result.stdout)
            if deps_result.stderr:
                logger.warning("dbt deps stderr:\n%s", deps_result.stderr)

            if deps_result.returncode != 0:
                raise RuntimeError(
                    f"dbt deps failed (exit {deps_result.returncode}):\n"
                    f"{deps_result.stderr[-2000:]}"
                )

        result = subprocess.run(
            ["dbt", "--no-use-colors", "run",
             "--project-dir", DBT_PROJECT_DIR,
             "--profiles-dir", DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
            check=False,
        )
        logger.info("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("dbt stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt run failed (exit {result.returncode}):\n"
                f"{result.stderr[-2000:]}"
            )

        # Runs the not_null/unique/etc. tests already defined in
        # dbt_project/models/staging/*.yml — previously dead code, since
        # nothing ever invoked `dbt test`, so failures like the
        # data_quality injection scenario (NULL drug_name) went silently
        # undetected. accepted_values_stg_fda_adverse_events_patient_sex
        # is excluded: patient_sex is stored as '1.0'/'2.0'/'NaN' (a
        # pre-existing, unrelated formatting bug), so that test currently
        # fails on virtually every row and would make this gate
        # permanently red rather than reflecting real data quality.
        test_result = subprocess.run(
            ["dbt", "--no-use-colors", "test",
             "--project-dir", DBT_PROJECT_DIR,
             "--profiles-dir", DBT_PROJECT_DIR,
             "--exclude", "accepted_values_stg_fda_adverse_events_patient_sex__1__2"],
            capture_output=True,
            text=True,
            check=False,
        )
        logger.info("dbt test stdout:\n%s", test_result.stdout)
        if test_result.stderr:
            logger.warning("dbt test stderr:\n%s", test_result.stderr)

        if test_result.returncode != 0:
            raise RuntimeError(
                f"dbt test failed (exit {test_result.returncode}):\n"
                f"{test_result.stdout[-2000:]}"
            )

        _log_finish(run_row_id, "success")
        logger.info("dbt run and tests completed successfully")

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


def alert_agent(context):
    """on_failure_callback: notifies the self-healing agent (agent/main.py,
    POST /alert) whenever a task in this DAG fails. Best-effort only — a
    down or slow agent must never block or fail the pipeline itself, so
    failures here are logged and swallowed rather than raised."""
    exception = context.get("exception")
    try:
        requests.post(
            AGENT_ALERT_URL,
            json={
                "dag_id":     context["dag"].dag_id,
                "run_id":     context["run_id"],
                "task_id":    context["task_instance"].task_id,
                "log_text":   str(exception) if exception else None,
                "error_type": type(exception).__name__ if exception else None,
            },
            timeout=5,
        )
    except Exception as exc:
        logger.warning("alert_agent: failed to notify self-healing agent: %s", exc)


default_args = {
    "owner":               "Geetha",
    "retries":             1,
    "retry_delay":         datetime.timedelta(minutes=2),
    "on_failure_callback": alert_agent,
}

with DAG(
    dag_id="fda_pipeline_dag",
    description="FDA adverse event reports — extract, validate, load, transform.",
    default_args=default_args,
    schedule="0 */2 * * *",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "load_date": Param(
            str(date.today()),
            type="string",
            description="Run date label (YYYY-MM-DD).",
        )
    },
    tags=["fda", "healthcare", "pipeline"],
) as dag:

    check_failure = PythonOperator(
        task_id="check_failure_injection",
        python_callable=check_failure_injection,
    )

    extract = PythonOperator(
        task_id="extract_fda_data",
        python_callable=extract_fda_data,
    )

    validate = PythonOperator(
        task_id="validate_raw_schema",
        python_callable=validate_raw_schema,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    dbt_run = PythonOperator(
        task_id="trigger_dbt_run",
        python_callable=trigger_dbt_run,
    )

    check_failure >> extract >> validate >> load >> dbt_run
