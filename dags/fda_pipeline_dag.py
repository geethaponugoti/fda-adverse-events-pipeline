"""
Airflow DAG: extract -> validate -> load -> transform for FDA adverse events.
Pulls real data from the openFDA API (quarterly updates, last updated April 2026).
No agent code in Part 1.
"""

import datetime
import logging
import os
import subprocess
from datetime import date, timedelta
from io import StringIO

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline"
)

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")

S3_BUCKET = "pipeline-fda"

FDA_API_URL = "https://api.fda.gov/drug/event.json"

PAGE_LIMIT = 100
MAX_PAGES  = 10

EXPECTED_COLUMNS = {
    "report_id", "received_date", "serious",
    "drug_name", "reaction", "country"
}

# Date range that has confirmed data in the API
FDA_DATE_RANGE = "receiptdate:[20250401 TO 20260401]"


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


def extract_fda_data(**context):
    dag_id    = context["dag"].dag_id
    run_id    = context["run_id"]
    task_id   = context["task"].task_id
    load_date = str(date.today())

    run_row_id = _log_start(dag_id, run_id, task_id)
    try:
        s3_key = f"raw/fda_events_{load_date}.csv"

        with _engine().begin() as conn:
            existing_count = conn.execute(text(
                "SELECT COUNT(*) FROM raw.fda_adverse_events WHERE load_date = :d"
            ), {"d": load_date}).scalar_one()

        if existing_count > 0:
            if _s3_object_exists(s3_key):
                logger.info("data already loaded for today, skipping")

                context["ti"].xcom_push(key="s3_key",    value=s3_key)
                context["ti"].xcom_push(key="row_count", value=existing_count)
                context["ti"].xcom_push(key="load_date", value=load_date)

                _log_finish(run_row_id, "success", rows_processed=existing_count)
                return
            else:
                logger.warning(
                    "raw.fda_adverse_events has %d rows for load_date=%s but "
                    "s3://%s/%s is missing — re-fetching instead of skipping",
                    existing_count, load_date, S3_BUCKET, s3_key
                )

        all_rows = []
        skip     = 0

        logger.info("Fetching FDA adverse events for range 2025-01-01 to 2025-12-31")

        for page in range(MAX_PAGES):
            params = {
                "search": FDA_DATE_RANGE,
                "limit":  PAGE_LIMIT,
                "skip":   skip,
            }
            resp = requests.get(FDA_API_URL, params=params, timeout=30)

            if resp.status_code == 404:
                logger.info("No FDA reports found")
                break

            resp.raise_for_status()
            data    = resp.json()
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

        if not all_rows:
            raise ValueError("Zero rows extracted from FDA API — check date range")

        df = pd.DataFrame(all_rows)

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

        context["ti"].xcom_push(key="s3_key",    value=s3_key)
        context["ti"].xcom_push(key="row_count", value=len(df))
        context["ti"].xcom_push(key="load_date", value=load_date)

        _log_finish(run_row_id, "success", rows_processed=len(df))

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
            conn.execute(text(
                "DELETE FROM monitoring.schema_snapshots "
                "WHERE table_name = 'fda_adverse_events'"
            ))
            for col in actual_cols:
                conn.execute(text("""
                    INSERT INTO monitoring.schema_snapshots
                        (table_name, schema_name, column_name,
                         data_type, is_nullable)
                    VALUES ('fda_adverse_events', 'raw',
                            :col, 'varchar', 'YES')
                """), {"col": col})

        logger.info("Schema validation passed. Columns: %s", sorted(actual_cols))
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

            conn.execute(text("""
                INSERT INTO monitoring.freshness_checks
                    (table_name, schema_name, last_loaded, is_stale)
                VALUES ('fda_adverse_events', 'raw', NOW(), FALSE)
            """))

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
                ["dbt", "deps",
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
            ["dbt", "run",
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

        _log_finish(run_row_id, "success")
        logger.info("dbt run completed successfully")

    except Exception as exc:
        _log_finish(run_row_id, "failed", error_message=str(exc))
        raise


default_args = {
    "owner":               "Geetha",
    "retries":             1,
    "retry_delay":         datetime.timedelta(minutes=2),
    "on_failure_callback": None,
}

with DAG(
    dag_id="fda_pipeline_dag",
    description="FDA adverse event reports — extract, validate, load, transform.",
    default_args=default_args,
    schedule="*/15 * * * *",
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
