"""
Tool: parses Airflow error logs, extracting a clean error_message and a
heuristic error_type hint. Used by the `ingest` node in graph.py.

The alert posted to /alert may already include the raw exception text
(`log_text`) — e.g. an Airflow on_failure_callback passing
str(context["exception"]). If it doesn't, this falls back to reading
the error_message monitoring.pipeline_runs already has for that
dag_id/run_id/task_id, since every task in fda_pipeline_dag writes one
there via _log_finish() before re-raising.
"""

import os
import re

from sqlalchemy import create_engine, text

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)

# Heuristic keyword -> error_type hints, checked in order. These are
# matched against real exception strings raised by fda_pipeline_dag.py:
# schema validation failures, missing S3 objects, OOM worker kills, and
# NULL/row-count anomalies from the failure-injection scenarios.
_ERROR_TYPE_PATTERNS = [
    ("schema_drift", re.compile(
        r"missing columns|does not exist.*column|column.*does not exist|"
        r"schema validation failed|rename column",
        re.IGNORECASE,
    )),
    ("upstream_failure", re.compile(
        r"nosuchkey|s3 object.*does not exist|out of memory|sigkill|"
        r"connection refused|timeout|i/o error|input/output error",
        re.IGNORECASE,
    )),
    ("volume_anomaly", re.compile(
        r"row count|deviat|zero rows extracted|deleted \d+ rows",
        re.IGNORECASE,
    )),
    ("data_quality", re.compile(
        r"null|missing value|drug_name.*null|out-of-range|bad_serious",
        re.IGNORECASE,
    )),
]


def _fetch_error_message_from_db(dag_id: str, run_id: str, task_id: str) -> str:
    engine = create_engine(PIPELINE_DB_CONN)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT error_message FROM monitoring.pipeline_runs
            WHERE dag_id = :dag_id AND run_id = :run_id AND task_id = :task_id
              AND status = 'failed'
            ORDER BY finished_at DESC
            LIMIT 1
        """), {"dag_id": dag_id, "run_id": run_id, "task_id": task_id}).fetchone()

    if row is None or row[0] is None:
        return (
            f"No error_message found in monitoring.pipeline_runs for "
            f"dag_id={dag_id} run_id={run_id} task_id={task_id}"
        )
    return row[0]


def _extract_error_message(log_text: str) -> str:
    """If log_text is a full Python traceback, return just the final
    exception line; otherwise return it as-is (already concise, as is
    the case for error_message values stored by the DAG itself)."""
    lines = [ln for ln in log_text.strip().splitlines() if ln.strip()]
    if not lines:
        return log_text.strip()

    if any("Traceback (most recent call last):" in ln for ln in lines):
        return lines[-1].strip()

    return log_text.strip()


def _guess_error_type(error_message: str):
    for error_type, pattern in _ERROR_TYPE_PATTERNS:
        if pattern.search(error_message):
            return error_type
    return None


def parse_alert(alert: dict) -> dict:
    """Turns a raw AlertPayload into a ParsedLog dict: task_id,
    error_message, error_type_hint, raw_log."""
    log_text = alert.get("log_text")
    if not log_text:
        log_text = _fetch_error_message_from_db(
            alert["dag_id"], alert["run_id"], alert["task_id"]
        )

    error_message = _extract_error_message(log_text)
    error_type_hint = _guess_error_type(error_message)

    return {
        "task_id": alert["task_id"],
        "error_message": error_message,
        "error_type_hint": error_type_hint,
        "raw_log": log_text,
    }
