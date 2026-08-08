"""
Tool: thin wrapper around Airflow's stable REST API (v1) for the two
actions the agent takes directly on the pipeline itself — retrying a
failed task and pausing the DAG. Authenticated via HTTP basic auth
(Airflow's default "simple auth" backend); credentials come from
AIRFLOW_USERNAME/AIRFLOW_PASSWORD, never hardcoded.
"""

import os

import requests

AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")

_TIMEOUT_SECONDS = 15


def _auth():
    return (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


def retry_task(dag_id: str, dag_run_id: str, task_id: str) -> dict:
    """Retries a failed task instance. Airflow's REST API models this as
    a "clear" (not a distinct retry verb) — clearing a failed task
    instance puts it back in a schedulable state, and the scheduler
    reschedules it (and, with reset_dag_runs, resumes the rest of the
    run) automatically."""
    resp = requests.post(
        f"{AIRFLOW_API_URL}/dags/{dag_id}/clearTaskInstances",
        auth=_auth(),
        json={
            "dry_run": False,
            "task_ids": [task_id],
            "dag_run_id": dag_run_id,
            "only_failed": True,
            "reset_dag_runs": True,
        },
        timeout=_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def pause_dag(dag_id: str) -> dict:
    resp = requests.patch(
        f"{AIRFLOW_API_URL}/dags/{dag_id}",
        auth=_auth(),
        params={"update_mask": "is_paused"},
        json={"is_paused": True},
        timeout=_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def is_dag_paused(dag_id: str) -> bool:
    resp = requests.get(
        f"{AIRFLOW_API_URL}/dags/{dag_id}", auth=_auth(), timeout=_TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    return bool(resp.json().get("is_paused"))
