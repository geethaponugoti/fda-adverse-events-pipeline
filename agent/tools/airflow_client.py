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
    a "clear" (not a distinct retry verb) — clearing a task instance
    puts it back in a schedulable state, and the scheduler reschedules
    it (and, with reset_dag_runs, resumes the rest of the run)
    automatically.

    include_downstream matters here: this DAG is a linear chain, so a
    failed task cascades "upstream_failed" (a *different* terminal
    state from "failed") to everything after it. Without
    include_downstream, clearing just the one task_id leaves those
    downstream tasks permanently stuck even after the retry succeeds —
    Airflow doesn't retroactively re-evaluate an already-terminal
    downstream task just because its upstream succeeded later.
    only_failed is deliberately omitted (not just left False) —
    "upstream_failed" tasks aren't "failed", so that filter would
    silently exclude exactly the downstream tasks include_downstream
    is meant to catch."""
    resp = requests.post(
        f"{AIRFLOW_API_URL}/dags/{dag_id}/clearTaskInstances",
        auth=_auth(),
        json={
            "dry_run": False,
            "task_ids": [task_id],
            "dag_run_id": dag_run_id,
            "include_downstream": True,
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
