"""
FastAPI app for the FDA pipeline self-healing agent (Part 2).

Receives failure alerts (e.g. from an Airflow on_failure_callback) on
POST /alert and runs them through the LangGraph graph defined in
graph.py: ingest -> classify -> investigate -> fix ->
approve_or_escalate -> postmortem.

load_dotenv() must run before the `graph` import below — tools/ and
mcp_servers/ read config with os.environ.get(...) at import time, so
.env values need to already be in the environment by then.
"""

from dotenv import load_dotenv
load_dotenv()

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import escalation_scheduler
from graph import compiled_graph
from mcp_servers.slack_server import handle_slack_action, post_notification
from tools import schema_inspector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("agent.main")


def _startup_schema_check() -> None:
    """Runs once per boot: if raw.fda_adverse_events is left in a
    drifted state from a prior incident (e.g. the agent crashed or was
    redeployed between detecting a schema_drift failure and fixing it),
    fix it now rather than waiting for the next alert to rediscover it."""
    try:
        detail = schema_inspector.apply_safe_rename_fix()
    except Exception:
        logger.exception("Startup schema check failed")
        return

    if detail:
        logger.warning("Startup schema check found and fixed drift: %s", detail)
        try:
            post_notification(f"🔧 Startup check: {detail}")
        except Exception as exc:
            logger.warning("Slack notification failed (non-fatal): %s", exc)
    else:
        logger.info("Startup schema check: no drift found.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _startup_schema_check()
    scheduler = escalation_scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="FDA Pipeline Self-Healing Agent", lifespan=lifespan)


class AlertRequest(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    log_text: Optional[str] = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/alert")
def receive_alert(alert: AlertRequest):
    logger.info("Received alert: dag_id=%s run_id=%s task_id=%s",
                alert.dag_id, alert.run_id, alert.task_id)

    initial_state = {"alert": alert.model_dump()}

    try:
        final_state = compiled_graph.invoke(initial_state)
    except Exception as exc:
        logger.exception("Graph execution failed for alert %s/%s",
                          alert.run_id, alert.task_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "error_type": final_state.get("error_type"),
        "investigation": final_state.get("investigation"),
        "proposed_fix": final_state.get("proposed_fix"),
        "approval_status": final_state.get("approval_status"),
        "postmortem": final_state.get("postmortem"),
    }


@app.post("/slack/actions")
async def slack_actions(request: Request):
    """Slack's interactivity request URL for the Approve/Reject buttons
    posted by mcp_servers.slack_server.request_approval(). Delegates to
    the shared handler in slack_server.py so the verification/parsing
    logic lives in exactly one place."""
    raw_body = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    status_code, body = await handle_slack_action(raw_body, timestamp, signature)
    return JSONResponse(body, status_code=status_code)
