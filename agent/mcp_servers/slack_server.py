"""
FastMCP server exposing Slack notification tools to the agent, plus a
plain HTTP webhook (via FastMCP's custom_route) that handles Slack's
interactive Approve/Reject button clicks.

Two ways this file is used:
  1. graph.py imports and calls request_approval() / get_approval_status()
     / post_notification() directly as plain Python functions — no MCP
     transport involved, just a normal function call.
  2. Run standalone (`python slack_server.py`) it's also a real MCP
     server exposing the same three functions as MCP tools, and serves
     the /slack/actions webhook Slack POSTs button clicks to.

Approval state lives in monitoring.agent_approvals (created here if it
doesn't exist) rather than blocking in-process, since Slack approval is
inherently asynchronous — the graph posts the request and moves on;
whoever clicks the button in Slack resolves it later via the webhook.
"""

import hashlib
import hmac
import json
import os
import time
from urllib.parse import parse_qs

import httpx
from fastmcp import FastMCP
from sqlalchemy import create_engine, text
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#pipeline-alerts")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")

_SLACK_TIMESTAMP_TOLERANCE_SECONDS = 60 * 5

mcp = FastMCP("fda-pipeline-slack")


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _ensure_approvals_table() -> None:
    with _engine().begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS monitoring.agent_approvals (
                incident_id  VARCHAR(64) PRIMARY KEY,
                dag_id       VARCHAR(200),
                task_id      VARCHAR(200),
                summary      TEXT,
                risk_level   VARCHAR(20),
                status       VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at   TIMESTAMP DEFAULT NOW(),
                resolved_at  TIMESTAMP,
                resolved_by  VARCHAR(200)
            )
        """))


def _insert_pending(
    incident_id: str, summary: str, risk_level: str, dag_id: str, task_id: str
) -> None:
    with _engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.agent_approvals
                (incident_id, dag_id, task_id, summary, risk_level, status)
            VALUES (:incident_id, :dag_id, :task_id, :summary, :risk_level, 'pending')
            ON CONFLICT (incident_id) DO UPDATE SET
                summary    = EXCLUDED.summary,
                risk_level = EXCLUDED.risk_level
        """), {
            "incident_id": incident_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "summary": summary,
            "risk_level": risk_level,
        })


def _update_approval_status(incident_id: str, status: str, resolved_by: str) -> None:
    with _engine().begin() as conn:
        conn.execute(text("""
            UPDATE monitoring.agent_approvals
            SET status = :status, resolved_at = NOW(), resolved_by = :resolved_by
            WHERE incident_id = :incident_id
        """), {"status": status, "resolved_by": resolved_by, "incident_id": incident_id})


def _post_to_slack(text_: str, blocks: list) -> dict:
    if not SLACK_BOT_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN is not set — cannot post to Slack.")

    resp = httpx.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": text_, "blocks": blocks},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API error: {data.get('error')}")
    return data


def request_approval(
    incident_id: str,
    summary: str,
    risk_level: str,
    dag_id: str = "",
    task_id: str = "",
) -> dict:
    """Posts an incident to Slack with Approve/Reject buttons and records
    it as pending in monitoring.agent_approvals. Called for medium-risk
    fixes (need a human nod) and high-risk fixes (need a human, full
    stop) alike — the only difference is the urgency framing."""
    _ensure_approvals_table()
    _insert_pending(incident_id, summary, risk_level, dag_id, task_id)

    urgency = "🚨 HIGH RISK — do not auto-apply" if risk_level == "high" else "⚠️ Review needed"
    message_text = f"{urgency}: {summary}"
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{urgency}*\n{summary}\n"
                    f"_dag: `{dag_id}` task: `{task_id}` incident: `{incident_id}`_"
                ),
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "style": "primary",
                    "action_id": "approve",
                    "value": incident_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject"},
                    "style": "danger",
                    "action_id": "reject",
                    "value": incident_id,
                },
            ],
        },
    ]

    return _post_to_slack(message_text, blocks)


def get_approval_status(incident_id: str) -> dict:
    """Looks up the current status of a previously-requested approval:
    'pending', 'approved', or 'rejected'."""
    _ensure_approvals_table()
    with _engine().connect() as conn:
        row = conn.execute(text("""
            SELECT status, resolved_at, resolved_by
            FROM monitoring.agent_approvals
            WHERE incident_id = :incident_id
        """), {"incident_id": incident_id}).fetchone()

    if row is None:
        return {"incident_id": incident_id, "status": "unknown"}

    return {
        "incident_id": incident_id,
        "status": row[0],
        "resolved_at": row[1].isoformat() if row[1] else None,
        "resolved_by": row[2],
    }


def post_notification(text_: str) -> dict:
    """Posts a plain informational message to Slack — used for
    low-risk incidents that were auto-fixed, so a human still sees it
    happened without needing to act on it."""
    return _post_to_slack(text_, blocks=[
        {"type": "section", "text": {"type": "mrkdwn", "text": text_}}
    ])


mcp.tool(request_approval)
mcp.tool(get_approval_status)
mcp.tool(post_notification)


def _verify_slack_signature(raw_body: bytes, timestamp: str, signature: str) -> bool:
    if not SLACK_SIGNING_SECRET:
        return False
    if not timestamp or not signature:
        return False
    try:
        if abs(time.time() - int(timestamp)) > _SLACK_TIMESTAMP_TOLERANCE_SECONDS:
            return False
    except ValueError:
        return False

    sig_basestring = f"v0:{timestamp}:{raw_body.decode('utf-8')}"
    computed_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode("utf-8"),
        sig_basestring.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(computed_signature, signature)


async def handle_slack_action(raw_body: bytes, timestamp: str, signature: str) -> tuple:
    """Core handler for Slack's interactivity request (Approve/Reject
    button clicks): verifies the request came from Slack, records the
    decision, and acks Slack's response_url. Returns (status_code, body)
    so callers can wrap it in whatever response type their framework
    uses — this module's own FastMCP route below, and agent/main.py's
    FastAPI route, both call this instead of duplicating the logic."""
    if not _verify_slack_signature(raw_body, timestamp, signature):
        return 401, {"error": "invalid Slack signature"}

    form = parse_qs(raw_body.decode("utf-8"))
    payload_raw = form.get("payload", [None])[0]
    if not payload_raw:
        return 400, {"error": "missing payload"}

    payload = json.loads(payload_raw)
    actions = payload.get("actions", [])
    if not actions:
        return 400, {"error": "no action in payload"}

    action = actions[0]
    action_id = action.get("action_id")
    incident_id = action.get("value")
    user = payload.get("user", {}).get("username", "unknown")

    new_status = "approved" if action_id == "approve" else "rejected"
    _update_approval_status(incident_id, new_status, user)

    response_url = payload.get("response_url")
    if response_url:
        async with httpx.AsyncClient() as client:
            await client.post(response_url, json={
                "text": f"Incident `{incident_id}` marked *{new_status}* by @{user}.",
                "replace_original": True,
            }, timeout=10)

    return 200, {"ok": True}


@mcp.custom_route("/slack/actions", methods=["POST"])
async def slack_actions(request: Request) -> Response:
    """Slack's interactivity request URL, exposed when this module is run
    standalone (`python slack_server.py`). In the actual deployed stack,
    agent/main.py registers the same handler on its own FastAPI app so
    everything serves off one port (8000) — see main.py's /slack/actions
    route."""
    raw_body = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    status_code, body = await handle_slack_action(raw_body, timestamp, signature)
    return JSONResponse(body, status_code=status_code)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    mcp.run(transport="http", host="0.0.0.0", port=8001)
