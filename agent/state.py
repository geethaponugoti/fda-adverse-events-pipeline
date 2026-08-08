"""
Agent state schema for the FDA pipeline self-healing agent.

This is the shared state object threaded through every node in the
LangGraph graph defined in graph.py. Each node reads what it needs and
returns a partial dict of the fields it updates; LangGraph merges that
into the running state.
"""

from typing import Literal, Optional, TypedDict

ErrorType = Literal[
    "data_corruption", "schema_drift", "volume_anomaly",
    "data_quality", "freshness", "upstream_failure",
]

RiskLevel = Literal["low", "medium", "high"]

# P0 = data corruption: pause the DAG immediately, escalate.
# P1 = schema drift / volume anomaly: auto-fix, notify Slack, auto-apply
#      if unanswered after SLACK_TIMEOUT_MINUTES.
# P2 = data quality / freshness: auto-fix silently, log only.
# P3 = upstream/infra failure: log only, no fix attempted.
Severity = Literal["P0", "P1", "P2", "P3"]

ApprovalStatus = Literal[
    "pending", "auto_approved", "escalated", "approved", "rejected"
]


class AlertPayload(TypedDict):
    """Raw alert as received on the /alert endpoint."""
    dag_id: str
    run_id: str
    task_id: str
    log_text: Optional[str]


class ParsedLog(TypedDict):
    """Output of tools/log_parser.py."""
    task_id: str
    error_message: str
    error_type_hint: Optional[ErrorType]
    raw_log: str


class InvestigationFindings(TypedDict):
    """Output of the investigate node."""
    tool_used: str
    summary: str
    details: dict


class ProposedFix(TypedDict):
    """Output of the fix node."""
    description: str
    risk_level: RiskLevel
    sql: Optional[str]
    auto_applicable: bool


class Postmortem(TypedDict):
    """Output of postmortem.py."""
    incident_id: str
    created_at: str
    dag_id: str
    run_id: str
    task_id: str
    error_type: Optional[ErrorType]
    severity: Optional[Severity]
    summary: str
    root_cause: str
    resolution: str
    risk_level: Optional[RiskLevel]
    approval_status: ApprovalStatus


class AgentState(TypedDict, total=False):
    """Full graph state. All fields except `alert` are populated as the
    graph progresses through ingest -> classify -> investigate -> fix ->
    approve_or_escalate -> postmortem."""

    alert: AlertPayload
    parsed_log: ParsedLog
    error_type: Optional[ErrorType]
    severity: Optional[Severity]
    investigation: InvestigationFindings
    proposed_fix: ProposedFix
    approval_status: ApprovalStatus
    postmortem: Postmortem
