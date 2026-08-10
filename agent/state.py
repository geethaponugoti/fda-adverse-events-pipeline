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
    # Where this proposal came from — "mechanical" (the deterministic,
    # information_schema-verified rename), "qdrant_reuse" (a prior
    # incident's fix that worked), "llm" (a fresh OpenAI proposal), or
    # "none" (no fix attempted, e.g. P3). Purely for observability —
    # tools/sql_validator.py's classification, not this field, is what
    # actually gates execution.
    source: str


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
    sql: Optional[str]
    sql_tier: Optional[str]
    has_sql_fix: bool


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
    # True only if proposed_fix.sql (or the re-derived verified rename
    # for the AUTO_EXECUTABLE tier) actually ran against the database —
    # approval_status alone doesn't distinguish that from "the graph
    # completed successfully via a plain retry with no SQL executed at
    # all" (e.g. a P1 auto-fix that fell back to retry-only because the
    # proposed rename no longer applied). postmortem.py uses this, not
    # approval_status, to decide whether a fix is safe to cache in
    # Qdrant for future reuse.
    fix_executed: Optional[bool]
    postmortem: Postmortem
