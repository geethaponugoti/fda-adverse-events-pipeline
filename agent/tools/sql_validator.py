"""
Tool: classifies proposed SQL into a safety tier using structural
parsing (sqlparse), not substring/keyword matching. Keyword matching
is a known-weak pattern for this — it's bypassed by multi-statement
payloads (a safe-looking first statement followed by a destructive
one after a semicolon), comments splitting keywords, or a
`WHERE 1=1` that textually "has" a WHERE clause while still matching
every row.

This is the authoritative safety gate for any SQL an LLM proposes —
graph.py must never execute or offer-for-approval SQL this module
hasn't classified first. The LLM's own self-reported risk_level is
advisory only, same as everywhere else in this agent; it is never the
thing that decides whether SQL runs.

Tiers, in order of how graph.py should treat them:
  REJECTED        — never executed, never sent to Slack either. DROP,
                     TRUNCATE, DELETE without a WHERE, multiple
                     statements in one payload, a table outside
                     ALLOWED_TABLES, or anything this parser can't
                     confidently classify (fail closed, not open).
  NEEDS_APPROVAL   — ALTER TABLE (other than a column rename), UPDATE,
                     DELETE with a WHERE, INSERT.
  AUTO_EXECUTABLE  — ALTER TABLE ... RENAME COLUMN only. Even then,
                     this module only classifies the *statement
                     shape* — it's schema_inspector's job to verify
                     the specific column names against live
                     information_schema before anything actually runs.
"""

import re
from enum import Enum
from typing import NamedTuple, Optional

import sqlparse
from sqlparse.sql import Where

# This pipeline only ever legitimately needs to touch its own tables.
# A fix proposal naming anything else — information_schema, pg_catalog,
# another database's tables, whatever an LLM might invent — is
# rejected regardless of what kind of statement it is.
ALLOWED_TABLES = {
    "raw.fda_adverse_events",
    "monitoring.schema_snapshots",
    "monitoring.incident_reports",
    "monitoring.agent_approvals",
    "monitoring.pipeline_runs",
    "monitoring.row_count_baselines",
    "monitoring.freshness_checks",
}

_TABLE_REF_RE = re.compile(
    r'\b(?:FROM|UPDATE|INTO|TABLE|TRUNCATE)\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s*\.\s*"?([a-zA-Z_][a-zA-Z0-9_]*)"?',
    re.IGNORECASE,
)

_RENAME_COLUMN_RE = re.compile(
    r'^\s*ALTER\s+TABLE\s+\S+\s+RENAME\s+COLUMN\s+\S+\s+TO\s+\S+\s*;?\s*$',
    re.IGNORECASE,
)


class SqlTier(str, Enum):
    AUTO_EXECUTABLE = "auto_executable"
    NEEDS_APPROVAL = "needs_approval"
    REJECTED = "rejected"


class SqlClassification(NamedTuple):
    tier: SqlTier
    reason: str
    statement_type: Optional[str]


def _referenced_tables(sql: str) -> set:
    return {
        f"{schema.lower()}.{table.lower()}"
        for schema, table in _TABLE_REF_RE.findall(sql)
    }


def _has_where_clause(statement) -> bool:
    return any(isinstance(tok, Where) for tok in statement.tokens)


def classify_sql(sql: Optional[str]) -> SqlClassification:
    if not sql or not sql.strip():
        return SqlClassification(SqlTier.REJECTED, "No SQL provided.", None)

    statements = [s for s in sqlparse.parse(sql) if s.tokens and s.token_first(skip_ws=True)]
    if len(statements) != 1:
        return SqlClassification(
            SqlTier.REJECTED,
            f"Expected exactly one SQL statement, found {len(statements)} — "
            f"multi-statement payloads are rejected outright regardless of content.",
            None,
        )

    statement = statements[0]
    stmt_type = statement.get_type()  # 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'UNKNOWN', ...
    upper_sql = sql.upper()

    tables = _referenced_tables(sql)
    if not tables:
        return SqlClassification(
            SqlTier.REJECTED,
            "Could not confidently identify which table this SQL touches — "
            "failing closed rather than assuming it's safe.",
            stmt_type,
        )
    disallowed = tables - ALLOWED_TABLES
    if disallowed:
        return SqlClassification(
            SqlTier.REJECTED,
            f"References table(s) outside the allowed set: {sorted(disallowed)}.",
            stmt_type,
        )

    if "DROP" in upper_sql or stmt_type == "DROP" or "TRUNCATE" in upper_sql:
        return SqlClassification(SqlTier.REJECTED, "DROP/TRUNCATE is never allowed.", stmt_type)

    if stmt_type == "DELETE":
        if not _has_where_clause(statement):
            return SqlClassification(
                SqlTier.REJECTED, "DELETE without a WHERE clause is never allowed.", stmt_type
            )
        return SqlClassification(SqlTier.NEEDS_APPROVAL, "DELETE with a WHERE clause.", stmt_type)

    if stmt_type == "ALTER" or upper_sql.strip().startswith("ALTER TABLE"):
        if _RENAME_COLUMN_RE.match(sql.strip()):
            return SqlClassification(
                SqlTier.AUTO_EXECUTABLE,
                "ALTER TABLE ... RENAME COLUMN — the one narrowly-scoped auto-executable "
                "form. Column names still need live information_schema verification "
                "before this actually runs (see schema_inspector).",
                stmt_type,
            )
        return SqlClassification(
            SqlTier.NEEDS_APPROVAL, "ALTER TABLE other than a plain column rename.", stmt_type
        )

    if stmt_type == "UPDATE":
        return SqlClassification(SqlTier.NEEDS_APPROVAL, "UPDATE always needs approval.", stmt_type)

    if stmt_type == "INSERT":
        return SqlClassification(
            SqlTier.NEEDS_APPROVAL,
            "INSERT always needs approval — an LLM-authored row landing in production "
            "with no human review is too risky to auto-execute.",
            stmt_type,
        )

    if stmt_type == "SELECT":
        return SqlClassification(
            SqlTier.REJECTED,
            "SELECT doesn't fix anything — it's not a valid fix action in this system.",
            stmt_type,
        )

    return SqlClassification(
        SqlTier.REJECTED, f"Unrecognized/unclassifiable statement type '{stmt_type}'.", stmt_type
    )
