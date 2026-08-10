"""
Tool: queries PostgreSQL information_schema for the live column set of
raw.fda_adverse_events. get_schema_diff() diffs it against the most
recent batch in monitoring.schema_snapshots (written by
validate_raw_schema in fda_pipeline_dag.py) for investigation
purposes. find_safe_rename()/apply_safe_rename_fix() check against a
fixed canonical schema instead — see their docstrings for why
snapshot-diffing alone isn't reliable enough to safely auto-fix from.
"""

import os
from typing import Optional

from sqlalchemy import create_engine, text

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)

# The exact columns raw.fda_adverse_events is created with (see
# scripts/init_db.sql). Used by find_safe_rename()/apply_safe_rename_fix()
# as a fixed reference point — see those functions' docstrings for why
# diffing against monitoring.schema_snapshots isn't reliable enough for
# this on its own.
CANONICAL_COLUMNS = {
    "report_id", "received_date", "serious", "serious_death", "serious_hosp",
    "serious_life", "patient_age", "patient_age_unit", "patient_sex",
    "drug_name", "drug_indication", "reaction", "outcome", "country",
    "loaded_at", "load_date",
}


def _engine():
    return create_engine(PIPELINE_DB_CONN)


def _current_columns(conn, schema_name: str, table_name: str) -> dict:
    rows = conn.execute(text("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema_name AND table_name = :table_name
    """), {"schema_name": schema_name, "table_name": table_name}).fetchall()
    return {row[0]: {"data_type": row[1], "is_nullable": row[2]} for row in rows}


def _latest_snapshot(conn, schema_name: str, table_name: str):
    latest_ts = conn.execute(text("""
        SELECT MAX(snapshotted_at) FROM monitoring.schema_snapshots
        WHERE table_name = :table_name AND schema_name = :schema_name
    """), {"table_name": table_name, "schema_name": schema_name}).scalar_one()

    if latest_ts is None:
        return None

    rows = conn.execute(text("""
        SELECT column_name, data_type, is_nullable
        FROM monitoring.schema_snapshots
        WHERE table_name = :table_name AND schema_name = :schema_name
          AND snapshotted_at = :snapshotted_at
    """), {
        "table_name": table_name,
        "schema_name": schema_name,
        "snapshotted_at": latest_ts,
    }).fetchall()

    return {row[0]: {"data_type": row[1], "is_nullable": row[2]} for row in rows}


def get_schema_diff(
    schema_name: str = "raw", table_name: str = "fda_adverse_events"
) -> dict:
    """Returns added/removed/changed-type columns between the live table
    and the most recent monitoring.schema_snapshots batch."""
    engine = _engine()
    with engine.connect() as conn:
        current = _current_columns(conn, schema_name, table_name)
        previous = _latest_snapshot(conn, schema_name, table_name)

    if previous is None:
        return {
            "has_prior_snapshot": False,
            "added": [],
            "removed": [],
            "changed_types": [],
            "current_columns": sorted(current),
        }

    added = sorted(set(current) - set(previous))
    removed = sorted(set(previous) - set(current))
    changed_types = sorted(
        col for col in (set(current) & set(previous))
        if current[col] != previous[col]
    )

    return {
        "has_prior_snapshot": True,
        "added": added,
        "removed": removed,
        "changed_types": changed_types,
        "current_columns": sorted(current),
        "previous_columns": sorted(previous),
    }


def get_current_columns(
    schema_name: str = "raw", table_name: str = "fda_adverse_events"
) -> dict:
    """Public wrapper around _current_columns() — live schema context
    for the LLM fix-generation prompt in graph.py's fix_node."""
    with _engine().connect() as conn:
        return _current_columns(conn, schema_name, table_name)


def get_previous_snapshot(
    schema_name: str = "raw", table_name: str = "fda_adverse_events"
) -> Optional[dict]:
    """Public wrapper around _latest_snapshot() — same caveat as
    get_schema_diff(): this is the last snapshot batch, which can
    itself already reflect a drift (see find_safe_rename()'s
    docstring). Still useful as LLM context ("here's what it used to
    look like"), just not a safe basis for auto-executing anything."""
    with _engine().connect() as conn:
        return _latest_snapshot(conn, schema_name, table_name)


def find_safe_rename(
    schema_name: str = "raw", table_name: str = "fda_adverse_events"
) -> Optional[tuple]:
    """Returns (current_name, canonical_name) if the live table is
    missing exactly one CANONICAL_COLUMNS entry and has exactly one
    extra column not in that set — the narrow, unambiguous-rename
    signature this codebase treats as safe to auto-fix. None otherwise.

    Deliberately checks against the fixed CANONICAL_COLUMNS set, not
    get_schema_diff()'s snapshot comparison: validate_raw_schema
    re-snapshots the live table's columns on every run, including runs
    where check_failure_injection already renamed a column earlier in
    the same DAG run. That makes the "latest snapshot" an unreliable
    reference — it can itself already reflect the drifted state, which
    would make diff-based detection blind to a drift that's still
    actually there."""
    with _engine().connect() as conn:
        current = set(_current_columns(conn, schema_name, table_name))

    missing = CANONICAL_COLUMNS - current
    extra = current - CANONICAL_COLUMNS

    if len(missing) != 1 or len(extra) != 1:
        return None

    return next(iter(extra)), next(iter(missing))


def apply_safe_rename_fix(
    schema_name: str = "raw", table_name: str = "fda_adverse_events"
) -> Optional[str]:
    """Applies the rename find_safe_rename() identifies, if any, and
    returns a description of what it did (None if there was nothing to
    fix). Identifiers always come from information_schema — resolved
    fresh by find_safe_rename() — never from LLM output."""
    rename = find_safe_rename(schema_name, table_name)
    if rename is None:
        return None

    current_name, canonical_name = rename
    with _engine().begin() as conn:
        conn.execute(text(
            f'ALTER TABLE {schema_name}.{table_name} '
            f'RENAME COLUMN "{current_name}" TO "{canonical_name}"'
        ))

    return f"Renamed column '{current_name}' back to '{canonical_name}'."
