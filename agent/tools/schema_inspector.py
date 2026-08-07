"""
Tool: queries PostgreSQL information_schema for the live column set of
raw.fda_adverse_events and diffs it against the most recent batch in
monitoring.schema_snapshots (written by validate_raw_schema in
fda_pipeline_dag.py) to detect what changed.
"""

import os

from sqlalchemy import create_engine, text

PIPELINE_DB_CONN = os.environ.get(
    "PIPELINE_DB_CONN",
    "postgresql+psycopg2://pipeline:pipeline@postgres/fda_pipeline",
)


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
