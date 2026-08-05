"""
Failure injection CLI for the FDA adverse events pipeline demo/test
environment.

Each function simulates a realistic way this pipeline breaks in production:

  volume_anomaly    -- a silent partial-delete, common when an upstream
                        job is killed mid-write (deletes 60% of rows).
  data_quality      -- a batch of rows lands with NULL drug names and a
                        batch lands with a garbage seriousness code.
  schema_drift      -- openFDA renames drug_name to drug_brand_nm between
                        API versions.
  freshness_failure -- the pipeline silently stops running; loaded_at
                        drifts into the past.

Before the first mutation, a pristine snapshot of raw.fda_adverse_events
is captured in raw.fda_adverse_events_backup. `restore` uses that snapshot
to repair both the schema and the data.

Usage:
    python inject_failure.py --type volume_anomaly
    python inject_failure.py --type data_quality
    python inject_failure.py --type schema_drift
    python inject_failure.py --type freshness_failure
    python inject_failure.py --type restore
"""

import argparse
import logging
import os

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("inject_failure")

DB_CONN = os.environ.get("PIPELINE_DB_CONN", "postgresql+psycopg2://pipeline:pipeline@localhost/fda_pipeline")

RAW_TABLE = "raw.fda_adverse_events"
BACKUP_TABLE = "raw.fda_adverse_events_backup"

RAW_COLUMNS = [
    "report_id", "received_date", "serious", "serious_death", "serious_hosp",
    "serious_life", "patient_age", "patient_age_unit", "patient_sex",
    "drug_name", "drug_indication", "reaction", "outcome", "country",
    "loaded_at", "load_date",
]


def _engine():
    return create_engine(DB_CONN)


def _table_exists(conn, table: str) -> bool:
    schema_name, table_name = table.split(".")
    row = conn.execute(text("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = :schema_name AND table_name = :table_name
    """), {"schema_name": schema_name, "table_name": table_name}).fetchone()
    return row is not None


def _column_exists(conn, table: str, column: str) -> bool:
    schema_name, table_name = table.split(".")
    row = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = :schema_name AND table_name = :table_name AND column_name = :column_name
    """), {"schema_name": schema_name, "table_name": table_name, "column_name": column}).fetchone()
    return row is not None


def _ensure_backup(conn):
    """Snapshot raw.fda_adverse_events the first time any failure is
    injected, so `restore` always has a pristine copy to roll back to."""
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} AS SELECT * FROM {RAW_TABLE}"))


def inject_volume_anomaly(fraction: float = 0.6):
    """Silently delete a large fraction of rows, simulating a truncated /
    partial upstream load that nobody noticed."""
    engine = _engine()
    with engine.begin() as conn:
        _ensure_backup(conn)
        total = conn.execute(text(f"SELECT COUNT(*) FROM {RAW_TABLE}")).scalar_one()
        if total == 0:
            logger.warning("%s is empty; nothing to delete. Load data first.", RAW_TABLE)
            return
        n_delete = int(total * fraction)

        conn.execute(text(f"""
            WITH target AS (
                SELECT ctid FROM {RAW_TABLE}
                ORDER BY random()
                LIMIT :n
            )
            DELETE FROM {RAW_TABLE}
            WHERE ctid IN (SELECT ctid FROM target)
        """), {"n": n_delete})

        remaining = conn.execute(text(f"SELECT COUNT(*) FROM {RAW_TABLE}")).scalar_one()

    logger.warning(
        "VOLUME ANOMALY injected: deleted %s of %s rows from %s (remaining: %s).",
        n_delete, total, RAW_TABLE, remaining,
    )


def inject_data_quality(null_fraction: float = 0.1, bad_serious_fraction: float = 0.05):
    """Corrupt a fraction of rows with NULL drug_name and a fraction with
    an out-of-range serious code, simulating a bad upstream extract."""
    engine = _engine()
    with engine.begin() as conn:
        _ensure_backup(conn)
        total = conn.execute(text(f"SELECT COUNT(*) FROM {RAW_TABLE}")).scalar_one()
        if total == 0:
            logger.warning("%s is empty; nothing to corrupt. Load data first.", RAW_TABLE)
            return

        n_null = max(1, int(total * null_fraction))
        n_bad_serious = max(1, int(total * bad_serious_fraction))

        conn.execute(text(f"""
            WITH target AS (
                SELECT ctid FROM {RAW_TABLE}
                ORDER BY random()
                LIMIT :n
            )
            UPDATE {RAW_TABLE}
            SET drug_name = NULL
            WHERE ctid IN (SELECT ctid FROM target)
        """), {"n": n_null})

        conn.execute(text(f"""
            WITH target AS (
                SELECT ctid FROM {RAW_TABLE}
                ORDER BY random()
                LIMIT :n
            )
            UPDATE {RAW_TABLE}
            SET serious = '-999'
            WHERE ctid IN (SELECT ctid FROM target)
        """), {"n": n_bad_serious})

    logger.warning(
        "DATA QUALITY issue injected: ~%s rows in %s now have NULL drug_name and ~%s rows have serious = '-999'.",
        n_null, RAW_TABLE, n_bad_serious,
    )


def inject_schema_drift():
    """Rename drug_name to drug_brand_nm, simulating openFDA renaming the
    field between API versions."""
    engine = _engine()
    with engine.begin() as conn:
        _ensure_backup(conn)

        if not _column_exists(conn, RAW_TABLE, "drug_name"):
            logger.warning("drug_name is already missing from %s; schema drift already injected?", RAW_TABLE)
            return

        conn.execute(text(f"ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS drug_brand_nm VARCHAR(500)"))
        conn.execute(text(f"UPDATE {RAW_TABLE} SET drug_brand_nm = drug_name"))
        conn.execute(text(f"ALTER TABLE {RAW_TABLE} DROP COLUMN drug_name"))

    logger.warning(
        "SCHEMA DRIFT injected: %s.drug_name renamed to drug_brand_nm (simulates an openFDA field rename).",
        RAW_TABLE,
    )


def inject_freshness_failure():
    """Push loaded_at 3 days into the past on every row, simulating a
    pipeline that silently stopped running."""
    engine = _engine()
    with engine.begin() as conn:
        _ensure_backup(conn)
        result = conn.execute(text(f"""
            UPDATE {RAW_TABLE}
            SET loaded_at = NOW() - INTERVAL '3 days'
        """))
        n_updated = result.rowcount

    logger.warning(
        "FRESHNESS FAILURE injected: loaded_at set to 3 days ago on %s rows in %s.",
        n_updated, RAW_TABLE,
    )


def restore():
    """Undo any injected failure by restoring both schema and data from
    the pristine backup snapshot."""
    engine = _engine()
    with engine.begin() as conn:
        if not _table_exists(conn, BACKUP_TABLE):
            logger.warning(
                "No backup found at %s; nothing has been injected yet, or restore was already run.",
                BACKUP_TABLE,
            )
            return

        if not _column_exists(conn, RAW_TABLE, "drug_name"):
            conn.execute(text(f"ALTER TABLE {RAW_TABLE} ADD COLUMN drug_name VARCHAR(500)"))
        if _column_exists(conn, RAW_TABLE, "drug_brand_nm"):
            conn.execute(text(f"ALTER TABLE {RAW_TABLE} DROP COLUMN drug_brand_nm"))

        conn.execute(text(f"TRUNCATE TABLE {RAW_TABLE}"))

        columns_sql = ", ".join(RAW_COLUMNS)
        conn.execute(text(f"""
            INSERT INTO {RAW_TABLE} ({columns_sql})
            SELECT {columns_sql} FROM {BACKUP_TABLE}
        """))

        restored = conn.execute(text(f"SELECT COUNT(*) FROM {RAW_TABLE}")).scalar_one()

    logger.info("Restored %s to its pristine snapshot (%s rows).", RAW_TABLE, restored)


FAILURE_FUNCS = {
    "volume_anomaly": inject_volume_anomaly,
    "data_quality": inject_data_quality,
    "schema_drift": inject_schema_drift,
    "freshness_failure": inject_freshness_failure,
    "restore": restore,
}


def main():
    parser = argparse.ArgumentParser(description="Inject (or restore from) a pipeline failure scenario.")
    parser.add_argument(
        "--type", required=True, choices=sorted(FAILURE_FUNCS.keys()),
        help="Which failure scenario to inject, or 'restore' to fix everything.",
    )
    args = parser.parse_args()

    FAILURE_FUNCS[args.type]()


if __name__ == "__main__":
    main()
