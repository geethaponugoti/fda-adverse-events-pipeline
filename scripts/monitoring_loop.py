"""
Monitoring loop for the FDA adverse events pipeline.

Every 5 minutes (or once, with --once) this checks raw.fda_adverse_events for:

  1. Row count deviation of more than 30% from the recorded baseline.
  2. The table not having been loaded (MAX(loaded_at)) in over 24 hours.
  3. Any change to the table's column set (schema drift).

Part 1 has no downstream agent yet, so alerts are only printed to the
console as structured JSON payloads -- nothing is POSTed anywhere.
"""

import argparse
import datetime as dt
import json
import logging
import os
import time

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("monitoring_loop")

DB_CONN = os.environ.get("PIPELINE_DB_CONN", "postgresql+psycopg2://pipeline:pipeline@localhost/fda_pipeline")

MONITORED_SCHEMA = "raw"
MONITORED_TABLE = "fda_adverse_events"

ROW_COUNT_DEVIATION_THRESHOLD = 0.30
FRESHNESS_THRESHOLD_HOURS = 24
CHECK_INTERVAL_SECONDS = 300


def get_engine():
    return create_engine(DB_CONN)


def emit_alert(alert_type: str, severity: str, message: str, details: dict):
    payload = {
        "alert_type": alert_type,
        "severity": severity,
        "schema_name": MONITORED_SCHEMA,
        "table_name": MONITORED_TABLE,
        "message": message,
        "details": details,
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
    }
    logger.warning("ALERT (%s/%s): %s", alert_type, severity, message)
    print(json.dumps(payload, indent=2, default=str))


def check_row_count_deviation(conn):
    current_count = conn.execute(
        text(f"SELECT COUNT(*) FROM {MONITORED_SCHEMA}.{MONITORED_TABLE}")
    ).scalar_one()

    baseline_row = conn.execute(text("""
        SELECT baseline_count FROM monitoring.row_count_baselines
        WHERE table_name = :table_name AND schema_name = :schema_name
        ORDER BY recorded_at DESC
        LIMIT 1
    """), {"table_name": MONITORED_TABLE, "schema_name": MONITORED_SCHEMA}).fetchone()

    if baseline_row is None:
        conn.execute(text("""
            INSERT INTO monitoring.row_count_baselines (table_name, schema_name, baseline_count)
            VALUES (:table_name, :schema_name, :baseline_count)
        """), {"table_name": MONITORED_TABLE, "schema_name": MONITORED_SCHEMA, "baseline_count": current_count})
        logger.info("No baseline existed; recorded initial baseline of %s rows.", current_count)
        return

    baseline_count = baseline_row[0]
    if baseline_count == 0:
        deviation = 1.0 if current_count > 0 else 0.0
    else:
        deviation = abs(current_count - baseline_count) / baseline_count

    logger.info(
        "Row count check: current=%s baseline=%s deviation=%.1f%%",
        current_count, baseline_count, deviation * 100,
    )

    if deviation > ROW_COUNT_DEVIATION_THRESHOLD:
        emit_alert(
            alert_type="row_count_deviation",
            severity="critical" if deviation > 0.5 else "warning",
            message=(
                f"{MONITORED_SCHEMA}.{MONITORED_TABLE} row count deviated "
                f"{deviation * 100:.1f}% from baseline ({current_count} vs {baseline_count})"
            ),
            details={
                "current_count": current_count,
                "baseline_count": baseline_count,
                "deviation_pct": round(deviation * 100, 2),
                "threshold_pct": ROW_COUNT_DEVIATION_THRESHOLD * 100,
            },
        )


def check_freshness(conn):
    last_loaded = conn.execute(
        text(f"SELECT MAX(loaded_at) FROM {MONITORED_SCHEMA}.{MONITORED_TABLE}")
    ).scalar_one()

    hours_since = None

    if last_loaded is None:
        is_stale = True
        message = f"{MONITORED_SCHEMA}.{MONITORED_TABLE} has no rows / no loaded_at timestamp."
    else:
        now = dt.datetime.utcnow()
        hours_since = (now - last_loaded).total_seconds() / 3600
        is_stale = hours_since > FRESHNESS_THRESHOLD_HOURS
        message = (
            f"{MONITORED_SCHEMA}.{MONITORED_TABLE} last loaded {hours_since:.1f}h ago "
            f"(threshold: {FRESHNESS_THRESHOLD_HOURS}h)"
        )

    conn.execute(text("""
        INSERT INTO monitoring.freshness_checks (table_name, schema_name, last_loaded, is_stale)
        VALUES (:table_name, :schema_name, :last_loaded, :is_stale)
    """), {
        "table_name": MONITORED_TABLE,
        "schema_name": MONITORED_SCHEMA,
        "last_loaded": last_loaded,
        "is_stale": is_stale,
    })

    logger.info("Freshness check: %s", message)

    if is_stale:
        emit_alert(
            alert_type="freshness_violation",
            severity="critical",
            message=message,
            details={
                "last_loaded": last_loaded.isoformat() if last_loaded else None,
                "hours_since_load": round(hours_since, 2) if hours_since is not None else None,
                "threshold_hours": FRESHNESS_THRESHOLD_HOURS,
            },
        )


def _current_columns(conn):
    rows = conn.execute(text("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema_name AND table_name = :table_name
        ORDER BY column_name
    """), {"schema_name": MONITORED_SCHEMA, "table_name": MONITORED_TABLE}).fetchall()
    return {row[0]: {"data_type": row[1], "is_nullable": row[2]} for row in rows}


def _latest_snapshot(conn):
    latest_ts = conn.execute(text("""
        SELECT MAX(snapshotted_at) FROM monitoring.schema_snapshots
        WHERE table_name = :table_name AND schema_name = :schema_name
    """), {"table_name": MONITORED_TABLE, "schema_name": MONITORED_SCHEMA}).scalar_one()

    if latest_ts is None:
        return None

    rows = conn.execute(text("""
        SELECT column_name, data_type, is_nullable
        FROM monitoring.schema_snapshots
        WHERE table_name = :table_name AND schema_name = :schema_name
          AND snapshotted_at = :snapshotted_at
    """), {"table_name": MONITORED_TABLE, "schema_name": MONITORED_SCHEMA, "snapshotted_at": latest_ts}).fetchall()

    return {row[0]: {"data_type": row[1], "is_nullable": row[2]} for row in rows}


def _record_snapshot(conn, columns: dict):
    for col_name, meta in columns.items():
        conn.execute(text("""
            INSERT INTO monitoring.schema_snapshots (table_name, schema_name, column_name, data_type, is_nullable)
            VALUES (:table_name, :schema_name, :column_name, :data_type, :is_nullable)
        """), {
            "table_name": MONITORED_TABLE,
            "schema_name": MONITORED_SCHEMA,
            "column_name": col_name,
            "data_type": meta["data_type"],
            "is_nullable": meta["is_nullable"],
        })


def check_schema_drift(conn):
    current_columns = _current_columns(conn)
    previous_columns = _latest_snapshot(conn)

    if previous_columns is None:
        _record_snapshot(conn, current_columns)
        logger.info("No prior schema snapshot; recorded initial snapshot with %s columns.", len(current_columns))
        return

    added = sorted(set(current_columns) - set(previous_columns))
    removed = sorted(set(previous_columns) - set(current_columns))
    changed_types = sorted(
        col for col in (set(current_columns) & set(previous_columns))
        if current_columns[col] != previous_columns[col]
    )

    if added or removed or changed_types:
        _record_snapshot(conn, current_columns)
        message = (
            f"Schema change detected on {MONITORED_SCHEMA}.{MONITORED_TABLE}: "
            f"added={added} removed={removed} changed_types={changed_types}"
        )
        logger.warning(message)
        emit_alert(
            alert_type="schema_drift",
            severity="critical",
            message=message,
            details={"added_columns": added, "removed_columns": removed, "changed_type_columns": changed_types},
        )
    else:
        logger.info("Schema check: no changes detected (%s columns).", len(current_columns))


def run_checks_once():
    engine = get_engine()
    with engine.begin() as conn:
        try:
            check_row_count_deviation(conn)
        except Exception:
            logger.exception("Row count deviation check failed.")
        try:
            check_freshness(conn)
        except Exception:
            logger.exception("Freshness check failed.")
        try:
            check_schema_drift(conn)
        except Exception:
            logger.exception("Schema drift check failed.")


def main():
    parser = argparse.ArgumentParser(description=f"Monitor {MONITORED_SCHEMA}.{MONITORED_TABLE} for anomalies.")
    parser.add_argument("--once", action="store_true", help="Run a single check cycle and exit.")
    parser.add_argument(
        "--interval", type=int, default=CHECK_INTERVAL_SECONDS,
        help=f"Seconds between check cycles (default: {CHECK_INTERVAL_SECONDS}).",
    )
    args = parser.parse_args()

    if args.once:
        run_checks_once()
        return

    logger.info("Starting monitoring loop (interval=%ss)", args.interval)
    while True:
        run_checks_once()
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
