"""Bounded recent inspections and latest observations for the dashboard."""

import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from quality_monitor.events import validate_event


COLUMNS = (
    "run_id", "inspection_id", "line_id", "category", "model_version", "threshold_version",
    "scenario_id", "product_id", "image_id", "image_uri", "event_time", "published_at",
    "anomaly_score", "is_reject", "stored_at",
)


def inspection_time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Spark returns naive timestamps; the projection session is explicitly UTC.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def write_inspections(database, rows, epoch, batch_id, *, retention=1000):
    if retention < 1:
        raise ValueError("Inspection retention must be positive")
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=30)) as connection, connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS inspection_samples (
                run_id TEXT NOT NULL, inspection_id TEXT NOT NULL, line_id TEXT NOT NULL,
                category TEXT NOT NULL, model_version TEXT NOT NULL, threshold_version TEXT NOT NULL,
                scenario_id TEXT NOT NULL, product_id TEXT NOT NULL, image_id TEXT NOT NULL,
                image_uri TEXT NOT NULL, event_time TEXT NOT NULL, published_at TEXT NOT NULL,
                anomaly_score REAL NOT NULL, is_reject INTEGER NOT NULL, stored_at TEXT NOT NULL,
                PRIMARY KEY(run_id, inspection_id)
            );
            CREATE INDEX IF NOT EXISTS samples_recent
                ON inspection_samples(run_id, line_id, event_time DESC, inspection_id DESC);
            CREATE TABLE IF NOT EXISTS station_latest (
                run_id TEXT NOT NULL, inspection_id TEXT NOT NULL, line_id TEXT NOT NULL,
                category TEXT NOT NULL, model_version TEXT NOT NULL, threshold_version TEXT NOT NULL,
                scenario_id TEXT NOT NULL, product_id TEXT NOT NULL, image_id TEXT NOT NULL,
                image_uri TEXT NOT NULL, event_time TEXT NOT NULL, published_at TEXT NOT NULL,
                anomaly_score REAL NOT NULL, is_reject INTEGER NOT NULL, stored_at TEXT NOT NULL,
                PRIMARY KEY(run_id, line_id, category, model_version, threshold_version)
            );
            CREATE TABLE IF NOT EXISTS processed_batches (
                query_epoch TEXT NOT NULL, batch_id INTEGER NOT NULL, processed_at TEXT NOT NULL,
                PRIMARY KEY(query_epoch, batch_id)
            );
        """)
        connection.execute("BEGIN IMMEDIATE")
        if connection.execute(
            "SELECT 1 FROM processed_batches WHERE query_epoch=? AND batch_id=?", (epoch, batch_id),
        ).fetchone():
            return
        now = datetime.now(timezone.utc).isoformat()
        groups = set()
        placeholders = ",".join("?" for _ in COLUMNS)
        assignments = ",".join(f"{name}=excluded.{name}" for name in COLUMNS)
        for original in rows:
            row = dict(original)
            for name in ("event_time", "published_at"):
                row[name] = inspection_time(row[name])
            validate_event(row)
            if row["record_kind"] != "inspection":
                continue
            row["stored_at"] = now
            values = [row[name] for name in COLUMNS]
            inserted = connection.execute(
                f"INSERT INTO inspection_samples VALUES ({placeholders}) ON CONFLICT(run_id, inspection_id) DO NOTHING",
                values,
            ).rowcount
            if not inserted:
                continue
            connection.execute(f"""
                INSERT INTO station_latest VALUES ({placeholders})
                ON CONFLICT(run_id, line_id, category, model_version, threshold_version)
                DO UPDATE SET {assignments}
                WHERE excluded.event_time > station_latest.event_time
                   OR (excluded.event_time = station_latest.event_time
                       AND excluded.inspection_id > station_latest.inspection_id)
            """, values)
            groups.add((row["run_id"], row["line_id"]))
        for run_id, line_id in groups:
            connection.execute("""
                DELETE FROM inspection_samples WHERE rowid IN (
                    SELECT rowid FROM inspection_samples WHERE run_id=? AND line_id=?
                    ORDER BY event_time DESC, inspection_id DESC LIMIT -1 OFFSET ?
                )
            """, (run_id, line_id, retention))
        connection.execute("INSERT INTO processed_batches VALUES (?, ?, ?)", (epoch, batch_id, now))
