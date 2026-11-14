"""Local, transactional serving view for bounded Spark aggregate batches."""

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def query_epoch(checkpoint, contract, *, tables=("windows", "alerts")):
    path = Path(checkpoint) / "quality-store.json"
    if not path.exists() and any((path.parent / name).exists() for name in ("metadata", "offsets", "commits")):
        raise ValueError("Existing Spark checkpoint has no SQLite epoch; use a new checkpoint")
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {"epoch": uuid4().hex, "contract": contract}
    try:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(metadata, handle, sort_keys=True)
    except FileExistsError:
        metadata = json.loads(path.read_text(encoding="utf-8"))
        if metadata["contract"] != contract:
            raise ValueError("SQLite query settings changed; use a new checkpoint")
    commits = path.parent / "commits"
    committed = [int(item.name) for item in commits.iterdir() if item.name.isdigit()] if commits.is_dir() else []
    if committed:
        # A retained Spark commit must still have its matching durable sink transaction.
        database = Path(contract["database"]).resolve()
        try:
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro")) as connection:
                for table in tables:
                    connection.execute(f"SELECT 1 FROM {table} LIMIT 1")
                applied = connection.execute(
                    "SELECT 1 FROM processed_batches WHERE query_epoch=? AND batch_id=?",
                    (metadata["epoch"], max(committed)),
                ).fetchone()
        except sqlite3.Error as error:
            raise ValueError("Serving database is missing or incomplete; restore it with its checkpoint") from error
        if not applied:
            raise ValueError("Serving batch ledger does not match the checkpoint; restore matching runtime state")
    return metadata["epoch"]


def write_batch(database, rows, epoch, batch_id, *, threshold=0.15, minimum=30, final=False):
    if not math.isfinite(threshold) or not 0 <= threshold <= 1 or minimum < 1:
        raise ValueError("Alert threshold must be in [0, 1] and minimum must be positive")
    rule = {"version": "reject-rate-v1", "threshold": threshold, "minimum": minimum, "operator": ">"}
    rule_id = identity(rule)
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=30)) as connection, connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS windows (
                window_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, line_id TEXT NOT NULL,
                category TEXT NOT NULL, model_version TEXT NOT NULL, threshold_version TEXT NOT NULL,
                window_start TEXT NOT NULL, window_end TEXT NOT NULL,
                inspections INTEGER NOT NULL, rejects INTEGER NOT NULL, reject_rate REAL NOT NULL,
                is_final INTEGER NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY, window_key TEXT NOT NULL, rule_id TEXT NOT NULL,
                rule_version TEXT NOT NULL, threshold REAL NOT NULL, minimum_inspections INTEGER NOT NULL,
                status TEXT NOT NULL, is_provisional INTEGER NOT NULL, updated_at TEXT NOT NULL,
                UNIQUE(window_key, rule_id)
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
        for row in rows:
            count, rejects = int(row["inspections"]), int(row["rejects"])
            if count == 0:
                continue
            if count < 0 or not 0 <= rejects <= count:
                raise ValueError("Invalid window counts")
            boundaries = []
            for name in ("start", "end"):
                value = row[name]
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                boundaries.append(value.astimezone(timezone.utc).isoformat())
            group = [row[name] for name in (
                "run_id", "line_id", "category", "model_version", "threshold_version",
            )]
            key = identity([*group, *boundaries])
            previous = connection.execute(
                "SELECT inspections, rejects, is_final FROM windows WHERE window_key=?", (key,),
            ).fetchone()
            if previous:
                if previous[2] and not final:
                    continue
                if count < previous[0]:
                    raise ValueError("Window counts regressed; replay must reconstruct complete snapshots")
                if (previous[2] or count == previous[0]) and (count, rejects) != previous[:2]:
                    raise ValueError("Conflicting window snapshot")
            rate = rejects / count
            connection.execute("""
                INSERT INTO windows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(window_key) DO UPDATE SET
                    inspections=excluded.inspections, rejects=excluded.rejects,
                    reject_rate=excluded.reject_rate, is_final=excluded.is_final, updated_at=excluded.updated_at
            """, (key, *group, *boundaries, count, rejects, rate, int(final), now))
            alert_id = identity([key, rule_id])
            old = connection.execute("SELECT status FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            if count < minimum:
                status = "insufficient_data"
            elif rate > threshold:
                status = "active"
            else:
                status = "resolved" if old and old[0] in ("active", "resolved") else "clear"
            connection.execute("""
                INSERT INTO alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(alert_id) DO UPDATE SET status=excluded.status,
                    is_provisional=excluded.is_provisional, updated_at=excluded.updated_at
            """, (alert_id, key, rule_id, rule["version"], threshold, minimum, status, int(not final), now))
        connection.execute("INSERT INTO processed_batches VALUES (?, ?, ?)", (epoch, batch_id, now))


def sqlite_sink(database, epoch, *, threshold=0.15, minimum=30, final=False, max_rows=1000):
    def save(batch, batch_id):
        # Collect only a bounded aggregate batch, never the inspection archive.
        rows = batch.limit(max_rows + 1).collect()
        if len(rows) > max_rows:
            raise RuntimeError(f"Aggregate batch exceeds --max-store-rows={max_rows}")
        write_batch(database, rows, epoch, batch_id, threshold=threshold, minimum=minimum, final=final)
    return save
