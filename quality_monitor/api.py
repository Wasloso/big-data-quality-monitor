"""Read-only HTTP access to the local quality serving database."""

import os
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="Quality monitor API")
DATABASE = Path(os.environ.get("QUALITY_DATABASE", "/data/serving/quality.sqlite"))


@contextmanager
def snapshot():
    if not DATABASE.is_file():
        yield None
        return
    try:
        with closing(sqlite3.connect(DATABASE.resolve().as_uri() + "?mode=ro", timeout=5)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            yield connection
    except sqlite3.Error as error:
        raise HTTPException(503, "Serving database is unavailable or not initialized") from error


@app.get("/api/runs")
def runs():
    with snapshot() as connection:
        rows = [] if connection is None else connection.execute("""
            SELECT run_id, category, model_version, threshold_version, MAX(updated_at) AS updated_at
            FROM windows GROUP BY run_id, category, model_version, threshold_version
            ORDER BY updated_at DESC, run_id LIMIT 101
        """).fetchall()
    return {
        "state": "missing" if connection is None else "ready" if rows else "empty",
        "runs": [dict(row) for row in rows[:100]], "truncated": len(rows) > 100,
    }


def utc_bound(value):
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(422, "Time filters must include a UTC offset")
    return value.astimezone(timezone.utc).isoformat()


@app.get("/api/overview")
def overview(
    run_id: str = Query(min_length=1, max_length=200),
    category: str = Query(min_length=1, max_length=200),
    model_version: str = Query(min_length=1, max_length=200),
    threshold_version: str = Query(min_length=1, max_length=200),
    line_id: str | None = Query(default=None, min_length=1, max_length=200),
    from_time: datetime | None = None,
    to_time: datetime | None = None,
):
    start, end = utc_bound(from_time), utc_bound(to_time)
    if from_time is not None and to_time is not None and from_time >= to_time:
        raise HTTPException(422, "Time range must have a start before its end")
    where = ["run_id=?", "category=?", "model_version=?", "threshold_version=?"]
    values = [run_id, category, model_version, threshold_version]
    for clause, value in (("line_id=?", line_id), ("window_start>=?", start), ("window_end<=?", end)):
        if value is not None:
            where.append(clause)
            values.append(value)
    with snapshot() as connection:
        windows = [] if connection is None else [dict(row) for row in connection.execute(
            "SELECT * FROM windows WHERE " + " AND ".join(where)
            + " ORDER BY window_start, line_id LIMIT 501", values,
        )]
        if len(windows) > 500:
            raise HTTPException(413, "Over 500 windows selected; narrow the time range or select a station")
        alerts = []
        if windows:
            keys = [row["window_key"] for row in windows]
            placeholders = ",".join("?" for _ in keys)
            alerts = [dict(row) for row in connection.execute(
                f"SELECT * FROM alerts WHERE window_key IN ({placeholders}) ORDER BY updated_at DESC LIMIT 1001",
                keys,
            )]
            if len(alerts) > 1000:
                raise HTTPException(413, "Over 1000 rule results selected; narrow the time range")
    stations = {}
    for row in windows:
        station = stations.setdefault(row["line_id"], {
            "line_id": row["line_id"], "inspections": 0, "rejects": 0, "updated_at": row["updated_at"],
        })
        station["inspections"] += row["inspections"]
        station["rejects"] += row["rejects"]
        station["updated_at"] = max(station["updated_at"], row["updated_at"])
    for station in stations.values():
        station["accepted"] = station["inspections"] - station["rejects"]
        station["reject_rate"] = station["rejects"] / station["inspections"] if station["inspections"] else None
    return {
        "state": "missing" if connection is None else "ready" if windows else "empty",
        "response_time": datetime.now(timezone.utc).isoformat(),
        "updated_at": max((row["updated_at"] for row in windows), default=None),
        "stations": list(stations.values()), "windows": windows, "alerts": alerts,
    }
