"""Read-only HTTP access to the local quality serving database."""

import os
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse


app = FastAPI(title="Quality monitor API")
DATABASE = Path(os.environ.get("QUALITY_DATABASE", "/data/serving/quality.sqlite"))
STATIONS_DATABASE = Path(os.environ.get("STATIONS_DATABASE", "/data/serving/stations.sqlite"))
IMAGE_ROOT = Path(os.environ["INSPECTION_IMAGE_ROOT"]) if os.environ.get("INSPECTION_IMAGE_ROOT") else None


@contextmanager
def snapshot(database=None):
    database = DATABASE if database is None else database
    if not database.is_file():
        yield None
        return
    try:
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", timeout=5)) as connection:
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


def utc_bound(value, timespec="auto"):
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(422, "Time filters must include a UTC offset")
    return value.astimezone(timezone.utc).isoformat(timespec=timespec)


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


@app.get("/api/inspections")
def inspections(
    run_id: str = Query(min_length=1, max_length=200),
    line_id: str = Query(min_length=1, max_length=200),
    category: str = Query(min_length=1, max_length=200),
    model_version: str = Query(min_length=1, max_length=200),
    threshold_version: str = Query(min_length=1, max_length=200),
    limit: int = Query(default=50, ge=1, le=100),
    before_time: datetime | None = None,
    before_id: str | None = Query(default=None, min_length=1, max_length=200),
    from_time: datetime | None = None,
    to_time: datetime | None = None,
):
    if (before_time is None) != (before_id is None):
        raise HTTPException(422, "Provide both before_time and before_id for pagination")
    before = utc_bound(before_time, timespec="microseconds")
    start, end = utc_bound(from_time, timespec="microseconds"), utc_bound(to_time, timespec="microseconds")
    if from_time is not None and to_time is not None and from_time >= to_time:
        raise HTTPException(422, "Time range must have a start before its end")
    where = "run_id=? AND line_id=? AND category=? AND model_version=? AND threshold_version=?"
    values = [run_id, line_id, category, model_version, threshold_version]
    latest, batch_time, retained = None, None, 0
    with snapshot(STATIONS_DATABASE) as connection:
        rows = []
        if connection is not None:
            latest_row = connection.execute("SELECT * FROM station_latest WHERE " + where, values).fetchone()
            latest = dict(latest_row) if latest_row else None
            for clause, value in (("event_time>=?", start), ("event_time<?", end)):
                if value is not None:
                    where += " AND " + clause
                    values.append(value)
            retained = connection.execute("SELECT COUNT(*) FROM inspection_samples WHERE " + where, values).fetchone()[0]
            batch_time = connection.execute("SELECT MAX(processed_at) FROM processed_batches").fetchone()[0]
            if before is not None:
                where += " AND (event_time, inspection_id) < (?, ?)"
                values.extend([before, before_id])
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM inspection_samples WHERE " + where
                + " ORDER BY event_time DESC, inspection_id DESC LIMIT ?", [*values, limit + 1],
            )]
    more = len(rows) > limit
    rows = rows[:limit]
    for row in rows + ([latest] if latest else []):
        row["is_reject"] = bool(row["is_reject"])
        row["image_url"] = "/api/inspection-image?" + urlencode({
            "run_id": row["run_id"], "inspection_id": row["inspection_id"],
        }) if IMAGE_ROOT is not None else None
    return {
        "state": "missing" if connection is None else "ready" if rows else "empty",
        "response_time": datetime.now(timezone.utc).isoformat(),
        "projection_batch_at": batch_time, "history_scope": "retained_samples",
        "retained_count": retained, "latest_inspection": latest, "inspections": rows,
        "next_cursor": {"before_time": rows[-1]["event_time"], "before_id": rows[-1]["inspection_id"]} if more else None,
    }


@app.get("/api/inspection-image")
def inspection_image(
    run_id: str = Query(min_length=1, max_length=200),
    inspection_id: str = Query(min_length=1, max_length=200),
):
    with snapshot(STATIONS_DATABASE) as connection:
        row = None if connection is None else connection.execute(
            "SELECT image_uri FROM inspection_samples WHERE run_id=? AND inspection_id=?", (run_id, inspection_id),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Inspection image is unavailable")
    if IMAGE_ROOT is None:
        raise HTTPException(503, "Inspection image storage is not configured")
    types = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp"}
    try:
        root = IMAGE_ROOT.resolve()
        relative = Path(row["image_uri"])
        path = (root / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(root) or path.suffix.lower() not in types or not path.is_file():
            raise HTTPException(404, "Inspection image is unavailable")
    except (OSError, ValueError, RuntimeError) as error:
        raise HTTPException(404, "Inspection image is unavailable") from error
    return FileResponse(path, media_type=types[path.suffix.lower()], headers={
        "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
    })


# Register API routes first; the built client uses hash links so refresh needs no router fallback.
frontend = Path(__file__).resolve().parent.parent / "dashboard" / "dist"
if frontend.is_dir():
    app.mount("/", StaticFiles(directory=frontend, html=True), name="dashboard")
