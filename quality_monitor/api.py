"""Read-only HTTP access to the local quality serving database."""

import os
import json
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from quality_monitor.events import validate_station_context
from quality_monitor.producer_status import read_statuses


app = FastAPI(title="Quality monitor API")
DATABASE = Path(os.environ.get("QUALITY_DATABASE", "/data/serving/quality-v2.sqlite"))
STATIONS_DATABASE = Path(os.environ.get("STATIONS_DATABASE", "/data/serving/stations-v2.sqlite"))
IMAGE_ROOT = Path(os.environ["INSPECTION_IMAGE_ROOT"]) if os.environ.get("INSPECTION_IMAGE_ROOT") else None

STATUS_ROOT = Path(os.environ["PRODUCER_STATUS_ROOT"]) if os.environ.get("PRODUCER_STATUS_ROOT") else None


@contextmanager
def snapshot(database=None):
    database = DATABASE if database is None else database
    if not database.is_file():
        yield None
        return
    try:
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            yield connection
    except sqlite3.Error as error:
        raise HTTPException(503, "Serving database is unavailable or not initialized") from error


def status_snapshot(filters=None):
    try:
        return read_statuses(STATUS_ROOT, filters)
    except OverflowError as error:
        raise HTTPException(413, str(error)) from error
    except OSError as error:
        raise HTTPException(503, "Producer-status storage is unavailable") from error


def detector_filters(category, model_version, threshold_version):
    values = {"category": category, "model_version": model_version, "threshold_version": threshold_version}
    if any(value is not None for value in values.values()) and any(value is None for value in values.values()):
        raise HTTPException(422, "Provide category, model_version and threshold_version together, or omit all three")
    return {name: value for name, value in values.items() if value is not None}


@app.get("/api/producer-status")
def producer_status(
    run_id: str = Query(min_length=1, max_length=200),
    category: str | None = Query(default=None, min_length=1, max_length=200),
    model_version: str | None = Query(default=None, min_length=1, max_length=200),
    threshold_version: str | None = Query(default=None, min_length=1, max_length=200),
    line_id: str | None = Query(default=None, min_length=1, max_length=200),
):
    detector = detector_filters(category, model_version, threshold_version)
    return status_snapshot({"run_id": run_id, **detector, "station": line_id})


@app.get("/api/runs")
def runs():
    with snapshot() as connection:
        rows = [] if connection is None else connection.execute("""
            SELECT run_id, category, model_version, threshold_version, MAX(updated_at) AS updated_at
            FROM windows GROUP BY run_id, category, model_version, threshold_version
            ORDER BY updated_at DESC, run_id LIMIT 101
        """).fetchall()
    contexts = {tuple(row[name] for name in ("run_id", "category", "model_version", "threshold_version")): dict(row) for row in rows}
    try:
        status = status_snapshot()
    except HTTPException:
        status = {"state": "unavailable", "sessions": []}
    for session in status["sessions"]:
        key = tuple(session[name] for name in ("run_id", "category", "model_version", "threshold_version"))
        current = contexts.get(key)
        if current is None or session["updated_at"] > current["updated_at"]:
            contexts[key] = {name: session[name] for name in ("run_id", "category", "model_version", "threshold_version", "updated_at")}
    listed = sorted(contexts.values(), key=lambda row: (row["updated_at"], row["run_id"]), reverse=True)
    return {
        "state": "ready" if listed else "missing" if connection is None else "empty",
        "runs": listed[:100], "truncated": len(listed) > 100,
        "producer_status_state": status["state"],
    }


def utc_bound(value, timespec="auto"):
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(422, "Time filters must include a UTC offset")
    return value.astimezone(timezone.utc).isoformat(timespec=timespec)


def decoded_context(value):
    if value is None:
        return None
    try:
        context = json.loads(value)
        validate_station_context(context)
    except (ValueError, TypeError) as error:
        raise HTTPException(503, "Stored station context is unavailable or invalid") from error
    return context


def station_observations(run_id, category, model_version, threshold_version):
    try:
        with snapshot(STATIONS_DATABASE) as connection:
            if connection is None:
                return {}, "missing", None
            detector = detector_filters(category, model_version, threshold_version)
            where = ["run_id=?", *(f"{name}=?" for name in detector)]
            rows = connection.execute(
                "SELECT * FROM station_latest WHERE " + " AND ".join(where) + " LIMIT 501",
                [run_id, *detector.values()],
            ).fetchall()
            if len(rows) > 500:
                raise HTTPException(413, "Over 500 station observations selected")
            batch = connection.execute("SELECT MAX(processed_at) FROM processed_batches").fetchone()[0]
        observations = {tuple(row[name] for name in ("line_id", "category", "model_version", "threshold_version")): {
            "event_time": row["event_time"], "inspection_id": row["inspection_id"],
            "station_context": decoded_context(dict(row).get("station_context")),
        } for row in rows}
        return observations, "ready", batch
    except (HTTPException, ValueError):
        # The independent observation source must not hide valid quality-window totals.
        return {}, "unavailable", None


@app.get("/api/overview")
def overview(
    run_id: str = Query(min_length=1, max_length=200),
    category: str | None = Query(default=None, min_length=1, max_length=200),
    model_version: str | None = Query(default=None, min_length=1, max_length=200),
    threshold_version: str | None = Query(default=None, min_length=1, max_length=200),
    line_id: str | None = Query(default=None, min_length=1, max_length=200),
    from_time: datetime | None = None,
    to_time: datetime | None = None,
):
    start, end = utc_bound(from_time), utc_bound(to_time)
    if from_time is not None and to_time is not None and from_time >= to_time:
        raise HTTPException(422, "Time range must have a start before its end")
    detector = detector_filters(category, model_version, threshold_version)
    where = ["run_id=?", *(f"{name}=?" for name in detector)]
    values = [run_id, *detector.values()]
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
    # Each station has one selected detector; snapshots must be disjoint to sum counts.
    contexts, last_end = {}, {}
    for row in windows:
        line = row["line_id"]
        context = tuple(row[name] for name in ("category", "model_version", "threshold_version"))
        if line in contexts and contexts[line] != context:
            raise HTTPException(409, f"Multiple detector contexts for station {line}; select an explicit station/detector or use a separate run")
        contexts[line] = context
        start_at = datetime.fromisoformat(row["window_start"])
        end_at = datetime.fromisoformat(row["window_end"])
        if line in last_end and start_at < last_end[line]:
            raise HTTPException(409, f"Overlapping quality windows for station {line}; use a single window contract")
        last_end[line] = end_at
    stations = {}
    for row in windows:
        station = stations.setdefault(row["line_id"], {
            "line_id": row["line_id"], **{name: row[name] for name in ("category", "model_version", "threshold_version")}, "inspections": 0, "rejects": 0, "updated_at": row["updated_at"],
        })
        station["inspections"] += row["inspections"]
        station["rejects"] += row["rejects"]
        station["updated_at"] = max(station["updated_at"], row["updated_at"])
    observations, observation_state, observation_batch = station_observations(run_id, category, model_version, threshold_version)
    for station in stations.values():
        station["latest_observation"] = observations.get(tuple(station[name] for name in ("line_id", "category", "model_version", "threshold_version")))
        station["accepted"] = station["inspections"] - station["rejects"]
        station["reject_rate"] = station["rejects"] / station["inspections"] if station["inspections"] else None
    return {
        "state": "missing" if connection is None else "ready" if windows else "empty",
        "response_time": datetime.now(timezone.utc).isoformat(),
        "updated_at": max((row["updated_at"] for row in windows), default=None),
        "scope": "detector" if detector else "run",
        "stations": list(stations.values()), "windows": windows, "alerts": alerts,
        "observation_state": observation_state, "observation_batch_at": observation_batch,
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
        row["station_context"] = decoded_context(row.get("station_context"))
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
