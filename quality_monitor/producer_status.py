"""Read bounded local producer-session snapshots without altering them."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quality_monitor.events import finite_number


COUNTERS = ("attempted", "completed", "queued", "published", "inference_errors", "enqueue_errors", "delivery_errors", "pending")
IDENTITY = ("run_id", "station", "session_id", "category", "model_version", "threshold_version")
MAX_FILES = 200
MAX_BYTES = 16 * 1024
MAX_SESSIONS = 100


def utc_time(value):
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("Status timestamps must be UTC")
    return timestamp


def session_record(document, now):
    if type(document.get("schema_version", 1)) is not int or document.get("schema_version", 1) != 1:
        raise ValueError("Unknown producer-status schema")
    for name in IDENTITY:
        if not isinstance(document.get(name), str) or not document[name].strip():
            raise ValueError("Missing session identity")
    if document.get("state") not in ("running", "stopping", "stopped", "failed"):
        raise ValueError("Unknown producer state")
    for name in COUNTERS:
        if type(document.get(name)) is not int or document[name] < 0:
            raise ValueError("Invalid producer counter")
    if not finite_number(document.get("target_rate")) or document["target_rate"] <= 0 or not finite_number(document.get("threshold")):
        raise ValueError("Invalid producer rate/threshold")
    if not (document["completed"] + document["inference_errors"] <= document["attempted"]
            and document["queued"] + document["enqueue_errors"] <= document["completed"]
            and document["published"] + document["delivery_errors"] + document["pending"] == document["queued"]):
        raise ValueError("Inconsistent producer counters")
    started, updated = utc_time(document["started_at"]), utc_time(document["updated_at"])
    if updated < started:
        raise ValueError("Status update predates the session")
    ended = document.get("stopped_at")
    if document["state"] in ("stopped", "failed") and (ended is None or utc_time(ended) < started):
        raise ValueError("Terminal session needs its recorded end time")
    if ended is not None and document["state"] not in ("stopped", "failed"):
        raise ValueError("Active session cannot have an end time")
    error = document.get("last_error")
    if error is not None and not isinstance(error, str):
        raise ValueError("Invalid error text")
    seconds = (now - updated).total_seconds()
    stale_after = max(30.0, 2 / document["target_rate"] + 10)
    if not finite_number(stale_after):
        raise ValueError("Unrepresentable status freshness interval")
    freshness = "clock_skew" if seconds < -5 else "ended" if ended else "stale" if seconds > stale_after else "recent"
    return {
        **{name: document[name] for name in (*IDENTITY, *COUNTERS, "state", "target_rate", "threshold")},
        "started_at": started.isoformat(), "updated_at": updated.isoformat(), "stopped_at": ended,
        "work_order": document.get("work_order"), "operation": document.get("operation"),
        "last_error": error[:2000] if error else None,
        "status_age_seconds": max(0, seconds), "stale_after_seconds": stale_after, "freshness": freshness,
    }


def read_statuses(root, filters=None, now=None):
    now = now or datetime.now(timezone.utc)
    result = {"state": "disabled", "response_time": now.isoformat(), "sessions": [], "invalid_files": 0}
    if root is None:
        return result
    root = Path(root).resolve()
    if not root.exists():
        return {**result, "state": "missing"}
    files = []
    for path in root.iterdir():
        if path.suffix == ".json":
            files.append(path)
            if len(files) > MAX_FILES:
                raise OverflowError("Over 200 status files; archive older snapshots outside the configured directory")
    sessions, invalid = [], 0
    for path in files:
        try:
            if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
                raise ValueError("Status files must stay inside their configured directory")
            with path.open("rb") as handle:
                payload = handle.read(MAX_BYTES + 1)
            if len(payload) > MAX_BYTES:
                raise ValueError("Oversized status file")
            session = session_record(json.loads(payload), now)
            if filters and any(value is not None and session[name] != value for name, value in filters.items()):
                continue
            sessions.append(session)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            invalid += 1
    if len(sessions) > MAX_SESSIONS:
        raise OverflowError("Over 100 matching producer sessions; narrow the selection or archive older snapshots")
    sessions.sort(key=lambda row: (row["started_at"], row["session_id"]), reverse=True)
    groups = set()
    for session in sessions:
        group = tuple(session[name] for name in ("run_id", "station", "category", "model_version", "threshold_version"))
        session["latest_started_session"] = group not in groups
        groups.add(group)
    return {**result, "state": "partial" if invalid else "ready" if sessions else "empty", "sessions": sessions, "invalid_files": invalid}
