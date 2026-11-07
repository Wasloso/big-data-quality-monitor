import json
import math
from datetime import datetime, timedelta


TEXT_FIELDS = (
    "inspection_id", "run_id", "scenario_id", "product_id", "image_id",
    "image_uri", "line_id", "category", "model_version", "threshold_version",
)


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("An inspection must be a JSON object")
    if type(event.get("schema_version")) is not int or event["schema_version"] != 1:
        raise ValueError("schema_version must be 1")
    if event.get("record_kind") != "inspection":
        raise ValueError("Only inspection records are supported for now")

    for name in TEXT_FIELDS:
        value = event.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")

    for name in ("event_time", "published_at"):
        value = event.get(name)
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a UTC timestamp")
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"{name} must be a UTC timestamp") from error
        if timestamp.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must include a UTC offset")

    score = event.get("anomaly_score")
    if type(score) not in (int, float):
        raise ValueError("anomaly_score must be a finite number")
    try:
        finite = math.isfinite(score)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError("anomaly_score must be a finite number")
    if type(event.get("is_reject")) is not bool:
        raise ValueError("is_reject must be a boolean")


def parse_event(line):
    event = json.loads(line)
    validate_event(event)
    return event
