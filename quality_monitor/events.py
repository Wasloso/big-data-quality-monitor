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
    if event.get("record_kind") not in ("inspection", "progress"):
        raise ValueError("record_kind must be inspection or progress")

    for name in TEXT_FIELDS:
        if event["record_kind"] == "progress" and name in ("product_id", "image_id", "image_uri"):
            if event.get(name) is not None:
                raise ValueError(f"{name} must be null or absent for progress records")
            continue
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

    if event["record_kind"] == "progress":
        if event.get("station_context") is not None:
            raise ValueError("Progress records must not contain station observations")
        if event.get("anomaly_score") is not None or event.get("is_reject") is not None:
            raise ValueError("Progress records must not contain predictions")
        return

    context = event.get("station_context")
    if context is not None:
        validate_station_context(context)
    if not finite_number(event.get("anomaly_score")):
        raise ValueError("anomaly_score must be a finite number")
    if type(event.get("is_reject")) is not bool:
        raise ValueError("is_reject must be a boolean")


def finite_number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def validate_station_context(context):
    if not isinstance(context, dict) or type(context.get("version")) is not int or context["version"] != 1:
        raise ValueError("station_context must use version 1")
    if context.get("simulated") is not True:
        raise ValueError("Station readings must be explicitly simulated")
    for name in ("work_order", "operation"):
        if not isinstance(context.get(name), str) or not context[name].strip():
            raise ValueError(f"station_context.{name} must be nonempty")
    if context.get("reported_state") not in ("running", "idle", "maintenance"):
        raise ValueError("Unknown reported station state")
    for name in ("temperature_c", "conveyor_speed_m_min"):
        if not finite_number(context.get(name)):
            raise ValueError(f"station_context.{name} must be finite numeric data")
    if context["conveyor_speed_m_min"] < 0:
        raise ValueError("Conveyor speed must be nonnegative")


def parse_event(line):
    event = json.loads(line)
    validate_event(event)
    return event
