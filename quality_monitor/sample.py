"""Print a repeatable inspection fixture as JSON Lines."""

import json
from datetime import datetime, timedelta, timezone

from quality_monitor.events import validate_event


def progress_events(events, event_time):
    """Make one distinct control per active quality group after upstream drain."""
    groups = set()
    for event in events:
        group = tuple(event[name] for name in (
            "run_id", "line_id", "category", "model_version", "threshold_version",
        ))
        if group in groups:
            continue
        groups.add(group)
        control = {**event, "record_kind": "progress", "event_time": event_time}
        control["inspection_id"] = "progress:" + json.dumps([*group, event_time], separators=(",", ":"))
        for name in ("product_id", "image_id", "image_uri", "anomaly_score", "is_reject"):
            control[name] = None
        validate_event(control)
        yield control


def sample_events():
    start = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
    for second in range(60):
        for line_id, reject_every in (("A", 10), ("B", 5)):
            is_reject = second % reject_every == 0
            timestamp = (start + timedelta(seconds=second)).isoformat()
            event = {
                "schema_version": 1,
                "record_kind": "inspection",
                "inspection_id": f"sample-{line_id}-{second:03d}",
                "run_id": "fixture-001",
                "scenario_id": "two-lines",
                "product_id": f"product-{line_id}-{second:03d}",
                "image_id": f"placeholder-{second % 10:02d}",
                "image_uri": f"fixtures/images/{second % 10:02d}.png",
                "line_id": line_id,
                "category": "bottle",
                "event_time": timestamp,
                # Fixture time only; the Kafka producer will set the real time.
                "published_at": timestamp,
                "anomaly_score": 0.9 if is_reject else 0.1,
                "is_reject": is_reject,
                "model_version": "fixture-v1",
                "threshold_version": "fixture-0.5",
            }
            validate_event(event)
            yield event


if __name__ == "__main__":
    for event in sample_events():
        print(json.dumps(event, allow_nan=False))
