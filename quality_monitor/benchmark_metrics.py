"""Small JSON telemetry helpers shared by the deliberate C14 commands."""

import json
import math
import threading
from datetime import datetime, timezone


PREFIX = "BDQM_METRIC "
_OUTPUT_LOCK = threading.Lock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def emit(value):
    with _OUTPUT_LOCK:
        print(PREFIX + json.dumps(value, allow_nan=False), flush=True)


def read_jsonl(path):
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    return {"count": len(values), **{
        name: values[min(len(values) - 1, math.ceil(len(values) * fraction) - 1)]
        for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99))
    }, "max": values[-1]}


def load_segments(rate, warmup, duration, profile, burst_multiplier):
    segments = [("warmup", 0.0, warmup, rate)] if warmup else []
    if profile == "burst":
        third = duration / 3
        segments.extend((
            ("measurement", warmup, third, rate),
            ("measurement", warmup + third, third, rate * burst_multiplier),
            ("measurement", warmup + 2 * third, third, rate),
        ))
    else:
        segments.append(("measurement", warmup, duration, rate))
    return segments
