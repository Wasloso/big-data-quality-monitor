"""Small JSON telemetry helpers shared by the deliberate C14 commands."""

import json
import math
import threading
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse


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


def file_log(directory, through=None):
    logs = []
    if directory.is_dir():
        for path in directory.iterdir():
            name = path.name.removesuffix(".compact")
            if name.isdecimal() and (through is None or int(name) <= through):
                logs.append((int(name), path.name.endswith(".compact"), path))
    compacted = max((index for index, compact, _ in logs if compact), default=-1)
    files = {}
    for index, _, path in sorted(logs):
        if index < compacted:
            continue
        for line in path.read_text().splitlines()[1:]:
            entry = json.loads(line)
            name = unquote(urlparse(entry["path"]).path)
            if entry.get("action", "add") == "add":
                files[name] = entry
            else:
                files.pop(name, None)
    return files


def stage_files(root, stage, committed_offset=None):
    output = "raw" if stage == "q1" else "clean" if stage == "q2" else None
    written = file_log(root / output / "_spark_metadata") if output else {}
    result = {"output_files": len(written), "output_bytes": sum(row["size"] for row in written.values())}
    if stage != "q1":
        source = "raw" if stage == "q2" else "clean"
        available = file_log(root / source / "_spark_metadata")
        offset = -1 if committed_offset is None else committed_offset["logOffset"]
        consumed = file_log(root / stage / "sources/0", through=offset)
        pending = available.keys() - consumed.keys()
        result.update({"input_files": len(available), "consumed_input_files": len(available.keys() & consumed.keys()),
                       "pending_input_files": len(pending),
                       "pending_input_bytes": sum(available[name]["size"] for name in pending)})
    return result
