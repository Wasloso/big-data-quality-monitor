"""Inspect a repeating image sequence and publish real predictions until Ctrl+C."""

import hashlib
import json
import math
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from uuid import uuid4

from quality_monitor.data_manifest import checked_path, encoded
from quality_monitor.events import validate_event
from quality_monitor.producer import argument_parser, make_producer, positive_rate


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_image_list(path, root, category):
    document = json.loads(path.read_text())
    if document.get("category") != category:
        raise ValueError("Image-list category differs from the requested category")
    images = document.get("images")
    if not isinstance(images, list) or not images:
        raise ValueError("Expected a nonempty list of relative image paths")
    for relative in images:
        if not isinstance(relative, str):
            raise ValueError("Image-list entries must be path strings, without labels or scores")
        parts = Path(relative).parts
        if not parts or Path(relative).is_absolute() or ".." in parts or parts[0] != category:
            raise ValueError(f"Image path does not belong to {category}: {relative}")
        checked_path(root, relative)
    return images


def finite_setting(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Use a finite number")
    return number


def simulated_context(args, number):
    return {
        "version": 1, "simulated": True, "work_order": args.work_order, "operation": args.operation,
        "reported_state": "running",
        "temperature_c": round(args.temperature_c + .8 * math.sin(number / 8), 2),
        "conveyor_speed_m_min": round(max(0, args.conveyor_speed_m_min + .2 * math.sin(number / 5)), 2),
    }


def inspection(args, metadata, relative, session, number, acquired_at, score):
    identity = f"{args.run_id}:{args.station}:{session}:{number}"
    event = {
        "schema_version": 1, "record_kind": "inspection",
        "station_context": simulated_context(args, number),
        "inspection_id": identity, "product_id": f"product:{identity}",
        "run_id": args.run_id, "scenario_id": args.scenario,
        "image_id": hashlib.sha256(relative.encode()).hexdigest(), "image_uri": relative,
        "line_id": args.station, "category": args.category,
        "event_time": acquired_at, "published_at": utc_now(),
        "anomaly_score": score, "is_reject": score > metadata["threshold"],
        "model_version": metadata["model_version"], "threshold_version": metadata["threshold_version"],
    }
    validate_event(event)
    return event


def run_station(args, producer, score_image, metadata, images, stop):
    if metadata["category"] != args.category:
        raise ValueError("Model category differs from the requested category")
    threshold = metadata["threshold"]
    if type(threshold) not in (int, float) or not math.isfinite(threshold):
        raise ValueError("Model threshold must be finite")
    if metadata["config"]["operator"] != ">":
        raise ValueError("Unsupported model threshold operator")
    session = uuid4().hex
    path = args.status or Path(f"artifacts/stations/{args.station}-{session}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "run_id": args.run_id, "station": args.station, "session_id": session,
        "category": args.category, "model_version": metadata["model_version"],
        "threshold_version": metadata["threshold_version"], "threshold": metadata["threshold"],
        "work_order": args.work_order, "operation": args.operation,
        "mode": "live-inference", "target_rate": args.rate, "state": "running",
        "started_at": utc_now(), "attempted": 0, "completed": 0, "queued": 0,
        "published": 0, "inference_errors": 0, "delivery_errors": 0, "enqueue_errors": 0,
        "pending": 0, "last_error": None,
    }
    # A restart gets a new status file/session, never resets another process's counters.
    with path.open("x") as handle:
        json.dump(status, handle, indent=2)

    def save():
        status["updated_at"] = utc_now()
        status["pending"] = status["queued"] - status["published"] - status["delivery_errors"]
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(encoded(status))
        temporary.replace(path)

    def delivered(error, message):
        if error is None:
            status["published"] += 1
        else:
            status["delivery_errors"] += 1
            status["last_error"] = str(error)

    print(f"Station {args.station}; run_id={args.run_id}; status={path}", flush=True)
    try:
        save()
        while not stop.is_set() and (args.max_items is None or status["attempted"] < args.max_items):
            started = time.monotonic()
            relative = images[status["attempted"] % len(images)]
            status["attempted"] += 1
            acquired_at = utc_now()
            try:
                score = score_image(relative)
                event = inspection(args, metadata, relative, session, status["attempted"], acquired_at, score)
            except (OSError, ValueError, RuntimeError) as error:
                status["inference_errors"] += 1
                status["last_error"] = f"{relative}: {error}"
                print(f"Inference failed: {status['last_error']}", file=sys.stderr, flush=True)
            else:
                status["completed"] += 1
                try:
                    producer.produce(args.topic, key=args.station.encode(),
                                     value=json.dumps(event, allow_nan=False).encode(), on_delivery=delivered)
                except Exception:
                    status["enqueue_errors"] += 1
                    raise
                status["queued"] += 1
            producer.poll(0)
            save()
            if status["delivery_errors"]:
                raise RuntimeError("Kafka reported a failed delivery")
            if args.max_items is None or status["attempted"] < args.max_items:
                # Do not burst to catch up when inference is slower than the target rate.
                stop.wait(max(0, 1 / args.rate - (time.monotonic() - started)))
    except Exception as error:
        status["state"] = "failed"
        status["last_error"] = str(error)
    finally:
        if status["state"] != "failed":
            status["state"] = "stopping"
        save()
        try:
            remaining = producer.flush(10)
            if remaining or status["delivery_errors"] or status["published"] != status["queued"]:
                raise RuntimeError("Kafka delivery incomplete; inspect published/pending/error counts")
        except Exception as error:
            status["state"] = "failed"
            status["last_error"] = str(error)
        if status["state"] != "failed":
            status["state"] = "stopped"
        status["stopped_at"] = utc_now()
        save()
    print(json.dumps(status, indent=2), flush=True)
    return status


def main():
    parser = argument_parser(__doc__)
    parser.set_defaults(bootstrap_servers="localhost:29092")
    parser.add_argument("--station", required=True, help="Station/line identity, e.g. A")
    parser.add_argument("--category", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scenario", default="continuous-images")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--images", type=Path, required=True, help="Label-free JSON category/image-path list")
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--rate", type=positive_rate, default=1.0)
    parser.add_argument("--work-order", default="demo-work-order")
    parser.add_argument("--operation", default="visual-inspection")
    parser.add_argument("--temperature-c", type=finite_setting, default=35.0, help="Simulated nominal temperature, degrees Celsius")
    parser.add_argument("--conveyor-speed-m-min", type=finite_setting, default=12.0, help="Simulated nominal speed, metres per minute")
    parser.add_argument("--status", type=Path, help="New status file; defaults to artifacts/stations/<station>-<session>.json")
    parser.add_argument("--max-items", type=int, help="Optional attempt limit for a finite integration smoke")
    args = parser.parse_args()
    if any(not value.strip() for value in (args.station, args.category, args.run_id, args.scenario, args.work_order, args.operation)):
        parser.error("Station, category, run ID and scenario must be nonempty")
    if Path(args.station).name != args.station or args.station in (".", ".."):
        parser.error("Station must be a single path-safe name")
    if args.conveyor_speed_m_min < 0:
        parser.error("Conveyor speed must be nonnegative")
    if args.max_items is not None and args.max_items < 1:
        parser.error("--max-items must be positive")
    try:
        images = read_image_list(args.images, args.data_root, args.category)
        # Keep optional ML dependencies out of normal event/sink checks.
        from quality_monitor.vision import load_model, score_image
        model, transform, metadata = load_model(args.model)
        stop = Event()
        for name in (signal.SIGINT, signal.SIGTERM):
            signal.signal(name, lambda signum, frame: stop.set())
        status = run_station(
            args, make_producer(args.bootstrap_servers),
            lambda relative: score_image(model, transform, args.data_root, relative),
            metadata, images, stop,
        )
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"Station error: {error}\n")
    if status["state"] == "failed" or status["inference_errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
