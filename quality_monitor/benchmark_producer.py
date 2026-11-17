"""Open-loop fixture-score publication; does not wait for Spark or run inference."""

import argparse
import json
import importlib.metadata
import math
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from quality_monitor.benchmark_metrics import emit, load_segments, utc_now
from quality_monitor.events import validate_event
from quality_monitor.producer import make_producer, positive_rate
from quality_monitor.sample import sample_events


def main():
    from confluent_kafka import Consumer, TopicPartition

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--rate", type=positive_rate, default=20)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--profile", choices=("steady", "burst", "skew", "duplicate-late"), default="steady")
    parser.add_argument("--burst-multiplier", type=positive_rate, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--max-events", type=int, default=1000000)
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    if not math.isfinite(args.warmup) or args.warmup < 0 or not math.isfinite(args.duration) or args.duration <= 0:
        parser.error("Use finite nonnegative warmup and positive duration")
    if args.payload_bytes < 0 or args.max_events < 1:
        parser.error("Payload padding must be nonnegative and event limit positive")
    segments = load_segments(args.rate, args.warmup, args.duration, args.profile, args.burst_multiplier)
    planned = sum(math.ceil(length * rate) for _, _, length, rate in segments)
    if planned > args.max_events:
        parser.error("Planned load exceeds --max-events; raise the explicit bound deliberately")
    producer = make_producer(args.bootstrap_servers)
    consumer = Consumer({"bootstrap.servers": args.bootstrap_servers,
                         "group.id": args.run_id + "-metrics", "enable.auto.commit": False})
    metadata = consumer.list_topics(args.topic, timeout=20).topics[args.topic]
    if metadata.error:
        raise RuntimeError(str(metadata.error))
    partitions = list(metadata.partitions)
    counts = {phase: {"attempted": 0, "queued": 0, "acknowledged": 0, "unique_acknowledged": 0,
                      "duplicates": 0, "delayed": 0, "errors": 0}
              for phase in ("warmup", "measurement", "bootstrap")}
    template = next(sample_events())
    rng = random.Random(args.seed)
    recent = deque(maxlen=100)
    line_counts = {phase: {"A": 0, "B": 0} for phase in counts}
    started = time.monotonic()
    started_wall = time.time()
    event_origin = datetime(2026, 11, 7, 12, tzinfo=timezone.utc).timestamp()
    last_report = started - 1
    overdue_max = 0.0
    errors = []

    def report(final=False):
        nonlocal last_report
        heads = {str(partition): consumer.get_watermark_offsets(
            TopicPartition(args.topic, partition), timeout=10, cached=False,
        )[1] for partition in partitions}
        value = {"kind": "producer", "observed_at": utc_now(), "counts": counts,
                 "observed_monotonic": time.monotonic(), "end_offsets": heads, "final": final}
        emit(value)
        last_report = time.monotonic()

    def send(event, phase, duplicate, delayed, scheduled_at, journal):
        nonlocal overdue_max
        state = counts[phase]
        state["attempted"] += 1
        event = {**event, "published_at": utc_now()}
        event["benchmark_published_monotonic"] = time.monotonic()
        validate_event(event)
        overdue_max = max(overdue_max, time.monotonic() - scheduled_at)
        def delivered(error, message):
            if error is not None:
                state["errors"] += 1
                errors.append(str(error))
            else:
                state["acknowledged"] += 1
                state["unique_acknowledged"] += int(not duplicate)
        producer.produce(args.topic, key=event["line_id"].encode(), value=json.dumps(event).encode(), on_delivery=delivered)
        state["queued"] += 1
        state["duplicates"] += int(duplicate)
        state["delayed"] += int(delayed)
        journal.write(json.dumps({"phase": phase, "duplicate": duplicate, "delayed": delayed,
                                 "published_monotonic": event["benchmark_published_monotonic"], **{
            name: event[name] for name in ("inspection_id", "run_id", "line_id", "event_time", "published_at", "is_reject")
        }}) + "\n")
        producer.poll(0)

    summary = {"run_id": args.run_id, "profile": args.profile, "seed": args.seed,
               "started_at": datetime.fromtimestamp(started_wall, timezone.utc).isoformat(),
               "measurement_start_epoch": started_wall + args.warmup,
               "measurement_end_epoch": started_wall + args.warmup + args.duration,
               "target_rate": args.rate, "segments": segments, "counts": counts,
               "track": "infrastructure replay with explicit fixture scores", "result": "in_progress"}
    summary["synthetic_event_time_origin"] = datetime.fromtimestamp(event_origin, timezone.utc).isoformat()
    summary["kafka_partitions"] = len(partitions)
    summary["client_version"] = importlib.metadata.version("confluent-kafka")
    summary.update({"started_monotonic": started, "measurement_start_monotonic": started + args.warmup,
                    "measurement_end_monotonic": started + args.warmup + args.duration})
    journal_path = args.root / ("bootstrap.jsonl" if args.bootstrap else "publications.jsonl")
    summary_path = args.root / ("bootstrap.json" if args.bootstrap else "producer.json")
    try:
        emit({"kind": "clock", "observed_monotonic": time.monotonic(), "observed_at": utc_now()})
        with journal_path.open("x") as journal:
            if args.bootstrap:
                for index in range(2):
                    event = {**template, "line_id": "A" if index == 0 else "B",
                             "run_id": args.run_id + "-bootstrap", "inspection_id": f"bootstrap-{index}",
                             "product_id": f"bootstrap-product-{index}",
                             "event_time": datetime.fromtimestamp(event_origin, timezone.utc).isoformat()}
                    send(event, "bootstrap", False, False, started, journal)
            else:
                index = 0
                previous_phase = None
                for phase, offset, length, rate in segments:
                    if phase != previous_phase:
                        recent.clear()
                        previous_phase = phase
                    for slot in range(math.ceil(length * rate)):
                        scheduled = started + offset + slot / rate
                        while time.monotonic() < scheduled:
                            producer.poll(max(0, min(.05, scheduled - time.monotonic())))
                        phase_run = args.run_id if phase == "measurement" else args.run_id + "-warmup"
                        duplicate = args.profile == "duplicate-late" and index % 10 == 9 and bool(recent)
                        delayed = args.profile == "duplicate-late" and index % 20 == 15 and not duplicate
                        if duplicate:
                            event = dict(recent[-1])
                        else:
                            line = "A" if (rng.random() < .9 if args.profile == "skew" else index % 2 == 0) else "B"
                            reject = line_counts[phase][line] % (10 if line == "A" else 5) == 0
                            line_counts[phase][line] += 1
                            event = {**template, "run_id": phase_run, "inspection_id": f"{phase_run}:{index}",
                                     "product_id": f"product:{phase_run}:{index}", "line_id": line,
                                     "scenario_id": "benchmark-" + args.profile, "model_version": "fixture-benchmark-v1",
                                     "event_time": datetime.fromtimestamp(event_origin + offset + slot / rate - (180 if delayed else 0), timezone.utc).isoformat(),
                                     "anomaly_score": .9 if reject else .1, "is_reject": reject,
                                     "padding": "x" * args.payload_bytes}
                            recent.append(event)
                        send(event, phase, duplicate, delayed, scheduled, journal)
                        index += 1
                        # Offset sampling is deliberately outside the publication schedule's clock.
                        # Its generator cost is included and scheduling overrun is reported.
                        if time.monotonic() - last_report >= 1:
                            report()
        remaining = producer.flush(40)
        report(final=True)
        if remaining or errors or any(state["queued"] != state["acknowledged"] for state in counts.values()):
            raise RuntimeError(f"Unacknowledged publication: remaining={remaining}, delivery_errors={len(errors)}")
        summary["result"] = "passed"
    except Exception as error:
        summary.update({"result": "failed", "error": f"{type(error).__name__}: {error}"})
        raise
    finally:
        elapsed = time.monotonic() - started
        summary.update({"finished_at": utc_now(), "elapsed_seconds": elapsed,
                        "utc_elapsed_minus_monotonic_seconds": time.time() - started_wall - elapsed,
                        "maximum_schedule_overrun_seconds": overdue_max})
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        consumer.close()
        emit({"kind": "producer-summary", **summary})


if __name__ == "__main__":
    main()
