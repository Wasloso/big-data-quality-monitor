"""Publish a finite synthetic inspection run; image inference comes later."""

import argparse
import json
import math
import time
from datetime import datetime, timezone
from uuid import uuid4

from quality_monitor.events import validate_event
from quality_monitor.sample import progress_events, sample_events


def prepare_events(run_id):
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be non-empty")
    for event in sample_events():
        event["run_id"] = run_id
        event["inspection_id"] = f"{run_id}:{event['inspection_id']}"
        event["product_id"] = f"{run_id}:{event['product_id']}"
        yield event


def positive_rate(value):
    rate = float(value)
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("rate must be a positive finite number")
    return rate


def publish_events(producer, events, topic, rate=None):
    if rate is not None:
        rate = positive_rate(rate)
    delivered = 0
    errors = []

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    queued = 0
    started = time.monotonic()
    for original in events:
        if rate is not None:
            # Fixed schedule: time spent publishing does not add to every interval.
            delay = started + queued / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        event = {**original, "published_at": datetime.now(timezone.utc).isoformat()}
        validate_event(event)
        producer.produce(
            topic,
            key=event["line_id"].encode(),
            value=json.dumps(event, allow_nan=False).encode(),
            on_delivery=on_delivery,
        )
        queued += 1
        producer.poll(0)
    remaining = producer.flush(40)
    if errors or remaining or delivered != queued:
        raise RuntimeError(
            f"Kafka delivery failed: {delivered}/{queued} acknowledged, "
            f"{remaining} pending; errors={errors}"
        )
    return delivered


def make_producer(bootstrap_servers):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 30000,
    })


def argument_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="inspections")
    return parser


def main():
    parser = argument_parser(__doc__)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--progress-at", default=None, metavar="UTC_TIMESTAMP",
        help="Publish only progress controls for an explicit run ID, after inspections have drained",
    )
    parser.add_argument(
        "--rate", type=positive_rate, default=None,
        help="Target inspections per second across all lines; omit for an unpaced replay",
    )
    args = parser.parse_args()
    if args.progress_at and not args.run_id:
        parser.error("--progress-at requires the original --run-id")
    run_id = args.run_id or str(uuid4())
    events = prepare_events(run_id)
    if args.progress_at:
        try:
            events = list(progress_events(events, args.progress_at))
        except ValueError as error:
            parser.error(str(error))
    delivered = publish_events(
        make_producer(args.bootstrap_servers), events, args.topic,
        rate=args.rate,
    )
    kind = "progress records" if args.progress_at else "inspections"
    print(f"Published {delivered} {kind} to {args.topic}; run_id={run_id}")


if __name__ == "__main__":
    main()
