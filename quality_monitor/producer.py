"""Publish a finite synthetic inspection run; image inference comes later."""

import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4

from quality_monitor.events import validate_event
from quality_monitor.sample import sample_events


def prepare_events(run_id):
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be non-empty")
    for event in sample_events():
        event["run_id"] = run_id
        event["inspection_id"] = f"{run_id}:{event['inspection_id']}"
        event["product_id"] = f"{run_id}:{event['product_id']}"
        yield event


def publish_events(producer, events, topic):
    delivered = 0
    errors = []

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    queued = 0
    for original in events:
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
    args = parser.parse_args()
    run_id = args.run_id or str(uuid4())
    delivered = publish_events(
        make_producer(args.bootstrap_servers), prepare_events(run_id), args.topic
    )
    print(f"Published {delivered} inspections to {args.topic}; run_id={run_id}")


if __name__ == "__main__":
    main()
