"""Check a synthetic run through a real Kafka broker against fixture totals."""

import json
import time
from pathlib import Path
from uuid import uuid4

from quality_monitor.events import parse_event
from quality_monitor.producer import (
    argument_parser, make_producer, prepare_events, publish_events,
)


def main():
    from confluent_kafka import Consumer, TopicPartition

    args = argument_parser(__doc__).parse_args()
    run_id = str(uuid4())
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": f"kafka-check-{run_id}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(args.topic, timeout=20)
        topic = metadata.topics[args.topic]
        if topic.error:
            raise RuntimeError(str(topic.error))
        # Read only data appended after this check starts, on every partition.
        starts = []
        for partition in topic.partitions:
            tp = TopicPartition(args.topic, partition)
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            starts.append(TopicPartition(args.topic, partition, high))
        consumer.assign(starts)
        events = list(prepare_events(run_id))
        publish_events(make_producer(args.bootstrap_servers), events, args.topic)
        received = {}
        counts = {}
        deadline = time.monotonic() + 60
        while len(received) < len(events) and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            event = parse_event(message.value())
            if event["run_id"] != run_id:
                continue
            if message.key() != event["line_id"].encode():
                raise RuntimeError("Kafka key differs from line_id")
            identity = event["inspection_id"]
            if identity in received:
                raise RuntimeError(f"Unexpected duplicate: {identity}")
            received[identity] = event
            line = counts.setdefault(event["line_id"], {"inspections": 0, "rejects": 0})
            line["inspections"] += 1
            line["rejects"] += int(event["is_reject"])
        if set(received) != {event["inspection_id"] for event in events}:
            raise RuntimeError(f"Expected 120 inspections, received {len(received)}")
        for line in counts.values():
            line["reject_rate"] = line["rejects"] / line["inspections"]
        fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/expected_counts.json"
        if counts != json.loads(fixture.read_text()):
            raise RuntimeError(f"Unexpected totals: {counts}")
        print(f"Kafka check passed: 120 inspections; {counts}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
