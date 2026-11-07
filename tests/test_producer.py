import json
import unittest
from datetime import datetime, timezone

from quality_monitor.producer import prepare_events, publish_events


class FakeProducer:
    def __init__(self, error=None, pending=0):
        self.error = error
        self.pending = pending
        self.messages = []

    def produce(self, topic, **kwargs):
        self.messages.append((topic, kwargs))

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        for _, message in self.messages:
            message["on_delivery"](self.error, None)
        return self.pending


class ProducerTests(unittest.TestCase):
    def test_new_runs_have_distinct_inspection_and_product_ids(self):
        first = list(prepare_events("first"))
        second = list(prepare_events("second"))
        self.assertEqual(len(first), 120)
        for name in ("inspection_id", "product_id"):
            self.assertTrue({e[name] for e in first}.isdisjoint(e[name] for e in second))
        self.assertEqual(first, list(prepare_events("first")))
        with self.assertRaises(ValueError):
            list(prepare_events(" "))

    def test_publication_sets_wall_clock_and_preserves_original(self):
        event = next(prepare_events("test"))
        original = dict(event)
        producer = FakeProducer()
        before = datetime.now(timezone.utc)
        self.assertEqual(publish_events(producer, [event], "inspections"), 1)
        after = datetime.now(timezone.utc)
        topic, message = producer.messages[0]
        payload = json.loads(message["value"])
        self.assertEqual(topic, "inspections")
        self.assertEqual(message["key"], b"A")
        published = datetime.fromisoformat(payload.pop("published_at"))
        self.assertLessEqual(before, published)
        self.assertLessEqual(published, after)
        self.assertEqual(payload, {k: v for k, v in event.items() if k != "published_at"})
        self.assertEqual(event, original)

    def test_delivery_failures_and_pending_messages_fail_the_run(self):
        for producer in (FakeProducer(error="broker failed"), FakeProducer(pending=1)):
            with self.subTest(producer=producer), self.assertRaises(RuntimeError):
                publish_events(producer, prepare_events("failure"), "inspections")


if __name__ == "__main__":
    unittest.main()
