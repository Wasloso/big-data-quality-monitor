import contextlib
import io
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from types import SimpleNamespace

from quality_monitor.producer import prepare_events, publish_events
from quality_monitor.station import read_image_list, run_station


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


    def test_station_fresh_ids_and_inference_failure_without_fabricated_results(self):
        metadata = {"category": "bottle", "config": {"operator": ">"}, "threshold": 1.5,
                    "model_version": "test-model", "threshold_version": "test-threshold"}
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            args = SimpleNamespace(run_id="station-test", station="A", scenario="test", category="bottle",
                                   status=Path(directory) / "first.json", rate=100000, topic="inspections", max_items=3)
            calls = 0

            def score(relative):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise ValueError("Unreadable image")
                return 2.0

            producer = FakeProducer()
            before = datetime.now(timezone.utc)
            result = run_station(args, producer, score, metadata, ["bottle/test/good/000.png"], Event())
            self.assertEqual((result["attempted"], result["completed"], result["published"], result["inference_errors"]), (3, 2, 2, 1))
            self.assertEqual(result["state"], "stopped")
            first = [json.loads(message["value"]) for _, message in producer.messages]
            self.assertEqual(len({event["inspection_id"] for event in first}), 2)
            self.assertEqual(len({event["image_id"] for event in first}), 1)
            self.assertTrue(all(event["is_reject"] for event in first))
            for event in first:
                self.assertLessEqual(before, datetime.fromisoformat(event["event_time"]))
                self.assertLessEqual(datetime.fromisoformat(event["event_time"]), datetime.fromisoformat(event["published_at"]))
            args.status = Path(directory) / "restart.json"
            args.max_items = 1
            second = FakeProducer()
            run_station(args, second, lambda path: 0.1, metadata, ["bottle/test/good/000.png"], Event())
            restarted = json.loads(second.messages[0][1]["value"])
            self.assertNotIn(restarted["inspection_id"], {event["inspection_id"] for event in first})
            self.assertEqual(restarted["image_id"], first[0]["image_id"])
            self.assertEqual(result, json.loads((Path(directory) / "first.json").read_text()))

    def test_station_rejects_wrong_category_and_reports_delivery_failures(self):
        metadata = {"category": "bottle", "config": {"operator": ">"}, "threshold": 1.5,
                    "model_version": "test-model", "threshold_version": "test-threshold"}
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            root = Path(directory)
            (root / "images.json").write_text(json.dumps({"category": "capsule", "images": ["bottle/a.png"]}))
            with self.assertRaises(ValueError):
                read_image_list(root / "images.json", root, "bottle")
            args = SimpleNamespace(run_id="test", station="A", scenario="test", category="capsule",
                                   status=root / "wrong.json", rate=1, topic="inspections", max_items=1)
            with self.assertRaises(ValueError):
                run_station(args, FakeProducer(), lambda path: 0.1, metadata, ["capsule/a.png"], Event())
            self.assertFalse(args.status.exists())
            args.category = "bottle"
            for index, producer in enumerate((FakeProducer(error="broker failed"), FakeProducer(pending=1))):
                args.status = root / f"failed-{index}.json"
                result = run_station(args, producer, lambda path: 0.1, metadata, ["bottle/a.png"], Event())
                self.assertEqual(result["state"], "failed")
                self.assertIsNotNone(result["last_error"])


if __name__ == "__main__":
    unittest.main()
