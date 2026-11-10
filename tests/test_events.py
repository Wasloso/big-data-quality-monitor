import json
import unittest
from pathlib import Path

from quality_monitor.events import parse_event, validate_event
from quality_monitor.sample import progress_events, sample_events


FIXTURES = Path(__file__).parent / "fixtures"


class InspectionTests(unittest.TestCase):
    def test_sample_matches_expected_counts(self):
        events = [
            parse_event(line)
            for line in (FIXTURES / "inspections.jsonl").read_text().splitlines()
        ]
        expected = json.loads((FIXTURES / "expected_counts.json").read_text())
        counts = {}
        for event in events:
            line = counts.setdefault(event["line_id"], {"inspections": 0, "rejects": 0})
            line["inspections"] += 1
            line["rejects"] += int(event["is_reject"])
        for line in counts.values():
            line["reject_rate"] = line["rejects"] / line["inspections"]
        self.assertEqual(counts, expected)
        self.assertEqual(len({event["inspection_id"] for event in events}), 120)
        self.assertEqual(events, list(sample_events()))
        controls = list(progress_events(events, "2026-11-07T12:04:00+00:00"))
        self.assertEqual({e["line_id"] for e in controls}, {"A", "B"})
        self.assertEqual(len({e["inspection_id"] for e in controls}), 2)
        for control in controls:
            self.assertEqual(parse_event(json.dumps(control)), control)

    def test_invalid_payloads_are_rejected(self):
        valid = next(sample_events())
        invalid = [
            {**valid, "schema_version": 2},
            {**valid, "schema_version": True},
            {**valid, "record_kind": "unknown"},
            {**valid, "record_kind": "progress"},
            {**next(progress_events([valid], valid["event_time"])), "is_reject": False},
            {**valid, "inspection_id": " "},
            {**valid, "event_time": "not a date"},
            {**valid, "published_at": "2026-11-07T12:00:00"},
            {**valid, "event_time": "2026-11-07T12:00:00+01:00"},
            {**valid, "anomaly_score": float("nan")},
            {**valid, "anomaly_score": float("inf")},
            {**valid, "anomaly_score": True},
            {**valid, "is_reject": 1},
            {key: value for key, value in valid.items() if key != "line_id"},
            [],
        ]
        for event in invalid:
            with self.subTest(event=event):
                with self.assertRaises(ValueError):
                    validate_event(event)
        with self.assertRaises(ValueError):
            parse_event("{broken json")


if __name__ == "__main__":
    unittest.main()
