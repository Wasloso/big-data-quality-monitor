import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from quality_monitor.inspection_store import write_inspections
from quality_monitor.sample import progress_events, sample_events
from quality_monitor.store import query_epoch


class InspectionStoreTests(unittest.TestCase):
    def test_retry_retention_and_late_observations_across_categories(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "stations.sqlite"
            events = list(sample_events())
            line_a = [row for row in events if row["line_id"] == "A"]
            context = {"version": 1, "simulated": True, "work_order": "WO-A", "operation": "inspection",
                       "reported_state": "running", "temperature_c": 31.2, "conveyor_speed_m_min": 8.0}
            line_a[-1] = {**line_a[-1], "station_context": context}
            capsule = {**line_a[-1], "line_id": "B", "category": "capsule", "inspection_id": "capsule-1"}
            write_inspections(database, [*line_a[-3:], capsule], "epoch", 0, retention=2)
            write_inspections(database, [{**line_a[-1], "anomaly_score": 99}], "epoch", 0, retention=2)
            # Same observation at another precision remains a duplicate, not a new latest item.
            duplicate = {**line_a[-1], "event_time": datetime(2026, 11, 7, 12, 0, 59, tzinfo=timezone.utc)}
            controls = progress_events(events, "2026-11-07T12:04:00+00:00")
            write_inspections(database, [line_a[0], duplicate, *controls], "epoch", 1, retention=2)
            with closing(sqlite3.connect(database)) as connection:
                rows = connection.execute("SELECT line_id, COUNT(*) FROM inspection_samples GROUP BY line_id").fetchall()
                self.assertEqual(rows, [("A", 2), ("B", 1)])
                value = connection.execute("SELECT station_context FROM station_latest WHERE line_id='A'").fetchone()[0]
                self.assertEqual(json.loads(value), context)
                unknown = connection.execute("SELECT station_context FROM inspection_samples WHERE inspection_id=?", (line_a[-2]["inspection_id"],)).fetchone()[0]
                self.assertIsNone(unknown)
                latest = connection.execute("SELECT line_id, category, inspection_id FROM station_latest ORDER BY line_id").fetchall()
                self.assertEqual(latest, [("A", "bottle", line_a[-1]["inspection_id"]), ("B", "capsule", "capsule-1")])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_batches").fetchone()[0], 2)

    def test_batch_rollback_and_missing_projection_on_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "stations.sqlite"
            checkpoint = Path(directory) / "checkpoint"
            contract = {"database": str(database), "query_version": "inspection-serving-v2"}
            tables = ("inspection_samples", "station_latest")
            epoch = query_epoch(checkpoint, contract, tables=tables)
            row = next(iter(sample_events()))
            with self.assertRaises(ValueError):
                write_inspections(database, [row, {**row, "inspection_id": "bad", "is_reject": 1}], epoch, 0)
            with closing(sqlite3.connect(database)) as connection:
                for table in (*tables, "processed_batches"):
                    self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
            write_inspections(database, [row], epoch, 0)
            (checkpoint / "commits").mkdir()
            (checkpoint / "commits" / "0").write_text("v1\n{}")
            self.assertEqual(query_epoch(checkpoint, contract, tables=tables), epoch)
            database.unlink()
            with self.assertRaisesRegex(ValueError, "missing or incomplete"):
                query_epoch(checkpoint, contract, tables=tables)
            self.assertFalse(database.exists())
            legacy = Path(directory) / "legacy.sqlite"
            with closing(sqlite3.connect(legacy)) as connection, connection:
                connection.execute("CREATE TABLE inspection_samples (legacy_id INTEGER)")
            with self.assertRaisesRegex(ValueError, "Legacy station database"):
                write_inspections(legacy, [row], "new", 0)
            with closing(sqlite3.connect(legacy)) as connection:
                self.assertEqual([r[1] for r in connection.execute("PRAGMA table_info(inspection_samples)")], ["legacy_id"])
