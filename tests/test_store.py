import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path

from quality_monitor.store import write_batch


class StoreTests(unittest.TestCase):
    def test_retry_rule_boundary_and_provisional_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "quality.sqlite")
            row = {
                "start": datetime(2026, 11, 7, 12), "end": datetime(2026, 11, 7, 12, 1),
                "run_id": "run", "line_id": "A", "category": "bottle",
                "model_version": "fixture-v1", "threshold_version": "fixture-0.5",
                "inspections": 10, "rejects": 3,
            }
            write_batch(database, [row], "epoch", 0, threshold=0.2, minimum=10)
            with closing(sqlite3.connect(database)) as connection:
                alert_id, status = connection.execute("SELECT alert_id, status FROM alerts").fetchone()
                self.assertEqual(status, "active")
            # A retry is ignored even if handed a different payload.
            write_batch(database, [{**row, "inspections": 15}], "epoch", 0, threshold=0.2, minimum=10)
            write_batch(database, [{**row, "inspections": 15}], "epoch", 1, threshold=0.2, minimum=10)
            write_batch(database, [{**row, "inspections": 0, "rejects": 0}], "epoch", 2, threshold=0.2, minimum=10)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(connection.execute("SELECT alert_id, status FROM alerts").fetchall(), [(alert_id, "resolved")])
                self.assertEqual(connection.execute("SELECT inspections, rejects FROM windows").fetchall(), [(15, 3)])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_batches").fetchone()[0], 3)

    def test_failed_batch_rolls_back_and_final_window_is_not_downgraded(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "quality.sqlite")
            row = {
                "start": datetime(2026, 11, 7, 12), "end": datetime(2026, 11, 7, 12, 1),
                "run_id": "run", "line_id": "A", "category": "bottle",
                "model_version": "fixture-v1", "threshold_version": "fixture-0.5",
                "inspections": 60, "rejects": 6,
            }
            with self.assertRaises(ValueError):
                write_batch(database, [row, {**row, "line_id": "B", "rejects": 61}], "epoch", 0)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM windows").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_batches").fetchone()[0], 0)
            write_batch(database, [row], "epoch", 0, final=True)
            write_batch(database, [{**row, "inspections": 30}], "preview-epoch", 0)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(connection.execute("SELECT inspections, is_final FROM windows").fetchall(), [(60, 1)])


if __name__ == "__main__":
    unittest.main()
