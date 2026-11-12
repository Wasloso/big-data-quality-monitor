"""Opt-in API checks: run with the API dependencies and httpx installed."""

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from quality_monitor.api import app
from quality_monitor.store import write_batch


class ApiChecks(unittest.TestCase):
    def test_snapshot_filters_counts_and_missing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "quality.sqlite"
            with patch("quality_monitor.api.DATABASE", database), TestClient(app) as client:
                self.assertEqual(client.get("/api/runs").json()["state"], "missing")
                self.assertFalse(database.exists())
                write_batch(database, [], "epoch", 0)
                self.assertEqual(client.get("/api/runs").json()["state"], "empty")
                base = {
                    "start": datetime(2026, 11, 7, 12), "end": datetime(2026, 11, 7, 12, 1),
                    "run_id": "run", "line_id": "A", "category": "bottle",
                    "model_version": "fixture-v1", "threshold_version": "fixture-0.5",
                    "inspections": 60, "rejects": 6,
                }
                write_batch(database, [base, {**base, "line_id": "B", "rejects": 12},
                                      {**base, "run_id": "other", "rejects": 30},
                                      {**base, "model_version": "another-model", "rejects": 20}], "epoch", 1)
                params = {name: base[name] for name in ("run_id", "category", "model_version", "threshold_version")}
                data = client.get("/api/overview", params=params).json()
                self.assertEqual(sum(row["inspections"] for row in data["stations"]), 120)
                self.assertEqual(sum(row["rejects"] for row in data["stations"]), 18)
                self.assertEqual(sorted(row["status"] for row in data["alerts"]), ["active", "clear"])
                selected = client.get("/api/overview", params={**params, "line_id": "B"}).json()
                self.assertEqual(selected["stations"][0]["rejects"], 12)
                self.assertEqual([row["status"] for row in selected["alerts"]], ["active"])
                self.assertEqual(client.get("/api/overview", params={**params, "line_id": "' OR 1=1 --"}).json()["state"], "empty")
                self.assertEqual(client.get("/api/overview", params={**params, "from_time": "2026-11-07T13:00:00+01:00"}).json()["state"], "ready")
                self.assertEqual(client.get("/api/overview", params={**params, "from_time": "2026-11-07T12:00:00"}).status_code, 422)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM windows").fetchone()[0], 4)
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute("UPDATE windows SET rejects=40 WHERE run_id='run'")
                    concurrent = client.get("/api/overview", params=params).json()
                    self.assertEqual(sum(row["rejects"] for row in concurrent["stations"]), 18)
                    connection.rollback()

    def test_oversized_selection_fails_instead_of_returning_partial_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "quality.sqlite"
            base = {
                "start": datetime(2026, 11, 7, 12), "end": datetime(2026, 11, 7, 12, 1),
                "run_id": "run", "category": "bottle", "model_version": "fixture-v1",
                "threshold_version": "fixture-0.5", "inspections": 1, "rejects": 0,
            }
            write_batch(database, [{**base, "line_id": str(i)} for i in range(501)], "epoch", 0)
            params = {name: base[name] for name in ("run_id", "category", "model_version", "threshold_version")}
            with patch("quality_monitor.api.DATABASE", database), TestClient(app) as client:
                self.assertEqual(client.get("/api/overview", params=params).status_code, 413)
                self.assertEqual(client.get("/api/overview", params={**params, "line_id": "0"}).status_code, 200)
