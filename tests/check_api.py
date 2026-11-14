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
from quality_monitor.inspection_store import write_inspections
from quality_monitor.sample import sample_events


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

    def test_recent_inspections_are_paged_and_isolated_from_other_categories(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "stations.sqlite"
            base = next(iter(sample_events()))
            params = {name: base[name] for name in ("run_id", "line_id", "category", "model_version", "threshold_version")}
            with patch("quality_monitor.api.STATIONS_DATABASE", database), TestClient(app) as client:
                self.assertEqual(client.get("/api/inspections", params=params).json()["state"], "missing")
                self.assertFalse(database.exists())
                rows = [{**base, "inspection_id": f"item-{index}"} for index in range(4)]
                write_inspections(database, [*rows, {**base, "inspection_id": "capsule-1", "category": "capsule"},
                                            {**base, "inspection_id": "other-run", "run_id": "other"}], "epoch", 0)
                first = client.get("/api/inspections", params={**params, "limit": 2}).json()
                self.assertEqual(first["retained_count"], 4)
                self.assertEqual([row["inspection_id"] for row in first["inspections"]], ["item-3", "item-2"])
                self.assertIs(first["inspections"][0]["is_reject"], True)
                self.assertEqual(first["latest_inspection"]["inspection_id"], "item-3")
                second = client.get("/api/inspections", params={**params, "limit": 2, **first["next_cursor"]}).json()
                self.assertEqual([row["inspection_id"] for row in second["inspections"]], ["item-1", "item-0"])
                self.assertIsNone(second["next_cursor"])
                self.assertEqual(client.get("/api/inspections", params={**params, "to_time": base["event_time"]}).json()["state"], "empty")
                self.assertEqual(client.get("/api/inspections", params={**params, "from_time": base["event_time"]}).json()["retained_count"], 4)
                self.assertEqual(client.get("/api/inspections", params={**params, "from_time": base["event_time"], "to_time": base["event_time"]}).status_code, 422)
                self.assertEqual(client.get("/api/inspections", params={**params, "category": "capsule"}).json()["retained_count"], 1)
                self.assertEqual(client.get("/api/inspections", params={**params, "line_id": "B"}).json()["state"], "empty")
                self.assertEqual(client.get("/api/inspections", params={**params, "limit": 101}).status_code, 422)
                self.assertEqual(client.get("/api/inspections", params={**params, "before_id": "item-2"}).status_code, 422)
                self.assertEqual(client.get("/api/inspections", params={**params, "before_id": "item-2", "before_time": "2026-11-07T12:00:00"}).status_code, 422)

    def test_images_are_resolved_from_retained_records_inside_the_configured_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "images"
            root.mkdir()
            database = Path(directory) / "stations.sqlite"
            image = b"\x89PNG\r\n\x1a\n"
            (root / "sample.png").write_bytes(image)
            (Path(directory) / "outside.png").write_bytes(image)
            (root / "escape.png").symlink_to(Path(directory) / "outside.png")
            (root / "note.txt").write_text("not an image")
            base = next(iter(sample_events()))
            paths = ["sample.png", "../outside.png", str(Path(directory) / "outside.png"),
                     "escape.png", "note.txt", "missing.png"]
            write_inspections(database, [{**base, "inspection_id": str(i), "image_uri": path}
                                        for i, path in enumerate(paths)], "epoch", 0)
            with patch("quality_monitor.api.STATIONS_DATABASE", database), patch("quality_monitor.api.IMAGE_ROOT", root), TestClient(app) as client:
                response = client.get("/api/inspection-image", params={"run_id": base["run_id"], "inspection_id": "0"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"], "image/png")
                self.assertEqual(response.content, image)
                for identity in ("1", "2", "3", "4", "5", "unknown"):
                    self.assertEqual(client.get("/api/inspection-image", params={"run_id": base["run_id"], "inspection_id": identity}).status_code, 404)
                self.assertEqual(client.get("/api/inspection-image", params={"run_id": "another-run", "inspection_id": "0"}).status_code, 404)
                with patch("quality_monitor.api.IMAGE_ROOT", None):
                    self.assertEqual(client.get("/api/inspection-image", params={"run_id": base["run_id"], "inspection_id": "0"}).status_code, 503)

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
