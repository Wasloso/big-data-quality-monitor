"""Opt-in API checks: run with the API dependencies and httpx installed."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
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
                broken_projection = Path(directory) / "broken.sqlite"
                broken_projection.touch()
                with patch("quality_monitor.api.STATUS_ROOT", broken_projection):
                    discovered = client.get("/api/runs").json()
                    self.assertEqual(discovered["producer_status_state"], "unavailable")
                    self.assertEqual(len(discovered["runs"]), 3)
                with patch("quality_monitor.api.STATIONS_DATABASE", broken_projection):
                    partial = client.get("/api/overview", params=params).json()
                    self.assertEqual(partial["observation_state"], "unavailable")
                    self.assertEqual(sum(row["inspections"] for row in partial["stations"]), 120)
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
                context = {"version": 1, "simulated": True, "work_order": "WO-A", "operation": "inspection",
                           "reported_state": "running", "temperature_c": 35.5, "conveyor_speed_m_min": 12.0}
                rows = [{**base, "inspection_id": f"item-{index}"} for index in range(4)]
                rows[-1]["station_context"] = context
                write_inspections(database, [*rows, {**base, "inspection_id": "capsule-1", "category": "capsule"},
                                            {**base, "inspection_id": "other-run", "run_id": "other"}], "epoch", 0)
                first = client.get("/api/inspections", params={**params, "limit": 2}).json()
                self.assertEqual(first["retained_count"], 4)
                self.assertEqual([row["inspection_id"] for row in first["inspections"]], ["item-3", "item-2"])
                self.assertIs(first["inspections"][0]["is_reject"], True)
                self.assertEqual(first["inspections"][0]["station_context"], context)
                self.assertEqual(first["latest_inspection"]["station_context"], context)
                from quality_monitor.api import station_observations
                observed, state, batch_at = station_observations(base["run_id"], base["category"], base["model_version"], base["threshold_version"])
                self.assertEqual(state, "ready")
                self.assertIsNotNone(batch_at)
                self.assertEqual(observed[("A", base["category"], base["model_version"], base["threshold_version"])]["station_context"], context)
                self.assertEqual(station_observations("other-selection", base["category"], base["model_version"], base["threshold_version"])[0], {})
                self.assertIsNone(first["inspections"][1]["station_context"])
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
                self.assertEqual(client.get("/api/overview", params={"run_id": "run"}).status_code, 413)
                self.assertEqual(client.get("/api/overview", params={**params, "line_id": "0"}).status_code, 200)


    def test_producer_status_is_scoped_bounded_and_available_before_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "status"
            root.mkdir()
            now = datetime.now(timezone.utc)
            base = {
                "schema_version": 1, "run_id": "status-run", "station": "A", "session_id": "old-active",
                "category": "bottle", "model_version": "model", "threshold_version": "threshold", "threshold": 1.5,
                "state": "running", "target_rate": 2, "started_at": (now - timedelta(seconds=300)).isoformat(),
                "updated_at": (now - timedelta(seconds=1)).isoformat(),
                "attempted": 2, "completed": 2, "queued": 2, "published": 2, "inference_errors": 0,
                "enqueue_errors": 0, "delivery_errors": 0, "pending": 0, "last_error": None,
            }
            (root / "old.json").write_text(json.dumps(base))
            newer = {**base, "session_id": "new-ended", "state": "stopped",
                     "started_at": (now - timedelta(seconds=120)).isoformat(),
                     "updated_at": (now - timedelta(seconds=60)).isoformat(),
                     "stopped_at": (now - timedelta(seconds=60)).isoformat()}
            (root / "new.json").write_text(json.dumps(newer))
            slow = {**base, "station": "B", "session_id": "slow", "target_rate": .01,
                    "updated_at": (now - timedelta(seconds=100)).isoformat()}
            (root / "slow.json").write_text(json.dumps(slow))
            stale = {**slow, "station": "C", "session_id": "stale", "target_rate": 2}
            (root / "stale.json").write_text(json.dumps(stale))
            before = (root / "old.json").read_bytes()
            database = Path(directory) / "missing.sqlite"
            params = {name: base[name] for name in ("run_id", "category", "model_version", "threshold_version")}
            with patch("quality_monitor.api.STATUS_ROOT", root), patch("quality_monitor.api.DATABASE", database), TestClient(app) as client:
                response = client.get("/api/producer-status", params=params)
                self.assertEqual(response.status_code, 200)
                data = response.json()
                self.assertEqual(data["state"], "ready")
                sessions = {row["session_id"]: row for row in data["sessions"]}
                self.assertTrue(sessions["new-ended"]["latest_started_session"])
                self.assertFalse(sessions["old-active"]["latest_started_session"])
                self.assertEqual(sessions["slow"]["freshness"], "recent")
                self.assertEqual(sessions["stale"]["freshness"], "stale")
                self.assertEqual(len(client.get("/api/producer-status", params={**params, "line_id": "A"}).json()["sessions"]), 2)
                self.assertEqual(client.get("/api/producer-status", params={**params, "run_id": "other"}).json()["sessions"], [])
                listing = client.get("/api/runs").json()
                self.assertEqual(listing["state"], "ready")
                self.assertEqual(listing["runs"][0]["run_id"], base["run_id"])
                self.assertEqual(client.get("/api/overview", params=params).json()["windows"], [])
                self.assertFalse(database.exists())
                self.assertEqual((root / "old.json").read_bytes(), before)
                (root / "bad.json").write_text(json.dumps({**base, "published": True}))
                outside = Path(directory) / "outside.json"
                outside.write_text(json.dumps(base))
                (root / "escape.json").symlink_to(outside)
                partial = client.get("/api/producer-status", params=params).json()
                self.assertEqual(partial["state"], "partial")
                self.assertEqual(partial["invalid_files"], 2)
                with patch("quality_monitor.producer_status.MAX_FILES", 1):
                    self.assertEqual(client.get("/api/producer-status", params=params).status_code, 413)
                with patch("quality_monitor.api.STATUS_ROOT", None):
                    self.assertEqual(client.get("/api/producer-status", params=params).json()["state"], "disabled")

    def test_run_overview_keeps_detectors_and_rejects_ambiguous_or_overlapping_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "quality.sqlite"
            base = {"start": datetime(2026, 11, 7, 12), "end": datetime(2026, 11, 7, 12, 1),
                    "run_id": "mixed", "line_id": "A", "category": "bottle", "model_version": "bottle-model",
                    "threshold_version": "bottle-threshold", "inspections": 60, "rejects": 6}
            capsule = {**base, "line_id": "B", "category": "capsule", "model_version": "capsule-model",
                       "threshold_version": "capsule-threshold", "inspections": 20, "rejects": 4}
            later = {**base, "start": datetime(2026, 11, 7, 12, 1), "end": datetime(2026, 11, 7, 12, 2),
                     "inspections": 40, "rejects": 4}
            write_batch(database, [base, capsule, later], "epoch", 0)
            with patch("quality_monitor.api.DATABASE", database), TestClient(app) as client:
                result = client.get("/api/overview", params={"run_id": "mixed"})
                self.assertEqual(result.status_code, 200)
                data = result.json()
                self.assertEqual(data["scope"], "run")
                self.assertEqual(sum(row["inspections"] for row in data["stations"]), 120)
                self.assertEqual(sum(row["rejects"] for row in data["stations"]), 14)
                self.assertEqual({r["line_id"]: r["category"] for r in data["stations"]}, {"A": "bottle", "B": "capsule"})
                filtered = {"run_id": "mixed", "category": "capsule", "model_version": "capsule-model", "threshold_version": "capsule-threshold"}
                self.assertEqual(client.get("/api/overview", params=filtered).json()["stations"][0]["inspections"], 20)
                self.assertEqual(client.get("/api/overview", params={"run_id": "mixed", "category": "capsule"}).status_code, 422)
                self.assertEqual(client.get("/api/producer-status", params={"run_id": "mixed", "category": "capsule"}).status_code, 422)
                self.assertEqual(client.get("/api/overview", params={"run_id": "' OR 1=1 --"}).json()["stations"], [])
                write_batch(database, [{**base, "model_version": "competing-model"}], "epoch", 1)
                self.assertEqual(client.get("/api/overview", params={"run_id": "mixed"}).status_code, 409)
                self.assertEqual(client.get("/api/overview", params=filtered).status_code, 200)
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.execute("DELETE FROM windows WHERE model_version='competing-model'")
                overlap = {**base, "start": datetime(2026, 11, 7, 12, 0, 30), "end": datetime(2026, 11, 7, 12, 1, 30)}
                write_batch(database, [overlap], "epoch", 2)
                self.assertEqual(client.get("/api/overview", params={"run_id": "mixed"}).status_code, 409)
