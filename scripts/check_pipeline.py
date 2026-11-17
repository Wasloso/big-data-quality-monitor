"""Fixture correctness check, optionally with real Kafka and a killed Q3 driver."""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

from quality_monitor.deduplicate import clean_records
from quality_monitor.ingest import archive_records, file_query_contract
from quality_monitor.history import compare_windows, reconcile, served_windows, window_record
from quality_monitor.store import query_epoch, sqlite_sink
from quality_monitor.sample import progress_events, sample_events
from quality_monitor.windows import quality_windows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recovery", action="store_true", help="Exercise real Kafka and a hard Q3 process interruption")
    parser.add_argument("--topic", help="Existing empty dedicated Kafka topic, required with --recovery")
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--timeout", type=float, default=180, help="Seconds allowed per worker recovery wait")
    args = parser.parse_args()
    if args.recovery and (not args.topic or os.name != "posix"):
        parser.error("--recovery requires --topic and the Linux Spark container")
    if not 0 < args.timeout < float("inf"):
        parser.error("--timeout must be positive and finite")
    spark = (
        SparkSession.builder.appName("pipeline-check")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    root = Path("/data") / (("c13-check-" if args.recovery else "c06-check-") + uuid4().hex)
    root.mkdir()
    queries = []
    worker = None
    logs = []
    offset = 0
    events = list(sample_events())
    context = {"version": 1, "simulated": True, "work_order": "fixture-context", "operation": "inspection",
               "reported_state": "running", "temperature_c": 35.0, "conveyor_speed_m_min": 12.0}
    events[0]["station_context"] = context
    expected = json.loads(Path("/app/tests/fixtures/expected_counts.json").read_text())
    evidence = {"spark_version": spark.version, "path": str(root), "result": "in_progress"}
    (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"Scenario artifacts: {root}", flush=True)
    database = root / "serving" / "quality.sqlite"
    contract = {
        "query_version": 2, "input": str(root / "clean"), "database": str(database),
        "window": "1 minute", "lateness": "2 minutes", "final": True,
        "threshold": 0.15, "minimum": 30, "shuffle_partitions": 2,
    }
    epoch = query_epoch(root / "q3", contract)

    def archive(payloads):
        nonlocal offset
        if args.recovery:
            records = spark.createDataFrame([("check", payload) for payload in payloads], "key string, value string")
            records.write.format("kafka").option("kafka.bootstrap.servers", args.bootstrap_servers).option("topic", args.topic).save()
            ingestion.processAllAvailable()
            return
        rows = []
        for payload in payloads:
            rows.append(("inspections", 0, offset, datetime.now(timezone.utc), "check", payload))
            offset += 1
        records = spark.createDataFrame(
            rows, "topic string, partition int, offset long, timestamp timestamp, key string, value string",
        )
        archive_records(records).write.mode("append").parquet(str(root / "raw"))

    def publish(batch):
        archive([json.dumps(event) for event in batch])

    def drain():
        if args.recovery:
            ingestion.processAllAvailable()
        dedup.processAllAvailable()
        windows.processAllAvailable()

    def finalized():
        return [row for row in served_windows(database, "fixture-001", 1000) if row["is_final"]] if database.is_file() else []

    def wait_for(predicate, label):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            if worker.poll() is not None:
                raise RuntimeError(f"Q3 worker exited ({worker.returncode}) waiting for {label}; inspect {root}/*.log")
            if predicate():
                return
            time.sleep(0.25)
        raise TimeoutError(f"Timed out waiting for {label}; inspect {root}/*.log")

    def start_worker(crash=False):
        log = (root / ("crashed-worker.log" if crash else "resumed-worker.log")).open("w")
        logs.append(log)
        command = ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--conf", "spark.ui.enabled=false",
                   "/app/scripts/recovery_worker.py", "--root", str(root)]
        if crash:
            command.append("--crash")
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def stored_count():
        rows = served_windows(database, "fixture-001", 1000) if database.is_file() else []
        return sum(row["inspections"] for row in rows)

    def alert_keys():
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
            return connection.execute("SELECT alert_id, window_key FROM alerts ORDER BY alert_id").fetchall()

    try:
        if args.recovery:
            kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", args.bootstrap_servers)
                     .option("subscribe", args.topic).option("startingOffsets", "earliest").option("endingOffsets", "latest"))
            if kafka.load().limit(1).count():
                raise ValueError("Recovery requires a new empty topic; retained topics are never reset")
            file_query_contract(root / "q1", {
                "query_version": "ingest-v2", "output": str(root / "raw"),
                "topic": args.topic, "bootstrap_servers": args.bootstrap_servers,
            })
            source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.bootstrap_servers)
                      .option("subscribe", args.topic).option("startingOffsets", "earliest").load())
            ingestion = (archive_records(source).writeStream.format("parquet").outputMode("append")
                         .option("path", str(root / "raw")).option("checkpointLocation", str(root / "q1")).start())
            queries.append(ingestion)
            evidence["kafka_topic"] = args.topic
        # Hold two rejects back until the watermark is within their minute.
        delayed = [e for e in events if e["inspection_id"].endswith("040")]
        initial = [e for e in events if e not in delayed]
        publish(initial[:60] if args.recovery else initial + initial[-10:])
        if not args.recovery:
            archive(["{broken json", json.dumps({**events[0], "is_reject": 1})])
        file_query_contract(root / "q2", {
            "query_version": "deduplicate-v2", "input": str(root / "raw"), "output": str(root / "clean"),
            "lateness": "2 minutes", "shuffle_partitions": 2,
        })
        raw = spark.readStream.schema(spark.read.parquet(str(root / "raw")).schema).parquet(str(root / "raw"))
        dedup = (
            clean_records(raw).writeStream.format("parquet")
            .option("path", str(root / "clean"))
            .option("checkpointLocation", str(root / "q2"))
            .start()
        )
        queries.append(dedup)
        dedup.processAllAvailable()
        if args.recovery:
            worker = start_worker(crash=True)
            wait_for(
                lambda: stored_count() == 60 and (root / "q3-recovery" / "commits" / "0").exists(),
                "the first committed stateful batch",
            )
            publish(initial[60:])
            dedup.processAllAvailable()
            wait_for(lambda: (root / "fault.json").exists(), "the committed SQLite fault point")
            fault = json.loads((root / "fault.json").read_text())
            assert fault["batch_id"] > 0
            assert (root / "q3-recovery" / "commits" / "0").exists()
            assert not (root / "q3-recovery" / "commits" / str(fault["batch_id"])).exists()
            assert stored_count() == 118
            original_alerts = alert_keys()
            stopped_at = time.monotonic()
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=30)
            assert worker.returncode != 0
            # Event time advances while Q3 is down; Q2 accepts one delayed inspection after its watermark.
            publish(list(progress_events(events, "2026-11-07T12:02:30+00:00")))
            dedup.processAllAvailable()
            publish([delayed[0], *initial[-10:]])
            archive(["{broken json", json.dumps({**events[0], "is_reject": 1})])
            dedup.processAllAvailable()
            assert stored_count() == 118, "Q3 changed while its driver was stopped"
            restart_at = time.monotonic()
            worker = start_worker()
            wait_for(lambda: (root / "replayed.json").exists() and stored_count() == 119, "batch retry and backlog catch-up")
            recovered_at = time.monotonic()
            replay = json.loads((root / "replayed.json").read_text())
            assert replay["unchanged"] and replay["epoch"] == fault["epoch"]
            assert replay["batch_id"] == fault["batch_id"]
            assert (root / "q3-recovery" / "commits" / str(fault["batch_id"])).exists()
            def watermark_reached():
                path = root / "worker-progress.json"
                value = json.loads(path.read_text()).get("watermark") if path.exists() else None
                return value is not None and datetime.fromisoformat(value.replace("Z", "+00:00")) == datetime(
                    2026, 11, 7, 12, 0, 30, tzinfo=timezone.utc,
                )
            wait_for(watermark_reached, "Q3 watermark advancement")
            publish(delayed[1:])
            dedup.processAllAvailable()
            wait_for(lambda: stored_count() == 120, "the remaining within-allowance inspection")
            recovered = served_windows(database, "fixture-001", 1000)
            recovered_counts = {
                row["line_id"]: {name: row[name] for name in ("inspections", "rejects", "reject_rate")}
                for row in recovered
            }
            assert len(recovered) == 2 and recovered_counts == expected, recovered
            assert all(not row["is_final"] for row in recovered)
            assert alert_keys() == original_alerts
            (root / "worker-stop").touch()
            if worker.wait(timeout=args.timeout) != 0:
                raise RuntimeError("Resumed Q3 failed during ordinary shutdown")
            evidence["recovery"] = {
                "fault_point": fault["point"], "replayed_batch": fault["batch_id"], "epoch_preserved": True,
                "sink_retry_unchanged": True, "logical_alert_keys_preserved": True,
                "committed_prefix_inspections": 60, "earlier_spark_commit_preserved": True,
                "recovered_provisional_counts": recovered_counts, "recovered_snapshots_match_oracle": True,
                "counts_at_stop": 118, "counts_after_catch_up": 119,
                "downtime_including_backlog_preparation_seconds": restart_at - stopped_at,
                "restart_to_backlog_catch_up_seconds": recovered_at - restart_at,
                "scope": "Q3 hard process interruption with live Kafka/Q1/Q2, retained checkpoint and SQLite",
            }
            evidence["watermark_before_delayed_inspections"] = "2026-11-07T12:00:30.000Z"
        clean = spark.readStream.schema(spark.read.parquet(str(root / "clean")).schema).parquet(str(root / "clean"))
        windows = (
            quality_windows(clean).writeStream.foreachBatch(
                sqlite_sink(str(database), epoch, final=True),
            ).outputMode("append")
            .option("checkpointLocation", str(root / "q3"))
            .start()
        )
        queries.append(windows)
        drain()
        assert not finalized(), "Fixture window finalized before its lateness allowance"
        if not args.recovery:
            publish(list(progress_events(events, "2026-11-07T12:02:30+00:00")))
            drain()
            assert not finalized(), "Partial minute finalized too early"
            watermark = windows.lastProgress["eventTime"]["watermark"]
            assert datetime.fromisoformat(watermark.replace("Z", "+00:00")) == datetime(
                2026, 11, 7, 12, 0, 30, tzinfo=timezone.utc,
            ), watermark
            evidence["watermark_before_delayed_inspections"] = watermark
            publish(delayed)
            drain()
        clean_history = spark.read.parquet(str(root / "clean"))
        assert clean_history.filter("record_kind = 'inspection'").count() == 120
        observation = clean_history.filter(F.col("inspection_id") == events[0]["inspection_id"]).first()
        assert observation.station_context.asDict() == context
        publish(list(progress_events(events, "2026-11-07T12:04:00+00:00")))
        drain()
        rows = finalized()
        counts = {
            row["line_id"]: {name: row[name] for name in ("inspections", "rejects", "reject_rate")}
            for row in rows
        }
        assert len(rows) == 2 and counts == expected, (rows, counts)
        for row in rows:
            assert row["start"] == "2026-11-07T12:00:00+00:00"
            assert row["end"] == "2026-11-07T12:01:00+00:00"
            assert tuple(row[name] for name in ("run_id", "category", "model_version", "threshold_version")) == (
                "fixture-001", "bottle", "fixture-v1", "fixture-0.5",
            )
        # A distinct inspection older than both watermarks cannot revise a closed window.
        beyond = {**events[40], "inspection_id": "beyond-watermark", "product_id": "late-product"}
        publish([beyond])
        drain()
        assert finalized() == rows, "Late input changed finalized results"
        history = spark.read.parquet(str(root / "raw"))
        assert history.filter("NOT is_valid").count() == 2
        clean_history = spark.read.parquet(str(root / "clean"))
        assert clean_history.filter("record_kind = 'progress'").count() == 4
        assert not clean_history.groupBy("run_id", "inspection_id").count().filter("count > 1").count()
        expected_ids = spark.createDataFrame([(event["inspection_id"],) for event in events], "inspection_id string")
        actual_ids = clean_history.filter("record_kind = 'inspection'").select("inspection_id")
        assert not expected_ids.join(actual_ids, "inspection_id", "left_anti").count()
        assert not actual_ids.join(expected_ids, "inspection_id", "left_anti").count()
        assert not history.groupBy("topic", "partition", "offset").count().filter("count > 1").count()
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
            alerts = connection.execute("""
                SELECT w.line_id, a.status, a.is_provisional FROM alerts a
                JOIN windows w USING(window_key) ORDER BY w.line_id
            """).fetchall()
        assert alerts == [("A", "clear", 0), ("B", "active", 0)], alerts
        if args.recovery:
            assert alert_keys() == original_alerts
        report = reconcile(spark, root / "clean", root / "raw", database, "fixture-001", expected=expected)
        assert report["result"] == "passed", report
        assert report["raw_archive_global"]["invalid_rows"] == 2
        assert report["raw_run"]["repeated_inspection_rows"] == 10
        assert report["raw_run"]["unique_raw_ids_absent_from_clean"] == 1
        # Ensure the comparison detects an incorrect served count, not just matching row numbers.
        changed = [window_record(row) for row in rows]
        changed[0] = {**changed[0], "rejects": changed[0]["rejects"] + 1}
        assert compare_windows(report["historical_windows"], changed)
        (root / "history.json").write_text(json.dumps(report, indent=2) + "\n")
        evidence.update({
            "final_counts": counts, "within_allowance_accepted": len(delayed),
            "beyond_allowance_clean_rows": clean_history.filter(F.col("inspection_id") == "beyond-watermark").count(),
            "invalid_raw_rows": 2, "duplicate_injections": 10, "controls_in_clean": 4,
            "final_windows": len(rows), "historical_reconciliation": report["result"], "station_context_preserved": True,
            "history_report": str(root / "history.json"), "result": "passed",
            "exact_fixture_ids": True, "unique_kafka_offsets": True, "expected_final_alerts": True,
        })
        (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))
    except Exception as error:
        evidence.update({"result": "failed", "error": f"{type(error).__name__}: {error}"})
        (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        raise
    finally:
        if worker is not None and worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=30)
        for query in reversed(queries):
            query.stop()
        spark.stop()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
