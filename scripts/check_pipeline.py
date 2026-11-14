"""Explicit Spark integration check; uses Kafka-shaped input without a broker."""

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

from quality_monitor.deduplicate import clean_records
from quality_monitor.ingest import archive_records
from quality_monitor.history import compare_windows, reconcile, served_windows, window_record
from quality_monitor.store import query_epoch, sqlite_sink
from quality_monitor.sample import progress_events, sample_events
from quality_monitor.windows import quality_windows


def main():
    spark = (
        SparkSession.builder.appName("pipeline-check")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    root = Path("/data") / ("c06-check-" + uuid4().hex)
    root.mkdir()
    queries = []
    offset = 0
    events = list(sample_events())
    context = {"version": 1, "simulated": True, "work_order": "fixture-context", "operation": "inspection",
               "reported_state": "running", "temperature_c": 35.0, "conveyor_speed_m_min": 12.0}
    events[0]["station_context"] = context
    expected = json.loads(Path("/app/tests/fixtures/expected_counts.json").read_text())
    evidence = {"spark_version": spark.version, "path": str(root)}
    database = root / "serving" / "quality.sqlite"
    contract = {
        "query_version": 2, "input": str(root / "clean"), "database": str(database),
        "window": "1 minute", "lateness": "2 minutes", "final": True,
        "threshold": 0.15, "minimum": 30, "shuffle_partitions": 2,
    }
    epoch = query_epoch(root / "q3", contract)

    def archive(payloads):
        nonlocal offset
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
        dedup.processAllAvailable()
        windows.processAllAvailable()

    def finalized():
        return served_windows(database, "fixture-001", 1000) if database.is_file() else []

    try:
        # Hold two rejects back until the watermark is within their minute.
        delayed = [e for e in events if e["inspection_id"].endswith("040")]
        initial = [e for e in events if e not in delayed]
        publish(initial + initial[-10:])
        archive(["{broken json", json.dumps({**events[0], "is_reject": 1})])
        raw = spark.readStream.schema(spark.read.parquet(str(root / "raw")).schema).parquet(str(root / "raw"))
        dedup = (
            clean_records(raw).writeStream.format("parquet")
            .option("path", str(root / "clean"))
            .option("checkpointLocation", str(root / "q2"))
            .start()
        )
        queries.append(dedup)
        dedup.processAllAvailable()
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
        })
        (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))
    finally:
        for query in reversed(queries):
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
