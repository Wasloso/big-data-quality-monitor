"""C14 stage runner and bounded Spark report; run in the existing Spark image."""

import argparse
import json
import platform
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from quality_monitor.benchmark_metrics import emit, quantiles, read_jsonl, utc_now
from quality_monitor.deduplicate import clean_records
from quality_monitor.ingest import archive_records, file_query_contract
from quality_monitor.store import query_epoch, write_batch
from quality_monitor.windows import quality_windows


def analyze(spark, root, run_id):
    plan = json.loads((root / "producer.json").read_text())
    raw = spark.read.parquet(str(root / "raw"))
    raw_run = raw.filter(F.col("event.run_id") == run_id)
    clean = spark.read.parquet(str(root / "clean")).filter(F.col("run_id") == run_id)
    journal = spark.read.json(str(root / "publications.jsonl")).filter(F.col("run_id") == run_id)
    expected = journal.filter(~F.col("duplicate")).select("inspection_id").distinct()
    in_interval = journal.filter((F.col("published_monotonic") >= plan["measurement_start_monotonic"]) &
                                  (F.col("published_monotonic") < plan["measurement_end_monotonic"])).count()
    after_deadline = journal.filter(F.col("published_monotonic") >= plan["measurement_end_monotonic"]).count()
    raw_ids = raw_run.select(F.col("event.inspection_id").alias("inspection_id")).distinct()
    clean_ids = clean.select("inspection_id").distinct()
    ranges = []
    for progress in read_jsonl(root / "q1-progress.jsonl"):
        for source in progress["sources"]:
            start, end = source.get("startOffset"), source.get("endOffset")
            if isinstance(start, str):
                start = json.loads(start)
            if isinstance(end, str):
                end = json.loads(end)
            for topic, partitions in (end or {}).items():
                for partition, high in partitions.items():
                    low = (start or {}).get(topic, {}).get(partition, 0)
                    if high > low:
                        ranges.append((topic, int(partition), int(low), int(high), progress["observed_monotonic"]))
    latency = {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    unmatched = raw_run.count()
    if ranges:
        commits = spark.createDataFrame(ranges, "commit_topic string, commit_partition int, low long, high long, observed double")
        joined = raw_run.join(commits, (F.col("topic") == F.col("commit_topic")) &
                              (F.col("partition") == F.col("commit_partition")) &
                              (F.col("offset") >= F.col("low")) & (F.col("offset") < F.col("high")))
        values = joined.select((F.col("observed") - F.get_json_object(
            "raw_payload", "$.benchmark_published_monotonic",
        ).cast("double")).alias("seconds"))
        row = values.agg(F.count("*").alias("count"), F.min("seconds").alias("minimum"),
                         F.max("seconds").alias("maximum"),
                         F.percentile_approx("seconds", [.5, .95, .99], 10000).alias("percentiles")).first()
        if row["minimum"] is not None and row["minimum"] < 0:
            raise ValueError("Negative publication-to-commit observation latency; check clocks")
        if row["count"]:
            latency = dict(zip(("p50", "p95", "p99"), row["percentiles"]))
            latency.update({"count": row["count"], "max": row["maximum"]})
        unmatched -= row["count"]
    database = root / "serving.sqlite"
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro")) as connection:
        served = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(inspections),0), COALESCE(SUM(rejects),0), COALESCE(SUM(is_final=0),0) FROM windows WHERE run_id=?",
            (run_id,),
        ).fetchone()
    snapshots = read_jsonl(root / "window-observations.jsonl")
    start, end = plan["measurement_start_monotonic"], plan["measurement_end_monotonic"]
    def count_at(epoch):
        rows = [row for row in snapshots if row["observed_monotonic"] <= epoch]
        return rows[-1]["served_inspections"] if rows else 0
    processed_during_measurement = count_at(end) - count_at(start)
    report = {
        "run_id": run_id, "profile": plan["profile"], "spark_version": spark.version,
        "producer": plan,
        "measurement_processed_inspections": processed_during_measurement,
        "measurement_processed_inspections_per_second": processed_during_measurement / (end - start),
        "offered_messages_per_second_within_measurement_interval": in_interval / (end - start),
        "publications_after_measurement_deadline": after_deadline,
        "raw_inspection_messages": raw_run.count(), "unique_raw_inspections": raw_ids.count(),
        "clean_inspections": clean.count(), "clean_duplicate_ids": clean.count() - clean_ids.count(),
        "expected_unique_inspections": expected.count(),
        "published_ids_not_archived": expected.join(raw_ids, "inspection_id", "left_anti").count(),
        "archived_ids_excluded_from_clean": raw_ids.join(clean_ids, "inspection_id", "left_anti").count(),
        "clean_ids_absent_from_publication_journal": clean_ids.join(expected, "inspection_id", "left_anti").count(),
        "served_windows": served[0], "served_inspections": served[1], "served_rejects": served[2],
        "provisional_serving_windows_at_shutdown": served[3],
        "clean_inspections_minus_served": clean.count() - served[1],
        "archival_commit_observed_latency_seconds": latency,
        "archived_rows_without_commit_observation": unmatched,
        "latency_scope": "Real publication monotonic timestamp to observation of Q1's completed batch on the same Docker Linux kernel; includes polling delay",
        "quality_scope": "Provisional SQLite counts; no closure controls or final-window latency measurement",
        "aggregate_collect_and_sqlite_batch_seconds": quantiles([
            row["sink_seconds"] for row in snapshots
            if start <= row["observed_monotonic"] <= end
        ]),
    }
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    emit({"kind": "report", **report})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("prepare", "q1", "q2", "q3", "analyze"), required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--shuffle-partitions", type=int, default=2)
    args = parser.parse_args()
    root = args.root
    if args.stage == "prepare":
        root.mkdir(parents=True, exist_ok=False)
        emit({"kind": "prepared", "root": str(root)})
        return
    spark = (SparkSession.builder.appName("benchmark-" + args.stage)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .config("spark.sql.streaming.noDataMicroBatches.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        if args.stage == "analyze":
            analyze(spark, root, args.run_id)
            return
        checkpoint = root / args.stage
        if args.stage == "q1":
            file_query_contract(checkpoint, {"query_version": "ingest-v2", "output": str(root / "raw"),
                                            "topic": args.topic, "bootstrap_servers": "kafka:19092"})
            source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092")
                      .option("subscribe", args.topic).option("startingOffsets", "earliest").load())
            writer = archive_records(source).writeStream.format("parquet").option("path", str(root / "raw"))
        elif args.stage == "q2":
            file_query_contract(checkpoint, {"query_version": "deduplicate-v2", "input": str(root / "raw"),
                                            "output": str(root / "clean"), "lateness": "2 minutes",
                                            "shuffle_partitions": args.shuffle_partitions})
            schema = spark.read.parquet(str(root / "raw")).schema
            writer = clean_records(spark.readStream.schema(schema).parquet(str(root / "raw"))).writeStream.format("parquet").option("path", str(root / "clean"))
        else:
            database = root / "serving.sqlite"
            epoch = query_epoch(checkpoint, {
                "query_version": 2, "input": str(root / "clean"), "database": str(database),
                "window": "1 minute", "lateness": "2 minutes", "final": False,
                "threshold": .15, "minimum": 30, "shuffle_partitions": args.shuffle_partitions,
            })
            schema = spark.read.parquet(str(root / "clean")).schema
            source = spark.readStream.schema(schema).parquet(str(root / "clean"))
            changed_at, changed_monotonic, previous = None, None, {}
            def save(batch, batch_id):
                nonlocal changed_at, changed_monotonic
                started = time.monotonic()
                rows = batch.limit(1001).collect()
                if len(rows) > 1000:
                    raise RuntimeError("Benchmark exceeded the production sink's 1000-row bound")
                write_batch(str(database), rows, epoch, batch_id)
                changed = False
                for row in rows:
                    if row.run_id != args.run_id:
                        continue
                    key = (str(row.start), str(row.end), row.line_id)
                    value = (row.inspections, row.rejects)
                    if previous.get(key) != value:
                        changed = True
                        previous[key] = value
                observed = utc_now()
                if changed:
                    changed_at = observed
                    changed_monotonic = time.monotonic()
                with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro")) as connection:
                    count = connection.execute("SELECT COALESCE(SUM(inspections),0) FROM windows WHERE run_id=?", (args.run_id,)).fetchone()[0]
                value = {"kind": "windows", "stage": "q3", "observed_at": observed,
                         "observed_monotonic": time.monotonic(),
                         "served_inspections": count, "last_count_change_at": changed_at, "batch_id": batch_id,
                         "last_count_change_monotonic": changed_monotonic,
                         "sink_seconds": time.monotonic() - started}
                with (root / "window-observations.jsonl").open("a") as handle:
                    handle.write(json.dumps(value) + "\n")
                emit(value)
            writer = quality_windows(source).writeStream.foreachBatch(save).outputMode("update")
        query = writer.option("checkpointLocation", str(checkpoint)).trigger(processingTime=args.trigger).start()
        emit({"kind": "ready", "stage": args.stage, "spark_version": spark.version,
              "python_version": platform.python_version(), "master": spark.sparkContext.master,
              "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
              "jvm_max_heap_bytes": spark._jvm.java.lang.Runtime.getRuntime().maxMemory()})
        last_batch, last_status = None, 0.0
        with (root / (args.stage + "-progress.jsonl")).open("a") as handle:
            while query.isActive and not (root / ("stop-" + args.stage)).exists():
                for raw_progress in query.recentProgress:
                    progress = json.loads(raw_progress.json)
                    # Idle reports reuse the next batch ID; only committed progress
                    # represents consumed offsets and must advance our cursor.
                    if "commitOffsets" in progress.get("durationMs", {}) and (last_batch is None or progress["batchId"] > last_batch):
                        last_batch = progress["batchId"]
                        value = {"kind": "progress", "stage": args.stage, "observed_at": utc_now(),
                                 "observed_monotonic": time.monotonic(), **progress}
                        handle.write(json.dumps(value) + "\n")
                        handle.flush()
                        emit(value)
                if time.monotonic() - last_status >= 1:
                    emit({"kind": "status", "stage": args.stage, **query.status})
                    last_status = time.monotonic()
                query.awaitTermination(.25)
            if query.exception() is not None:
                raise RuntimeError(str(query.exception()))
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
