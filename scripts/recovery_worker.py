"""Q3 subprocess used only by check_pipeline.py --recovery (Linux container)."""

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from threading import Event

from pyspark.sql import SparkSession

from quality_monitor.store import query_epoch, sqlite_sink
from quality_monitor.windows import quality_windows


def save_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def serving_snapshot(database):
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        return {table: connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
                for table in ("windows", "alerts", "processed_batches")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--crash", action="store_true")
    args = parser.parse_args()
    root = args.root
    database = root / "serving" / "quality.sqlite"
    checkpoint = root / "q3-recovery"
    contract = {
        "query_version": 2, "input": str(root / "clean"), "database": str(database),
        "window": "1 minute", "lateness": "2 minutes", "final": False,
        "threshold": 0.15, "minimum": 30, "shuffle_partitions": 2,
    }
    epoch = query_epoch(checkpoint, contract)
    fault = root / "fault.json"
    interrupted_batch = None if args.crash else json.loads(fault.read_text())["batch_id"]
    sink = sqlite_sink(str(database), epoch)

    def save(batch, batch_id):
        before = serving_snapshot(database) if batch_id == interrupted_batch else None
        batch.persist()
        try:
            sink(batch, batch_id)
            if before is not None:
                after = serving_snapshot(database)
                if before != after:
                    raise RuntimeError("Replayed committed sink batch changed SQLite results")
                save_json(root / "replayed.json", {"batch_id": batch_id, "epoch": epoch, "unchanged": True})
            if args.crash and batch_id > 0 and not fault.exists():
                # Update mode can re-emit a snapshot in a no-data batch. Interrupt only
                # after the intended second portion, not just any nonempty callback.
                with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
                    count = connection.execute("SELECT SUM(inspections) FROM windows").fetchone()[0]
                if count == 118:
                    save_json(fault, {"batch_id": batch_id, "epoch": epoch, "point": "after-sqlite-before-spark-commit"})
                    # Parent kills this entire process group while foreachBatch is blocked.
                    Event().wait()
        finally:
            batch.unpersist()

    spark = (
        SparkSession.builder.appName("quality-recovery-worker")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        source = spark.readStream.schema(spark.read.parquet(str(root / "clean")).schema).parquet(str(root / "clean"))
        query = (quality_windows(source).writeStream.foreachBatch(save).outputMode("update")
                 .option("checkpointLocation", str(checkpoint)).trigger(processingTime="1 second").start())
        watermark = None
        while query.isActive and not (root / "worker-stop").exists():
            progress = query.lastProgress
            if progress:
                watermark = progress.get("eventTime", {}).get("watermark", watermark)
                save_json(root / "worker-progress.json", {"watermark": watermark, "batch_id": progress["batchId"]})
            query.awaitTermination(0.5)
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
