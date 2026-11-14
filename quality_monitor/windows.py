"""Display provisional or finalized event-time quality windows."""

import argparse
import math
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from quality_monitor.store import query_epoch, sqlite_sink


def quality_windows(inspections, duration="1 minute", lateness="2 minutes"):
    is_inspection = F.col("record_kind") == "inspection"
    return (
        inspections.withWatermark("event_time", lateness)
        .groupBy(
            F.window("event_time", duration),
            "run_id", "line_id", "category", "model_version", "threshold_version",
        )
        .agg(
            F.sum(F.when(is_inspection, 1).otherwise(0)).alias("inspections"),
            F.sum(F.when(is_inspection & F.col("is_reject"), 1).otherwise(0)).alias("rejects"),
        )
        # Filter after aggregation: controls must advance the aggregation watermark.
        .filter(F.col("inspections") > 0)
        .withColumn("reject_rate", F.col("rejects") / F.col("inspections"))
        .select(
            "window.start", "window.end", "run_id", "line_id", "category",
            "model_version", "threshold_version", "inspections", "rejects", "reject_rate",
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/data/v2/clean")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--lateness", default="2 minutes")
    parser.add_argument("--final", action="store_true", help="Print only watermark-finalized windows (append mode)")
    parser.add_argument("--once", action="store_true", help="Drain available clean files and exit")
    parser.add_argument("--database", help="Write aggregate snapshots and alerts to this local SQLite file")
    parser.add_argument("--alert-threshold", type=float, default=0.15)
    parser.add_argument("--minimum-inspections", type=int, default=30)
    parser.add_argument("--max-store-rows", type=int, default=1000)
    args = parser.parse_args()
    if not math.isfinite(args.alert_threshold) or not 0 <= args.alert_threshold <= 1:
        parser.error("--alert-threshold must be finite and in [0, 1]")
    if args.minimum_inspections < 1 or args.max_store_rows < 1:
        parser.error("Inspection and aggregate-row limits must be positive")
    checkpoint = args.checkpoint or (
        f"/data/v2/checkpoints/windows-v2-{'sqlite-' if args.database else ''}{'final' if args.final else 'preview'}"
    )
    if args.database:
        database = str(Path(args.database).resolve())
        epoch = query_epoch(checkpoint, {
            "query_version": 2, "input": str(Path(args.input).resolve()), "database": database,
            "window": args.window, "lateness": args.lateness, "final": args.final,
            "threshold": args.alert_threshold, "minimum": args.minimum_inspections,
            "shuffle_partitions": 2,
        })
    spark = (
        SparkSession.builder.appName("inspection-quality-windows")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        schema = spark.read.parquet(args.input).schema
        inspections = spark.readStream.schema(schema).parquet(args.input)
        writer = quality_windows(inspections, args.window, args.lateness).writeStream.outputMode(
            "append" if args.final else "update",
        ).option("checkpointLocation", checkpoint)
        if args.database:
            writer = writer.foreachBatch(sqlite_sink(
                database, epoch, threshold=args.alert_threshold, minimum=args.minimum_inspections,
                final=args.final, max_rows=args.max_store_rows,
            ))
        else:
            writer = writer.format("console").option("truncate", "false")
        if args.once:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="5 seconds")
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
