"""Display provisional or finalized event-time quality windows."""

import argparse

from pyspark.sql import SparkSession, functions as F


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
    parser.add_argument("--input", default="/data/clean")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--lateness", default="2 minutes")
    parser.add_argument("--final", action="store_true", help="Print only watermark-finalized windows (append mode)")
    parser.add_argument("--once", action="store_true", help="Drain available clean files and exit")
    args = parser.parse_args()
    checkpoint = args.checkpoint or (
        "/data/checkpoints/windows-v2-final" if args.final else "/data/checkpoints/windows-v2-preview"
    )
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
        writer = (
            quality_windows(inspections, args.window, args.lateness)
            .writeStream.format("console").outputMode("append" if args.final else "update")
            .option("truncate", "false")
            .option("checkpointLocation", checkpoint)
        )
        if args.once:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="5 seconds")
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
