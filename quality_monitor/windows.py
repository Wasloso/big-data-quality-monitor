"""Display provisional event-time quality windows from clean inspections."""

import argparse

from pyspark.sql import SparkSession, functions as F


def quality_windows(inspections, duration="1 minute", lateness="2 minutes"):
    return (
        inspections.withWatermark("event_time", lateness)
        .groupBy(
            F.window("event_time", duration),
            "run_id", "line_id", "category", "model_version", "threshold_version",
        )
        .agg(
            F.count("*").alias("inspections"),
            F.sum(F.col("is_reject").cast("long")).alias("rejects"),
        )
        .withColumn("reject_rate", F.col("rejects") / F.col("inspections"))
        .select(
            "window.start", "window.end", "run_id", "line_id", "category",
            "model_version", "threshold_version", "inspections", "rejects", "reject_rate",
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/data/clean")
    parser.add_argument("--checkpoint", default="/data/checkpoints/windows")
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--lateness", default="2 minutes")
    parser.add_argument("--once", action="store_true", help="Drain available clean files and exit")
    args = parser.parse_args()
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
            .writeStream.format("console").outputMode("update")
            .option("truncate", "false")
            .option("checkpointLocation", args.checkpoint)
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
