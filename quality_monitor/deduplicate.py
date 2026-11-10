"""Keep valid, unique inspections from the raw Parquet stream."""

import argparse

from pyspark.sql import SparkSession, functions as F


def clean_records(raw):
    # Controls must reach both stateful stages to advance their event-time watermarks.
    return (
        raw.filter("is_valid")
        .select("event.*", "topic", "partition", "offset", "ingested_at")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withWatermark("event_time", "2 minutes")
        .dropDuplicatesWithinWatermark(["run_id", "inspection_id"])
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/data/raw")
    parser.add_argument("--output", default="/data/clean")
    parser.add_argument("--checkpoint", default="/data/checkpoints/deduplicate")
    parser.add_argument("--once", action="store_true", help="Drain available archive files and exit")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("inspection-deduplication")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Ingestion must have written at least one raw file before this job starts.
        schema = spark.read.parquet(args.input).schema
        raw = spark.readStream.schema(schema).parquet(args.input)
        clean = clean_records(raw)
        writer = (
            clean.writeStream.format("parquet").outputMode("append")
            .option("path", args.output)
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
