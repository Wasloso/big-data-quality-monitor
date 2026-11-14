"""Check an archived fixture run and optionally its invalid test messages."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--path", default="/data/v2/raw")
    parser.add_argument("--clean", action="store_true", help="Check flattened clean inspections and logical uniqueness")
    parser.add_argument("--invalid-key", default=None)
    parser.add_argument("--expected-invalid", type=int, default=0)
    args = parser.parse_args()
    if args.clean and args.invalid_key is not None:
        parser.error("--invalid-key applies only to the raw archive")
    spark = SparkSession.builder.appName("archive-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        archived = spark.read.parquet(args.path)
        duplicates = archived.groupBy("topic", "partition", "offset").count().filter("count > 1")
        if duplicates.limit(1).count():
            raise RuntimeError("The same Kafka offset was archived more than once")
        if args.clean:
            repeated = archived.groupBy("run_id", "inspection_id").count().filter("count > 1")
            if repeated.limit(1).count():
                raise RuntimeError("Repeated inspection identity in the clean archive")
            events = archived
        else:
            events = archived.select("event.*", "is_valid")
        run = events.filter(F.col("run_id") == args.run_id)
        if not args.clean and run.filter(~F.col("is_valid")).count():
            raise RuntimeError("Valid fixture records were marked invalid")
        run = run.filter(F.col("record_kind") == "inspection")
        rows = run.groupBy("line_id").agg(
            F.count("*").alias("inspections"),
            F.sum(F.col("is_reject").cast("int")).alias("rejects"),
        ).collect()
        counts = {
            row.line_id: {"inspections": row.inspections, "rejects": row.rejects,
                          "reject_rate": row.rejects / row.inspections}
            for row in rows
        }
        expected = Path("/app/tests/fixtures/expected_counts.json")
        if counts != json.loads(expected.read_text()):
            raise RuntimeError(f"Unexpected fixture counts: {counts}")
        if args.invalid_key is not None:
            invalid = archived.filter(F.col("message_key") == args.invalid_key)
            if invalid.count() != args.expected_invalid or invalid.filter("is_valid").count():
                raise RuntimeError("Invalid messages were lost or marked valid")
            invalid.select("raw_payload", "validation_error").show(truncate=False)
        print(f"Archive check passed: {counts}; no repeated Kafka offsets")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
