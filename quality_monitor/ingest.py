"""Archive Kafka inspections, keeping invalid payloads for investigation."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

from quality_monitor.events import TEXT_FIELDS, parse_event


EVENT_SCHEMA = T.StructType(
    [T.StructField(name, T.StringType()) for name in TEXT_FIELDS]
    + [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("record_kind", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("published_at", T.StringType()),
        T.StructField("anomaly_score", T.DoubleType()),
        T.StructField("is_reject", T.BooleanType()),
        T.StructField("station_context", T.StructType([
            T.StructField("version", T.IntegerType()), T.StructField("simulated", T.BooleanType()),
            T.StructField("work_order", T.StringType()), T.StructField("operation", T.StringType()),
            T.StructField("reported_state", T.StringType()), T.StructField("temperature_c", T.DoubleType()),
            T.StructField("conveyor_speed_m_min", T.DoubleType()),
        ])),
    ]
)


def validation_error(payload):
    try:
        parse_event(payload)
    except (ValueError, TypeError) as error:
        return str(error)
    return None


def archive_records(records):
    # Validate the original JSON before Spark can coerce incorrect field types.
    validate = F.udf(validation_error, T.StringType())
    return (
        records.selectExpr(
            "topic", "partition", "offset", "timestamp AS kafka_timestamp",
            "CAST(key AS STRING) AS message_key", "CAST(value AS STRING) AS raw_payload",
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("validation_error", validate("raw_payload"))
        .withColumn("is_valid", F.col("validation_error").isNull())
        .withColumn("event", F.from_json("raw_payload", EVENT_SCHEMA))
    )


def file_query_contract(checkpoint, contract):
    path = Path(checkpoint) / "file-query.json"
    if not path.exists():
        if any((path.parent / name).exists() for name in ("metadata", "offsets", "commits")):
            raise ValueError("Legacy file-query checkpoint; keep it intact and use new v2 state paths")
        output = Path(contract["output"])
        if output.exists() and any(output.iterdir()):
            raise ValueError("A fresh file query requires an empty output path; do not append a replay to old archives")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x") as handle:
            json.dump(contract, handle, sort_keys=True)
    except FileExistsError:
        if json.loads(path.read_text()) != contract:
            raise ValueError("File-query settings changed; use new checkpoint/output paths")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="inspections")
    parser.add_argument("--output", default="/data/v2/raw")
    parser.add_argument("--checkpoint", default="/data/v2/checkpoints/ingest")
    parser.add_argument("--once", action="store_true", help="Drain currently available records and exit")
    args = parser.parse_args()
    file_query_contract(args.checkpoint, {
        "query_version": "ingest-v2", "output": str(Path(args.output).resolve()),
        "topic": args.topic, "bootstrap_servers": args.bootstrap_servers,
    })
    spark = SparkSession.builder.appName("inspection-ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        writer = (
            archive_records(records).writeStream.format("parquet")
            .outputMode("append")
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
