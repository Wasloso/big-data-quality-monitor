"""Archive Kafka inspections, keeping invalid payloads for investigation."""

import argparse

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="inspections")
    parser.add_argument("--output", default="/data/raw")
    parser.add_argument("--checkpoint", default="/data/checkpoints/ingest")
    parser.add_argument("--once", action="store_true", help="Drain currently available records and exit")
    args = parser.parse_args()
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
