"""Project clean Parquet into bounded SQLite inspection history."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from quality_monitor.inspection_store import write_inspections
from quality_monitor.store import query_epoch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/data/clean")
    parser.add_argument("--database", default="/data/serving/stations.sqlite")
    parser.add_argument("--checkpoint", default="/data/checkpoints/inspection-serving-v1")
    parser.add_argument("--retention", type=int, default=1000, help="Recent samples retained per run/station")
    parser.add_argument("--max-batch-rows", type=int, default=1000)
    parser.add_argument("--once", action="store_true", help="Drain available clean files and exit")
    args = parser.parse_args()
    if args.retention < 1 or args.max_batch_rows < 1:
        parser.error("Retention and batch limits must be positive")
    database = str(Path(args.database).resolve())
    epoch = query_epoch(args.checkpoint, {
        "query_version": "inspection-serving-v1", "input": str(Path(args.input).resolve()),
        "database": database, "retention": args.retention, "max_batch_rows": args.max_batch_rows,
    }, tables=("inspection_samples", "station_latest"))
    spark = (
        SparkSession.builder.appName("inspection-serving")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        schema = spark.read.parquet(args.input).schema
        inspections = (
            spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).parquet(args.input)
            .filter("record_kind = 'inspection'")
        )

        def save(batch, batch_id):
            # Bound the driver write, and fail rather than silently lose source records.
            rows = batch.limit(args.max_batch_rows + 1).collect()
            if len(rows) > args.max_batch_rows:
                raise RuntimeError(f"Inspection batch exceeds --max-batch-rows={args.max_batch_rows}")
            write_inspections(database, [row.asDict() for row in rows], epoch, batch_id, retention=args.retention)

        writer = inspections.writeStream.foreachBatch(save).outputMode("append").option(
            "checkpointLocation", args.checkpoint,
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
