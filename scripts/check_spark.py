import platform

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("quality-monitor-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"Python: {platform.python_version()}")
        print(f"Spark: {spark.version}")

        # Small made-up results to check that a local Spark query works.
        inspections = spark.createDataFrame(
            [("A", False), ("A", True), ("A", False), ("B", False)],
            "line_id string, is_reject boolean",
        )
        summary = inspections.groupBy("line_id").agg(
            F.count("*").alias("inspections"),
            F.sum(F.col("is_reject").cast("int")).alias("rejects"),
        )
        counts = {
            row.line_id: (row.inspections, row.rejects)
            for row in summary.collect()
        }
        if counts != {"A": (3, 1), "B": (1, 0)}:
            raise RuntimeError(f"Unexpected inspection counts: {counts}")

        for line_id, (total, rejects) in sorted(counts.items()):
            print(f"Line {line_id}: {total} inspections, {rejects} rejects")
        print("Local Spark check passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
