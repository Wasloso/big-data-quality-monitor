"""Query clean Parquet history and reconcile a closed run with SQLite windows."""

import argparse
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


GROUP = ("run_id", "line_id", "category", "model_version", "threshold_version")


def historical_windows(clean, run_id, duration):
    return (
        clean.filter((F.col("run_id") == run_id) & (F.col("record_kind") == "inspection"))
        .groupBy(F.window("event_time", duration), *GROUP)
        .agg(F.count("*").alias("inspections"), F.sum(F.col("is_reject").cast("long")).alias("rejects"))
        .withColumn("reject_rate", F.col("rejects") / F.col("inspections"))
        .select("window.start", "window.end", *GROUP, "inspections", "rejects", "reject_rate")
    )


def timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def window_record(row):
    return {**{name: row[name] for name in (*GROUP, "inspections", "rejects", "reject_rate")},
            "start": timestamp(row["start"]), "end": timestamp(row["end"])}


def window_key(row):
    return tuple(row[name] for name in (*GROUP, "start", "end"))


def compare_windows(batch, served):
    left = {window_key(row): row for row in batch}
    right = {window_key(row): row for row in served}
    if len(left) != len(batch) or len(right) != len(served):
        raise ValueError("Repeated logical window key")
    mismatches = []
    for key in sorted(left.keys() | right.keys()):
        a, b = left.get(key), right.get(key)
        if (a is None or b is None
                or any(a[name] != b[name] for name in ("inspections", "rejects"))
                or not math.isclose(a["reject_rate"], b["reject_rate"], abs_tol=1e-12)):
            mismatches.append({"historical": a, "streaming": b})
    return mismatches


def served_windows(database, run_id, limit):
    with closing(sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro")) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute("""
            SELECT run_id, line_id, category, model_version, threshold_version,
                   window_start AS start, window_end AS end, inspections, rejects, reject_rate, is_final
            FROM windows WHERE run_id=? ORDER BY window_start, line_id LIMIT ?
        """, (run_id, limit + 1)).fetchall()
    if len(rows) > limit:
        raise ValueError("Too many serving windows; select a smaller run or raise the explicit limit")
    return [dict(row) for row in rows]


def totals(rows):
    stations = {}
    for row in rows:
        station = stations.setdefault(row["line_id"], {"inspections": 0, "rejects": 0})
        station["inspections"] += row["inspections"]
        station["rejects"] += row["rejects"]
    for station in stations.values():
        station["reject_rate"] = station["rejects"] / station["inspections"]
    return stations


def reconcile(spark, clean_path, raw_path, database, run_id, duration="1 minute", limit=1000, expected=None):
    clean = spark.read.parquet(str(clean_path)).filter(F.col("run_id") == run_id)
    stats = clean.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("record_kind") == "inspection", 1).otherwise(0)).alias("inspections"),
        F.sum(F.when(F.col("record_kind") == "progress", 1).otherwise(0)).alias("controls"),
    ).first().asDict()
    if not stats["inspections"]:
        raise ValueError("No clean inspections for this run")
    unique = clean.select("run_id", "inspection_id").distinct().count()
    stats["repeated_id_rows"] = stats["rows"] - unique
    rows = historical_windows(clean, run_id, duration).orderBy("start", "line_id").limit(limit + 1).collect()
    if len(rows) > limit:
        raise ValueError("Too many historical windows; select a smaller run or raise the explicit limit")
    batch = [window_record(row.asDict()) for row in rows]
    served = served_windows(database, run_id, limit)
    mismatches = compare_windows(batch, [window_record(row) for row in served])

    raw = spark.read.parquet(str(raw_path))
    global_stats = raw.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("invalid_rows"),
        F.sum(F.when(~F.col("is_valid") & F.col("event.run_id").isNull(), 1).otherwise(0)).alias("invalid_without_run_id"),
    ).first().asDict()
    raw_run = raw.filter(F.col("event.run_id") == run_id)
    invalid_in_run = raw_run.filter(~F.col("is_valid")).count()
    valid = raw_run.filter(F.col("is_valid") & (F.col("event.record_kind") == "inspection"))
    raw_ids = valid.select(F.col("event.run_id").alias("run_id"), F.col("event.inspection_id").alias("inspection_id")).distinct()
    raw_inspections, raw_unique = valid.count(), raw_ids.count()
    clean_ids = clean.filter("record_kind = 'inspection'").select("run_id", "inspection_id").distinct()
    not_in_clean = raw_ids.join(clean_ids, ["run_id", "inspection_id"], "left_anti").count()
    not_in_raw = clean_ids.join(raw_ids, ["run_id", "inspection_id"], "left_anti").count()
    counts = totals(batch)
    provisional = sum(not row["is_final"] for row in served)
    oracle_matches = None if expected is None else counts == expected
    passed = bool(served) and not (mismatches or provisional or stats["repeated_id_rows"] or not_in_raw) and oracle_matches is not False
    return {
        "run_id": run_id, "spark_version": spark.version, "window": duration,
        "clean_input": str(clean_path), "raw_input": str(raw_path), "database": str(database),
        "raw_archive_global": global_stats,
        "raw_run": {"invalid_rows_with_this_run_id": invalid_in_run, "valid_inspection_rows": raw_inspections,
                    "unique_inspection_ids": raw_unique, "repeated_inspection_rows": raw_inspections - raw_unique,
                    "unique_raw_ids_absent_from_clean": not_in_clean},
        "clean_run": {**stats, "unique_clean_inspections_absent_from_raw": not_in_raw},
        "historical_windows": batch, "station_totals": counts,
        "serving_windows": len(served), "provisional_serving_windows": provisional,
        "window_mismatches": mismatches, "oracle_supplied": expected is not None, "oracle_matches": oracle_matches,
        "result": "passed" if passed else "not_reconciled",
        "limits": [
            "Raw archive global counts include other runs; malformed records without a run ID cannot be attributed to this run.",
            "Absent raw IDs are exclusions to investigate, not automatically classified as late or invalid.",
            "Static history does not replay arrival order or reconstruct watermark decisions. Reconciliation requires a drained, supported closed-run scenario.",
            "Matching counts without an independent oracle do not prove source completeness or correct labels.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", type=Path, required=True, help="Q3 SQLite checkpoint containing quality-store.json")
    parser.add_argument("--raw", type=Path, default=Path("/data/v2/raw"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True, help="New JSON report file, never overwritten")
    parser.add_argument("--expected", type=Path, help="Optional independent per-station counts/rates JSON")
    parser.add_argument("--max-windows", type=int, default=1000)
    args = parser.parse_args()
    if args.max_windows < 1:
        parser.error("--max-windows must be positive")
    metadata = json.loads((args.checkpoint / "quality-store.json").read_text())
    contract = metadata["contract"]
    if contract["query_version"] != 2:
        parser.error("Expected the version-2 SQLite window contract")
    if contract.get("final") is not True:
        parser.error("Use a finalized-window checkpoint; provisional snapshots are not a closed-run result")
    if args.output.exists():
        parser.error("Output already exists; choose a new report file")
    spark = SparkSession.builder.appName("quality-history").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = reconcile(
            spark, contract["input"], args.raw, contract["database"], args.run_id, contract["window"],
            args.max_windows, json.loads(args.expected.read_text()) if args.expected else None,
        )
        report["query_epoch"] = metadata["epoch"]
        report["query_contract"] = contract
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
        print(json.dumps({"result": report["result"], "station_totals": report["station_totals"], "output": str(args.output)}, indent=2))
        if report["result"] != "passed":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
