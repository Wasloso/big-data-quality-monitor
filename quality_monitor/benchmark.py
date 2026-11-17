"""Run deliberate, isolated C14 Docker experiments from the repository root."""

import argparse
import hashlib
import itertools
import json
import math
import os
import platform
import re
import subprocess
import statistics
import threading
import time
from pathlib import Path
from uuid import uuid4

from quality_monitor.benchmark_metrics import PREFIX, load_segments, quantiles, utc_now


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--profiles", nargs="+", choices=("steady", "burst", "skew", "duplicate-late"), default=["steady"])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--rate", type=float, default=20)
    parser.add_argument("--rates", type=float, nargs="+", help="Offered-rate sweep; overrides --rate")
    parser.add_argument("--with-serving", action="store_true", help="Include inspection projection, producer status and HTTP polling")
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--drain-seconds", type=float, default=60)
    parser.add_argument("--burst-multiplier", type=float, default=4)
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=2)
    parser.add_argument("--trigger-seconds", type=float, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--max-events", type=int, default=1000000)
    parser.add_argument("--output", type=Path, default=Path("artifacts/benchmarks"))
    args = parser.parse_args()
    rates = args.rates or [args.rate]
    if any(value not in (1, 2, 4) for value in args.threads) or min(args.repetitions, args.partitions, args.shuffle_partitions) < 1:
        parser.error("Use local thread counts 1, 2 or 4 and positive count settings")
    if any(not math.isfinite(value) or value <= 0 for value in (*rates, args.duration, args.drain_seconds, args.burst_multiplier, args.trigger_seconds)):
        parser.error("Rate, duration, drain, burst multiplier and trigger must be positive and finite")
    if not math.isfinite(args.warmup) or args.warmup < 0 or args.payload_bytes < 0 or args.max_events < 1:
        parser.error("Warmup and padding must be nonnegative")
    if len(set(rates)) != len(rates) or len(set(args.threads)) != len(args.threads) or len(set(args.profiles)) != len(args.profiles):
        parser.error("Thread, rate and profile selections must not repeat")
    for rate, profile in itertools.product(rates, args.profiles):
        planned = sum(math.ceil(length * rate) for _, _, length, rate in load_segments(
            rate, args.warmup, args.duration, profile, args.burst_multiplier,
        ))
        if planned > args.max_events:
            parser.error("Planned workload exceeds --max-events")
    repo = Path(__file__).resolve().parents[1]
    suite = args.output.resolve() / ("suite-" + uuid4().hex[:12])
    suite.mkdir(parents=True)
    compose = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.benchmark.yaml"]
    env = dict(os.environ)

    def run(command, *, capture=False):
        result = subprocess.run(command, cwd=repo, env=env, text=True, capture_output=capture, check=True)
        return result.stdout.strip() if capture else None

    revision = subprocess.run(["git", "-c", f"safe.directory={repo.as_posix()}", "rev-parse", "HEAD"],
                              cwd=repo, text=True, capture_output=True, check=True)
    hardware = json.loads(run(["docker", "info", "--format",
                               '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"storage_driver":"{{.Driver}}","server":"{{.ServerVersion}}"}'], capture=True))
    manifest = {"started_at": utc_now(), "revision": revision.stdout.strip(), "host": platform.platform(),
                "docker": hardware, "settings": {**vars(args), "output": str(args.output)},
                "spark_memory_limit": env.get("BENCHMARK_SPARK_MEMORY", "2g"),
                "producer_cpu_limit": 1, "producer_memory_limit": "512m",
                "api_cpu_limit": 1 if args.with_serving else None,
                "api_memory_limit": "512m" if args.with_serving else None,
                "source_sha256": {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in [repo / "quality_monitor" / name for name in (
                                      "benchmark.py", "benchmark_producer.py", "benchmark_metrics.py", "benchmark_observer.py",
                                      "events.py", "ingest.py", "deduplicate.py", "windows.py", "store.py",
                                      "inspection_store.py", "api.py", "producer_status.py",
                                  )] + [repo / name for name in (
                                      "scripts/benchmark_worker.py", "compose.yaml", "compose.benchmark.yaml",
                                      "Dockerfile.spark", "Dockerfile.kafka", "requirements-kafka.txt",
                                  )]},
                "track": "infrastructure replay; fixture scores, separate producer and Spark drivers",
                "serving_enabled": args.with_serving, "spark_drivers": 4 if args.with_serving else 3,
                "storage": "archive-data named volume; host files contain observations/logs only"}
    (suite / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    run(compose + ["build", "spark-windows", "benchmark-producer"] + (["benchmark-api"] if args.with_serving else []))
    results = []
    for threads, rate in itertools.product(args.threads, rates):
        env["BENCHMARK_THREADS"] = str(threads)
        for profile in args.profiles:
            for repetition in range(1, args.repetitions + 1):
                identifier = "c14-" + uuid4().hex[:16]
                env["BENCHMARK_RUN_ID"] = identifier
                root = "/data/" + identifier
                destination = suite / f"local-{threads}-rate-{rate:g}-{profile}-{repetition}"
                destination.mkdir()
                processes = {}
                samples = []
                print(f"Benchmark {identifier}: local[{threads}], {rate:g}/s, {profile}, repetition {repetition}", flush=True)
                stages = ("q1", "q2", "q3", "q4") if args.with_serving else ("q1", "q2", "q3")
                monitored = (*stages, "api") if args.with_serving else stages

                def start(role, command):
                    log = (destination / (role + ".log")).open("w", encoding="utf-8")
                    state = {"name": identifier + "-" + role, "latest": {}, "batches": {}, "ready": False,
                             "last_nonempty": time.monotonic(), "log": log}
                    proc = subprocess.Popen(command, cwd=repo, env=env, stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
                    state["process"] = proc
                    def collect():
                        for line in proc.stdout:
                            log.write(line)
                            log.flush()
                            if line.startswith(PREFIX):
                                value = json.loads(line[len(PREFIX):])
                                state["latest"][value["kind"]] = value
                                if "observed_monotonic" in value and role == "generator":
                                    state["clock"] = (value["observed_monotonic"], time.monotonic())
                                if value["kind"] == "ready":
                                    state["ready"] = True
                                if value["kind"] == "status":
                                    state["status_received"] = time.monotonic()
                                if value["kind"] == "progress":
                                    state["batches"][value["batchId"]] = value["numInputRows"]
                                    if value["numInputRows"]:
                                        state["last_nonempty"] = time.monotonic()
                    state["thread"] = threading.Thread(target=collect, daemon=True)
                    state["thread"].start()
                    processes[role] = state
                    return state

                def wait_for(predicate, seconds=180):
                    deadline = time.monotonic() + seconds
                    while not predicate():
                        for role in monitored:
                            if role in processes and processes[role]["process"].poll() is not None:
                                raise RuntimeError(f"{role} exited; inspect {destination / (role + '.log')}")
                        if time.monotonic() > deadline:
                            raise TimeoutError(f"Benchmark wait timed out; inspect {destination}")
                        time.sleep(.25)

                def worker(stage):
                    command = compose + ["run", "--rm", "--no-deps", "--name", identifier + "-" + stage,
                                         "--entrypoint", "/opt/spark/bin/spark-submit", "spark-windows",
                                         "--master", f"local[{threads}]", "--conf", "spark.ui.enabled=false"]
                    if stage == "q1":
                        command += ["--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
                                    "--conf", "spark.jars.ivy=/data/ivy"]
                    return command + ["/app/scripts/benchmark_worker.py", "--stage", stage,
                                      "--root", root, "--run-id", identifier, "--topic", identifier,
                                      "--shuffle-partitions", str(args.shuffle_partitions),
                                      "--trigger", f"{args.trigger_seconds} seconds"]

                def producer(bootstrap=False):
                    command = compose + ["run", "--rm", "--no-deps", "--name", identifier + ("-bootstrap" if bootstrap else "-generator"),
                                         "benchmark-producer", "--root", root, "--topic", identifier, "--run-id", identifier,
                                         "--rate", str(rate), "--warmup", str(args.warmup), "--duration", str(args.duration),
                                         "--profile", profile, "--seed", str(args.seed), "--burst-multiplier", str(args.burst_multiplier),
                                         "--payload-bytes", str(args.payload_bytes), "--max-events", str(args.max_events)]
                    return command + (["--bootstrap"] if bootstrap else []) + (["--with-serving"] if args.with_serving else [])

                result = {"run_id": identifier, "threads_per_driver": threads, "profile": profile,
                          "target_rate": rate, "serving_enabled": args.with_serving,
                          "repetition": repetition, "volume_path": root, "result": "in_progress"}
                try:
                    run(compose + ["run", "--rm", "kafka-init", "--bootstrap-server", "kafka:19092", "--create",
                                   "--topic", identifier, "--partitions", str(args.partitions), "--replication-factor", "1"])
                    run(worker("prepare"))
                    start("q1", worker("q1"))
                    wait_for(lambda: processes["q1"]["ready"])
                    bootstrap = start("bootstrap", producer(True))
                    if bootstrap["process"].wait(timeout=120) != 0:
                        raise RuntimeError("Bootstrap producer failed")
                    wait_for(lambda: sum(processes["q1"]["batches"].copy().values()) >= 2)
                    start("q2", worker("q2"))
                    wait_for(lambda: sum(processes["q2"]["batches"].copy().values()) >= 2)
                    start("q3", worker("q3"))
                    wait_for(lambda: sum(processes["q3"]["batches"].copy().values()) >= 2)
                    if args.with_serving:
                        start("q4", worker("q4"))
                        wait_for(lambda: sum(processes["q4"]["batches"].copy().values()) >= 2)
                        start("api", compose + ["run", "--rm", "--no-deps", "--name", identifier + "-api",
                                                "benchmark-api", "--root", "/data/serving", "--run-id", identifier])
                        wait_for(lambda: processes["api"]["ready"])
                    generator = start("generator", producer())
                    finished = None
                    drained = False
                    next_stats = 0.0
                    with (destination / "samples.jsonl").open("w") as sample_log, (destination / "resources.jsonl").open("w") as resource_log:
                        while True:
                            now = time.monotonic()
                            for role in monitored:
                                if processes[role]["process"].poll() is not None:
                                    raise RuntimeError(f"{role} exited; inspect its log")
                            windows = processes["q3"]["latest"].get("windows", {})
                            offered = generator["latest"].get("producer", {})
                            progress = processes["q1"]["latest"].get("progress", {})
                            committed = {}
                            for source in progress.get("sources", []):
                                end = source.get("endOffset") or {}
                                if isinstance(end, str):
                                    end = json.loads(end)
                                committed.update(end.get(identifier, {}))
                            heads = offered.get("end_offsets", {})
                            lag = None if not heads else sum(max(0, high - committed.get(partition, 0)) for partition, high in heads.items())
                            observed_at = utc_now()
                            clock = generator.get("clock")
                            observed_monotonic = None if clock is None else clock[0] + time.monotonic() - clock[1]
                            changed = windows.get("last_count_change_monotonic")
                            age = None if changed is None or observed_monotonic is None else max(0, observed_monotonic - changed)
                            sample = {"observed_at": observed_at, "kafka_offset_lag": lag,
                                      "file_stages": {role: processes[role]["latest"].get("files") for role in stages},
                                      "projected_inspections": processes.get("q4", {}).get("latest", {}).get("projection", {}).get("projected_inspections"),
                                      "estimated_container_monotonic": observed_monotonic,
                                      "producer_offsets_observed_monotonic": offered.get("observed_monotonic"),
                                      "producer_offsets_observed_at": offered.get("observed_at"),
                                      "q1_commit_observed_at": progress.get("observed_at"),
                                      "served_inspections": windows.get("served_inspections", 0),
                                      "window_count_change_age_seconds": age,
                                      "acknowledged_unique": offered.get("counts", {}).get("measurement", {}).get("unique_acknowledged", 0)}
                            samples.append(sample)
                            sample_log.write(json.dumps(sample) + "\n")
                            sample_log.flush()
                            if now >= next_stats:
                                running = subprocess.run(["docker", "ps", "--filter", "name=" + identifier, "--format", "{{.Names}}"],
                                                         text=True, capture_output=True, check=True).stdout.splitlines()
                                active = [state["name"] for role, state in processes.items()
                                          if role in (*monitored, "generator") and state["name"] in running]
                                if not active:
                                    raise RuntimeError("No benchmark containers available for resource observation")
                                stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *active],
                                                       text=True, capture_output=True, timeout=15)
                                clock = generator.get("clock")
                                resource_monotonic = None if clock is None else clock[0] + time.monotonic() - clock[1]
                                resource_log.write(json.dumps({"observed_at": utc_now(), "estimated_container_monotonic": resource_monotonic,
                                                               "rows": stats.stdout.splitlines(),
                                                               "error": stats.stderr.strip() or None}) + "\n")
                                resource_log.flush()
                                next_stats = time.monotonic() + 2
                            now = time.monotonic()
                            if generator["process"].poll() is not None:
                                if generator["process"].returncode != 0:
                                    raise RuntimeError("Generator failed; no valid offered-load result")
                                finished = now if finished is None else finished
                                counts = offered.get("counts", {})
                                total = 2 + sum(row["queued"] for row in counts.values())
                                q2_rows = sum(processes["q2"]["batches"].copy().values())
                                settled = now - processes["q2"]["last_nonempty"] > 2 * args.trigger_seconds + 2
                                status = processes["q3"]["latest"].get("status", {})
                                q3_idle = bool(status and not status["isTriggerActive"] and not status["isDataAvailable"]
                                               and now - processes["q3"].get("status_received", 0) < 3
                                               and now - processes["q3"]["last_nonempty"] > args.trigger_seconds)
                                drained = bool(offered.get("final") and lag == 0 and q2_rows >= total and settled and
                                               (windows.get("served_inspections", 0) >= counts.get("measurement", {}).get("unique_acknowledged", 0)
                                                if profile != "duplicate-late" else q3_idle and now - finished > 2 * args.trigger_seconds + 2))
                                file_samples = [processes[role]["latest"].get("files") for role in stages if role != "q1"]
                                files_drained = all(row is not None and row.get("pending_input_files") == 0 for row in file_samples)
                                drained = drained and files_drained
                                if args.with_serving:
                                    projected = processes["q4"]["latest"].get("projection", {}).get("projected_inspections", 0)
                                    api = processes["api"]["latest"].get("api", {})
                                    overview = next((row for row in api.get("requests", []) if row["endpoint"] == "overview"), {})
                                    drained = drained and bool(
                                        projected == windows.get("served_inspections", 0)
                                        and api.get("observed_monotonic", 0) >= offered.get("observed_monotonic", float("inf"))
                                        and all(row["error"] is None for row in api.get("requests", []))
                                        and overview.get("served_inspections") == windows.get("served_inspections", 0)
                                    )
                                if drained or now - finished >= args.drain_seconds:
                                    break
                            time.sleep(.5)
                    result["drain_settled"] = drained
                    result["remaining_kafka_lag"] = samples[-1]["kafka_offset_lag"]
                    result["remaining_file_backlog"] = {
                        role: processes[role]["latest"].get("files") for role in stages if role != "q1"
                    }
                    for role in monitored:
                        stop_root = "/data/serving" if role == "api" else root
                        run(["docker", "exec", processes[role]["name"], "python3", "-c",
                             f"from pathlib import Path; Path('{stop_root}/stop-{role}').touch()"])
                        if processes[role]["process"].wait(timeout=60) != 0:
                            raise RuntimeError(f"{role} shutdown failed")
                        processes[role]["thread"].join(timeout=10)
                    analysis = start("analyze", worker("analyze"))
                    if analysis["process"].wait(timeout=180) != 0:
                        raise RuntimeError("Spark analysis failed")
                    analysis["thread"].join(timeout=10)
                    report = analysis["latest"]["report"]
                    result["report"] = report
                    start_time, end_time = report["producer"]["measurement_start_monotonic"], report["producer"]["measurement_end_monotonic"]
                    measured = [row for row in samples if row["estimated_container_monotonic"] is not None
                                and start_time <= row["estimated_container_monotonic"] <= end_time]
                    result["measurement_kafka_lag"] = quantiles([row["kafka_offset_lag"] for row in measured if row["kafka_offset_lag"] is not None])
                    result["measurement_window_count_change_age_seconds"] = quantiles([row["window_count_change_age_seconds"] for row in measured if row["window_count_change_age_seconds"] is not None])
                    result["measurement_file_backlog"] = {
                        role: {field: quantiles([
                            row["file_stages"][role][field] for row in measured
                            if row["file_stages"][role] is not None and field in row["file_stages"][role]
                        ]) for field in ("pending_input_files", "pending_input_bytes")}
                        for role in stages if role != "q1"
                    }
                    resources = {}
                    for line in (destination / "resources.jsonl").read_text().splitlines():
                        observation = json.loads(line)
                        instant = observation["estimated_container_monotonic"]
                        if instant is None or not start_time <= instant <= end_time:
                            continue
                        for encoded in observation["rows"]:
                            row = json.loads(encoded)
                            role = row["Name"].removeprefix(identifier + "-")
                            summary = resources.setdefault(role, {"samples": 0, "peak_cpu_percent": 0, "peak_memory_bytes": 0})
                            summary["samples"] += 1
                            summary["peak_cpu_percent"] = max(summary["peak_cpu_percent"], float(row["CPUPerc"].rstrip("%")))
                            match = re.match(r"([\d.]+)([A-Za-z]+)", row["MemUsage"].split("/")[0].strip())
                            if match:
                                units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "kB": 1000, "MB": 1000**2, "GB": 1000**3}
                                summary["peak_memory_bytes"] = max(summary["peak_memory_bytes"], float(match[1]) * units[match[2]])
                    result["measurement_container_resources"] = resources
                    if profile == "burst":
                        burst_start = start_time + args.duration / 3
                        burst_end = start_time + 2 * args.duration / 3
                        burst_samples = [row for row in samples if row["estimated_container_monotonic"] is not None
                                         and burst_start <= row["estimated_container_monotonic"] <= burst_end
                                         and row["kafka_offset_lag"] is not None]
                        positive = any(row["kafka_offset_lag"] > 0 for row in burst_samples)
                        cleared = next((row for row in samples if row["estimated_container_monotonic"] is not None
                                        and row["estimated_container_monotonic"] >= burst_end
                                        and row["kafka_offset_lag"] == 0 and row["producer_offsets_observed_at"] is not None
                                        and (row["producer_offsets_observed_monotonic"] or 0) >= burst_end), None)
                        result["burst_backlog"] = {
                            "positive_lag_observed_during_burst": positive,
                            "seconds_after_burst_to_first_observed_zero_lag":
                                cleared["estimated_container_monotonic"] - burst_end if positive and cleared else None,
                            "scope": "Sampled Q1 offset lag only; not proof of all-stage recovery or sustained overload",
                        }
                    result["validity_issues"] = []
                    for field in ("clean_duplicate_ids", "clean_ids_absent_from_publication_journal"):
                        if report[field]:
                            result["validity_issues"].append(field)
                    if drained and report["published_ids_not_archived"]:
                        result["validity_issues"].append("published_ids_not_archived")
                    if profile != "duplicate-late" and drained and (report["archived_ids_excluded_from_clean"] or report["clean_inspections_minus_served"]):
                        result["validity_issues"].append("unexpected_inspection_exclusion")
                    if profile != "duplicate-late" and drained and not report["history_matches_served_totals"]:
                        result["validity_issues"].append("historical_totals_differ_from_serving")
                    if args.with_serving and drained and report.get("clean_inspections_minus_projected") != 0:
                        result["validity_issues"].append("inspection_projection_incomplete")
                    result["result"] = "completed" if drained else "drain_timeout"
                    if result["validity_issues"]:
                        result["result"] = "failed"
                except (Exception, KeyboardInterrupt) as error:
                    result.update({"result": "failed", "error": f"{type(error).__name__}: {error}"})
                finally:
                    result["workers"] = {role: state["latest"].get("ready") for role, state in processes.items()
                                         if role in monitored}
                    for state in processes.values():
                        if state["process"].poll() is None:
                            subprocess.run(["docker", "stop", "--time", "20", state["name"]], capture_output=True)
                            state["process"].wait(timeout=40)
                        state["thread"].join(timeout=10)
                        state["log"].close()
                    (destination / "result.json").write_text(json.dumps(result, indent=2) + "\n")
                    results.append(result)
                    (suite / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                print(f"{identifier}: {result['result']}; results at {destination}", flush=True)
                if result["result"] == "failed":
                    raise SystemExit(1)
    comparison = []
    for threads, rate in itertools.product(args.threads, rates):
        for profile in args.profiles:
            runs = [row for row in results if row["threads_per_driver"] == threads and row["target_rate"] == rate and row["profile"] == profile]
            completed = [row for row in runs if row["result"] == "completed"]
            processed_rates = [row["report"]["measurement_processed_inspections_per_second"] for row in completed]
            p95 = [row["report"]["archival_commit_observed_latency_seconds"]["p95"] for row in completed
                   if row["report"]["archival_commit_observed_latency_seconds"]["p95"] is not None]
            comparison.append({"threads_per_driver": threads, "profile": profile,
                               "target_rate": rate, "serving_enabled": args.with_serving,
                               "attempts": len(runs), "completed_repetitions": len(completed),
                               "incomplete_repetitions": len(runs) - len(completed),
                               "processed_inspections_per_second_mean": statistics.mean(processed_rates) if processed_rates else None,
                               "archival_p95_seconds_median": statistics.median(p95) if p95 else None,
                               "generator_deadline_overrun_repetitions": sum(row["report"]["publications_after_measurement_deadline"] > 0 for row in completed),
                               "scope": "Completed runs only; incomplete run reports retained separately"})
    (suite / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")
    print(f"Suite results: {suite}", flush=True)
    if any(row["result"] != "completed" for row in results):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
