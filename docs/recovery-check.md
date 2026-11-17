# Retained-state processor recovery (C13)

The existing `scripts/check_pipeline.py` has an explicit `--recovery` mode using a real Kafka topic and a separate Q3 Spark driver process. It reuses the 120-inspection fixture, independent counts, Q1 validation, Q2 deduplication, Q3 aggregation/SQLite sink, and C12 historical reconciliation. The default broker-free correctness command remains available.

Run from the repository root with Docker Desktop/Engine running Linux containers. Use a **new topic name** on every attempt, including after a failed attempt. This example creates two partitions with the project's existing single broker:

```bash
docker compose run --rm kafka-init --bootstrap-server kafka:19092 --create --if-not-exists --topic c13-recovery-01 --partitions 2 --replication-factor 1
docker compose run --build --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-windows --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 --conf spark.jars.ivy=/data/ivy --conf spark.ui.enabled=false /app/scripts/check_pipeline.py --recovery --topic c13-recovery-01
```

The connector matches the pinned Spark 4.0.2 version. Its first download and the two local Spark drivers need sufficient memory; this is a deliberate integration activity, separate from the fast unit suite. `--bootstrap-servers` defaults to `kafka:19092`; `--timeout` defaults to 180 seconds per worker condition. A nonempty topic is rejected, rather than reset. Do not publish unrelated records to the dedicated topic while the scenario runs.

Each invocation creates `/data/c13-check-<id>` inside the existing `archive-data` volume. It prints that path at startup and retains raw/clean Parquet, all checkpoints, SQLite, driver logs, fault/retry markers, `history.json`, and `result.json`. An exception records a failed result and returns nonzero. Initial failures before artifact setup can leave no report. The code never deletes the topic, archive, database or checkpoint. Kafka remains running afterward; only the scenario's own Spark workers are stopped. Do not run `docker compose down -v` as part of recovery.

The scenario proceeds as follows:

1. Publish 60 fixture inspections. Q1 reads real Kafka offsets and Q2 creates the clean archive. Q3 runs in its own process and stores provisional snapshots. Wait until Spark commits its first batch, then publish another 58 inspections, holding back the two reject inspections from second 40.
2. On the later batch that stores 118 inspections, a check-only callback blocks **after SQLite commits but before Spark commits it**. The parent confirms that an earlier Spark commit exists and the interrupted batch's commit is absent, then sends SIGKILL to that worker's isolated process group. Recovery must restore already committed aggregate state as well as retry the interrupted batch. The fault condition uses the stored count because update mode can also re-emit snapshots in a no-data batch. Production sink behavior is unchanged.
3. Keep Q1/Q2 and Kafka alive. Publish progress controls at `12:02:30Z`, then one held-back inspection, ten repeated IDs and two invalid payloads. Q3's serving counts must remain at 118 while it is down. No state is reset.
4. Start a new Q3 process with the same checkpoint, query epoch, database and settings. The interrupted batch must run again and leave **all SQLite windows, alerts and batch-ledger rows unchanged**. It then consumes the queued clean records and reaches 119 inspections. The Spark commit for the replayed batch must now exist.
5. Wait for Q3's watermark to reach `12:00:30Z`; send the other held-back inspection at `12:00:40Z`. Both intended late inspections must survive. Counts reach 120 without changing the logical alert/window IDs. Compare each recovered provisional window's inspection count, rejects and reject rate with the independent oracle before launching the final writer.
6. Stop the recovered provisional worker normally. A separate final-mode checkpoint reads the retained supported history into the same serving database, then new controls at `12:04Z` close the fixture minute. This final pass is an explicit mode change using new state, **not** the recovery being measured; the recovered provisional checkpoint is retained intact.
7. Inject a distinct beyond-horizon inspection. Confirm exact fixture inspection IDs, unique raw Kafka offsets, ten repeated inspection messages, two retained invalid records, four clean controls, and unchanged closed results. Compare final windows and alert keys/status with expectations and run the existing historical reconciliation.

A passing result requires A=60 inspections/6 predicted rejects (10%) and B=60/12 (20%). There must be exactly two final quality windows and two corresponding rule evaluations: A `clear`, B `active`, both non-provisional. The alert IDs must match those created before the interruption.

`restart_to_backlog_catch_up_seconds` measures from relaunch to observing the expected post-backlog serving count, including driver startup, state reload and polling. `downtime_including_backlog_preparation_seconds` measures the intentionally stopped interval. Both use a monotonic clock, independent of synthetic event time. They describe this small recovery scenario; C14 still needs repeated capacity/latency experiments.

The fault covers the critical Q3 sink/checkpoint boundary. It does not establish recovery from every possible Q1/Q2 failure, broker/disk loss, insufficient Kafka retention, the inspection projection, or simultaneous infrastructure failure. Fixture scores verify processing correctness; real-image model behavior is covered separately. Add observed results and the actual code revision only after the command passes; the existence of this runner alone is not execution evidence.

## Observed result — 17 November 2026

The command passed with topic `c13-check-20261117-c` on Spark 4.0.2 / Kafka 4.1.2 / Java 17.0.17, Docker Engine 28.5.1 / WSL2 amd64. Batch 0 had committed a 60-inspection prefix before the later interruption. The process was killed after SQLite committed batch 3, while its Spark commit was absent. The restarted driver reused the epoch and checkpoint, replayed batch 3 without changing the SQLite tables, and caught up to 119 inspections in 32.621 seconds including process startup. The intentionally stopped interval, including backlog preparation, was 11.015 seconds. Both recovered provisional snapshots matched the oracle at 120 inspections before the separate finalization pass.

Final results were A=60/6 (10%), B=60/12 (20%), with stable alert keys, final A `clear` / B `active`, no repeated Kafka offsets, exact fixture identities, and passing historical reconciliation. The raw archive retained both invalid inputs; ten injected duplicates did not add inspections. The two delayed inspections were accepted, and the known beyond-horizon inspection did not appear in clean history.

An earlier committed-prefix attempt caught an overly broad fault selector: Spark update mode re-emitted an existing snapshot during a no-data batch. The selector now waits for the intended 118 stored inspections; the parent additionally requires an earlier Spark commit and the absence of the interrupted batch's commit.

The [compact measured record](results/processor-recovery.json) includes runner hashes and the exact retained artifact path. Full reports remain at `/data/c13-check-b1e83e43234049c88e38dda86bf849c0` in `archive-data`. This is C13 recovery evidence for the scoped Q3 failure; C14 still needs repeated workload measurements.
