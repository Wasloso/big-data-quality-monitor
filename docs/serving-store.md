# Window serving view and threshold alerts (C07)

`quality_monitor.store` uses Python's standard-library SQLite module. Raw/clean Parquet remains the historical archive; SQLite stores only bounded aggregate snapshots and their rule status. The `spark-windows` service already mounts `/data`, so the database survives normal Compose shutdown.

After a clean archive exists, start the provisional serving writer:

```bash
docker compose run --build --rm spark-windows --database /data/serving/quality-v2.sqlite --alert-threshold 0.15 --minimum-inspections 30
```

Use `--input` and `--checkpoint` for an isolated experiment as in the C06 instructions. `--once` drains available files. `--final` writes only watermark-finalized rows instead of provisional updates. Default SQLite checkpoints are `/data/v2/checkpoints/windows-v2-sqlite-preview` and `/data/v2/checkpoints/windows-v2-sqlite-final`; console checkpoints use the same versioned `/data/v2/checkpoints` base. Follow the C06 staged-drain/progress protocol for finalization. Only one driver should write a serving database at a time.

`foreachBatch` collects at most `--max-store-rows` aggregate rows (default 1000), using an extra row to detect overflow. An oversized batch fails before any database write; it is not truncated. Change this bound to match a measured aggregate workload. No inspection-history collection is used.

Tables are directly inspectable with a SQLite client:

| Table | Meaning |
| --- | --- |
| `windows` | One snapshot per run, line, category, model version, threshold version, and UTC start/end. Includes counts, recomputed reject rate, final flag, and last update time. |
| `alerts` | One rule status per logical window and rule ID; includes threshold, minimum count, provisional flag, and update time. Join with `windows` using `window_key`. |
| `processed_batches` | Applied query-epoch/batch IDs, committed atomically with window and alert changes. |

The rule is `reject-rate-v1`: a rate **strictly greater than** the configured threshold activates an alert once the count reaches the minimum. Equality does not activate it. Defaults give no alert for fixture A (0.10) and an active alert for B (0.20), after at least 30 inspections. These are demonstration settings, not validated production limits. Zero-count controls create no window/alert row. Rule statuses are `insufficient_data`, `clear`, `active`, and `resolved`; an active provisional alert that falls back to or below the threshold becomes `resolved`. Reactivation uses the same alert ID. Rows with `clear`/`insufficient_data` describe rule evaluation, not triggered alerts. No notification is sent.

Window IDs derive from the complete logical key. Rule IDs include the comparison, threshold, minimum, and rule version. Alert IDs combine the window and rule IDs, so retrying a batch updates the same logical result. Each batch uses one SQLite transaction; an exception rolls back its window changes, alert changes, and ledger entry together. Retrying a committed epoch/batch is a no-op.

`quality-store.json` in the checkpoint directory persists a query epoch and its input/database/window/lateness/mode/rule contract. A restart reuses the epoch; changing those settings requires a new checkpoint. If Spark has committed batches, startup checks that the existing database has its serving tables and the ledger entry for the latest committed batch/epoch. A missing database, missing table or mismatched ledger fails before Spark resumes; a fresh query with no committed batch may still initialize its database. This also detects a database restored from before the latest Spark commit, but does not audit every row for corruption. Keep the file with Spark's checkpoint and preserve the database with its ledger. These are local filesystem paths, not cloud checkpoint URIs. Do not attach this sink to an old console checkpoint, move only part of the runtime state, or recreate a missing database and expect a resumed Spark checkpoint to reconstruct old windows.

A new checkpoint has a new epoch, while logical window/alert keys remain stable. Replays must rebuild complete snapshots from the same supported input. Regressing counts or conflicting finalized snapshots fail rather than silently replacing results. Final rows cannot be downgraded by provisional output. Provisional update mode does not emit a separate finalization notification: its stored rows remain provisional even after Spark evicts state. Choose `--final` with its own input/checkpoint and serving database when the consumer needs only closed results; do not switch an existing query's output mode.

WAL permits readers to inspect committed snapshots while the single driver writes. This is a tested-design target, not a claim of end-to-end exactly-once delivery or completed crash-recovery verification. Integration/recovery execution remains a separate deliberate check.
