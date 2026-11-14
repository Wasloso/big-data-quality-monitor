# Recent inspection serving view (UI3)

The dashboard needs individual inspection references as well as window totals. `quality_monitor.project_inspections` reads the existing clean Parquet stream and writes `/data/serving/stations.sqlite`. Q1/Q2/Q3 and their checkpoints are unchanged. The full historical archive remains Parquet; this database is a bounded recent-item view, not the source for total production counts.

Once clean Parquet exists:

```bash
docker compose run --build --rm --no-deps spark-inspections --once
```

Omit `--once` to process new files every five seconds. The defaults are input `/data/clean`, database `/data/serving/stations.sqlite` and checkpoint `/data/checkpoints/inspection-serving-v1`. Use `--input`, `--database` and `--checkpoint` together for an isolated replay. One process writes this database at a time. The API already mounts the serving subdirectory; it does not need access to raw/clean files.

The projection stores valid inspections only. Controls contribute no sample or latest observation. Its two views are:

- `inspection_samples`: identity, station, category, model/threshold version, acquisition/publication timestamps, score/decision and image reference. Defaults retain the most recent 1,000 items **per run/station**, across categories and detector versions.
- `station_latest`: latest inspection per run/station/category/model/threshold, ordered by acquisition time and inspection ID as a deterministic tie-breaker. Delayed older items cannot move it backwards. This is an observation, not a physical machine heartbeat or status.

An index supports recent-item lookup and retention. UTC times use consistent microsecond precision so timestamp ordering is stable. Different categories need no separate database or schema; the model and threshold versions remain attached to every item. No detector is trained by this step.

`--retention` changes the sample limit. Pruning does not modify the independent quality windows or their counts. Retention is per run/station, not a global database-size guarantee; keep the number of experimental runs deliberate and retain their Parquet history separately.

Each `foreachBatch` write commits samples, latest observations, retention and its processed epoch/batch ledger together. Retrying a committed batch is a no-op. Repeated inspection IDs retain the first stored result while present; old/pruned events do not get to replace a newer latest observation. The sink is not a permanent deduplication registry and does not establish exactly-once behavior for the full pipeline.

The driver accepts at most `--max-batch-rows` inspections (default 1,000) per batch; an extra row detects overflow and fails before writing instead of silently discarding input. One file is read per trigger, but an individual large file can still exceed this bound. Increase it only for a measured, memory-safe workload. This is independent of the 1,000-row retention limit.

The checkpoint persists the input/database/retention/batch-limit contract through the existing query-epoch helper. Keep it with the database and ledger. A completed Spark batch requires matching serving tables and the latest epoch/batch entry before resume. Changed settings need a new compatible checkpoint/replay; deleting only the database does not rebuild historical items.

Current fixtures have placeholder image paths. Actual image files, image HTTP serving, machine context and live model inference remain later increments. `image_uri` is stored metadata and is not permission to read an arbitrary host file. Quality and item projections update independently; compare their coverage explicitly rather than assuming an atomic snapshot across both SQLite files.
