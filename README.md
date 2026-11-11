# Big Data Quality Monitor

Project for the **Big Data in Industry 4.0** university course, developed by a three-person team.

We want to monitor product quality across simulated production lines. A vision model will check product images, Kafka will carry inspection results, and Spark will calculate quality statistics. We plan to save inspection history in Parquet and display results in a Streamlit dashboard.

## Current stage

The local Spark setup, an [inspection event format](docs/event-schema.md), and a Kafka publication step are implemented. A repeatable sample contains 120 made-up inspections across two lines: 60 per line, with 6 rejects on A and 12 on B. The producer publishes that sample to a local Kafka broker, and a separate integration check reads it back and verifies the totals. Spark Structured Streaming validates these messages and archives them in Parquet, including invalid payloads with error reasons. Separate streaming jobs deduplicate valid records into clean Parquet and display provisional or finalized quality windows. Explicit progress controls close finite runs. A SQLite sink now persists window snapshots and threshold alert status, and a local MVTec manifest tool prepares reproducible splits. A read-only Streamlit dashboard now displays stored windows and alert evaluations. The C07 store unit checks pass; a clean-archive-to-SQLite writer run is also checked. Full pipeline crash-recovery verification and C09 execution on actual local data remain pending. The script in `scripts/check_spark.py` remains the original four-row batch smoke check.

## Roadmap

- [x] Local Spark batch smoke check in Docker Compose.
- [x] Versioned inspection contract, validator, deterministic sample, and unit tests.
- [x] Local Kafka broker, inspection producer, and broker round-trip check.
- [x] Read Kafka events with Spark Structured Streaming; validate and retain invalid records with error reasons.
- [x] Deduplicate valid inspections within an event-time watermark into clean Parquet.
- [x] Display provisional event-time quality windows from the clean archive.
- [x] Finalize quality windows with progress records and verify delayed arrivals.
- [x] Store raw inspection history in Parquet with persistent ingestion checkpoints.
- [x] Implement transactional SQLite window snapshots and deterministic threshold alerts (C07; full pipeline/recovery verification pending).
- [x] Implement reproducible local MVTec manifests and split integrity checking (C09; local dataset execution pending).
- [ ] Add historical queries and reconciliation.
- [ ] Run MVTec AD images through PatchCore with a separately evaluated threshold.
- [x] Display stored windows and alert evaluations in a read-only Streamlit dashboard.
- [ ] Add real inspection-image previews and measure correctness, throughput, and delays.

## Publish inspections to Kafka

With Docker running, execute from the repository root (WSL with Docker integration is also supported):

```bash
docker compose run --build --rm producer
docker compose run --build --rm kafka-check
```

Compose starts a single Kafka broker and creates the `inspections` topic with two partitions and replication factor 1. The first command publishes 120 inspections and prints the run ID only after every message is acknowledged. The second command publishes its own fresh run, reads it back, and checks IDs, line keys, and fixture totals (A: 60/6; B: 60/12). It exits nonzero on a delivery error, timeout, or mismatched result. Repeating the check works with existing topic history.

The producer uses `line_id` as the Kafka key; keys can share a partition. Each invocation gets a fresh run ID and distinct inspection/product IDs. To deliberately replay the same logical inspections, use `docker compose run --rm producer --run-id example-run` twice. Idempotence covers producer retries within a session; separate invocations can append duplicates, and the clean-archive step removes duplicates within its documented watermark horizon.

For a paced replay, set a target rate across both lines:

```bash
docker compose run --build --rm producer --rate 20
```

This sends the first inspection immediately and schedules subsequent inspections against a monotonic clock (120 records at 20/s take about 5.95 seconds before delivery flush). The schedule does not wait for individual delivery acknowledgements. If publication falls behind, overdue records are enqueued without an extra delay; the rate is a target, not a measured throughput guarantee. Omit `--rate` for the original unpaced replay. Zero, negative, and non-finite rates are rejected.

`published_at` is set from the wall clock after any pacing wait, before each message is enqueued. `event_time` keeps the synthetic fixture time; pacing does not change event-time windows. Image paths and predictions remain placeholders. This is a finite replay without image inference. Publication acknowledgement does not measure aggregate or dashboard latency.

Kafka is available only inside the Compose network at `kafka:19092`; no host port is published. Broker data is kept in the `kafka-data` volume across `docker compose down` / restart. To stop services while keeping history:

```bash
docker compose down
```

The broker and Python client are pinned to `apache/kafka:4.1.2` and `confluent-kafka==2.16.0`. The producer image uses Python 3.12.12. A single broker provides local buffering and replay, without broker-failure redundancy.

## Archive inspections with Spark

Publish a sample with a new run ID, then drain the currently available Kafka messages:

```bash
docker compose run --build --rm producer --run-id archive-demo-1
docker compose run --build --rm spark-ingest --once
```

Use a different run ID for each new sample; deliberately publishing the same logical inspections twice will still create two records at this stage. For ongoing ingestion, omit `--once`. Stop that process before running another ingestion job with the same checkpoint.

The Spark–Kafka connector is pinned to `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`, matching Spark 4.0.2. Its first run downloads Maven dependencies; these are cached in the `archive-data` volume. The same volume stores `/data/raw` and `/data/checkpoints/ingest`, so ordinary `docker compose down` preserves both. Keep output and checkpoint together; do not delete one or reuse them with a different topic/query. `startingOffsets=earliest` applies only to the first run; later runs resume from their checkpoint. Kafka retention still limits how long an unread message is available.

Every archived row includes the Kafka topic, partition, offset, key, Kafka timestamp, ingestion timestamp, original JSON payload, parsed `event` fields, `is_valid`, and `validation_error`. The existing Python event validator runs on the original payload before Spark can coerce incorrect types. Invalid messages remain in the same raw archive, tagged `is_valid=false`; this is a logical quarantine, not a separate directory. Consumers must filter for valid records. Event timestamps remain strings in the parsed structure for now.

To check the archived sample against the existing expected counts:

```bash
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-ingest --master 'local[2]' /app/quality_monitor/archive_check.py --run-id archive-demo-1
```

This explicitly invoked integration check verifies fixture totals and that each Kafka topic/partition/offset appears only once. After rerunning ingestion with the same checkpoint and no new input, it should still pass. It does not claim logical inspection deduplication or fault tolerance across every kind of failure. Raw payloads and volumes are local runtime data, not Git artifacts.

## Deduplicate valid inspections

After ingestion has created the raw archive, run:

```bash
docker compose run --build --rm spark-deduplicate --once
```

This reads `/data/raw`, filters `is_valid=true`, converts `event_time` to a Spark timestamp in UTC, and deduplicates by `(run_id, inspection_id)` using a two-minute event-time watermark. Validated event fields are flattened in `/data/clean`, with the original topic/partition/offset and ingestion timestamp retained. Raw history, including invalid messages and repeated inspections, is unchanged.

The deduplication job has its own persistent checkpoint at `/data/checkpoints/deduplicate`. Omit `--once` for ongoing file-stream processing. Run only one job per checkpoint; keep its input, output, checkpoint, and shuffle settings consistent on restart. The local job uses two shuffle partitions. No Kafka connector is needed for this file-to-file stage.

For the sample run from the ingestion example, reuse the existing check:

```bash
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-ingest --master 'local[2]' /app/quality_monitor/archive_check.py --path /data/clean --clean --run-id archive-demo-1
```

`--clean` checks the flattened fields and also rejects repeated logical inspection identities. The expected sample still has 120 inspections: A=60/6 and B=60/12. This check is explicitly invoked, not part of the fast unit suite.

To exercise duplicates, publish ten existing IDs with the same run ID using `publish_events` and the first ten records from `prepare_events`, then run ingestion and deduplication again. A separate producer invocation updates publication time but preserves those logical identities. Raw history grows; clean counts should remain unchanged while the IDs remain within the deduplication horizon. The original event time must be preserved for these retries.

The watermark uses the largest event time seen across the stream, not elapsed wall-clock time or a separate clock for each run. It bounds state, so this is not permanent uniqueness enforcement: sufficiently old records can be dropped and identities can be forgotten. Replaying older synthetic dates after newer data has advanced the watermark is outside the guaranteed range; use a separate raw input/output/checkpoint set for an isolated historical experiment. Conflicting predictions with the same logical identity are not reconciled; do not use duplicate IDs for a new inference result. Progress controls pass through deduplication with their own identities and never contribute to quality counts.

## Preview quality windows

After deduplication has created the clean archive, run:

```bash
docker compose run --build --rm spark-windows --once
```

Omit `--once` to process new clean files every five seconds. The job groups inspections into non-overlapping one-minute UTC event-time windows, separately by run, line, category, model version, and threshold version. It prints inspection counts, predicted reject counts, and predicted reject rates in console update mode. The sample's minute should show A=60/6 (0.10) and B=60/12 (0.20).

`--window '30 seconds'` and `--lateness '2 minutes'` configure the window and watermark allowance. The default checkpoint is `/data/checkpoints/windows-v2-preview`, or `/data/checkpoints/windows-v2-final` with `--final`; use one writer per checkpoint and keep the input, mode, grouping, window, watermark, and shuffle settings consistent on restart. Use a separate checkpoint when changing query settings. The earlier `/data/checkpoints/windows` belongs to the inspection-only aggregate state: do not reuse it with this version. Keep old outputs/checkpoints for inspection; replay into new state deliberately rather than deleting existing runtime data.

Default console rows are provisional snapshots, not increments to sum. Add `--final` to emit only watermark-finalized windows using append mode. Controls contribute zero counts; control-only windows are suppressed after aggregation. A restart resumes processing without reprinting all past rows. The console shows at most 20 rows per batch and is intended for small demos, not durable serving. A finite or idle stream does not advance its watermark by wall-clock time. Add `--database /data/serving/quality.sqlite` to persist snapshots and rule statuses; see [the serving-store instructions](docs/serving-store.md).

## Close a finite fixture run

Use an isolated topic and paths for each historical replay. Start Q3 in final mode in another terminal after the initial clean archive exists, **before publishing controls**. This lets it consume inspections before later progress advances its watermark. For example, use `c06-demo-1` as a new topic, experiment directory, and run ID (choose a new name if it already exists):

```bash
docker compose run --rm kafka-init --bootstrap-server kafka:19092 --create --if-not-exists --topic c06-demo-1 --partitions 2 --replication-factor 1
docker compose run --build --rm producer --topic c06-demo-1 --run-id c06-demo-1
docker compose run --build --rm spark-ingest --topic c06-demo-1 --output /data/c06-demo-1/raw --checkpoint /data/c06-demo-1/q1 --once
docker compose run --build --rm spark-deduplicate --input /data/c06-demo-1/raw --output /data/c06-demo-1/clean --checkpoint /data/c06-demo-1/q2 --once
```

In the second terminal, keep this running; wait for the initial batch to finish before continuing in the first terminal. No finalized fixture row is expected yet:

```bash
docker compose run --build --rm spark-windows --input /data/c06-demo-1/clean --checkpoint /data/c06-demo-1/q3-final --final
```

Back in the first terminal, publish controls for the same run and drain upstream stages in order:

```bash
docker compose run --build --rm producer --topic c06-demo-1 --run-id c06-demo-1 --progress-at '2026-11-07T12:04:00Z'
docker compose run --rm spark-ingest --topic c06-demo-1 --output /data/c06-demo-1/raw --checkpoint /data/c06-demo-1/q1 --once
docker compose run --rm spark-deduplicate --input /data/c06-demo-1/raw --output /data/c06-demo-1/clean --checkpoint /data/c06-demo-1/q2 --once
```

Q3 should print two finalized rows for `12:00–12:01 UTC`: A=60/6 (0.10), B=60/12 (0.20). The dedicated topic keeps previous runs and their progress controls out of this replay. A control timestamp must be strictly beyond the evaluated window end plus Q3's allowance, and beyond Q2's watermark horizon; `12:04Z` covers this fixture with the default settings. Do not publish controls while intended records remain undrained in any stage, and do not start final Q3 for the first time over an archive already containing both historical inspections and closure controls. File enumeration is not a replay-order guarantee. Controls never imply completed inference or production health.

## Check window correctness and delayed arrivals

Run the explicitly invoked integration scenario (separate from unit tests):

```bash
docker compose run --build --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark-windows --master 'local[2]' --conf spark.ui.enabled=false /app/scripts/check_pipeline.py
```

It uses the real Q1 validation transformation on Kafka-shaped rows, then separate Q2/Q3 file streams with persistent checkpoints. It requires no broker and does not test Kafka transport or crash recovery. Each invocation retains artifacts and `result.json` in a new `/data/c06-check-<id>` directory in `archive-data`.

The scenario injects ten duplicates and two invalid payloads, holds back two reject inspections, advances event time to `12:02:30Z`, then supplies those inspections at `12:00:40Z` within the allowance. Controls at `12:04Z` finalize the fixture minute. It compares the two final rows with the separate fixture oracle, checks window boundaries/group metadata and exclusion of control-only windows, then injects an older distinct inspection and verifies that closed results stay unchanged. It records whether that inspection survived Q2; it does not assume all beyond-watermark arrivals are dropped under every batching arrangement.

## View stored quality results

Create the SQLite serving database with the [window writer](docs/serving-store.md), then start the dashboard:

```bash
docker compose up --build -d dashboard
```

Open [localhost:8501](http://localhost:8501). This service publishes only to the local loopback address. It reads `/data/serving/quality.sqlite` through a read-only SQLite connection and a mount limited to the serving subdirectory; it does not start Kafka or Spark. Only the serving subdirectory is writable so SQLite can create WAL coordination sidecars; raw and clean history are not mounted into the dashboard. The application uses `mode=ro` and SELECT queries. The window writer must first create the serving directory, which the Docker volume subpath requires. If the directory exists but has no database or stored windows, the app displays a setup/no-data message rather than creating a database.

Select a run, detector/category version, and line to see inspection/reject totals, a reject-rate trend, provisional/final window rows, and rule evaluations. Summaries use only the selected snapshots and detector version. Active rule results are shown separately from clear, resolved, or insufficient-data evaluations. Fixture model scores remain synthetic; there is no image inference or image viewer yet.

The view refreshes every ten seconds and has a manual Refresh button. It shows the stored update timestamp and warns after five minutes without window updates; this can mean an idle source as well as stopped processing. It does not certify production health or count inspections that never reached the serving writer. Provisional windows can change; use the separate final writer/database described in the serving-store instructions for finalized-only results.

This first view reads the small aggregate tables, not the raw inspection archive. It is intended for local demo data; pagination and larger-query tuning can follow measured need. To use another serving database inside the mounted volume, change the `QUALITY_DATABASE` setting. Keep database files outside Git.

## Run the Spark check

For the C07 SQLite writer, retry contract, rule statuses, and CLI options, see [the serving-store instructions](docs/serving-store.md). For C09 dataset layout, manifest commands, exact validation split policy, and source attribution, see [the local MVTec manifest instructions](docs/data-manifest.md). Both additions use standard-library Python alongside the existing Spark environment; no model dependency or dataset download is required to import their helper modules.

Requirements: Docker Engine or Docker Desktop with Linux containers, and Docker Compose v2. On Windows, run the commands from WSL with Docker integration enabled.

From the repository root:

```bash
docker compose run --rm spark-check
```

The first run downloads the image and may take a while. Spark and its Python/Java runtime run inside the container, so a host Java installation or Python virtual environment is not needed for this step. No ports are published and no datasets are downloaded.

After the Spark startup logs, the script should print:

```text
Line A: 3 inspections, 1 rejects
Line B: 1 inspections, 0 rejects
Local Spark check passed.
```

A different result raises an error and returns a nonzero exit code. The script also prints the Python and Spark versions. This remains the setup smoke check. The event-format checks below run separately without Spark.

## Inspection sample

With Python 3.10 or later, run the unit checks from the repository root:

```bash
python3 -m unittest discover -s tests -v
```

No additional Python dependencies are needed for unit tests. They cover sample totals, invalid inputs, run identities, publication timestamps, and delivery-error handling with a fake producer. The real broker is tested separately with `kafka-check`. See [the event format](docs/event-schema.md) for the fields and generator command. Scores and image paths in this sample are placeholders.

## Next steps
- Run the deferred C07 sink/recovery checks and C09 manifest creation on local data.
- Add visual anomaly detection with MVTec AD images and PatchCore.
- Add real inspection-image previews and measure correctness, throughput, and delays.

Checked on 6 November 2026 using Docker Engine 29.2.1 on WSL2 (Linux x86-64). The container reported Spark 4.0.2, Python 3.10.12, and Java 17.0.17.

On 7 November 2026, all five unit tests and the Kafka round-trip check passed using Docker Desktop with Linux containers. The broker check confirmed 120 inspections: 60/6 rejects on A and 60/12 on B.

On 8 November 2026, the five existing unit checks passed. The Kafka-to-Parquet check preserved 120 valid inspections (A: 60/6, B: 60/12) and two deliberately invalid messages (malformed JSON and an integer in `is_reject`). Resuming with the same checkpoint, including after a normal broker restart, preserved these results without repeated Kafka offsets. The archive check can inspect injected invalid messages with `--invalid-key KEY --expected-invalid COUNT`.

Also on 8 November 2026, a separate run added 120 inspections and ten repeats, followed by another ten repeats after deduplication restarted with its existing checkpoint. Clean fixture totals remained A=60/6 and B=60/12. Raw history held 140 records for that run; clean history held 120, with no invalid source records carried over. These checks cover duplicates within the configured watermark range, not behavior after state eviction.

On 10 November 2026, `scripts/check_pipeline.py` passed on Spark 4.0.2 / Java 17.0.17, Docker Engine 28.5.1 / WSL2 amd64, with `local[2]` and two shuffle partitions. The observed Q3 watermark before the two delayed inspections was `2026-11-07T12:00:30.000Z`; both inspections at `12:00:40Z` were accepted. Four controls passed Q2; only the two fixture quality windows were finalized, matching A=60/6 (0.10) and B=60/12 (0.20). Ten duplicate injections did not add inspections, and two invalid payloads remained in raw history. The later beyond-allowance inspection was excluded by Q2 in this run and did not revise closed results. Artifacts and the measured summary remain in `archive-data:/data/c06-check-a62380f818ae400a969bdfbbb42089e1/result.json`. All five fast unit checks, Compose validation, and `git diff --check` also passed. This verifies C06's Spark behavior without a new Kafka round-trip, crash-recovery test, or peer review.

On 11 November 2026, all seven existing unit checks and Compose validation passed. A temporary Streamlit AppTest smoke verified missing/empty database states, 120 inspections / 18 predicted rejects, run/line filtering, the B-line active alert, provisional/final labels, stale-data warnings, and reading the previous committed snapshot while a WAL writer held an uncommitted update. The local HTTP health endpoint returned 200. The existing Spark window writer then drained the local clean archive into the serving database; the dashboard read its serving-subdirectory mount through a read-only SQL connection and showed 120 inspections, 18 predicted rejects, and clear/active rule statuses for the selected fixture run. No UI test suite was added; this does not verify the full live Kafka-to-dashboard pipeline or real image inference.

The Spark image and Kafka connector are pinned to version 4.0.2. Model dependencies will be pinned when inference is added.

## References

- [Apache Spark Docker images](https://github.com/apache/spark-docker)
- [Apache Kafka Docker setup](https://kafka.apache.org/41/getting-started/docker/)
- [Confluent Python producer documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Spark–Kafka integration](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html)
- [Spark bounded deduplication](https://spark.apache.org/docs/4.0.2/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html)
- [Spark 4.0.2 documentation](https://spark.apache.org/docs/4.0.2/)
- [MVTec AD dataset](https://www.mvtec.com/research-teaching/datasets/mvtec-ad)
