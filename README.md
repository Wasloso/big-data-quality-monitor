# Big Data Quality Monitor

Project for the **Big Data in Industry 4.0** university course, developed by a three-person team.

We want to monitor product quality across simulated production lines. A vision model will check product images, Kafka will carry inspection results, and Spark will calculate quality statistics. We plan to save inspection history in Parquet and display results in a Streamlit dashboard.

## Current stage

The local Spark setup, an [inspection event format](docs/event-schema.md), and a Kafka publication step are implemented. A repeatable sample contains 120 made-up inspections across two lines: 60 per line, with 6 rejects on A and 12 on B. The producer publishes that sample to a local Kafka broker, and a separate integration check reads it back and verifies the totals. Spark Structured Streaming now validates these messages and archives them in Parquet, including invalid payloads with error reasons. Deduplication and quality windows are next. The script in `scripts/check_spark.py` remains the original four-row batch smoke check.

## Roadmap

- [x] Local Spark batch smoke check in Docker Compose.
- [x] Versioned inspection contract, validator, deterministic sample, and unit tests.
- [x] Local Kafka broker, inspection producer, and broker round-trip check.
- [x] Read Kafka events with Spark Structured Streaming; validate and retain invalid records with error reasons.
- [ ] Add deduplication, event-time windows, watermarks, and correctness checks against fixture totals.
- [x] Store raw inspection history in Parquet with persistent ingestion checkpoints.
- [ ] Add historical queries and quality alerts.
- [ ] Run MVTec AD images through PatchCore with a separately evaluated threshold.
- [ ] Build a Streamlit dashboard and evaluate correctness, throughput, and delays.

## Publish inspections to Kafka

With Docker running, execute from the repository root (WSL with Docker integration is also supported):

```bash
docker compose run --build --rm producer
docker compose run --build --rm kafka-check
```

Compose starts a single Kafka broker and creates the `inspections` topic with two partitions and replication factor 1. The first command publishes 120 inspections and prints the run ID only after every message is acknowledged. The second command publishes its own fresh run, reads it back, and checks IDs, line keys, and fixture totals (A: 60/6; B: 60/12). It exits nonzero on a delivery error, timeout, or mismatched result. Repeating the check works with existing topic history.

The producer uses `line_id` as the Kafka key; keys can share a partition. Each invocation gets a fresh run ID and distinct inspection/product IDs. To deliberately replay the same logical inspections, use `docker compose run --rm producer --run-id example-run` twice. Idempotence covers producer retries within a session; separate invocations can append duplicates, and downstream deduplication is still planned.

`published_at` is set from the wall clock before each message is enqueued. `event_time` keeps the synthetic fixture time; image paths and predictions remain placeholders. This is a finite replay, without traffic pacing or image inference. Publication acknowledgement does not measure aggregate or dashboard latency.

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

## Run the Spark check

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
- Add bounded inspection deduplication and event-time quality windows over the archive.
- Add visual anomaly detection with MVTec AD images and PatchCore.
- Build the dashboard and measure correctness, throughput, and delays.

Checked on 6 November 2026 using Docker Engine 29.2.1 on WSL2 (Linux x86-64). The container reported Spark 4.0.2, Python 3.10.12, and Java 17.0.17.

On 7 November 2026, all five unit tests and the Kafka round-trip check passed using Docker Desktop with Linux containers. The broker check confirmed 120 inspections: 60/6 rejects on A and 60/12 on B.

On 8 November 2026, the five existing unit checks passed. The Kafka-to-Parquet check preserved 120 valid inspections (A: 60/6, B: 60/12) and two deliberately invalid messages (malformed JSON and an integer in `is_reject`). Resuming with the same checkpoint, including after a normal broker restart, preserved these results without repeated Kafka offsets. The archive check can inspect injected invalid messages with `--invalid-key KEY --expected-invalid COUNT`.

The Spark image and Kafka connector are pinned to version 4.0.2. Model dependencies will be pinned when inference is added.

## References

- [Apache Spark Docker images](https://github.com/apache/spark-docker)
- [Apache Kafka Docker setup](https://kafka.apache.org/41/getting-started/docker/)
- [Confluent Python producer documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Spark–Kafka integration](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html)
- [Spark 4.0.2 documentation](https://spark.apache.org/docs/4.0.2/)
- [MVTec AD dataset](https://www.mvtec.com/research-teaching/datasets/mvtec-ad)
