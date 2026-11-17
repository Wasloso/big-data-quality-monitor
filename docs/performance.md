# Controlled infrastructure measurements (C14)

`python -m quality_monitor.benchmark` runs a paced fixture-score producer and **three independent Spark driver containers**, using the actual Q1 validator, Q2 bounded deduplication, Q3 quality-window transformation and transactional SQLite writer. Kafka uses a fresh topic per repetition; data/checkpoints/databases stay in the existing `archive-data` named volume under `/data/c14-<id>`. Production checkpoints and serving databases are not reused. Host-side orchestration needs Python 3.10+, Docker and Compose, with commands run from the repository root.

This measures the infrastructure replay track. Predictions are explicitly labelled `fixture-benchmark-v1`; images are placeholders. The fitted-model evaluation and actual image-inference demonstration remain separate tracks.

## Commands

Start with one short functional check:

```bash
python -m quality_monitor.benchmark --threads 1 --profiles steady --repetitions 1 --rate 10 --warmup 2 --duration 8 --trigger-seconds 1
```

For a measured comparison, allow warmup long enough for all three stages to reach a steady cadence, use longer measurement intervals, and repeat at least three times:

```bash
python -m quality_monitor.benchmark --threads 1 2 4 --profiles steady --repetitions 3 --rate 20 --warmup 30 --duration 60
```

Then increase the offered rate under otherwise identical settings until lag/latency grows persistently. Run the other profiles at a chosen controlled rate:

```bash
python -m quality_monitor.benchmark --threads 1 2 4 --profiles burst skew duplicate-late --repetitions 3 --rate 20 --warmup 30 --duration 60 --drain-seconds 120
```

Choose thread counts appropriate to Docker's available hardware. `local[N]` applies **to each of three drivers**, with a CPU limit of N per container. The optional `compose.benchmark.yaml` sets each Spark container's memory limit to 2 GiB (`BENCHMARK_SPARK_MEMORY` overrides it); the pinned Spark image retains its normal Java heap default. The independent producer has a fixed one-CPU / 512 MiB limit. This is a controlled change of local threads and CPU allocation, not a comparison of a fixed total CPU budget. Broker limits and Docker-host totals belong to the recorded configuration. Other applications and production jobs on the same machine affect results; record their presence when interpreting a campaign.

The defaults hold Kafka partitions and Spark shuffle partitions at two. Set `--partitions`, `--shuffle-partitions`, `--trigger-seconds`, `--seed`, and `--payload-bytes` explicitly when comparing other configurations. Padding is extra JSON payload content, not a total packet-size promise. Each command applies the same seed, partition settings, trigger and workload policy to every repetition. `--max-events` caps the planned generator workload at one million messages by default, including warmup; raise that bound deliberately for larger runs.

Messages keep the production `line_id` Kafka key and client partitioner. The two keys can share a partition; inspect recorded end offsets instead of assuming every topic partition carried traffic. Two fixed shuffle partitions also limit stage parallelism. These are properties of the selected local topology, and belong in the interpretation of a `local[1]`/`local[2]`/`local[4]` comparison.

## Workload and publication clock

- `steady`: one constant target rate across the two lines, with alternating line assignments.
- `burst`: equal thirds of the measured interval use base rate, `--burst-multiplier` times base rate (default 4), then base rate again. A multiplier does not prove overload: inspect the measured lag and resource use.
- `skew`: a deterministic seeded selection directs about 90% of acquisitions to A.
- `duplicate-late`: every tenth scheduled message repeats a recent identity and original event time; selected distinct messages have event time shifted back by three minutes. Actual exclusions depend on arrival batches and watermark state, so the report counts them explicitly rather than declaring every older event dropped.

Two bootstrap inspections initialize archive schemas and all stages before the timed generator starts. They use a separate run ID. Warmup also uses a separate run ID, and measurement uses the main run ID. Neither bootstrap nor warmup contributes to measured inspection totals. Their CPU/state/resource effects are part of running the pipeline and are not removed from the system.

The generator schedules against an absolute monotonic clock independently of Spark. It sets actual UTC `published_at` immediately before enqueueing. Synthetic event time starts at the same `2026-11-07T12:00:00Z` origin in every isolated run, then follows scheduled offsets and the duplicate/late rules above. This keeps window alignment comparable across configurations; it is never used to calculate infrastructure latency. The producer does not throttle itself based on consumer progress. Publication, validation, journaling, Kafka offset sampling and delivery-callback work all consume its fixed CPU budget. If the generator falls behind, it sends overdue slots without adding a fresh delay. The report records maximum schedule overrun, actual publications within the fixed measurement interval, and publications after its deadline. Never treat the requested rate as the achieved rate. Delivery/enqueue failures invalidate a run rather than silently reducing its offered load.

## Metrics and their meanings

| Metric | Definition |
| --- | --- |
| Offered messages/s | Journalled actual publications inside the fixed monotonic measurement interval divided by its duration. Includes deliberate duplicate messages. Target rate and late generator publications are reported separately. |
| Processed inspections/s | Increase in committed provisional SQLite inspection counts during that same monotonic interval, divided by its duration. Includes only the main run's unique inspections that reached Q3. Finite short checks may finish most processing during drain instead. |
| Kafka offset lag | Sum of sampled broker end offsets minus Q1's last observed committed end offsets, clipped to zero when samples differ in age. Both observation timestamps are retained. It is Q1 source lag, not a Kafka consumer-group committed-offset metric or all-stage backlog. |
| Archival persistence latency | Real publication-time monotonic stamp to observation of Q1's completed batch, matched by topic/partition/offset ranges. Includes progress polling/scheduling delay; does not use `ingested_at`, which is a processing timestamp rather than commit proof. Quantiles are approximate Spark percentiles over archived message rows, including duplicate messages. |
| Window freshness | Age since the latest observed **change in main-run window counts**, sampled by the host. A no-data snapshot re-emission does not reset it. This is stored-value freshness, not per-image inference latency or browser visibility latency. An idle source can age normally. |
| Aggregate sink cost | Bounded aggregate collection, SQLite transaction and the small post-commit count observation, measured with a monotonic clock per Q3 batch. Compare with trigger intervals and Q3/container load when investigating bottlenecks. |
| Resources | Timestamped `docker stats` CPU, memory, network and block I/O observations for Q1, Q2, Q3 and generator. Reported CPU percentages use Docker's convention (100% is one CPU). Sampled peaks can miss short spikes; monitoring also has overhead. |

The benchmark does not measure browser polling or API/UI rendering. It records provisional serving windows at shutdown; it does not emit future controls to force finalization or calculate final-window latency. Publication-to-Q3 per-inspection latency is also not inferred from aggregate timestamps.

All four measured processes run under the **same Docker Linux kernel** and use its common `time.monotonic()` clock for publication, commit observation and interval boundaries. Actual UTC `published_at` and observation timestamps remain available for audit, and the producer records UTC-elapsed minus monotonic-elapsed drift. A benchmark-only `benchmark_published_monotonic` JSON field is kept in the raw payload; normal event parsing and detector features ignore it. This avoids UTC adjustments or Windows/WSL clock differences corrupting latency. These stamps cannot be compared across independent Docker hosts or time namespaces. Host-side resource/freshness samples estimate current container monotonic time from a received producer clock marker and elapsed host monotonic time; pipe delivery and sampling delay make this alignment approximate. Exact producer and worker interval calculations stay within the common Linux clock.

Spark progress logs retain batch input/processed rates, state-operator statistics, source offsets and watermark observations. Per-event archival latency is joined in Spark over all archived main-run messages, including any generator deadline overruns; inspection history is never collected into the driver. The serving writer keeps the production 1000-aggregate-row bound and fails on overflow rather than truncating. Burst reports also show whether positive Q1 lag was observed and the sampled delay from the burst's end to the first observed zero lag. A null delay means no positive lag was observed or recovery was not observed; it is not silently converted to a successful zero-second recovery.

## Drain, omissions and comparison

After publication finishes, the controller samples lag and stage progress for up to `--drain-seconds`. Normal profiles require the expected main-run unique inspections in SQLite. The duplicate/late profile requires an observed idle Q3 and a settling interval after Q2 consumes the source, because some older IDs may legitimately be excluded. Its report still shows published IDs absent from raw, raw IDs absent from clean, and clean inspection count minus served count; those differences are not silently classified as successful processing or automatically diagnosed as lateness. Only committed Spark progress advances the source counters: idle progress reports may reuse the next batch ID and must not hide the later committed data batch.

Always inspect `drain_settled`, `remaining_kafka_lag`, `published_ids_not_archived`, `archived_ids_excluded_from_clean`, `clean_inspections_minus_served`, and `archived_rows_without_commit_observation`. A timeout returns exit code 2 with all partial data retained; other failures return 1. Persistence percentiles describe only matched archived messages: missing/unprocessed messages and observation coverage remain explicit beside them. They are not counted as fast or removed from the report.

Results go to ignored `artifacts/benchmarks/suite-<id>/`:

- `manifest.json`: revision, source hashes, selected workload, host/Docker capacity, storage and container limits.
- Per-run `result.json`, worker logs, `samples.jsonl` and raw `resources.jsonl`.
- `results.json`: every attempted run, including timeouts/failures.
- `comparison.json`: completed-run mean throughput and median per-run archival p95, with repetition counts and incomplete runs shown separately. Do not compare different offered rates, storage, partitioning or generator limits as if only threads changed.

The retained volume also holds the publication journal, producer report, full Spark progress JSONL, window observations, raw/clean archives, checkpoints and `report.json`. Stop actions target only that run's named containers; topics and volume data are preserved. Do not reset runtime data as a substitute for backlog recovery. Keep large outputs ignored, and commit compact observed summaries only after an actual measured campaign. A short smoke check demonstrates the tool's operation; completion of C14 still requires controlled repeated measurements and interpretation of the real hardware limits.

## Functional check — 17 November 2026

The short command above completed with three independent `local[1]` drivers. The main run published/acknowledged 80 messages at an observed 10 messages/s, archived and deduplicated all 80, and persisted 80 inspections/12 predicted rejects. Kafka lag reached zero, no IDs were missing or repeated in clean history, and archival commit observation covered all 80 messages. CPU/memory samples were recorded for all three stages and the producer. See [the compact observed record](results/stream-benchmark-smoke.json).

This check's measurement interval was shorter than the pipeline fill time: no main-run inspection had reached SQLite inside the eight-second interval, and all 80 arrived during drain. The report preserves the resulting zero interval throughput and null interval freshness/sink-cost samples. It does not substitute post-drain counts for sustained processing rate. Use a longer warmup and measurement interval for C14 conclusions; one short run is not the required repeated thread/profile comparison.

The check also exposed two instrumentation pitfalls, corrected before the successful run: Spark 4 UUID fields require its JSON progress representation, and idle progress can reuse the following data batch's ID. Shared monotonic measurements avoid the UTC/host clock differences observed in an earlier attempt. All attempt artifacts remain retained.
