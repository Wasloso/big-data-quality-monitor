# Inspection events, version 1

One JSON object per line. `quality_monitor.events.parse_event` decodes and validates it; invalid input raises `ValueError`. Version 1 supports inspections and explicit progress controls.

| Field | Type and meaning |
| --- | --- |
| `schema_version` | Integer `1`; booleans are not valid versions. |
| `record_kind` | String `inspection` or `progress`. |
| `inspection_id` | Non-empty string; unique within a run. Preserve it when retrying the same inspection. |
| `run_id`, `scenario_id` | Non-empty strings identifying the run and scenario. |
| `product_id` | Non-empty string identifying the inspected product. |
| `image_id`, `image_uri` | Non-empty strings identifying the source image and its location. Different inspections may reuse an image. |
| `line_id`, `category` | Non-empty strings for the production line and model category. |
| `event_time` | ISO timestamp with a UTC offset (`Z` or `+00:00`), used for quality windows. |
| `published_at` | Same timestamp format; actual publication time once a producer is connected. |
| `anomaly_score` | Finite integer or floating-point number, excluding booleans. Scores are not assumed to be probabilities. |
| `is_reject` | JSON boolean; strings and integers are rejected. |
| `model_version`, `threshold_version` | Non-empty strings identifying the detector and its decision rule. |

For inspections, all fields above are required. For progress controls, `product_id`, `image_id`, `image_uri`, `anomaly_score`, and `is_reject` must be null or absent; all other fields remain required. A control has a distinct `inspection_id` and the same run/line/category/model/threshold metadata as its active group. It carries no prediction and represents no inspected product. Extra fields are currently allowed. Identity uniqueness is checked across a run, not by the single-event validator. Paths need not exist during fixture tests; real image loading belongs to the inference stage. The validator does not recalculate model decisions from scores.

## Progress and event-time completion

For a finite run, drain all intended inspections through Q1, Q2, and Q3 before publishing one control per active group. A control timestamp strictly beyond the last window end plus the lateness allowance advances event time in both stateful queries. With the fixture minute ending at `12:01:00Z` and the default two-minute allowance, use `12:04:00Z`. `published_at` is still the actual publication time.

Controls pass validation and bounded deduplication, but contribute zero inspections and zero rejects. Control-only windows are filtered after aggregation, so controls still advance its watermark. They produce no quality output. Progress is a promise by the replay coordinator: do not send it while intended inspections are still buffered upstream. A Kafka acknowledgement alone is insufficient. Drain the stages in order as shown in README; do not mix a closure control and undrained historical inspections in one file-stream catch-up batch.

The watermark is global across runs/groups. Isolate historical replays using separate raw, clean, and checkpoint paths. A stopped live stream can leave windows pending indefinitely; wall-clock time does not finalize them. Input within the allowance can update an open window. Older input is outside the guarantee and may be excluded by Q2 or Q3; a finalized window cannot be revised. Do not assume every beyond-watermark input is always dropped under every batching arrangement.

## Repeatable sample

`tests/fixtures/inspections.jsonl` has 120 made-up inspections in the UTC minute starting at `2026-11-07T12:00:00+00:00`: one inspection per second per line. A has 6 rejects out of 60; B has 12 out of 60. Expected counts are written separately in `expected_counts.json`.

The sample uses scores 0.1 and 0.9 and a fixture threshold of 0.5. No model was run. Image paths are placeholders. `published_at` is deliberately frozen for reproducibility and must be replaced with wall-clock time when these records are published. Do not use the fixture timestamps for infrastructure latency measurements. New simulated inspections need new run/inspection identities; duplicate injections keep their existing identities.

The generator emits predictions only. There is no ground-truth label field for future inference or alert logic to consume.

From the repository root, reproduce the sample with Python 3.10 or later:

```bash
python3 -m quality_monitor.sample > /tmp/inspections.jsonl
cmp tests/fixtures/inspections.jsonl /tmp/inspections.jsonl
```

Run the small standard-library test file:

```bash
python3 -m unittest discover -s tests -v
```

These checks validate the event contract and known sample totals. They do not test streaming, model accuracy, or Kafka delivery.

## Kafka publication

`quality_monitor.producer` publishes the generated sample to the `inspections` topic. Each invocation uses a new run ID by default and prefixes inspection/product IDs with it. Passing the same `--run-id` reuses those identities for deliberate duplicate injection. The Kafka key is `line_id`; images remain outside Kafka. Before enqueueing each message the producer replaces `published_at` with the current UTC wall-clock time while preserving synthetic `event_time`.

The producer checks delivery callbacks and flushes pending messages before reporting success. Producer idempotence handles retries within a session, but does not deduplicate separate runs or guarantee exactly-once downstream results. See the repository README for Compose commands and the round-trip check.


## Actual image stations

The separate [continuous station CLI](continuous-stations.md) publishes this same schema with real model predictions. It sets `event_time` at actual UTC acquisition and `published_at` after inference, before enqueue. Every acquisition/restart has a fresh inspection/product identity, while a repeated relative source path retains its image ID. The configured category must match the image list and fitted model; ground truth and saved scores are not read by the online loop. Ctrl+C drains queued deliveries and emits no closure controls. Simulated machine context remains a future versioned parsed-schema extension.
