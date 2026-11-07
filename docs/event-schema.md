# Inspection events, version 1

One JSON object per line. `quality_monitor.events.parse_event` decodes and validates it; invalid input raises `ValueError`. Only inspection records are implemented at this stage. Progress/control records will be added with event-time aggregation.

| Field | Type and meaning |
| --- | --- |
| `schema_version` | Integer `1`; booleans are not valid versions. |
| `record_kind` | String `inspection`. |
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

All fields above are required. Extra fields are currently allowed. Identity uniqueness is checked across a run, not by the single-event validator. Paths need not exist during fixture tests; real image loading belongs to the inference stage. The validator does not recalculate model decisions from scores.

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
