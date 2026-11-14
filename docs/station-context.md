# Simulated station context (UI4)

The real-inference station now adds a version-1 `station_context` object to each successful inspection. It contains `simulated: true`, work order, operation, reported state, temperature in °C and conveyor speed in m/min. The station produces the readings; they survive original-JSON validation, the explicit Spark nested schema, raw/clean Parquet and the bounded SQLite projection. The API returns the same object for retained items and latest observations. Old events without context remain valid and return null, rather than invented readings.

The initial simulator always reports `running` when it acquires an inspection. Temperature and speed vary slightly and deterministically around configurable nominal values as acquisitions proceed. These are demonstration values, not measurements derived from the image, model scores or actual hardware. They do not create extra inspections or affect defect decisions.

```bash
.venv-vision/bin/python -m quality_monitor.station --station A --category bottle --run-id context-demo --rate 1 --data-root "$MVTEC_ROOT" --images artifacts/bottle-images.json --model artifacts/models/bottle-v1 --work-order WO-A --operation visual-inspection --temperature-c 30 --conveyor-speed-m-min 8
```

Use a different work order/nominal values for B. Explicit `idle` and `maintenance` values are recognized by the context validator, but this first station simulator emits only acquisition-time `running`. It does not publish idle/stop transitions or independent telemetry. Ctrl+C stops acquisition; its existing local producer-status artifact records shutdown separately. Producer-status API/UI remains pending.

## Runtime version transition

The optional nested field changes stored Spark schemas. New defaults use a separate runtime namespace, preserving older archives/checkpoints/databases:

| State | New default |
| --- | --- |
| Q1 raw / checkpoint | `/data/v2/raw` / `/data/v2/checkpoints/ingest` |
| Q2 clean / checkpoint | `/data/v2/clean` / `/data/v2/checkpoints/deduplicate` |
| Q3 checkpoints | `/data/v2/checkpoints/windows-v2-*` |
| Quality serving database | `/data/serving/quality-v2.sqlite` (pass this path to the Q3 writer) |
| Station projection / checkpoint | `/data/serving/stations-v2.sqlite` / `/data/v2/checkpoints/inspection-serving-v2` |

API/Streamlit Compose settings point to the new serving files. Old `/data/raw`, `/data/clean`, `/data/checkpoints/…`, `quality.sqlite` and `stations.sqlite` are retained. This is a schema transition with fresh query state, not a recovery demonstration or an in-place migration. Retained Kafka messages are replayed into the new archive on its first start; old messages have unknown context. Replay availability still depends on Kafka retention. Do not combine historical and new namespaces to claim one continuous watermark timeline.

Q1/Q2 now persist `file-query.json` alongside each checkpoint. They refuse a legacy checkpoint without this contract, changed query/source/output settings, or a new checkpoint targeting an already populated output directory. The new station writer refuses an old SQLite schema instead of silently altering it. Keep one writer/query per state path. Custom inputs/outputs/checkpoints remain available, but must be isolated coherently.

Rebuild the affected services and start/drain each stage in order. Existing [station](continuous-stations.md) and [serving](inspection-serving.md) instructions use the new defaults:

```bash
docker compose build spark-ingest spark-deduplicate spark-windows spark-inspections api
docker compose -f compose.yaml -f compose.local-kafka.yaml run --rm --no-deps spark-ingest --once
docker compose run --rm --no-deps spark-deduplicate --once
docker compose run --rm --no-deps spark-windows --database /data/serving/quality-v2.sqlite --once
docker compose run --rm --no-deps spark-inspections --once
docker compose up -d --no-deps api
```

For a fresh installation, first start Kafka/topic initialization and produce a bounded station run. For continuous processing, omit `--once` and run stages independently after upstream archives exist. Keep the optional image override when recreating an API that should serve source photos. Progress-control and finalization rules are unchanged: a normal station stop does not finalize a window.

## Observation and failure semantics

`/api/inspections` returns context per item and latest item. `/api/overview` attaches bounded `latest_observation` metadata to its station rows, plus separate observation-source state/batch time. Quality windows and station observations are read from separate files, so they are eventually consistent. An unavailable observation source does not hide valid quality totals. Latest observations are scoped to the selected run/detector and may lie outside a historical window/time selection.

Missing context stays unknown. A reported `running` value describes the observation time; it does not prove that a station is running now. Context updates only with completed inspections, so silence can mean stopped acquisition, failed inference or delayed processing. Treat old observations as stale/unknown. The browser must label simulation, show observation age, and preserve these distinctions; local producer acknowledgements are not Spark processed counts.

The inspection projection retains context with the same retry/latest-order/retention transaction as its item. Nested Spark rows are converted recursively before validation/serialization. Invalid original context is quarantined in raw data even if Spark could coerce a numeric-looking string. No new data service or default test suite is introduced.


## Checked pipeline result — 14 November 2026

Two actual bottle station processes published ten predictions with different A/B work orders and nominal readings. Raw and clean context objects/scores matched Kafka; one injected numeric-string temperature stayed invalid in raw and was absent from clean/serving results. API inspection/latest-observation fields matched all ten original records; quality totals were A=4/3 predicted rejects and B=6/4, with provisional windows. An actual source PNG passed the HTTP hash check.

The existing finalized-window/history scenario also passed with context present, retaining its independent fixture totals. Twelve fast checks and four opt-in API checks passed; existing methods were extended, with no new default tests or dependencies. Ingestion against the retained legacy checkpoint refused it with exit code 1. See the [small observed record](results/station-context-integration.json). Browser presentation is verified separately from this backend trace.
