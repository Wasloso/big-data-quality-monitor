# React dashboard and quality API (UI1/UI2)

The API exposes the existing SQLite window/rule results. It does not consume Kafka, run inference, write inspection data or start Spark. Keep the window writer and its serving directory setup from [serving-store.md](serving-store.md).

```bash
docker compose up --build -d api
```

Open [the React dashboard](http://localhost:8000) or [the API documentation](http://localhost:8000/docs) to inspect the routes. Only port `127.0.0.1:8000` is published. The service mounts only the `serving` volume subdirectory, with write access for WAL coordination sidecars; SQL connections use `mode=ro`. Raw/clean history is not accessible to the API. The serving directory must already exist before Docker mounts this subpath.

- `GET /api/runs` lists at most 100 run/category/model/threshold combinations, most recently updated first. `truncated` tells the caller when more combinations exist.
- `GET /api/overview` requires `run_id`. Omit all detector fields for a run-wide view, or supply `category`, `model_version` and `threshold_version` together for an explicit detector selection. It returns station totals, window snapshots and related rule evaluations from a single committed SQL snapshot. Optional `line_id`, `from_time` and `to_time` narrow the result. Time bounds need an explicit offset, use UTC internally, and select only windows entirely within the requested interval.

Missing databases return a `missing` state without creating a file. An initialized database with no matching rows returns `empty`; an unreadable/uninitialized schema returns HTTP 503. Requests exceeding 500 windows or 1000 rule rows return HTTP 413 rather than silently presenting partial totals. Invalid filters, including incomplete detector selections, return 422. Mixed detector contexts for one station or overlapping station windows return 409; the API never chooses a latest model or silently sums overlapping outputs. Distinct stations may use different categories/models/thresholds. Each station row retains those identifiers; counts are combined, raw anomaly scores are not. Database queries use bound values; run and detector filters apply to both windows and related alerts.

Station totals count stored inspection snapshots, and predicted accepted means inspections minus predicted rejects. `response_time` is API wall time; `updated_at` is the last serving update, not proof of current station activity or a measured commit time. Keep one window/rule/output-mode configuration per database as described in the serving-store guide. The API does not repair mixed or overlapping runtime datasets.

The React operations workspace has Station overview, Quality & alerts, and detector-linked station details. It polls every five seconds, filters by run and UTC range, carries explicit detector metadata on station links, and displays weighted totals, window history, rule evaluations, fixture labels and stale/error states. Station details now include a paged retained-inspection list and selection panel with score/decision/category/model/threshold version and an optional source image. Persisted simulated temperature/speed/work orders and independent producer-session error counters are available through their optional sources. Acquisition observations are not a machine heartbeat. Date filters select complete windows, not individual events. Provisional rows do not become final automatically. The existing Streamlit view remains available on port 8501 during migration. The Docker build compiles React once and serves the assets and API on port 8000; no Node server runs in the final container. No extra server database or ORM is used.

For Python development, create a local virtual environment, install `requirements-api.txt` and run from the repository root:

```bash
QUALITY_DATABASE=/absolute/path/quality.sqlite python -m uvicorn quality_monitor.api:app --host 127.0.0.1 --port 8000
```

API checks are deliberately opt-in, keeping the standard-library fast suite independent of web dependencies. With `httpx==0.28.1` installed alongside the API requirements:

```bash
python -m unittest discover -s tests -p check_api.py -v
```

## Frontend development

Node 22.20.0 was used for the initial build. Start the API as above and use a separate terminal:

```bash
cd dashboard
npm ci
npm run dev
```

Vite provides the development URL (normally `http://127.0.0.1:5173`) and proxies `/api` to `127.0.0.1:8000`. For a local single-origin run, execute `npm run build`, then start/restart the Python API from the repository root. Build artifacts and `node_modules` stay ignored; `package-lock.json` is committed. No UI test framework is introduced.

## Recent individual inspections (UI3 foundation)

First build the [inspection serving view](inspection-serving.md) with `spark-inspections`. The API reads its separate `STATIONS_DATABASE` (default `/data/serving/stations-v2.sqlite`) through the existing narrow serving mount. It never queries clean Parquet directly.

`GET /api/inspections` requires `run_id`, `line_id`, `category`, `model_version` and `threshold_version`. It returns recent inspection metadata, `latest_inspection`, `retained_count`, a projection batch timestamp and a `next_cursor`. `limit` defaults to 50 and is capped at 100. To request the next page, pass both returned cursor fields as `before_time` and `before_id`; time must include its UTC offset. Ordering is acquisition time descending, then inspection ID descending, so equal-time items do not disappear between pages.

The history scope is explicitly `retained_samples`. Its count is not the production total. The latest observation is for the selected station/detector even while paging older items; it is not a machine heartbeat. The projection batch timestamp describes that database's global last applied batch and does not prove that a particular station is currently running. Quality windows and this projection update independently.

Missing item databases return `missing` without creating a file, empty selections return `empty`, and invalid cursors/limits return 422. Run/station/category/model/threshold filters apply to the item list, count and latest observation. Returned decisions are booleans. Image references remain separate files. When the optional image root is configured, the metadata includes an `image_url` resolved through the [inspection image endpoint](inspection-images.md); this does not perform model inference.

Optional `from_time` and `to_time` on `/api/inspections` select individual items in the UTC half-open interval `[from, to)`, rather than complete aggregate windows. Bounds require offsets and a start before the end. They filter the item list and retained count before pagination. `latest_inspection` continues to describe the latest observation for the selected run/station/detector, independently of the page/time range.

## Station item panel

Open a station to see its retained item list, independently refreshed every five seconds. Select **View** to inspect one record; use **Older items** and **Latest items** to move between bounded 50-item pages. Run/station/detector and the UTC date filters apply to this list. Controls support seconds without treating browser-local time as acquisition time. The page keeps quality-window totals separate from retained sample counts.

Configure the [optional local image mount](inspection-images.md) to show photos. Without a configured root, the panel says image storage is not connected. A missing/unreadable image displays an unavailable state; manual Refresh can retry it. The fixture model remains labeled synthetic. Inspecting an image does not turn its score into a confirmed defect or probability. Numeric detector thresholds are available in matching producer-session reports when enabled; the selected-image panel still displays version identifiers. Optional offline anomaly-map exports are documented in [the model guide](vision-baseline.md); they are not streamed into this panel.


## Persisted simulated observations

Retained items and latest observations now include nullable `station_context`. Overview station rows include `latest_observation`, with separate top-level `observation_state` and `observation_batch_at`. The observation source is read independently of quality windows; its failure returns unavailable context without hiding valid quality totals. Latest observations are scoped to the run/detector and are not filtered to an old selected window range. Invalid stored context is an explicit unavailable state. See [context schema and runtime transition](station-context.md) for the versioned archive/checkpoint/database paths and simulator settings.


## Producer sessions

Enable the optional [producer-status source](producer-status.md) to show session state, callback acknowledgements, inference/delivery errors and numeric thresholds independently of Spark windows. `/api/runs` can discover these producer-only contexts before quality data exists. Session totals ignore historical window-time filters and are never presented as downstream processed counts.

## Mixed-category navigation

The selector lists each run once. Stations and Summary combine compatible station counts, using total rejects divided by total inspections. Cards retain their own category/model/threshold metadata. Station links include the run and complete detector trio so capsule photos do not inherit a bottle selection, including after a reload. Producer-session links offer explicit context selection even if an ambiguous run overview is refused.

Rule links also set the UTC window bounds. Aggregate queries select whole windows; retained photos use the half-open interval `[window_start, window_end)`. They are related examples, not causal evidence of an alert. Window totals, retained sample counts, producer acknowledgements and observation freshness remain separate. See [the checked live demo](two-category-demo.md).

## Operations workspace

The interface uses a compact sidebar, run/UTC filters, weighted quality metrics and station cards. Cards show the latest matching producer session separately from processed-window totals: reported acquisition state, acknowledged Kafka deliveries, inference/enqueue/delivery errors and pending deliveries. Producer-only stations remain visible before their first quality window; unavailable processed counts display a dash. Shared producer polling feeds both cards and diagnostics, preserving one request stream per selected scope. Stale, clock-skewed, failed and partial reports stay explicit; a stopped report describes that session only.

Persisted station observations show simulated temperature, conveyor speed, work order, operation and recorded state, with observation age. The quality chart groups matching window boundaries, sums counts and plots weighted predicted reject rates for the last twelve selected intervals. It does not pool anomaly scores or impose a global alert threshold; rule evaluations remain station-specific. The full window and rule tables retain the selected history.

Station details place the retained inspection log beside its actual source image, predicted decision and anomaly score. A numeric threshold is shown only from a producer report with the same category/model/threshold version; absent provenance remains unavailable. Long inspection/product/image IDs and detector versions are expandable. Existing 50-item keyset paging, source-image confinement and exact `[from,to)` item filtering are unchanged. Editing/clearing a rule-linked range removes the old window parameters from the link so a reload does not silently restore them.

The responsive layout uses local CSS and native SVG; no UI/chart library, external font, endpoint, service or default test suite was added. All displayed runtime metrics come from the existing bounded API. Kafka lag, Spark CPU, pipeline throughput and measured end-to-end latency are not supplied by these endpoints and are not invented in the dashboard. Benchmark outputs remain in [the separate measurement track](performance.md).

On 17 November 2026, the production/container builds and a temporary Chromium walkthrough passed against retained real bottle/capsule records: weighted 115/22/19.1% totals, session states, capsule photo/threshold/deep-link reload, 50+20 paging, exact alert-window photo scope and clearing/reloading a range. A 390px layout had no page overflow. HTTP fault injection checked missing photos, producer-source failure with retained reports, stale running reports, producer-only startup and ambiguous overview refusal. No page errors were observed. These checks verify presentation and existing source semantics; they are not a new live-stream, model-quality or performance benchmark. Scripts/screenshots remain temporary; no browser suite was added.
