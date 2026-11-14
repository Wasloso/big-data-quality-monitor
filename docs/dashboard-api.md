# React dashboard and quality API (UI1/UI2)

The API exposes the existing SQLite window/rule results. It does not consume Kafka, run inference, write inspection data or start Spark. Keep the window writer and its serving directory setup from [serving-store.md](serving-store.md).

```bash
docker compose up --build -d api
```

Open [the React dashboard](http://localhost:8000) or [the API documentation](http://localhost:8000/docs) to inspect the routes. Only port `127.0.0.1:8000` is published. The service mounts only the `serving` volume subdirectory, with write access for WAL coordination sidecars; SQL connections use `mode=ro`. Raw/clean history is not accessible to the API. The serving directory must already exist before Docker mounts this subpath.

- `GET /api/runs` lists at most 100 run/category/model/threshold combinations, most recently updated first. `truncated` tells the caller when more combinations exist.
- `GET /api/overview` requires `run_id`, `category`, `model_version` and `threshold_version`. It returns station totals, window snapshots and related rule evaluations from a single committed SQL snapshot. Optional `line_id`, `from_time` and `to_time` narrow the result. Time bounds need an explicit offset, use UTC internally, and select only windows entirely within the requested interval.

Missing databases return a `missing` state without creating a file. An initialized database with no matching rows returns `empty`; an unreadable/uninitialized schema returns HTTP 503. Requests exceeding 500 windows or 1000 rule rows return HTTP 413 rather than silently presenting partial totals. Invalid filters return 422. Database queries use bound values; run and detector filters apply to both windows and related alerts.

Station totals count stored inspection snapshots, and predicted accepted means inspections minus predicted rejects. `response_time` is API wall time; `updated_at` is the last serving update, not proof of current station activity or a measured commit time. Keep one window/rule/output-mode configuration per database as described in the serving-store guide. The API does not repair mixed or overlapping runtime datasets.

The first React increment has Stations, Summary & alerts, and hash-linked station details. It polls every five seconds, filters by run/detector and UTC range, and displays weighted totals, window history, rule evaluations, fixture labels and stale/error states. The detail page currently has no individual images, temperature, machine heartbeat, station metadata projection or inference-error counters; these are explicitly unavailable. Date filters select complete windows, not individual events. Provisional rows do not become final automatically. The existing Streamlit view remains available on port 8501 during migration. The Docker build compiles React once and serves the assets and API on port 8000; no Node server runs in the final container. No extra server database or ORM is used.

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

First build the [inspection serving view](inspection-serving.md) with `spark-inspections`. The API reads its separate `STATIONS_DATABASE` (default `/data/serving/stations.sqlite`) through the existing narrow serving mount. It never queries clean Parquet directly.

`GET /api/inspections` requires `run_id`, `line_id`, `category`, `model_version` and `threshold_version`. It returns recent inspection metadata, `latest_inspection`, `retained_count`, a projection batch timestamp and a `next_cursor`. `limit` defaults to 50 and is capped at 100. To request the next page, pass both returned cursor fields as `before_time` and `before_id`; time must include its UTC offset. Ordering is acquisition time descending, then inspection ID descending, so equal-time items do not disappear between pages.

The history scope is explicitly `retained_samples`. Its count is not the production total. The latest observation is for the selected station/detector even while paging older items; it is not a machine heartbeat. The projection batch timestamp describes that database's global last applied batch and does not prove that a particular station is currently running. Quality windows and this projection update independently.

Missing item databases return `missing` without creating a file, empty selections return `empty`, and invalid cursors/limits return 422. Run/station/category/model/threshold filters apply to the item list, count and latest observation. Returned decisions are booleans. Image references remain separate files. When the optional image root is configured, the metadata includes an `image_url` resolved through the [inspection image endpoint](inspection-images.md); this does not perform model inference.

Optional `from_time` and `to_time` on `/api/inspections` select individual items in the UTC half-open interval `[from, to)`, rather than complete aggregate windows. Bounds require offsets and a start before the end. They filter the item list and retained count before pagination. `latest_inspection` continues to describe the latest observation for the selected run/station/detector, independently of the page/time range.
