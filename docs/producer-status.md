# Producer-session status (UI4)

The station's local JSON snapshots are now exposed through a read-only API. The independent React panel follows in the UI increment. This reports acquisition/inference/publication activity without waiting for Spark windows. It is not a second source of processed-quality counts or a physical-machine heartbeat.

## Enable local status

Use the existing optional CPU vision/Kafka environment and [station command](continuous-stations.md). The default station writes one new file per session under `artifacts/stations`; create that directory before mounting it:

```bash
mkdir -p artifacts/stations
docker compose build api
PRODUCER_STATUS_DIR="$PWD/artifacts/stations" docker compose -f compose.yaml -f compose.status.yaml up -d --no-deps api
```

When using source images and the host Kafka listener too, keep their overrides:

```bash
PRODUCER_STATUS_DIR="$PWD/artifacts/stations" INSPECTION_IMAGE_DIR="$MVTEC_ROOT" docker compose -f compose.yaml -f compose.local-kafka.yaml -f compose.images.yaml -f compose.status.yaml up -d --no-deps api
```

The bind is read-only and limited to the selected status directory at `/data/producer-status`. It grants no access to raw/clean archives, model weights or arbitrary host files. `PRODUCER_STATUS_ROOT` is optional; without it the API reports `disabled`. A host Uvicorn process can set it to the existing status directory directly. Custom station `--status` paths must be inside the configured directory to appear. Keep one file per session; ordinary stop/restart creates a new session without resetting earlier counters.

## API and discovery

`GET /api/producer-status` requires `run_id`, `category`, `model_version` and `threshold_version`; optional `line_id` selects one station. These are exact metadata filters, not file paths. The response has `state`, `response_time`, `sessions` and `invalid_files`. Each session retains its own counters, reported state, start/update/end timestamps, threshold/version provenance, last error and derived freshness. Status counter definitions are in [the station guide](continuous-stations.md).

`/api/runs` merges bounded producer metadata with stored quality run/detector contexts. A producer-only run can therefore be selected before the serving database or its windows exist. The quality API still returns missing/empty windows until Spark writes them. A bad status source does not remove valid stored quality contexts; discovery exposes its own availability state. These reads are separate snapshots and do not establish atomic completeness across status, windows and retained items.

| State/freshness | Interpretation |
| --- | --- |
| `running` / `stopping`, recent | A recent producer report of that session's state; not proof of current hardware health. |
| Active report, stale | Current process state is unknown. Do not turn an old running snapshot into a stopped event. |
| `stopped` / `failed`, ended | A recorded terminal report for this session only. A different process/run may exist. Errors remain visible after a clean process stop. |
| `clock_skew` | The snapshot update is more than five seconds ahead of the reader clock; current state cannot be inferred. |
| Missing/disabled source | No status evidence is available; do not invent zero errors or successful production. |
| `partial` | Some files are malformed/unsafe/unreadable. The session list and latest-session markers may be incomplete. |

Active-report freshness uses `max(30 seconds, 2 / target_rate + 10 seconds)`. This accommodates deliberately slow acquisition; it is a display heuristic, not an industrial deadline. Updates occur after inspection attempts/polling and during shutdown, not on an independent heartbeat. Terminal reports retain their recorded outcome and show their age/time. Source errors leave the last successful UI response visibly unavailable, rather than keeping a fresh running badge.

Sessions are ordered by actual `started_at`, then session ID, not file modification/update time. `latest_started_session` is scoped to the run/station/category/model/threshold selection. The API returns earlier sessions too; simultaneous reports remain visible. The panel does not sum session acknowledgements and label them processed inspections. Its scope ignores window-time filters, which apply only to the quality/history views. Numeric detector thresholds come from the session artifact; they are separate from the quality-rule rate threshold.

## Bounds and failures

The reader scans only direct `.json` files, with limits of 200 files, 16 KiB per file and 100 matching sessions. It rejects symlinks/non-files/outside-root paths, invalid UTC timestamps, unsupported schema versions, invalid counters and inconsistent queue outcomes. Malformed files make the source partial; exceeding a count bound returns HTTP 413, never silent truncation. Move older snapshots to an archive outside the configured working directory when needed, preserving evidence. The API never writes or cleans up files.

New snapshots explicitly declare status `schema_version: 1`. The reader also accepts the previous unversioned format with the same fields/definitions. Acquisition event schemas and `/data/v2` checkpoints/databases are unchanged by this feature. No Spark reset or database migration is needed.

## Verification

One focused opt-in API method covers session scope, latest-by-start selection, rate-aware freshness, cold-start discovery, read-only access, malformed/symlink files and an explicit bound. The default fast suite remains unchanged. A real-process/browser check exercises live callback-count updates, stop/restart and deliberate inference errors separately from the HTTP-injected stale/unavailable display checks. See the [compact observed result](results/producer-status-integration.json); temporary scripts, full snapshots and predictions stay outside Git.


The actual run recorded three sessions after A stopped/restarted while B intentionally encountered two unreadable-image attempts. All 22 successful delivery callbacks matched unique Kafka inspection IDs; B published only its two successful predictions. The run was discoverable before any quality windows existed. This is producer/Kafka status evidence, not downstream processed counts. The original image dataset was unchanged; a separate ignored copy contained the deliberate decode failure.
