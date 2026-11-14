# Bottle/capsule integration

The two separately fitted artifacts now support the selected category assignment: A/bottle and B/capsule. Both use the same PatchCore implementation/configuration, with different normal memory banks, frozen normal-validation thresholds and held-out results. Capsule quality is limited: the first CPU baseline missed 54/109 anomalous test images. This is an integration demo with predicted decisions, not a claim of reliable capsule inspection. See [model results](vision-baseline.md).

Use the ignored combined `data/mvtec_ad` image root. The bottle files copied from the existing archive validate against its unchanged manifest; capsule comes from the [official per-category download](https://www.mvtec.com/research-teaching/datasets/mvtec-ad/downloads). The original bottle checkout was not modified. Do not point the API only at the old bottle directory when expecting capsule photos.

## Image lists and station commands

Create separate label-free lists of official test image paths. This reads no labels or saved scores:

```bash
python3 - <<'PY'
import json
from pathlib import Path
for category in ('bottle', 'capsule'):
    manifest = json.loads(Path(f'artifacts/manifests/{category}-v1/manifest.json').read_text())
    document = {'category': category, 'images': [row['path'] for row in manifest['images'] if row['split'] == 'test']}
    Path(f'artifacts/{category}-images.json').write_text(json.dumps(document, indent=2))
PY
```

After starting Kafka and the [versioned streaming stages](station-context.md), run each station in its own terminal with a shared new run ID:

```bash
MVTEC_ROOT="$PWD/data/mvtec_ad"
.venv-vision/bin/python -m quality_monitor.station --station A --category bottle --run-id two-categories-01 --rate 1 --data-root "$MVTEC_ROOT" --images artifacts/bottle-images.json --model artifacts/models/bottle-v1 --work-order WO-BOTTLE
.venv-vision/bin/python -m quality_monitor.station --station B --category capsule --run-id two-categories-01 --rate 1 --data-root "$MVTEC_ROOT" --images artifacts/capsule-images.json --model artifacts/models/capsule-v1 --work-order WO-CAPSULE
```

The commands do not train or calibrate models. They require matching image-list/category/artifact metadata and report a mismatch instead of falling back to another bank. Every acquisition is new; repeats are not independent accuracy samples. The unreadable-image/error behavior and ordinary Ctrl+C stop still apply; no closure controls are emitted.

Keep both narrow optional API mounts when recreating it:

```bash
PRODUCER_STATUS_DIR="$PWD/artifacts/stations" INSPECTION_IMAGE_DIR="$MVTEC_ROOT" docker compose -f compose.yaml -f compose.local-kafka.yaml -f compose.images.yaml -f compose.status.yaml up -d --no-deps api
```

The API image root contains both category directories and is mounted read-only. Raw/clean Parquet is not mounted into the API. Existing `/data/v2` checkpoints and serving files remain in place; use the normal drain sequence for a bounded run or independently running stages for continued processing.

## Run-wide dashboard

Select the shared run once in React. Stations A and B now appear together, with each card's category/model/threshold identifiers. Summary totals sum inspection/reject counts and compute a weighted rate; they never average the two station rates or combine raw score scales. Producer reports remain separate from Spark counts.

For this demo, each station has one detector context per run. A run overview with conflicting contexts for one station, or overlapping station windows, returns HTTP 409. Use a separate run for another detector, or open an explicit station/context from its producer-session link. The API does not silently choose a latest model. Existing 500-window/1000-rule bounds still fail explicitly; narrow the UTC range for long runs.

Station links preserve the run and complete detector metadata, including after reload. Rule links also set the exact UTC window bounds; retained photos match `[window_start, window_end)`. These are related examples, not evidence of an alert's cause. Latest observations and producer sessions remain independent of historical time filters.

## Continuous processing

Once the v2 raw/clean archives exist (see [initial stage setup](station-context.md)), keep all four jobs running in separate terminals, alongside the two station commands above:

```bash
docker compose -f compose.yaml -f compose.local-kafka.yaml run --rm --no-deps spark-ingest
docker compose run --rm --no-deps spark-deduplicate
docker compose run --rm --no-deps spark-windows --database /data/serving/quality-v2.sqlite
docker compose run --rm --no-deps spark-inspections
```

These are four independent commands, not a sequence in one terminal. Start Kafka/topic initialization first. On a fresh install, publish a small bounded station run using `--max-items` and follow the initial staged drains to create the archive/serving directories before starting the continuous jobs. Do not run two copies of any job against the same checkpoint/database. Keep retained state when restarting; no reset is needed for the overview change.

Open `http://localhost:8000`, select the shared run and observe counts/photos arriving after processing. The producer can be discovered before windows exist. Five-second polling and per-stage triggers do not imply five-second end-to-end delivery. Stop A with Ctrl+C, leave B running, then restart A with the same run/category arguments. A gets a new session and acquisition IDs; B keeps publishing. Stop both producers, allow the stages to catch up and compare their acknowledged counts with processed windows before stopping the Spark jobs. A normal stop emits no future closure controls: idle windows remain provisional.

## Checked bounded run — 14 November 2026

A/bottle and B/capsule emitted seven unique real predictions under one run, using their matching separately loaded artifacts/thresholds. Retained v2 Q1/Q2/Q3/projection drains and the API matched all IDs, scores, simulated context and source-photo hashes. A had three inspections/one predicted reject; B had four/zero predicted rejects. Wrong-detector station selections returned no items. Producer reports matched each group's acknowledged count. The fixed capsule crack example remained predicted accepted; benchmark labels did not override online decisions. Runtime `inference_errors` count decode/model-execution failures, not classifier mistakes revealed by offline labels.

A temporary real-container browser walkthrough checked each detector selection separately: its station, model/version, work order, source photo and stopped producer report, plus a 390px capsule layout with no page errors. The initial broker attempt found Docker services stopped and was retried after normal startup with retained volumes. Chromium needed its temporary audio library restored; no project/browser test framework or system installation was added. See the [actual small record](results/two-category-integration.json). This is a post-drain transport/serving/UI check, not combined-summary acceptance, a sustained live walkthrough, recovery or throughput/latency evidence.

## Checked continuous run — 14 November 2026

Four retained-state Spark jobs ran continuously while A/bottle and B/capsule published real CPU predictions. A stopped after 38 acknowledgements, B continued from 41 to 46, then A restarted and published seven more. Final counts matched all 115 unique Kafka IDs: A=45 inspections/22 predicted rejects; B=70/0. IDs, scores, decisions, context and actual source-photo hashes matched the API. Three session reports ended without inference/enqueue/delivery/pending errors. One quality rule was active; all windows remained provisional. Repeated source images are distinct acquisitions, not additional independent accuracy samples.

Chromium observed increasing counts while producers ran, both categories together, capsule detail/photo and reload, the weighted summary (115/22, 19.1%), exact window/photo links and a 390px layout without page errors. An injected HTTP 409 separately checked the overview warning and explicit producer-context navigation; actual ambiguous/overlap rejection is covered by the API check. The first browser attempt reached the date-bound assertion, which assumed the control retained `:00` seconds; comparing UTC instants corrected the check. The next launch briefly preceded API readiness after recreation; waiting for readiness and rerunning the remaining checks passed. No browser framework/dependency was added to the project.

Twelve fast checks, six opt-in API checks, the frontend/container builds and diff checks passed. Only one focused API method was added; the default suite is unchanged. The [compact result](results/run-overview-live.json) records the observed run. This is low-rate integrated functionality, not processor-interruption recovery, closed-window reconciliation, throughput/capacity or measured end-to-end latency. The capsule accuracy limitation above remains. Producers and the walkthrough's Spark workers were stopped after catch-up; retained state and the local API/Kafka services remain.
