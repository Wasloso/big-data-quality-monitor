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

## Current UI boundary

The run selector still identifies a run **and detector context**. Select the bottle context for A, or the capsule context for B. Item photos, context, scores, threshold versions and producer reports stay within that selected group. Choosing the capsule row does not currently combine it with bottle in the global summary.

A combined all-stations view therefore remains the next UI/API step. It must aggregate compatible non-overlapping station windows within one run, retain category/model/threshold provenance per station and avoid pooling raw anomaly scores. Do not silently choose a latest detector when a station has multiple incompatible contexts or sum overlapping outputs. Capsule performance limits remain visible in the report even if the transport/UI works.


## Checked bounded run — 14 November 2026

A/bottle and B/capsule emitted seven unique real predictions under one run, using their matching separately loaded artifacts/thresholds. Retained v2 Q1/Q2/Q3/projection drains and the API matched all IDs, scores, simulated context and source-photo hashes. A had three inspections/one predicted reject; B had four/zero predicted rejects. Wrong-detector station selections returned no items. Producer reports matched each group's acknowledged count. The fixed capsule crack example remained predicted accepted; benchmark labels did not override online decisions. Runtime `inference_errors` count decode/model-execution failures, not classifier mistakes revealed by offline labels.

A temporary real-container browser walkthrough checked each detector selection separately: its station, model/version, work order, source photo and stopped producer report, plus a 390px capsule layout with no page errors. The initial broker attempt found Docker services stopped and was retried after normal startup with retained volumes. Chromium needed its temporary audio library restored; no project/browser test framework or system installation was added. See the [actual small record](results/two-category-integration.json). This is a post-drain transport/serving/UI check, not combined-summary acceptance, a sustained live walkthrough, recovery or throughput/latency evidence.
