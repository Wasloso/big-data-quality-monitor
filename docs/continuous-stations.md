# Continuous image stations (C11, first increment)

`quality_monitor.station` runs actual CPU inference on a repeating image list and publishes completed predictions to Kafka until Ctrl+C. Each process loads one fitted category artifact; stations share the code, not a memory bank across different categories. The bottle artifact is checked. Capsule and persisted simulated machine readings are separate follow-ups.

## Start locally

Prepare the optional environment and fitted artifact as described in [the model guide](vision-baseline.md), then add the existing Kafka client pin:

```bash
uv pip install --python .venv-vision/bin/python -r requirements-kafka.txt
docker compose -f compose.yaml -f compose.local-kafka.yaml up -d kafka kafka-init
```

The optional override adds `localhost:29092`, bound only to loopback, for a Python station running on the same host as Docker. It keeps `kafka:19092` for the existing containers; Kafka's advertised addresses matter, so publishing an internal listener's port alone is insufficient. Include the override in subsequent Compose commands that recreate Kafka. The normal Compose configuration still has an internal-only broker. No vision dependencies are added to Spark/API images.

Create a label-free ordered image list from the prepared manifest, without opening the evaluation labels or using saved scores:

```bash
.venv-vision/bin/python - <<'PY'
import json
from pathlib import Path
manifest = json.loads(Path('artifacts/manifests/bottle-v1/manifest.json').read_text())
sequence = {'category': manifest['category'],
            'images': [row['path'] for row in manifest['images'] if row['split'] == 'test']}
Path('artifacts/bottle-images.json').write_text(json.dumps(sequence, indent=2))
PY
```

Paths are relative to the dataset root and start with the selected category. The CLI checks category/path confinement and file availability before acquisition. It does not read the evaluation labels, masks or previously computed predictions. Directory names locate the files; the detector receives RGB pixels. The list order can be changed or subsetted explicitly for a demo without changing the frozen held-out benchmark.

Run a station in its own terminal, setting the parent directory that contains `bottle/`:

```bash
MVTEC_ROOT=/absolute/path/to/mvtec_ad
.venv-vision/bin/python -m quality_monitor.station --station A --category bottle --run-id images-01 --rate 1 --data-root "$MVTEC_ROOT" --images artifacts/bottle-images.json --model artifacts/models/bottle-v1
```

A second terminal can run B with `--station B --rate 2` and the same arguments for an initial bottle-only integration check. For the final A/bottle and B/capsule demo, prepare/evaluate capsule separately and supply its list and artifact; a category mismatch fails before publication. `--scenario` labels the run scenario. `--max-items 5` bounds acquisition attempts for an explicit smoke; without it, the sequence repeats until a stop signal. Do not interpret looping images as new independent evaluation samples.

Ctrl+C (or SIGTERM) stops new acquisitions, allows the current inference to finish and attempts a delivery flush bounded to ten seconds. The command emits no progress/closure controls. Source images remain on disk; Kafka carries score, decision, source reference, actual UTC acquisition/publication times and model/threshold IDs. The score uses the artifact's frozen strict `>` threshold, never a label-derived decision. Raw scores can exceed one.

Every acquisition gets fresh inspection/product IDs; repeated images retain the same source-image ID. Restarting a station, even with the same run/station, creates a fresh session and new inspection IDs. Kafka's internal retries preserve each queued payload; there is no application-level replay of uncertain deliveries in this command. Target rate limits acquisition and includes inference work; the loop does not burst to compensate for slow inference.

## Status and failures

Each process prints the path of a new ignored `artifacts/stations/<station>-<session>.json`. `--status` can choose another new path; an existing file is refused. Snapshots are replaced atomically after attempts/polling and on shutdown; they are not independent telemetry heartbeats. A long gap or abrupt process loss leaves stale state rather than proof of a clean stop.

| Field | Meaning |
| --- | --- |
| `state` | Producer-reported `running`, `stopping`, `stopped` or `failed`; not current physical-machine state. |
| `attempted` | Acquisitions started, including failed inference. |
| `completed` | Valid predictions produced by the model and event validator. |
| `queued` | Predictions accepted by the Kafka client for delivery. |
| `published` | Successful Kafka delivery callbacks, not downstream processed count. |
| `inference_errors` | Failed decode/inference/score validation; these emit no inspection or fabricated normal result. |
| `enqueue_errors`, `delivery_errors`, `pending` | Failed enqueue, failed delivery callbacks and queued predictions without an observed final callback. |
| `threshold`, versions, session/run IDs, timestamps, `last_error` | Provenance and the latest observed error; successful later attempts do not erase error counts. |

Inference failures are logged and the next acquisition can continue. Delivery/enqueue failure stops the process; incomplete flush is a failure. A process can stop cleanly after earlier inference errors; error counts remain visible and its exit code is nonzero. Status is currently a local artifact, not served by FastAPI/React. It does not prove Spark processing or completeness.

## Downstream processing

The existing Spark commands accept these version-1 inspections. To drain a bounded smoke, run Q1, Q2 and then both serving writers, preserving their original state:

```bash
docker compose -f compose.yaml -f compose.local-kafka.yaml run --rm --no-deps spark-ingest --once
docker compose run --rm --no-deps spark-deduplicate --once
docker compose run --rm --no-deps spark-windows --database /data/serving/quality.sqlite --once
docker compose run --rm --no-deps spark-inspections --once
```

For continued processing, omit `--once` and run each stage independently after its upstream archive exists. Quality windows stay provisional until event-time progress supports closure; stopping a single station does not close them. Do not advance time with the finite fixture producer's controls: its fixed groups do not describe these actual model runs. Controlled run closure derived from actual groups is still pending. Preserve checkpoints, archives, serving databases and ledgers together.

Use the actual run and model/threshold selection in React. Enable source photos with the existing [optional read-only image mount](inspection-images.md); the image root must be the same parent of `bottle/` used by the station. Aggregate windows and bounded retained-item history are different data sources and may update at different times.

## Verification boundary

The first actual-broker check started two bottle processes at target rates 2/s and 3/s, stopped A with SIGINT, observed B continue, and restarted A with the same run/station. Kafka received 39 unique inspections across three reused images: A first session 4, A restart 3, B 32. Final producer counts matched callbacks/received records with no inference, enqueue, delivery or pending errors. All records were inspections with actual model scores and the frozen threshold; no closure controls were emitted.

After the producers stopped, sequential available-now drains resumed the retained Q1/Q2/Q3/projection checkpoints. The API matched all 39 Kafka inspection IDs, scores and image references; aggregate counts were A=7/5 predicted rejects and B=32/22. All quality windows remained provisional. One actual source PNG was served over HTTP and its SHA256 matched the dataset file.

This is integration evidence, not a throughput benchmark or capsule validation. See the small [station integration record](results/bottle-station-integration.json) for the downstream check outcome. Full continuously observed Kafka-to-browser timing, simulated work-order/temperature/speed context, producer-status API/UI, actual A/bottle+B/capsule acceptance, controlled finalization, C12 reconciliation, C13 retained-state recovery and C14 capacity experiments remain open.
