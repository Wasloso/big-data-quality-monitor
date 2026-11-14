# CPU PatchCore baseline (C10)

`quality_monitor.vision` fits a category-specific PatchCore memory bank through anomalib, freezes a threshold from normal validation scores, exports the model, and evaluates it in a separate command. It uses anomalib's PyTorch `PatchcoreModel` directly so automatic trainer test-label calibration/normalization does not silently select our threshold. It is not a replacement anomaly algorithm or a reproduction of the paper's exact configuration.

## Optional model environment

Keep model dependencies out of the API and Spark containers. With Python 3.12 and `uv` installed:

```bash
uv venv .venv-vision --python 3.12
uv pip install --python .venv-vision/bin/python --torch-backend cpu -r requirements-vision.txt
```

The verified environment uses anomalib 2.7.0, Torch 2.10.0+cpu, torchvision 0.25.0+cpu, timm 1.0.31, NumPy 2.5.4, Pillow 13.0.0 and scikit-learn 1.9.1. The requirements pin base Torch/torchvision versions; the uv backend flag selects their CPU wheels. Training, data downloads and heavyweight imports are not part of the default fast checks.

The feature extractor is pretrained `resnet18.tv_in1k` with layers 2 and 3. Its initial public ImageNet weights are fetched by timm if not cached. For the recorded run, an existing `resnet18-f37072fd.pth` was copied into this project's ignored `artifacts/cache/torch/hub/checkpoints/` and selected with `TIMM_USE_OLD_CACHE=1 TORCH_HOME=artifacts/cache/torch`. No model code or environment in the other project was changed. These environment variables are optional cache controls, not new training data.

## Prepare, fit, then evaluate

Use the [verified normal-only split policy](data-manifest.md). `MVTEC_ROOT` must contain the category folders; it can point to an existing read-only dataset instead of copying images:

```bash
MVTEC_ROOT=/absolute/path/to/mvtec_ad
python3 -m quality_monitor.data_manifest --data-root "$MVTEC_ROOT" --category bottle --output artifacts/manifests/bottle-v1
.venv-vision/bin/python -m quality_monitor.vision fit --data-root "$MVTEC_ROOT" --manifest artifacts/manifests/bottle-v1/manifest.json --output artifacts/models/bottle-v1
.venv-vision/bin/python -m quality_monitor.vision evaluate --data-root "$MVTEC_ROOT" --manifest artifacts/manifests/bottle-v1/manifest.json --model artifacts/models/bottle-v1 --output artifacts/evaluations/bottle-v1
```

Output directories must be new: the commands do not overwrite model/evaluation artifacts. The manifest and file/label integrity are verified before each operation. Labels are used for integrity checking and final metrics, never as model features or threshold-optimization input.

Defaults were fixed before looking at test predictions: 128×128 RGB bilinear resize, ImageNet channel normalization, ResNet18 layers 2/3, nine neighbors, 1% normal-feature coreset, fit batch size eight, two CPU threads and seed 42. The pretrained extractor remains in evaluation mode during fitting so its BatchNorm statistics do not change. Only `train` images build the memory bank. Normal `validation` images supply the threshold; `test` images are scored only by the separate evaluation command.

The decision policy is `score > threshold`, with the threshold set to the 95th percentile of normal validation scores using NumPy's `higher` quantile method. This is a predeclared demonstration policy, not supervised threshold optimization or a guarantee that future false rejects are at most 5%. Do not tune settings or threshold against the held-out test result.

The fit command writes `fit-config.json` before training and then `model.pt`/`metadata.json` after successful fitting/calibration. Metadata includes the source manifest ID, settings, versions, bank size, raw threshold, model/threshold IDs and exported-weight hash. `load_model()` verifies the weight hash, restores the state on CPU using `weights_only=True` and does not redownload a pretrained network. This helper will support C11; stations must still choose a compatible category artifact.

Evaluation first warms the model on one normal validation image, then scores the unchanged test set one image at a time. It saves score/decision records without ground truth in `predictions.json`, and only joins the separate labels for metrics. The prediction artifact is evaluation/replay evidence, not a live camera stream. No anomalib postprocessor probability scaling is applied; raw anomaly scores and the numeric threshold can exceed one.

## Recorded bottle result — 14 November 2026

See the compact [fit/evaluation record](results/bottle-patchcore-baseline.json) and [data provenance](results/bottle-data-preparation.json). The fitted artifact used 168 fit images and 41 normal validation images; the bank contains 430 vectors of dimension 384. It reloaded successfully before evaluation. Threshold: approximately 1.3622; two validation normals exceeded it (2/41).

Held-out evaluation used all 83 official test images, with no subsequent parameter/threshold adjustment:

| Measure | Observed result |
| --- | --- |
| Image AUROC | 0.99048 |
| Average precision | 0.99739 |
| Precision / recall / F1 at frozen threshold | 0.96875 / 0.98413 / 0.97638 |
| Normal items | 18 accepted, 2 falsely rejected |
| Anomalous items | 62 detected, 1 missed |
| Test-normal false-reject rate | 2/20 = 10% |
| Batch-one local median / p95 | 31.20 / 31.85 ms |

The timing covers decode, resize/normalization, CPU model and image-score extraction after one warm-up. Manifest hashing has already read the local image files, so these are warm-cache measurements. They exclude model loading, Kafka/Spark processing, queueing, browser polling and result visibility; they are not pipeline throughput or a guaranteed industrial deadline. Fit feature extraction/coreset took approximately 11.24 seconds, excluding pretrained loading and validation calibration.

This single-category benchmark is not evidence of factory accuracy, capsule performance or generalization to arbitrary objects. Its high anomalous-image prevalence also differs from many production lines. Keep this first held-out result and its false-reject limitations visible rather than adjusting the threshold to improve it.

## Remaining integration

Prepare/validate capsule separately, then repeat fit/calibration/evaluation with a capsule manifest and a new artifact directory. The station's category determines its matching memory bank and threshold; reuse the same code instead of applying a bottle model to an unsupported object. Capsule data and a capsule artifact are not present in this recorded result.

The live station CLI, publication of real predictions/image references, numeric-threshold display, anomaly-map artifacts and full Kafka-to-dashboard demonstration remain C11/UI4/UI5 work. C12 historical reconciliation, C13 recovery and C14 capacity experiments are still required. Keep fitted weights, full predictions, images and all runtime artifacts ignored; commit only small provenance/results records.

Algorithm/runtime references: [PatchCore paper](https://arxiv.org/abs/2106.08265), [anomalib PatchCore](https://anomalib.readthedocs.io/en/latest/markdown/guides/reference/models/image/patchcore.html), [torchvision ResNet18 weights/normalization](https://docs.pytorch.org/vision/0.25/models/generated/torchvision.models.resnet18.html).
