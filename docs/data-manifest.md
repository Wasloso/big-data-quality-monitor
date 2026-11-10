# Local MVTec AD manifest (C09)

Start with the `bottle` category from the original MVTec AD dataset, published by MVTec Software GmbH. Obtain it from the [official dataset page](https://www.mvtec.com/company/research/datasets/mvtec-ad), review its supplied terms, and retain the original download information locally. This command does not download data, train a detector, or add vision dependencies. MVTec AD 2 and other datasets are not interchangeable inputs.

Extract without moving images between the official directories:

```text
data/mvtec/bottle/
  train/good/*.png
  test/good/*.png
  test/<defect>/*.png
  ground_truth/<defect>/*_mask.png
```

Create a reproducible manifest with standard-library Python (3.10 or later):

```bash
python -m quality_monitor.data_manifest --data-root data/mvtec --category bottle --output artifacts/manifests/bottle-v1 --seed 42 --validation-fraction 0.2
python -m quality_monitor.data_manifest --data-root data/mvtec --check artifacts/manifests/bottle-v1/manifest.json
```

The data root contains category directories, rather than pointing directly at `bottle`. The output contains `manifest.json` and a separate `labels.json`. Recreating an output directory replaces those two files; use a new versioned directory when changing input or policy. Paths inside the manifest are relative to the data root, so the dataset can be relocated. It records every image's size and SHA-256, source attribution, category, split policy, seed, requested validation fraction, and the evaluation-label file hash. The deterministic manifest ID changes when the inventory or policy changes. Actual split sizes are printed; record the ID in later training/evaluation artifacts.

The exact split policy is `normal-holdout-v1`:

- Rank official `train/good` images by SHA-256 of `seed:relative_posix_path` in ascending order. Reserve the first `floor(n * validation_fraction)` for validation, bounded to leave at least one image in each training partition. No images are copied or deleted.
- Fit the detector only on the remaining normal training images. C10 must use the held-out normal validation subset for threshold selection, with a documented false-reject policy. It has no anomalous validation examples; do not claim supervised threshold optimization or validation AUROC from it.
- Keep every locally supplied official test image in `test`. Never tune the model, coreset, image resolution, or threshold against those images or their labels. Test labels and anomaly-mask paths/hashes live only in `labels.json`, for held-out evaluation.
- Reject repeated paths, identical image content across different splits, missing files, changed hashes/sizes, training images outside `train/good`, and inconsistent or missing anomaly masks. Normal test images have no mask. Duplicate content within one split is inventoried but does not trigger the cross-split leakage check.

The checker verifies complete coverage of the **local** PNG train/test inventory and its ground-truth masks. It cannot certify that a download contains every image distributed by MVTec without an independent reference inventory, nor does it decode images. Preserve the download/archive provenance alongside local experiment artifacts. Image decode failures belong to subsequent model preparation and must be reported explicitly.

`data/` and `artifacts/` are ignored, as are generated SQLite serving files. Keep images, masks, fitted weights, and large manifests outside commits. Only deliberately chosen small reproducibility summaries belong in Git. This step provides manifest creation and integrity checking; no actual dataset manifest or model evaluation result is claimed until run on local data.
