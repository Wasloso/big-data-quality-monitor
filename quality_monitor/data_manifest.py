"""Prepare a reproducible local MVTec AD category without modifying source data."""

import argparse
import hashlib
import json
from pathlib import Path


SOURCE = "https://www.mvtec.com/company/research/datasets/mvtec-ad"


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def checked_path(root, relative):
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError(f"Missing file or path outside dataset: {relative}")
    return path


def validate_manifest(manifest, labels, root):
    if manifest["schema_version"] != 1:
        raise ValueError("Unsupported data manifest version")
    paths, hashes = set(), {}
    splits = {"train": 0, "validation": 0, "test": 0}
    test_paths = set()
    category = manifest["category"]
    for item in manifest["images"]:
        relative, split = item["path"], item["split"]
        parts = Path(relative).parts
        if split not in splits or len(parts) != 4 or parts[0] != category:
            raise ValueError(f"Invalid image entry: {relative}")
        if split in ("train", "validation") and parts[1:3] != ("train", "good"):
            raise ValueError("Training and validation must use official normal training images only")
        if split == "test" and parts[1] != "test":
            raise ValueError("Evaluation must preserve the official test set")
        if relative in paths:
            raise ValueError(f"Repeated image path: {relative}")
        paths.add(relative)
        path = checked_path(root, relative)
        digest = file_hash(path)
        if digest != item["sha256"] or path.stat().st_size != item["bytes"]:
            raise ValueError(f"Image changed: {relative}")
        previous_split = hashes.setdefault(digest, split)
        if previous_split != split:
            raise ValueError(f"Image content overlaps {previous_split} and {split}: {relative}")
        splits[split] += 1
        if split == "test":
            test_paths.add(relative)
    if not all(splits.values()):
        raise ValueError("Train, validation, and test must all be nonempty")
    official_train = {p.relative_to(root).as_posix() for p in (root / category / "train").rglob("*.png")}
    official_test = {p.relative_to(root).as_posix() for p in (root / category / "test").rglob("*.png")}
    if paths != official_train | official_test or test_paths != official_test:
        raise ValueError("Manifest must include every official train/test image exactly once")
    if labels["category"] != category or labels["schema_version"] != 1:
        raise ValueError("Evaluation labels do not match this category")
    by_path = {item["path"]: item for item in labels["labels"]}
    if len(by_path) != len(labels["labels"]) or set(by_path) != test_paths:
        raise ValueError("Evaluation labels must cover exactly the official test images")
    masks = set()
    for relative, label in by_path.items():
        defect = Path(relative).parts[2]
        if label["defect_type"] != defect or type(label["is_anomaly"]) is not bool:
            raise ValueError(f"Invalid evaluation label: {relative}")
        if label["is_anomaly"] != (defect != "good"):
            raise ValueError(f"Label disagrees with official test directory: {relative}")
        if defect == "good":
            if label["mask"] is not None or label["mask_sha256"] is not None:
                raise ValueError("Normal test images must have no anomaly mask")
            continue
        expected = f"{category}/ground_truth/{defect}/{Path(relative).stem}_mask.png"
        if label["mask"] != expected or file_hash(checked_path(root, expected)) != label["mask_sha256"]:
            raise ValueError(f"Missing or changed anomaly mask: {relative}")
        masks.add(expected)
    official_masks = {p.relative_to(root).as_posix() for p in (root / category / "ground_truth").rglob("*.png")}
    if masks != official_masks:
        raise ValueError("Ground-truth masks do not match anomalous test images")
    return splits


def create_manifest(root, category, output, validation_fraction=0.2, seed=42):
    if not category or Path(category).name != category or category in (".", "..") or "/" in category or "\\" in category:
        raise ValueError("Category must be a single directory name")
    if not 0 < validation_fraction < 1:
        raise ValueError("Validation fraction must be between 0 and 1")
    train = sorted((root / category / "train" / "good").glob("*.png"))
    test = sorted((root / category / "test").glob("*/*.png"))
    if len(train) < 2 or not test:
        raise ValueError("Expected at least two normal training images and a nonempty official test set")
    ranked = sorted(train, key=lambda p: hashlib.sha256(f"{seed}:{p.relative_to(root).as_posix()}".encode()).hexdigest())
    validation_count = max(1, min(len(train) - 1, int(len(train) * validation_fraction)))
    validation = set(ranked[:validation_count])
    images, labels = [], []
    for path in train + test:
        relative = path.relative_to(root).as_posix()
        split = "validation" if path in validation else "train" if path in train else "test"
        images.append({"path": relative, "split": split, "sha256": file_hash(path), "bytes": path.stat().st_size})
        if split == "test":
            defect = path.parent.name
            mask = f"{category}/ground_truth/{defect}/{path.stem}_mask.png" if defect != "good" else None
            labels.append({
                "path": relative, "defect_type": defect, "is_anomaly": defect != "good", "mask": mask,
                "mask_sha256": file_hash(checked_path(root, mask)) if mask else None,
            })
    label_document = {"schema_version": 1, "category": category, "labels": labels}
    manifest = {
        "schema_version": 1, "dataset": "MVTec AD", "category": category, "source": SOURCE,
        "policy": {
            "version": "normal-holdout-v1", "seed": seed, "validation_fraction": validation_fraction,
            "ranking": "sha256(seed:relative_posix_path), ascending; floor with at least one image per split",
            "training": "official train/good excluding normal validation holdout",
            "threshold_tuning": "normal validation holdout only; never official test images or labels",
            "evaluation": "entire unchanged official test set; labels kept separately",
            "overlap": "reject matching SHA-256 image content across splits",
        },
        "labels_file": "labels.json", "labels_sha256": hashlib.sha256(encoded(label_document)).hexdigest(),
        "images": images,
    }
    counts = validate_manifest(manifest, label_document, root)
    manifest["manifest_id"] = hashlib.sha256(encoded(manifest)).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    (output / "labels.json").write_bytes(encoded(label_document))
    (output / "manifest.json").write_bytes(encoded(manifest))
    return manifest["manifest_id"], counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True, help="Directory containing bottle/train, bottle/test, etc.")
    parser.add_argument("--category", default="bottle")
    parser.add_argument("--output", type=Path, default=Path("artifacts/manifests/bottle-v1"))
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--check", type=Path, metavar="MANIFEST", help="Verify an existing manifest and local files")
    args = parser.parse_args()
    try:
        if args.check:
            manifest = json.loads(args.check.read_text(encoding="utf-8"))
            manifest_id = manifest.pop("manifest_id")
            if hashlib.sha256(encoded(manifest)).hexdigest() != manifest_id:
                raise ValueError("Manifest identity mismatch")
            label_path = checked_path(args.check.parent, manifest["labels_file"])
            if file_hash(label_path) != manifest["labels_sha256"]:
                raise ValueError("Evaluation label file changed")
            counts = validate_manifest(manifest, json.loads(label_path.read_text(encoding="utf-8")), args.data_root)
        else:
            manifest_id, counts = create_manifest(
                args.data_root, args.category, args.output, args.validation_fraction, args.seed,
            )
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Manifest error: {error}\n")
    print(json.dumps({"manifest_id": manifest_id, "counts": counts}, indent=2))


if __name__ == "__main__":
    main()
