"""Fit and evaluate a small category-specific CPU PatchCore baseline."""

import argparse
import hashlib
import importlib.metadata
import json
import platform
import time
from pathlib import Path

import numpy as np
import torch
from anomalib.models.image.patchcore.torch_model import PatchcoreModel
from PIL import Image
from sklearn.metrics import (
    average_precision_score, confusion_matrix, precision_recall_fscore_support, roc_auc_score,
)
from torchvision import transforms

from quality_monitor.data_manifest import checked_path, encoded, file_hash, validate_manifest


def read_manifest(path, root):
    manifest = json.loads(path.read_text())
    identity = manifest["manifest_id"]
    content = {key: value for key, value in manifest.items() if key != "manifest_id"}
    if hashlib.sha256(encoded(content)).hexdigest() != identity:
        raise ValueError("Manifest identity mismatch")
    labels_path = checked_path(path.parent, manifest["labels_file"])
    if file_hash(labels_path) != manifest["labels_sha256"]:
        raise ValueError("Evaluation label file changed")
    validate_manifest(manifest, json.loads(labels_path.read_text()), root)
    if manifest["policy"]["version"] != "normal-holdout-v1":
        raise ValueError("Expected the normal-only holdout policy")
    return manifest


def image_transform(size):
    return transforms.Compose([
        transforms.Resize((size, size), antialias=True), transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
    ])


def image_tensor(root, relative, transform):
    try:
        with Image.open(checked_path(root, relative)) as image:
            return transform(image.convert("RGB"))
    except (OSError, ValueError) as error:
        raise ValueError(f"Cannot decode image: {relative}") from error


def make_model(config, pretrained):
    return PatchcoreModel(
        layers=config["layers"], backbone=config["backbone"], pre_trained=pretrained,
        num_neighbors=config["neighbors"],
    ).cpu()


def score_image(model, transform, root, relative):
    with torch.inference_mode():
        score = float(model(image_tensor(root, relative, transform).unsqueeze(0)).pred_score.item())
    if not np.isfinite(score):
        raise ValueError(f"Non-finite score for {relative}")
    return score


def load_model(directory):
    metadata = json.loads((directory / "metadata.json").read_text())
    weights = directory / "model.pt"
    if file_hash(weights) != metadata["weights_sha256"]:
        raise ValueError("Model weights changed")
    torch.set_num_threads(metadata["config"]["threads"])
    model = make_model(metadata["config"], pretrained=False)
    model.load_state_dict(torch.load(weights, map_location="cpu", weights_only=True))
    model.eval()
    return model, image_transform(metadata["config"]["image_size"]), metadata


def fit(args):
    manifest = read_manifest(args.manifest, args.data_root)
    config = {
        "backbone": "resnet18.tv_in1k", "layers": ["layer2", "layer3"], "neighbors": 9,
        "image_size": args.image_size, "coreset_ratio": args.coreset_ratio,
        "batch_size": args.batch_size, "threads": args.threads, "seed": args.seed,
        "normal_quantile": args.normal_quantile, "quantile_method": "higher", "operator": ">",
    }
    args.output.mkdir(parents=True, exist_ok=False)
    # Freeze settings before fitting or examining any test scores.
    (args.output / "fit-config.json").write_bytes(encoded({"manifest_id": manifest["manifest_id"], **config}))
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)
    torch.set_num_threads(args.threads)
    model = make_model(config, pretrained=True)
    model.train()
    model.feature_extractor.eval()  # Preserve the pretrained backbone's BatchNorm statistics.
    transform = image_transform(args.image_size)
    training = [row for row in manifest["images"] if row["split"] == "train"]
    validation = [row for row in manifest["images"] if row["split"] == "validation"]
    started = time.perf_counter()
    with torch.inference_mode():
        for index in range(0, len(training), args.batch_size):
            batch = torch.stack([image_tensor(args.data_root, row["path"], transform)
                                 for row in training[index:index + args.batch_size]])
            model(batch)  # anomalib accumulates normal embeddings for coreset selection.
        model.subsample_embedding(sampling_ratio=args.coreset_ratio)
    fit_seconds = time.perf_counter() - started
    model.eval()
    validation_scores = np.array([score_image(model, transform, args.data_root, row["path"]) for row in validation])
    threshold = float(np.quantile(validation_scores, args.normal_quantile, method="higher"))
    torch.save(model.state_dict(), args.output / "model.pt")
    weights_hash = file_hash(args.output / "model.pt")
    calibration = {"manifest_id": manifest["manifest_id"], "threshold": threshold,
                   "quantile": args.normal_quantile, "method": "higher", "operator": ">"}
    metadata = {
        "category": manifest["category"], "manifest_id": manifest["manifest_id"], "config": config,
        "model_version": f"patchcore-{manifest['category']}-{weights_hash[:12]}",
        "threshold_version": "normal-quantile-" + hashlib.sha256(encoded(calibration)).hexdigest()[:12],
        "threshold": threshold, "weights_sha256": weights_hash, "fit_images": len(training),
        "validation_images": len(validation), "validation_false_reject_rate": float(np.mean(validation_scores > threshold)),
        "memory_bank_shape": list(model.memory_bank.shape), "fit_seconds": fit_seconds,
        "device": "cpu", "platform": platform.platform(),
        "versions": {name: importlib.metadata.version(name) for name in (
            "anomalib", "torch", "torchvision", "timm", "numpy", "pillow", "scikit-learn")},
    }
    (args.output / "metadata.json").write_bytes(encoded(metadata))
    print(json.dumps(metadata, indent=2))


def evaluate(args):
    manifest = read_manifest(args.manifest, args.data_root)
    model, transform, metadata = load_model(args.model)
    if manifest["manifest_id"] != metadata["manifest_id"] or manifest["category"] != metadata["category"]:
        raise ValueError("Evaluation manifest/category differs from the fitted model")
    args.output.mkdir(parents=True, exist_ok=False)
    normal = next(row for row in manifest["images"] if row["split"] == "validation")
    score_image(model, transform, args.data_root, normal["path"])  # Warm-up excluded from timing.
    predictions, durations = [], []
    for row in manifest["images"]:
        if row["split"] != "test":
            continue
        started = time.perf_counter()
        score = score_image(model, transform, args.data_root, row["path"])
        durations.append(time.perf_counter() - started)
        predictions.append({"path": row["path"], "anomaly_score": score, "is_reject": score > metadata["threshold"]})
    # Ground truth is joined only after predictions using the frozen threshold exist.
    labels = json.loads(checked_path(args.manifest.parent, manifest["labels_file"]).read_text())
    by_path = {row["path"]: row["is_anomaly"] for row in labels["labels"]}
    truth = [by_path[row["path"]] for row in predictions]
    decisions = [row["is_reject"] for row in predictions]
    scores = [row["anomaly_score"] for row in predictions]
    precision, recall, f1, _ = precision_recall_fscore_support(truth, decisions, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(truth, decisions, labels=[False, True]).ravel().tolist()
    summary = {
        "category": metadata["category"], "manifest_id": manifest["manifest_id"],
        "model_version": metadata["model_version"], "threshold_version": metadata["threshold_version"],
        "threshold": metadata["threshold"], "test_images": len(truth),
        "auroc": float(roc_auc_score(truth, scores)) if len(set(truth)) == 2 else None,
        "average_precision": float(average_precision_score(truth, scores)) if len(set(truth)) == 2 else None,
        "precision": float(precision), "recall": float(recall), "f1": float(f1),
        "confusion": {"true_normal": tn, "false_reject": fp, "missed_anomaly": fn, "true_anomaly": tp},
        "test_normal_false_reject_rate": fp / (tn + fp) if tn + fp else None,
        "batch_one_seconds": {"median": float(np.median(durations)), "p95": float(np.quantile(durations, .95))},
        "timing_scope": "CPU batch-one image decode, RGB resize/normalization, model and image-score extraction; one validation warm-up excluded",
        "config": metadata["config"], "versions": metadata["versions"], "device": "cpu",
    }
    (args.output / "predictions.json").write_bytes(encoded({
        "model_version": metadata["model_version"], "threshold_version": metadata["threshold_version"],
        "predictions": predictions,
    }))
    (args.output / "summary.json").write_bytes(encoded(summary))
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("fit", "evaluate"):
        command = commands.add_parser(name)
        command.add_argument("--data-root", type=Path, required=True)
        command.add_argument("--manifest", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True, help="New artifact directory; never overwritten")
        if name == "evaluate":
            command.add_argument("--model", type=Path, required=True)
        else:
            command.add_argument("--image-size", type=int, default=128)
            command.add_argument("--coreset-ratio", type=float, default=.01)
            command.add_argument("--batch-size", type=int, default=8)
            command.add_argument("--threads", type=int, default=2)
            command.add_argument("--seed", type=int, default=42)
            command.add_argument("--normal-quantile", type=float, default=.95)
    args = parser.parse_args()
    if args.command == "fit":
        if (args.image_size < 32 or args.batch_size < 1 or args.threads < 1
                or not 0 < args.coreset_ratio <= 1 or not 0 < args.normal_quantile < 1):
            parser.error("Use a size >=32, positive batch/thread limits, coreset ratio in (0,1] and quantile in (0,1)")
        fit(args)
    else:
        evaluate(args)


if __name__ == "__main__":
    main()
