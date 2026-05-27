#!/usr/bin/env python3
"""Create a fixed query-point sidecar for an immutable spatial PTD dataset artifact."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
  digest = hashlib.sha256()
  with path.open("rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""):
      digest.update(block)
  return digest.hexdigest()


def extent_from_manifest(document: dict) -> list[float]:
  bounds = [entry["mbr"] for entry in document["partitionIndex"] if entry.get("mbr")]
  if not bounds:
    raise SystemExit("Dataset manifest has no populated partition MBR bounds.")
  return [
      min(value[0] for value in bounds),
      min(value[1] for value in bounds),
      max(value[2] for value in bounds),
      max(value[3] for value in bounds),
  ]


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--dataset-manifest", required=True)
  parser.add_argument("--output", required=True)
  parser.add_argument("--manifest", required=True)
  parser.add_argument("--queries", type=int, default=20)
  parser.add_argument("--seed", type=int, default=42)
  args = parser.parse_args()
  if args.queries < 1:
    raise SystemExit("queries must be positive")

  dataset_manifest_path = Path(args.dataset_manifest)
  dataset_manifest = json.loads(dataset_manifest_path.read_text())
  extent = extent_from_manifest(dataset_manifest)
  rng = random.Random(args.seed)
  points = [
      (rng.uniform(extent[0], extent[2]), rng.uniform(extent[1], extent[3]))
      for _ in range(args.queries)
  ]
  output = Path(args.output)
  output.parent.mkdir(parents=True, exist_ok=True)
  with output.open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(["queryId", "queryA0", "queryA1"])
    for index, point in enumerate(points):
      writer.writerow([f"q{index}", f"{point[0]:.3f}", f"{point[1]:.3f}"])

  manifest = {
      "schemaVersion": 1,
      "artifact": "ptd-query-set",
      "createdUtc": datetime.now(timezone.utc).isoformat(),
      "querySetPath": str(output),
      "querySetSha256": sha256(output),
      "datasetManifestPath": str(dataset_manifest_path),
      "datasetCsvSha256": dataset_manifest["csvSha256"],
      "queries": args.queries,
      "dimensions": 2,
      "seed": args.seed,
      "distribution": "uniform within curated projected dataset extent",
      "extent": extent,
  }
  path = Path(args.manifest)
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
  print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
  main()
