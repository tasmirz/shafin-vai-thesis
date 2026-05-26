#!/usr/bin/env python3
"""Validate local OSM source readiness for the Bangladesh road PTD replication."""

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def repository_path(value: str) -> Path:
  path = (ROOT / value).resolve()
  if ROOT not in path.parents and path != ROOT:
    raise ValueError(f"path must stay within repository: {value}")
  return path


def sha256(path: Path) -> str:
  digest = hashlib.sha256()
  with path.open("rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""):
      digest.update(block)
  return digest.hexdigest()


def check(condition: bool, message: str, checks: list[dict]) -> None:
  checks.append({"check": message, "passed": bool(condition)})


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--config",
      default="config/research/bangladesh-osm-replication.json",
      help="replication protocol relative to the repository root")
  parser.add_argument("--output", help="optional path for the JSON readiness report")
  parser.add_argument(
      "--checksum",
      action="store_true",
      help="calculate SHA-256 for the large source file for a frozen artifact manifest")
  args = parser.parse_args()

  config_path = repository_path(args.config)
  config = json.loads(config_path.read_text())
  source = config["source"]
  data_path = repository_path(source["dataPath"])
  metadata_path = repository_path(source["metadataPath"])
  readme_path = repository_path(source["readmePath"])

  checks: list[dict] = []
  check(config_path.exists(), "replication protocol exists", checks)
  check(data_path.exists(), "configured OSM GeoPackage exists", checks)
  check(metadata_path.exists(), "source metadata exists", checks)
  check(readme_path.exists(), "source provenance README exists", checks)

  metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
  geometry_types = metadata.get("geometry_types", {})
  required_geometry = source["requiredGeometryType"]
  line_count = int(geometry_types.get(required_geometry, 0))
  check(
      line_count >= int(source["minimumLineFeatures"]),
      f"{required_geometry} supply meets baseline-scale minimum",
      checks)
  check(
      len(metadata.get("bbox", [])) == 4,
      "source contains a declared Bangladesh bounding box",
      checks)

  readme = readme_path.read_text(errors="replace") if readme_path.exists() else ""
  snapshot_match = re.search(r"^Snapshot:\s*(\S+)", readme, re.MULTILINE)
  snapshot = snapshot_match.group(1) if snapshot_match else None
  check(snapshot == source["expectedSnapshot"], "OSM snapshot matches protocol", checks)
  check("OpenStreetMap contributors" in readme, "OSM attribution is recorded", checks)

  geometry = config["geometry"]
  check(
      geometry["projectedUnits"] == "metre" and geometry["projectedCrs"].startswith("EPSG:"),
      "metric projected CRS is declared before MBR generation",
      checks)
  uncertainty = config["uncertainty"]
  check(
      uncertainty["instancesPerObject"] == {"minimum": 5, "maximum": 11},
      "upgrade-paper instance range is fixed at 5-11",
      checks)

  report = {
      "profile": config["profile"],
      "generatedUtc": datetime.now(timezone.utc).isoformat(),
      "source": {
          "path": source["dataPath"],
          "sizeBytes": data_path.stat().st_size if data_path.exists() else None,
          "sha256": sha256(data_path) if args.checksum and data_path.exists() else None,
          "snapshot": snapshot,
          "featureCount": metadata.get("feature_count"),
          "lineFeatureCount": line_count,
          "bbox": metadata.get("bbox")
      },
      "checks": checks,
      "readyForCuration": all(item["passed"] for item in checks),
      "curationStatus": config["status"]["curation"],
      "paperReproductionClaim": config["status"]["paperReproductionClaim"]
  }

  rendered = json.dumps(report, indent=2) + "\n"
  if args.output:
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)
  print(rendered, end="")
  if not report["readyForCuration"]:
    raise SystemExit(1)


if __name__ == "__main__":
  main()
