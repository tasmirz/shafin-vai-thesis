#!/usr/bin/env python3
"""Compare persisted PTD benchmark runs and flag non-comparable setups."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ROOT = ROOT / "reports" / "runs"
FAIR_FIELDS = (
    "dataset.sha256",
    "parameters.dataset",
    "parameters.objects",
    "parameters.queries",
    "parameters.dimensions",
    "parameters.missingRate",
    "parameters.k",
    "parameters.partitions",
    "parameters.seed",
)


def value(document, dotted):
  current = document
  for key in dotted.split("."):
    current = current.get(key) if isinstance(current, dict) else None
  return current


def read_run(identifier):
  path = Path(identifier)
  path = path if path.is_dir() else RUN_ROOT / identifier
  manifest = json.loads((path / "manifest.json").read_text())
  metrics = json.loads((path / "metrics.json").read_text())
  return path, manifest, metrics


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("runs", nargs="+")
  args = parser.parse_args()
  if len(args.runs) < 2:
    raise SystemExit("provide at least two saved run IDs or directories")

  loaded = [read_run(run) for run in args.runs]
  differences = []
  for field in FAIR_FIELDS:
    values = [value(manifest, field) for _, manifest, _ in loaded]
    if len(set(values)) > 1:
      differences.append(f"{field}: " + ", ".join(str(item) for item in values))

  print("| run | mode | source | dataset | algorithm ms | validation ms | avg prune ratio | exact agreement |")
  print("| --- | --- | --- | --- | ---: | ---: | ---: | --- |")
  for _, manifest, metrics in loaded:
    spark = metrics["spark"]
    validation = metrics["validation"]
    prune = spark["avgPruneRatio"]
    print(
        f"| {manifest['runId']} | {manifest['mode']} | {spark['source']} | "
        f"{spark['dataset']} | {spark.get('algorithmElapsedMs')} | {spark.get('validationMs')} | "
        f"{prune:.4f} | {validation['exactTopKAgreement']} |")
  if differences:
    print("\nComparison warning: fairness-critical configuration differs.")
    for difference in differences:
      print(f"- {difference}")
  else:
    print("\nComparison hygiene: fairness-critical recorded configuration matches.")


if __name__ == "__main__":
  main()
