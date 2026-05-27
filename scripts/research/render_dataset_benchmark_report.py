#!/usr/bin/env python3
"""Compile multi-dataset CSV treatment and MQTT/Kafka/Spark benchmark evidence."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ROOT = ROOT / "reports" / "runs"
OUTPUT_ROOT = ROOT / "reports" / "publication"
VARIANTS = ("baseline", "aes-only", "dscp-only", "aes-dscp")


def read_run(run_id: str) -> tuple[dict, dict]:
  directory = RUN_ROOT / run_id
  return (
      json.loads((directory / "metrics.json").read_text()),
      json.loads((directory / "manifest.json").read_text()),
  )


def treatment_row(label: str, suite_id: str) -> dict:
  runs = {variant: read_run(f"{suite_id}-{variant}") for variant in VARIANTS}
  baseline, baseline_manifest = runs["baseline"]
  reference = baseline["spark"]
  dataset_hash = baseline_manifest["dataset"]["sha256"]
  query_hash = baseline_manifest.get("querySet", {}).get("sha256")
  for variant, (_, manifest) in runs.items():
    if manifest["dataset"]["sha256"] != dataset_hash:
      raise SystemExit(f"{suite_id}: {variant} has a different dataset checksum")
    if manifest.get("querySet", {}).get("sha256") != query_hash:
      raise SystemExit(f"{suite_id}: {variant} has a different query-set checksum")

  def elapsed(variant: str) -> int:
    return runs[variant][0]["spark"]["algorithmElapsedMs"]

  def reduction(variant: str) -> float:
    return (elapsed("baseline") - elapsed(variant)) / elapsed("baseline") * 100.0

  full = runs["aes-dscp"][0]["spark"]
  return {
      "dataset": label,
      "suite_id": suite_id,
      "dataset_sha256": dataset_hash,
      "query_set_sha256": query_hash or "embedded-in-csv",
      "objects_per_query": reference["queries"][0]["objects"],
      "instances": reference["probabilisticInstances"],
      "queries": len(reference["queries"]),
      "k": reference["k"],
      "partitions": reference["partitions"],
      "baseline_ms": elapsed("baseline"),
      "aes_only_ms": elapsed("aes-only"),
      "aes_only_reduction_pct": reduction("aes-only"),
      "dscp_only_ms": elapsed("dscp-only"),
      "dscp_only_reduction_pct": reduction("dscp-only"),
      "aes_dscp_ms": elapsed("aes-dscp"),
      "aes_dscp_reduction_pct": reduction("aes-dscp"),
      "baseline_emissions": reference["totalEmittedRecords"],
      "aes_dscp_emissions": full["totalEmittedRecords"],
      "emission_reduction_pct": (
          (reference["totalEmittedRecords"] - full["totalEmittedRecords"])
          / reference["totalEmittedRecords"] * 100.0),
      "baseline_shuffle_bytes": reference["totalShuffleBytes"],
      "aes_dscp_shuffle_bytes": full["totalShuffleBytes"],
      "shuffle_reduction_pct": (
          (reference["totalShuffleBytes"] - full["totalShuffleBytes"])
          / reference["totalShuffleBytes"] * 100.0),
      "full_validation": "executed" if all(
          query.get("validationPerformed") for query in full["queries"]) else "not executed",
  }


def stream_row(label: str, run_id: str) -> dict:
  metrics, manifest = read_run(run_id)
  spark = metrics["spark"]
  e2e = metrics["e2e"]
  return {
      "dataset": label,
      "run_id": run_id,
      "dataset_sha256": manifest["dataset"]["sha256"],
      "algorithm": spark["algorithm"],
      "messages": int(e2e.get("expected_messages", spark["rawEvents"])),
      "queries": len(spark["queries"]),
      "algorithm_ms": spark["algorithmElapsedMs"],
      "pipeline_total_ms": int(e2e["total_ms"]) if e2e.get("total_ms") else None,
      "e2e_rate_msg_s": e2e.get("e2e_rate_msg_s"),
      "bound_mode": spark["boundMode"],
      "indexed_mbr_path": spark["indexedMbrPath"],
      "validation": metrics["validation"]["exactTopKAgreement"],
  }


def write_csv(path: Path, rows: list[dict]) -> None:
  if not rows:
    return
  with path.open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)


def parse_assignment(value: str) -> tuple[str, str]:
  if "=" not in value:
    raise argparse.ArgumentTypeError("value must be LABEL=RUN_OR_SUITE_ID")
  return tuple(value.split("=", 1))


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--suite", action="append", default=[], type=parse_assignment)
  parser.add_argument("--stream", action="append", default=[], type=parse_assignment)
  parser.add_argument("--output-dir", default=str(OUTPUT_ROOT))
  args = parser.parse_args()
  treatments = [treatment_row(label, suite) for label, suite in args.suite]
  streams = [stream_row(label, run) for label, run in args.stream]
  output = Path(args.output_dir)
  output.mkdir(parents=True, exist_ok=True)
  write_csv(output / "all-dataset-treatment-matrix.csv", treatments)
  write_csv(output / "all-dataset-stream-matrix.csv", streams)

  lines = [
      "# Multi-Dataset Benchmark Evidence",
      "",
      f"Generated: {datetime.now(timezone.utc).isoformat()}",
      "",
      "## Claim Boundary",
      "",
      "CSV treatment rows compare the Spark indexed baseline with AES and DSCP treatments on",
      "identical saved data and query-set checksums. MQTT/Kafka/Spark rows measure the bounded",
      "stream ingress and execution route separately. Published Hadoop times are reference values",
      "only; they are not combined with these Spark measurements as an engine speedup.",
      "",
      "## CSV Treatment Matrix",
      "",
      "| Dataset | Queries | Indexed baseline | AES-only | DSCP-only | AES+DSCP | Full reduction | Emission reduction | Shuffle reduction |",
      "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
  ]
  for row in treatments:
    lines.append(
        f"| {row['dataset']} | {row['queries']} | {row['baseline_ms']:,} ms | "
        f"{row['aes_only_ms']:,} ms | {row['dscp_only_ms']:,} ms | "
        f"{row['aes_dscp_ms']:,} ms | {row['aes_dscp_reduction_pct']:.2f}% | "
        f"{row['emission_reduction_pct']:.2f}% | {row['shuffle_reduction_pct']:.2f}% |")
  lines.extend([
      "",
      "## MQTT -> Kafka -> Spark Matrix",
      "",
      "| Dataset | Messages | Queries | Algorithm time | End-to-end time | Rate (msg/s) | Indexed MBR path | Exact validation |",
      "|---|---:|---:|---:|---:|---:|---|---|",
  ])
  for row in streams:
    validation = "not run" if row["validation"] is None else str(row["validation"]).lower()
    lines.append(
        f"| {row['dataset']} | {row['messages']:,} | {row['queries']} | "
        f"{row['algorithm_ms']:,} ms | {row['pipeline_total_ms']:,} ms | "
        f"{row['e2e_rate_msg_s']} | {str(row['indexed_mbr_path']).lower()} | {validation} |")
  lines.extend([
      "",
      "## Dataset Provenance",
      "",
      "- Synthetic smartphone follows the ICCIT paper generator controls.",
      "- Bangladesh OSM road is an ICCIT-style road-MBR artifact curated from the supplied OSM layer.",
      "- California/TIGER LA roads use the supplied `tl_2018_06037_roads` layer and match the",
      "  Rai-Lian object-count scale; they are not asserted to be the paper's exact California file.",
      "",
      "## Files",
      "",
      "- `reports/publication/all-dataset-treatment-matrix.csv`",
      "- `reports/publication/all-dataset-stream-matrix.csv`",
  ])
  (output / "all-dataset-benchmark-report.md").write_text("\n".join(lines) + "\n")
  (output / "all-dataset-benchmark-report.json").write_text(json.dumps({
      "generatedUtc": datetime.now(timezone.utc).isoformat(),
      "treatments": treatments,
      "streams": streams,
      "claimBoundary": "same-machine Spark comparisons only; Hadoop papers are reference",
  }, indent=2, allow_nan=False) + "\n")
  print(f"datasetBenchmarkReport={output / 'all-dataset-benchmark-report.md'}")


if __name__ == "__main__":
  main()
