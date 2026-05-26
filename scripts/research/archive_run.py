#!/usr/bin/env python3
"""Store one finite PTD experiment as a comparable research run artifact."""

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ROOT = ROOT / "reports" / "runs"


def read_text(path):
  return Path(path).read_text(errors="replace") if path else ""


def git_metadata():
  def git(*args):
    result = subprocess.run(
        ["git", *args], cwd=ROOT, text=True, capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else "unknown"

  return {
      "commit": git("rev-parse", "HEAD"),
      "branch": git("branch", "--show-current"),
      "dirty": bool(git("status", "--porcelain")),
  }


def sha256(path):
  digest = hashlib.sha256()
  with Path(path).open("rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""):
      digest.update(block)
  return digest.hexdigest()


def key_values(text, prefix):
  match = re.search(rf"^{re.escape(prefix)} (.+)$", text, re.MULTILINE)
  if not match:
    return {}
  return dict(re.findall(r"(\w+)=([^\s]+)", match.group(1)))


def parse_metrics(spark_log, algorithm_log, summary_path):
  # Spark writes warnings to stderr while result rows are printed to stdout. When both streams
  # are archived together, a warning can be inserted in the middle of a result row.
  metric_log = re.sub(
      r"\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} WARN [^\n]*(?:\n|$)", "", spark_log)
  # The engine line begins with a key rather than a marker.
  engine_match = re.search(r"^engine=apache-spark (.+)$", metric_log, re.MULTILINE)
  engine = dict(re.findall(r"(\w+)=([^\s]+)", engine_match.group(1))) if engine_match else {}
  count_match = re.search(r"^rawEvents=(\d+) probabilisticInstances=(\d+)", metric_log, re.MULTILINE)
  rankings = []
  # Prefer comma-delimited result records: Spark WARN output can interleave with the compact
  # query line when stdout and stderr are redirected into one experiment log.
  result_lines = re.findall(r"^TopKResult\{engine=apache-spark, (.+?)\}$", metric_log, re.MULTILINE)
  if result_lines:
    rows = [
        dict(re.findall(r"(\w+)=([^,}]+)", line))
        for line in result_lines
    ]
  else:
    rows = [
        dict(re.findall(r"(\w+)=([^\s]+)", line))
        for line in re.findall(r"^query=.+$", metric_log, re.MULTILINE)
        if " algorithm=" in line and " objects=" in line
    ]
  for fields in rows:
    emitted = fields.get("emittedRecords", fields.get("compactShuffleRecords", "0"))
    rankings.append({
        "queryId": fields.get("query", fields.get("queryId")),
        "algorithm": fields.get("algorithm", engine.get("algorithm")),
        "objects": int(fields["objects"]),
        "refined": int(fields["refined"]),
        "pruned": int(fields["pruned"]),
        "pruneRatio": float(fields["pruneRatio"]),
        "tau": fields["tau"],
        "emittedRecords": int(emitted),
        "compactShuffleRecords": int(emitted),
        "baselineEmissions": int(fields.get("baselineEmissions", emitted)),
        "aesEmissions": int(fields.get("aesEmissions", emitted)),
        "aer": float(fields.get("AER", "1.0")),
        "falsePrunes": int(fields.get("falsePrunes", "0")),
        "indexedMbrPath": fields.get("indexedMbrPath") == "true",
        "partialMbrRefs": int(fields.get("partialMbrRefs", "0")),
        "filterMs": int(fields.get("filterMs", "0")),
        "emissionMs": int(fields.get("emissionMs", "0")),
        "refineMs": int(fields.get("refineMs", "0")),
        "shuffleRecords": int(fields.get("shuffleRecords", "0")),
        "shuffleBytes": int(fields.get("shuffleBytes", "0")),
        "tasks": int(fields.get("tasks", "0")),
        "executorRunMs": int(fields.get("executorRunMs", "0")),
        "gcMs": int(fields.get("gcMs", "0")),
        "stragglerRatio": float(fields.get("stragglerRatio", "0.0")),
    })
  agreement = re.findall(r"topKAgreement=(true|false)", algorithm_log)
  spark_agreement = re.findall(
      r"^query=.*validationPerformed=true exactAgreement=(true|false)", metric_log, re.MULTILINE)
  all_agreement = agreement + spark_agreement
  exact_agreement = None if not all_agreement else all(value == "true" for value in all_agreement)
  benchmark_summary = key_values(algorithm_log, "summary")
  e2e = {}
  if summary_path:
    with Path(summary_path).open(newline="") as source:
      rows = list(csv.DictReader(source))
    e2e = rows[-1] if rows else {}
  return {
      "spark": {
          "source": engine.get("source"),
          "dataset": engine.get("dataset"),
          "k": int(engine["k"]) if "k" in engine else None,
          "partitions": int(engine["partitions"]) if "partitions" in engine else None,
          "elapsedMs": int(engine["elapsedMs"]) if "elapsedMs" in engine else None,
          "algorithmElapsedMs": (
              int(engine["algorithmElapsedMs"]) if "algorithmElapsedMs" in engine else None),
          "validationMs": int(engine["validationMs"]) if "validationMs" in engine else None,
          "algorithm": engine.get("algorithm"),
          "dscpEnabled": engine.get("dscp") == "true" if "dscp" in engine else None,
          "aesEnabled": engine.get("aes") == "true" if "aes" in engine else None,
          "boundMode": engine.get("boundMode"),
          "emissionScope": engine.get("emissionScope"),
          "rawEvents": int(count_match.group(1)) if count_match else None,
          "probabilisticInstances": int(count_match.group(2)) if count_match else None,
          "structuredStreamingKafka": "reader=structured-streaming" in spark_log,
          "queries": rankings,
          "avgPruneRatio": (
              sum(row["pruneRatio"] for row in rankings) / len(rankings) if rankings else None),
          "totalEmittedRecords": sum(row["emittedRecords"] for row in rankings),
          "totalBaselineEmissions": sum(row["baselineEmissions"] for row in rankings),
          "totalAesEmissions": sum(row["aesEmissions"] for row in rankings),
          "avgAER": sum(row["aer"] for row in rankings) / len(rankings) if rankings else None,
          "falsePruneCount": sum(row["falsePrunes"] for row in rankings),
          "indexedMbrPath": any(row["indexedMbrPath"] for row in rankings),
          "totalPartialMbrRefs": sum(row["partialMbrRefs"] for row in rankings),
          "totalShuffleRecords": sum(row["shuffleRecords"] for row in rankings),
          "totalShuffleBytes": sum(row["shuffleBytes"] for row in rankings),
          "totalFilterMs": sum(row["filterMs"] for row in rankings),
          "totalEmissionMs": sum(row["emissionMs"] for row in rankings),
          "totalRefineMs": sum(row["refineMs"] for row in rankings),
          "totalExecutorRunMs": sum(row["executorRunMs"] for row in rankings),
          "totalGcMs": sum(row["gcMs"] for row in rankings),
          "maxStragglerRatio": max((row["stragglerRatio"] for row in rankings), default=0.0),
      },
      "validation": {
          "queriesChecked": len(all_agreement),
          "exactTopKAgreement": exact_agreement,
      },
      "algorithm": benchmark_summary,
      "e2e": e2e,
  }


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--run-id", required=True)
  parser.add_argument("--mode", required=True, choices=("csv", "stream", "simulator"))
  parser.add_argument("--spark-log", required=True)
  parser.add_argument("--algorithm-log")
  parser.add_argument("--e2e-summary")
  parser.add_argument("--dataset-file")
  parser.add_argument("--dataset-manifest")
  parser.add_argument("--parameter", action="append", default=[])
  args = parser.parse_args()
  if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.run_id):
    raise SystemExit("run ID may contain only letters, digits, '.', '_' and '-'")

  parameters = dict(value.split("=", 1) for value in args.parameter)
  run_dir = RUN_ROOT / args.run_id
  if run_dir.exists():
    raise SystemExit(f"run already exists: {run_dir.relative_to(ROOT)}")

  spark_log = read_text(args.spark_log)
  algorithm_log = read_text(args.algorithm_log)
  metrics = parse_metrics(spark_log, algorithm_log, args.e2e_summary)
  manifest = {
      "schemaVersion": 1,
      "runId": args.run_id,
      "createdUtc": datetime.now(timezone.utc).isoformat(),
      "mode": args.mode,
      "git": git_metadata(),
      "parameters": parameters,
      "dataset": {
          "path": args.dataset_file,
          "sha256": sha256(args.dataset_file) if args.dataset_file else None,
      },
      "artifacts": ["manifest.json", "metrics.json", "metrics.csv", "spark.log"],
  }

  run_dir.mkdir(parents=True)
  shutil.copyfile(args.spark_log, run_dir / "spark.log")
  if args.algorithm_log:
    shutil.copyfile(args.algorithm_log, run_dir / "algorithm.log")
    manifest["artifacts"].append("algorithm.log")
  if args.e2e_summary:
    shutil.copyfile(args.e2e_summary, run_dir / "e2e-summary.csv")
    manifest["artifacts"].append("e2e-summary.csv")
  if args.dataset_manifest:
    shutil.copyfile(args.dataset_manifest, run_dir / "dataset-manifest.json")
    manifest["artifacts"].append("dataset-manifest.json")

  (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
  (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
  query_rows = metrics["spark"]["queries"]
  with (run_dir / "metrics.csv").open("w", newline="") as target:
    writer = csv.DictWriter(target, fieldnames=[
        "run_id", "mode", "algorithm", "query_id", "objects", "refined", "pruned",
        "prune_ratio", "tau", "emitted_records", "baseline_emissions", "aes_emissions",
        "aer", "false_prunes", "filter_ms", "emission_ms", "refine_ms", "shuffle_records",
        "shuffle_bytes", "tasks", "executor_run_ms", "gc_ms", "straggler_ratio",
        "algorithm_elapsed_ms", "validation_ms", "exact_topk_agreement"])
    writer.writeheader()
    for ranking in query_rows:
      writer.writerow({
          "run_id": args.run_id,
          "mode": args.mode,
          "algorithm": ranking["algorithm"],
          "query_id": ranking["queryId"],
          "objects": ranking["objects"],
          "refined": ranking["refined"],
          "pruned": ranking["pruned"],
          "prune_ratio": ranking["pruneRatio"],
          "tau": ranking["tau"],
          "emitted_records": ranking["emittedRecords"],
          "baseline_emissions": ranking["baselineEmissions"],
          "aes_emissions": ranking["aesEmissions"],
          "aer": ranking["aer"],
          "false_prunes": ranking["falsePrunes"],
          "filter_ms": ranking["filterMs"],
          "emission_ms": ranking["emissionMs"],
          "refine_ms": ranking["refineMs"],
          "shuffle_records": ranking["shuffleRecords"],
          "shuffle_bytes": ranking["shuffleBytes"],
          "tasks": ranking["tasks"],
          "executor_run_ms": ranking["executorRunMs"],
          "gc_ms": ranking["gcMs"],
          "straggler_ratio": ranking["stragglerRatio"],
          "algorithm_elapsed_ms": metrics["spark"]["algorithmElapsedMs"],
          "validation_ms": metrics["spark"]["validationMs"],
          "exact_topk_agreement": metrics["validation"]["exactTopKAgreement"],
      })
  print(f"savedRun={run_dir.relative_to(ROOT)}")


if __name__ == "__main__":
  main()
