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
  # The engine line begins with a key rather than a marker.
  engine_match = re.search(r"^engine=apache-spark (.+)$", spark_log, re.MULTILINE)
  engine = dict(re.findall(r"(\w+)=([^\s]+)", engine_match.group(1))) if engine_match else {}
  count_match = re.search(r"^rawEvents=(\d+) probabilisticInstances=(\d+)", spark_log, re.MULTILINE)
  rankings = [
      {
          "queryId": m.group(1),
          "objects": int(m.group(2)),
          "refined": int(m.group(3)),
          "pruned": int(m.group(4)),
          "pruneRatio": float(m.group(5)),
          "tau": m.group(6),
          "compactShuffleRecords": int(m.group(7)),
      }
      for m in re.finditer(
          r"^query=(\S+) objects=(\d+) refined=(\d+) pruned=(\d+) "
          r"pruneRatio=([0-9.]+) tau=(\S+) compactShuffleRecords=(\d+)",
          spark_log,
          re.MULTILINE)
  ]
  agreement = re.findall(r"topKAgreement=(true|false)", algorithm_log)
  spark_agreement = re.findall(
      r"^query=.*validationPerformed=true exactAgreement=(true|false)", spark_log, re.MULTILINE)
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
          "rawEvents": int(count_match.group(1)) if count_match else None,
          "probabilisticInstances": int(count_match.group(2)) if count_match else None,
          "structuredStreamingKafka": "reader=structured-streaming" in spark_log,
          "queries": rankings,
          "avgPruneRatio": (
              sum(row["pruneRatio"] for row in rankings) / len(rankings) if rankings else None),
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
  parser.add_argument("--parameter", action="append", default=[])
  args = parser.parse_args()
  if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.run_id):
    raise SystemExit("run ID may contain only letters, digits, '.', '_' and '-'")

  parameters = dict(value.split("=", 1) for value in args.parameter)
  run_dir = RUN_ROOT / args.run_id
  if run_dir.exists():
    raise SystemExit(f"run already exists: {run_dir.relative_to(ROOT)}")
  run_dir.mkdir(parents=True)

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

  shutil.copyfile(args.spark_log, run_dir / "spark.log")
  if args.algorithm_log:
    shutil.copyfile(args.algorithm_log, run_dir / "algorithm.log")
    manifest["artifacts"].append("algorithm.log")
  if args.e2e_summary:
    shutil.copyfile(args.e2e_summary, run_dir / "e2e-summary.csv")
    manifest["artifacts"].append("e2e-summary.csv")

  (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
  (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
  query_rows = metrics["spark"]["queries"]
  with (run_dir / "metrics.csv").open("w", newline="") as target:
    writer = csv.DictWriter(target, fieldnames=[
        "run_id", "mode", "query_id", "objects", "refined", "pruned", "prune_ratio",
        "tau", "compact_shuffle_records", "algorithm_elapsed_ms", "validation_ms",
        "exact_topk_agreement"])
    writer.writeheader()
    for ranking in query_rows:
      writer.writerow({
          "run_id": args.run_id,
          "mode": args.mode,
          "query_id": ranking["queryId"],
          "objects": ranking["objects"],
          "refined": ranking["refined"],
          "pruned": ranking["pruned"],
          "prune_ratio": ranking["pruneRatio"],
          "tau": ranking["tau"],
          "compact_shuffle_records": ranking["compactShuffleRecords"],
          "algorithm_elapsed_ms": metrics["spark"]["algorithmElapsedMs"],
          "validation_ms": metrics["spark"]["validationMs"],
          "exact_topk_agreement": metrics["validation"]["exactTopKAgreement"],
      })
  print(f"savedRun={run_dir.relative_to(ROOT)}")


if __name__ == "__main__":
  main()
