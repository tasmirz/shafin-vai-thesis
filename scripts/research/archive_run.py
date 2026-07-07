#!/usr/bin/env python3
"""Store one finite PTD experiment as a comparable research run artifact."""

import argparse
import csv
import hashlib
import json
import math
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
  engine_match = re.search(r"^engine=(apache-(?:spark|hadoop)) (.+)$", metric_log, re.MULTILINE)
  engine = dict(re.findall(r"(\w+)=([^\s]+)", engine_match.group(2))) if engine_match else {}
  execution_engine = engine_match.group(1) if engine_match else "unknown"
  count_match = re.search(r"^rawEvents=(\d+) probabilisticInstances=(\d+)", metric_log, re.MULTILINE)
  rankings = []
  trace_rows = []
  # Prefer the compact record: it is emitted on one complete line after each presentation row,
  # while Spark WARN output has occasionally split the longer TopKResult presentation record.
  compact_lines = [
      line for line in re.findall(r"^query=.+$", metric_log, re.MULTILINE)
      if " algorithm=" in line and " objects=" in line
  ]
  if compact_lines:
    rows = [
        dict(re.findall(r"(\w+)=([^\s]+)", line))
        for line in compact_lines
    ]
  else:
    result_lines = re.findall(
        r"^TopKResult\{engine=apache-spark, (.+?)\}$", metric_log, re.MULTILINE)
    rows = [
        dict(re.findall(r"(\w+)=([^,}]+)", line))
        for line in result_lines
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
  for line in re.findall(r"^ObjectTrace\{(.+?)\}$", metric_log, re.MULTILINE):
    fields = dict(re.findall(r"(\w+)=([^,}]+)", line))
    tau = float(fields["tau"])
    trace_rows.append({
        "queryId": fields.get("queryId"),
        "objectId": fields.get("objectId"),
        "partition": int(fields["partition"]),
        "lb": float(fields["lb"]),
        "ub": float(fields["ub"]),
        "tau": None if math.isnan(tau) else tau,
        "decision": fields["decision"],
        "partialMbrRefs": int(fields["partialMbrRefs"]),
        "baselineEmissions": int(fields["baselineEmissions"]),
        "aesEmissions": int(fields["aesEmissions"]),
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
          "engine": execution_engine,
          "source": engine.get("source"),
          "dataset": engine.get("dataset"),
          "k": int(engine["k"]) if "k" in engine else None,
          "partitions": int(engine["partitions"]) if "partitions" in engine else None,
          "elapsedMs": int(engine["elapsedMs"]) if "elapsedMs" in engine else None,
          "algorithmElapsedMs": (
              int(engine["algorithmElapsedMs"]) if "algorithmElapsedMs" in engine else None),
          "setupMs": int(engine["setupMs"]) if "setupMs" in engine else None,
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
          "objectTraces": trace_rows,
      },
      "validation": {
          "queriesChecked": len(all_agreement),
          "exactTopKAgreement": exact_agreement,
      },
      "algorithm": benchmark_summary,
      "e2e": e2e,
  }


def parse_topk_results(spark_log):
  query_header_regex = re.compile(r"(?:query=|queryId=)([A-Za-z0-9._-]+)", re.IGNORECASE)
  rank_regex = re.compile(r"^\s*rank\s+object=([^\s]+)\s+score=([^\s]+)\s+lb=([^\s]+)\s+ub=([^\s]+)\s+instances=(\d+)", re.IGNORECASE)
  candidate_score_regex = re.compile(r"CandidateScore\s*\{\s*objectId\s*=\s*'([^']*)'\s*,\s*queryId\s*=\s*'([^']*)'\s*,\s*exactScore\s*=\s*([^,]*)\s*,\s*lowerBound\s*=\s*([^,]*)\s*,\s*upperBound\s*=\s*([^,]*)\s*,\s*instanceCount\s*=\s*(\d+)", re.IGNORECASE)

  queries = []
  query_map = {}
  current_query_id = "Default Query"

  for line in spark_log.splitlines():
    if "TopKResult" in line or line.startswith("query="):
      match = query_header_regex.search(line)
      if match:
        current_query_id = match.group(1)

    rank_match = rank_regex.search(line)
    if rank_match:
      object_id = rank_match.group(1)
      score = float(rank_match.group(2))
      lb = float(rank_match.group(3))
      ub = float(rank_match.group(4))
      instances = int(rank_match.group(5))

      if current_query_id not in query_map:
        query_map[current_query_id] = {"queryId": current_query_id, "ranks": []}
        queries.append(query_map[current_query_id])

      if not any(r["objectId"] == object_id for r in query_map[current_query_id]["ranks"]):
        query_map[current_query_id]["ranks"].append({
          "objectId": object_id,
          "score": score,
          "lb": lb,
          "ub": ub,
          "instances": instances
        })
      continue

    if "CandidateScore" in line:
      for match in candidate_score_regex.finditer(line):
        object_id = match.group(1)
        q_id = match.group(2) or current_query_id
        score = float(match.group(3))
        lb = float(match.group(4))
        ub = float(match.group(5))
        instances = int(match.group(6))

        if q_id not in query_map:
          query_map[q_id] = {"queryId": q_id, "ranks": []}
          queries.append(query_map[q_id])

        if not any(r["objectId"] == object_id for r in query_map[q_id]["ranks"]):
          query_map[q_id]["ranks"].append({
            "objectId": object_id,
            "score": score,
            "lb": lb,
            "ub": ub,
            "instances": instances
          })

  for q in queries:
    q["ranks"].sort(key=lambda x: x["score"], reverse=True)

  return queries


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--run-id", required=True)
  parser.add_argument("--mode", required=True, choices=("csv", "stream", "simulator", "hadoop"))
  parser.add_argument("--spark-log", required=True)
  parser.add_argument("--algorithm-log")
  parser.add_argument("--e2e-summary")
  parser.add_argument("--dataset-file")
  parser.add_argument("--dataset-manifest")
  parser.add_argument("--query-set-file")
  parser.add_argument("--query-set-manifest")
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
  if args.query_set_file:
    shutil.copyfile(args.query_set_file, run_dir / "query-set.csv")
    manifest["querySet"] = {
        "path": args.query_set_file,
        "sha256": sha256(args.query_set_file),
    }
    manifest["artifacts"].append("query-set.csv")
  if args.query_set_manifest:
    shutil.copyfile(args.query_set_manifest, run_dir / "query-set-manifest.json")
    manifest["artifacts"].append("query-set-manifest.json")

  (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
  (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, allow_nan=False) + "\n")
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
  traces = metrics["spark"].get("objectTraces", [])
  if traces:
    with (run_dir / "object-traces.csv").open("w", newline="") as target:
      writer = csv.DictWriter(target, fieldnames=[
          "queryId", "objectId", "partition", "lb", "ub", "tau", "decision",
          "partialMbrRefs", "baselineEmissions", "aesEmissions"])
      writer.writeheader()
      writer.writerows(traces)
    manifest["artifacts"].append("object-traces.csv")

  # Extract and save Top-K results
  topk_results = parse_topk_results(spark_log)
  if topk_results:
    (run_dir / "topk-results.json").write_text(json.dumps(topk_results, indent=2, allow_nan=False) + "\n")
    manifest["artifacts"].append("topk-results.json")
    
    with (run_dir / "topk-results.csv").open("w", newline="") as target:
      writer = csv.DictWriter(target, fieldnames=[
          "query_id", "rank", "object_id", "score", "lower_bound", "upper_bound", "instances"])
      writer.writeheader()
      for q in topk_results:
        q_id = q["queryId"]
        for idx, rank_entry in enumerate(q["ranks"]):
          writer.writerow({
              "query_id": q_id,
              "rank": idx + 1,
              "object_id": rank_entry["objectId"],
              "score": rank_entry["score"],
              "lower_bound": rank_entry["lb"],
              "upper_bound": rank_entry["ub"],
              "instances": rank_entry["instances"]
          })
    manifest["artifacts"].append("topk-results.csv")
    print(f"savedTopKResults={run_dir.relative_to(ROOT)}/topk-results.csv")

  (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
  print(f"savedRun={run_dir.relative_to(ROOT)}")


if __name__ == "__main__":
  main()
