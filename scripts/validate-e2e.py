#!/usr/bin/env python3
import argparse
import csv
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def fail(message):
  raise SystemExit(f"FAIL {message}")


def require(condition, message):
  if not condition:
    fail(message)


def run_json(cmd):
  result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
  if result.returncode != 0:
    fail(f"{' '.join(cmd)} exited {result.returncode}: {result.stderr or result.stdout}")
  try:
    return json.loads(result.stdout)
  except json.JSONDecodeError as exc:
    fail(f"{' '.join(cmd)} did not return JSON: {exc}")


def last_csv_row(path):
  require(path.exists(), f"missing {path.relative_to(ROOT)}")
  with path.open(newline="") as f:
    rows = list(csv.DictReader(f))
  require(rows, f"{path.relative_to(ROOT)} has no rows")
  return rows[-1]


def positive_int(row, key):
  try:
    value = int(row[key])
  except (KeyError, ValueError) as exc:
    fail(f"summary field {key} is not an int: {exc}")
  require(value > 0, f"summary field {key} must be > 0, got {value}")
  return value


def positive_float(row, key):
  try:
    value = float(row[key])
  except (KeyError, ValueError) as exc:
    fail(f"summary field {key} is not a float: {exc}")
  require(value > 0.0, f"summary field {key} must be > 0, got {value}")
  return value


def validate_summary(expected):
  row = last_csv_row(ROOT / "reports/e2e/summary.csv")
  for key in ("expected_messages", "kafka_messages", "topk_results"):
    require(int(row[key]) == expected, f"{key} expected {expected}, got {row[key]}")
  for key in ("publish_ms", "ingress_ms", "flink_ms", "total_ms"):
    positive_int(row, key)
  for key in ("publish_rate_msg_s", "e2e_rate_msg_s"):
    positive_float(row, key)
  return row


def validate_logs(expected):
  flink_log = ROOT / "reports/e2e/flink.log"
  submit_log = ROOT / "reports/e2e/flink-submit.log"
  require(flink_log.exists(), "missing reports/e2e/flink.log")
  require(submit_log.exists(), "missing reports/e2e/flink-submit.log")
  flink_text = flink_log.read_text(errors="replace")
  submit_text = submit_log.read_text(errors="replace")
  topk = len(re.findall(r"TopKResult\{", flink_text))
  require(topk == expected, f"Flink log TopKResult count expected {expected}, got {topk}")
  require("Job has been submitted with JobID" in submit_text, "Flink submit log missing JobID")
  require("Program execution finished" in submit_text, "Flink submit log missing completion marker")
  require("has finished" in submit_text, "Flink submit log missing finished marker")
  require(not re.search(r"\b(ERROR|Exception)\b", flink_text), "Flink log contains ERROR or Exception")


def validate_algorithm(expected_queries):
  report = ROOT / f"reports/algorithm/topk-200x2.txt"
  if not report.exists():
    reports = sorted((ROOT / "reports/algorithm").glob("topk-*.txt"))
    require(reports, "missing algorithm benchmark report")
    report = reports[-1]
  text = report.read_text(errors="replace")
  agreements = re.findall(r"topKAgreement=(true|false)", text)
  require(len(agreements) >= expected_queries, f"expected at least {expected_queries} agreement rows, got {len(agreements)}")
  require(all(value == "true" for value in agreements), f"topKAgreement failures in {report.relative_to(ROOT)}")
  prune = [float(x) for x in re.findall(r"certifiedPruneRatio=([0-9.]+)", text)]
  if not prune:
    prune = [float(x) for x in re.findall(r"pruneRatio=([0-9.]+)", text)]
  require(prune and min(prune) > 0.0, f"missing positive pruneRatio in {report.relative_to(ROOT)}")
  fast_precision = [float(x) for x in re.findall(r"fastPrecisionAtK=([0-9.]+)", text)]
  require(
      fast_precision and min(fast_precision) >= 0.8,
      f"fastPrecisionAtK below 0.8 in {report.relative_to(ROOT)}")
  comm_reduction = [float(x) for x in re.findall(r"candidateCommunicationReduction=([0-9.]+)", text)]
  require(
      comm_reduction and min(comm_reduction) >= 0.5,
      f"candidateCommunicationReduction below 0.5 in {report.relative_to(ROOT)}")
  partitioned_precision = [float(x) for x in re.findall(r"partitionedPrecisionAtK=([0-9.]+)", text)]
  require(
      partitioned_precision and min(partitioned_precision) >= 0.8,
      f"partitionedPrecisionAtK below 0.8 in {report.relative_to(ROOT)}")
  partitioned_reduction = [float(x) for x in re.findall(r"partitionedCommunicationReduction=([0-9.]+)", text)]
  require(
      partitioned_reduction and min(partitioned_reduction) >= 0.5,
      f"partitionedCommunicationReduction below 0.5 in {report.relative_to(ROOT)}")
  shuffle = [int(x) for x in re.findall(r"partitionedShuffleWriteBytes=([0-9]+)", text)]
  require(shuffle and min(shuffle) > 0, f"missing partitionedShuffleWriteBytes in {report.relative_to(ROOT)}")
  return report


def validate_monitor(expected):
  payload = run_json(["python3", "scripts/monitor.py", "--once", "--json"])
  require(payload["mqtt"]["published"] >= expected, f"monitor mqtt expected >= {expected}, got {payload['mqtt']['published']}")
  require(payload["kafka"]["messages"] == expected, f"monitor kafka expected {expected}, got {payload['kafka']['messages']}")
  require(payload["flink"]["topKResults"] == expected, f"monitor topK expected {expected}, got {payload['flink']['topKResults']}")
  require(payload["accuracy"]["ingestionCompleteness"] == 1.0, "monitor ingestion completeness is not 1.0")
  require(payload["accuracy"]["processingCompleteness"] == 1.0, "monitor processing completeness is not 1.0")
  require(payload["flink"]["overview"].get("flink-version") == "2.2.0", "Flink REST did not report version 2.2.0")
  require(payload["flink"]["jobCounts"].get("finished", 0) >= 1, "Flink REST has no finished jobs")
  require(not payload["issues"], f"monitor reported issues: {payload['issues']}")
  return payload


def validate_monitor_negative(expected):
  too_high = expected + 1
  result = subprocess.run(
      ["python3", "scripts/monitor.py", "--once", "--expect-kafka", str(too_high)],
      cwd=ROOT,
      text=True,
      capture_output=True,
      check=False)
  require(result.returncode != 0, "monitor assertion should fail when expected Kafka count is too high")
  require("FAIL kafka.messages" in result.stdout, "monitor negative assertion did not explain Kafka failure")


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--expected-messages", type=int, required=True)
  parser.add_argument("--expected-queries", type=int, default=2)
  args = parser.parse_args()

  summary = validate_summary(args.expected_messages)
  validate_logs(args.expected_messages)
  algorithm_report = validate_algorithm(args.expected_queries)
  monitor = validate_monitor(args.expected_messages)
  validate_monitor_negative(args.expected_messages)

  print(
      "validated "
      f"expected={args.expected_messages} "
      f"kafka={summary['kafka_messages']} "
      f"topK={summary['topk_results']} "
      f"e2eRate={summary['e2e_rate_msg_s']} "
      f"flinkVersion={monitor['flink']['overview'].get('flink-version')} "
      f"algorithmReport={algorithm_report.relative_to(ROOT)}")


if __name__ == "__main__":
  main()
