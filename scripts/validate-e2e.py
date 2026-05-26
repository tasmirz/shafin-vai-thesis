#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def fail(message):
  raise SystemExit(f"FAIL {message}")


def require(condition, message):
  if not condition:
    fail(message)


def display_path(path):
  try:
    return path.relative_to(ROOT)
  except ValueError:
    return path


def run_json(cmd, env=None):
  result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False, env=env)
  if result.returncode != 0:
    fail(f"{' '.join(cmd)} exited {result.returncode}: {result.stderr or result.stdout}")
  try:
    return json.loads(result.stdout)
  except json.JSONDecodeError as exc:
    fail(f"{' '.join(cmd)} did not return JSON: {exc}")


def last_csv_row(path):
  require(path.exists(), f"missing {display_path(path)}")
  with path.open(newline="") as f:
    rows = list(csv.DictReader(f))
  require(rows, f"{display_path(path)} has no rows")
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


def validate_summary(report_dir, expected_messages, expected_queries):
  row = last_csv_row(report_dir / "summary.csv")
  require(int(row["expected_messages"]) == expected_messages,
          f"expected_messages expected {expected_messages}, got {row['expected_messages']}")
  require(int(row["kafka_messages"]) >= expected_messages,
          f"kafka_messages expected >= {expected_messages}, got {row['kafka_messages']}")
  require(int(row["topk_results"]) >= expected_queries,
          f"topk_results expected >= {expected_queries}, got {row['topk_results']}")
  for key in ("publish_ms", "ingress_ms", "spark_ms", "total_ms"):
    positive_int(row, key)
  for key in ("publish_rate_msg_s", "e2e_rate_msg_s"):
    positive_float(row, key)
  return row


def validate_logs(report_dir, expected_queries):
  spark_log = report_dir / "spark.log"
  submit_log = report_dir / "spark-submit.log"
  require(spark_log.exists(), f"missing {spark_log}")
  require(submit_log.exists(), f"missing {submit_log}")
  spark_text = spark_log.read_text(errors="replace")
  submit_text = submit_log.read_text(errors="replace")
  topk = len(re.findall(r"TopKResult\{", spark_text))
  require(topk >= expected_queries, f"Spark log TopKResult count expected >= {expected_queries}, got {topk}")
  require("engine=apache-spark" in submit_text, "Spark submit log missing engine marker")
  require("sparkKafkaRead" in submit_text, "Spark submit log missing Kafka read marker")
  require("reader=structured-streaming trigger=available-now" in submit_text,
          "Spark submit log missing Structured Streaming bounded-reader marker")
  validated = re.findall(r"validationPerformed=true exactAgreement=(true|false)", spark_text)
  require(len(validated) >= expected_queries, "Spark log missing exact-validation results")
  require(all(value == "true" for value in validated), "Spark exact-validation failure")
  require(not re.search(r"\b(ERROR|Exception)\b", spark_text), "Spark log contains ERROR or Exception")


def validate_algorithm(report_dir, expected_queries, dataset):
  attached = report_dir / "algorithm-validation.log"
  reports = ([attached] if attached.exists() else []) + list((ROOT / "reports/algorithm").glob("topk-*.txt"))
  require(reports, "missing algorithm benchmark report")
  matching = [
      path for path in reports
      if f"dataset provider={dataset} " in path.read_text(errors="replace")
  ]
  report = attached if attached in matching else max(matching or reports, key=lambda path: path.stat().st_mtime)
  text = report.read_text(errors="replace")
  agreements = re.findall(r"topKAgreement=(true|false)", text)
  require(len(agreements) >= expected_queries, f"expected at least {expected_queries} agreement rows, got {len(agreements)}")
  require(all(value == "true" for value in agreements), f"topKAgreement failures in {display_path(report)}")
  prune = [float(x) for x in re.findall(r"certifiedPruneRatio=([0-9.]+)", text)]
  if not prune:
    prune = [float(x) for x in re.findall(r"pruneRatio=([0-9.]+)", text)]
  require(prune, f"missing pruneRatio in {display_path(report)}")
  synopsis = re.search(
      r"imputationSynopsis rules=([0-9]+) bins=([0-9]+).*holdoutMAE=(n/a|[0-9.]+)",
      text)
  require(synopsis, f"missing imputation synopsis metrics in {display_path(report)}")
  require(int(synopsis.group(1)) > 0, f"synopsis has no trained rules in {display_path(report)}")
  return report


def monitor_environment(report_dir):
  env = os.environ.copy()
  env["SPARK_LOG"] = str(report_dir / "spark.log")
  env["SUMMARY_CSV"] = str(report_dir / "summary.csv")
  env["ALGORITHM_REPORT"] = str(report_dir / "algorithm-validation.log")
  return env


def validate_monitor(report_dir, expected_messages, expected_queries):
  payload = run_json(["python3", "scripts/monitor.py", "--once", "--json"], monitor_environment(report_dir))
  require(payload["kafka"]["messages"] >= expected_messages,
          f"monitor kafka expected >= {expected_messages}, got {payload['kafka']['messages']}")
  require(payload["spark"]["topKResults"] >= expected_queries,
          f"monitor topK expected >= {expected_queries}, got {payload['spark']['topKResults']}")
  require(payload["accuracy"]["ingestionCompleteness"] >= 1.0, "monitor ingestion completeness is below 1.0")
  require(payload["accuracy"]["synopsisRules"] is None or payload["accuracy"]["synopsisRules"] > 0,
          "monitor reports zero DD synopsis rules")
  require(not payload["issues"], f"monitor reported issues: {payload['issues']}")
  return payload


def validate_monitor_negative(report_dir, expected_messages):
  too_high = expected_messages + 1_000_000
  result = subprocess.run(
      ["python3", "scripts/monitor.py", "--once", "--expect-kafka", str(too_high)],
      cwd=ROOT,
      text=True,
      capture_output=True,
      env=monitor_environment(report_dir),
      check=False)
  require(result.returncode != 0, "monitor assertion should fail when expected Kafka count is too high")
  require("FAIL kafka.messages" in result.stdout, "monitor negative assertion did not explain Kafka failure")


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--expected-messages", type=int, required=True)
  parser.add_argument("--expected-queries", type=int, default=2)
  parser.add_argument("--report-dir", default="reports/e2e")
  args = parser.parse_args()
  report_dir = Path(args.report_dir)
  if not report_dir.is_absolute():
    report_dir = ROOT / report_dir

  summary = validate_summary(report_dir, args.expected_messages, args.expected_queries)
  validate_logs(report_dir, args.expected_queries)
  algorithm_report = validate_algorithm(report_dir, args.expected_queries, summary.get("dataset", "synthetic"))
  monitor = validate_monitor(report_dir, args.expected_messages, args.expected_queries)
  validate_monitor_negative(report_dir, args.expected_messages)

  print(
      "validated "
      f"expectedMessages={args.expected_messages} "
      f"kafka={summary['kafka_messages']} "
      f"topK={summary['topk_results']} "
      f"e2eRate={summary['e2e_rate_msg_s']} "
      f"sparkFinishedJobs={monitor['spark'].get('jobCounts', {}).get('finished', 0)} "
      f"algorithmReport={display_path(algorithm_report)}")


if __name__ == "__main__":
  main()
