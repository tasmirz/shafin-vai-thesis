#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", str(ROOT / "docker-compose.e2e.yml"))
EMQX_URL = os.environ.get("EMQX_URL", "http://localhost:18084")
EMQX_USER = os.environ.get("EMQX_USER", "admin")
EMQX_PASSWORD = os.environ.get("EMQX_PASSWORD", "public")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "thesis.raw.incomplete")
TOPIC_MAPPINGS = os.environ.get("TOPIC_MAPPINGS", "")
ACTION_ID = os.environ.get("ACTION_ID", "kafka_producer:raw_incomplete_to_kafka_thesis_raw")
ACTION_IDS = [x for x in os.environ.get("ACTION_IDS", ACTION_ID).split(",") if x]
CONNECTOR_ID = os.environ.get("CONNECTOR_ID", "kafka_producer:kafka_ingress")
RULE_ID = os.environ.get("RULE_ID", "mqtt_raw_to_kafka_thesis_raw")
SPARK_LOG = ROOT / os.environ.get("SPARK_LOG", "reports/e2e/spark.log")
SPARK_REST_URL = os.environ.get("SPARK_REST_URL", "http://localhost:8080")
SUMMARY_CSV = ROOT / os.environ.get("SUMMARY_CSV", "reports/e2e/summary.csv")
ALGORITHM_REPORT_ENV = os.environ.get("ALGORITHM_REPORT")
TEST_LOG = ROOT / "reports/tests/latest.log"

TOKEN = None
TOKEN_TIME = 0.0
TEST_LOCK = threading.Lock()
TEST_PROCESS = None
TEST_STATE = {"state": "idle", "profile": None, "startedAt": None, "finishedAt": None, "returnCode": None}


def display_path(path):
  try:
    return str(path.relative_to(ROOT))
  except ValueError:
    return str(path)


def run(cmd, timeout=8):
  try:
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False)
    if result.returncode != 0:
      return {"ok": False, "stdout": result.stdout, "stderr": result.stderr.strip()}
    return {"ok": True, "stdout": result.stdout, "stderr": result.stderr.strip()}
  except Exception as exc:
    return {"ok": False, "stdout": "", "stderr": str(exc)}


def http_json(path, method="GET", body=None, auth=True):
  global TOKEN, TOKEN_TIME
  headers = {"content-type": "application/json"}
  if auth:
    if not TOKEN or time.time() - TOKEN_TIME > 300:
      payload = json.dumps({"username": EMQX_USER, "password": EMQX_PASSWORD}).encode()
      req = urllib.request.Request(f"{EMQX_URL}/api/v5/login", data=payload, method="POST", headers=headers)
      with urllib.request.urlopen(req, timeout=5) as resp:
        TOKEN = json.loads(resp.read().decode())["token"]
        TOKEN_TIME = time.time()
    headers["Authorization"] = f"Bearer {TOKEN}"
  data = json.dumps(body).encode() if body is not None else None
  api_path = path if path.startswith("/api/v5/") else f"/api/v5{path}"
  req = urllib.request.Request(f"{EMQX_URL}{api_path}", data=data, method=method, headers=headers)
  try:
    with urllib.request.urlopen(req, timeout=5) as resp:
      raw = resp.read().decode()
      return json.loads(raw) if raw else {}
  except urllib.error.HTTPError as exc:
    if exc.code == 401 and auth:
      TOKEN = None
      TOKEN_TIME = 0.0
      return http_json(path, method=method, body=body, auth=auth)
    return {"error": f"http {exc.code}", "body": exc.read().decode(errors="replace")}
  except Exception as exc:
    return {"error": str(exc)}


def kafka_offset(topics=None):
  topics = topics or kafka_topics()
  topic_pattern = ".*" if len(topics) > 1 else topics[0]
  res = run([
    "docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "kafka",
    "/opt/kafka/bin/kafka-get-offsets.sh",
    "--bootstrap-server", "kafka:9092",
    "--topic", topic_pattern,
  ])
  if not res["ok"]:
    return {"messages": 0, "error": res["stderr"] or res["stdout"]}
  total = 0
  by_topic = {}
  for line in res["stdout"].splitlines():
    parts = line.strip().split(":")
    if len(parts) == 3 and parts[0] in topics and parts[2].isdigit():
      count = int(parts[2])
      total += count
      by_topic[parts[0]] = by_topic.get(parts[0], 0) + count
  return {"messages": total, "byTopic": by_topic, "raw": res["stdout"].strip()}


def kafka_topics():
  if TOPIC_MAPPINGS:
    return [item.split("=", 1)[1] for item in TOPIC_MAPPINGS.split(",") if "=" in item]
  return [KAFKA_TOPIC]


def emqx_actions(action_ids=None):
  metrics = {}
  total = {}
  for action_id in action_ids or ACTION_IDS:
    payload = http_json(f"/actions/{action_id}/metrics")
    metrics[action_id] = payload
    values = payload.get("metrics", payload) if isinstance(payload, dict) else {}
    for key, value in values.items() if isinstance(values, dict) else []:
      if isinstance(value, (int, float)):
        total[key] = total.get(key, 0) + value
  total["actions"] = metrics
  return total


def parse_summary():
  if not SUMMARY_CSV.exists():
    return {}
  with SUMMARY_CSV.open(newline="") as f:
    rows = list(csv.DictReader(f))
  return rows[-1] if rows else {}


def parse_spark_log():
  if not SPARK_LOG.exists():
    return {"topKResults": 0, "errors": 0, "warnings": 0}
  text = SPARK_LOG.read_text(errors="replace")
  return {
    "topKResults": len(re.findall(r"TopKResult\{", text)),
    "errors": len(re.findall(r"\b(ERROR|Exception)\b", text)),
    "warnings": len(re.findall(r"\bWARN\b", text)),
    "path": display_path(SPARK_LOG),
  }


def spark_rest(path):
  req = urllib.request.Request(f"{SPARK_REST_URL}{path}")
  try:
    with urllib.request.urlopen(req, timeout=4) as resp:
      return json.loads(resp.read().decode())
  except Exception as exc:
    return {"error": str(exc)}


def spark_cluster():
  overview = spark_rest("/json/")
  if isinstance(overview, dict) and overview.get("error"):
    return {"overview": overview, "jobCounts": {"running": 0, "finished": 0, "failed": 0, "canceled": 0}}
  active_apps = overview.get("activeapps", []) if isinstance(overview, dict) else []
  completed_apps = overview.get("completedapps", []) if isinstance(overview, dict) else []
  active_drivers = overview.get("activedrivers", []) if isinstance(overview, dict) else []
  completed_drivers = overview.get("completeddrivers", []) if isinstance(overview, dict) else []
  return {
    "overview": overview,
    "jobCounts": {
      "running": len(active_apps) + len(active_drivers),
      "finished": len(completed_apps) + len(completed_drivers),
      "failed": 0,
      "canceled": 0,
    },
  }

def parse_algorithm():
  if ALGORITHM_REPORT_ENV:
    report = ROOT / ALGORITHM_REPORT_ENV
  else:
    reports = list((ROOT / "reports/algorithm").glob("topk-*.txt"))
    report = max(reports, key=lambda path: path.stat().st_mtime) if reports else ROOT / "reports/algorithm/topk-none.txt"
  if not report.exists():
    return {"agreement": None, "queries": 0}
  text = report.read_text(errors="replace")
  agreements = re.findall(r"topKAgreement=(true|false)", text)
  prune = [float(x) for x in re.findall(r"certifiedPruneRatio=([0-9.]+)", text)]
  if not prune:
    prune = [float(x) for x in re.findall(r"pruneRatio=([0-9.]+)", text)]
  fast_precision = [float(x) for x in re.findall(r"fastPrecisionAtK=([0-9.]+)", text)]
  comm_reduction = [float(x) for x in re.findall(r"candidateCommunicationReduction=([0-9.]+)", text)]
  partitioned_precision = [float(x) for x in re.findall(r"partitionedPrecisionAtK=([0-9.]+)", text)]
  partitioned_reduction = [float(x) for x in re.findall(r"partitionedCommunicationReduction=([0-9.]+)", text)]
  shuffle_write = [
      float(x)
      for x in re.findall(r"partitionedShuffleWrite(?:Proxy)?Bytes=([0-9.]+)", text)
  ]
  exact = [float(x) for x in re.findall(r"exactMs=([0-9.]+)", text)]
  pruned = [float(x) for x in re.findall(r"certifiedPrunedMs=([0-9.]+)", text)]
  if not pruned:
    pruned = [float(x) for x in re.findall(r"prunedMs=([0-9.]+)", text)]
  synopsis = re.search(
      r"imputationSynopsis rules=([0-9]+) bins=([0-9]+) avgCandidateCount=([0-9.]+) "
      r"trainingEvents=([0-9]+) holdoutEvents=([0-9]+) evaluatedValues=([0-9]+) "
      r"holdoutMAE=(n/a|[0-9.]+)",
      text)
  true_count = sum(1 for value in agreements if value == "true")
  return {
    "agreement": (true_count / len(agreements)) if agreements else None,
    "queries": len(agreements),
    "avgPruneRatio": (sum(prune) / len(prune)) if prune else None,
    "avgFastPrecisionAtK": (sum(fast_precision) / len(fast_precision)) if fast_precision else None,
    "avgCommunicationReduction": (sum(comm_reduction) / len(comm_reduction)) if comm_reduction else None,
    "avgPartitionedPrecisionAtK": (sum(partitioned_precision) / len(partitioned_precision)) if partitioned_precision else None,
    "avgPartitionedCommunicationReduction": (sum(partitioned_reduction) / len(partitioned_reduction)) if partitioned_reduction else None,
    "avgPartitionedShuffleWriteBytes": (sum(shuffle_write) / len(shuffle_write)) if shuffle_write else None,
    "avgExactMs": (sum(exact) / len(exact)) if exact else None,
    "avgPrunedMs": (sum(pruned) / len(pruned)) if pruned else None,
    "synopsisRules": int(synopsis.group(1)) if synopsis else None,
    "synopsisBins": int(synopsis.group(2)) if synopsis else None,
    "synopsisAvgCandidateCount": float(synopsis.group(3)) if synopsis else None,
    "imputationEvaluatedValues": int(synopsis.group(6)) if synopsis else None,
    "imputationHoldoutMae": (
        None if not synopsis or synopsis.group(7) == "n/a" else float(synopsis.group(7))),
  }


def metrics():
  summary = parse_summary()
  is_all = summary.get("dataset") == "all"
  topics = ["thesis.raw.intel", "thesis.raw.pump", "thesis.raw.gas"] if is_all else kafka_topics()
  action_ids = [
      "kafka_producer:raw_incomplete_to_kafka_thesis_raw_intel",
      "kafka_producer:raw_incomplete_to_kafka_thesis_raw_pump",
      "kafka_producer:raw_incomplete_to_kafka_thesis_raw_gas",
  ] if is_all else ACTION_IDS
  emqx_metrics = http_json("/metrics")
  emqx_stats = http_json("/stats")
  action_totals = emqx_actions(action_ids)
  connector = http_json(f"/connectors/{CONNECTOR_ID}")
  rule_metrics = http_json(f"/rules/{RULE_ID}/metrics")
  kafka = kafka_offset(topics)
  spark = parse_spark_log()
  spark_status = spark_cluster()
  algorithm = parse_algorithm()

  mqtt_received = 0
  mqtt_dropped = 0
  if isinstance(emqx_metrics, list):
    mqtt_received = sum(item.get("messages.publish", 0) for item in emqx_metrics if isinstance(item, dict))
    mqtt_dropped = sum(item.get("messages.dropped", 0) for item in emqx_metrics if isinstance(item, dict))

  expected = int(summary.get("expected_messages") or summary.get("expectedMessages") or 0)
  kafka_messages = int(kafka.get("messages", 0))
  topk = int(spark.get("topKResults", 0))
  processing_completeness = (1.0 if topk > 0 and (not expected or kafka_messages >= expected) else 0.0 if expected else None)
  ingestion_completeness = (kafka_messages / expected) if expected else None

  issues = []
  if connector.get("status") not in (None, "connected"):
    issues.append(f"connector status: {connector.get('status')}")
  if action_totals.get("failed", 0):
    issues.append(f"action failures: {action_totals.get('failed')}")
  if action_totals.get("dropped", 0):
    issues.append(f"action dropped: {action_totals.get('dropped')}")
  if spark.get("errors", 0):
    issues.append(f"spark log errors/exceptions: {spark.get('errors')}")
  # Spark REST/UI may be unavailable during a just-finished local run; do not fail metrics for that alone.
  if expected and kafka_messages < expected:
    issues.append(f"kafka has {kafka_messages}/{expected} expected records")

  return {
    "timestamp": int(time.time() * 1000),
    "mqtt": {"published": mqtt_received, "dropped": mqtt_dropped, "stats": emqx_stats},
    "kafka": kafka,
    "emqx": {
      "connector": {"status": connector.get("status"), "name": connector.get("name")},
      "action": action_totals,
      "rule": rule_metrics.get("metrics", {}) if isinstance(rule_metrics, dict) else {},
    },
    "spark": {**spark, **spark_status},
    "accuracy": {
      "algorithmAgreement": algorithm.get("agreement"),
      "processingCompleteness": processing_completeness,
      "ingestionCompleteness": ingestion_completeness,
      "avgPruneRatio": algorithm.get("avgPruneRatio"),
      "avgFastPrecisionAtK": algorithm.get("avgFastPrecisionAtK"),
      "avgCommunicationReduction": algorithm.get("avgCommunicationReduction"),
      "avgPartitionedPrecisionAtK": algorithm.get("avgPartitionedPrecisionAtK"),
      "avgPartitionedCommunicationReduction": algorithm.get("avgPartitionedCommunicationReduction"),
      "avgPartitionedShuffleWriteBytes": algorithm.get("avgPartitionedShuffleWriteBytes"),
      "avgExactMs": algorithm.get("avgExactMs"),
      "avgPrunedMs": algorithm.get("avgPrunedMs"),
      "synopsisRules": algorithm.get("synopsisRules"),
      "synopsisBins": algorithm.get("synopsisBins"),
      "synopsisAvgCandidateCount": algorithm.get("synopsisAvgCandidateCount"),
      "imputationEvaluatedValues": algorithm.get("imputationEvaluatedValues"),
      "imputationHoldoutMae": algorithm.get("imputationHoldoutMae"),
      "algorithmQueries": algorithm.get("queries"),
    },
    "summary": summary,
    "issues": issues,
  }


def assert_at_least(name, actual, expected, failures):
  if expected is None:
    return
  if actual is None or actual < expected:
    failures.append(f"{name} expected >= {expected}, got {actual}")


def check_metrics(payload, args):
  failures = []
  assert_at_least("mqtt.published", payload["mqtt"].get("published"), args.expect_mqtt, failures)
  assert_at_least("kafka.messages", payload["kafka"].get("messages"), args.expect_kafka, failures)
  assert_at_least("spark.topKResults", payload["spark"].get("topKResults"), args.expect_topk, failures)
  assert_at_least(
      "accuracy.ingestionCompleteness",
      payload["accuracy"].get("ingestionCompleteness"),
      args.min_ingestion_completeness,
      failures)
  assert_at_least(
      "accuracy.processingCompleteness",
      payload["accuracy"].get("processingCompleteness"),
      args.min_processing_completeness,
      failures)
  assert_at_least(
      "spark.jobCounts.finished",
      payload["spark"].get("jobCounts", {}).get("finished"),
      args.expect_finished_jobs,
      failures)
  if args.require_no_issues and payload.get("issues"):
    failures.append(f"issues present: {payload['issues']}")
  return failures


def print_metrics_summary(payload):
  print(
      "monitor "
      f"mqtt={payload['mqtt'].get('published', 0)} "
      f"kafka={payload['kafka'].get('messages', 0)} "
      f"topK={payload['spark'].get('topKResults', 0)} "
      f"ingestion={payload['accuracy'].get('ingestionCompleteness')} "
      f"processing={payload['accuracy'].get('processingCompleteness')} "
      f"sparkFinishedJobs={payload['spark'].get('jobCounts', {}).get('finished', 0)} "
      f"issues={len(payload.get('issues', []))}")


def tail_text(path, max_chars=16000):
  if not path.exists():
    return ""
  text = path.read_text(errors="replace")
  return text[-max_chars:]


def test_status():
  with TEST_LOCK:
    payload = dict(TEST_STATE)
  payload["log"] = tail_text(TEST_LOG)
  return payload


def _wait_for_test(process):
  code = process.wait()
  with TEST_LOCK:
    TEST_STATE["state"] = "passed" if code == 0 else "failed"
    TEST_STATE["returnCode"] = code
    TEST_STATE["finishedAt"] = int(time.time() * 1000)


def start_test(profile, dataset="synthetic"):
  global TEST_PROCESS
  with TEST_LOCK:
    if TEST_PROCESS is not None and TEST_PROCESS.poll() is None:
      return False, dict(TEST_STATE)
    TEST_LOG.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()

    # Configure dataset-specific TOPIC_MAPPINGS and ACTION_IDS
    if dataset == "all":
      env["TOPIC_MAPPINGS"] = "thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas"
      env["ACTION_IDS"] = "kafka_producer:raw_incomplete_to_kafka_thesis_raw_intel,kafka_producer:raw_incomplete_to_kafka_thesis_raw_pump,kafka_producer:raw_incomplete_to_kafka_thesis_raw_gas"
      topics = 3
    elif dataset in ("intel", "pump", "gas"):
      env["TOPIC_MAPPINGS"] = f"thesis/raw/{dataset}=thesis.raw.{dataset}"
      env["ACTION_IDS"] = f"kafka_producer:raw_incomplete_to_kafka_thesis_raw_{dataset}"
      topics = 1
    else:
      topics = 1
    
    if profile == "raw":
      max_events = 5
      expected = max_events * topics
      env.update({
          "DATASET": dataset,
          "MAX_EVENTS": str(max_events),
          "EXPECTED_MESSAGES": str(expected),
          "OBJECTS": "5",
          "QUERIES": "1",
          "K": "2",
      })
      command = [
          "bash", "-lc",
          "scripts/setup-venv.sh && "
          f"DATASET={dataset} MAX_EVENTS={max_events} EXPECTED_MESSAGES={expected} OBJECTS=5 QUERIES=1 DIMENSIONS=4 K=2 "
          "MISSING_RATE=0.2 RATE_PER_SECOND=200 QOS=0 BUILD_IMAGE=0 "
          "scripts/e2e-benchmark.sh && "
          f"python3 scripts/validate-e2e.py --expected-messages {expected} --expected-queries 1",
      ]
      profile = f"raw ({dataset})"
    else:
      objects = 100
      queries = 2
      max_events = objects * queries
      expected = max_events * topics if dataset == "all" else max_events
      env.update({"OBJECTS": str(objects), "QUERIES": str(queries), "EXPECTED_MESSAGES": str(expected), "DATASET": dataset})
      profile = f"default ({dataset})"
      command = ["just", "test-all"]
    log = TEST_LOG.open("w")
    TEST_PROCESS = subprocess.Popen(
        command, cwd=ROOT, env=env, text=True, stdout=log, stderr=subprocess.STDOUT)
    log.close()
    TEST_STATE.update({
        "state": "running",
        "profile": profile,
        "startedAt": int(time.time() * 1000),
        "finishedAt": None,
        "returnCode": None,
    })
    threading.Thread(target=_wait_for_test, args=(TEST_PROCESS,), daemon=True).start()
    return True, dict(TEST_STATE)


INDEX = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Thesis Stream Monitor</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; background: #f6f7f9; color: #17202a; }
    header { background: #17202a; color: #fff; padding: 18px 24px; display: flex; justify-content: space-between; align-items: center; gap: 16px; }
    h1 { margin: 0; font-size: 20px; font-weight: 700; }
    main { padding: 18px; max-width: 1440px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; }
    .card { background: #fff; border: 1px solid #d9dee7; border-radius: 8px; padding: 14px; box-shadow: 0 1px 2px rgba(16,24,40,.04); }
    .label { color: #5f6b7a; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 28px; font-weight: 750; margin-top: 8px; overflow-wrap: anywhere; }
    .sub { color: #667085; font-size: 13px; margin-top: 6px; }
    .wide { grid-column: span 2; }
    canvas { width: 100%; height: 220px; display: block; }
    .ok { color: #087443; }
    .warn { color: #b54708; }
    .bad { color: #b42318; }
    .issues { margin: 0; padding-left: 18px; }
    button { height: 36px; border: 1px solid #cbd5e1; border-radius: 6px; background: #fff; padding: 0 12px; font-weight: 600; cursor: pointer; }
    button.primary { background: #17202a; color: #fff; border-color: #17202a; }
    button:disabled { opacity: .55; cursor: wait; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }
    pre { background: #101828; color: #e4e7ec; height: 280px; overflow: auto; padding: 12px; border-radius: 6px; font-size: 12px; white-space: pre-wrap; }
    code { background: #eef1f5; padding: 2px 5px; border-radius: 4px; }
    @media (max-width: 900px) { .grid { grid-template-columns: repeat(2, minmax(160px, 1fr)); } .wide { grid-column: span 2; } }
    @media (max-width: 520px) { .grid { grid-template-columns: 1fr; } .wide { grid-column: span 1; } header { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <header>
    <h1>Thesis Stream Monitor</h1>
    <div id="status">Connecting...</div>
  </header>
  <main>
    <section class="grid">
      <div class="card"><div class="label">MQTT Ingress</div><div class="value" id="mqtt">0</div><div class="sub">published messages</div></div>
      <div class="card"><div class="label">Kafka Ingress</div><div class="value" id="kafka">0</div><div class="sub">topic offset</div></div>
      <div class="card"><div class="label">Spark Outgress</div><div class="value" id="spark">0</div><div class="sub">TopKResult rows</div></div>
      <div class="card"><div class="label">Bridge Status</div><div class="value" id="bridge">unknown</div><div class="sub" id="bridgeSub">Kafka action</div></div>
      <div class="card"><div class="label">Ingestion Completeness</div><div class="value" id="ingAcc">n/a</div><div class="sub">Kafka / expected</div></div>
      <div class="card"><div class="label">Processing Completeness</div><div class="value" id="procAcc">n/a</div><div class="sub">Spark / Kafka</div></div>
      <div class="card"><div class="label">Top-k Agreement</div><div class="value" id="agreement">n/a</div><div class="sub">algorithm benchmark</div></div>
      <div class="card"><div class="label">Prune Ratio</div><div class="value" id="prune">n/a</div><div class="sub">algorithm benchmark</div></div>
      <div class="card"><div class="label">Partitioned Precision</div><div class="value" id="partitionedPrecision">n/a</div><div class="sub">4-partition model</div></div>
      <div class="card"><div class="label">Shuffle Proxy</div><div class="value" id="shuffleWrite">n/a</div><div class="sub">calculated candidate bytes</div></div>
      <div class="card"><div class="label">Imputation MAE</div><div class="value" id="imputationMae">n/a</div><div class="sub">masked holdout values</div></div>
      <div class="card"><div class="label">DD Synopsis Rules</div><div class="value" id="synopsisRules">n/a</div><div class="sub">cost-selected histogram rules</div></div>
      <div class="card wide"><div class="label">Topic Traffic</div><pre id="topics">Waiting for traffic...</pre></div>
      <div class="card wide"><div class="label">Throughput</div><canvas id="chart"></canvas></div>
      <div class="card wide"><div class="label">Issues</div><ul class="issues" id="issues"><li>Loading...</li></ul><div class="sub">Spark log: <code>reports/e2e/spark.log</code></div></div>
      <div class="card wide"><div class="label">CLI Test Runner</div>
        <div class="actions">
          <select id="datasetSelector" style="height:36px; border:1px solid #cbd5e1; border-radius:6px; background:#fff; padding:0 8px;">
            <option value="synthetic">Synthetic</option>
            <option value="intel">Intel Lab (Real)</option>
            <option value="pump">Water Pump (Real)</option>
            <option value="gas">Gas Sensors (Real)</option>
            <option value="all">Real (Multitopic: Intel/Pump/Gas)</option>
          </select>
          <button class="primary" id="runDefault">Run Full CLI Tests</button>
          <button id="runRaw">Run Raw Topics E2E</button>
          <span class="sub" id="testState">idle</span>
        </div>
        <pre id="testLog">No test run started.</pre>
      </div>
    </section>
  </main>
<script>
const history = [];
const $ = id => document.getElementById(id);
const pct = x => x == null ? "n/a" : `${(x * 100).toFixed(1)}%`;
function draw() {
  const canvas = $("chart"), ctx = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(320, rect.width * devicePixelRatio);
  canvas.height = 220 * devicePixelRatio;
  ctx.scale(devicePixelRatio, devicePixelRatio);
  ctx.clearRect(0, 0, rect.width, 220);
  const pad = 50, w = rect.width - pad * 2, h = 160, top = 20;

  // Calculate max from recent data for better scaling
  const recentData = history.slice(-20).flatMap(p => [p.mqtt, p.kafka, p.spark]);
  const max = Math.max(10, ...recentData); // min 10 for scale

  // Draw grid
  ctx.strokeStyle = "#e5e7eb"; ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = top + (h / 4) * i;
    ctx.beginPath(); ctx.moveTo(pad - 5, y); ctx.lineTo(pad + w, y); ctx.stroke();
  }

  // Draw axis
  ctx.strokeStyle = "#d0d5dd"; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(pad, top + h); ctx.lineTo(pad + w, top + h); ctx.stroke();

  // Draw y-axis labels
  ctx.fillStyle = "#666"; ctx.font = "11px monospace"; ctx.textAlign = "right";
  for (let i = 0; i <= 4; i++) {
    const y = top + (h / 4) * i;
    const val = Math.round((max * (4 - i) / 4));
    ctx.fillText(val, pad - 10, y + 4);
  }

  // Draw data lines
  [["mqtt","#2563eb"],["kafka","#0f766e"],["spark","#b54708"]].forEach(([key,color]) => {
    ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.beginPath();
    history.forEach((p, i) => {
      const x = pad + (history.length === 1 ? 0 : i * w / (history.length - 1));
      const y = top + h - (p[key] / max) * h;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });

  // Draw legend
  ctx.fillStyle = "#344054"; ctx.font = "12px sans-serif"; ctx.textAlign = "left";
  ctx.fillText("MQTT", pad, 204); ctx.fillStyle = "#2563eb"; ctx.fillRect(pad + 38, 196, 18, 3);
  ctx.fillStyle = "#344054"; ctx.fillText("Kafka", pad + 70, 204); ctx.fillStyle = "#0f766e"; ctx.fillRect(pad + 112, 196, 18, 3);
  ctx.fillStyle = "#344054"; ctx.fillText("Spark", pad + 144, 204); ctx.fillStyle = "#b54708"; ctx.fillRect(pad + 184, 196, 18, 3);
}
async function tick() {
  try {
    const r = await fetch("/api/metrics", {cache: "no-store"});
    const m = await r.json();
    $("status").textContent = new Date(m.timestamp).toLocaleTimeString();
    $("mqtt").textContent = m.mqtt.published ?? 0;
    $("kafka").textContent = m.kafka.messages ?? 0;
    $("spark").textContent = m.spark.topKResults ?? 0;
    $("bridge").textContent = m.emqx.connector.status || "unknown";
    $("bridge").className = `value ${m.emqx.connector.status === "connected" ? "ok" : "bad"}`;
    $("bridgeSub").textContent = `success ${m.emqx.action.success ?? 0}, failed ${m.emqx.action.failed ?? 0}, dropped ${m.emqx.action.dropped ?? 0}`;
    $("ingAcc").textContent = pct(m.accuracy.ingestionCompleteness);
    $("procAcc").textContent = pct(m.accuracy.processingCompleteness);
    $("agreement").textContent = pct(m.accuracy.algorithmAgreement);
    $("prune").textContent = pct(m.accuracy.avgPruneRatio);
    $("partitionedPrecision").textContent = pct(m.accuracy.avgPartitionedPrecisionAtK);
    $("shuffleWrite").textContent = m.accuracy.avgPartitionedShuffleWriteBytes == null ? "n/a" : Math.round(m.accuracy.avgPartitionedShuffleWriteBytes).toLocaleString();
    $("imputationMae").textContent = m.accuracy.imputationHoldoutMae == null ? "n/a" : m.accuracy.imputationHoldoutMae.toFixed(4);
    $("synopsisRules").textContent = m.accuracy.synopsisRules == null ? "n/a" : m.accuracy.synopsisRules.toLocaleString();
    $("topics").textContent = Object.entries(m.kafka.byTopic || {}).map(([topic, count]) => `${topic.padEnd(26)} ${count}`).join("\n") || "No topic offsets available.";
    const issues = $("issues"); issues.innerHTML = "";
    (m.issues.length ? m.issues : ["No current issues detected"]).forEach(issue => {
      const li = document.createElement("li"); li.textContent = issue; issues.appendChild(li);
    });
    const curRaw = {mqtt: m.mqtt.published || 0, kafka: m.kafka.messages || 0, spark: m.spark.topKResults || 0};
    const prevRaw = history.length > 0 ? history[history.length - 1].raw : curRaw;
    const dMqtt = Math.max(0, curRaw.mqtt - prevRaw.mqtt);
    const dKafka = Math.max(0, curRaw.kafka - prevRaw.kafka);
    const dSpark = Math.max(0, curRaw.spark - prevRaw.spark);
    history.push({raw: curRaw, mqtt: dMqtt, kafka: dKafka, spark: dSpark});
    while (history.length > 80) history.shift();
    draw();
  } catch (e) {
    $("status").textContent = `monitor error: ${e.message}`;
  }
}
async function pollTests() {
  const result = await fetch("/api/tests/status", {cache: "no-store"});
  const test = await result.json();
  $("testState").textContent = test.state + (test.profile ? ` (${test.profile})` : "");
  $("testState").className = `sub ${test.state === "passed" ? "ok" : test.state === "failed" ? "bad" : ""}`;
  $("testLog").textContent = test.log || "No test output available.";
  $("testLog").scrollTop = $("testLog").scrollHeight;
  const running = test.state === "running";
  $("runDefault").disabled = running;
  $("runRaw").disabled = running;
}
async function runTests(profile) {
  const dataset = $("datasetSelector").value;
  await fetch("/api/tests/run", {method: "POST", headers: {"content-type": "application/json"}, body: JSON.stringify({profile, dataset})});
  pollTests();
}
$("runDefault").onclick = () => runTests("default");
$("runRaw").onclick = () => runTests("raw");
setInterval(tick, 2000); tick();
setInterval(pollTests, 1500); pollTests();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path == "/" or self.path.startswith("/?"):
      self.send_response(200)
      self.send_header("content-type", "text/html; charset=utf-8")
      self.end_headers()
      self.wfile.write(INDEX.encode())
      return
    if self.path == "/api/metrics":
      payload = json.dumps(metrics(), indent=2).encode()
      self.send_response(200)
      self.send_header("content-type", "application/json")
      self.send_header("cache-control", "no-store")
      self.end_headers()
      self.wfile.write(payload)
      return
    if self.path == "/api/tests/status":
      payload = json.dumps(test_status(), indent=2).encode()
      self.send_response(200)
      self.send_header("content-type", "application/json")
      self.send_header("cache-control", "no-store")
      self.end_headers()
      self.wfile.write(payload)
      return
    self.send_response(404)
    self.end_headers()

  def do_POST(self):
    if self.path == "/api/tests/run":
      length = int(self.headers.get("content-length", "0"))
      request = json.loads(self.rfile.read(length) or b"{}")
      started, state = start_test(request.get("profile", "default"), request.get("dataset", "synthetic"))
      payload = json.dumps(state).encode()
      self.send_response(202 if started else 409)
      self.send_header("content-type", "application/json")
      self.end_headers()
      self.wfile.write(payload)
      return
    self.send_response(404)
    self.end_headers()

  def log_message(self, fmt, *args):
    return


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--port", type=int, default=8088)
  parser.add_argument("--once", action="store_true", help="print one metrics sample and exit")
  parser.add_argument("--json", action="store_true", help="print full JSON in --once mode")
  parser.add_argument("--expect-mqtt", type=int)
  parser.add_argument("--expect-kafka", type=int)
  parser.add_argument("--expect-topk", type=int)
  parser.add_argument("--expect-finished-jobs", type=int)
  parser.add_argument("--min-ingestion-completeness", type=float)
  parser.add_argument("--min-processing-completeness", type=float)
  parser.add_argument("--require-no-issues", action="store_true")
  args = parser.parse_args()
  if args.once:
    payload = metrics()
    if args.json:
      print(json.dumps(payload, indent=2))
    else:
      print_metrics_summary(payload)
    failures = check_metrics(payload, args)
    if failures:
      for failure in failures:
        print(f"FAIL {failure}")
      raise SystemExit(1)
    return
  server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
  print(f"monitor=http://localhost:{args.port}")
  server.serve_forever()


if __name__ == "__main__":
  main()
