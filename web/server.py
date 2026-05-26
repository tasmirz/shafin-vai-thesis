#!/usr/bin/env python3
"""Local PTD-BenchLab API and static website server.

The server deliberately exposes only repository-contained artifacts and the
validated CSV/stream benchmark launch scripts. It is intended for local
research use, not as an internet-facing multi-user service.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import mimetypes
import os
import re
import subprocess
import threading
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = ROOT / "web" / "static"
RUN_ROOT = ROOT / "reports" / "runs"
JOB_ROOT = ROOT / "reports" / "web-jobs"
CSV_FIXTURE = ROOT / "tests" / "fixtures" / "csv" / "smartphone-small.csv"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ALGORITHMS = ("baseline", "dscp-only", "aes-only", "aes-dscp")

PAPER_TARGETS = [
    {
        "dataset": "Synthetic smartphone",
        "baselineMs": 66520,
        "proposedMs": 43757,
        "reductionPct": 34.2,
        "status": "Algorithm treatments implemented; requires paper-shaped smartphone dataset",
    },
    {
        "dataset": "Bangladesh road / OSM",
        "baselineMs": 56274,
        "proposedMs": 42366,
        "reductionPct": 24.7,
        "status": "Requires spatial dataset generator",
    },
]

FAIR_FIELDS = (
    ("dataset.sha256", "Dataset checksum"),
    ("parameters.dataset", "Dataset type"),
    ("parameters.objects", "Objects"),
    ("parameters.queries", "Queries"),
    ("parameters.dimensions", "Dimensions"),
    ("parameters.missingRate", "Missing rate"),
    ("parameters.k", "k"),
    ("parameters.partitions", "Partitions"),
    ("parameters.seed", "Seed"),
)


def utc_now() -> str:
  return datetime.now(timezone.utc).isoformat()


def dotted(document: dict, key: str):
  current = document
  for part in key.split("."):
    current = current.get(part) if isinstance(current, dict) else None
  return current


def require_run_id(run_id: str) -> str:
  if not RUN_ID_PATTERN.fullmatch(run_id):
    raise ValueError("Run ID may contain only letters, digits, '.', '_' and '-'.")
  return run_id


def json_file(path: Path) -> dict:
  return json.loads(path.read_text())


def run_dir(run_id: str) -> Path:
  require_run_id(run_id)
  path = RUN_ROOT / run_id
  if not path.is_dir():
    raise FileNotFoundError(f"Unknown saved run: {run_id}")
  return path


def load_run(run_id: str, include_logs: bool = False) -> dict:
  path = run_dir(run_id)
  manifest = json_file(path / "manifest.json")
  metrics = json_file(path / "metrics.json")
  spark = metrics.get("spark", {})
  validation = metrics.get("validation", {})
  item = {
      "id": manifest["runId"],
      "createdUtc": manifest.get("createdUtc"),
      "mode": manifest.get("mode"),
      "manifest": manifest,
      "metrics": metrics,
      "summary": {
          "source": spark.get("source"),
          "dataset": spark.get("dataset"),
          "k": spark.get("k"),
          "partitions": spark.get("partitions"),
          "algorithm": spark.get("algorithm"),
          "algorithmElapsedMs": spark.get("algorithmElapsedMs"),
          "validationMs": spark.get("validationMs"),
          "rawEvents": spark.get("rawEvents"),
          "queries": len(spark.get("queries", [])),
          "avgPruneRatio": spark.get("avgPruneRatio"),
          "avgAER": spark.get("avgAER"),
          "totalEmittedRecords": spark.get("totalEmittedRecords"),
          "falsePruneCount": spark.get("falsePruneCount"),
          "exactAgreement": validation.get("exactTopKAgreement"),
          "streamingKafka": spark.get("structuredStreamingKafka"),
      },
      "artifacts": manifest.get("artifacts", []),
  }
  if include_logs:
    item["logs"] = {
        artifact: (path / artifact).read_text(errors="replace")[-12000:]
        for artifact in ("spark.log", "algorithm.log")
        if (path / artifact).exists()
    }
  return item


def list_runs() -> list[dict]:
  if not RUN_ROOT.exists():
    return []
  items = []
  for candidate in RUN_ROOT.iterdir():
    if candidate.is_dir() and (candidate / "manifest.json").exists():
      try:
        items.append(load_run(candidate.name))
      except (OSError, KeyError, json.JSONDecodeError):
        continue
  return sorted(items, key=lambda item: item.get("createdUtc") or "", reverse=True)


def comparison(ids: list[str]) -> dict:
  if len(ids) < 2:
    raise ValueError("Select at least two runs to compare.")
  runs = [load_run(run_id) for run_id in ids]
  differences = []
  for field, label in FAIR_FIELDS:
    values = [dotted(item["manifest"], field) for item in runs]
    if len({json.dumps(value, sort_keys=True) for value in values}) > 1:
      differences.append({"field": field, "label": label, "values": values})
  baseline = runs[0]["summary"].get("algorithmElapsedMs")
  rows = []
  for item in runs:
    elapsed = item["summary"].get("algorithmElapsedMs")
    reduction = None
    if baseline and elapsed is not None:
      reduction = round((baseline - elapsed) / baseline * 100, 2)
    rows.append({**item["summary"], "id": item["id"], "reductionVsFirstPct": reduction})
  return {
      "runs": rows,
      "fair": not differences,
      "differences": differences,
      "notice": (
          "Recorded fairness-critical configuration matches."
          if not differences else
          "Configuration differs. Treat speedup as descriptive, not an algorithmic comparison."
      ),
  }


def within_root(path: Path) -> Path:
  resolved = path.expanduser().resolve()
  if resolved != ROOT and ROOT not in resolved.parents:
    raise ValueError("CSV inspection is restricted to files within this project.")
  return resolved


def project_file(raw_path: str | None, default: Path) -> Path:
  path = Path(raw_path) if raw_path else default
  if not path.is_absolute():
    path = ROOT / path
  return within_root(path)


def csv_profile(raw_path: str | None = None) -> dict:
  path = project_file(raw_path, CSV_FIXTURE)
  if path.suffix.lower() != ".csv" or not path.is_file():
    raise FileNotFoundError("CSV file does not exist inside the project.")
  with path.open(newline="") as source:
    reader = csv.DictReader(source)
    rows = list(reader)
    fields = reader.fieldnames or []
  dimension_fields = [field for field in fields if re.fullmatch(r"a\d+", field)]
  objects = {row.get("objectId", "") for row in rows}
  queries = {row.get("queryId", "") for row in rows}
  missing = sum(
      1 for row in rows for field in dimension_fields
      if row.get(field, "").strip().lower() in {"", "null", "nan", "?"})
  query_counts = {}
  for row in rows:
    query_id = row.get("queryId", "")
    query_counts[query_id] = query_counts.get(query_id, 0) + 1
  return {
      "path": str(path.relative_to(ROOT)),
      "columns": fields,
      "dimensions": dimension_fields,
      "records": len(rows),
      "objects": len(objects),
      "queries": len(queries),
      "missingAttributeValues": missing,
      "queryCounts": query_counts,
      "preview": rows[:50],
      "qualityChecks": [
          {"name": "Readable CSV schema", "passed": bool(fields), "value": f"{len(fields)} columns"},
          {"name": "At least one object", "passed": bool(objects), "value": f"{len(objects)} objects"},
          {"name": "At least one query", "passed": bool(queries), "value": f"{len(queries)} queries"},
          {
              "name": "Missing attributes recorded for imputation",
              "passed": True,
              "value": f"{missing} missing attribute values",
          },
      ],
      "note": (
          "The current normalized stream CSV contains incomplete attributes rather than "
          "paper-style appearance probabilities; probability auditing becomes active once "
          "probabilistic uncertain-object datasets are added."
      ),
  }


def dashboard() -> dict:
  runs = list_runs()
  exact_runs = [item for item in runs if item["summary"]["exactAgreement"] is True]
  fastest = min(
      exact_runs,
      key=lambda item: item["summary"]["algorithmElapsedMs"]
      if item["summary"]["algorithmElapsedMs"] is not None else float("inf"),
      default=None)
  warnings = []
  if not runs:
    warnings.append("No saved runs are present. Launch a CSV or stream validation run.")
  if not any(item["mode"] == "stream" for item in runs):
    warnings.append("No saved MQTT/Kafka/Spark stream run is available.")
  return {
      "project": {
          "name": "PTD-BenchLab",
          "subtitle": (
              "A reproducible benchmarking, validation, simulation, and raw-inspection "
              "platform for uncertain-database dominance-query algorithms."
          ),
          "runtime": "Apache Spark Structured Streaming and batch CSV validation",
          "pipeline": ["MQTT", "EMQX Kafka sink", "Kafka", "Spark", "Exact validation", "Saved run"],
      },
      "counts": {
          "savedRuns": len(runs),
          "exactRuns": len(exact_runs),
          "streamRuns": sum(item["mode"] == "stream" for item in runs),
          "csvRuns": sum(item["mode"] == "csv" for item in runs),
      },
      "fastestExactRun": fastest,
      "recentRuns": runs[:8],
      "paperTargets": PAPER_TARGETS,
      "warnings": warnings,
      "capabilities": {
          "implemented": [
              "CSV-to-Spark deterministic benchmark",
              "MQTT -> Kafka -> Spark bounded stream benchmark",
              "Brute-force exact agreement validation",
              "Immutable run archive and comparison hygiene",
              "Artifact bundle export",
              "Selectable baseline, DSCP-only, AES-only and AES + DSCP treatments",
              "Executed emission counts, AER and DSCP false-prune audit",
          ],
          "pending": [
              "Road/OSM uncertain-object generator",
              "Per-object DDR/MBR and AES trace records",
              "Actual Spark shuffle-byte instrumentation",
          ],
      },
  }


def bundle(run_id: str) -> bytes:
  path = run_dir(run_id)
  output = io.BytesIO()
  with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
    for artifact in sorted(path.iterdir()):
      if artifact.is_file():
        archive.write(artifact, f"{run_id}/{artifact.name}")
  return output.getvalue()


@dataclass
class Job:
  job_id: str
  run_id: str
  mode: str
  status: str
  command: list[str]
  created_utc: str
  log_path: Path
  process: subprocess.Popen | None = None
  finished_utc: str | None = None
  return_code: int | None = None

  def document(self) -> dict:
    output = self.log_path.read_text(errors="replace")[-12000:] if self.log_path.exists() else ""
    return {
        "jobId": self.job_id,
        "runId": self.run_id,
        "mode": self.mode,
        "status": self.status,
        "createdUtc": self.created_utc,
        "finishedUtc": self.finished_utc,
        "returnCode": self.return_code,
        "log": output,
    }


JOBS: dict[str, Job] = {}
JOB_LOCK = threading.Lock()


def launch_job(payload: dict) -> dict:
  mode = payload.get("mode")
  if mode not in {"csv", "stream"}:
    raise ValueError("Mode must be csv or stream.")
  run_id = require_run_id(str(payload.get("runId", "")))
  if (RUN_ROOT / run_id).exists():
    raise ValueError("A saved run already exists with this ID.")
  with JOB_LOCK:
    if any(job.run_id == run_id and job.status == "running" for job in JOBS.values()):
      raise ValueError("A running job already uses this run ID.")
  JOB_ROOT.mkdir(parents=True, exist_ok=True)
  job_id = uuid.uuid4().hex[:12]
  env = os.environ.copy()
  env["RUN_ID"] = run_id
  env["BUILD_IMAGE"] = "1" if payload.get("buildImage", False) else "0"
  env["ALGORITHM"] = algorithm_id(payload.get("algorithm", "aes-dscp"))
  if mode == "csv":
    csv_path = project_file(payload.get("csvPath"), CSV_FIXTURE)
    if csv_path.suffix.lower() != ".csv" or not csv_path.is_file():
      raise ValueError("Select an existing CSV file within this project.")
    env.update({
        "CSV_PATH": str(csv_path),
        "K": positive_int(payload.get("k", 2), "k"),
        "PARTITIONS": positive_int(payload.get("partitions", 2), "partitions"),
        "SEED": integer(payload.get("seed", 42), "seed"),
        "SYNOPSIS_BINS": positive_int(payload.get("synopsisBins", 4), "synopsisBins"),
    })
    command = [str(ROOT / "scripts" / "research" / "run_csv_benchmark.sh")]
  else:
    objects = positive_int(payload.get("objects", 12), "objects")
    queries = positive_int(payload.get("queries", 2), "queries")
    env.update({
        "OBJECTS": objects,
        "QUERIES": queries,
        "DIMENSIONS": positive_int(payload.get("dimensions", 2), "dimensions"),
        "K": positive_int(payload.get("k", 2), "k"),
        "PARTITIONS": positive_int(payload.get("partitions", 2), "partitions"),
        "RATE_PER_SECOND": positive_int(payload.get("ratePerSecond", 100), "ratePerSecond"),
        "EXPECTED_MESSAGES": str(int(objects) * int(queries)),
        "E2E_REPORT_DIR": str(JOB_ROOT / job_id / "e2e"),
    })
    command = [str(ROOT / "tests" / "e2e" / "test_mqtt_kafka_spark.sh")]
  log_path = JOB_ROOT / f"{job_id}.log"
  job = Job(job_id, run_id, mode, "running", command, utc_now(), log_path)
  log = log_path.open("w")
  process = subprocess.Popen(
      command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, text=True)
  job.process = process
  with JOB_LOCK:
    JOBS[job_id] = job

  def monitor():
    code = process.wait()
    log.close()
    with JOB_LOCK:
      job.return_code = code
      job.status = "completed" if code == 0 else "failed"
      job.finished_utc = utc_now()

  threading.Thread(target=monitor, daemon=True).start()
  return job.document()


def positive_int(value, name: str) -> str:
  try:
    parsed = int(value)
  except (ValueError, TypeError) as exc:
    raise ValueError(f"{name} must be an integer.") from exc
  if parsed < 1:
    raise ValueError(f"{name} must be positive.")
  return str(parsed)


def integer(value, name: str) -> str:
  try:
    return str(int(value))
  except (ValueError, TypeError) as exc:
    raise ValueError(f"{name} must be an integer.") from exc


def algorithm_id(value) -> str:
  identifier = str(value).lower()
  if identifier not in ALGORITHMS:
    raise ValueError(f"algorithm must be one of: {', '.join(ALGORITHMS)}.")
  return identifier


def list_jobs() -> list[dict]:
  with JOB_LOCK:
    return [job.document() for job in sorted(JOBS.values(), key=lambda item: item.created_utc, reverse=True)]


class Handler(BaseHTTPRequestHandler):
  server_version = "PTD-BenchLab/0.1"

  def log_message(self, format_string, *args):
    print(f"[web] {self.address_string()} {format_string % args}")

  def send_json(self, document, status=HTTPStatus.OK):
    body = json.dumps(document).encode()
    self.send_response(status)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def send_error_json(self, message, status=HTTPStatus.BAD_REQUEST):
    self.send_json({"error": str(message)}, status)

  def do_GET(self):
    request = urlparse(self.path)
    try:
      if request.path == "/api/dashboard":
        self.send_json(dashboard())
      elif request.path == "/api/runs":
        self.send_json({"runs": list_runs()})
      elif request.path.startswith("/api/runs/") and request.path.endswith("/bundle"):
        run_id = unquote(request.path.split("/")[3])
        body = bundle(run_id)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", f'attachment; filename="{run_id}-bundle.zip"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
      elif request.path.startswith("/api/runs/"):
        self.send_json(load_run(unquote(request.path.split("/")[3]), include_logs=True))
      elif request.path == "/api/compare":
        ids = parse_qs(request.query).get("ids", [""])[0].split(",")
        self.send_json(comparison([run_id for run_id in ids if run_id]))
      elif request.path == "/api/datasets/csv":
        raw_path = parse_qs(request.query).get("path", [None])[0]
        self.send_json(csv_profile(raw_path))
      elif request.path == "/api/jobs":
        self.send_json({"jobs": list_jobs()})
      elif request.path.startswith("/api/jobs/"):
        job_id = unquote(request.path.split("/")[3])
        with JOB_LOCK:
          job = JOBS.get(job_id)
        if not job:
          raise FileNotFoundError("Unknown web-launched job.")
        self.send_json(job.document())
      elif request.path.startswith("/api/"):
        raise FileNotFoundError("Unknown API endpoint.")
      else:
        self.serve_static(request.path)
    except FileNotFoundError as exc:
      self.send_error_json(exc, HTTPStatus.NOT_FOUND)
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
      self.send_error_json(exc)

  def do_POST(self):
    try:
      if self.path != "/api/jobs":
        self.send_error_json("Unknown endpoint.", HTTPStatus.NOT_FOUND)
        return
      length = int(self.headers.get("Content-Length", "0"))
      payload = json.loads(self.rfile.read(length) or b"{}")
      self.send_json(launch_job(payload), HTTPStatus.ACCEPTED)
    except (ValueError, json.JSONDecodeError) as exc:
      self.send_error_json(exc)
    except OSError as exc:
      self.send_error_json(f"Could not launch benchmark: {exc}", HTTPStatus.INTERNAL_SERVER_ERROR)

  def serve_static(self, request_path: str):
    relative = "index.html" if request_path in {"", "/"} else request_path.lstrip("/")
    candidate = (STATIC_ROOT / relative).resolve()
    if STATIC_ROOT not in candidate.parents and candidate != STATIC_ROOT:
      raise FileNotFoundError("Static file not found.")
    if not candidate.is_file():
      candidate = STATIC_ROOT / "index.html"
    body = candidate.read_bytes()
    mime_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
    self.send_response(HTTPStatus.OK)
    self.send_header("Content-Type", f"{mime_type}; charset=utf-8" if mime_type.startswith("text/") else mime_type)
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)


def main():
  parser = argparse.ArgumentParser(description="Serve the PTD-BenchLab research GUI locally.")
  parser.add_argument("--host", default="127.0.0.1")
  parser.add_argument("--port", default=8090, type=int)
  args = parser.parse_args()
  server = ThreadingHTTPServer((args.host, args.port), Handler)
  print(f"PTD-BenchLab available at http://{args.host}:{args.port}")
  print("This local research server can launch benchmark scripts and access project artifacts.")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
  finally:
    server.server_close()


if __name__ == "__main__":
  main()
