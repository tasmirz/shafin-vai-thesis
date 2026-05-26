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
import sys
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
DATASET_ROOT = ROOT / "datasets-curated"
DATASET_MANIFEST_ROOT = ROOT / "reports" / "datasets"
OSM_PROTOCOL = ROOT / "config" / "research" / "bangladesh-osm-replication.json"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ALGORITHMS = ("baseline", "dscp-only", "aes-only", "aes-dscp")

PAPER_TARGETS = [
    {
        "dataset": "Synthetic smartphone",
        "baselineMs": 66520,
        "proposedMs": 43757,
        "reductionPct": 34.2,
        "status": "Paper-shaped generator and probability audit available; execute full sweep for reproduction",
    },
    {
        "dataset": "Bangladesh road / OSM",
        "baselineMs": 56274,
        "proposedMs": 42366,
        "reductionPct": 24.7,
        "status": "OSM MBR generator implemented; execute curated benchmark suite for reproduction",
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
          "boundMode": spark.get("boundMode"),
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
          "shuffleBytes": spark.get("totalShuffleBytes"),
          "shuffleRecords": spark.get("totalShuffleRecords"),
          "filterMs": spark.get("totalFilterMs"),
          "refineMs": spark.get("totalRefineMs"),
          "stragglerRatio": spark.get("maxStragglerRatio"),
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
    fields = reader.fieldnames or []
    dimension_fields = [field for field in fields if re.fullmatch(r"a\d+", field)]
    objects = set()
    queries = set()
    missing = 0
    records = 0
    preview = []
    query_counts = {}
    probability_totals = {}
    mbr_errors = 0
    for row in reader:
      records += 1
      if len(preview) < 50:
        preview.append(row)
      objects.add(row.get("objectId", ""))
      query_id = row.get("queryId", "")
      queries.add(query_id)
      query_counts[query_id] = query_counts.get(query_id, 0) + 1
      missing += sum(
          row.get(field, "").strip().lower() in {"", "null", "nan", "?"}
          for field in dimension_fields)
      if "probability" in fields:
        key = (query_id, row.get("objectId", ""))
        probability_totals[key] = probability_totals.get(key, 0.0) + float(row["probability"])
        if {"mbrMinX", "mbrMinY", "mbrMaxX", "mbrMaxY"}.issubset(fields):
          if not (
              float(row["mbrMinX"]) <= float(row["a0"]) <= float(row["mbrMaxX"])
              and float(row["mbrMinY"]) <= float(row["a1"]) <= float(row["mbrMaxY"])):
            mbr_errors += 1
  paper_style = "probability" in fields
  normalization_errors = sum(
      abs(total - 1.0) > 1.0e-6 for total in probability_totals.values())
  return {
      "path": str(path.relative_to(ROOT)),
      "columns": fields,
      "dimensions": dimension_fields,
      "records": records,
      "objects": len(objects),
      "queries": len(queries),
      "missingAttributeValues": missing,
      "paperStyle": paper_style,
      "probabilityAudit": {
          "enabled": paper_style,
          "groups": len(probability_totals),
          "normalizationErrors": normalization_errors,
          "mbrContainmentErrors": mbr_errors,
          "passed": paper_style and normalization_errors == 0 and mbr_errors == 0,
      },
      "queryCounts": query_counts,
      "preview": preview,
      "qualityChecks": [
          {"name": "Readable CSV schema", "passed": bool(fields), "value": f"{len(fields)} columns"},
          {"name": "At least one object", "passed": bool(objects), "value": f"{len(objects)} objects"},
          {"name": "At least one query", "passed": bool(queries), "value": f"{len(queries)} queries"},
          {
              "name": "Missing attributes recorded for imputation",
              "passed": True,
              "value": f"{missing} missing attribute values",
          },
          {
              "name": "Probability normalization",
              "passed": not paper_style or normalization_errors == 0,
              "value": (
                  f"{len(probability_totals)} object/query groups validated"
                  if paper_style else "not applicable to raw event fixture"),
          },
          {
              "name": "MBR contains sampled instances",
              "passed": not paper_style or mbr_errors == 0,
              "value": f"{mbr_errors} containment errors" if paper_style else "not applicable",
          },
      ],
      "note": (
          "Appearance probabilities and MBR containment were validated for this uncertain-object artifact."
          if paper_style else
          "This stream CSV contains incomplete attributes; select a generated paper dataset "
          "to activate probability and MBR auditing."
      ),
  }


def paper_datasets() -> list[dict]:
  if not DATASET_MANIFEST_ROOT.exists():
    return []
  documents = []
  for path in DATASET_MANIFEST_ROOT.glob("*.json"):
    try:
      document = json_file(path)
      document["manifestPath"] = str(path.relative_to(ROOT))
      documents.append(document)
    except (OSError, json.JSONDecodeError):
      continue
  return sorted(documents, key=lambda item: item.get("createdUtc", ""), reverse=True)


def osm_readiness() -> dict:
  protocol = json_file(OSM_PROTOCOL)
  metadata_path = ROOT / protocol["source"]["metadataPath"]
  metadata = json_file(metadata_path) if metadata_path.is_file() else {"geometry_types": {}}
  lines = metadata["geometry_types"].get("LINESTRING", 0)
  return {
      "protocol": protocol,
      "availableLineFeatures": lines,
      "minimumLineFeatures": protocol["source"]["minimumLineFeatures"],
      "ready": lines >= protocol["source"]["minimumLineFeatures"],
      "sourceAvailable": metadata_path.is_file(),
      "manifests": [
          document for document in paper_datasets()
          if document.get("dataset") == "bangladesh-osm-road-mbr"],
  }


def experiment_matrix(payload: dict | None = None) -> dict:
  values = payload or {}
  fields = {
      "datasets": values.get("datasets", ["synthetic-smartphone", "bangladesh-osm-road-mbr"]),
      "algorithms": values.get("algorithms", list(ALGORITHMS)),
      "k": values.get("k", [5, 10, 20]),
      "partitions": values.get("partitions", [2, 5, 8, 10]),
      "repetitions": int(values.get("repetitions", 20)),
  }
  total = (
      len(fields["datasets"]) * len(fields["algorithms"]) * len(fields["k"])
      * len(fields["partitions"]) * fields["repetitions"])
  return {
      "parameters": fields,
      "runCount": total,
      "warning": "Large experiment matrix; schedule in batches." if total > 1000 else None,
      "templates": [
          "Paper reproduction", "AES/DSCP ablation", "Partition skew", "Correctness validation"],
  }


def latex_report(ids: list[str]) -> str:
  selected = [load_run(run_id) for run_id in ids] if ids else list_runs()
  lines = [
      r"\begin{tabular}{lrrrrr}",
      r"\hline",
      r"Method & Time (ms) & Shuffle (B) & Emissions & AER & Exact \\",
      r"\hline",
  ]
  for run in selected:
    summary = run["summary"]
    lines.append(
        f"{summary.get('algorithm', 'unknown')} & {summary.get('algorithmElapsedMs') or 0} & "
        f"{summary.get('shuffleBytes') or 0} & {summary.get('totalEmittedRecords') or 0} & "
        f"{(summary.get('avgAER') or 0):.4f} & "
        f"{'yes' if summary.get('exactAgreement') else 'no'} \\\\")
  lines.extend([r"\hline", r"\end{tabular}", ""])
  return "\n".join(lines)


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
              "Paper-shaped smartphone and Bangladesh road MBR dataset builders",
              "Probability normalization and MBR containment audit",
              "Observed Spark shuffle bytes, phase time and skew metrics",
          ],
          "pending": [
              "Per-object DDR/MBR and AES trace records",
              "Full published-scale benchmark execution and paper-number reproduction",
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
  if mode not in {"csv", "stream", "smartphone", "osm"}:
    raise ValueError("Mode must be csv, stream, smartphone or osm.")
  run_id = require_run_id(str(payload.get("runId", "")))
  if mode in {"csv", "stream"} and (RUN_ROOT / run_id).exists():
    raise ValueError("A saved run already exists with this ID.")
  if mode in {"smartphone", "osm"} and (DATASET_MANIFEST_ROOT / f"{run_id}.json").exists():
    raise ValueError("A generated dataset already exists with this ID.")
  with JOB_LOCK:
    if any(job.run_id == run_id and job.status == "running" for job in JOBS.values()):
      raise ValueError("A running job already uses this run ID.")
  JOB_ROOT.mkdir(parents=True, exist_ok=True)
  job_id = uuid.uuid4().hex[:12]
  env = os.environ.copy()
  env["RUN_ID"] = run_id
  env["BUILD_IMAGE"] = "1" if payload.get("buildImage", False) else "0"
  if mode in {"csv", "stream"}:
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
  elif mode == "stream":
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
  else:
    output = DATASET_ROOT / f"{run_id}.csv"
    manifest = DATASET_MANIFEST_ROOT / f"{run_id}.json"
    instances_min = positive_int(payload.get("instancesMin", 5), "instancesMin")
    instances_max = positive_int(payload.get("instancesMax", 11), "instancesMax")
    if int(instances_min) > int(instances_max):
      raise ValueError("instancesMin must not exceed instancesMax.")
    common = [
        "--output", str(output),
        "--manifest", str(manifest),
        "--objects", positive_int(payload.get("objects", 100), "objects"),
        "--instances-min", instances_min,
        "--instances-max", instances_max,
        "--queries", positive_int(payload.get("queries", 1), "queries"),
        "--partitions", positive_int(payload.get("partitions", 8), "partitions"),
        "--seed", integer(payload.get("seed", 42), "seed"),
    ]
    script = str(ROOT / "scripts" / "research" / "build_paper_dataset.py")
    command = (
        [sys.executable, script, "smartphone", *common] if mode == "smartphone"
        else ["uv", "run", "--with", "pyproj", script, "osm", *common])
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
      elif request.path == "/api/datasets/paper":
        self.send_json({"datasets": paper_datasets()})
      elif request.path == "/api/datasets/osm-readiness":
        self.send_json(osm_readiness())
      elif request.path == "/api/experiment-matrix":
        self.send_json(experiment_matrix())
      elif request.path == "/api/reports/latex":
        ids = parse_qs(request.query).get("ids", [""])[0].split(",")
        body = latex_report([run_id for run_id in ids if run_id]).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Disposition", 'attachment; filename="ptd-results.tex"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
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
      if self.path == "/api/experiment-matrix":
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        self.send_json(experiment_matrix(payload))
        return
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
