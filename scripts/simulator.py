#!/usr/bin/env python3
"""Preprocess raw datasets and publish normalized incomplete records to MQTT."""

import argparse
import csv
import gzip
import io
import json
import math
import random
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "datasets-raw"
TOPICS = {
    "synthetic": "thesis/raw",
    "csv": "thesis/raw/csv",
    "intel": "thesis/raw/intel",
    "pump": "thesis/raw/pump",
    "gas": "thesis/raw/gas",
}


def record(object_id, query_id, event_time, attributes):
  missing = [value is None or (isinstance(value, float) and math.isnan(value)) for value in attributes]
  clean = [None if missing[i] else attributes[i] for i in range(len(attributes))]
  return {
      "objectId": object_id,
      "queryId": query_id,
      "eventTime": event_time,
      "opType": "UPSERT",
      "attributes": clean,
      "missingMask": missing,
  }


def number(value):
  if value is None or not value.strip() or value.strip().lower() in {"null", "nan", "?"}:
    return None
  return float(value)


def synthetic(args):
  rng = random.Random(args.seed)
  emitted = 0
  limit = args.max_events or args.objects * args.queries
  for query in range(args.queries):
    for obj in range(args.objects):
      if emitted >= limit:
        return
      attrs = []
      for dimension in range(args.dimensions):
        cluster = (obj % 17) / 16.0 * 0.45
        value = min(1.0, max(0.0, cluster + 0.55 * rng.random()))
        attrs.append(None if rng.random() < args.missing_rate else value)
      yield record(
          f"obj-{obj}", f"q{query}", args.start_time_ms + emitted * args.event_gap_ms, attrs)
      emitted += 1


def intel(args):
  path = Path(args.dataset_path) if args.dataset_path else RAW_DIR / "intel_lab_data.gz"
  limit = args.max_events or args.objects * args.queries
  with gzip.open(path, "rt") as source:
    for index, line in enumerate(source):
      if index >= limit:
        return
      columns = line.split()
      timestamp = datetime.fromisoformat(f"{columns[0]}T{columns[1]}").replace(
          tzinfo=timezone.utc).timestamp() * 1000
      attrs = [number(value) for value in columns[4:8]]
      yield record(f"intel-mote-{columns[3]}", "intel", int(timestamp), attrs)


def pump(args):
  path = Path(args.dataset_path) if args.dataset_path else RAW_DIR / "pump_sensor_data.zip"
  limit = args.max_events or args.objects * args.queries
  with zipfile.ZipFile(path) as archive:
    with archive.open("sensor.csv") as source:
      reader = csv.DictReader(io.TextIOWrapper(source))
      sensors = [name for name in reader.fieldnames or [] if name.startswith("sensor_")]
      for index, row in enumerate(reader):
        if index >= limit:
          return
        timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc).timestamp() * 1000
        status = (row.get("machine_status") or "UNKNOWN").lower()
        attrs = [number(row[name]) for name in sensors]
        yield record(f"pump-row-{index}", f"pump-{status}", int(timestamp), attrs)


def gas(args):
  path = Path(args.dataset_path) if args.dataset_path else RAW_DIR / "gas+sensors+for+home+activity+monitoring.zip"
  limit = args.max_events or args.objects * args.queries
  with zipfile.ZipFile(path) as outer:
    nested = outer.read("HT_Sensor_dataset.zip")
  with zipfile.ZipFile(io.BytesIO(nested)) as inner:
    with inner.open("HT_Sensor_dataset.dat") as source:
      reader = io.TextIOWrapper(source)
      next(reader)
      for index, line in enumerate(reader):
        if index >= limit:
          return
        columns = line.split()
        attrs = [number(value) for value in columns[2:12]]
        timestamp = 1_420_070_400_000 + round(float(columns[1]) * 3_600_000)
        yield record(f"gas-id-{columns[0]}", "gas", timestamp, attrs)


def generic_csv(args):
  if not args.dataset_path:
    raise SystemExit("--dataset-path is required for dataset=csv")
  limit = args.max_events or args.objects * args.queries
  with Path(args.dataset_path).open(newline="") as source:
    reader = csv.DictReader(source)
    attributes = [name for name in reader.fieldnames or [] if name.startswith("a")]
    for index, row in enumerate(reader):
      if index >= limit:
        return
      payload = record(
          row["objectId"], row["queryId"], int(row["eventTime"]),
          [number(row[name]) for name in attributes])
      # Preserve paper-shaped uncertain-object metadata across MQTT/Kafka so stream
      # runs execute the same MBR/indexed semantics as direct CSV runs.
      if row.get("instanceId"):
        payload["instanceId"] = row["instanceId"]
      if row.get("probability"):
        payload["appearanceProbability"] = float(row["probability"])
      if row.get("serverId"):
        payload["serverPartition"] = int(row["serverId"])
      mbr_min = [
          number(row[name]) for name in ("mbrMinX", "mbrMinY") if name in row
      ]
      mbr_max = [
          number(row[name]) for name in ("mbrMaxX", "mbrMaxY") if name in row
      ]
      if len(mbr_min) == len(attributes) and len(mbr_max) == len(attributes):
        payload["mbrMin"] = mbr_min
        payload["mbrMax"] = mbr_max
      yield payload


PROVIDERS = {
    "synthetic": synthetic,
    "csv": generic_csv,
    "intel": intel,
    "pump": pump,
    "gas": gas,
}


def batches(args):
  datasets = ["intel", "pump", "gas"] if args.dataset == "all" else [args.dataset]
  for dataset in datasets:
    if dataset not in PROVIDERS:
      raise SystemExit(f"unsupported dataset: {dataset}")
    topic = args.topic if args.topic and args.dataset != "all" else TOPICS[dataset]
    yield dataset, topic, PROVIDERS[dataset](args)


def publish(args):
  try:
    import paho.mqtt.client as mqtt
  except ImportError as exc:
    raise SystemExit("paho-mqtt is not installed; run: just venv") from exc

  client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id)
  client.connect(args.mqtt_host, args.mqtt_port, 60)
  client.loop_start()
  total = 0
  by_topic = {}
  try:
    rounds = 0
    while args.repeat == 0 or rounds < args.repeat:
      for _, topic, records in batches(args):
        for payload in records:
          message = json.dumps(payload, separators=(",", ":"))
          info = client.publish(topic, message, qos=args.qos)
          if args.qos > 0:
            info.wait_for_publish()
          total += 1
          by_topic[topic] = by_topic.get(topic, 0) + 1
          if args.rate_per_second > 0:
            time.sleep(1.0 / args.rate_per_second)
      rounds += 1
  finally:
    client.disconnect()
    client.loop_stop()
  return {"published": total, "topics": by_topic}


def preview(args):
  totals = {}
  sample = {}
  for _, topic, records in batches(args):
    items = list(records)
    totals[topic] = len(items)
    if items:
      sample[topic] = items[0]
  return {"published": sum(totals.values()), "topics": totals, "sample": sample}


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("--dataset", default="synthetic")
  parser.add_argument("--dataset-path", default="")
  parser.add_argument("--topic", default="")
  parser.add_argument("--mqtt-host", default="localhost")
  parser.add_argument("--mqtt-port", type=int, default=1883)
  parser.add_argument("--client-id", default="python-incomplete-data-publisher")
  parser.add_argument("--objects", type=int, default=500)
  parser.add_argument("--queries", type=int, default=2)
  parser.add_argument("--dimensions", type=int, default=4)
  parser.add_argument("--missing-rate", type=float, default=0.2)
  parser.add_argument("--max-events", type=int)
  parser.add_argument("--seed", type=int, default=42)
  parser.add_argument("--start-time-ms", type=int, default=1_700_000_000_000)
  parser.add_argument("--event-gap-ms", type=int, default=20)
  parser.add_argument("--rate-per-second", type=float, default=10.0)
  parser.add_argument("--qos", type=int, default=1)
  parser.add_argument("--repeat", type=int, default=1, help="number of batches to publish; 0 repeats forever")
  parser.add_argument("--dry-run", action="store_true")
  parser.add_argument("--json", action="store_true")
  return parser.parse_args()


def main():
  args = parse_args()
  result = preview(args) if args.dry_run else publish(args)
  if args.json:
    print(json.dumps(result, indent=2))
  else:
    print(f"simulator dataset={args.dataset} published={result['published']} topics={result['topics']}")


if __name__ == "__main__":
  main()
