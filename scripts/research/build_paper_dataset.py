#!/usr/bin/env python3
"""Generate and validate paper-shaped uncertain-object CSV benchmark artifacts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import struct
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROADS = ROOT / "datasets-osm" / "hotosm_bgd_roads_osm_shp" / "roads_lines.shp"


def digest(path: Path) -> str:
  value = hashlib.sha256()
  with path.open("rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""):
      value.update(block)
  return value.hexdigest()


def ensure_parent(path: Path):
  path.parent.mkdir(parents=True, exist_ok=True)


def probability_weights(count: int, rng: random.Random) -> list[float]:
  raw = [rng.uniform(0.2, 1.0) for _ in range(count)]
  total = sum(raw)
  values = [round(value / total, 12) for value in raw]
  values[-1] += 1.0 - sum(values)
  return values


def make_index(partitions: int) -> dict[int, dict]:
  return {
      sid: {
          "serverId": sid,
          "objects": 0,
          "instances": 0,
          "mbr": [math.inf, math.inf, -math.inf, -math.inf],
      }
      for sid in range(partitions)
  }


def update_index(index: dict[int, dict], server_id: int, mbr: tuple[float, float, float, float],
                 instance_count: int):
  entry = index[server_id]
  entry["objects"] += 1
  entry["instances"] += instance_count
  entry["mbr"][0] = min(entry["mbr"][0], mbr[0])
  entry["mbr"][1] = min(entry["mbr"][1], mbr[1])
  entry["mbr"][2] = max(entry["mbr"][2], mbr[2])
  entry["mbr"][3] = max(entry["mbr"][3], mbr[3])


def finalize_index(index: dict[int, dict]) -> list[dict]:
  entries = []
  for entry in index.values():
    if entry["objects"] == 0:
      entry["mbr"] = None
    else:
      entry["mbr"] = [round(value, 3) for value in entry["mbr"]]
    entries.append(entry)
  return entries


def csv_writer(path: Path):
  ensure_parent(path)
  target = path.open("w", newline="")
  columns = [
      "objectId", "instanceId", "probability", "serverId", "queryId", "eventTime",
      "opType", "queryA0", "queryA1", "a0", "a1",
      "mbrMinX", "mbrMinY", "mbrMaxX", "mbrMaxY", "source",
  ]
  writer = csv.DictWriter(target, fieldnames=columns)
  writer.writeheader()
  return target, writer


def smartphone(args) -> dict:
  output = Path(args.output)
  rng = random.Random(args.seed)
  index = make_index(args.partitions)
  query_points = [
      (rng.uniform(0.15, 0.85), rng.uniform(0.15, 0.85))
      for _ in range(args.queries)
  ]
  row_count = 0
  target, writer = csv_writer(output)
  try:
    for object_no in range(args.objects):
      object_id = f"phone-{object_no:06d}"
      server_id = rng.randrange(args.partitions)
      count = rng.randint(args.instances_min, args.instances_max)
      probabilities = probability_weights(count, rng)
      price_center = rng.uniform(150.0, 1400.0)
      battery_center = rng.uniform(2500.0, 6500.0)
      points = []
      for instance_no in range(count):
        price = max(50.0, price_center + rng.gauss(0.0, 70.0))
        battery = max(1000.0, battery_center + rng.gauss(0.0, 240.0))
        # Lower coordinates are preferred relative to a normalized query; invert battery.
        point = (price / 1500.0, 1.0 - min(battery, 7000.0) / 7000.0)
        points.append(point)
      mbr = (
          min(point[0] for point in points), min(point[1] for point in points),
          max(point[0] for point in points), max(point[1] for point in points))
      update_index(index, server_id, mbr, count)
      for query_no in range(args.queries):
        for instance_no, (probability, point) in enumerate(zip(probabilities, points)):
          writer.writerow({
              "objectId": object_id,
              "instanceId": f"{object_id}-i{instance_no}",
              "probability": f"{probability:.12f}",
              "serverId": server_id,
              "queryId": f"q{query_no}",
              "eventTime": args.start_time + row_count,
              "opType": "UPSERT",
              "queryA0": f"{query_points[query_no][0]:.9f}",
              "queryA1": f"{query_points[query_no][1]:.9f}",
              "a0": f"{point[0]:.9f}",
              "a1": f"{point[1]:.9f}",
              "mbrMinX": f"{mbr[0]:.9f}",
              "mbrMinY": f"{mbr[1]:.9f}",
              "mbrMaxX": f"{mbr[2]:.9f}",
              "mbrMaxY": f"{mbr[3]:.9f}",
              "source": "synthetic-smartphone",
          })
          row_count += 1
  finally:
    target.close()
  return dataset_manifest(
      args, output, "synthetic-smartphone", row_count, args.objects, index,
      {"attributes": ["normalized_price", "inverse_normalized_battery"]})


def zipf_coordinate(rng: random.Random, skew: float) -> float:
  ranks = list(range(1, 101))
  selected = rng.choices(ranks, weights=[rank ** (-skew) for rank in ranks], k=1)[0]
  return min(100.0, max(0.0, selected - 1.0 + rng.random()))


def uncertain_region_center(rng: random.Random, distribution: str, zipf_skew: float):
  if distribution == "uniform":
    return rng.uniform(0.0, 100.0), rng.uniform(0.0, 100.0)
  if distribution == "gaussian":
    return (
        min(100.0, max(0.0, rng.gauss(50.0, 15.0))),
        min(100.0, max(0.0, rng.gauss(50.0, 15.0))),
    )
  return zipf_coordinate(rng, zipf_skew), zipf_coordinate(rng, zipf_skew)


def rai_synthetic(args) -> dict:
  output = Path(args.output)
  rng = random.Random(args.seed)
  index = make_index(args.partitions)
  query_points = [
      (rng.uniform(0.0, 100.0), rng.uniform(0.0, 100.0))
      for _ in range(args.queries)
  ]
  row_count = 0
  target, writer = csv_writer(output)
  try:
    for object_no in range(args.objects):
      object_id = f"{args.distribution}-{object_no:06d}"
      server_id = rng.randrange(args.partitions)
      count = rng.randint(args.instances_min, args.instances_max)
      probability = 1.0 / count
      center_x, center_y = uncertain_region_center(rng, args.distribution, args.zipf_skew)
      side = rng.uniform(1.0e-9, args.lmax)
      half = side / 2.0
      mbr = (
          max(0.0, center_x - half),
          max(0.0, center_y - half),
          min(100.0, center_x + half),
          min(100.0, center_y + half),
      )
      if mbr[2] <= mbr[0] or mbr[3] <= mbr[1]:
        continue
      update_index(index, server_id, mbr, count)
      points = [
          (rng.uniform(mbr[0], mbr[2]), rng.uniform(mbr[1], mbr[3]))
          for _ in range(count)
      ]
      for query_no, query_point in enumerate(query_points):
        for instance_no, point in enumerate(points):
          writer.writerow({
              "objectId": object_id,
              "instanceId": f"{object_id}-i{instance_no}",
              "probability": f"{probability:.12f}",
              "serverId": server_id,
              "queryId": f"q{query_no}",
              "eventTime": args.start_time + row_count,
              "opType": "UPSERT",
              "queryA0": f"{query_point[0]:.9f}",
              "queryA1": f"{query_point[1]:.9f}",
              "a0": f"{point[0]:.9f}",
              "a1": f"{point[1]:.9f}",
              "mbrMinX": f"{mbr[0]:.9f}",
              "mbrMinY": f"{mbr[1]:.9f}",
              "mbrMaxX": f"{mbr[2]:.9f}",
              "mbrMaxY": f"{mbr[3]:.9f}",
              "source": f"rai-lian-{args.distribution}",
          })
          row_count += 1
  finally:
    target.close()
  return dataset_manifest(
      args, output, f"rai-lian-{args.distribution}", row_count, args.objects, index,
      {
          "attributes": ["x", "y"],
          "uncertaintyRegion": "square MBR in normalized [0,100]^2 domain",
          "centerDistribution": args.distribution,
          "zipfSkewness": args.zipf_skew if args.distribution == "zipf" else None,
          "lmax": args.lmax,
          "samplePolicy": "uniform points inside each uncertain-object MBR",
      })


def line_mbrs(path: Path, maximum: int):
  with path.open("rb") as source:
    source.read(100)
    count = 0
    while maximum <= 0 or count < maximum:
      record_header = source.read(8)
      if len(record_header) < 8:
        return
      _, length_words = struct.unpack(">ii", record_header)
      content = source.read(length_words * 2)
      if len(content) < 36:
        continue
      shape_type = struct.unpack("<i", content[:4])[0]
      if shape_type not in (3, 5):
        continue
      bbox = struct.unpack("<dddd", content[4:36])
      yield bbox
      count += 1


def transformer():
  try:
    from pyproj import Transformer
  except ImportError as exc:
    raise SystemExit(
        "OSM curation needs pyproj for EPSG:9678 conversion; run with "
        "`uv run --with pyproj scripts/research/build_paper_dataset.py osm ...`.") from exc
  return Transformer.from_crs("EPSG:4326", "EPSG:9678", always_xy=True)


def projected_mbr(bounds, convert) -> tuple[float, float, float, float]:
  minx, miny, maxx, maxy = bounds
  corners = [
      convert.transform(minx, miny), convert.transform(maxx, miny),
      convert.transform(maxx, maxy), convert.transform(minx, maxy),
  ]
  xs = [point[0] for point in corners]
  ys = [point[1] for point in corners]
  result = [min(xs), min(ys), max(xs), max(ys)]
  for first, second in ((0, 2), (1, 3)):
    if result[second] - result[first] < 1.0:
      middle = (result[first] + result[second]) / 2.0
      result[first], result[second] = middle - 0.5, middle + 0.5
  return tuple(result)


def osm(args) -> dict:
  source = Path(args.source)
  if not source.is_file():
    raise SystemExit(f"Road shapefile not found: {source}")
  output = Path(args.output)
  rng = random.Random(args.seed)
  convert = transformer()
  index = make_index(args.partitions)
  row_count = 0
  object_count = 0
  mbrs = [projected_mbr(bounds, convert) for bounds in line_mbrs(source, args.objects)]
  if len(mbrs) != args.objects:
    raise SystemExit(f"Requested {args.objects} road objects, found only {len(mbrs)}.")
  extent = (
      min(mbr[0] for mbr in mbrs), min(mbr[1] for mbr in mbrs),
      max(mbr[2] for mbr in mbrs), max(mbr[3] for mbr in mbrs))
  query_points = [
      (rng.uniform(extent[0], extent[2]), rng.uniform(extent[1], extent[3]))
      for _ in range(args.queries)
  ]
  target, writer = csv_writer(output)
  try:
    for object_no, mbr in enumerate(mbrs):
      object_id = f"road-{object_no:06d}"
      server_id = rng.randrange(args.partitions)
      count = rng.randint(args.instances_min, args.instances_max)
      probability = 1.0 / count
      update_index(index, server_id, mbr, count)
      points = [
          (rng.uniform(mbr[0], mbr[2]), rng.uniform(mbr[1], mbr[3]))
          for _ in range(count)
      ]
      for query_no in range(args.queries):
        for instance_no, point in enumerate(points):
          writer.writerow({
              "objectId": object_id,
              "instanceId": f"{object_id}-i{instance_no}",
              "probability": f"{probability:.12f}",
              "serverId": server_id,
              "queryId": f"q{query_no}",
              "eventTime": args.start_time + row_count,
              "opType": "UPSERT",
              "queryA0": f"{query_points[query_no][0]:.3f}",
              "queryA1": f"{query_points[query_no][1]:.3f}",
              "a0": f"{point[0]:.3f}",
              "a1": f"{point[1]:.3f}",
              "mbrMinX": f"{mbr[0]:.3f}",
              "mbrMinY": f"{mbr[1]:.3f}",
              "mbrMaxX": f"{mbr[2]:.3f}",
              "mbrMaxY": f"{mbr[3]:.3f}",
              "source": "bangladesh-osm-road-mbr",
          })
          row_count += 1
      object_count += 1
  finally:
    target.close()
  return dataset_manifest(
      args, output, "bangladesh-osm-road-mbr", row_count, object_count, index,
      {
          "sourcePath": str(source),
          "sourceSha256": digest(source),
          "sourceCrs": "EPSG:4326",
          "projectedCrs": "EPSG:9678",
          "queryPolicy": "seeded uniformly sampled point within projected dataset extent",
          "samplePolicy": "uniform points inside each road-segment MBR",
      })


def dataset_manifest(args, output: Path, dataset_type: str, rows: int, objects: int,
                     index: dict[int, dict], extra: dict) -> dict:
  document = {
      "schemaVersion": 1,
      "dataset": dataset_type,
      "createdUtc": datetime.now(timezone.utc).isoformat(),
      "csvPath": str(output),
      "csvSha256": digest(output),
      "objects": objects,
      "rows": rows,
      "queries": args.queries,
      "instancesPerObject": {"minimum": args.instances_min, "maximum": args.instances_max},
      "probabilityPolicy": "normalized appearance probability per object and query",
      "seed": args.seed,
      "partitions": args.partitions,
      "partitionIndex": finalize_index(index),
      "validation": validate_csv(output),
  }
  document.update(extra)
  return document


def validate_csv(path: Path) -> dict:
  totals = defaultdict(float)
  servers = {}
  mbr_errors = 0
  rows = 0
  with path.open(newline="") as source:
    for row in csv.DictReader(source):
      rows += 1
      key = (row["queryId"], row["objectId"])
      totals[key] += float(row["probability"])
      previous = servers.setdefault(row["objectId"], row["serverId"])
      if previous != row["serverId"]:
        raise ValueError(f"Object appears on multiple servers: {row['objectId']}")
      if not (
          float(row["mbrMinX"]) <= float(row["a0"]) <= float(row["mbrMaxX"])
          and float(row["mbrMinY"]) <= float(row["a1"]) <= float(row["mbrMaxY"])):
        mbr_errors += 1
  normalization_errors = sum(abs(total - 1.0) > 1.0e-6 for total in totals.values())
  return {
      "rows": rows,
      "objectQueryGroups": len(totals),
      "normalizationErrors": normalization_errors,
      "mbrContainmentErrors": mbr_errors,
      "serverAssignmentErrors": 0,
      "passed": normalization_errors == 0 and mbr_errors == 0,
  }


def write_manifest(path: Path, document: dict):
  ensure_parent(path)
  path.write_text(json.dumps(document, indent=2) + "\n")
  print(json.dumps(document, indent=2))
  if not document["validation"]["passed"]:
    raise SystemExit("Dataset validation failed.")


def generation_arguments(parser):
  parser.add_argument("--output", required=True)
  parser.add_argument("--manifest", required=True)
  parser.add_argument("--objects", type=int, required=True)
  parser.add_argument("--instances-min", type=int, default=5)
  parser.add_argument("--instances-max", type=int, default=11)
  parser.add_argument("--queries", type=int, default=1)
  parser.add_argument("--partitions", type=int, default=8)
  parser.add_argument("--seed", type=int, default=42)
  parser.add_argument("--start-time", type=int, default=1700000000000)


def main():
  parser = argparse.ArgumentParser()
  commands = parser.add_subparsers(dest="command", required=True)
  phone = commands.add_parser("smartphone")
  generation_arguments(phone)
  synthetic = commands.add_parser("synthetic")
  generation_arguments(synthetic)
  synthetic.add_argument("--distribution", choices=("uniform", "gaussian", "zipf"),
                         default="uniform")
  synthetic.add_argument("--lmax", type=float, default=10.0)
  synthetic.add_argument("--zipf-skew", type=float, default=0.8)
  roads = commands.add_parser("osm")
  generation_arguments(roads)
  roads.add_argument("--source", default=str(DEFAULT_ROADS))
  check = commands.add_parser("validate")
  check.add_argument("--input", required=True)
  args = parser.parse_args()

  if args.command == "validate":
    result = validate_csv(Path(args.input))
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["passed"] else 1)
  if args.instances_min < 1 or args.instances_max < args.instances_min:
    raise SystemExit("Invalid instance range.")
  if args.command == "synthetic" and args.lmax <= 0.0:
    raise SystemExit("lmax must be greater than zero.")
  if args.command == "smartphone":
    document = smartphone(args)
  elif args.command == "synthetic":
    document = rai_synthetic(args)
  else:
    document = osm(args)
  write_manifest(Path(args.manifest), document)


if __name__ == "__main__":
  main()
