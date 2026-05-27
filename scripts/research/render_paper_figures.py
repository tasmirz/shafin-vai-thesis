#!/usr/bin/env python3
"""Render paper-shaped SVG figures and tables from immutable treatment runs."""

from __future__ import annotations

import argparse
import html
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ROOT = ROOT / "reports" / "runs"
OUTPUT_ROOT = ROOT / "reports" / "figures"
VARIANTS = ("baseline", "aes-only", "dscp-only", "aes-dscp")
LABELS = {
    "baseline": "Baseline",
    "aes-only": "AES-only",
    "dscp-only": "DSCP-only",
    "aes-dscp": "Proposed",
}
PAPER = {
    "smartphone": {"baseline": 66520, "aes-only": 44760, "dscp-only": 58484, "aes-dscp": 43757},
    "road-full": {"baseline": 56274, "aes-only": 42967, "dscp-only": 46848, "aes-dscp": 42366},
    "road-smoke": {"baseline": 56274, "aes-only": 42967, "dscp-only": 46848, "aes-dscp": 42366},
}


def suite_metrics(suite: str) -> dict[str, dict]:
  return {
      variant: json.loads((RUN_ROOT / f"{suite}-{variant}" / "metrics.json").read_text())["spark"]
      for variant in VARIANTS
  }


def reduction(values: dict[str, int], variant: str) -> float:
  baseline = values["baseline"]
  return 0.0 if not baseline else (baseline - values[variant]) / baseline * 100.0


def svg_bar_chart(title: str, subtitle: str, values: dict[str, int], output: Path) -> None:
  width, height = 690, 430
  left, top, bottom = 76, 62, 76
  chart_height = height - top - bottom
  chart_width = width - left - 36
  maximum = max(values.values()) * 1.12 or 1
  bar_width = 92
  gap = (chart_width - len(VARIANTS) * bar_width) / (len(VARIANTS) + 1)
  colors = ("#fefefe", "#d3d3d3", "#919191", "#525252")
  pieces = [
      f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
      '<rect width="100%" height="100%" fill="white"/>',
      f'<text x="{width / 2}" y="24" text-anchor="middle" font-family="Georgia, serif" font-size="16">{html.escape(title)}</text>',
      f'<text x="{width / 2}" y="45" text-anchor="middle" font-family="Arial, sans-serif" fill="#555" font-size="11">{html.escape(subtitle)}</text>',
  ]
  for step in range(5):
    value = maximum * step / 4
    y = top + chart_height - chart_height * step / 4
    pieces.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + chart_width}" y2="{y:.1f}" stroke="#d8d8d8" stroke-dasharray="3 3"/>')
    pieces.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" font-family="Arial, sans-serif" font-size="10">{int(value):,}</text>')
  pieces.extend([
      f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_height}" stroke="#222"/>',
      f'<line x1="{left}" y1="{top + chart_height}" x2="{left + chart_width}" y2="{top + chart_height}" stroke="#222"/>',
      f'<text x="18" y="{top + chart_height / 2}" transform="rotate(-90 18 {top + chart_height / 2})" text-anchor="middle" font-family="Georgia, serif" font-size="12">Avg Wall Clock Time (ms)</text>',
  ])
  for index, variant in enumerate(VARIANTS):
    x = left + gap + index * (bar_width + gap)
    value = values[variant]
    bar_height = chart_height * value / maximum
    y = top + chart_height - bar_height
    pieces.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width}" height="{bar_height:.1f}" fill="{colors[index]}" stroke="#222" stroke-width="1.5"/>')
    pieces.append(f'<text x="{x + bar_width / 2:.1f}" y="{y - 8:.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="10">{value:,}</text>')
    pieces.append(f'<text x="{x + bar_width / 2:.1f}" y="{top + chart_height + 21}" text-anchor="middle" font-family="Georgia, serif" font-size="12">{LABELS[variant]}</text>')
    if variant != "baseline":
      pieces.append(f'<text x="{x + bar_width / 2:.1f}" y="{y + 18:.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="10">{reduction(values, variant):.1f}%</text>')
  pieces.append("</svg>\n")
  output.write_text("\n".join(pieces))


def svg_two_dataset(suites: dict[str, dict[str, int]], output: Path) -> None:
  width, height = 700, 420
  left, top, bottom = 78, 55, 72
  maximum = max(value for values in suites.values() for value in (values["baseline"], values["aes-dscp"])) * 1.12
  chart_height = height - top - bottom
  pieces = [
      f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
      '<rect width="100%" height="100%" fill="white"/>',
      f'<text x="{width / 2}" y="25" text-anchor="middle" font-family="Georgia, serif" font-size="16">Baseline vs. Proposed - Observed Spark act WC</text>',
  ]
  for step in range(5):
    value = maximum * step / 4
    y = top + chart_height - chart_height * step / 4
    pieces.append(f'<line x1="{left}" y1="{y:.1f}" x2="550" y2="{y:.1f}" stroke="#d8d8d8" stroke-dasharray="3 3"/>')
    pieces.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" font-family="Arial, sans-serif" font-size="10">{int(value):,}</text>')
  pieces.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_height}" stroke="#222"/>')
  pieces.append(f'<line x1="{left}" y1="{top + chart_height}" x2="550" y2="{top + chart_height}" stroke="#222"/>')
  for index, (dataset, values) in enumerate(suites.items()):
    start = 128 + index * 210
    for bar_index, variant in enumerate(("baseline", "aes-dscp")):
      value = values[variant]
      h = chart_height * value / maximum
      y = top + chart_height - h
      x = start + bar_index * 64
      fill = "#ffffff" if variant == "baseline" else "#c8c8c8"
      pieces.append(f'<rect x="{x}" y="{y:.1f}" width="54" height="{h:.1f}" fill="{fill}" stroke="#222" stroke-width="1.5"/>')
      pieces.append(f'<text x="{x + 27}" y="{y - 7:.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="9">{value:,}</text>')
    pieces.append(f'<text x="{start + 58}" y="{top + chart_height + 22}" text-anchor="middle" font-family="Georgia, serif" font-size="12">{html.escape(dataset)}</text>')
    pieces.append(f'<text x="{start + 91}" y="{top + 32}" font-family="Arial, sans-serif" font-size="10">{reduction(values, "aes-dscp"):.1f}% reduced</text>')
  pieces.extend([
      '<rect x="575" y="145" width="18" height="12" fill="#ffffff" stroke="#222"/><text x="600" y="155" font-family="Arial, sans-serif" font-size="11">Baseline</text>',
      '<rect x="575" y="168" width="18" height="12" fill="#c8c8c8" stroke="#222"/><text x="600" y="178" font-family="Arial, sans-serif" font-size="11">Proposed</text>',
      "</svg>\n",
  ])
  output.write_text("\n".join(pieces))


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--smartphone-suite")
  parser.add_argument("--road-suite")
  parser.add_argument("--output-dir", default=str(OUTPUT_ROOT))
  args = parser.parse_args()
  if not args.smartphone_suite and not args.road_suite:
    raise SystemExit("Provide --smartphone-suite and/or --road-suite.")
  output = Path(args.output_dir)
  output.mkdir(parents=True, exist_ok=True)
  documents: dict[str, dict] = {}
  for profile, suite in (("smartphone", args.smartphone_suite), ("road-full", args.road_suite)):
    if suite:
      metrics = suite_metrics(suite)
      values = {variant: metrics[variant]["algorithmElapsedMs"] for variant in VARIANTS}
      documents[profile] = {"suite": suite, "values": values, "metrics": metrics}
      suffix = "synthetic" if profile == "smartphone" else "real"
      svg_bar_chart(
          f"Ablation Study on {suffix.title()} Dataset",
          "Measured Spark execution; same-machine treatments",
          values,
          output / f"{suite}-ablation.svg")
  if documents:
    svg_two_dataset(
        {("Synthetic" if name == "smartphone" else "Real"): document["values"]
         for name, document in documents.items()},
        output / "observed-spark-baseline-proposed.svg")
  lines = [
      "# Paper-Style Comparable Results",
      "",
      f"Generated: {datetime.now(timezone.utc).isoformat()}",
      "",
      "Published Hadoop values and observed Spark values are shown separately. Absolute wall-clock",
      "numbers cannot be compared as algorithmic speedups across different hardware/runtime setups.",
      "",
      "## Published ICCIT Hadoop Reference",
      "",
      "| Dataset | Baseline ms | AES-only ms | DSCP-only ms | AES+DSCP ms | Full reduction |",
      "|---|---:|---:|---:|---:|---:|",
      f"| Synthetic | {PAPER['smartphone']['baseline']:,} | {PAPER['smartphone']['aes-only']:,} | {PAPER['smartphone']['dscp-only']:,} | {PAPER['smartphone']['aes-dscp']:,} | 34.2% |",
      f"| Real | {PAPER['road-full']['baseline']:,} | {PAPER['road-full']['aes-only']:,} | {PAPER['road-full']['dscp-only']:,} | {PAPER['road-full']['aes-dscp']:,} | 24.7% |",
      "",
      "## Observed Spark Treatment Matrix",
      "",
      "| Dataset | Suite | Baseline ms | AES-only reduction | DSCP-only reduction | AES+DSCP reduction |",
      "|---|---|---:|---:|---:|---:|",
  ]
  for profile, document in documents.items():
    values = document["values"]
    label = "Synthetic smartphone" if profile == "smartphone" else "Bangladesh road"
    lines.append(
        f"| {label} | `{document['suite']}` | {values['baseline']:,} | "
        f"{reduction(values, 'aes-only'):.2f}% | {reduction(values, 'dscp-only'):.2f}% | "
        f"{reduction(values, 'aes-dscp'):.2f}% |")
  lines.extend([
      "",
      "## ICCIT Reference, Spark Baseline, And Spark Upgrade",
      "",
      "The `Spark indexed baseline` column is the implemented Rai-Lian-style distributed aR-tree",
      "treatment executed through Spark without the ICCIT AES/DSCP extensions. The published ICCIT",
      "figures were measured under Hadoop on different hardware and are reference values, not a",
      "same-machine engine speed comparison.",
      "",
      "| Dataset | Published ICCIT Hadoop baseline | Published ICCIT Hadoop AES+DSCP | Spark indexed baseline | Spark AES+DSCP upgrade | Within-Spark reduction |",
      "|---|---:|---:|---:|---:|---:|",
  ])
  for profile, document in documents.items():
    values = document["values"]
    reference = PAPER[profile]
    label = "Synthetic smartphone" if profile == "smartphone" else "Bangladesh road"
    lines.append(
        f"| {label} | {reference['baseline']:,} ms | {reference['aes-dscp']:,} ms | "
        f"{values['baseline']:,} ms | {values['aes-dscp']:,} ms | "
        f"{reduction(values, 'aes-dscp'):.2f}% |")
  lines.extend([
      "",
      "## Figure Artifacts",
      "",
      "- `observed-spark-baseline-proposed.svg`: observed baseline/proposed comparison.",
  ])
  for document in documents.values():
    lines.append(f"- `{document['suite']}-ablation.svg`: observed four-treatment ablation chart.")
  (output / "paper-comparable-results.md").write_text("\n".join(lines) + "\n")
  (output / "paper-comparable-results.json").write_text(json.dumps({
      "generatedUtc": datetime.now(timezone.utc).isoformat(),
      "publishedReference": PAPER,
      "observed": {
          name: {"suite": document["suite"], "values": document["values"]}
          for name, document in documents.items()
      },
  }, indent=2) + "\n")
  print(f"paperFigures={output}")


if __name__ == "__main__":
  main()
