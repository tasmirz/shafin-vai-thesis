#!/usr/bin/env python3
"""Generate a publication-formatted PTD performance and consistency report."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ROOT = ROOT / "reports" / "runs"
OUTPUT_ROOT = ROOT / "reports" / "publication"
VARIANTS = ("baseline", "aes-only", "dscp-only", "aes-dscp")
LABELS = {
    "baseline": "Spark indexed baseline",
    "aes-only": "AES-only",
    "dscp-only": "DSCP-only",
    "aes-dscp": "AES + DSCP",
}
PUBLISHED = {
    "smartphone": {
        "label": "Synthetic smartphone",
        "baseline": 66520,
        "aes-only": 44760,
        "dscp-only": 58484,
        "aes-dscp": 43757,
        "act_cc": "1.4179 x 10^11",
        "full_reduction": 34.2,
    },
    "road": {
        "label": "Bangladesh OSM road",
        "baseline": 56274,
        "aes-only": 42967,
        "dscp-only": 46848,
        "aes-dscp": 42366,
        "act_cc": "1.543 x 10^10",
        "full_reduction": 24.7,
    },
}


def load_suite(suite: str) -> dict[str, dict]:
  output: dict[str, dict] = {}
  for variant in VARIANTS:
    directory = RUN_ROOT / f"{suite}-{variant}"
    output[variant] = {
        "spark": json.loads((directory / "metrics.json").read_text())["spark"],
        "manifest": json.loads((directory / "manifest.json").read_text()),
    }
  return output


def percent_reduction(baseline: int, observed: int) -> float:
  return (baseline - observed) / baseline * 100.0 if baseline else 0.0


def metric_reduction(suite: dict[str, dict], field: str) -> float:
  baseline = suite["baseline"]["spark"][field]
  full = suite["aes-dscp"]["spark"][field]
  return percent_reduction(baseline, full)


def run_time_reduction(suite: dict[str, dict], variant: str) -> float:
  baseline = suite["baseline"]["spark"]["algorithmElapsedMs"]
  value = suite[variant]["spark"]["algorithmElapsedMs"]
  return percent_reduction(baseline, value)


def suite_fairness(suite: dict[str, dict]) -> list[str]:
  keys = ("k", "partitions")
  issues = []
  baseline_spark = suite["baseline"]["spark"]
  baseline_manifest = suite["baseline"]["manifest"]
  baseline_hash = baseline_manifest["dataset"]["sha256"]
  baseline_query_hash = baseline_manifest.get("querySet", {}).get("sha256")
  for variant in VARIANTS[1:]:
    spark = suite[variant]["spark"]
    manifest = suite[variant]["manifest"]
    for key in keys:
      if spark[key] != baseline_spark[key]:
        issues.append(f"{variant} uses a different `{key}` than baseline")
    if manifest["dataset"]["sha256"] != baseline_hash:
      issues.append(f"{variant} uses a different dataset checksum than baseline")
    if manifest.get("querySet", {}).get("sha256") != baseline_query_hash:
      issues.append(f"{variant} uses a different query-set checksum than baseline")
  return issues


def observed_row(profile: str, suite_id: str, suite: dict[str, dict]) -> dict:
  baseline = suite["baseline"]["spark"]
  full = suite["aes-dscp"]["spark"]
  query_count = len(baseline["queries"])
  objects = baseline["queries"][0]["objects"] if baseline["queries"] else 0
  return {
      "profile": profile,
      "dataset": PUBLISHED[profile]["label"],
      "suite_id": suite_id,
      "objects_per_query": objects,
      "instances": baseline["probabilisticInstances"],
      "queries": query_count,
      "k": baseline["k"],
      "partitions": baseline["partitions"],
      "baseline_ms": baseline["algorithmElapsedMs"],
      "aes_only_ms": suite["aes-only"]["spark"]["algorithmElapsedMs"],
      "dscp_only_ms": suite["dscp-only"]["spark"]["algorithmElapsedMs"],
      "full_ms": full["algorithmElapsedMs"],
      "aes_only_reduction": run_time_reduction(suite, "aes-only"),
      "dscp_only_reduction": run_time_reduction(suite, "dscp-only"),
      "full_reduction": run_time_reduction(suite, "aes-dscp"),
      "emission_reduction": metric_reduction(suite, "totalEmittedRecords"),
      "shuffle_reduction": metric_reduction(suite, "totalShuffleBytes"),
      "baseline_filtered": baseline["avgPruneRatio"] * 100,
      "full_filtered": full["avgPruneRatio"] * 100,
      "baseline_emissions": baseline["totalEmittedRecords"],
      "full_emissions": full["totalEmittedRecords"],
      "baseline_shuffle": baseline["totalShuffleBytes"],
      "full_shuffle": full["totalShuffleBytes"],
      "validation_performed": all(
          run["spark"]["queries"]
          and all(query.get("validationPerformed", False) for query in run["spark"]["queries"])
          for run in suite.values()),
  }


def write_csv(output: Path, rows: list[dict]) -> None:
  fields = list(rows[0].keys())
  with (output / "spark-treatment-matrix.csv").open("w", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)


def write_latex(output: Path, rows: list[dict]) -> None:
  lines = [
      r"\begin{table}[t]",
      r"\centering",
      r"\caption{Observed same-machine Spark treatment results. Times are algorithm elapsed time.}",
      r"\label{tab:spark-treatment-results}",
      r"\begin{tabular}{lrrrr}",
      r"\hline",
      r"Dataset & Baseline (ms) & AES-only & DSCP-only & AES+DSCP \\",
      r"\hline",
  ]
  for row in rows:
    lines.append(
        f"{row['dataset']} & {row['baseline_ms']:,} & "
        f"{row['aes_only_ms']:,} ({row['aes_only_reduction']:.2f}\\%) & "
        f"{row['dscp_only_ms']:,} ({row['dscp_only_reduction']:.2f}\\%) & "
        f"{row['full_ms']:,} ({row['full_reduction']:.2f}\\%) \\\\")
  lines.extend([r"\hline", r"\end{tabular}", r"\end{table}", ""])
  (output / "spark-treatment-table.tex").write_text("\n".join(lines))


def write_markdown(output: Path, rows: list[dict], suites: dict[str, dict[str, dict]]) -> None:
  created = datetime.now(timezone.utc).isoformat()
  smartphone, road = rows
  road_query_warning = road["queries"] != 20
  road_protocol = (
      "fixed 20-query set" if road["queries"] == 20
      else f"stored {road['queries']}-query input")
  outstanding_experiments = []
  if road_query_warning:
    outstanding_experiments.append(
        "Run the full Bangladesh OSM suite with a saved, fixed set of 20 query points.")
  outstanding_experiments.extend([
      "Repeat treatments sufficiently for variance/confidence interval reporting on the same machine.",
      "Run validation on tractable matched subsets and define the full-scale correctness audit protocol.",
      "Implement and execute equivalent Hadoop PTD treatments on the same input artifacts before comparing engines.",
  ])
  lines = [
      "# Performance Improvement and Experimental Consistency Report",
      "",
      f"Generated: {created}",
      "",
      "## Scope And Claim Boundary",
      "",
      "This report evaluates the implemented Spark extension against an indexed Spark control on",
      "the same machine and stored inputs. It does not report Hadoop-to-Spark speedup. The ICCIT",
      "numbers below are published Hadoop reference values; Rai and Lian's baseline was also a",
      "Hadoop/MapReduce study executed on different hardware and, for real data, California roads.",
      "",
      "The Spark control is the implemented Rai-Lian-style distributed aggregate R-tree treatment",
      "with selected exported levels and partial-MBR reducer traversal, with AES and DSCP disabled.",
      "The Spark upgrade enables the ICCIT AES and DSCP extensions on that same indexed path.",
      "",
      "## Source-Paper Experimental Reference",
      "",
      "| Dataset | ICCIT Hadoop baseline | ICCIT Hadoop AES-only | ICCIT Hadoop DSCP-only | ICCIT Hadoop AES+DSCP | Published full reduction | act_CC |",
      "|---|---:|---:|---:|---:|---:|---:|",
  ]
  for published in PUBLISHED.values():
    lines.append(
        f"| {published['label']} | {published['baseline']:,} ms | "
        f"{published['aes-only']:,} ms | {published['dscp-only']:,} ms | "
        f"{published['aes-dscp']:,} ms | {published['full_reduction']:.1f}% | "
        f"{published['act_cc']} |")
  lines.extend([
      "",
      "ICCIT states that its measurements use pseudo-distributed Hadoop on Windows 11 with an",
      "Intel Core i5-8265U, 8 GB RAM, Java 1.8, and 20 random queries. Rai and Lian evaluate",
      "distributed PTD on ten Dell PowerEdge R730 servers using Java 1.8 and Hadoop 1.2.1,",
      "with California road MBRs and synthetic uniform/Gaussian/Zipf distributions. ICCIT's",
      "real-data label refers to its Bangladesh road dataset.",
      "",
      "## Current Spark Experimental Protocol",
      "",
      "| Input | Suite ID | Objects/query | Instances/events | Queries | k | Partitions | Control | Upgrade |",
      "|---|---|---:|---:|---:|---:|---:|---|---|",
  ])
  for row in rows:
    lines.append(
        f"| {row['dataset']} | `{row['suite_id']}` | {row['objects_per_query']:,} | "
        f"{row['instances']:,} | {row['queries']} | {row['k']} | {row['partitions']} | "
        "`baseline` | `aes-dscp` |")
  lines.extend([
      "",
      "All four variants in each saved Spark suite use the same CSV checksum, `k`, and partition",
      "count. `algorithmElapsedMs` is filtering plus emission plus refinement time; index/data",
      "setup and optional exact-oracle validation are recorded separately.",
      "",
      "## Observed Same-Machine Spark Performance",
      "",
      "| Dataset | Spark indexed baseline | AES-only | DSCP-only | AES+DSCP | AES+DSCP reduction |",
      "|---|---:|---:|---:|---:|---:|",
  ])
  for row in rows:
    lines.append(
        f"| {row['dataset']} | {row['baseline_ms']:,} ms | "
        f"{row['aes_only_ms']:,} ms ({row['aes_only_reduction']:.2f}%) | "
        f"{row['dscp_only_ms']:,} ms ({row['dscp_only_reduction']:.2f}%) | "
        f"{row['full_ms']:,} ms ({row['full_reduction']:.2f}%) | "
        f"{row['full_reduction']:.2f}% |")
  lines.extend([
      "",
      "| Dataset | Emitted records: baseline to full | Emission reduction | Shuffle bytes: baseline to full | Shuffle reduction | Indexed filtered: baseline to full |",
      "|---|---:|---:|---:|---:|---:|",
  ])
  for row in rows:
    lines.append(
        f"| {row['dataset']} | {row['baseline_emissions']:,} to {row['full_emissions']:,} | "
        f"{row['emission_reduction']:.2f}% | {row['baseline_shuffle']:,} to "
        f"{row['full_shuffle']:,} | {row['shuffle_reduction']:.2f}% | "
        f"{row['baseline_filtered']:.2f}% to {row['full_filtered']:.2f}% |")
  lines.extend([
      "",
      "### Interpretation",
      "",
      f"- Bangladesh OSM: AES+DSCP reduces measured Spark algorithm time by "
      f"`{road['full_reduction']:.2f}%`, emissions by `{road['emission_reduction']:.2f}%`, "
      f"and shuffle bytes by `{road['shuffle_reduction']:.2f}%` against the indexed Spark control.",
      f"- Synthetic smartphone: AES+DSCP reduces time by `{smartphone['full_reduction']:.2f}%`; "
      f"AES is beneficial, while DSCP-only is `{abs(smartphone['dscp_only_reduction']):.2f}%` "
      "slower because it filters only a small fraction of indexed candidates on this input.",
      "- The road reduction is similar in scale to the ICCIT improvement, but it is not a",
      "  reproduction of ICCIT runtime because the executor, data provenance, and protocol differ.",
      "",
      "## Consistency Audit",
      "",
      "| Severity | Finding | Publication handling |",
      "|---|---|---|",
      "| High | ICCIT describes AES as collapsing redundant emissions and defines AER, but also states that baseline and proposed emitted the same number of records and leaves `act_CC` unchanged. | Report actual emitted records and shuffle bytes for the Spark implementation; do not claim reproduction of the ICCIT communication result. |",
      "| High | Rai-Lian uses California road MBRs; ICCIT and the primary implementation report use Bangladesh road geometries. A supplemental supplied California/TIGER artifact matches the 98,451-object scale but is not established as Rai-Lian's exact source file. | Label the primary result `Bangladesh OSM road`; label California/TIGER results as supplemental scale-matched evidence only. |",
      "| High | The published methods execute Hadoop MapReduce under different hardware conditions; the implemented treatments execute Spark `local[4]`. | Make only within-Spark treatment claims until genuine Hadoop PTD jobs run on identical inputs and hardware. |",
  ])
  if road_query_warning:
    lines.append(
        f"| High | The current Bangladesh full-scale suite contains `{road['queries']}` query, "
        "whereas both source papers report averages over `20` random queries. | Treat road "
        "performance as full-scale preliminary evidence; execute a fixed 20-query road suite "
        "before using it as the final paper result. |")
  lines.extend([
      "| Medium | Rai-Lian selects distributed aR-tree levels through an historical/uniform-query cost model; this Spark implementation selects levels through deterministic bounded probes and reducer traversal estimates. | Describe it as a Rai-Lian-style Spark adaptation, not a byte-for-byte reproduction. |",
      "| Medium | Full-scale performance runs set exact-oracle validation off; exact agreement and zero false-prune evidence comes from validation-enabled finite OSM suites. | Do not claim full-scale exactness until validation or an auditable sampled-validation protocol is run. |",
      "",
      "## Implementation Alignment Evidence",
      "",
      "| Paper mechanism | Implemented evidence | Status |",
      "|---|---|---|",
      "| Partition aggregate R-trees and distributed summaries | `AggregateRTree.build(...)`, `summaryOnly()`; Spark broadcasts summaries and keeps full reducer indexes keyed by partition. | Implemented adaptation |",
      "| Selected exported aR-tree level | `selectExportLevel(...)` evaluates exported-node, partial-reference, and traversal costs. | Implemented with deterministic probe calibration |",
      "| Partial-MBR reducer traversal | Spark joins partial work against reducer indexes and computes exact partial contribution. | Implemented |",
      "| Rai-Lian-style control without extensions | `baseline` uses indexed path with AES=false and DSCP=false. | Implemented |",
      "| ICCIT ablations | `aes-only`, `dscp-only`, and `aes-dscp` are saved comparable treatments. | Implemented |",
      "| Paper-scale real input | Bangladesh OSM curated artifact contains 98,451 uncertain MBR objects. | Implemented for Bangladesh, not California |",
      "| Hadoop engine reproduction | PTD MapReduce job implementing the same treatments on identical artifacts. | Not implemented |",
      "",
      "## Validation Evidence",
      "",
      "The performance suites above do not run the brute-force oracle. Exactness evidence currently",
      "comes from validation-enabled OSM runs:",
      "",
      "- `osm-str-packed-exact-20260527T072842Z`: AES+DSCP on 256 objects, exact agreement true, zero false prunes.",
      "- `osm-str-exact-suite-20260527T074100Z-*`: all four variants on the finite OSM fixture, exact agreement true, zero false prunes.",
      "- `paper-setup-role-exact-20260527T092000Z-*`: named Spark indexed baseline versus AES+DSCP, exact agreement true, zero false prunes.",
      "",
      "## Publication Readiness Decision",
      "",
      "**Publication-formatted preliminary Spark extension result:** supported.",
      "",
      "**Paper-number reproduction or Hadoop-vs-Spark speedup claim:** not yet supported.",
      "",
      "Required experiments before a final comparison claim:",
      "",
      *[f"{index}. {value}" for index, value in enumerate(outstanding_experiments, start=1)],
      "",
      "## Manuscript-Ready Results Paragraph",
      "",
      f"On the curated Spark smartphone workload (750 uncertain objects represented by "
      f"207,860 probabilistic instances over 20 fixed queries), the indexed control completed in "
      f"{smartphone['baseline_ms']:,} ms, while AES+DSCP completed in {smartphone['full_ms']:,} ms, "
      f"a {smartphone['full_reduction']:.2f}% reduction. On the 98,451-object Bangladesh OSM "
      f"road workload, the indexed control completed in {road['baseline_ms']:,} ms and AES+DSCP "
      f"completed in {road['full_ms']:,} ms, a {road['full_reduction']:.2f}% reduction for the "
      f"{road_protocol}. On road data, AES+DSCP also reduced emitted records by "
      f"{road['emission_reduction']:.2f}% and measured Spark shuffle bytes by "
      f"{road['shuffle_reduction']:.2f}%. These values quantify within-Spark algorithm treatment "
      "effects and are not direct runtime comparisons with the published Hadoop studies.",
      "",
      "## Generated Artifacts",
      "",
      "- `reports/publication/spark-treatment-matrix.csv`: machine-readable observed table.",
      "- `reports/publication/spark-treatment-table.tex`: LaTeX treatment result table.",
      "- `reports/figures/observed-spark-baseline-proposed.svg`: two-dataset Spark comparison figure.",
      f"- `reports/figures/{smartphone['suite_id']}-ablation.svg`: smartphone ablation figure.",
      f"- `reports/figures/{road['suite_id']}-ablation.svg`: road ablation figure.",
      "",
      "## Paper Sources",
      "",
      "- `papers/An_Efficient_Distributed_Framework_for_Top-k_Dominating_Query_Over_Uncertain_Databases.pdf`, Sections V-A through V-D, Tables II-III.",
      "- `papers/s10115-023-01917-3(Target Paper).pdf`, Sections 5 and 7, Table 2 and Figures 7-13.",
      "",
  ])
  (output / "performance-improvement-report.md").write_text("\n".join(lines))


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--smartphone-suite", default="iccit-smartphone-str-20260527T073310Z")
  parser.add_argument("--road-suite", default="iccit-road-full-20q-20260527T094500Z")
  parser.add_argument("--output-dir", default=str(OUTPUT_ROOT))
  args = parser.parse_args()

  suites = {
      "smartphone": load_suite(args.smartphone_suite),
      "road": load_suite(args.road_suite),
  }
  fairness_issues = {
      profile: suite_fairness(suite)
      for profile, suite in suites.items()
  }
  if any(fairness_issues.values()):
    raise SystemExit(f"Incomparable treatments in selected suites: {fairness_issues}")

  rows = [
      observed_row("smartphone", args.smartphone_suite, suites["smartphone"]),
      observed_row("road", args.road_suite, suites["road"]),
  ]
  output = Path(args.output_dir)
  output.mkdir(parents=True, exist_ok=True)
  write_csv(output, rows)
  write_latex(output, rows)
  write_markdown(output, rows, suites)
  (output / "performance-improvement-report.json").write_text(json.dumps({
      "generatedUtc": datetime.now(timezone.utc).isoformat(),
      "sources": [
          "papers/An_Efficient_Distributed_Framework_for_Top-k_Dominating_Query_Over_Uncertain_Databases.pdf",
          "papers/s10115-023-01917-3(Target Paper).pdf",
      ],
      "publishedReference": PUBLISHED,
      "sameMachineSparkResults": rows,
      "fairnessIssues": fairness_issues,
      "claimBoundary": "same-machine Spark treatment comparison; published Hadoop results are reference only",
  }, indent=2, allow_nan=False) + "\n")
  print(f"publicationReport={output / 'performance-improvement-report.md'}")


if __name__ == "__main__":
  main()
