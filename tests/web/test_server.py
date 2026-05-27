import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from web import server


def write_run(root, run_id, partitions=2, elapsed=100, exact=True):
  path = root / run_id
  path.mkdir()
  manifest = {
      "runId": run_id,
      "createdUtc": "2026-05-26T00:00:00+00:00",
      "mode": "csv",
      "parameters": {"dataset": "csv", "k": "2", "partitions": str(partitions), "seed": "42"},
      "dataset": {"sha256": "fixture"},
      "artifacts": ["manifest.json", "metrics.json"],
  }
  metrics = {
      "spark": {
          "source": "simulator",
          "dataset": "csv",
          "algorithm": "aes-dscp",
          "boundMode": "ddr-mbr-full-possible",
          "k": 2,
          "partitions": partitions,
          "algorithmElapsedMs": elapsed,
          "validationMs": 2,
          "rawEvents": 12,
          "avgPruneRatio": 0.25,
          "queries": [],
          "structuredStreamingKafka": False,
      },
      "validation": {"exactTopKAgreement": exact, "queriesChecked": 2},
  }
  (path / "manifest.json").write_text(json.dumps(manifest))
  (path / "metrics.json").write_text(json.dumps(metrics))


class ResearchApiTest(unittest.TestCase):
  def test_csv_profile_reads_fixture_and_missing_values(self):
    profile = server.csv_profile()
    self.assertEqual(12, profile["records"])
    self.assertEqual(2, profile["queries"])
    self.assertGreater(profile["missingAttributeValues"], 0)

  def test_csv_profile_rejects_path_outside_project(self):
    with self.assertRaises(ValueError):
      server.csv_profile("/tmp/outside-research-data.csv")

  def test_csv_profile_audits_paper_probability_fixture(self):
    profile = server.csv_profile("tests/fixtures/paper/smartphone-paper-small.csv")
    self.assertTrue(profile["paperStyle"])
    self.assertTrue(profile["probabilityAudit"]["passed"])
    self.assertEqual(0, profile["probabilityAudit"]["normalizationErrors"])

  def test_run_comparison_flags_partition_change(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      write_run(root, "a", partitions=2, elapsed=200)
      write_run(root, "b", partitions=3, elapsed=100)
      with patch.object(server, "RUN_ROOT", root):
        report = server.comparison(["a", "b"])
      self.assertFalse(report["fair"])
      self.assertIn("parameters.partitions", [item["field"] for item in report["differences"]])
      self.assertEqual(50.0, report["runs"][1]["reductionVsFirstPct"])

  def test_dashboard_selects_fastest_exact_run(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      write_run(root, "slow", elapsed=210)
      write_run(root, "fast", elapsed=90)
      write_run(root, "invalid", elapsed=10, exact=False)
      with patch.object(server, "RUN_ROOT", root):
        document = server.dashboard()
      self.assertEqual("fast", document["fastestExactRun"]["id"])
      self.assertEqual(2, document["counts"]["exactRuns"])

  def test_saved_run_exposes_recorded_bound_mode(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      write_run(root, "bounded")
      with patch.object(server, "RUN_ROOT", root):
        run = server.load_run("bounded")
      self.assertEqual("ddr-mbr-full-possible", run["summary"]["boundMode"])

  def test_bundle_includes_stored_artifacts(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      write_run(root, "run-1")
      with patch.object(server, "RUN_ROOT", root):
        packed = server.bundle("run-1")
      self.assertIn(b"manifest.json", packed)

  def test_stream_launch_writes_e2e_output_under_web_job_directory(self):
    captured = {}

    class CompletedProcess:
      def wait(self):
        return 0

    def fake_popen(command, **kwargs):
      captured["command"] = command
      captured["env"] = kwargs["env"]
      return CompletedProcess()

    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      run_root = root / "runs"
      job_root = root / "jobs"
      run_root.mkdir()
      with (
          patch.object(server, "RUN_ROOT", run_root),
          patch.object(server, "JOB_ROOT", job_root),
          patch.object(server.subprocess, "Popen", side_effect=fake_popen),
      ):
        job = server.launch_job({
            "mode": "stream",
            "runId": "stream-web-test",
            "objects": 2,
            "queries": 1,
            "dimensions": 2,
            "k": 1,
            "partitions": 1,
        })
        time.sleep(0.01)
      self.assertEqual(
          str(job_root / job["jobId"] / "e2e"),
          captured["env"]["E2E_REPORT_DIR"])
      self.assertEqual("aes-dscp", captured["env"]["ALGORITHM"])

  def test_launch_rejects_unknown_algorithm(self):
    with tempfile.TemporaryDirectory() as folder:
      with patch.object(server, "RUN_ROOT", Path(folder)):
        with self.assertRaisesRegex(ValueError, "algorithm must be one of"):
          server.launch_job({"mode": "csv", "runId": "bad-algorithm", "algorithm": "unknown"})

  def test_default_experiment_matrix_includes_four_treatments(self):
    matrix = server.experiment_matrix()
    self.assertEqual(4, len(matrix["parameters"]["algorithms"]))
    self.assertGreater(matrix["runCount"], 0)

  def test_latex_report_uses_observed_runs(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      write_run(root, "report-run", elapsed=90)
      with patch.object(server, "RUN_ROOT", root):
        report = server.latex_report(["report-run"])
      self.assertIn("aes-dscp", report)
      self.assertIn("\\begin{tabular}", report)

  def test_paper_figures_exposes_only_generated_svgs(self):
    with tempfile.TemporaryDirectory() as folder:
      root = Path(folder)
      (root / "observed.svg").write_text("<svg/>")
      (root / "notes.md").write_text("not an image")
      with patch.object(server, "FIGURE_ROOT", root):
        figures = server.paper_figures()
      self.assertEqual([{"name": "observed.svg", "url": "/api/reports/figures/observed.svg"}], figures)


if __name__ == "__main__":
  unittest.main()
