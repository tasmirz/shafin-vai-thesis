const state = {
  dashboard: null,
  runs: [],
  csv: null,
  jobs: [],
  paperDatasets: [],
  osm: null,
  matrix: null,
  figures: [],
  allDatasetReport: null,
  hadoopReference: null
};

const titles = {
  dashboard: "Research Dashboard",
  launcher: "Benchmark Launcher",
  datasets: "Dataset Inspector",
  algorithms: "Algorithm Registry",
  pipeline: "Pipeline Debugger",
  runs: "Runs & Comparison",
  validation: "Validation Center",
  results: "Result Analytics",
  artifacts: "Reproducibility Artifacts"
};

const algorithms = [
  { name: "Spark PTD variant engine", status: "Implemented", className: "teal", detail: "Treatment registry, executed emission stages and exact refinement validated against the oracle.", runtime: "Apache Spark / exact validation" },
  { name: "Brute-force exact oracle", status: "Implemented", className: "teal", detail: "Correctness reference executed for finite CSV and streaming profiles.", runtime: "Local Java benchmark oracle" },
  { name: "Baseline distributed PTD", status: "Implemented", className: "teal", detail: "Rai-Lian aggregate-R-tree score-bound treatment without AES/DSCP extensions; emits expanded instance-competitor records.", runtime: "--algorithm=baseline" },
  { name: "DSCP-only", status: "Implemented", className: "teal", detail: "Threshold pruning with expanded emissions and recorded false-prune evidence.", runtime: "--algorithm=dscp-only" },
  { name: "AES-only", status: "Implemented", className: "teal", detail: "Rai-Lian indexed candidate filtering with aggregated emissions, without the DSCP extension.", runtime: "--algorithm=aes-only" },
  { name: "AES + DSCP", status: "Implemented", className: "teal", detail: "Full named method with pruning and aggregated emissions.", runtime: "--algorithm=aes-dscp" }
];

const stages = [
  { name: "Input data", label: "CSV or MQTT", detail: "A run consumes either normalized CSV records or simulator events published through MQTT and bridged into Kafka.", metrics: [["CSV rows", "Dataset inspector"], ["MQTT events", "E2E summary"], ["Data checksum", "Run manifest"]] },
  { name: "Kafka ingestion", label: "Structured Streaming", detail: "Finite stream runs use Spark Structured Streaming with an AvailableNow trigger so every saved benchmark ranks a fixed snapshot.", metrics: [["Reader", "AvailableNow"], ["Evidence", "structuredStreamingKafka"], ["Source", "Kafka"]] },
  { name: "Imputation", label: "Probabilistic instances", detail: "Incomplete values are repaired, while curated paper instances retain supplied normalized appearance probability.", metrics: [["Raw events", "Recorded"], ["Probabilistic instances", "Recorded"], ["Probability normalization", "Audited"]] },
  { name: "Candidate filtering", label: "aR-tree + optional DSCP", detail: "Indexed variants use heap-ordered Rai-Lian score-bound traversal; named DSCP treatments apply the additional threshold filter. Exact runs audit all excluded objects.", metrics: [["Pruned objects", "Recorded"], ["Threshold tau", "DSCP"], ["False prunes", "Validated"]] },
  { name: "Emission / shuffle", label: "Optional AES", detail: "Spark materializes expanded or aggregated records and records task-level shuffle bytes and records.", metrics: [["Emitted records", "Executed"], ["AER", "Recorded"], ["Shuffle bytes", "Observed"]] },
  { name: "Refinement", label: "Exact top-k", detail: "Surviving candidate objects are exactly scored, then compared with the brute-force oracle when validation is enabled. Query phase time excludes setup and validation.", metrics: [["Algorithm elapsed", "Filter + emit + refine"], ["Exact agreement", "Required"], ["Setup/validation", "Separate"]] },
  { name: "Artifact store", label: "Immutable evidence", detail: "Completed runs store parameters, Git identity, dataset checksum, metrics and logs. Comparison checks fairness-critical parameters.", metrics: [["Manifest", "Recorded"], ["Metrics CSV/JSON", "Recorded"], ["Bundle export", "Available"]] }
];

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, character => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[character]));
}

function formatMs(value) {
  return value == null ? "n/a" : `${Number(value).toLocaleString()} ms`;
}

function percent(value) {
  return value == null ? "n/a" : `${(Number(value) * 100).toFixed(1)}%`;
}

function hasDscp(algorithm) {
  return algorithm === "dscp-only" || algorithm === "aes-dscp";
}

function pruningDisplay(algorithm, value) {
  return percent(value);
}

function validationDisplay(value) {
  if (value === true) return ["Exact", ""];
  if (value === false) return ["Incorrect", " failed"];
  return ["Not validated", " pending"];
}

async function api(path, options) {
  const response = await fetch(path, options);
  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("json") ? await response.json() : null;
  if (!response.ok) throw new Error(body?.error || `${response.status} ${response.statusText}`);
  return body;
}

function toast(message, error = false) {
  const element = document.getElementById("toast");
  element.textContent = message;
  element.className = `toast open${error ? " error" : ""}`;
  clearTimeout(element.timer);
  element.timer = setTimeout(() => element.className = "toast", 4200);
}

function openView(name) {
  document.querySelectorAll(".view").forEach(view => view.classList.remove("active"));
  document.querySelectorAll(".nav-link").forEach(link => link.classList.toggle("active", link.dataset.view === name));
  document.getElementById(`${name}-view`).classList.add("active");
  document.getElementById("view-title").textContent = titles[name];
}

async function refreshAll() {
  try {
    const [
      dashboard, runDocument, csv, jobs, datasets, osm, matrix, figures,
      allDatasetReport, hadoopReference
    ] = await Promise.all([
      api("/api/dashboard"), api("/api/runs"), api("/api/datasets/csv"), api("/api/jobs"),
      api("/api/datasets/paper"), api("/api/datasets/osm-readiness"), api("/api/experiment-matrix"),
      api("/api/reports/figures"), api("/api/reports/all-dataset"), api("/api/reports/hadoop-reference")
    ]);
    state.dashboard = dashboard;
    state.runs = runDocument.runs;
    state.csv = csv;
    state.jobs = jobs.jobs;
    state.paperDatasets = datasets.datasets;
    state.osm = osm;
    state.matrix = matrix;
    state.figures = figures.figures;
    state.allDatasetReport = allDatasetReport;
    state.hadoopReference = hadoopReference;
    renderDashboard();
    renderRunArchive();
    renderResults();
    renderArtifacts();
    renderValidation();
    renderCsv(csv);
    renderPaperDatasets();
    renderTelemetry();
    renderHadoopReferenceComparisons();
    renderAllDatasetReport();
    renderMatrix();
    renderPaperFigures();
    renderJobs();
  } catch (error) {
    toast(error.message, true);
  }
}

function runTable(runs, selection = false) {
  if (!runs.length) return '<div class="empty-state">No saved run artifacts have been generated yet.</div>';
  return `<div class="scroll-table"><table class="table"><thead><tr>
    ${selection ? "<th>Select</th>" : ""}
    <th>Run ID</th><th>Variant</th><th>Input</th><th>Algorithm time</th><th>Filtered</th><th>Validation</th>
  </tr></thead><tbody>${runs.map(run => {
    const validation = validationDisplay(run.summary.exactAgreement);
    return `<tr>
    ${selection ? `<td><input class="compare-check" type="checkbox" value="${escapeHtml(run.id)}"></td>` : ""}
    <td><button class="run-link" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button></td>
    <td>${escapeHtml(run.summary.algorithm || "legacy")}</td>
    <td>${escapeHtml(run.mode)}</td>
    <td>${formatMs(run.summary.algorithmElapsedMs)}</td>
    <td>${pruningDisplay(run.summary.algorithm, run.summary.avgPruneRatio)}</td>
    <td><span class="status-pill${validation[1]}">${validation[0]}</span></td>
  </tr>`;
  }).join("")}</tbody></table></div>`;
}

function renderDashboard() {
  const data = state.dashboard;
  document.getElementById("project-subtitle").textContent = data.project.subtitle;
  document.getElementById("pipeline-ribbon").innerHTML = data.project.pipeline
    .map((item, index) => `${index ? '<span class="pipeline-arrow">-&gt;</span>' : ""}<span class="pipeline-chip">${escapeHtml(item)}</span>`)
    .join("");
  const fastest = data.fastestExactRun?.summary.algorithmElapsedMs;
  document.getElementById("dashboard-stats").innerHTML = [
    ["Saved runs", data.counts.savedRuns, "Immutable experiment records"],
    ["Validated exact", data.counts.exactRuns, "Oracle agreement passed"],
    ["Stream runs", data.counts.streamRuns, "MQTT / Kafka / Spark"],
    ["Fastest exact", fastest == null ? "n/a" : formatMs(fastest), data.fastestExactRun?.id || "No run yet"]
  ].map(([label, value, note]) => `<div class="stat"><label>${label}</label><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></div>`).join("");
  document.getElementById("recent-runs").innerHTML = runTable(data.recentRuns);
  document.getElementById("paper-targets").innerHTML = data.paperTargets.map(target => `<article class="target">
    <header><strong>${escapeHtml(target.dataset)}</strong><span class="reduction">${target.reductionPct}%</span></header>
    <p>Paper: ${formatMs(target.baselineMs)} to ${formatMs(target.proposedMs)}</p>
    <p>${escapeHtml(target.status)}</p>
  </article>`).join("");
  list("implemented-capabilities", data.capabilities.implemented);
  list("pending-capabilities", data.capabilities.pending);
  list("dashboard-warnings", data.warnings);
  bindRunLinks();
}

function list(id, values) {
  document.getElementById(id).innerHTML = values.map(item => `<li>${escapeHtml(item)}</li>`).join("");
}

function renderRunArchive() {
  document.getElementById("run-archive").innerHTML = runTable(state.runs, true);
  bindRunLinks();
}

function renderResults() {
  const maximumTime = Math.max(1, ...state.runs.map(run => run.summary.algorithmElapsedMs || 0));
  document.getElementById("runtime-chart").innerHTML = chartRows(
    state.runs, run => run.summary.algorithmElapsedMs || 0, maximumTime, value => formatMs(value), "");
  document.getElementById("pruning-chart").innerHTML = chartRows(
    state.runs.filter(run => hasDscp(run.summary.algorithm)),
    run => run.summary.avgPruneRatio || 0, 1, value => percent(value), "teal");
}

function chartRows(runs, value, maximum, formatter, className) {
  if (!runs.length) return '<div class="empty-state">No saved result measurements.</div>';
  return runs.map(run => `<div class="bar-row">
      <button class="bar-label" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button>
      <div class="bar-track"><div class="bar-fill ${className}" style="width:${Math.max(2, value(run) / maximum * 100)}%"></div></div>
      <strong>${formatter(value(run))}</strong>
    </div>`).join("");
}

function renderArtifacts() {
  const element = document.getElementById("artifact-grid");
  element.innerHTML = state.runs.length ? state.runs.map(run => `<article class="artifact-card">
    <span class="badge ${run.mode === "stream" ? "teal" : ""}">${escapeHtml(run.mode)}</span>
    <h3>${escapeHtml(run.id)}</h3>
    <p>${escapeHtml(run.manifest.createdUtc || "Unknown creation time")}</p>
    <div class="artifact-list">${run.artifacts.map(escapeHtml).join("<br>")}</div>
    <a class="button primary compact download" href="/api/runs/${encodeURIComponent(run.id)}/bundle">Export bundle</a>
  </article>`).join("") : '<div class="empty-state">No runs are available to package.</div>';
}

function renderValidation() {
  const element = document.getElementById("validation-cards");
  element.innerHTML = state.runs.length ? state.runs.map(run => {
    const validation = validationDisplay(run.summary.exactAgreement);
    return `<article class="validation-card">
    <span class="status-pill${validation[1]}">${validation[0]}</span>
    <h4><button class="run-link" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button></h4>
    <p>${run.summary.queries} queries; ${run.metrics.validation.queriesChecked} recorded agreement checks.</p>
    <p>Validation overhead: ${formatMs(run.summary.validationMs)}</p>
  </article>`;
  }).join("") : '<div class="empty-state">Run a validation profile to populate this center.</div>';
  bindRunLinks();
}

async function loadCsv(path) {
  try {
    const data = await api(`/api/datasets/csv?path=${encodeURIComponent(path)}`);
    state.csv = data;
    renderCsv(data);
  } catch (error) {
    toast(error.message, true);
  }
}

function renderCsv(data) {
  const container = document.getElementById("csv-profile");
  container.innerHTML = `<div class="dataset-summary">
    ${[["Records", data.records], ["Objects", data.objects], ["Queries", data.queries], ["Missing values", data.missingAttributeValues]]
      .map(([label, value]) => `<div class="mini-stat"><span>${label}</span><strong>${value}</strong></div>`).join("")}
  </div>
  <section class="panel">
    <p class="eyebrow">Quality checks</p>
    <h3>${escapeHtml(data.path)}</h3>
    <div class="quality">${data.qualityChecks.map(check => `<span class="quality-item">${escapeHtml(check.name)}: ${escapeHtml(check.value)}</span>`).join("")}</div>
    <p class="callout info">${escapeHtml(data.note)}</p>
  </section>
  <section class="panel">
    <h3>Object-instance record preview</h3>
    <div class="scroll-table"><table class="table"><thead><tr>${data.columns.map(column => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead><tbody>
      ${data.preview.map(row => `<tr>${data.columns.map(column => `<td>${escapeHtml(row[column] || "-")}</td>`).join("")}</tr>`).join("")}
    </tbody></table></div>
  </section>`;
}

function renderPaperDatasets() {
  const osm = state.osm;
  document.getElementById("osm-readiness").innerHTML = `<div class="detail-stats">
    <div><label>Line features</label><strong>${Number(osm.availableLineFeatures).toLocaleString()}</strong></div>
    <div><label>Baseline minimum</label><strong>${Number(osm.minimumLineFeatures).toLocaleString()}</strong></div>
  </div><p class="callout ${osm.ready ? "info" : "warning"}">${osm.ready ? "Source is sufficient for paper-scale MBR curation." : "Source is below required scale."}</p>`;
  const target = document.getElementById("paper-datasets");
  target.innerHTML = state.paperDatasets.length ? state.paperDatasets.map(dataset => `<article class="target">
    <header><strong>${escapeHtml(dataset.dataset)}</strong><span class="status-pill">${dataset.validation.passed ? "Valid" : "Invalid"}</span></header>
    <p>${Number(dataset.objects).toLocaleString()} objects; ${Number(dataset.rows).toLocaleString()} rows; ${dataset.partitions} partitions</p>
    <button class="text-action dataset-open" data-path="${escapeHtml(dataset.csvPath)}">Inspect probability audit</button>
  </article>`).join("") : '<div class="empty-state">Generate a paper dataset to record its manifest.</div>';
  document.querySelectorAll(".dataset-open").forEach(button => button.onclick = () => loadCsv(button.dataset.path));
}

function renderTelemetry() {
  const measured = state.runs.filter(run => run.summary.shuffleBytes != null);
  document.getElementById("telemetry-summary").innerHTML = measured.length
    ? `<div class="scroll-table"><table class="table"><thead><tr><th>Run</th><th>Shuffle bytes</th><th>Shuffle records</th><th>Filter ms</th><th>Refine ms</th><th>Skew ratio</th></tr></thead><tbody>${measured.slice(0, 10).map(run => `<tr><td>${escapeHtml(run.id)}</td><td>${Number(run.summary.shuffleBytes).toLocaleString()}</td><td>${Number(run.summary.shuffleRecords).toLocaleString()}</td><td>${run.summary.filterMs}</td><td>${run.summary.refineMs}</td><td>${Number(run.summary.stragglerRatio || 0).toFixed(2)}</td></tr>`).join("")}</tbody></table></div>`
    : '<div class="empty-state">Execute a freshly instrumented run to record Spark telemetry.</div>';
}

function renderAllDatasetReport() {
  const target = document.getElementById("all-dataset-report");
  const report = state.allDatasetReport;
  if (!target || !report) return;
  if (!report.generated) {
    target.innerHTML = `<div class="empty-state">Generate the all-dataset publication report to populate this comparison.</div>
      <p class="callout warning">${escapeHtml(report.claimBoundary)}</p>`;
    return;
  }
  const treatmentRows = report.treatments.map(row => `<tr>
    <td>${escapeHtml(row.dataset)}</td>
    <td>${row.queries}</td>
    <td>${formatMs(row.baseline_ms)}</td>
    <td>${formatMs(row.aes_dscp_ms)}</td>
    <td>${Number(row.aes_dscp_reduction_pct).toFixed(2)}%</td>
    <td>${Number(row.emission_reduction_pct).toFixed(2)}%</td>
    <td>${Number(row.shuffle_reduction_pct).toFixed(2)}%</td>
  </tr>`).join("");
  const streamRows = report.streams.map(row => `<tr>
    <td>${escapeHtml(row.dataset)}</td>
    <td>${Number(row.messages).toLocaleString()}</td>
    <td>${row.queries}</td>
    <td>${formatMs(row.algorithm_ms)}</td>
    <td>${formatMs(row.pipeline_total_ms)}</td>
    <td>${escapeHtml(row.validation == null ? "not run" : row.validation)}</td>
  </tr>`).join("");
  target.innerHTML = `<p class="callout info">${escapeHtml(report.claimBoundary)}. Source: ${escapeHtml(report.reportPath)}</p>
    <div class="scroll-table"><table class="table"><thead><tr><th>Dataset</th><th>Queries</th><th>Baseline</th><th>AES+DSCP</th><th>Runtime reduction</th><th>Emission reduction</th><th>Shuffle reduction</th></tr></thead><tbody>
      ${treatmentRows || '<tr><td colspan="7">No treatment rows recorded.</td></tr>'}
    </tbody></table></div>
    <div class="scroll-table"><table class="table"><thead><tr><th>Stream dataset</th><th>Messages</th><th>Queries</th><th>Algorithm time</th><th>E2E time</th><th>Exact validation</th></tr></thead><tbody>
      ${streamRows || '<tr><td colspan="6">No stream rows recorded.</td></tr>'}
    </tbody></table></div>`;
}

function renderHadoopReferenceComparisons() {
  const target = document.getElementById("hadoop-reference-comparison");
  const report = state.hadoopReference;
  if (!target || !report) return;
  if (!report.publishedHadoop.length && !report.sparkSuites.length) {
    target.innerHTML = `<div class="empty-state">Run a four-treatment Spark ablation suite to populate this comparison.</div>
      <p class="callout warning">${escapeHtml(report.claimBoundary)}</p>`;
    return;
  }
  const hadoopTables = report.publishedHadoop.map(group => `<div class="detail-section">
    <h4>ICCIT Hadoop reference: ${escapeHtml(group.dataset)}</h4>
    <p class="subtle">act_CC: ${escapeHtml(group.actCc)}; published AES+DSCP reduction: ${Number(group.fullReductionPct).toFixed(1)}%</p>
    ${treatmentTable(group.rows)}
  </div>`).join("");
  const sparkTables = report.sparkSuites.slice(0, 6).map(suite => `<div class="detail-section">
    <h4>Spark observed suite: ${escapeHtml(suite.suiteId)}</h4>
    <p class="subtle">dataset=${escapeHtml(suite.dataset)}; k=${escapeHtml(suite.k)}; partitions=${escapeHtml(suite.partitions)}</p>
    ${treatmentTable(suite.rows)}
  </div>`).join("");
  target.innerHTML = `<p class="callout info">${escapeHtml(report.claimBoundary)} ${escapeHtml(report.aliasNote)}</p>
    ${hadoopTables}
    ${sparkTables || '<div class="empty-state">No complete Spark baseline/AES/DSCP/AES+DSCP suite is saved yet.</div>'}`;
  bindRunLinks();
}

function treatmentTable(rows) {
  return `<div class="scroll-table"><table class="table"><thead><tr>
      <th>Source</th><th>Treatment</th><th>Clock time</th><th>Reduction vs baseline</th><th>Pruning rate</th><th>Emitted</th><th>Shuffle bytes</th><th>False prunes</th><th>Exact</th>
    </tr></thead><tbody>
      ${rows.map(row => `<tr>
        <td>${escapeHtml(row.source)}</td>
        <td>${row.runId ? `<button class="run-link" data-run="${escapeHtml(row.runId)}">${escapeHtml(row.label)}</button>` : escapeHtml(row.label)}</td>
        <td>${formatMs(row.clockTimeMs)}</td>
        <td>${row.reductionVsBaselinePct == null ? "n/a" : `${Number(row.reductionVsBaselinePct).toFixed(2)}%`}</td>
        <td>${row.pruningRate == null ? "not reported" : percent(row.pruningRate)}</td>
        <td>${row.emittedRecords == null ? "not reported" : Number(row.emittedRecords).toLocaleString()}</td>
        <td>${row.shuffleBytes == null ? "not reported" : Number(row.shuffleBytes).toLocaleString()}</td>
        <td>${row.falsePrunes == null ? "not reported" : Number(row.falsePrunes).toLocaleString()}</td>
        <td>${escapeHtml(row.exactAgreement == null ? "not reported" : row.exactAgreement)}</td>
      </tr>`).join("")}
      </tbody></table></div>
    `;
}

function renderMatrix() {
  const matrix = state.matrix;
  document.getElementById("experiment-matrix").innerHTML = `<div class="mini-stat"><span>Planned executions</span><strong>${Number(matrix.runCount).toLocaleString()}</strong></div>
    <div class="quality">${matrix.templates.map(item => `<span class="quality-item">${escapeHtml(item)}</span>`).join("")}</div>
    ${matrix.warning ? `<p class="callout warning">${escapeHtml(matrix.warning)}</p>` : ""}`;
}

function renderPaperFigures() {
  const element = document.getElementById("paper-figures");
  element.innerHTML = state.figures.length ? state.figures.map(figure => `<figure class="paper-figure">
    <img src="${escapeHtml(figure.url)}" alt="${escapeHtml(figure.name)}">
    <figcaption>${escapeHtml(figure.name)}</figcaption>
  </figure>`).join("") : '<div class="empty-state">Render completed treatment suites to produce paper-style SVG figures.</div>';
}

function renderAlgorithms() {
  document.getElementById("algorithm-grid").innerHTML = algorithms.map(item => `<article class="algorithm">
    <span class="badge ${item.className || ""}">${escapeHtml(item.status)}</span>
    <h4>${escapeHtml(item.name)}</h4>
    <p>${escapeHtml(item.detail)}</p>
    <footer>${escapeHtml(item.runtime)}</footer>
  </article>`).join("");
}

function renderPipeline(active = 0) {
  document.getElementById("stage-list").innerHTML = stages.map((stage, index) => `<button class="stage ${index === active ? "active" : ""}" data-stage="${index}">
    <strong>${escapeHtml(stage.name)}</strong><span>${escapeHtml(stage.label)}</span>
  </button>`).join("");
  const stage = stages[active];
  document.getElementById("stage-detail").innerHTML = `<p class="eyebrow">Execution stage</p><h3>${escapeHtml(stage.name)}</h3>
    <p>${escapeHtml(stage.detail)}</p>
    <div class="metric-list">${stage.metrics.map(metric => `<div class="metric-row"><span>${escapeHtml(metric[0])}</span><strong>${escapeHtml(metric[1])}</strong></div>`).join("")}</div>`;
  document.querySelectorAll("[data-stage]").forEach(button => button.addEventListener("click", () => renderPipeline(Number(button.dataset.stage))));
}

function renderJobs() {
  const element = document.getElementById("job-console");
  if (!state.jobs.length) {
    element.className = "empty-state";
    element.innerHTML = "No benchmark has been launched from this server process.";
    return;
  }
  element.className = "";
  element.innerHTML = state.jobs.map(job => `<article class="job-entry">
    <div class="job-row"><div><strong>${escapeHtml(job.runId)}</strong> <span class="badge ${job.mode === "stream" ? "teal" : ""}">${escapeHtml(job.mode)}</span></div>
    <span class="status-pill ${job.status === "failed" ? "failed" : ""}">${escapeHtml(job.status)}</span></div>
    ${job.log ? `<pre>${escapeHtml(job.log)}</pre>` : ""}
  </article>`).join("");
}

async function compareSelected() {
  const ids = [...document.querySelectorAll(".compare-check:checked")].map(box => box.value);
  try {
    const result = await api(`/api/compare?ids=${encodeURIComponent(ids.join(","))}`);
    const alert = result.fair ? "comparison-good" : "comparison-warning";
    document.getElementById("comparison-panel").innerHTML = `<div class="${alert}">${escapeHtml(result.notice)}</div>
      ${runTable(result.runs.map(run => ({id: run.id, mode: run.source, summary: {algorithm: run.algorithm, algorithmElapsedMs: run.algorithmElapsedMs, avgPruneRatio: run.avgPruneRatio, exactAgreement: run.exactAgreement}})))}
      ${result.differences.map(diff => `<div class="diff"><strong>${escapeHtml(diff.label)}:</strong> ${diff.values.map(value => escapeHtml(value ?? "not recorded")).join(" versus ")}</div>`).join("")}`;
    bindRunLinks();
  } catch (error) {
    toast(error.message, true);
  }
}

async function showRun(runId) {
  try {
    const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
    const spark = run.metrics.spark;
    document.getElementById("drawer-title").textContent = run.id;
    document.getElementById("drawer-content").innerHTML = `<div class="detail-stats">
      <div><label>Algorithm elapsed</label><strong>${formatMs(spark.algorithmElapsedMs)}</strong></div>
      <div><label>Setup elapsed</label><strong>${formatMs(spark.setupMs)}</strong></div>
      <div><label>Variant</label><strong>${escapeHtml(spark.algorithm || "legacy")}</strong></div>
      <div><label>Setup role</label><strong>${escapeHtml(run.summary.setupRole || "general run")}</strong></div>
      <div><label>Validation elapsed</label><strong>${formatMs(spark.validationMs)}</strong></div>
      <div><label>Raw events</label><strong>${spark.rawEvents ?? "n/a"}</strong></div>
      <div><label>Avg candidate filtered</label><strong>${pruningDisplay(spark.algorithm, spark.avgPruneRatio)}</strong></div>
      <div><label>Avg AER</label><strong>${percent(spark.avgAER)}</strong></div>
    </div>
    <div class="detail-section"><h4>Recorded configuration</h4>
      ${Object.entries(run.manifest.parameters).map(([name, value]) => `<div class="metric-row"><span>${escapeHtml(name)}</span><strong>${escapeHtml(value)}</strong></div>`).join("")}
      <div class="metric-row"><span>Bound mode</span><strong>${escapeHtml(spark.boundMode || "not recorded")}</strong></div>
      <p class="callout info">${hasDscp(spark.algorithm)
        ? (spark.avgPruneRatio === 0 ? "No candidate was excluded under the recorded index/DSCP bounds." : "This value includes indexed baseline filtering plus the DSCP threshold. Use the paired baseline to isolate DSCP.")
        : "This value is Rai-Lian indexed candidate filtering; this variant does not execute the DSCP extension."}</p>
    </div>
    <div class="detail-section"><h4>Query metrics</h4>
      <table class="table"><thead><tr><th>Query</th><th>Filtered</th><th>tau</th><th>Emitted</th><th>AER</th><th>False prune</th></tr></thead><tbody>
      ${spark.queries.map(query => `<tr><td>${escapeHtml(query.queryId)}</td><td>${query.pruned} / ${query.objects}</td><td>${hasDscp(spark.algorithm) ? escapeHtml(query.tau) : "index frontier"}</td><td>${query.emittedRecords ?? query.compactShuffleRecords}</td><td>${percent(query.aer)}</td><td>${query.falsePrunes ?? 0}</td></tr>`).join("")}
      </tbody></table>
    </div>
    <div class="detail-section"><h4>Validation</h4><p class="callout ${run.summary.exactAgreement === true ? "info" : "warning"}">
      Exact top-k agreement: ${escapeHtml(validationDisplay(run.summary.exactAgreement)[0])}. Queries checked: ${escapeHtml(run.metrics.validation.queriesChecked)}.
    </p></div>
    <div class="detail-section"><h4>Observed Spark telemetry</h4>
      <div class="metric-row"><span>Shuffle bytes</span><strong>${Number(spark.totalShuffleBytes || 0).toLocaleString()}</strong></div>
      <div class="metric-row"><span>Shuffle records</span><strong>${Number(spark.totalShuffleRecords || 0).toLocaleString()}</strong></div>
      <div class="metric-row"><span>Filter / emission / refine ms</span><strong>${spark.totalFilterMs || 0} / ${spark.totalEmissionMs || 0} / ${spark.totalRefineMs || 0}</strong></div>
      <div class="metric-row"><span>Max straggler ratio</span><strong>${Number(spark.maxStragglerRatio || 0).toFixed(2)}</strong></div>
    </div>
    <div class="detail-section"><h4>DDR / MBR / AES trace sample</h4>
      ${spark.objectTraces?.length ? `<table class="table"><thead><tr><th>Object</th><th>LB / UB / tau</th><th>Decision</th><th>Partial MBR refs</th><th>AES / baseline</th></tr></thead><tbody>
      ${spark.objectTraces.slice(0, 25).map(trace => `<tr><td>${escapeHtml(trace.objectId)} (p${trace.partition})</td><td>${Number(trace.lb).toFixed(3)} / ${Number(trace.ub).toFixed(3)} / ${trace.tau == null ? "n/a" : Number(trace.tau).toFixed(3)}</td><td>${escapeHtml(trace.decision)}</td><td>${trace.partialMbrRefs}</td><td>${trace.aesEmissions} / ${trace.baselineEmissions}</td></tr>`).join("")}
      </tbody></table>` : `<p class="subtle">This historical run predates object-level trace capture.</p>`}
    </div>
    <div class="detail-section"><h4>Spark log excerpt</h4><pre class="log">${escapeHtml(run.logs["spark.log"] || "No Spark log stored.")}</pre></div>
    <a class="button primary download" href="/api/runs/${encodeURIComponent(run.id)}/bundle">Export evidence bundle</a>`;
    document.getElementById("detail-drawer").classList.add("open");
    document.getElementById("overlay").classList.add("open");
    document.getElementById("detail-drawer").setAttribute("aria-hidden", "false");
  } catch (error) {
    toast(error.message, true);
  }
}

function bindRunLinks() {
  document.querySelectorAll("[data-run]").forEach(button => {
    button.onclick = () => showRun(button.dataset.run);
  });
}

function formBody(form, mode) {
  const values = Object.fromEntries(new FormData(form).entries());
  values.mode = mode;
  values.buildImage = form.querySelector("[name=buildImage]")?.checked || false;
  return values;
}

async function launch(event, mode) {
  event.preventDefault();
  try {
    const job = await api("/api/jobs", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(formBody(event.currentTarget, mode))
    });
    toast(`Started ${job.runId}. Monitor its output in the job console.`);
    state.jobs = (await api("/api/jobs")).jobs;
    renderJobs();
    window.setTimeout(pollJobs, 1600);
  } catch (error) {
    toast(error.message, true);
  }
}

async function pollJobs() {
  try {
    state.jobs = (await api("/api/jobs")).jobs;
    renderJobs();
    if (state.jobs.some(job => job.status === "running")) {
      window.setTimeout(pollJobs, 2300);
    } else {
      await refreshAll();
    }
  } catch (error) {
    toast(error.message, true);
  }
}

function closeDrawer() {
  document.getElementById("detail-drawer").classList.remove("open");
  document.getElementById("overlay").classList.remove("open");
  document.getElementById("detail-drawer").setAttribute("aria-hidden", "true");
}

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".nav-link").forEach(button => button.addEventListener("click", () => openView(button.dataset.view)));
  document.querySelectorAll("[data-open-view]").forEach(button => button.addEventListener("click", () => openView(button.dataset.openView)));
  document.getElementById("refresh-button").addEventListener("click", refreshAll);
  document.getElementById("refresh-jobs").addEventListener("click", pollJobs);
  document.getElementById("inspect-csv").addEventListener("click", () => loadCsv(document.getElementById("csv-profile-path").value));
  document.getElementById("compare-selected").addEventListener("click", compareSelected);
  document.getElementById("csv-launch-form").addEventListener("submit", event => launch(event, "csv"));
  document.getElementById("stream-launch-form").addEventListener("submit", event => launch(event, "stream"));
  document.getElementById("paper-suite-launch-form").addEventListener("submit", event => launch(event, "paper-suite"));
  document.getElementById("full-compare-launch-form").addEventListener("submit", event => launch(event, "full-compare"));
  document.getElementById("hadoop-aes-dscp-test-form").addEventListener("submit", event => launch(event, "hadoop-aes-dscp-test"));
  document.getElementById("smartphone-generate-form").addEventListener("submit", event => launch(event, "smartphone"));
  document.getElementById("osm-generate-form").addEventListener("submit", event => launch(event, "osm"));
  document.getElementById("close-drawer").addEventListener("click", closeDrawer);
  document.getElementById("overlay").addEventListener("click", closeDrawer);
  renderAlgorithms();
  renderPipeline();
  refreshAll();
});
