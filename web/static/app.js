const state = { dashboard: null, runs: [], csv: null, jobs: [] };

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
  { name: "Spark certified pruning", status: "Implemented", className: "teal", detail: "Distributed bounds, candidate pruning and exact refinement validated against the oracle.", runtime: "Apache Spark / exact validation" },
  { name: "Brute-force exact oracle", status: "Implemented", className: "teal", detail: "Correctness reference executed for finite CSV and streaming profiles.", runtime: "Local Java benchmark oracle" },
  { name: "Baseline distributed PTD", status: "Pending", detail: "Required as the explicit no-AES/no-DSCP paper comparison variant.", runtime: "Paper reproduction work" },
  { name: "DSCP-only", status: "Pending", detail: "Required to isolate pruning contribution and false-prune evidence.", runtime: "Paper reproduction work" },
  { name: "AES-only", status: "Pending", detail: "Required to measure aggregated emission reduction independently.", runtime: "Paper reproduction work" },
  { name: "AES + DSCP", status: "Pending", detail: "Required named full-method variant for faithful ablation reporting.", runtime: "Paper reproduction work" }
];

const stages = [
  { name: "Input data", label: "CSV or MQTT", detail: "A run consumes either normalized CSV records or simulator events published through MQTT and bridged into Kafka.", metrics: [["CSV rows", "Dataset inspector"], ["MQTT events", "E2E summary"], ["Data checksum", "Run manifest"]] },
  { name: "Kafka ingestion", label: "Structured Streaming", detail: "Finite stream runs use Spark Structured Streaming with an AvailableNow trigger so every saved benchmark ranks a fixed snapshot.", metrics: [["Reader", "AvailableNow"], ["Evidence", "structuredStreamingKafka"], ["Source", "Kafka"]] },
  { name: "Imputation", label: "Probabilistic instances", detail: "Incomplete values are repaired into probabilistic instances before query-relative ranking.", metrics: [["Raw events", "Recorded"], ["Probabilistic instances", "Recorded"], ["Probability normalization", "Pending paper dataset"]] },
  { name: "Candidate filtering", label: "LB / UB pruning", detail: "The implemented Spark engine records candidates refined, candidates pruned, pruning ratio and the threshold tau per query.", metrics: [["Pruned objects", "Recorded"], ["Threshold tau", "Recorded"], ["Per-object trace", "Pending"]] },
  { name: "Refinement", label: "Exact top-k", detail: "Surviving candidate objects are exactly scored, then compared with the brute-force oracle when validation is enabled.", metrics: [["Algorithm elapsed", "Recorded"], ["Exact agreement", "Required"], ["Validation elapsed", "Excluded from algorithm time"]] },
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
    const [dashboard, runDocument, csv, jobs] = await Promise.all([
      api("/api/dashboard"), api("/api/runs"), api("/api/datasets/csv"), api("/api/jobs")
    ]);
    state.dashboard = dashboard;
    state.runs = runDocument.runs;
    state.csv = csv;
    state.jobs = jobs.jobs;
    renderDashboard();
    renderRunArchive();
    renderResults();
    renderArtifacts();
    renderValidation();
    renderCsv(csv);
    renderJobs();
  } catch (error) {
    toast(error.message, true);
  }
}

function runTable(runs, selection = false) {
  if (!runs.length) return '<div class="empty-state">No saved run artifacts have been generated yet.</div>';
  return `<div class="scroll-table"><table class="table"><thead><tr>
    ${selection ? "<th>Select</th>" : ""}
    <th>Run ID</th><th>Input</th><th>Algorithm time</th><th>Pruned</th><th>Validation</th>
  </tr></thead><tbody>${runs.map(run => `<tr>
    ${selection ? `<td><input class="compare-check" type="checkbox" value="${escapeHtml(run.id)}"></td>` : ""}
    <td><button class="run-link" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button></td>
    <td>${escapeHtml(run.mode)}</td>
    <td>${formatMs(run.summary.algorithmElapsedMs)}</td>
    <td>${percent(run.summary.avgPruneRatio)}</td>
    <td><span class="status-pill${run.summary.exactAgreement ? "" : " failed"}">${run.summary.exactAgreement ? "Exact" : "Not exact"}</span></td>
  </tr>`).join("")}</tbody></table></div>`;
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
    state.runs, run => run.summary.avgPruneRatio || 0, 1, value => percent(value), "teal");
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
  element.innerHTML = state.runs.length ? state.runs.map(run => `<article class="validation-card">
    <span class="status-pill${run.summary.exactAgreement ? "" : " failed"}">${run.summary.exactAgreement ? "Exact" : "Failed"}</span>
    <h4><button class="run-link" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button></h4>
    <p>${run.summary.queries} queries; ${run.metrics.validation.queriesChecked} recorded agreement checks.</p>
    <p>Validation overhead: ${formatMs(run.summary.validationMs)}</p>
  </article>`).join("") : '<div class="empty-state">Run a validation profile to populate this center.</div>';
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
      ${runTable(result.runs.map(run => ({id: run.id, mode: run.source, summary: {algorithmElapsedMs: run.algorithmElapsedMs, avgPruneRatio: run.avgPruneRatio, exactAgreement: run.exactAgreement}})))}
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
      <div><label>Validation elapsed</label><strong>${formatMs(spark.validationMs)}</strong></div>
      <div><label>Raw events</label><strong>${spark.rawEvents ?? "n/a"}</strong></div>
      <div><label>Avg pruning</label><strong>${percent(spark.avgPruneRatio)}</strong></div>
    </div>
    <div class="detail-section"><h4>Recorded configuration</h4>
      ${Object.entries(run.manifest.parameters).map(([name, value]) => `<div class="metric-row"><span>${escapeHtml(name)}</span><strong>${escapeHtml(value)}</strong></div>`).join("")}
    </div>
    <div class="detail-section"><h4>Query metrics</h4>
      <table class="table"><thead><tr><th>Query</th><th>Pruned</th><th>tau</th><th>Records</th></tr></thead><tbody>
      ${spark.queries.map(query => `<tr><td>${escapeHtml(query.queryId)}</td><td>${query.pruned} / ${query.objects}</td><td>${escapeHtml(query.tau)}</td><td>${query.compactShuffleRecords}</td></tr>`).join("")}
      </tbody></table>
    </div>
    <div class="detail-section"><h4>Validation</h4><p class="callout ${run.summary.exactAgreement ? "info" : "warning"}">
      Exact top-k agreement: ${escapeHtml(run.summary.exactAgreement)}. Queries checked: ${escapeHtml(run.metrics.validation.queriesChecked)}.
    </p></div>
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
  values.buildImage = form.querySelector("[name=buildImage]").checked;
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
  document.getElementById("close-drawer").addEventListener("click", closeDrawer);
  document.getElementById("overlay").addEventListener("click", closeDrawer);
  renderAlgorithms();
  renderPipeline();
  refreshAll();
});
