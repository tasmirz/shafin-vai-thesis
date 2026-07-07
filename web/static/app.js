const state = {
  dashboard: null,
  runs: [],
  figures: []
};

const titles = {
  dashboard: "Research Dashboard",
  runs: "Runs & Comparison",
  visualizations: "Visual Analytics"
};

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
  const el = document.getElementById(`${name}-view`);
  if (el) el.classList.add("active");
  const titleEl = document.getElementById("view-title");
  if (titleEl) titleEl.textContent = titles[name];
}

async function refreshAll() {
  try {
    const [dashboard, runDocument, figures] = await Promise.all([
      api("/api/dashboard"), 
      api("/api/runs"), 
      api("/api/reports/figures")
    ]);
    state.dashboard = dashboard;
    state.runs = runDocument.runs;
    state.figures = figures.figures;
    
    renderDashboard();
    renderRunArchive();
    renderPaperFigures();
  } catch (error) {
    toast(error.message, true);
  }
}

function runTable(runs, selection = false) {
  if (!runs.length) return '<div class="empty-state">No saved run artifacts have been generated yet.</div>';
  return `<div class="scroll-table"><table class="table"><thead><tr>
    ${selection ? "<th>Select</th>" : ""}
    <th>Run ID</th><th>Variant</th><th>Input</th><th>Algorithm time</th><th>Filtered</th><th>Validation</th>${selection ? "<th>Actions</th>" : ""}
  </tr></thead><tbody>${runs.map(run => {
    const validation = validationDisplay(run.summary.exactAgreement);
    let suiteId = run.id.replace(/-(spark|hadoop)?-?(improved-)?(baseline|dscp-only|aes-only|aes-dscp)$/, '');
    if (suiteId.endsWith('-')) suiteId = suiteId.slice(0, -1);
    
    return `<tr>
    ${selection ? `<td><input class="compare-check" type="checkbox" value="${escapeHtml(run.id)}"></td>` : ""}
    <td><button class="run-link" data-run="${escapeHtml(run.id)}">${escapeHtml(run.id)}</button></td>
    <td>${escapeHtml(run.summary.algorithm || "legacy")}</td>
    <td>${escapeHtml(run.mode)}</td>
    <td>${formatMs(run.summary.algorithmElapsedMs)}</td>
    <td>${pruningDisplay(run.summary.algorithm, run.summary.avgPruneRatio)}</td>
    <td><span class="status-pill${validation[1]}">${validation[0]}</span></td>
    ${selection ? `<td><button class="button compact compare-suite-btn" style="padding: 0.2rem 0.5rem; font-size: 0.75rem;" data-suite="${escapeHtml(suiteId)}">Compare Suite</button></td>` : ""}
  </tr>`;
  }).join("")}</tbody></table></div>`;
}

function renderDashboard() {
  const data = state.dashboard;
  const subEl = document.getElementById("project-subtitle");
  if (subEl) subEl.textContent = data.project.subtitle;
  
  const fastest = data.fastestExactRun?.summary.algorithmElapsedMs;
  const statsEl = document.getElementById("dashboard-stats");
  if (statsEl) {
    statsEl.innerHTML = [
      ["Saved runs", data.counts.savedRuns, "Immutable experiment records"],
      ["Validated exact", data.counts.exactRuns, "Oracle agreement passed"],
      ["Stream runs", data.counts.streamRuns, "MQTT / Kafka / Spark"],
      ["Fastest exact", fastest == null ? "n/a" : formatMs(fastest), data.fastestExactRun?.id || "No run yet"]
    ].map(([label, value, note]) => `<div class="stat"><label>${label}</label><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></div>`).join("");
  }
  
  const recentEl = document.getElementById("recent-runs");
  if (recentEl) recentEl.innerHTML = runTable(data.recentRuns);
  
  bindRunLinks();
}

function renderRunArchive() {
  const el = document.getElementById("run-archive");
  if (el) el.innerHTML = runTable(state.runs, true);
  bindRunLinks();
}

function renderPaperFigures() {
  const container = document.getElementById("paper-figures");
  if (!container) return;
  if (!state.figures.length) {
    container.innerHTML = '<div class="empty-state">No figures generated. Run a benchmark suite to generate plots.</div>';
    return;
  }
  
  container.innerHTML = state.figures.map(figure => `
    <div class="figure-card" style="margin-bottom: 2rem;">
      <h4 style="margin-bottom: 1rem; color: var(--fg); font-weight: 500;">${escapeHtml(figure.name)}</h4>
      <a href="${escapeHtml(figure.url)}" target="_blank" style="display: block; background: #fff; padding: 10px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <img src="${escapeHtml(figure.url)}" alt="${escapeHtml(figure.name)}" style="max-width: 100%; height: auto; display: block;" loading="lazy">
      </a>
    </div>
  `).join("");
}

// Side drawer logic
function bindRunLinks() {
  document.querySelectorAll(".run-link").forEach(button => {
    button.onclick = async () => {
      try {
        const data = await api(`/api/runs/${encodeURIComponent(button.dataset.run)}`);
        document.getElementById("drawer-title").textContent = data.id;
        
        let logsHtml = '';
        if (data.logs) {
            logsHtml = `<h4>Logs</h4>
            <div class="log-container">
            ${Object.entries(data.logs).map(([name, log]) => `
                <div class="log-block">
                <h5>${escapeHtml(name)}</h5>
                <pre><code>${escapeHtml(log)}</code></pre>
                </div>
            `).join("")}
            </div>`;
        }

        document.getElementById("drawer-content").innerHTML = `
          <div class="drawer-stats">
            <div class="mini-stat"><span>Elapsed</span><strong>${formatMs(data.summary.algorithmElapsedMs)}</strong></div>
            <div class="mini-stat"><span>Pruning</span><strong>${percent(data.summary.avgPruneRatio)}</strong></div>
            <div class="mini-stat"><span>Emitted</span><strong>${Number(data.summary.totalEmittedRecords || 0).toLocaleString()}</strong></div>
          </div>
          <h4>Manifest</h4>
          <pre><code>${escapeHtml(JSON.stringify(data.manifest, null, 2))}</code></pre>
          <h4>Metrics</h4>
          <pre><code>${escapeHtml(JSON.stringify(data.metrics, null, 2))}</code></pre>
          ${logsHtml}
        `;
        document.getElementById("detail-drawer").setAttribute("aria-hidden", "false");
        document.getElementById("overlay").classList.add("active");
      } catch (error) {
        toast(error.message, true);
      }
    };
  });

  document.querySelectorAll(".compare-suite-btn").forEach(button => {
    button.onclick = () => {
      const suiteId = button.dataset.suite;
      document.querySelectorAll(".compare-check").forEach(cb => {
        cb.checked = cb.value.startsWith(suiteId);
      });
      const compareBtn = document.getElementById("compare-selected");
      if (compareBtn) compareBtn.click();
    };
  });
}

document.getElementById("close-drawer")?.addEventListener("click", () => {
  document.getElementById("detail-drawer").setAttribute("aria-hidden", "true");
  document.getElementById("overlay").classList.remove("active");
});

document.getElementById("overlay")?.addEventListener("click", () => {
  document.getElementById("detail-drawer").setAttribute("aria-hidden", "true");
  document.getElementById("overlay").classList.remove("active");
});

// Compare selected logic
document.getElementById("compare-selected")?.addEventListener("click", async () => {
  const selected = Array.from(document.querySelectorAll(".compare-check:checked")).map(el => el.value);
  if (selected.length < 2) {
    toast("Select at least two runs to compare.", true);
    return;
  }
  try {
    const report = await api(`/api/compare?ids=${selected.map(encodeURIComponent).join(",")}`);
    const panel = document.getElementById("comparison-panel");
    if (!panel) return;

    let noticeHtml = report.fair ? 
      `<div class="callout success">${escapeHtml(report.notice)}</div>` : 
      `<div class="callout warning">${escapeHtml(report.notice)}<br><small>Differing fields: ${report.differences.map(d => escapeHtml(d.label)).join(', ')}</small></div>`;

    // Generate dynamic HTML bar charts based exactly on selected runs
    const maxTime = Math.max(...report.runs.map(r => r.algorithmElapsedMs || 0));
    const maxEmissions = Math.max(...report.runs.map(r => r.totalEmittedRecords || 0));
    const maxPruning = Math.max(...report.runs.map(r => r.avgPruneRatio || 0));

    function makeChart(title, maxVal, valueFn, formatFn, color) {
      if (maxVal === 0) return '';
      return `
        <div class="panel" style="margin-top: 1rem; padding: 1.5rem; background: var(--bg-surface);">
          <h4 style="margin-bottom: 1rem; color: var(--fg);">${title}</h4>
          <div style="display: flex; flex-direction: column; gap: 0.75rem;">
            ${report.runs.map(run => {
              const val = valueFn(run);
              const pct = maxVal > 0 ? (val / maxVal) * 100 : 0;
              return `
                <div style="display: flex; align-items: center; gap: 1rem;">
                  <div style="width: 150px; font-size: 0.85rem; text-align: right; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(run.id)}">
                    ${escapeHtml(run.algorithm)} <small class="subtle">(${escapeHtml(run.source)})</small>
                  </div>
                  <div style="flex-grow: 1; background: var(--bg); border-radius: 4px; height: 1.5rem; overflow: hidden;">
                    <div style="width: ${pct}%; height: 100%; background: ${color}; transition: width 0.3s;"></div>
                  </div>
                  <div style="width: 80px; font-size: 0.85rem; font-weight: 500;">
                    ${formatFn(val)}
                  </div>
                </div>
              `;
            }).join("")}
          </div>
        </div>
      `;
    }

    const plotsHtml = `
      <div class="section-head" style="margin-top: 2rem;">
        <h3>Dynamic Comparison Analytics</h3>
      </div>
      <div style="display: flex; flex-direction: column; gap: 1rem;">
        ${makeChart('Algorithm Execution Time', maxTime, r => r.algorithmElapsedMs || 0, val => formatMs(val), 'var(--accent)')}
        ${makeChart('Candidate Pruning Ratio', maxPruning, r => r.avgPruneRatio || 0, val => percent(val), '#4f46e5')}
        ${makeChart('Total Emitted Records', maxEmissions, r => r.totalEmittedRecords || 0, val => Number(val).toLocaleString(), '#e11d48')}
      </div>
    `;

    panel.innerHTML = `
      <div class="section-head">
        <h3>Comparison Report</h3>
      </div>
      ${noticeHtml}
      <div class="scroll-table">
        <table class="table">
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Variant</th>
              <th>Engine</th>
              <th>Elapsed</th>
              <th>Reduction vs Base</th>
            </tr>
          </thead>
          <tbody>
            ${report.runs.map(run => `
              <tr>
                <td>${escapeHtml(run.id)}</td>
                <td>${escapeHtml(run.algorithm)}</td>
                <td>${escapeHtml(run.source)}</td>
                <td>${formatMs(run.algorithmElapsedMs)}</td>
                <td>${run.reductionVsFirstPct != null ? `${run.reductionVsFirstPct}%` : '-'}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
      ${plotsHtml}
    `;
    panel.scrollIntoView({ behavior: "smooth" });
  } catch (error) {
    toast(error.message, true);
  }
});

// Launcher Forms
function handleLaunch(formId, mode) {
  const form = document.getElementById(formId);
  if (!form) return;
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const formData = new FormData(form);
    const payload = { mode };
    for (const [key, value] of formData.entries()) {
      payload[key] = value === "on" ? true : value;
    }
    // Handle checkboxes that are unchecked (FormData excludes them)
    form.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      if (!cb.checked) payload[cb.name] = false;
    });

    try {
      await api("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      toast(`Successfully launched ${mode} job in the background.`);
      form.reset();
    } catch (error) {
      toast(error.message, true);
    }
  });
}

handleLaunch("single-run-form", "single-run");
handleLaunch("ablation-suite-form", "ablation-suite");
handleLaunch("full-evaluation-form", "full-evaluation");

// Jobs Polling
async function renderJobs() {
  try {
    const data = await api("/api/jobs");
    const activeJobs = data.jobs.filter(j => j.status === "running");
    
    const panel = document.getElementById("active-jobs-panel");
    const list = document.getElementById("active-jobs-list");
    
    if (activeJobs.length > 0) {
      if (panel) panel.style.display = "block";
      if (list) {
        list.innerHTML = activeJobs.map(job => `
          <div style="padding: 1rem; background: var(--bg-surface); border: 1px solid var(--border); border-radius: 4px; margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <strong>${escapeHtml(job.mode)}</strong>
              <div>
                <span class="badge teal pulse" style="margin-right: 0.5rem;">Running</span>
                <button class="button" style="padding: 0.25rem 0.75rem; font-size: 0.85rem;" onclick="stopJob('${escapeHtml(job.jobId)}')">Stop</button>
              </div>
            </div>
            <p class="subtle" style="margin-top: 0.5rem; margin-bottom: 1rem; font-family: monospace;">Suite ID: ${escapeHtml(job.runId)}</p>
            <div style="background: #1e1e1e; color: #d4d4d4; padding: 1rem; border-radius: 4px; max-height: 300px; overflow-y: auto; font-family: monospace; font-size: 0.85rem; white-space: pre-wrap;" class="job-log-container">${escapeHtml(job.log || "Waiting for output...")}</div>
          </div>
        `).join("");
        
        // Auto-scroll to bottom of logs
        document.querySelectorAll('.job-log-container').forEach(el => el.scrollTop = el.scrollHeight);
      }
    } else {
      if (panel) panel.style.display = "none";
    }
  } catch (error) {
    console.error("Failed to fetch jobs:", error);
  } finally {
    setTimeout(renderJobs, 3000);
  }
}

window.stopJob = async function(jobId) {
  if (!confirm("Are you sure you want to stop this running job?")) return;
  try {
    await api(`/api/jobs/${jobId}`, { method: "DELETE" });
    toast("Job stopped successfully.");
    renderJobs(); // Immediate refresh
  } catch (error) {
    toast(`Failed to stop job: ${error.message}`, true);
  }
};

// Navigation
document.addEventListener("click", event => {
  if (event.target.matches(".nav-link")) {
    openView(event.target.dataset.view);
  } else if (event.target.matches("[data-open-view]")) {
    openView(event.target.dataset.openView);
  }
});

const refreshBtn = document.getElementById("refresh-button");
if (refreshBtn) refreshBtn.onclick = refreshAll;

// Init
refreshAll();
renderJobs();
