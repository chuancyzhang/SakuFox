let sqlToolboxState = {
  sandboxes: [],
  currentSandboxId: "",
  runs: [],
  views: [],
  currentRun: null,
  currentColumns: [],
};

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function tr(key, fallback, params = {}) {
  const text = i18n.t(key, params);
  return !text || text === key ? fallback : text;
}

function currentSandbox() {
  return sqlToolboxState.sandboxes.find((item) => item.sandbox_id === sqlToolboxState.currentSandboxId) || null;
}

function normalizeFields(fields) {
  if (!fields) return [];
  if (Array.isArray(fields)) return fields;
  return [];
}

function renderResultTable(rows, columns) {
  const wrap = document.getElementById("resultTableWrap");
  const table = document.getElementById("resultTable");
  if (!wrap || !table) return;

  const safeRows = Array.isArray(rows) ? rows : [];
  const safeColumns = Array.isArray(columns) && columns.length > 0
    ? columns.map((col) => typeof col === "string" ? col : col.name).filter(Boolean)
    : (safeRows[0] ? Object.keys(safeRows[0]) : []);

  if (!safeColumns.length) {
    wrap.style.display = "none";
    table.innerHTML = "";
    return;
  }

  const headHtml = `<thead><tr>${safeColumns.map((col) => `<th>${escapeHtml(col)}</th>`).join("")}</tr></thead>`;
  const bodyHtml = safeRows.length
    ? `<tbody>${safeRows.map((row) => `<tr>${safeColumns.map((col) => `<td>${escapeHtml(row?.[col])}</td>`).join("")}</tr>`).join("")}</tbody>`
    : `<tbody><tr><td colspan="${safeColumns.length}" style="text-align:center;color:#64748b;">${escapeHtml(tr("sql_toolbox_no_rows", "No rows returned"))}</td></tr></tbody>`;
  table.innerHTML = headHtml + bodyHtml;
  wrap.style.display = "block";
}

function renderFieldDescriptionInputs(columns) {
  const grid = document.getElementById("fieldDescGrid");
  if (!grid) return;
  const safeColumns = Array.isArray(columns) ? columns : [];

  if (!safeColumns.length) {
    grid.innerHTML = "";
    return;
  }

  grid.innerHTML = safeColumns
    .map((col) => {
      const name = typeof col === "string" ? col : (col?.name || "");
      return `
        <div class="field-desc-item">
          <label style="display:block;font-size:12px;color:#475569;margin-bottom:4px;">${escapeHtml(name)}</label>
          <input type="text" data-field-name="${escapeHtml(name)}" placeholder="${escapeHtml(tr("sql_toolbox_field_desc_placeholder", "Field description (optional)"))}" style="padding-left:10px;" />
        </div>
      `;
    })
    .join("");
}

function syncFieldDescriptionsFromRun(run) {
  const columns = normalizeFields(run?.columns || []);
  sqlToolboxState.currentColumns = columns;
  renderFieldDescriptionInputs(columns);
}

function renderModelList() {
  const list = document.getElementById("modelList");
  if (!list) return;

  const sandbox = currentSandbox();
  if (!sandbox) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"))}</li>`;
    return;
  }

  const physicalTables = Array.isArray(sandbox.tables) ? sandbox.tables : [];
  const virtualViews = Array.isArray(sandbox.virtual_views) ? sandbox.virtual_views : [];
  const uploads = sandbox.uploads && typeof sandbox.uploads === "object" ? Object.entries(sandbox.uploads) : [];

  const items = [];
  physicalTables.forEach((name) => {
    items.push(`
      <li class="list-item">
        <strong><i class="fa-solid fa-table-columns" style="color:#2563eb;"></i> ${escapeHtml(name)}</strong>
        <div class="section-muted">${escapeHtml(tr("sql_toolbox_physical_table", "Physical Table"))}</div>
      </li>
    `);
  });
  virtualViews.forEach((view) => {
    const name = view?.name || "";
    if (!name) return;
    items.push(`
      <li class="list-item">
        <strong><i class="fa-solid fa-diagram-project" style="color:#f59e0b;"></i> ${escapeHtml(name)}</strong>
        <div class="section-muted">${escapeHtml(tr("virtual_view_label", "Virtual View"))}</div>
        ${view.description ? `<div style="margin-top:4px;">${escapeHtml(view.description)}</div>` : ""}
      </li>
    `);
  });
  uploads.forEach(([key, value]) => {
    const name = typeof key === "string" ? key : (value?.name || value?.dataset_name || "");
    if (!name) return;
    items.push(`
      <li class="list-item">
        <strong><i class="fa-solid fa-file-lines" style="color:#10b981;"></i> ${escapeHtml(name)}</strong>
        <div class="section-muted">${escapeHtml(tr("sql_toolbox_upload_file", "Uploaded File"))}</div>
      </li>
    `);
  });

  list.innerHTML = items.length ? items.join("") : `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_models", "No tables/views available"))}</li>`;
}

function renderRuns() {
  const list = document.getElementById("runList");
  if (!list) return;
  const runs = sqlToolboxState.runs || [];
  if (!runs.length) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_runs", "No execution history"))}</li>`;
    return;
  }

  list.innerHTML = runs
    .map((run) => {
      const status = run.status === "success"
        ? `<span style="color:#16a34a;font-weight:600;">success</span>`
        : `<span style="color:#dc2626;font-weight:600;">${escapeHtml(run.status || "failed")}</span>`;
      const rowCount = typeof run.row_count === "number" ? run.row_count : 0;
      return `
        <li class="list-item" data-run-id="${escapeHtml(run.run_id)}">
          <div class="view-row">
            <div>
              <strong>${escapeHtml(run.run_id)}</strong>
              <div class="section-muted">${status} | rows=${rowCount} | ${escapeHtml(String(run.duration_ms || 0))}ms</div>
              <div style="margin-top:4px;white-space:pre-wrap;">${escapeHtml((run.sql || "").slice(0, 180))}</div>
            </div>
            <button class="btn btn-outline btn-sm load-run-btn" data-run-id="${escapeHtml(run.run_id)}">${escapeHtml(tr("sql_toolbox_load_btn", "Load"))}</button>
          </div>
        </li>
      `;
    })
    .join("");

  list.querySelectorAll(".load-run-btn").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      const runId = btn.getAttribute("data-run-id");
      const run = sqlToolboxState.runs.find((item) => item.run_id === runId);
      if (!run) return;
      await loadRunIntoEditor(run);
    });
  });
}

function renderViews() {
  const list = document.getElementById("viewList");
  if (!list) return;
  const views = sqlToolboxState.views || [];
  if (!views.length) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_views", "No analysis views yet"))}</li>`;
    return;
  }

  list.innerHTML = views
    .map((view) => {
      const cols = Array.isArray(view.columns) ? view.columns : [];
      return `
        <li class="list-item">
          <div class="view-row">
            <div style="min-width:0;">
              <strong>${escapeHtml(view.name || view.view_id)}</strong>
              <div class="section-muted">${escapeHtml(view.description || "")}</div>
              <div class="section-muted" style="margin-top:4px;">${escapeHtml(String(cols.length))} cols | source ${escapeHtml(view.source_run_id || "")}</div>
            </div>
            <button class="btn btn-outline btn-sm delete-view-btn" data-view-id="${escapeHtml(view.view_id)}">${escapeHtml(tr("sql_toolbox_delete_btn", "Delete"))}</button>
          </div>
        </li>
      `;
    })
    .join("");

  list.querySelectorAll(".delete-view-btn").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      const viewId = btn.getAttribute("data-view-id");
      if (!viewId) return;
      await deleteVirtualView(viewId);
    });
  });
}

async function loadSandboxes(selectId = "") {
  const res = await api("/api/sandboxes");
  sqlToolboxState.sandboxes = res.sandboxes || [];

  const sandboxSelect = document.getElementById("sandboxSelect");
  if (!sandboxSelect) return;

  const previous = selectId || sqlToolboxState.currentSandboxId || sandboxSelect.value;
  sandboxSelect.innerHTML = "";

  if (!sqlToolboxState.sandboxes.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = tr("sql_toolbox_no_sandbox", "No sandboxes available");
    sandboxSelect.appendChild(opt);
    sqlToolboxState.currentSandboxId = "";
    renderModelList();
    renderRuns();
    renderViews();
    return;
  }

  sqlToolboxState.sandboxes.forEach((sb) => {
    const opt = document.createElement("option");
    opt.value = sb.sandbox_id;
    opt.textContent = sb.name || sb.sandbox_id;
    sandboxSelect.appendChild(opt);
  });

  if (previous && sqlToolboxState.sandboxes.some((sb) => sb.sandbox_id === previous)) {
    sandboxSelect.value = previous;
  } else {
    sandboxSelect.value = sqlToolboxState.sandboxes[0].sandbox_id;
  }

  sqlToolboxState.currentSandboxId = sandboxSelect.value || "";
  renderModelList();
}

async function loadRunsAndViews() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  if (!sandboxId) {
    sqlToolboxState.runs = [];
    sqlToolboxState.views = [];
    renderRuns();
    renderViews();
    return;
  }

  const [runsRes, viewsRes] = await Promise.all([
    api(`/api/sql-toolbox/runs?sandbox_id=${encodeURIComponent(sandboxId)}`),
    api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views`),
  ]);

  sqlToolboxState.runs = runsRes.runs || [];
  sqlToolboxState.views = viewsRes.virtual_views || [];
  renderRuns();
  renderViews();
}

async function refreshEverything(selectId = "") {
  await loadSandboxes(selectId);
  await loadRunsAndViews();
}

function setRunStatus(text, kind = "") {
  const el = document.getElementById("runStatus");
  if (!el) return;
  el.textContent = text;
  el.style.color = kind === "error" ? "#dc2626" : kind === "success" ? "#16a34a" : "#334155";
}

function setSaveStatus(text, kind = "") {
  const el = document.getElementById("saveStatus");
  if (!el) return;
  el.textContent = text;
  el.style.color = kind === "error" ? "#dc2626" : kind === "success" ? "#16a34a" : "#334155";
}

async function loadRunIntoEditor(run) {
  const sqlInput = document.getElementById("sqlInput");
  const viewNameInput = document.getElementById("viewNameInput");
  const viewDescInput = document.getElementById("viewDescInput");
  if (!sqlInput || !viewNameInput || !viewDescInput) return;

  sqlInput.value = run.sql || "";
  sqlToolboxState.currentRun = run;
  syncFieldDescriptionsFromRun(run);

  const resultMeta = document.getElementById("resultMeta");
  if (resultMeta) {
    resultMeta.textContent = `${run.status || "unknown"} | rows=${run.row_count || 0} | ${run.duration_ms || 0}ms`;
  }
  renderResultTable(run.result_preview || [], run.columns || []);

  const inferredName = `${(currentSandbox()?.name || "sandbox").replace(/[^A-Za-z0-9_]/g, "_")}_view`;
  if (!viewNameInput.value) {
    viewNameInput.value = inferredName.replace(/_+/g, "_").replace(/^_+|_+$/g, "").slice(0, 48);
  }
  if (!viewDescInput.value) {
    viewDescInput.value = "";
  }
  setSaveStatus(
    run.status === "success"
      ? tr("sql_toolbox_can_save_run", "This successful run can be saved")
      : tr("sql_toolbox_cannot_save_run", "This run cannot be saved"),
    run.status === "success" ? "success" : "error",
  );
}

async function executeCurrentSql() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  const sqlInput = document.getElementById("sqlInput");
  const resultMeta = document.getElementById("resultMeta");
  if (!sandboxId) {
    setRunStatus(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"), "error");
    return;
  }
  if (!sqlInput || !sqlInput.value.trim()) {
    setRunStatus(tr("sql_toolbox_need_sql", "Please enter SQL"), "error");
    return;
  }

  const payload = {
    sandbox_id: sandboxId,
    sql: sqlInput.value,
  };

  sqlToolboxState.currentRun = null;
  setRunStatus(tr("sql_toolbox_executing", "Executing..."));
  if (resultMeta) resultMeta.textContent = tr("sql_toolbox_executing", "Executing...");

  try {
    const res = await api("/api/sql-toolbox/execute", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const run = res.run;
    sqlToolboxState.currentRun = run;
    syncFieldDescriptionsFromRun(run);
    renderResultTable(run.result_preview || [], run.columns || []);
    if (resultMeta) {
      resultMeta.textContent = `run ${run.run_id} | ${run.status} | rows=${run.row_count || 0} | ${run.duration_ms || 0}ms`;
    }
    setRunStatus(tr("sql_toolbox_execute_success", "Execution successful"), "success");
    setSaveStatus(tr("sql_toolbox_can_save", "You can save this as an analysis view"), "success");
    await loadRunsAndViews();
  } catch (err) {
    const message = err?.message || tr("sql_toolbox_execute_failed", "Execution failed");
    setRunStatus(message, "error");
    if (resultMeta) resultMeta.textContent = message;
    setSaveStatus(tr("sql_toolbox_cannot_save_run", "This run cannot be saved"), "error");
    await loadRunsAndViews();
  }
}

async function saveCurrentView() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  const run = sqlToolboxState.currentRun;
  const nameInput = document.getElementById("viewNameInput");
  const descInput = document.getElementById("viewDescInput");
  if (!sandboxId) {
    setSaveStatus(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"), "error");
    return;
  }
  if (!run || run.status !== "success") {
    setSaveStatus(tr("sql_toolbox_select_success_run", "Please select a successful execution run first"), "error");
    return;
  }

  const name = (nameInput?.value || "").trim();
  const description = (descInput?.value || "").trim();
  if (!name) {
    setSaveStatus(tr("sql_toolbox_need_view_name", "Please enter a view name"), "error");
    return;
  }
  if (!description) {
    setSaveStatus(tr("sql_toolbox_need_view_desc", "Please enter a business description"), "error");
    return;
  }

  const fieldDescriptions = {};
  document.querySelectorAll("#fieldDescGrid input[data-field-name]").forEach((input) => {
    const fieldName = input.getAttribute("data-field-name");
    const value = (input.value || "").trim();
    if (fieldName && value) {
      fieldDescriptions[fieldName] = value;
    }
  });

  try {
    const res = await api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views`, {
      method: "POST",
      body: JSON.stringify({
        source_run_id: run.run_id,
        name,
        description,
        field_descriptions: fieldDescriptions,
      }),
    });
    setSaveStatus(tr("sql_toolbox_save_success", "Saved: {name}", { name: res.virtual_view.name }), "success");
    await loadRunsAndViews();
  } catch (err) {
    setSaveStatus(err?.message || tr("sql_toolbox_save_failed", "Save failed"), "error");
  }
}

async function deleteVirtualView(viewId) {
  const sandboxId = sqlToolboxState.currentSandboxId;
  if (!sandboxId || !viewId) return;
  const view = sqlToolboxState.views.find((item) => item.view_id === viewId);
  const label = view?.name || viewId;
  if (!confirm(tr("sql_toolbox_delete_confirm", `Delete analysis view "${label}"?`, { name: label }))) return;

  try {
    await api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views/${encodeURIComponent(viewId)}`, {
      method: "DELETE",
    });
    await loadRunsAndViews();
    setSaveStatus(tr("sql_toolbox_delete_success", "Analysis view deleted"), "success");
  } catch (err) {
    setSaveStatus(err?.message || tr("sql_toolbox_delete_failed", "Delete failed"), "error");
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const userInfo = document.getElementById("userInfo");
  const sandboxSelect = document.getElementById("sandboxSelect");
  const runBtn = document.getElementById("runBtn");
  const saveViewBtn = document.getElementById("saveViewBtn");
  const sqlInput = document.getElementById("sqlInput");

  try {
    const me = await api("/api/me");
    userInfo.textContent = `${me.user.display_name} (${me.user.groups.join(", ")})`;
  } catch (err) {
    window.location.href = "/web/login.html";
    return;
  }

  sandboxSelect.addEventListener("change", async () => {
    sqlToolboxState.currentSandboxId = sandboxSelect.value || "";
    sqlToolboxState.currentRun = null;
    document.getElementById("resultMeta").textContent = tr("sql_toolbox_not_run", "Not executed yet");
    renderResultTable([], []);
    document.getElementById("viewNameInput").value = "";
    document.getElementById("viewDescInput").value = "";
    document.getElementById("fieldDescGrid").innerHTML = "";
    renderModelList();
    await loadRunsAndViews();
  });

  runBtn.addEventListener("click", executeCurrentSql);
  saveViewBtn.addEventListener("click", saveCurrentView);
  sqlInput.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      executeCurrentSql();
    }
  });

  try {
    await refreshEverything();
    if (sqlToolboxState.currentSandboxId) {
      const currentRuns = sqlToolboxState.runs || [];
      if (currentRuns.length > 0) {
        await loadRunIntoEditor(currentRuns[0]);
      }
    }
  } catch (err) {
    setRunStatus(err?.message || tr("sql_toolbox_loading_fail", "Load failed"), "error");
  }
});
