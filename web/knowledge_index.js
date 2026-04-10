document.addEventListener("DOMContentLoaded", async () => {
  const overviewGrid = document.getElementById("overviewGrid");
  const indexAssetViewport = document.getElementById("indexAssetViewport");
  const indexAssetBody = document.getElementById("indexAssetBody");
  const debugSandbox = document.getElementById("debugSandbox");
  const debugQuery = document.getElementById("debugQuery");
  const debugResults = document.getElementById("debugResults");
  const fullContent = document.getElementById("fullContent");
  const jobList = document.getElementById("jobList");
  const userInfo = document.getElementById("userInfo");
  const state = {
    assets: [],
    virtual: {
      rowHeight: 92,
      overscan: 5,
      rafId: 0,
    },
  };

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function badgeClass(asset) {
    if (asset.index_status === "failed") return "warn";
    if ((asset.chunk_count || 0) > 0) return "good";
    return "";
  }

  function statusText(asset) {
    if (asset.index_status === "failed") return "索引失败";
    if (asset.index_status === "running") return "索引中";
    if ((asset.chunk_count || 0) > 0) return "可检索";
    return "未建立索引";
  }

  async function readContent(assetId) {
    const res = await api(`/api/knowledge/assets/${assetId}/content?mode=full`);
    fullContent.textContent = res.content || "";
  }

  function renderOverview(data) {
    const stats = [
      ["资产总数", data.asset_count],
      ["已建立索引", data.indexed_asset_count],
      ["失败资产", data.failed_asset_count],
      ["Chunk 数", data.chunk_count],
      ["Embedding 数", data.embedding_count],
    ];
    overviewGrid.innerHTML = stats.map(([label, value]) => `
      <div class="stat-card">
        <span>${label}</span>
        <strong>${value}</strong>
      </div>
    `).join("");
  }

  function scheduleAssetVirtualRender() {
    if (state.virtual.rafId) {
      cancelAnimationFrame(state.virtual.rafId);
    }
    state.virtual.rafId = requestAnimationFrame(() => {
      state.virtual.rafId = 0;
      renderAssets();
    });
  }

  function renderAssets() {
    if (!state.assets.length) {
      indexAssetBody.style.height = "auto";
      indexAssetBody.innerHTML = `<div class="empty-state">暂无索引资产。</div>`;
      return;
    }
    const rowHeight = state.virtual.rowHeight;
    const scrollTop = indexAssetViewport.scrollTop;
    const viewportHeight = indexAssetViewport.clientHeight;
    const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - state.virtual.overscan);
    const endIndex = Math.min(
      state.assets.length,
      Math.ceil((scrollTop + viewportHeight) / rowHeight) + state.virtual.overscan,
    );
    const totalHeight = state.assets.length * rowHeight;
    let html = "";
    for (let index = startIndex; index < endIndex; index += 1) {
      const asset = state.assets[index];
      const top = index * rowHeight;
      html += `
        <div class="index-row" style="top:${top}px;height:${rowHeight}px;">
          <div>
            <div class="index-asset-title">${escapeHtml(asset.title)}</div>
            <div class="index-asset-sub">${escapeHtml(asset.source_ref || asset.description || "")}</div>
          </div>
          <div>${escapeHtml(asset.asset_type)}</div>
          <div><span class="badge-pill ${badgeClass(asset)}">${statusText(asset)}</span></div>
          <div>${asset.chunk_count || 0}</div>
          <div>
            <div class="toolbar" style="margin-top:0;">
              <button class="btn btn-outline btn-sm btn-reindex" data-id="${asset.asset_id}">重建</button>
              <button class="btn btn-outline btn-sm btn-read" data-id="${asset.asset_id}">读原文</button>
            </div>
          </div>
        </div>
      `;
    }
    indexAssetBody.style.height = `${Math.max(totalHeight, rowHeight)}px`;
    indexAssetBody.innerHTML = html;
  }

  function renderJobs(jobs) {
    if (!jobs.length) {
      jobList.innerHTML = `<div class="job-card">暂无索引任务。</div>`;
      return;
    }
    jobList.innerHTML = jobs.map(job => `
      <div class="job-card">
        <div style="display:flex;justify-content:space-between;gap:12px;">
          <strong>${escapeHtml(job.scope)}</strong>
          <span class="badge-pill ${job.status === "failed" ? "warn" : job.status === "success" ? "good" : ""}">${escapeHtml(job.status)}</span>
        </div>
        <div style="margin-top:8px;color:#475569;">${escapeHtml(job.message || "")}</div>
        <div style="margin-top:8px;font-size:12px;color:#64748b;">${escapeHtml(job.updated_at || "")}</div>
      </div>
    `).join("");
  }

  async function runDebugSearch() {
    const sandboxId = debugSandbox.value || "";
    const query = (debugQuery.value || "").trim();
    if (!query) {
      debugResults.innerHTML = `<div class="result-card">请输入检索 query。</div>`;
      return;
    }
    const res = await api("/api/knowledge/index/search-debug", {
      method: "POST",
      body: JSON.stringify({ query, sandbox_id: sandboxId, top_k: 5 }),
    });
    const results = res.results || [];
    if (!results.length) {
      debugResults.innerHTML = `<div class="result-card">没有命中结果。</div>`;
      return;
    }
    debugResults.innerHTML = results.map(item => `
      <div class="result-card">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">
          <div>
            <strong>${escapeHtml(item.title)}</strong>
            <div style="margin-top:6px;color:#64748b;font-size:12px;">asset_id: ${escapeHtml(item.asset_id)}</div>
          </div>
          <span class="badge-pill good">score ${Number(item.score || 0).toFixed(3)}</span>
        </div>
        <div style="margin-top:10px;color:#334155;white-space:pre-wrap;">${escapeHtml(item.snippet || "")}</div>
        <div class="toolbar">
          <button class="btn btn-outline btn-sm btn-read-result" data-id="${item.asset_id}">读取原文</button>
        </div>
      </div>
    `).join("");
    debugResults.querySelectorAll(".btn-read-result").forEach(btn => {
      btn.onclick = async () => readContent(btn.dataset.id);
    });
  }

  async function loadPage() {
    const [me, overview, assetsRes, jobsRes, sandboxesRes] = await Promise.all([
      api("/api/me"),
      api("/api/knowledge/index/overview"),
      api("/api/knowledge/index/assets"),
      api("/api/knowledge/index/jobs"),
      api("/api/sandboxes"),
    ]);
    userInfo.textContent = `${me.user.display_name} (${(me.user.groups || []).join(", ")})`;
    renderOverview(overview);
    state.assets = assetsRes.assets || [];
    indexAssetViewport.scrollTop = 0;
    renderAssets();
    renderJobs(jobsRes.jobs || []);
    debugSandbox.innerHTML = (sandboxesRes.sandboxes || []).map(item => `<option value="${item.sandbox_id}">${escapeHtml(item.name)}</option>`).join("");
  }

  document.getElementById("btnDebugSearch").onclick = runDebugSearch;
  document.getElementById("btnRebuildAll").onclick = async () => {
    await api("/api/knowledge/index/rebuild", { method: "POST", body: JSON.stringify({}) });
    await loadPage();
  };
  indexAssetViewport.addEventListener("scroll", scheduleAssetVirtualRender, { passive: true });
  indexAssetBody.addEventListener("click", async event => {
    const reindexBtn = event.target.closest(".btn-reindex");
    if (reindexBtn) {
      await api(`/api/knowledge/index/assets/${reindexBtn.dataset.id}/reindex`, { method: "POST" });
      await loadPage();
      return;
    }
    const readBtn = event.target.closest(".btn-read");
    if (readBtn) {
      await readContent(readBtn.dataset.id);
    }
  });
  window.addEventListener("resize", scheduleAssetVirtualRender);

  try {
    await loadPage();
  } catch (error) {
    overviewGrid.innerHTML = `<div class="stat-card"><strong>加载失败</strong><div>${escapeHtml(error.message)}</div></div>`;
  }
});
