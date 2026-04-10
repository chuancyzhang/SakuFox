document.addEventListener("DOMContentLoaded", async () => {
  const state = {
    assets: [],
    pendingExperiences: [],
    filtered: [],
    activeType: "all",
    selectedId: "",
    pendingSourceRef: "",
    sandboxes: [],
    detail: null,
  };

  const assetTabs = document.getElementById("assetTabs");
  const assetListViewport = document.getElementById("assetListViewport");
  const assetList = document.getElementById("assetList");
  const detailPanel = document.getElementById("detailPanel");
  const pendingExperienceViewport = document.getElementById("pendingExperienceViewport");
  const pendingExperienceList = document.getElementById("pendingExperienceList");
  const assetSearch = document.getElementById("assetSearch");
  const statGrid = document.getElementById("statGrid");
  const userInfo = document.getElementById("userInfo");
  const btnCreateKb = document.getElementById("btnCreateKb");

  const typeDefs = [
    { key: "all", label: "全部资产" },
    { key: "enterprise_kb", label: "企业知识" },
    { key: "uploaded_file", label: "上传文件" },
    { key: "experience", label: "沉淀经验" },
  ];
  const pendingVirtualState = {
    itemHeight: 320,
    gap: 12,
    minColumnWidth: 260,
    overscanRows: 2,
  };
  const assetVirtualState = {
    itemHeight: 152,
    gap: 12,
    overscan: 4,
    rafId: 0,
  };

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

  function typeText(assetType) {
    return typeDefs.find(item => item.key === assetType)?.label || assetType;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function renderStats() {
    const assets = state.assets;
    const mountedCount = assets.reduce((sum, asset) => sum + (asset.mounted_sandboxes || []).length, 0);
    const searchable = assets.filter(asset => (asset.chunk_count || 0) > 0).length;
    const pendingCount = state.pendingExperiences.length;
    const stats = [
      ["资产总数", assets.length],
      ["已挂载工作空间", mountedCount],
      ["可检索资产", searchable],
      ["待确认经验", pendingCount],
    ];
    statGrid.innerHTML = stats.map(([label, value]) => `
      <div class="stat-card">
        <div class="stat-label">${label}</div>
        <div class="stat-value">${value}</div>
      </div>
    `).join("");
  }

  function applyFilters() {
    const keyword = (assetSearch.value || "").trim().toLowerCase();
    state.filtered = state.assets.filter(asset => {
      const matchesType = state.activeType === "all" || asset.asset_type === state.activeType;
      const haystack = [asset.title, asset.description, asset.source_ref, asset.content_preview].join(" ").toLowerCase();
      const matchesSearch = !keyword || haystack.includes(keyword);
      return matchesType && matchesSearch;
    });
    assetListViewport.scrollTop = 0;
    renderAssetList();
  }

  function renderTabs() {
    assetTabs.innerHTML = typeDefs.map(item => `
      <button class="tab-btn ${state.activeType === item.key ? "active" : ""}" data-type="${item.key}">${item.label}</button>
    `).join("");
    assetTabs.querySelectorAll("button").forEach(btn => {
      btn.onclick = () => {
        state.activeType = btn.dataset.type;
        renderTabs();
        applyFilters();
      };
    });
  }

  function renderAssetList() {
    if (!state.filtered.length) {
      assetList.style.height = "auto";
      assetList.innerHTML = `<div class="empty-state">没有匹配的知识资产。</div>`;
      return;
    }
    const stride = assetVirtualState.itemHeight + assetVirtualState.gap;
    const scrollTop = assetListViewport.scrollTop;
    const viewportHeight = assetListViewport.clientHeight;
    const startIndex = Math.max(0, Math.floor(scrollTop / stride) - assetVirtualState.overscan);
    const endIndex = Math.min(
      state.filtered.length,
      Math.ceil((scrollTop + viewportHeight) / stride) + assetVirtualState.overscan,
    );
    const totalHeight = (state.filtered.length * assetVirtualState.itemHeight) + (Math.max(state.filtered.length - 1, 0) * assetVirtualState.gap);
    let html = "";
    for (let index = startIndex; index < endIndex; index += 1) {
      const asset = state.filtered[index];
      const top = index * stride;
      html += `
        <div class="asset-virtual-item" style="top:${top}px;height:${assetVirtualState.itemHeight}px;">
          <article class="asset-card ${state.selectedId === asset.asset_id ? "active" : ""}" data-id="${asset.asset_id}">
            <div class="asset-top">
              <div>
                <h4 class="asset-title">${escapeHtml(asset.title)}</h4>
                <p class="asset-sub">${escapeHtml(asset.description || asset.source_ref || "暂无描述")}</p>
              </div>
              <span class="badge-pill ${badgeClass(asset)}">${statusText(asset)}</span>
            </div>
            <div class="badge-row" style="margin-top:auto;">
              <span class="badge-pill">${typeText(asset.asset_type)}</span>
              <span class="badge-pill">Chunks ${asset.chunk_count || 0}</span>
              <span class="badge-pill">挂载 ${(asset.mounted_sandboxes || []).length}</span>
              ${asset.source_path ? `<span class="badge-pill">有原文件</span>` : ""}
            </div>
          </article>
        </div>
      `;
    }
    assetList.style.height = `${Math.max(totalHeight, assetVirtualState.itemHeight)}px`;
    assetList.innerHTML = html;
  }

  function scheduleAssetVirtualRender() {
    if (assetVirtualState.rafId) {
      cancelAnimationFrame(assetVirtualState.rafId);
    }
    assetVirtualState.rafId = requestAnimationFrame(() => {
      assetVirtualState.rafId = 0;
      renderAssetList();
    });
  }

  function schedulePendingVirtualRender() {
    if (pendingVirtualState.rafId) {
      cancelAnimationFrame(pendingVirtualState.rafId);
    }
    pendingVirtualState.rafId = requestAnimationFrame(() => {
      pendingVirtualState.rafId = 0;
      renderPendingExperiences();
    });
  }

  function buildPendingEditorHtml(pending, suggestion) {
    const knowledgeText = (suggestion.knowledge || []).join("\n");
    const defaultMounted = new Set([pending.sandbox_id].filter(Boolean));
    return `
      <div id="pendingExperienceOverlay" style="position:fixed;inset:0;background:rgba(15,23,42,0.45);display:flex;align-items:center;justify-content:center;padding:20px;z-index:1200;">
        <div style="width:min(980px,100%);max-height:92vh;overflow:auto;background:#fff;border-radius:24px;padding:24px;box-shadow:0 30px 80px rgba(15,23,42,0.25);">
          <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;">
            <div>
              <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Pending Experience</div>
              <h3 style="margin:8px 0 0;font-size:28px;">发布经验资产</h3>
              <p style="margin:8px 0 0;color:#475569;">基于本次分析结果生成经验草稿，确认后会写入经验资产池，并可挂载到工作空间。</p>
            </div>
            <button type="button" id="btnClosePendingModal" class="btn btn-outline">关闭</button>
          </div>
          <div style="display:grid;gap:18px;margin-top:20px;">
            <div class="detail-content" style="max-height:none;">
source: ${escapeHtml(pending.report_title || pending.message || "未命名分析")}
sandbox: ${escapeHtml(pending.sandbox_name || pending.sandbox_id || "")}
session_id: ${escapeHtml(pending.session_id || "")}
created_at: ${escapeHtml(pending.created_at || "")}
summary: ${escapeHtml(pending.report_summary || "暂无摘要")}
            </div>
            <form id="pendingExperienceForm" style="display:grid;gap:16px;">
              <label style="display:grid;gap:8px;">
                <span style="font-weight:600;">经验名称</span>
                <input name="name" required value="${escapeHtml(suggestion.name || "")}" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;" />
              </label>
              <label style="display:grid;gap:8px;">
                <span style="font-weight:600;">经验描述</span>
                <textarea name="description" rows="4" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;resize:vertical;">${escapeHtml(suggestion.description || "")}</textarea>
              </label>
              <label style="display:grid;gap:8px;">
                <span style="font-weight:600;">标签</span>
                <input name="tags" value="${escapeHtml((suggestion.tags || []).join(", "))}" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;" />
              </label>
              <label style="display:grid;gap:8px;">
                <span style="font-weight:600;">业务经验</span>
                <textarea name="knowledge" rows="8" style="border:1px solid #cbd5e1;border-radius:18px;padding:14px;resize:vertical;">${escapeHtml(knowledgeText)}</textarea>
              </label>
              <div style="display:grid;gap:10px;">
                <span style="font-weight:600;">发布后挂载到工作空间</span>
                <div class="mount-list">
                  ${state.sandboxes.map(sandbox => `
                    <label class="mount-item">
                      <input type="checkbox" class="publish-mount-checkbox" value="${sandbox.sandbox_id}" ${defaultMounted.has(sandbox.sandbox_id) ? "checked" : ""} />
                      <span>${escapeHtml(sandbox.name)} <span style="color:#64748b;font-size:12px;">(${escapeHtml((sandbox.allowed_groups || []).join(", "))})</span></span>
                    </label>
                  `).join("")}
                </div>
              </div>
              <div class="row-actions">
                <button type="submit" class="btn btn-primary">确认发布</button>
                <button type="button" id="btnDismissPending" class="btn btn-outline">暂不沉淀</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    `;
  }

  function closePendingEditor() {
    document.getElementById("pendingExperienceOverlay")?.remove();
  }

  async function openPendingEditor(pending) {
    const suggestion = await api("/api/skills/propose", {
      method: "POST",
      body: JSON.stringify({
        proposal_id: pending.proposal_id,
        message: pending.message || pending.report_title || "请总结为可复用经验",
        sandbox_id: pending.sandbox_id,
      }),
    });
    const overlay = document.createElement("div");
    overlay.innerHTML = buildPendingEditorHtml(pending, suggestion);
    document.body.appendChild(overlay.firstElementChild);

    document.getElementById("btnClosePendingModal").onclick = closePendingEditor;
    document.getElementById("pendingExperienceOverlay").onclick = event => {
      if (event.target.id === "pendingExperienceOverlay") {
        closePendingEditor();
      }
    };
    document.getElementById("btnDismissPending").onclick = async () => {
      await api(`/api/knowledge/experiences/${pending.proposal_id}/dismiss`, { method: "POST" });
      closePendingEditor();
      await loadAll();
    };
    document.getElementById("pendingExperienceForm").onsubmit = async event => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const payload = {
        proposal_id: pending.proposal_id,
        name: String(formData.get("name") || "").trim(),
        description: String(formData.get("description") || "").trim(),
        tags: String(formData.get("tags") || "").split(",").map(item => item.trim()).filter(Boolean),
        knowledge: String(formData.get("knowledge") || "").split(/\r?\n/).map(item => item.trim()).filter(Boolean),
        mount_sandbox_ids: Array.from(document.querySelectorAll(".publish-mount-checkbox:checked")).map(cb => cb.value),
      };
      if (!payload.name) {
        alert("请输入经验名称。");
        return;
      }
      const result = await api("/api/knowledge/experiences/publish-from-proposal", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      state.selectedId = result.asset?.asset_id || "";
      closePendingEditor();
      await loadAll(state.selectedId);
    };
  }

  function renderPendingExperiences() {
    if (!state.pendingExperiences.length) {
      pendingExperienceList.style.height = "auto";
      pendingExperienceList.innerHTML = `<div class="empty-state">当前没有待确认经验，新的分析成果会在这里等待发布。</div>`;
      return;
    }
    const viewportWidth = Math.max(pendingExperienceViewport.clientWidth, pendingVirtualState.minColumnWidth);
    const cols = Math.max(1, Math.floor((viewportWidth + pendingVirtualState.gap) / (pendingVirtualState.minColumnWidth + pendingVirtualState.gap)));
    const cardWidth = (viewportWidth - (pendingVirtualState.gap * (cols - 1))) / cols;
    const rowStride = pendingVirtualState.itemHeight + pendingVirtualState.gap;
    const rowCount = Math.ceil(state.pendingExperiences.length / cols);
    const totalHeight = (rowCount * pendingVirtualState.itemHeight) + (Math.max(rowCount - 1, 0) * pendingVirtualState.gap);
    const scrollTop = pendingExperienceViewport.scrollTop;
    const viewportHeight = pendingExperienceViewport.clientHeight;
    const startRow = Math.max(0, Math.floor(scrollTop / rowStride) - pendingVirtualState.overscanRows);
    const endRow = Math.min(
      rowCount - 1,
      Math.ceil((scrollTop + viewportHeight) / rowStride) + pendingVirtualState.overscanRows,
    );
    const startIndex = startRow * cols;
    const endIndex = Math.min(state.pendingExperiences.length, (endRow + 1) * cols);
    let html = "";
    for (let index = startIndex; index < endIndex; index += 1) {
      const item = state.pendingExperiences[index];
      const row = Math.floor(index / cols);
      const col = index % cols;
      const top = row * rowStride;
      const left = col * (cardWidth + pendingVirtualState.gap);
      html += `
        <div class="virtual-grid-item" style="top:${top}px;left:${left}px;width:${cardWidth}px;height:${pendingVirtualState.itemHeight}px;">
          <article class="pending-card">
            <div class="badge-row" style="margin-top:0;">
              <span class="badge-pill warn">待确认</span>
              <span class="badge-pill">${escapeHtml(item.mode || "manual")}</span>
              <span class="badge-pill">${escapeHtml(item.sandbox_name || item.sandbox_id || "未知工作空间")}</span>
            </div>
            <h4 style="margin-top:12px;">${escapeHtml(item.report_title || item.message || "未命名经验建议")}</h4>
            <p>${escapeHtml(item.report_summary || "这条经验来自最近一次分析，建议补充命名与描述后发布为正式经验资产。")}</p>
            <div class="badge-row">
              ${(item.selected_tables || []).slice(0, 3).map(table => `<span class="badge-pill">${escapeHtml(table)}</span>`).join("")}
              ${(item.selected_files || []).slice(0, 2).map(file => `<span class="badge-pill">${escapeHtml(file)}</span>`).join("")}
            </div>
            <div class="row-actions" style="margin-top:auto;">
              <button class="btn btn-primary btn-review-pending" data-proposal-id="${item.proposal_id}">审核并发布</button>
              <button class="btn btn-outline btn-dismiss-pending" data-proposal-id="${item.proposal_id}">暂不沉淀</button>
            </div>
          </article>
        </div>
      `;
    }
    pendingExperienceList.style.height = `${Math.max(totalHeight, pendingVirtualState.itemHeight)}px`;
    pendingExperienceList.innerHTML = html;
  }

  async function saveMounts(asset) {
    const checked = Array.from(detailPanel.querySelectorAll(".mount-checkbox:checked")).map(cb => cb.value);
    await api(`/api/knowledge/assets/${asset.asset_id}/mounts`, {
      method: "POST",
      body: JSON.stringify({ sandbox_ids: checked }),
    });
    await loadAll(asset.asset_id);
  }

  async function loadContent(assetId, mode = "preview", cursor = "") {
    const suffix = cursor ? `&cursor=${encodeURIComponent(cursor)}` : "";
    return api(`/api/knowledge/assets/${assetId}/content?mode=${encodeURIComponent(mode)}${suffix}`);
  }

  function buildKbFormHtml(existing) {
    const syncType = existing?.metadata?.sync_type || "manual";
    return `
      <div id="kbModalOverlay" style="position:fixed;inset:0;background:rgba(15,23,42,0.45);display:flex;align-items:center;justify-content:center;padding:20px;z-index:1200;">
        <div style="width:min(880px,100%);max-height:90vh;overflow:auto;background:#fff;border-radius:24px;padding:24px;box-shadow:0 30px 80px rgba(15,23,42,0.25);">
          <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;">
            <div>
              <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Enterprise Knowledge</div>
              <h3 style="margin:8px 0 0;font-size:28px;">${existing ? "编辑企业知识" : "新建企业知识"}</h3>
            </div>
            <button type="button" id="btnCloseKbModal" class="btn btn-outline">关闭</button>
          </div>
          <form id="kbEditorForm" style="display:grid;gap:16px;margin-top:20px;">
            <label style="display:grid;gap:8px;">
              <span style="font-weight:600;">名称</span>
              <input name="name" required value="${escapeHtml(existing?.title || "")}" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;" />
            </label>
            <label style="display:grid;gap:8px;">
              <span style="font-weight:600;">描述</span>
              <textarea name="description" rows="3" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;resize:vertical;">${escapeHtml(existing?.description || "")}</textarea>
            </label>
            <label style="display:grid;gap:8px;">
              <span style="font-weight:600;">同步方式</span>
              <select name="sync_type" style="border:1px solid #cbd5e1;border-radius:14px;padding:12px 14px;">
                <option value="manual" ${syncType === "manual" ? "selected" : ""}>手工维护</option>
                <option value="api" ${syncType === "api" ? "selected" : ""}>API 同步</option>
              </select>
            </label>
            <label style="display:grid;gap:8px;">
              <span style="font-weight:600;">知识正文</span>
              <textarea name="content" rows="14" style="border:1px solid #cbd5e1;border-radius:18px;padding:14px;resize:vertical;font-family:'IBM Plex Mono','SFMono-Regular',Consolas,monospace;">${escapeHtml(existing?.fullContent || "")}</textarea>
            </label>
            <div class="row-actions" style="margin-top:4px;">
              <button type="submit" class="btn btn-primary">${existing ? "保存更新" : "创建知识"}</button>
              ${existing ? `<button type="button" id="btnDeleteKb" class="btn btn-outline">删除知识</button>` : ""}
            </div>
          </form>
        </div>
      </div>
    `;
  }

  function closeKbEditor() {
    document.getElementById("kbModalOverlay")?.remove();
  }

  async function openKbEditor(existingAsset = null) {
    const fullContent = existingAsset ? await loadContent(existingAsset.asset_id, "full") : { content: "" };
    const overlay = document.createElement("div");
    overlay.innerHTML = buildKbFormHtml({
      ...existingAsset,
      fullContent: fullContent.content || "",
    });
    document.body.appendChild(overlay.firstElementChild);

    document.getElementById("btnCloseKbModal").onclick = closeKbEditor;
    document.getElementById("kbModalOverlay").onclick = event => {
      if (event.target.id === "kbModalOverlay") {
        closeKbEditor();
      }
    };

    const form = document.getElementById("kbEditorForm");
    form.onsubmit = async event => {
      event.preventDefault();
      const formData = new FormData(form);
      const payload = {
        name: String(formData.get("name") || "").trim(),
        description: String(formData.get("description") || "").trim(),
        sync_type: String(formData.get("sync_type") || "manual"),
        content: String(formData.get("content") || ""),
      };
      if (!payload.name) {
        alert("请输入知识名称。");
        return;
      }
      if (payload.sync_type === "manual" && !payload.content.trim()) {
        alert("手工知识需要填写正文内容。");
        return;
      }

      if (existingAsset) {
        await api(`/api/knowledge_bases/${existingAsset.source_ref}`, {
          method: "PATCH",
          body: JSON.stringify(payload),
        });
      } else {
        const created = await api("/api/knowledge_bases", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        state.selectedId = "";
        state.pendingSourceRef = created.id || "";
      }

      closeKbEditor();
      await loadAll(existingAsset?.asset_id || "");
    };

    const btnDeleteKb = document.getElementById("btnDeleteKb");
    if (btnDeleteKb && existingAsset) {
      btnDeleteKb.onclick = async () => {
        const confirmed = window.confirm(`确定删除企业知识“${existingAsset.title}”吗？`);
        if (!confirmed) {
          return;
        }
        await api(`/api/knowledge_bases/${existingAsset.source_ref}`, { method: "DELETE" });
        closeKbEditor();
        state.selectedId = "";
        await loadAll();
      };
    }
  }

  function renderDetail(asset, content) {
    const mountable = ["enterprise_kb", "experience"].includes(asset.asset_type);
    const mounted = new Set((asset.mounted_sandboxes || []).map(item => item.sandbox_id));
    const isEnterpriseKb = asset.asset_type === "enterprise_kb";
    detailPanel.innerHTML = `
      <h3 class="detail-title">${escapeHtml(asset.title)}</h3>
      <div class="detail-meta">
        <span class="badge-pill">${typeText(asset.asset_type)}</span>
        <span class="badge-pill ${badgeClass(asset)}">${statusText(asset)}</span>
        <span class="badge-pill">Embedding ${asset.embedding_count || 0}</span>
        <span class="badge-pill">版本 ${asset.index_version || 0}</span>
      </div>
      <div class="row-actions">
        <button class="btn btn-outline" id="btnReadFull"><i class="fa-regular fa-file-lines"></i> 查看完整内容</button>
        <button class="btn btn-outline" id="btnReindex"><i class="fa-solid fa-arrows-rotate"></i> 重建索引</button>
        ${isEnterpriseKb ? `<button class="btn btn-outline" id="btnEditKb"><i class="fa-regular fa-pen-to-square"></i> 编辑知识</button>` : ""}
        <a class="btn btn-outline" href="/knowledge-index"><i class="fa-solid fa-compass"></i> 打开索引页</a>
      </div>
      <div class="detail-section">
        <h4>内容预览</h4>
        <div class="detail-content" id="assetContentPreview">${escapeHtml(content.content || asset.content_preview || "暂无内容")}</div>
        ${content.truncated ? `<div style="margin-top:10px;color:#64748b;font-size:12px;">当前为截断预览，可继续读取完整原文。</div>` : ""}
      </div>
      <div class="detail-section">
        <h4>来源与检索</h4>
        <div class="detail-content">source_ref: ${escapeHtml(asset.source_ref || "")}
source_path: ${escapeHtml(asset.source_path || "")}
locator: ${escapeHtml(asset.full_document_locator || "")}
last_indexed_at: ${escapeHtml(asset.last_indexed_at || "")}
last_error: ${escapeHtml(asset.last_error || "")}</div>
      </div>
      <div class="detail-section">
        <h4>挂载工作空间</h4>
        ${mountable ? `
          <div class="mount-list">
            ${state.sandboxes.map(sandbox => `
              <label class="mount-item">
                <input type="checkbox" class="mount-checkbox" value="${sandbox.sandbox_id}" ${mounted.has(sandbox.sandbox_id) ? "checked" : ""} />
                <span>${escapeHtml(sandbox.name)} <span style="color:#64748b;font-size:12px;">(${escapeHtml((sandbox.allowed_groups || []).join(", "))})</span></span>
              </label>
            `).join("")}
          </div>
          <div class="row-actions"><button class="btn btn-primary" id="btnSaveMounts">保存挂载</button></div>
        ` : `<div class="detail-content">该资产由工作空间原生拥有，无需额外挂载。</div>`}
      </div>
    `;

    document.getElementById("btnReadFull").onclick = async () => {
      const full = await loadContent(asset.asset_id, "full");
      const previewEl = document.getElementById("assetContentPreview");
      if (previewEl) {
        previewEl.textContent = full.content || "";
      }
    };
    document.getElementById("btnReindex").onclick = async () => {
      await api(`/api/knowledge/index/assets/${asset.asset_id}/reindex`, { method: "POST" });
      await loadAll(asset.asset_id);
    };
    if (isEnterpriseKb) {
      document.getElementById("btnEditKb").onclick = async () => openKbEditor(asset);
    }
    if (mountable) {
      document.getElementById("btnSaveMounts").onclick = async () => saveMounts(asset);
    }
  }

  async function loadDetail(assetId) {
    const detail = await api(`/api/knowledge/assets/${assetId}`);
    const content = await loadContent(assetId, "preview");
    state.detail = detail;
    renderDetail(detail, content);
  }

  async function loadAll(selectedId = "") {
    const [me, assetRes, sandboxRes, pendingRes] = await Promise.all([
      api("/api/me"),
      api("/api/knowledge/assets"),
      api("/api/sandboxes"),
      api("/api/knowledge/experiences/pending"),
    ]);
    userInfo.textContent = `${me.user.display_name} (${(me.user.groups || []).join(", ")})`;
    state.assets = assetRes.assets || [];
    state.sandboxes = sandboxRes.sandboxes || [];
    state.pendingExperiences = pendingRes.pending_experiences || [];
    const pendingAsset = state.pendingSourceRef
      ? state.assets.find(asset => asset.source_ref === state.pendingSourceRef)
      : null;
    state.selectedId = selectedId || pendingAsset?.asset_id || state.selectedId || (state.assets[0]?.asset_id || "");
    state.pendingSourceRef = "";
    renderStats();
    renderPendingExperiences();
    renderTabs();
    applyFilters();
    if (state.selectedId) {
      await loadDetail(state.selectedId);
    } else {
      detailPanel.innerHTML = `<div class="detail-empty">当前还没有可展示的知识资产。</div>`;
    }
  }

  assetSearch.oninput = applyFilters;
  document.getElementById("btnRefresh").onclick = async () => loadAll(state.selectedId);
  btnCreateKb.onclick = async () => openKbEditor();
  assetListViewport.addEventListener("scroll", scheduleAssetVirtualRender, { passive: true });
  assetList.addEventListener("click", async event => {
    const card = event.target.closest(".asset-card");
    if (!card) {
      return;
    }
    state.selectedId = card.dataset.id;
    renderAssetList();
    await loadDetail(state.selectedId);
  });
  pendingExperienceViewport.addEventListener("scroll", schedulePendingVirtualRender, { passive: true });
  pendingExperienceList.addEventListener("click", async event => {
    const reviewBtn = event.target.closest(".btn-review-pending");
    if (reviewBtn) {
      const pending = state.pendingExperiences.find(item => item.proposal_id === reviewBtn.dataset.proposalId);
      if (pending) {
        await openPendingEditor(pending);
      }
      return;
    }
    const dismissBtn = event.target.closest(".btn-dismiss-pending");
    if (dismissBtn) {
      await api(`/api/knowledge/experiences/${dismissBtn.dataset.proposalId}/dismiss`, { method: "POST" });
      await loadAll(state.selectedId);
    }
  });
  window.addEventListener("resize", () => {
    scheduleAssetVirtualRender();
    schedulePendingVirtualRender();
  });

  try {
    await loadAll();
  } catch (error) {
    detailPanel.innerHTML = `<div class="detail-empty">加载失败：${escapeHtml(error.message)}</div>`;
    assetList.innerHTML = `<div class="empty-state">加载失败：${escapeHtml(error.message)}</div>`;
    pendingExperienceList.innerHTML = `<div class="empty-state">加载失败：${escapeHtml(error.message)}</div>`;
  }
});
