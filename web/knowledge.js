document.addEventListener("DOMContentLoaded", async () => {
  const kbList = document.getElementById("kbList");
  const btnCreateKB = document.getElementById("btnCreateKB");
  const kbModal = document.getElementById("kbModal");
  const userInfo = document.getElementById("userInfo");

  // Auth / Profile Header
  api("/api/me").then(me => {
    userInfo.textContent = `${me.user.display_name} (${me.user.groups.join(", ")})`;
  }).catch(() => {
    window.location.href = "/web/login.html";
  });

  // Modal Fields
  const kbIdInput = document.getElementById("kbIdInput");
  const kbNameInput = document.getElementById("kbNameInput");
  const kbDescInput = document.getElementById("kbDescInput");
  const kbContentInput = document.getElementById("kbContentInput");
  const apiUrlInput = document.getElementById("apiUrlInput");
  const apiMethodInput = document.getElementById("apiMethodInput");
  const apiHeadersInput = document.getElementById("apiHeadersInput");
  const apiParamsInput = document.getElementById("apiParamsInput");
  const apiJsonPathInput = document.getElementById("apiJsonPathInput");
  const radios = document.getElementsByName("syncType");
  const manualSection = document.getElementById("manualSection");
  const apiSection = document.getElementById("apiSection");
  const kbTokenHint = document.getElementById("kbTokenHint");
  
  function getSyncType() {
    for (const r of radios) { if (r.checked) return r.value; }
    return "manual";
  }

  function setSyncType(val) {
    for (const r of radios) { r.checked = (r.value === val); }
    toggleSections();
  }

  function toggleSections() {
    if (getSyncType() === "manual") {
      manualSection.style.display = "block";
      apiSection.style.display = "none";
    } else {
      manualSection.style.display = "block"; // Still show content as read-only preview or fallback
      kbContentInput.placeholder = i18n.t("api_pull_hint") || "点击列表中的同步按钮，成功后数据将覆写于此。";
      apiSection.style.display = "block";
    }
  }

  Array.from(radios).forEach(r => r.addEventListener("change", toggleSections));

  // Estimate Token rough logic
  kbContentInput.addEventListener("input", () => {
     const len = kbContentInput.value.length;
     const count = Math.floor(len * 0.75);
     kbTokenHint.textContent = (i18n.t("estimated_tokens") || "预估 Tokens: {count}").replace("{count}", count);
  });

  async function loadKBs() {
    try {
      const res = await api("/api/knowledge_bases");
      const kbs = res.knowledge_bases || [];
      
      kbList.innerHTML = "";
      if (kbs.length === 0) {
         kbList.innerHTML = `<div style="text-align:center; padding: 40px; color: #94a3b8; border: 1px dashed #cbd5e1; border-radius: 8px;">${i18n.t("no_kbs") || "没有任何知识库记录，点击上方按钮新建一个。"}</div>`;
         return;
      }
      
      kbs.forEach(kb => {
        const div = document.createElement("div");
        div.className = "kb-card";
        div.innerHTML = `
          <div class="kb-header">
            <div>
              <h3 class="kb-title">${escapeHtml(kb.name)}</h3>
              <p class="kb-desc">${escapeHtml(kb.description || "暂无描述")}</p>
            </div>
            <div class="kb-actions">
              ${kb.sync_type === "api" ? `<button class="btn btn-outline btn-sm" onclick="syncKB('${kb.id}')" title="拉取数据" style="border-radius: 20px;"><i class="fa-solid fa-arrows-rotate"></i></button>` : ""}
              <button class="btn btn-outline btn-sm" onclick="editKB('${kb.id}')" style="border-color: #3b82f6; color: #3b82f6; border-radius: 20px;"><i class="fa-solid fa-pen"></i></button>
              <button class="btn btn-outline btn-sm" onclick="deleteKB('${kb.id}')" style="border-color: #ef4444; color: #ef4444; border-radius: 20px;"><i class="fa-solid fa-trash"></i></button>
            </div>
          </div>
          <div class="kb-meta">
            <span title="类型"><i class="fa-solid ${kb.sync_type === 'api' ? 'fa-cloud' : 'fa-keyboard'}"></i> ${kb.sync_type === 'api' ? 'API 同步' : '手动文本'}</span>
            <span title="预估容量" style="color:#f59e0b;"><i class="fa-solid fa-coins"></i> ~${kb.token_count || 0} Tokens</span>
            <span title="更新时间"><i class="fa-regular fa-clock"></i> ${new Date(kb.updated_at).toLocaleString()}</span>
          </div>
        `;
        kbList.appendChild(div);
      });
    } catch(e) {
      kbList.innerHTML = `<div style="color:red; padding:20px;">加载失败: ${e.message}</div>`;
    }
  }

  btnCreateKB.onclick = () => {
    document.getElementById("kbModalTitle").textContent = i18n.t("create_kb") || "新建知识库";
    kbIdInput.value = "";
    kbNameInput.value = "";
    kbDescInput.value = "";
    kbContentInput.value = "";
    apiUrlInput.value = "";
    apiJsonPathInput.value = "";
    apiHeadersInput.value = "";
    apiParamsInput.value = "";
    setSyncType("manual");
    kbTokenHint.textContent = "当前令牌数估计: 0 Tokens";
    kbModal.style.display = "flex";
  };

  document.getElementById("btnSaveKB").onclick = async () => {
    const id = kbIdInput.value;
    const body = {
      name: kbNameInput.value.trim(),
      description: kbDescInput.value.trim(),
      sync_type: getSyncType(),
      content: kbContentInput.value,
      api_url: apiUrlInput.value.trim(),
      api_method: apiMethodInput.value,
      api_headers: apiHeadersInput.value ? JSON.parse(apiHeadersInput.value) : {},
      api_params: apiParamsInput.value ? JSON.parse(apiParamsInput.value) : {},
      api_json_path: apiJsonPathInput.value.trim()
    };
    
    if(!body.name) return alert("名称不能为空");

    try {
      const btn = document.getElementById("btnSaveKB");
      btn.disabled = true;
      btn.textContent = i18n.t("saving") || "保存中...";
      
      if(id) {
         await api(`/api/knowledge_bases/${id}`, { method: "PATCH", body: JSON.stringify(body) });
      } else {
         await api(`/api/knowledge_bases`, { method: "POST", body: JSON.stringify(body) });
      }
      
      kbModal.style.display = "none";
      loadKBs();
    } catch(e) {
      alert("保存失败: " + e.message);
    } finally {
      const btn = document.getElementById("btnSaveKB");
      btn.disabled = false;
      btn.textContent = i18n.t("save") || "保存";
    }
  };

  window.editKB = async (id) => {
    try {
      // Find row in global data or let's just cheat and fetch everything 
      const res = await api("/api/knowledge_bases");
      const kb = res.knowledge_bases.find(k => k.id === id);
      if(!kb) return;
      
      document.getElementById("kbModalTitle").textContent = "编辑知识库";
      kbIdInput.value = kb.id;
      kbNameInput.value = kb.name || "";
      kbDescInput.value = kb.description || "";
      kbContentInput.value = kb.content || "";
      setSyncType(kb.sync_type || "manual");
      
      apiUrlInput.value = kb.api_url || "";
      apiMethodInput.value = kb.api_method || "GET";
      apiHeadersInput.value = (kb.api_headers && Object.keys(kb.api_headers).length > 0) ? JSON.stringify(kb.api_headers, null, 2) : "";
      apiParamsInput.value = (kb.api_params && Object.keys(kb.api_params).length > 0) ? JSON.stringify(kb.api_params, null, 2) : "";
      apiJsonPathInput.value = kb.api_json_path || "";
      
      // Trigger token event manually
      kbContentInput.dispatchEvent(new Event("input"));
      
      kbModal.style.display = "flex";
    } catch(e) {
      alert("编辑失败: " + e.message);
    }
  };

  window.deleteKB = async (id) => {
    if(!confirm("确定要删除该知识库吗？ (已关联沙盒中的该库也将失效)")) return;
    try {
      await api(`/api/knowledge_bases/${id}`, { method: "DELETE" });
      loadKBs();
    } catch(e) {
      alert("删除失败: " + e.message);
    }
  };

  window.syncKB = async (id) => {
    try {
      const btn = document.querySelector(`button[onclick="syncKB('${id}')"]`);
      const ogHtml = btn.innerHTML;
      btn.disabled = true;
      btn.innerHTML = `<i class="fa-solid fa-circle-notch fa-spin"></i>`;
      
      const res = await api(`/api/knowledge_bases/${id}/sync`, { method: "POST" });
      alert("同步成功，预估包含 Tokens：" + res.token_count);
      loadKBs();
    } catch(e) {
      alert("同步失败: " + e.message);
      // Ensure the button is enabled again if it failed
      loadKBs();
    }
  };

  function escapeHtml(unsafe) {
    if (!unsafe) return "";
    return unsafe
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  // Initial load
  loadKBs();
});
