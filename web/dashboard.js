let sessionId = "";

let lastProposalId = "";

let sandboxesData = [];
let dbConnectionsData = [];
let currentDbMountTableNames = [];
let currentDbMountTableFilter = "";

let uploadedFiles = [];

let currentEditingSkillId = ""; // skill being edited, or "" for create mode

let skillModal = null; // Global reference for the skill detail modal
let skillSourceSessionId = "";

let activeAnalysisController = null;
let activeAnalysisMode = "";
let activeAnalysisWrapper = null;

const userInfo = document.getElementById("userInfo");

const tableList = document.getElementById("tableList");

const sandboxSelect = document.getElementById("sandboxSelect");

const cards = document.getElementById("cards");

const skillList = document.getElementById("skillList");

const sessionList = document.getElementById("sessionList");



function scrollToBottom() {

  const chatContainer = document.querySelector(".chat-container");

  if (chatContainer) {

    chatContainer.scrollTop = chatContainer.scrollHeight;

  }

}



function createAiMessageContainer() {

  const row = document.createElement("div");

  row.className = "message-row ai";



  const wrapper = document.createElement("div");

  wrapper.className = "ai-card-wrapper";



  row.appendChild(wrapper);

  cards.appendChild(row);

  scrollToBottom();



  return wrapper;

}



function updateAiCard(wrapper, title, html, thought = null) {

  let icon = '<i class="fa-solid fa-circle-info"></i>';

  if (title.includes(i18n.t("thought") || "思考")) icon = '<i class="fa-solid fa-brain"></i>';

  if (title.includes(i18n.t("conclusion") || "结论")) icon = '<i class="fa-solid fa-lightbulb"></i>';

  if (title.includes(i18n.t("feedback") || "反馈")) icon = '<i class="fa-solid fa-comment-dots"></i>';

  if (title.includes(i18n.t("knowledge") || "知识")) icon = '<i class="fa-solid fa-book-open"></i>';

  if (title.includes(i18n.t("failed") || "失败") || title.includes(i18n.t("error") || "错误")) icon = '<i class="fa-solid fa-circle-exclamation" style="color:#ef4444"></i>';



  let thoughtHtml = "";

  if (thought) {

    thoughtHtml = `<div class="thought-process">

          <div class="thought-label"><i class="fa-solid fa-brain"></i> ${i18n.t('thought_process')}</div>

          <div class="thought-content" style="white-space: pre-wrap;">${thought}</div>

      </div>`;

  }



  wrapper.innerHTML = `<div class="card"><h3>${icon} ${i18n.t(title) || title}</h3>${thoughtHtml}${html}</div>`;

  scrollToBottom();

}

function getStopButtonText() {
  return (window.i18n && i18n.lang) === "en" ? "Stop" : "停止";
}

function setAnalysisRunningState(running, mode = "", wrapper = null) {
  const sendBtn = document.getElementById("sendBtn");
  const autoBtn = document.getElementById("autoAnalyzeBtn");
  const stopText = getStopButtonText();

  if (sendBtn) {
    if (!sendBtn.dataset.normalLabel) sendBtn.dataset.normalLabel = sendBtn.textContent || "发送";
    sendBtn.textContent = running ? stopText : sendBtn.dataset.normalLabel;
    sendBtn.classList.toggle("btn-danger", running);
    sendBtn.classList.toggle("btn-primary", !running);
    sendBtn.disabled = false;
  }

  if (autoBtn) {
    if (!autoBtn.dataset.normalLabel) autoBtn.dataset.normalLabel = autoBtn.textContent || "一键分析";
    autoBtn.disabled = running;
    autoBtn.textContent = autoBtn.dataset.normalLabel;
    autoBtn.classList.toggle("btn-disabled", running);
  }

  if (running) {
    activeAnalysisMode = mode || activeAnalysisMode;
    activeAnalysisWrapper = wrapper || activeAnalysisWrapper;
  } else {
    activeAnalysisMode = "";
    activeAnalysisWrapper = null;
  }
}

function stopActiveAnalysis() {
  if (activeAnalysisController) {
    activeAnalysisController.abort();
  }
}

function releaseAnalysisControls(controller = null) {
  if (!controller || activeAnalysisController === controller) {
    activeAnalysisController = null;
    activeAnalysisMode = "";
    activeAnalysisWrapper = null;
    setAnalysisRunningState(false);
  }
}

function normalizeStaticText() {

  document.title = i18n.t("app_title") || "SakuFox 🦊 - 敏捷智能数据分析平台";

  const langLabel = document.getElementById("langLabel");

  if (langLabel) {

    langLabel.textContent = "English / 中文";

  }

  const navAnalysis = document.querySelector('[data-i18n="nav_analysis"]');
  const navKnowledge = document.querySelector('[data-i18n="nav_knowledge"]');

  if (navAnalysis) navAnalysis.textContent = i18n.t("nav_analysis") || "数据分析";
  if (navKnowledge) navKnowledge.textContent = i18n.t("nav_knowledge") || "知识库配置";
}

function setSessionIdInUrl(value) {
  const url = new URL(window.location.href);
  if (value) url.searchParams.set("session_id", value);
  else url.searchParams.delete("session_id");
  window.history.replaceState({}, "", url.toString());
}

function getSessionIdFromUrl() {
  const url = new URL(window.location.href);
  return (url.searchParams.get("session_id") || "").trim();
}

function tr(key, fallback, params = {}) {
  const text = i18n.t(key, params);
  if (!text || text === key) return fallback;
  return text;
}

function renderSkillContextSnapshot(snapshot) {
  const contentEl = document.getElementById("skillContextSnapshotContent");
  const jumpBtn = document.getElementById("jumpSourceSessionBtn");
  if (!contentEl || !jumpBtn) return;

  skillSourceSessionId = "";
  jumpBtn.style.display = "none";

  if (!snapshot || typeof snapshot !== "object") {
    contentEl.innerHTML = `<div class="empty-state">${tr("no_context_snapshot", "该经验暂无来源对话元数据")}</div>`;
    return;
  }

  const source = snapshot.source || {};
  const db = snapshot.database || {};
  const tables = snapshot.tables || {};
  const mountedSkills = snapshot.mounted_skills || [];
  const knowledgeBases = snapshot.knowledge_bases || [];
  const files = snapshot.files || [];
  const sessionPatches = snapshot.session_patches || [];
  const contextSources = snapshot.context_sources || {};

  const sourceSessionId = (source.session_id || "").trim();
  skillSourceSessionId = sourceSessionId;
  if (sourceSessionId) {
    jumpBtn.style.display = "block";
  }

  const mountedText = mountedSkills.length
    ? mountedSkills.map(item => escapeHtml(`${item.name || item.skill_id || ""}${item.version ? ` (v${item.version})` : ""}`)).join(", ")
    : "-";
  const kbText = knowledgeBases.length
    ? knowledgeBases.map(item => escapeHtml(`${item.name || item.id || ""}${item.sync_type ? ` [${item.sync_type}]` : ""}`)).join(", ")
    : "-";
  const fileText = files.length
    ? files.map(item => escapeHtml(`${item.name || ""}${item.selected ? " (selected)" : ""}`)).join(", ")
    : "-";
  const patchText = sessionPatches.length ? escapeHtml(sessionPatches.join(" | ")) : "-";
  const selectedTablesText = (tables.selected_tables || []).length ? escapeHtml((tables.selected_tables || []).join(", ")) : "-";
  const sandboxTablesText = (tables.sandbox_tables || []).length ? escapeHtml((tables.sandbox_tables || []).join(", ")) : "-";
  const sourceFlags = Object.keys(contextSources)
    .filter(key => !!contextSources[key])
    .join(", ") || "-";
  const dbText = db && (db.db_type || db.database)
    ? escapeHtml(`${db.db_type || ""} ${db.host || ""}${db.port ? `:${db.port}` : ""} / ${db.database || ""}`.trim())
    : "-";

  contentEl.innerHTML = `
    <div><strong>${tr("source_conversation", "来源会话")}:</strong> ${escapeHtml(source.session_title || "-")}</div>
    <div><strong>${tr("session_id_label", "会话ID")}:</strong> ${escapeHtml(sourceSessionId || "-")}</div>
    <div><strong>${tr("sandbox_label", "沙盒")}:</strong> ${escapeHtml(source.sandbox_name || source.sandbox_id || "-")}</div>
    <div><strong>${tr("db_label", "数据库")}:</strong> ${dbText}</div>
    <div><strong>${tr("selected_tables_label", "选中表")}:</strong> ${selectedTablesText}</div>
    <div><strong>${tr("sandbox_tables_label", "沙盒表")}:</strong> ${sandboxTablesText}</div>
    <div><strong>${tr("mounted_skills_label", "挂载经验")}:</strong> ${mountedText}</div>
    <div><strong>${tr("knowledge_bases_label", "知识库")}:</strong> ${kbText}</div>
    <div><strong>${tr("related_files_label", "关联文件")}:</strong> ${fileText}</div>
    <div><strong>${tr("session_patches_label", "会话补丁")}:</strong> ${patchText}</div>
    <div><strong>${tr("context_sources_label", "上下文来源")}:</strong> ${escapeHtml(sourceFlags)}</div>
  `;
}



function addCard(title, html, thought = null) {

  const wrapper = createAiMessageContainer();

  updateAiCard(wrapper, title, html, thought);

}



function addUserMessage(text) {

  const row = document.createElement("div");

  row.className = "message-row user";

  const bubble = document.createElement("div");

  bubble.className = "user-bubble";

  bubble.textContent = text;

  row.appendChild(bubble);

  cards.appendChild(row);

  scrollToBottom();

}



async function refreshProfile(selectId = null) {

  try {

    const me = await api("/api/me");

    userInfo.textContent = `${me.user.display_name} (${me.user.groups.join(", ")})`;



    const sandboxesRes = await api("/api/sandboxes");

    sandboxesData = sandboxesRes.sandboxes || [];



    // Use selectId if provided, otherwise stick to current selection

    const currentSandboxId = selectId || sandboxSelect.value;



    sandboxSelect.innerHTML = "";

    if (sandboxesData.length === 0) {

      const opt = document.createElement("option");

      opt.textContent = i18n.t("no_sandbox");

      opt.disabled = true;

      opt.selected = true;

      sandboxSelect.appendChild(opt);

    } else {

      sandboxesData.forEach((s) => {

        const opt = document.createElement("option");

        opt.value = s.sandbox_id;

        opt.textContent = s.name; // Removed the annoying [table] suffix

        sandboxSelect.appendChild(opt);

      });



      // Restore selection if it still exists

      if (currentSandboxId && sandboxesData.find(s => s.sandbox_id === currentSandboxId)) {

        sandboxSelect.value = currentSandboxId;

      }

    }



    renderDataModels();

    await refreshSkills();

    updateMountedSkillSummary();
    updateMountedDbSummary();
  } catch (e) {

    console.error(i18n.t("load_config_failed") || "加载配置失败", e);

  }

}



function renderDataModels() {

  const currentSandboxId = sandboxSelect.value;

  const btnRename = document.getElementById("btnRenameSandbox");

  const btnDelete = document.getElementById("btnDeleteSandbox");



  if (currentSandboxId) {

    btnRename.disabled = false;

    btnDelete.disabled = false;

  } else {

    btnRename.disabled = true;

    btnDelete.disabled = true;

  }



  const currentSandbox = sandboxesData.find(s => s.sandbox_id === currentSandboxId);



  tableList.innerHTML = "";

  let hasItems = false;



  // 1. Render DB Tables from current sandbox

  if (currentSandbox && currentSandbox.tables && currentSandbox.tables.length > 0) {

    hasItems = true;

    currentSandbox.tables.forEach(t => {

      const div = document.createElement("div");

      div.style.marginBottom = "6px";

      div.style.marginTop = "6px";

      div.style.paddingLeft = "16px";



      const cb = document.createElement("input");

      cb.type = "checkbox";

      cb.value = t;

      cb.id = `chk_table_${t}`;

      cb.style.marginRight = "6px";

      cb.className = "db-table-checkbox-sidebar";

      cb.checked = true;



      const label = document.createElement("label");

      label.htmlFor = `chk_table_${t}`;

      label.style.cursor = "pointer";

      label.style.fontSize = "13px";

      label.innerHTML = `<i class="fa-solid fa-table-columns" style="color:#3b82f6;"></i> ${t}`;



      div.appendChild(cb);

      div.appendChild(label);

      tableList.appendChild(div);

    });

  }



  // 2. Render Uploaded files

  if (uploadedFiles && uploadedFiles.length > 0) {

    hasItems = true;

    uploadedFiles.forEach(f => {

      const div = document.createElement("div");

      div.style.marginBottom = "6px";

      div.style.marginTop = "6px";

      div.style.paddingLeft = "16px";

      const cb = document.createElement("input");

      cb.type = "checkbox";

      cb.value = f.dataset_name;

      cb.id = `chk_file_${f.dataset_name}`;

      cb.style.marginRight = "6px";

      cb.className = "uploaded-file-checkbox";

      cb.checked = true;



      const label = document.createElement("label");

      label.htmlFor = `chk_file_${f.dataset_name}`;

      label.style.cursor = "pointer";

      label.style.fontSize = "13px";



      let text = f.dataset_name;

      if (f.is_tabular) {

        text += ` (${f.rows}${i18n.t('rows')})`;

      } else {

        text += ` (${i18n.t('knowledge_doc')})`;

      }

      label.innerHTML = `<i class="fa-solid fa-file-csv" style="color:#10b981;"></i> ${text}`;

      label.title = f.is_tabular ? `${i18n.t("columns") || "列"}: ${f.columns.join(", ")}` : (i18n.t("doc_ai_hint") || "文档内容将在提问时交给 AI 分析");



      div.appendChild(cb);

      div.appendChild(label);

      tableList.appendChild(div);

    });

  }



  if (!hasItems) {

    tableList.innerHTML = `<li class="empty-state" style="padding-left:16px;">${i18n.t('no_data')}</li>`;

  }

}



sandboxSelect.addEventListener('change', () => {

  renderDataModels();

  refreshSkills();

  updateMountedSkillSummary();
  updateMountedDbSummary();
});



function getCurrentSandbox() {

  return sandboxesData.find(s => s.sandbox_id === sandboxSelect.value) || null;

}


function updateMountedSkillSummary() {

  const summary = document.getElementById("mountedSkillSummary");

  if (!summary) return;

  const currentSandbox = getCurrentSandbox();

  if (!currentSandbox) {

    summary.textContent = i18n.t("select_sandbox_first") || "请先选择沙盒";

    return;

  }

  const count = (currentSandbox.mounted_skills || []).length;

  summary.textContent = i18n.t("mounted_skill_count", { count }) || `当前沙盒已挂载 ${count} 条经验`;

}

function updateMountedDbSummary() {
  const summary = document.getElementById("mountedDbSummary");
  if (!summary) return;
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) {
    summary.textContent = i18n.t("select_sandbox_first") || "请先选择沙盒";
    return;
  }
  if (!currentSandbox.db_connection) {
    summary.textContent = i18n.t("db_not_mounted") || "当前沙盒未挂载数据库连接";
    return;
  }
  const conn = currentSandbox.db_connection;
  summary.textContent = i18n.t("db_mounted_summary", { name: conn.name }) || `当前挂载: ${conn.name}`;
}


async function refreshSkills() {
  const skills = await api("/api/skills");

  const currentSandbox = getCurrentSandbox();

  const mountedSkillIds = new Set(currentSandbox?.mounted_skills || []);
  skillList.innerHTML = "";

  if (skills.skills.length === 0) {

    skillList.innerHTML = `<li class="empty-state">${i18n.t('no_skills')}</li>`;

  } else {

    skills.skills.forEach((s) => {

      const li = document.createElement("li");

      li.className = "skill-item";

      li.title = i18n.t("click_edit_skill") || "点击查看或修改经验";

      li.innerHTML = `

        <div class="skill-item-header">

          <span class="skill-item-title">${escapeHtml(s.name)}</span>

          ${s.version ? `<span class="badge" style="font-size:10px; padding:2px 4px; border-radius:4px; border:none; background:#e0e7ff; color:#4f46e5; margin-left:6px;">v${s.version}</span>` : ''}

          ${mountedSkillIds.has(s.skill_id) ? `<span class="badge" style="font-size:10px; padding:2px 6px; border-radius:999px; border:none; background:#dcfce7; color:#166534; margin-left:6px;">${i18n.t('mounted_skill') || '已挂载'}</span>` : ''}
          <div class="delete-btn-round delete-icon" title="${i18n.t('delete')}">

            <i class="fa-solid fa-xmark"></i>

          </div>

        </div>

        <div class="skill-item-meta">

          <span><i class="fa-solid fa-tag"></i> ${(s.tags || []).slice(0, 2).join(", ") || i18n.t('no_tags')}</span>

          <i class="fa-solid fa-pen-to-square" style="font-size: 11px; opacity: 0.6;"></i>

        </div>

      `;

      li.onclick = () => loadSkillIntoForm(s.skill_id, s);

      

      const deleteBtn = li.querySelector(".delete-icon");

      deleteBtn.onclick = async (e) => {

        e.stopPropagation();

        if (!confirm(i18n.t("confirm_delete_skill", { name: s.name }))) return;

        try {

          await api(`/api/skills/${s.skill_id}`, { method: "DELETE" });

          await refreshSkills();

        } catch (err) {

          alert((i18n.t("delete_failed") || "删除失败") + ": " + err.message);

        }

      };

      

      skillList.appendChild(li);

    });

  }

  updateMountedSkillSummary();
}





function loadSkillIntoForm(skillId, skill) {
  currentEditingSkillId = skillId;



  const overwriteGroup = document.getElementById("overwriteSkillGroup");

  if (overwriteGroup) overwriteGroup.style.display = "none";



  if (skillModal) skillModal.style.display = "flex";



  // Populate form

  document.getElementById("skillNameInput").value = skill.name || "";

  if (document.getElementById("skillDescInput")) document.getElementById("skillDescInput").value = skill.description || "";

  if (document.getElementById("skillTagsInput")) document.getElementById("skillTagsInput").value = (skill.tags || []).join(", ");

  // Knowledge from layers.knowledge

  const knowledge = (skill.layers?.knowledge || []).join("\n");

  if (document.getElementById("skillKnowledgeInput")) document.getElementById("skillKnowledgeInput").value = knowledge;
  renderSkillContextSnapshot(skill.layers?.context_snapshot || null);



  // Switch button label to edit mode

  const btn = document.getElementById("saveSkillBtn");

  btn.innerHTML = `<i class="fa-solid fa-pen-to-square"></i> ${i18n.t('update_skill')}`;

  btn.style.borderColor = "var(--accent, #6366f1)";



  // Add cancel link if not already present

  let cancelLink = document.getElementById("skillEditCancelLink");

  if (!cancelLink) {

    cancelLink = document.createElement("a");

    cancelLink.id = "skillEditCancelLink";

    cancelLink.href = "#";

    cancelLink.style.cssText = "font-size:12px;color:var(--text-muted);text-align:center;display:block;margin-top:4px;";

    cancelLink.textContent = i18n.t('cancel_edit');

    cancelLink.onclick = (e) => { e.preventDefault(); cancelSkillEdit(); };

    btn.parentNode.insertBefore(cancelLink, btn.nextSibling);

  }



  // No need to scroll as it's a modal now

  document.getElementById("skillNameInput").focus();

}



function cancelSkillEdit() {

  currentEditingSkillId = "";

  document.getElementById("skillNameInput").value = "";

  if (document.getElementById("skillDescInput")) document.getElementById("skillDescInput").value = "";

  if (document.getElementById("skillTagsInput")) document.getElementById("skillTagsInput").value = "";

  if (document.getElementById("skillKnowledgeInput")) document.getElementById("skillKnowledgeInput").value = "";
  renderSkillContextSnapshot(null);

  if (skillModal) skillModal.style.display = "none";

}



async function refreshSessions() {

  if (!sessionList) return;

  try {

    const res = await api("/api/chat/sessions");

    sessionList.innerHTML = "";

    if (!res.sessions || res.sessions.length === 0) {

      sessionList.innerHTML = `<li class="empty-state">${i18n.t('no_history')}</li>`;

      return;

    }

    res.sessions.forEach(sess => {

      const li = document.createElement("li");

      if (sess.session_id === sessionId) {

        li.className = "active";

      }

      const date = sess.created_at ? new Date(sess.created_at).toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "";

      li.innerHTML = `

        <div class="session-item-header">

          <span class="session-item-title">${escapeHtml(sess.title || i18n.t('new_session'))}</span>

          <div class="delete-btn-round delete-session-btn" title="${i18n.t('delete')}">

            <i class="fa-solid fa-xmark"></i>

          </div>

        </div>

        <div class="session-item-meta">

          <span><i class="fa-solid fa-comments"></i> ${i18n.t('iterations_count', { count: sess.iteration_count })}</span>

          <span>${date}</span>

        </div>

      `;

      li.onclick = () => switchSession(sess.session_id);



      const delBtn = li.querySelector(".delete-session-btn");

      delBtn.onclick = async (e) => {

        e.stopPropagation();

        if (!confirm(i18n.t('confirm_delete_session'))) return;

        try {

          await api(`/api/chat/sessions/${sess.session_id}`, { method: "DELETE" });

          if (sessionId === sess.session_id) {

            startNewSession();

          } else {

            refreshSessions();

          }

        } catch (err) {

          alert((i18n.t("delete_failed") || "删除失败") + ": " + err.message);

        }

      };



      sessionList.appendChild(li);

    });

  } catch (e) {

    console.error("refreshSessions error", e);

  }

}



async function switchSession(targetSessionId) {

  if (targetSessionId === sessionId) return;

  sessionId = targetSessionId;
  setSessionIdInUrl(targetSessionId);

  lastProposalId = "";



  cards.innerHTML = `<div style="padding:20px;color:var(--text-muted);text-align:center;"><i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("loading_history") || "正在加载历史对话..."}</div>`;

  refreshSessions();



  try {

    const res = await api(`/api/chat/history?session_id=${targetSessionId}`);

    cards.innerHTML = "";

    lastProposalId = res.last_proposal_id || ""; // Sync for skill extraction from history



    if (!res.iterations || res.iterations.length === 0) {

      cards.innerHTML = `<div class="welcome-card"><h3>${i18n.t('empty_chat')}</h3><p>${i18n.t('no_record')}</p></div>`;

      return;

    }



    res.iterations.forEach(iter => {

      // 1. User message bubble

      if (iter.message) addUserMessage(iter.message);

      if (Array.isArray(iter.loop_rounds) && iter.loop_rounds.length > 0) {

        const wrapper = createAiMessageContainer();

        renderAutoAnalysisCard(wrapper, {
          mode: iter.mode === "auto_analysis" ? "auto" : "iterate",
          title: iter.mode === "auto_analysis" ? "一键分析" : (i18n.t("analysis_conclusion") || "分析结果"),
          status: iter.report_meta?.stop_reason
            ? `${i18n.t("analysis_conclusion") || "分析结果"} | ${iter.report_meta.stop_reason}`
            : (iter.mode === "auto_analysis" ? "已完成分析" : "已完成迭代"),
          stopReason: iter.report_meta?.stop_reason || "",
          reportTitle: iter.report_title || (iter.mode === "auto_analysis" ? "自动分析报告" : ""),
          reportSummary: iter.final_report_summary || (iter.final_report_md || "").slice(0, 500),
          reportHtml: iter.final_report_html || "",
          reportChartBindings: iter.final_report_chart_bindings || [],
          reportMarkdown: iter.final_report_md || "",
          reportUrl: iter.mode === "auto_analysis" && iter.iteration_id
            ? `/web/report.html?iteration_id=${encodeURIComponent(iter.iteration_id)}&lang=${encodeURIComponent((window.i18n && i18n.lang) || localStorage.getItem("lang") || "zh")}`
            : "",
          rounds: iter.loop_rounds || [],
          finalRound: (iter.loop_rounds || [])[Math.max(0, (iter.loop_rounds || []).length - 1)] || null,
          complete: true,
          liveThought: "",
        });

        return;

      }

      if (iter.mode === "auto_analysis") {

        const wrapper = createAiMessageContainer();

        replayAutoAnalysisIteration(iter, wrapper);

        return;

      }



      // 2. Render the AI analysis card (full replay)

      const wrapper = createAiMessageContainer();



      // -- Steps (SQL/Python code blocks) --

      let stepsHtml = "";

      const steps = iter.steps || [];

      if (steps.length > 0) {

        const stepsInner = steps.map((s, i) => {

          const lang = s.tool === "sql" ? "sql" : "python";

          const label = s.tool === "sql" ? `<i class="fa-solid fa-database"></i> SQL` : `<i class="fa-brands fa-python"></i> Python`;

          return `<details style="margin-bottom:6px;">

            <summary style="cursor:pointer;font-size:12px;font-weight:600;padding:4px 0;">${i18n.t("step")} ${i + 1}: ${label}</summary>

            <pre style="font-size:12px;background:#1e1e1e;color:#d4d4d4;padding:12px;border-radius:6px;overflow:auto;max-height:200px;"><code>${escapeHtml(s.code || '')}</code></pre>

          </details>`;

        }).join("");

        stepsHtml = `<div style="margin-bottom:12px;">${stepsInner}</div>`;

      }



      // -- Data rows preview --

      let dataHtml = "";

      if (iter.result_rows && iter.result_rows.length > 0) {

        dataHtml = `<details style="margin-bottom:12px;border:1px solid var(--border);border-radius:6px;overflow:hidden;">

          <summary style="background:#f8f9fa;padding:8px 14px;cursor:pointer;font-size:13px;font-weight:600;">${i18n.t('raw_data_preview')} (${iter.result_rows.length} ${i18n.t('rows')})</summary>

          <div>${jsonToTable(iter.result_rows)}</div>

        </details>`;

      }



      // -- Charts --

      let chartsHtml = "";

      if (iter.chart_specs && iter.chart_specs.length > 0) {

        iter.chart_specs.forEach((spec, ci) => {

          const cid = `hist_chart_${targetSessionId}_${iter.iteration_id || ci}_${ci}`;

          chartsHtml += `<div id="${cid}" style="height:280px;width:100%;margin-bottom:12px;"></div>`;

          setTimeout(() => {

            const dom = document.getElementById(cid);

            if (dom && spec) {

              const chart = echarts.init(dom);

              chart.setOption(spec);

            }

          }, 100);

        });

      }



      // -- Conclusions --

      let conclusionsHtml = "";

      if (iter.conclusions && iter.conclusions.length > 0) {

        conclusionsHtml = `<div style="margin-bottom:12px;">${iter.conclusions.map(c =>
          `• ${escapeHtml(c.text || '')} <span style="font-size:11px;color:#64748b;">(${i18n.t('confidence')} ${Math.round((c.confidence || 1) * 100)}%)</span>`
        ).join("<br/>")}</div>`;

      }



      // -- Hypotheses --

      let hypothesesHtml = "";

      if (iter.hypotheses && iter.hypotheses.length > 0) {

        hypothesesHtml = `<details style="margin-top:8px;">

          <summary style="cursor:pointer;font-size:12px;font-weight:600;color:var(--text-muted);"><i class="fa-solid fa-flask"></i> ${i18n.t("hypotheses")} (${iter.hypotheses.length})</summary>

          ${iter.hypotheses.map(h => `<div style="font-size:12px;padding:4px 0;">• ${escapeHtml(h.text || '')}</div>`).join("")}

        </details>`;

      }



      updateAiCard(wrapper, i18n.t("analysis_conclusion"), stepsHtml + dataHtml + chartsHtml + conclusionsHtml + hypothesesHtml);

    });

  } catch (e) {

    cards.innerHTML = `<div style="padding:20px;color:#ef4444;">${i18n.t("load_failed") || "加载失败"}: ${escapeHtml(e.message)}</div>`;

  }

}



function startNewSession() {

  sessionId = "";
  setSessionIdInUrl("");

  lastProposalId = "";

  cards.innerHTML = `

    <div class="welcome-card">

      <div class="icon-wrapper"><i class="fa-solid fa-magnifying-glass-chart fa-3x"></i></div>

      <h3 data-i18n="welcome_title">${i18n.t('welcome_title')}</h3>

      <p data-i18n="welcome_desc">${i18n.t('welcome_desc')}</p>

    </div>

  `;

  refreshSessions();

}



function parseProviderDirective(rawValue) {

  const patterns = [

    { provider: "openai", regex: /^openai(?::([^\s:]+))?\s*:\s*/i },

    { provider: "anthropic", regex: /^anthropic(?::([^\s:]+))?\s*:\s*/i },

    { provider: "mock", regex: /^mock(?::([^\s:]+))?\s*:\s*/i },

  ];

  for (const p of patterns) {

    const match = rawValue.match(p.regex);

    if (match) {

      return {

        provider: p.provider,

        model: match[1] ? match[1].trim() : null,

        message: rawValue.replace(p.regex, "").trim(),

      };

    }

  }

  return { provider: null, model: null, message: rawValue };

}



function jsonToTable(rows) {

  if (!rows || rows.length === 0) return `<div>${i18n.t('no_data')}</div>`;



  const headers = Object.keys(rows[0]);

  const thead = `<thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>`;

  const tbody = `<tbody>${rows

    .map(

      (row) =>

        `<tr>${headers.map((h) => `<td>${escapeHtml(row[h])}</td>`).join("")}</tr>`

    )

    .join("")}</tbody>`;



  return `<div class="table-container"><table>${thead}${tbody}</table></div>`;

}



function escapeHtml(text) {

  return String(text || "")

    .replace(/&/g, "&amp;")

    .replace(/</g, "&lt;")

    .replace(/>/g, "&gt;")

    .replace(/"/g, "&quot;")

    .replace(/'/g, "&#039;");

}

function sanitizeNarrativeText(text) {
  return String(text || "").replace(/\{[a-zA-Z_][a-zA-Z0-9_]*(?::[^}]*)?\}/g, "");
}


function normalizeReportHtmlDocument(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";

  const extractStandaloneHtml = (candidate) => {
    const match = String(candidate || "").match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
    return match ? match[0].trim() : "";
  };

  const tryParseJsonLike = (candidate) => {
    try {
      const parsed = JSON.parse(candidate);
      if (parsed && typeof parsed === "object" && typeof parsed.html_document === "string") {
        const rawHtml = parsed.html_document.trim();
        return extractStandaloneHtml(rawHtml) || rawHtml;
      }
    } catch (_) {
      // ignore
    }
    return "";
  };

  let html = tryParseJsonLike(text);
  if (html) return html;

  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    html = tryParseJsonLike(text.slice(firstBrace, lastBrace + 1));
    if (html) return html;
  }

  const htmlField = text.match(/"html_document"\s*:\s*"([\s\S]*?)"\s*(?:,\s*"chart_bindings"|,\s*"summary"|,\s*"title"|,\s*"legacy_markdown"|\})/i);
  if (htmlField && htmlField[1]) {
    try {
      const rawHtml = JSON.parse(`"${htmlField[1]}"`).trim();
      return extractStandaloneHtml(rawHtml) || rawHtml;
    } catch (_) {
      const rawHtml = htmlField[1].trim();
      return extractStandaloneHtml(rawHtml) || rawHtml;
    }
  }

  const htmlBlock = text.match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
  if (htmlBlock) return htmlBlock[0].trim();

  if (text.startsWith("{") || text.startsWith("[")) return "";

  return `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Report</title><style>body{font-family:Inter,Arial,sans-serif;margin:24px;color:#111827;background:#fff;}pre{white-space:pre-wrap;line-height:1.6;}</style></head><body><pre>${escapeHtml(text)}</pre></body></html>`;
}


function mountEmbeddedReportCharts(frame, chartBindings, lang = "zh") {
  if (!frame || !frame.contentDocument || !window.echarts || !Array.isArray(chartBindings)) return;
  const doc = frame.contentDocument;
  let chartSection = null;
  const ensureSection = () => {
    if (chartSection) return chartSection;
    chartSection = doc.createElement("section");
    chartSection.style.marginTop = "22px";
    const heading = doc.createElement("h2");
    heading.textContent = lang === "en" ? "Charts" : "图表";
    heading.style.margin = "0 0 10px";
    chartSection.appendChild(heading);
    const root = doc.body || doc.documentElement;
    if (root) root.appendChild(chartSection);
    return chartSection;
  };

  chartBindings.forEach((binding) => {
    if (!binding || typeof binding !== "object") return;
    const chartId = String(binding.chart_id || "").trim();
    const option = binding.option;
    if (!chartId || !option || typeof option !== "object") return;
    let host = doc.querySelector(`[data-chart-id="${chartId}"]`);
    if (!host) {
      const section = ensureSection();
      const block = doc.createElement("section");
      block.style.marginTop = "14px";
      const title = doc.createElement("h3");
      title.textContent = `${lang === "en" ? "Chart" : "图表"}: ${chartId}`;
      title.style.margin = "0 0 8px";
      host = doc.createElement("div");
      host.setAttribute("data-chart-id", chartId);
      block.appendChild(title);
      block.appendChild(host);
      section.appendChild(block);
    }
    const height = Math.max(200, Math.min(1200, parseInt(binding.height || 360, 10) || 360));
    host.innerHTML = "";
    const mount = doc.createElement("div");
    mount.style.width = "100%";
    mount.style.height = `${height}px`;
    host.appendChild(mount);
    const chart = echarts.init(mount);
    chart.setOption(option);
  });
}


function syncEmbeddedReportHeight(frame) {
  try {
    const doc = frame.contentDocument;
    if (!doc) return;
    if (doc.documentElement) doc.documentElement.style.overflow = "auto";
    if (doc.body) doc.body.style.overflow = "auto";
    if (doc.body) doc.body.style.margin = doc.body.style.margin || "0";
    const bodyHeight = doc.body ? doc.body.scrollHeight : 0;
    const htmlHeight = doc.documentElement ? doc.documentElement.scrollHeight : 0;
    const viewportFloor = Math.max(800, window.innerHeight - 140);
    const target = Math.max(bodyHeight, htmlHeight, viewportFloor);
    frame.style.height = `${target}px`;
  } catch (_) {
    // no-op
  }
}


function buildRoundChartId(seed, roundNo, chartIndex) {
  return `analysis_${seed}_round_${roundNo}_chart_${chartIndex}`;
}


function buildFinalChartId(seed, chartIndex) {
  return `analysis_${seed}_final_chart_${chartIndex}`;
}



function renderIterationResult(result, wrapper, accumulatedThought, chartContainers, dataRowsHtml, pendingCharts = []) {

  const conclusions = (result.conclusions || []).map(c => {

    let confBadge = "";

    if (c.confidence >= 0.8) confBadge = `<span style="color:#10b981;font-size:11px;margin-left:8px;">(${i18n.t('confidence_high')} ${(c.confidence * 100).toFixed(0)}%)</span>`;

    else if (c.confidence >= 0.5) confBadge = `<span style="color:#f59e0b;font-size:11px;margin-left:8px;">(${i18n.t('confidence_med')} ${(c.confidence * 100).toFixed(0)}%)</span>`;

    else confBadge = `<span style="color:#ef4444;font-size:11px;margin-left:8px;">(${i18n.t('confidence_low')} ${(c.confidence * 100).toFixed(0)}%)</span>`;

    return `<li style="margin-bottom:8px"><strong>${escapeHtml(c.text)}</strong>${confBadge}</li>`;

  }).join("");



  const hypotheses = (result.hypotheses || []).map(h => `

    <button class="btn btn-outline btn-sm hypothesis-btn" style="margin:4px 4px 4px 0;text-align:left;white-space:normal;height:auto;" data-id="${h.id}" data-text="${escapeHtml(h.text)}">

      <i class="fa-solid fa-magnifying-glass"></i> ${escapeHtml(h.text)}

    </button>

  `).join("");



  const actionItems = (result.actionItems || result.action_items || []).map(a => `<li style="margin-bottom:4px"><i class="fa-solid fa-check" style="color:#10b981"></i> ${escapeHtml(a)}</li>`).join("");



  let codeBlocks = "";

  // Multi-step rendering

  const steps = result.steps || [];

  if (steps.length > 0) {

    steps.forEach((step, idx) => {

      const label = step.tool === "sql" ? "SQL" : "Python";

      const icon = step.tool === "sql" ? "fa-database" : "fa-code";

      const langClass = step.tool === "sql" ? "language-sql" : "language-python";

      codeBlocks += `

        <details class="code-details" style="margin-top:10px;">

          <summary style="font-size:12px;font-weight:600;">

            <i class="fa-solid ${icon}" style="margin-right:4px;"></i>Step ${idx + 1}: ${label}

          </summary>

          <pre><code class="${langClass}">${escapeHtml(step.code)}</code></pre>

        </details>`;

    });

  } else {

    // Backward compatibility for old flat format

    if (result.sql) {

      codeBlocks += `<div style="margin-top:12px;font-weight:600;font-size:12px">${i18n.t("exec_sql") || "鎵ц SQL"}:</div><pre>${escapeHtml(result.sql)}</pre>`;

    }

    if (result.python_code) {

      codeBlocks += `<div style="margin-top:12px;font-weight:600;font-size:12px">${i18n.t("exec_python") || "鎵ц Python"}:</div>

        <details class="code-details">

          <summary>${i18n.t("view_python_code") || "查看分析代码 (Python)"}</summary>

          <pre><code class="language-python">${escapeHtml(result.python_code)}</code></pre>

        </details>`;

    }

  }



  const actionsSection = actionItems ? `

    <div style="margin-top:16px;padding-top:12px;border-top:1px dashed var(--border)">

      <div style="font-weight:600;margin-bottom:8px;color:var(--text-main)"><i class="fa-solid fa-person-running"></i> ${i18n.t('action_suggestions')}</div>

      <ul style="list-style:none;padding:0;margin:0;font-size:13px">${actionItems}</ul>

    </div>

  ` : "";



  const hypothesesSection = hypotheses ? `

    <div style="margin-top:16px;padding-top:12px;border-top:1px dashed var(--border)">

      <div style="font-weight:600;margin-bottom:8px;color:var(--text-main)"><i class="fa-solid fa-code-branch"></i> ${i18n.t('click_hypotheses')}</div>

      <div style="display:flex;flex-wrap:wrap;">${hypotheses}</div>

    </div>

  ` : "";



  const html = `

    <div style="margin-bottom:12px;font-size:13px;color:var(--text-muted)">

      ${i18n.t("using_tool") || "使用工具"}: ${(result.tools_used || []).join(", ") || (i18n.t("none") || "无")} | ${escapeHtml(result.explanation)}

    </div>

    <div style="margin-top:16px">

      <div style="font-weight:600;font-size:15px;margin-bottom:8px;color:var(--primary)">${i18n.t('main_conclusion')}</div>

      <ul style="padding-left:20px;margin:0;font-size:14px;line-height:1.6">${conclusions}</ul>

    </div>

    ${chartContainers}

    ${dataRowsHtml}

    ${actionsSection}

    ${hypothesesSection}

    ${codeBlocks}

  `;



  updateAiCard(wrapper, i18n.t("iter_conclusion"), html, accumulatedThought);



  // Bind hypotheses buttons

  wrapper.querySelectorAll(".hypothesis-btn").forEach(btn => {

    btn.onclick = () => {

      document.getElementById("questionInput").value = `${i18n.t("validate_hypothesis") || "验证猜想"}: ${btn.getAttribute("data-text")}`;

      handleSend(btn.getAttribute("data-id"));

    };

  });



  // Initialize deferred charts now that HTML is in the DOM

  if (pendingCharts && pendingCharts.length > 0) {

    setTimeout(() => {

        pendingCharts.forEach(pc => {

            const dom = document.getElementById(pc.id);

            if (dom && pc.spec) {

                const chart = echarts.init(dom);

                chart.setOption(pc.spec);

            }

        });

    }, 50);

  }

}


function renderMarkdownContent(markdown) {

  if (!markdown) return "";

  if (window.marked && typeof window.marked.parse === "function") {

    return marked.parse(markdown);

  }

  return `<pre style="white-space:pre-wrap;">${escapeHtml(markdown)}</pre>`;

}


function renderAutoAnalysisCard(wrapper, state) {

  const chartRefs = [];
  const lang = (window.i18n && i18n.lang) || localStorage.getItem("lang") || "zh";
  const isEn = lang === "en";
  const mode = state.mode === "iterate" ? "iterate" : "auto";
  const rounds = (state.rounds || []).filter(Boolean);
  const roundTitle = (roundNo) => (isEn ? `Round ${roundNo}` : `第 ${roundNo} 轮`);
  const noToolText = isEn ? "No tool calls" : "无工具调用";
  const findingsTitle = isEn ? "Key Findings" : "关键结论";
  const actionsTitle = isEn ? "Action Items" : "行动建议";
  const previewTitle = isEn ? "Data Preview" : "数据预览";
  const openReportText = isEn ? "Open Full Report" : "打开完整报告";
  const exportPdfText = isEn ? "Export PDF" : "导出 PDF";
  const reportReadyHint = isEn
    ? "After report generation, you can open the full report or export PDF."
    : "报告生成后可打开完整报告与导出 PDF";
  const reportSummaryPending = isEn ? "Generating HTML report..." : "HTML 报告生成中...";
  const reportFollowupHint = isEn
    ? "You can continue asking questions based on this report, or start another one-click analysis."
    : "可继续在聊天里追问该报告，或再次发起一键分析。";
  const analyzingText = isEn ? "Analyzing" : "正在分析";
  const processTitle = isEn ? "Analysis Process" : "分析过程";
  const finalTitle = mode === "auto" ? (isEn ? "Final Report" : "最终报告") : (isEn ? "Final Result" : "最终结果");
  const stopReasonLabel = isEn ? "Stop reason" : "停止原因";
  const reportLang = lang;
  const reportUrlBase = state.reportUrl || "";
  const reportUrl = reportUrlBase
    ? (reportUrlBase.includes("lang=")
      ? reportUrlBase
      : `${reportUrlBase}${reportUrlBase.includes("?") ? "&" : "?"}lang=${encodeURIComponent(reportLang)}`)
    : "";

  state.chartSeed = state.chartSeed || `${mode}_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`;
  state.reportFrameId = state.reportFrameId || `report_frame_${state.chartSeed}`;

  const formatStopReason = (reason) => {
    const text = String(reason || "").trim();
    if (!text) return "";
    const map = isEn
      ? {
          model_stopped_using_tools: "model stopped using tools",
          max_rounds_reached: "max rounds reached",
          execution_error: "execution error",
        }
      : {
          model_stopped_using_tools: "模型停止工具调用",
          max_rounds_reached: "达到最大轮次",
          execution_error: "执行出错",
        };
    return map[text] || text;
  };

  const buildRoundCard = (round, index, { final = false } = {}) => {
    const result = round.result || {};
    const execution = round.execution || {};
    const roundNo = round.round || index + 1;
    const summaryText = String(
      result.explanation ||
      result.summary ||
      result.final_answer ||
      result.conclusion ||
      ""
    ).trim();
    const directAnswerText = String(
      result.direct_answer ||
      result.directAnswer ||
      result.answer ||
      result.final_answer ||
      ""
    ).trim();
    const extractTopAnswer = () => {
      if (!Array.isArray(rows) || !rows.length) return "";
      const normalizedRows = rows.filter((row) => row && typeof row === "object");
      if (!normalizedRows.length) return "";
      const pickField = (obj, candidates) => {
        for (const key of Object.keys(obj)) {
          const lower = key.toLowerCase();
          if (candidates.some((candidate) => lower === candidate || lower.includes(candidate))) {
            const value = obj[key];
            if (value !== null && value !== undefined && String(value).trim() !== "") return { key, value };
          }
        }
        return null;
      };
      const topRow = normalizedRows[0];
      const dimension = pickField(topRow, ["department", "dept", "部门", "organization", "org"]);
      const metric = pickField(topRow, ["total_cost", "cost", "sum_cost", "amount", "total"]);
      if (!dimension) return "";
      const metricText = metric ? `，${escapeHtml(metric.key)}: ${escapeHtml(String(metric.value))}` : "";
      return `<div class="analysis-answer-callout"><strong>${isEn ? "Top department" : "成本最高部门"}：</strong>${escapeHtml(String(dimension.value))}${metricText}</div>`;
    };
    const conclusions = (result.conclusions || []).map((item) => {
      const text = typeof item === "object" ? item.text : item;
      const confidence = typeof item === "object" ? item.confidence : null;
      const suffix = confidence === null || confidence === undefined
        ? ""
        : ` <span style="font-size:11px;color:var(--text-muted)">(${Math.round(confidence * 100)}%)</span>`;
      return `<li>${escapeHtml(sanitizeNarrativeText(text || ""))}${suffix}</li>`;
    }).join("");
    const actions = (result.action_items || result.actionItems || []).map((item) => `<li>${escapeHtml(sanitizeNarrativeText(item))}</li>`).join("");
    const hypotheses = (result.hypotheses || []).map((h) => {
      const text = sanitizeNarrativeText(typeof h === "object" ? (h.text || "") : String(h || ""));
      const id = typeof h === "object" ? (h.id || "") : "";
      return `
      <button class="btn btn-outline btn-sm hypothesis-btn" style="margin:4px 6px 4px 0;text-align:left;white-space:normal;height:auto;" data-id="${escapeHtml(id)}" data-text="${escapeHtml(text)}">
        <i class="fa-solid fa-magnifying-glass"></i> ${escapeHtml(text)}
      </button>
    `;
    }).join("");
    const rows = execution.rows || [];
    const charts = execution.chart_specs || [];
    const chartsHtml = charts.map((spec, idx) => {
      const id = final
        ? buildFinalChartId(state.chartSeed, idx)
        : buildRoundChartId(state.chartSeed, roundNo, idx);
      chartRefs.push({ id, spec });
      return `<div id="${id}" class="analysis-chart-host"></div>`;
    }).join("");
    const stepsHtml = (result.steps || []).length ? `
      <details class="code-details analysis-step-details">
        <summary style="font-size:12px;font-weight:600;">${isEn ? "Steps" : "步骤"} (${(result.steps || []).length})</summary>
        ${(result.steps || []).map((step, idx) => `
          <details class="code-details" style="margin-top:8px;">
            <summary style="font-size:12px;font-weight:600;">${escapeHtml(step.tool || (isEn ? "step" : "步骤"))} ${idx + 1}</summary>
            <pre><code>${escapeHtml(step.code || "")}</code></pre>
          </details>
        `).join("")}
      </details>
    ` : "";
    const errorHtml = round.error || execution.error
      ? `<div class="analysis-error"><i class="fa-solid fa-circle-exclamation"></i> ${escapeHtml(sanitizeNarrativeText(round.error || execution.error))}</div>`
      : "";
    const thoughtHtml = round.thought
      ? `<details class="analysis-thought" ${final ? "" : "open"}>
          <summary style="cursor:pointer;font-size:12px;">${i18n.t("thought_process") || "思考过程"}</summary>
          <div style="white-space:pre-wrap;font-size:12px;color:var(--text-muted);margin-top:8px;">${escapeHtml(sanitizeNarrativeText(round.thought))}</div>
        </details>`
      : "";
    const summaryHtml = summaryText
      ? `<div class="analysis-section"><div class="analysis-section-title">${isEn ? "Result Summary" : "结果摘要"}</div><div style="white-space:pre-wrap;font-size:13px;line-height:1.7;color:var(--text-main);">${escapeHtml(sanitizeNarrativeText(summaryText))}</div></div>`
      : "";
    const directAnswerHtml = directAnswerText
      ? `<div class="analysis-answer-callout"><strong>${isEn ? "Direct Answer" : "明确答案"}：</strong>${escapeHtml(sanitizeNarrativeText(directAnswerText))}</div>`
      : "";
    const modeLabel = final && mode === "iterate" ? finalTitle : roundTitle(roundNo);
    const toolsText = escapeHtml((result.tools_used || []).join(", ") || noToolText);

    return `
      <article class="analysis-round-card ${final ? "final-round" : ""}">
        <div class="analysis-round-header">
          <div class="analysis-round-title">${modeLabel}</div>
          <div class="analysis-round-tools">${toolsText}</div>
        </div>
        ${thoughtHtml}
        ${final && mode === "iterate" ? summaryHtml : ""}
        ${final && mode === "iterate" ? (directAnswerHtml || extractTopAnswer()) : ""}
        ${conclusions ? `<div class="analysis-section"><div class="analysis-section-title">${findingsTitle}</div><ul class="analysis-list">${conclusions}</ul></div>` : ""}
        ${actions ? `<div class="analysis-section"><div class="analysis-section-title">${actionsTitle}</div><ul class="analysis-list">${actions}</ul></div>` : ""}
        ${chartsHtml ? `<div class="analysis-chart-grid">${chartsHtml}</div>` : ""}
        ${rows.length ? `<details class="analysis-data-details"${final ? " open" : ""}><summary style="cursor:pointer;font-size:12px;">${previewTitle} (${rows.length} ${i18n.t("rows") || (isEn ? "rows" : "行")})</summary>${jsonToTable(rows.slice(0, 50))}</details>` : ""}
        ${hypotheses ? `<div class="analysis-section"><div class="analysis-section-title">${isEn ? "Hypotheses" : "猜想"}</div><div style="display:flex;flex-wrap:wrap;">${hypotheses}</div></div>` : ""}
        ${stepsHtml}
        ${errorHtml}
      </article>
    `;
  };

  const roundHasMeaningfulOutput = (round) => {
    if (!round || typeof round !== "object") return false;
    const result = round.result || {};
    const execution = round.execution || {};
    const conclusions = Array.isArray(result.conclusions) ? result.conclusions : [];
    const actions = Array.isArray(result.action_items || result.actionItems) ? (result.action_items || result.actionItems) : [];
    const hypothesesList = Array.isArray(result.hypotheses) ? result.hypotheses : [];
    const stepsList = Array.isArray(result.steps) ? result.steps : [];
    const rowsList = Array.isArray(execution.rows) ? execution.rows : [];
    const chartsList = Array.isArray(execution.chart_specs) ? execution.chart_specs : [];
    const explanation = String(result.explanation || "").trim();
    const directReport = String(result.direct_report || "").trim();
    if (conclusions.length || actions.length || hypothesesList.length || stepsList.length || rowsList.length || chartsList.length) return true;
    if (directReport) return true;
    if (explanation && explanation !== "model stopped without additional tool calls") return true;
    return false;
  };

  const getMeaningfulFinalRound = () => {
    for (let i = rounds.length - 1; i >= 0; i -= 1) {
      if (roundHasMeaningfulOutput(rounds[i])) return rounds[i];
    }
    return rounds[rounds.length - 1] || null;
  };

  const processHtml = rounds.length ? `
    <details class="analysis-process-panel" ${state.complete ? "" : "open"}>
      <summary class="analysis-process-summary">
        <span>${processTitle}</span>
        <span class="analysis-process-meta">${rounds.length} ${isEn ? "rounds" : "轮"}</span>
      </summary>
      <div class="analysis-process-body">
        ${rounds.map((round, idx) => buildRoundCard(round, idx)).join("")}
      </div>
    </details>
  ` : "";

  const finalRound = mode === "iterate"
    ? (getMeaningfulFinalRound() || state.finalRound || rounds[rounds.length - 1] || null)
    : (state.finalRound || rounds[rounds.length - 1] || null);
  const finalSection = mode === "auto"
    ? (() => {
        const reportTitle = state.reportTitle || (isEn ? "Auto Analysis Report" : "自动分析报告");
        const reportMarkdown = String(state.reportMarkdown || "").trim();
        const reportSummary = String(state.reportSummary || "").trim();
        const chatReportMarkdown = reportMarkdown || (reportSummary ? `## ${reportTitle}\n\n${reportSummary}` : "");
        const reportActions = reportUrl
          ? `<div class="analysis-report-actions">
              <button class="btn btn-outline btn-sm open-report-btn" data-url="${escapeHtml(reportUrl)}"><i class="fa-solid fa-arrow-up-right-from-square"></i> ${openReportText}</button>
              <button class="btn btn-outline btn-sm print-report-btn" data-url="${escapeHtml(reportUrl)}"><i class="fa-solid fa-file-pdf"></i> ${exportPdfText}</button>
            </div>`
          : `<div class="analysis-report-hint">${reportReadyHint}</div>`;
        const reportPreview = chatReportMarkdown
          ? `<div class="analysis-report-markdown-summary">${renderMarkdownContent(chatReportMarkdown)}</div>`
          : `<div class="analysis-report-placeholder">${reportSummaryPending}</div>`;
        return `
          <section class="analysis-final-panel">
            <div class="analysis-final-panel-header">${finalTitle}</div>
            ${reportActions}
            ${reportPreview}
            <div class="analysis-report-followup">${reportFollowupHint}</div>
          </section>
        `;
      })()
    : (finalRound ? `
        <section class="analysis-final-panel">
          <div class="analysis-final-panel-header">${finalTitle}</div>
          ${buildRoundCard(finalRound, rounds.length - 1, { final: true })}
        </section>
      ` : "");

  const statusLine = `
    <div class="analysis-status-line">
      ${escapeHtml(state.status || analyzingText)}${state.stopReason ? ` | ${stopReasonLabel}: ${escapeHtml(formatStopReason(state.stopReason))}` : ""}
    </div>
  `;

  updateAiCard(wrapper, state.title || (isEn ? "Analysis" : "分析"), `${statusLine}${processHtml}${finalSection}`, state.liveThought || null);

  wrapper.querySelectorAll(".hypothesis-btn").forEach(btn => {
    btn.onclick = () => {
      document.getElementById("questionInput").value = `${i18n.t("validate_hypothesis") || "验证猜想"}: ${btn.getAttribute("data-text")}`;
      handleSend(btn.getAttribute("data-id"));
    };
  });

  if (chartRefs.length > 0) {
    setTimeout(() => {
      chartRefs.forEach(({ id, spec }) => {
        const dom = document.getElementById(id);
        if (dom && spec) {
          const chart = echarts.init(dom);
          chart.setOption(spec);
        }
      });
    }, 50);
  }

  wrapper.querySelectorAll(".open-report-btn").forEach((btn) => {
    btn.onclick = () => {
      const url = btn.getAttribute("data-url");
      if (url) window.open(url, "_blank", "noopener");
    };
  });
  wrapper.querySelectorAll(".print-report-btn").forEach((btn) => {
    btn.onclick = () => {
      const url = btn.getAttribute("data-url");
      if (!url) return;
      const printUrl = `${url}${url.includes("?") ? "&" : "?"}print=1`;
      window.open(printUrl, "_blank", "noopener");
    };
  });
}


function replayAutoAnalysisIteration(iter, wrapper) {
  const reportLang = (window.i18n && i18n.lang) || localStorage.getItem("lang") || "zh";

  renderAutoAnalysisCard(wrapper, {
    mode: "auto",

    title: "一键分析",

    status: `已完成 ${((iter.report_meta || {}).rounds_completed || (iter.loop_rounds || []).length)} 轮`,

    stopReason: (iter.report_meta || {}).stop_reason || "",

    reportTitle: iter.report_title || "自动分析报告",

    reportSummary: iter.final_report_summary || (iter.final_report_md || "").slice(0, 500),

    reportHtml: iter.final_report_html || "",

    reportChartBindings: iter.final_report_chart_bindings || [],

    reportMarkdown: iter.final_report_md || "",

    reportUrl: iter.iteration_id ? `/web/report.html?iteration_id=${encodeURIComponent(iter.iteration_id)}&lang=${encodeURIComponent(reportLang)}` : "",

    rounds: iter.loop_rounds || [],

    complete: true,

    allowMarkdownFallback: !iter.final_report_html && !!iter.final_report_md,

    liveThought: "",

  });

}



async function handleSend(hypothesisId = null) {

  const input = document.getElementById("questionInput");

  const rawValue = input.value.trim();

  if (!rawValue && !hypothesisId) return;



  const sandboxId = sandboxSelect.value;

  if (!sandboxId) {

    alert(i18n.t('select_sandbox_first'));

    return;

  }



  const welcomeCard = document.querySelector(".welcome-card");

  if (welcomeCard) welcomeCard.style.display = "none";



  addUserMessage(rawValue);

  input.value = "";



  // 1. Check for feedback / knowledge input

  const isKnowledge = rawValue.startsWith(i18n.t('knowledge_prefix') || "知识:") || rawValue.startsWith("业务知识:") || rawValue.startsWith("Knowledge:");

  const isFeedback = rawValue.startsWith(i18n.t('feedback_prefix') || "反馈:") || rawValue.startsWith("纠正:") || rawValue.startsWith("Feedback:");



  if (isKnowledge || isFeedback || (!hypothesisId && (rawValue.startsWith("fix:") || rawValue.startsWith("patch:")))) {

    const content = rawValue.replace(/^(知识:|业务知识:|反馈:|纠正:|fix:|patch:|修正:|补丁:|Knowledge:|Feedback:)\s*/i, "");

    try {

      const data = await api("/api/chat/feedback", {

        method: "POST",

        body: JSON.stringify({

          session_id: sessionId || "default",

          feedback: content,

          is_business_knowledge: isKnowledge

        }),

      });

      sessionId = data.session_id;

      addCard(isKnowledge ? i18n.t("knowledge_saved") : i18n.t("feedback_recorded"), `<div>${escapeHtml(content)}</div><div style="font-size:12px;color:var(--text-muted);margin-top:8px">${i18n.t('ai_will_ref')}</div>`);

    } catch (e) {

      addCard(i18n.t("op_failed") || "操作失败", `<div style="color: #ef4444">${e.message}</div>`);

    }

    return;

  }



  // 2. Standard Iteration loop

  const directive = parseProviderDirective(rawValue);



  const checkedFiles = Array.from(document.querySelectorAll(".uploaded-file-checkbox:checked"))

    .map(cb => cb.value);



  const checkedTables = Array.from(document.querySelectorAll(".db-table-checkbox-sidebar:checked"))

    .map(cb => cb.value);



  const reqBody = {

    sandbox_id: sandboxId,

    message: directive.message || rawValue,

    session_id: sessionId || null,

    hypothesis_id: hypothesisId,

    selected_files: checkedFiles,

    selected_tables: checkedTables.length > 0 ? checkedTables : null

  };

  if (directive.provider) reqBody.provider = directive.provider;

  if (directive.model) reqBody.model = directive.model;



  const wrapper = createAiMessageContainer();

  updateAiCard(wrapper, i18n.t("ai_thinking"), `<div>${i18n.t('thinking_desc')}</div>`, "");

  const notebookState = {
    mode: "iterate",
    title: i18n.t("analysis_conclusion") || "分析结果",
    status: i18n.t("ai_thinking") || "AI 正在思考",
    stopReason: "",
    rounds: [],
    finalRound: null,
    liveThought: "",
    complete: false,
  };

  const controller = new AbortController();
  activeAnalysisController = controller;
  setAnalysisRunningState(true, "iterate", wrapper);

  try {

    const token = localStorage.getItem("token");

    const headers = {
      "Content-Type": "application/json",
      "X-Language": i18n.lang || localStorage.getItem("lang") || "zh"
    };

    if (token) headers["Authorization"] = `Bearer ${token}`;



    const response = await fetch("/api/chat/iterate", {

      method: "POST",

      headers,

      signal: controller.signal,

      body: JSON.stringify(reqBody),

    });



    if (!response.ok) throw new Error((i18n.t("request_failed") || "请求失败") + `: ${response.status}`);



    const reader = response.body.getReader();

    const decoder = new TextDecoder();

    let buffer = "";

    let accumulatedThought = "";

    let autoCompleted = false;
    let sawRoundPayload = false;
    let legacyThought = "";
    let legacyFinalResult = null;



    while (true) {

      const { done, value } = await reader.read();

      if (done) break;



      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");

      buffer = lines.pop(); // keep partial line



      for (const line of lines) {

        if (!line.trim()) continue;

        try {

          const data = JSON.parse(line);

          if (data.type === "loop_status") {
            sawRoundPayload = true;
            const info = data.data || {};
            notebookState.status = info.message || notebookState.status;
            notebookState.liveThought = info.phase === "thinking" ? (info.message || "") : "";
            renderAutoAnalysisCard(wrapper, notebookState);
          } else if (data.type === "loop_round") {
            sawRoundPayload = true;
            const roundData = data.data || {};
            notebookState.rounds[roundData.round - 1] = roundData;
            notebookState.finalRound = roundData;
            notebookState.liveThought = "";
            notebookState.complete = false;
            notebookState.status = i18n.t("analysis_conclusion") || "分析结果";
            renderAutoAnalysisCard(wrapper, notebookState);
          } else if (data.type === "thought") {
            legacyThought += data.content || "";
            if (!sawRoundPayload) {
              notebookState.liveThought = legacyThought;
              renderAutoAnalysisCard(wrapper, notebookState);
            }
          } else if (data.type === "result") {

            legacyFinalResult = data.data;

          } else if (data.type === "data" || data.type === "chart_spec" || data.type === "step_result") {

            // legacy compatibility only; loop_round drives the new UI

          } else if (data.type === "iteration_complete") {

            sessionId = data.data.session_id;

            lastProposalId = data.data.proposal_id; // For skill saving

            notebookState.stopReason = data.data.stop_reason || notebookState.stopReason;
            notebookState.complete = true;
            notebookState.status = i18n.t("analysis_conclusion") || "分析结果";
            renderAutoAnalysisCard(wrapper, notebookState);

            autoCompleted = true;
            releaseAnalysisControls(controller);

            // Auto-refresh session list so current session appears immediately

            refreshSessions();

          } else if (data.type === "error") {

            updateAiCard(wrapper, i18n.t("error_occurred"), `<div style="color: #ef4444">${data.message}</div>`, legacyThought || notebookState.liveThought);

          }

        } catch (e) {

          console.error("JSON parse error", e, line);

        }

      }

    }



    // In case execution stream was incomplete but we had result data

    if (!autoCompleted && !sawRoundPayload && legacyFinalResult) {

      renderIterationResult(legacyFinalResult, wrapper, legacyThought, "", "", []);

    }



  } catch (e) {
    if (e && e.name === "AbortError") {
      notebookState.stopReason = "stopped_by_user";
      notebookState.status = i18n.t("analysis_stopped") || (i18n.lang === "en" ? "Stopped" : "已停止");
      notebookState.complete = true;
      renderAutoAnalysisCard(wrapper, notebookState);
    } else {
      updateAiCard(wrapper, i18n.t("request_failed") || "请求失败", `<div style="color: #ef4444">${e.message}</div>`);
    }
  } finally {
    releaseAnalysisControls(controller);
  }

}


async function handleAutoAnalyze() {

  const input = document.getElementById("questionInput");

  const rawValue = input.value.trim();

  const isKnowledge = rawValue.startsWith(i18n.t('knowledge_prefix') || "知识:") || rawValue.startsWith("业务知识:") || rawValue.startsWith("Knowledge:");

  const isFeedback = rawValue.startsWith(i18n.t('feedback_prefix') || "反馈:") || rawValue.startsWith("纠正:") || rawValue.startsWith("Feedback:");

  if (isKnowledge || isFeedback || rawValue.startsWith("fix:") || rawValue.startsWith("patch:")) {

    await handleSend();

    return;

  }

  const sandboxId = sandboxSelect.value;

  if (!sandboxId) {

    alert(i18n.t('select_sandbox_first'));

    return;

  }

  const welcomeCard = document.querySelector(".welcome-card");

  if (welcomeCard) welcomeCard.style.display = "none";

  if (rawValue) addUserMessage(rawValue);

  input.value = "";

  const directive = parseProviderDirective(rawValue);

  const checkedFiles = Array.from(document.querySelectorAll(".uploaded-file-checkbox:checked"))

    .map(cb => cb.value);

  const checkedTables = Array.from(document.querySelectorAll(".db-table-checkbox-sidebar:checked"))

    .map(cb => cb.value);

  const reqBody = {

    sandbox_id: sandboxId,

    message: directive.message || rawValue || "",

    session_id: sessionId || null,

    selected_files: checkedFiles,

    selected_tables: checkedTables.length > 0 ? checkedTables : null,

    max_rounds: 100,

    trace_mode: "full"

  };

  if (directive.provider) reqBody.provider = directive.provider;

  if (directive.model) reqBody.model = directive.model;

  const wrapper = createAiMessageContainer();
  const lang = (window.i18n && i18n.lang) || localStorage.getItem("lang") || "zh";
  const isEn = lang === "en";

  const state = {
    mode: "auto",

    title: isEn ? "Auto Analyze" : "一键分析",

    status: isEn ? "Preparing auto analysis" : "准备开始自动分析",

    stopReason: "",

    reportTitle: "",

    reportSummary: "",

    reportUrl: "",

    reportHtml: "",

    reportMarkdown: "",

    reportChartBindings: [],

    rounds: [],

    liveThought: ""

  };

  renderAutoAnalysisCard(wrapper, state);

  const controller = new AbortController();
  activeAnalysisController = controller;
  setAnalysisRunningState(true, "auto", wrapper);

  try {

    const token = localStorage.getItem("token");

    const headers = {
      "Content-Type": "application/json",
      "X-Language": i18n.lang || localStorage.getItem("lang") || "zh"
    };

    if (token) headers["Authorization"] = `Bearer ${token}`;

    const response = await fetch("/api/chat/auto-analyze", {

      method: "POST",

      headers,

      signal: controller.signal,

      body: JSON.stringify(reqBody),

    });

    if (!response.ok) throw new Error((i18n.t("request_failed") || "请求失败") + `: ${response.status}`);

    const reader = response.body.getReader();

    const decoder = new TextDecoder();

    let buffer = "";

    while (true) {

      const { done, value } = await reader.read();

      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split("\n");

      buffer = lines.pop();

      for (const line of lines) {

        if (!line.trim()) continue;

        try {

          const payload = JSON.parse(line);

          if (payload.type === "loop_status") {

            const data = payload.data || {};

            const phaseMap = isEn
              ? {
                  planning: "Planning",
                  thinking: "Thinking",
                  report_generating: "Generating report",
                }
              : {
                  planning: "规划中",
                  thinking: "思考中",
                  report_generating: "报告生成中",
                };
            const phase = phaseMap[data.phase] || (isEn ? "Processing" : "处理中");

            state.status = isEn
              ? `Round ${data.round || 0} ${phase}`
              : `第 ${data.round || 0} 轮 ${phase}`;

            if (data.phase === "thinking") state.liveThought = data.message || "";
            else state.liveThought = "";

            renderAutoAnalysisCard(wrapper, state);

          } else if (payload.type === "loop_round") {

            const data = payload.data || {};

            state.rounds[data.round - 1] = data;

            state.liveThought = "";

            state.status = isEn
              ? `Completed ${state.rounds.filter(Boolean).length} rounds`
              : `已完成 ${state.rounds.filter(Boolean).length} 轮`;

            renderAutoAnalysisCard(wrapper, state);

          } else if (payload.type === "report") {

            const data = payload.data || {};

            state.reportTitle = data.title || state.reportTitle || (isEn ? "Auto Analysis Report" : "自动分析报告");

            state.reportSummary = data.summary || state.reportSummary || "";

            state.reportMarkdown = data.markdown || state.reportMarkdown || "";

            state.reportHtml = data.html_document || state.reportHtml || "";

            state.reportChartBindings = data.chart_bindings || state.reportChartBindings || [];

            state.stopReason = data.stop_reason || "";

            state.status = isEn ? "Finalizing report" : "正在整理最终报告";

            renderAutoAnalysisCard(wrapper, state);

          } else if (payload.type === "analysis_complete") {

            const data = payload.data || {};

            sessionId = data.session_id || sessionId;

            lastProposalId = data.proposal_id || lastProposalId;

            state.stopReason = data.stop_reason || state.stopReason;

            state.status = isEn
              ? `Completed ${data.rounds_completed || state.rounds.length} rounds`
              : `已完成 ${data.rounds_completed || state.rounds.length} 轮`;

            const reportLang = (window.i18n && i18n.lang) || localStorage.getItem("lang") || "zh";
            const baseUrl = data.report_url || state.reportUrl || "";
            state.reportUrl = baseUrl
              ? (baseUrl.includes("lang=")
                ? baseUrl
                : `${baseUrl}${baseUrl.includes("?") ? "&" : "?"}lang=${encodeURIComponent(reportLang)}`)
              : "";

            state.reportTitle = data.report_title || state.reportTitle || (isEn ? "Auto Analysis Report" : "自动分析报告");

            state.complete = true;

            renderAutoAnalysisCard(wrapper, state);
            releaseAnalysisControls(controller);

            refreshSessions();

          } else if (payload.type === "error") {

            updateAiCard(wrapper, i18n.t("error_occurred"), `<div style="color: #ef4444">${escapeHtml(payload.message)}</div>`, state.liveThought);

          }

        } catch (e) {

          console.error("JSON parse error", e, line);

        }

      }

    }

  } catch (e) {
    if (e && e.name === "AbortError") {
      state.stopReason = "stopped_by_user";
      state.status = isEn ? "Stopped" : "已停止";
      state.complete = true;
      renderAutoAnalysisCard(wrapper, state);
    } else {
      updateAiCard(wrapper, i18n.t("request_failed") || "请求失败", `<div style="color: #ef4444">${escapeHtml(e.message)}</div>`);
    }
  } finally {
    releaseAnalysisControls(controller);
  }

}



document.getElementById("sendBtn").onclick = () => {
  if (activeAnalysisController) {
    stopActiveAnalysis();
    return;
  }
  handleSend();
};

document.getElementById("autoAnalyzeBtn").onclick = () => {
  if (activeAnalysisController) {
    stopActiveAnalysis();
    return;
  }
  handleAutoAnalyze();
};



document.getElementById("questionInput").onkeydown = (e) => {

  if (e.key === "Enter") {
    if (activeAnalysisController) {
      e.preventDefault();
      stopActiveAnalysis();
      return;
    }
    handleSend();
  }

};


document.getElementById("saveSkillBtn").onclick = async () => {

  try {

    const name = document.getElementById("skillNameInput").value.trim();

    if (!name) { alert(i18n.t("skill_name_required") || "请输入经验名称"); return; }



    const desc = document.getElementById("skillDescInput")?.value.trim() || "";

    const tagsRaw = document.getElementById("skillTagsInput")?.value.trim() || "";

    const tags = tagsRaw ? tagsRaw.split(/[,，]/).map(t => t.trim()).filter(Boolean) : [];

    const knowledgeRaw = document.getElementById("skillKnowledgeInput")?.value.trim() || "";

    const knowledge = knowledgeRaw ? knowledgeRaw.split("\n").map(l => l.trim()).filter(Boolean) : [];



    if (currentEditingSkillId) {

      // --- UPDATE existing skill ---

      const data = await api(`/api/skills/${currentEditingSkillId}`, {

        method: "PATCH",

        body: JSON.stringify({ name, description: desc, tags, knowledge }),

      });

      addCard(i18n.t("skill_updated") || "经验已更新", `<div>${i18n.t("success_update") || "更新成功："}<strong>${escapeHtml(data.name || name)}</strong>${desc ? `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">${escapeHtml(desc)}</div>` : ''}</div>`);

      cancelSkillEdit();

    } else {

      // --- CREATE new skill from proposal ---

      if (!lastProposalId) { alert(i18n.t("no_proposal_to_save") || "暂无成功执行的迭代记录可保存"); return; }

      const overwriteSelect = document.getElementById("overwriteSkillSelect");

      const overwriteId = overwriteSelect ? overwriteSelect.value : "";

      

      const data = await api("/api/skills/save", {

        method: "POST",

        body: JSON.stringify({

          proposal_id: lastProposalId,

          name,

          description: desc,

          tags,

          knowledge,

          overwrite_skill_id: overwriteId || null

        }),

      });

      const isOverwrite = !!overwriteId;

      addCard(i18n.t("skill_saved") || "经验已保存", `<div>${isOverwrite ? (i18n.t('success_update') || '更新成功：') : (i18n.t('success_save') || '保存成功：')}<strong>${data.skill.name}</strong>${data.skill.version ? ` (v${data.skill.version})` : ''}${desc ? `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">${escapeHtml(desc)}</div>` : ''}</div>`);

      document.getElementById("skillNameInput").value = "";

      if (document.getElementById("skillDescInput")) document.getElementById("skillDescInput").value = "";

      if (document.getElementById("skillTagsInput")) document.getElementById("skillTagsInput").value = "";

      if (document.getElementById("skillKnowledgeInput")) document.getElementById("skillKnowledgeInput").value = "";



      if (skillModal) skillModal.style.display = "none";

    }

    await refreshSkills();

  } catch (e) {

    addCard(i18n.t("op_failed") || "操作失败", `<div style="color: #ef4444">${e.message}</div>`);

  }

};



document.getElementById("btnNewSession")?.addEventListener("click", startNewSession);



document.getElementById("uploadBtn").onclick = async () => {

  try {

    const input = document.getElementById("uploadInput");

    if (!input.files || input.files.length === 0) {

      alert(i18n.t("select_files_first") || "请先选择文件");

      return;

    }



    // Disable button to prevent double-click

    const btn = document.getElementById("uploadBtn");

    const originalText = btn.innerHTML;

    btn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('uploading') || '涓婁紶涓?..'}`;

    btn.disabled = true;



    const form = new FormData();

    for (let i = 0; i < input.files.length; i++) {

      form.append("files", input.files[i]);

    }



    if (sessionId) form.append("session_id", sessionId);

    const sandboxId = sandboxSelect.value;

    if (sandboxId) form.append("sandbox_id", sandboxId);



    const token = localStorage.getItem("token");

    const headers = {
      "X-Language": i18n.lang || localStorage.getItem("lang") || "zh"
    };

    if (token) headers["Authorization"] = `Bearer ${token}`;



    const res = await fetch("/api/data/upload", { method: "POST", headers, body: form });

    if (!res.ok) throw new Error(await res.text());



    const data = await res.json();

    if (data.session_id) {

      sessionId = data.session_id;

    }



    if (data.uploaded_files && data.uploaded_files.length > 0) {

      data.uploaded_files.forEach(f => {

        const existingIdx = uploadedFiles.findIndex(uf => uf.dataset_name === f.dataset_name);

        if (existingIdx >= 0) {

          uploadedFiles[existingIdx] = f;

        } else {

          uploadedFiles.push(f);

        }

        // Log to chat

        addCard(i18n.t("upload_success"), `<div>${i18n.t('filename')}锛?{f.dataset_name}</div>${f.is_tabular ? `<div>${i18n.t('rows')}锛?{f.rows}</div><div>${i18n.t('cols')}锛?{f.columns.join(", ")}</div>` : `<div>${i18n.t('type_doc')}</div>`}<div>${i18n.t('auto_context')}</div>`);

      });



      renderDataModels();

    }



    input.value = "";

    btn.innerHTML = originalText;

    btn.disabled = false;



    // Close modal on success

    document.getElementById("uploadModal").style.display = "none";

  } catch (e) {

    addCard(i18n.t("upload_failed"), `<div style="color: #ef4444">${e.message}</div>`);

    document.getElementById("uploadBtn").innerHTML = i18n.t("upload_to_chat");

    document.getElementById("uploadBtn").disabled = false;

  }

};



const uploadModal = document.getElementById("uploadModal");

const openUploadModalBtn = document.getElementById("openUploadModalBtn");

const closeUploadModalBtn = document.getElementById("closeUploadModalBtn");



openUploadModalBtn.onclick = () => {

  uploadModal.style.display = "flex";

};



closeUploadModalBtn.onclick = () => {

  uploadModal.style.display = "none";

};



const closeSkillModalBtn = document.getElementById("closeSkillModalBtn");
const jumpSourceSessionBtn = document.getElementById("jumpSourceSessionBtn");

const skillEditCancelLink = document.getElementById("skillEditCancelLink");



if (closeSkillModalBtn) {

  closeSkillModalBtn.onclick = () => {

    if (skillModal) skillModal.style.display = "none";

    cancelSkillEdit();

  }

}

if (skillEditCancelLink) {

  skillEditCancelLink.onclick = (e) => {

    e.preventDefault();

    if (skillModal) skillModal.style.display = "none";

    cancelSkillEdit();

  };

}

if (jumpSourceSessionBtn) {

  jumpSourceSessionBtn.onclick = async () => {

    if (!skillSourceSessionId) return;

    if (sessionId === skillSourceSessionId) {

      if (skillModal) skillModal.style.display = "none";

      return;

    }

    if (skillModal) skillModal.style.display = "none";

    await switchSession(skillSourceSessionId);

  };

}



// External DB connection modal logic



const skillMountModal = document.getElementById("skillMountModal");

const openSkillMountModalBtn = document.getElementById("openSkillMountModalBtn");

const closeSkillMountModalBtn = document.getElementById("closeSkillMountModalBtn");

const cancelSkillMountBtn = document.getElementById("cancelSkillMountBtn");

const saveSkillMountBtn = document.getElementById("saveSkillMountBtn");

const skillMountTitle = skillMountModal ? skillMountModal.querySelector('[data-i18n="skill_mount_title"]') : null;
const skillMountOpenLabel = openSkillMountModalBtn ? openSkillMountModalBtn.querySelector('[data-i18n="manage_skill_mounts"]') : null;
const skillMountSaveLabel = saveSkillMountBtn ? saveSkillMountBtn.querySelector('[data-i18n="save_skill_mounts"]') : null;

if (skillMountTitle) {

  skillMountTitle.textContent = i18n.t("skill_mount_title") || "Mount Skills to Workspace";

}

if (skillMountOpenLabel) {

  skillMountOpenLabel.textContent = i18n.t("manage_skill_mounts") || "Manage Mounted Skills";

}

if (skillMountSaveLabel) {

  skillMountSaveLabel.textContent = i18n.t("save_skill_mounts") || "Save Mounted Skills";

}


function closeSkillMountModal() {

  if (skillMountModal) skillMountModal.style.display = "none";

}


function renderSkillMountList(skills) {

  const target = document.getElementById("skillMountTarget");

  const list = document.getElementById("skillMountList");

  const currentSandbox = getCurrentSandbox();

  if (!target || !list || !saveSkillMountBtn) return;

  if (!currentSandbox) {

    target.textContent = i18n.t("select_sandbox_first") || "请先选择沙盒";

    list.innerHTML = `<div class="empty-state">${i18n.t("select_sandbox_first") || "请先选择沙盒"}</div>`;

    saveSkillMountBtn.disabled = true;

    return;

  }

  target.textContent = `${i18n.t("current_sandbox_label") || "当前沙盒"}: ${currentSandbox.name}`;

  saveSkillMountBtn.disabled = false;

  if (!skills.length) {

    list.innerHTML = `<div class="empty-state">${i18n.t("no_skills")}</div>`;

    return;

  }

  const mountedSkillIds = new Set(currentSandbox.mounted_skills || []);

  list.innerHTML = skills.map((skill) => {

    const knowledgeCount = (skill.layers?.knowledge || []).length;
    const tags = (skill.tags || []).slice(0, 3).join(", ") || (i18n.t("no_tags") || "无标签");

    return `
      <label style="display:flex; gap:12px; align-items:flex-start; border:1px solid var(--border); border-radius:8px; padding:12px; cursor:pointer;">
        <input type="checkbox" class="skill-mount-checkbox" value="${skill.skill_id}" ${mountedSkillIds.has(skill.skill_id) ? "checked" : ""} style="margin-top:3px;" />
        <div style="flex:1; min-width:0;">
          <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">
            <strong style="font-size:14px; color:var(--text-primary);">${escapeHtml(skill.name)}</strong>
            ${mountedSkillIds.has(skill.skill_id) ? `<span class="badge" style="font-size:10px; padding:2px 6px; border:none; border-radius:999px; background:#dcfce7; color:#166534;">${i18n.t("mounted_skill") || "已挂载"}</span>` : ""}
          </div>
          <div style="font-size:12px; color:var(--text-muted); margin-bottom:4px;">${escapeHtml(skill.description || "")}</div>
          <div style="font-size:12px; color:var(--text-muted); display:flex; gap:10px; flex-wrap:wrap;">
            <span><i class="fa-solid fa-tag"></i> ${escapeHtml(tags)}</span>
            <span><i class="fa-solid fa-book-open"></i> ${i18n.t("knowledge_count", { count: knowledgeCount }) || `知识 ${knowledgeCount} 条`}</span>
          </div>
        </div>
      </label>
    `;

  }).join("");

}


async function openSkillMountModal() {

  if (!skillMountModal) return;

  if (!sandboxSelect.value) {

    alert(i18n.t("select_sandbox_first"));

    return;

  }

  skillMountModal.style.display = "flex";

  const skills = await api("/api/skills");

  renderSkillMountList(skills.skills || []);

}


if (openSkillMountModalBtn) {

  openSkillMountModalBtn.onclick = async () => {

    try {

      await openSkillMountModal();

    } catch (e) {

      addCard(i18n.t("op_failed") || "操作失败", `<div style="color: #ef4444">${e.message}</div>`);

  }

};

}


if (closeSkillMountModalBtn) {

  closeSkillMountModalBtn.onclick = closeSkillMountModal;

}


if (cancelSkillMountBtn) {

  cancelSkillMountBtn.onclick = closeSkillMountModal;

}


if (saveSkillMountBtn) {

  saveSkillMountBtn.onclick = async () => {

    const sandboxId = sandboxSelect.value;

    if (!sandboxId) {

      alert(i18n.t("select_sandbox_first"));

      return;

    }

    const selectedSkillIds = Array.from(document.querySelectorAll(".skill-mount-checkbox:checked")).map((cb) => cb.value);

    const originalHtml = saveSkillMountBtn.innerHTML;

    saveSkillMountBtn.disabled = true;
    saveSkillMountBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("saving") || "保存中..."}`;


    try {

      await api(`/api/sandboxes/${sandboxId}/skills`, {

        method: "POST",

        body: JSON.stringify({ skills: selectedSkillIds }),

      });

      await refreshProfile(sandboxId);

      closeSkillMountModal();
      addCard(i18n.t("skill_mount_saved") || "经验挂载已更新", `<div>${i18n.t("skill_mount_saved_desc", { count: selectedSkillIds.length }) || `当前沙盒已挂载 ${selectedSkillIds.length} 条经验`}</div>`);


    } catch (e) {

      addCard(i18n.t("op_failed") || "操作失败", `<div style="color: #ef4444">${e.message}</div>`);

    } finally {

      saveSkillMountBtn.disabled = false;

      saveSkillMountBtn.innerHTML = originalHtml;

    }

  };

}


const dbModal = document.getElementById("dbModal");
const dbMountModal = document.getElementById("dbMountModal");
const openDbModalBtn = document.getElementById("openDbModalBtn");
const closeDbModalBtn = document.getElementById("closeDbModalBtn");
const openDbMountModalBtn = document.getElementById("openDbMountModalBtn");
const closeDbMountModalBtn = document.getElementById("closeDbMountModalBtn");

let currentDbConnectionId = "";

function getDbFormData() {
  return {
    name: document.getElementById("dbConnNameInput").value.trim(),
    db_type: document.getElementById("dbTypeInput").value,
    host: document.getElementById("dbHostInput").value.trim() || "localhost",
    port: document.getElementById("dbPortInput").value.trim() ? parseInt(document.getElementById("dbPortInput").value, 10) : null,
    database: document.getElementById("dbNameInput").value.trim(),
    username: document.getElementById("dbUserInput").value.trim(),
    password: document.getElementById("dbPassInput").value
  };
}

function setDbMessage(message, color = "inherit") {
  const el = document.getElementById("dbMsg");
  if (el) el.innerHTML = message ? `<span style="color:${color}">${message}</span>` : "";
}

function setDbMountMessage(message, color = "inherit") {
  const el = document.getElementById("dbMountMsg");
  if (el) el.innerHTML = message ? `<span style="color:${color}">${message}</span>` : "";
}

function toggleDbTypeFields() {
  const isSqlite = document.getElementById("dbTypeInput").value === "sqlite";
  document.getElementById("dbHostInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbPortInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbUserInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbPassInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbNameInput").placeholder = isSqlite ? (i18n.t("sqlite_path_placeholder") || "SQLite DB Absolute Path (Required)") : "DB Name";
}

function clearDbForm() {
  currentDbConnectionId = "";
  document.getElementById("dbConnectionSelect").value = "";
  document.getElementById("dbConnNameInput").value = "";
  document.getElementById("dbTypeInput").value = "sqlite";
  document.getElementById("dbHostInput").value = "localhost";
  document.getElementById("dbPortInput").value = "";
  document.getElementById("dbNameInput").value = "";
  document.getElementById("dbUserInput").value = "";
  document.getElementById("dbPassInput").value = "";
  document.getElementById("dbPassInput").placeholder = i18n.t("password_retain_hint") || "密码(留空表示保持原值)";
  toggleDbTypeFields();
}

function fillDbForm(connection) {
  if (!connection) {
    clearDbForm();
    return;
  }
  currentDbConnectionId = connection.connection_id || "";
  document.getElementById("dbConnectionSelect").value = currentDbConnectionId;
  document.getElementById("dbConnNameInput").value = connection.name || "";
  document.getElementById("dbTypeInput").value = connection.db_type || "sqlite";
  document.getElementById("dbHostInput").value = connection.host || "localhost";
  document.getElementById("dbPortInput").value = connection.port ?? "";
  document.getElementById("dbNameInput").value = connection.database || "";
  document.getElementById("dbUserInput").value = connection.username || "";
  document.getElementById("dbPassInput").value = "";
  document.getElementById("dbPassInput").placeholder = i18n.t("password_retain_hint") || "密码(留空表示保持原值)";
  toggleDbTypeFields();
}

function renderDbConnectionSelects(selectedId = "") {
  const connSelect = document.getElementById("dbConnectionSelect");
  const mountSelect = document.getElementById("dbMountConnectionSelect");
  const options = ['<option value="">--</option>']
    .concat((dbConnectionsData || []).map((conn) => `<option value="${conn.connection_id}">${escapeHtml(conn.name || conn.connection_id)} (${escapeHtml(conn.db_type || "")})</option>`))
    .join("");
  if (connSelect) {
    connSelect.innerHTML = options;
    connSelect.value = selectedId || "";
  }
  if (mountSelect) {
    mountSelect.innerHTML = options;
    const currentSandbox = getCurrentSandbox();
    mountSelect.value = currentSandbox?.db_connection_id || selectedId || "";
  }
}

async function loadDbConnections(selectedId = "") {
  const res = await api("/api/db-connections");
  dbConnectionsData = res.connections || [];
  renderDbConnectionSelects(selectedId);
  if (selectedId) {
    fillDbForm(dbConnectionsData.find((conn) => conn.connection_id === selectedId) || null);
  } else if (!currentDbConnectionId) {
    clearDbForm();
  }
  return dbConnectionsData;
}

function renderDbMountTables(tableNames) {
  currentDbMountTableNames = Array.isArray(tableNames) ? tableNames.slice() : [];
  const container = document.getElementById("dbMountTablesContainer");
  const list = document.getElementById("dbMountTablesList");
  const searchInput = document.getElementById("dbMountTableSearchInput");
  if (!container || !list) return;
  const selectedTables = new Set(Array.from(list.querySelectorAll(".db-table-checkbox:checked")).map((cb) => cb.value));
  const filterText = (currentDbMountTableFilter || "").trim().toLowerCase();
  const filteredTables = currentDbMountTableNames.filter((table) => !filterText || String(table).toLowerCase().includes(filterText));
  list.innerHTML = "";
  if (searchInput) {
    searchInput.style.display = currentDbMountTableNames.length > 0 ? "" : "none";
  }
  if (!currentDbMountTableNames || currentDbMountTableNames.length === 0) {
    container.style.display = "none";
    return;
  }
  if (filteredTables.length === 0) {
    list.innerHTML = `<div style="font-size: 13px; color: var(--text-muted); padding: 4px 2px;">${i18n.t("db_mount_search_empty") || "没有匹配的表名"}</div>`;
    container.style.display = "block";
    return;
  }
  filteredTables.forEach((table) => {
    const div = document.createElement("div");
    div.style.marginBottom = "8px";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = table;
    cb.id = `chk_mount_${table}`;
    cb.style.marginRight = "8px";
    cb.className = "db-table-checkbox";
    cb.checked = selectedTables.has(table);
    cb.onchange = () => {
      const checkedCount = document.querySelectorAll(".db-table-checkbox:checked").length;
      if (checkedCount > MAX_SELECTED_TABLES) {
        cb.checked = false;
        alert(i18n.t("max_5_tables") || `Max ${MAX_SELECTED_TABLES} tables allowed`);
      }
    };
    const label = document.createElement("label");
    label.htmlFor = cb.id;
    label.textContent = table;
    label.style.cursor = "pointer";
    div.appendChild(cb);
    div.appendChild(label);
    list.appendChild(div);
  });
  container.style.display = "block";
}

function resetDbMountTables() {
  const container = document.getElementById("dbMountTablesContainer");
  const list = document.getElementById("dbMountTablesList");
  const searchInput = document.getElementById("dbMountTableSearchInput");
  if (container) container.style.display = "none";
  if (list) list.innerHTML = "";
  if (searchInput) searchInput.value = "";
  currentDbMountTableNames = [];
  currentDbMountTableFilter = "";
}

async function refreshDbMountTarget() {
  const target = document.getElementById("dbMountTarget");
  if (!target) return;
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) {
    target.textContent = i18n.t("select_sandbox_first") || "请先选择沙盒";
    return;
  }
  target.textContent = `${i18n.t("current_sandbox_label") || "当前工作空间"}: ${currentSandbox.name}`;
}

openDbModalBtn.onclick = async () => {
  dbModal.style.display = "flex";
  await loadDbConnections();
  clearDbForm();
  setDbMessage("");
};

closeDbModalBtn.onclick = () => {
  dbModal.style.display = "none";
};

openDbMountModalBtn.onclick = async () => {
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) {
    alert(i18n.t("select_sandbox_first") || "Please select a sandbox first");
    return;
  }
  dbMountModal.style.display = "flex";
  await loadDbConnections("");
  await refreshDbMountTarget();
  setDbMountMessage("");
  if (currentSandbox.db_connection_id) {
    document.getElementById("dbMountConnectionSelect").value = currentSandbox.db_connection_id;
  }
  resetDbMountTables();
};

closeDbMountModalBtn.onclick = () => {
  dbMountModal.style.display = "none";
};

window.onclick = (event) => {
  if (event.target === dbModal) {
    dbModal.style.display = "none";
  }
  if (event.target === dbMountModal) {
    dbMountModal.style.display = "none";
  }
  if (event.target === uploadModal) {
    uploadModal.style.display = "none";
  }
  if (event.target === skillModal) {
    skillModal.style.display = "none";
    cancelSkillEdit();
  }
  if (event.target === skillMountModal) {
    closeSkillMountModal();
  }
};

document.getElementById("dbConnectionSelect").onchange = (event) => {
  currentDbConnectionId = event.target.value || "";
  if (!currentDbConnectionId) {
    clearDbForm();
    return;
  }
  fillDbForm(dbConnectionsData.find((item) => item.connection_id === currentDbConnectionId) || null);
};

document.getElementById("dbMountConnectionSelect").onchange = () => {
  resetDbMountTables();
};

document.getElementById("dbMountTableSearchInput").oninput = (event) => {
  currentDbMountTableFilter = event.target.value || "";
  renderDbMountTables(currentDbMountTableNames);
};

document.getElementById("dbTestBtn").onclick = async () => {
  const payload = getDbFormData();
  if (!payload.database) {
    setDbMessage(i18n.t("enter_db_name"), "red");
    return;
  }
  setDbMessage(`<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("testing")}`, "#6b7280");
  try {
    const res = await api("/api/db-connections/test", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      throw new Error(res.error || (i18n.t("error_db_config") || "DB configuration error"));
    }
    setDbMessage(i18n.t("test_success") || "Test success", "green");
  } catch (e) {
    setDbMessage(e.message, "red");
  }
};

document.getElementById("dbSaveConnectionBtn").onclick = async () => {
  const payload = getDbFormData();
  if (!payload.database) {
    setDbMessage(i18n.t("enter_db_name"), "red");
    return;
  }
  const btn = document.getElementById("dbSaveConnectionBtn");
  const originalText = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("processing")}`;
  try {
    const endpoint = currentDbConnectionId ? `/api/db-connections/${currentDbConnectionId}` : "/api/db-connections";
    const method = currentDbConnectionId ? "PUT" : "POST";
    const res = await api(endpoint, {
      method,
      body: JSON.stringify(payload)
    });
    currentDbConnectionId = res.connection.connection_id;
    await loadDbConnections(currentDbConnectionId);
    document.getElementById("dbPassInput").value = "";
    setDbMessage((i18n.t("success_save") || "Saved: ") + res.connection.name, "green");
  } catch (e) {
    setDbMessage(e.message, "red");
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalText;
  }
};

document.getElementById("dbDeleteConnectionBtn").onclick = async () => {
  if (!currentDbConnectionId) {
    setDbMessage(i18n.t("select_db_connection_first") || "请选择数据库连接", "red");
    return;
  }
  const selected = dbConnectionsData.find((item) => item.connection_id === currentDbConnectionId);
  if (!confirm(i18n.t("confirm_delete_connection", { name: selected?.name || currentDbConnectionId }) || `Delete connection "${selected?.name || currentDbConnectionId}"?`)) return;
  try {
    await api(`/api/db-connections/${currentDbConnectionId}`, { method: "DELETE" });
    await loadDbConnections("");
    await refreshProfile();
    clearDbForm();
    setDbMessage(i18n.t("delete_success") || "Deleted", "green");
  } catch (e) {
    setDbMessage(e.message, "red");
  }
};

document.getElementById("dbMountBtn").onclick = async () => {
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) {
    setDbMountMessage(i18n.t("select_sandbox_first"), "red");
    return;
  }
  const connectionId = document.getElementById("dbMountConnectionSelect").value || "";
  if (!connectionId) {
    setDbMountMessage(i18n.t("select_db_connection_first") || "请先选择数据库连接", "red");
    return;
  }
  setDbMountMessage(`<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("connecting")}`, "#6b7280");
  try {
    const res = await api(`/api/sandboxes/${currentSandbox.sandbox_id}/db-connection`, {
      method: "PUT",
      body: JSON.stringify({ connection_id: connectionId })
    });
    renderDbMountTables(res.tables || []);
    setDbMountMessage(i18n.t("connect_success_select") || "Connect success, please select tables", "green");
    await refreshProfile();
    await refreshDbMountTarget();
  } catch (e) {
    setDbMountMessage(e.message, "red");
  }
};

document.getElementById("dbUnmountBtn").onclick = async () => {
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) {
    setDbMountMessage(i18n.t("select_sandbox_first"), "red");
    return;
  }
  try {
    await api(`/api/sandboxes/${currentSandbox.sandbox_id}/db-connection`, {
      method: "PUT",
      body: JSON.stringify({ connection_id: null })
    });
    resetDbMountTables();
    document.getElementById("dbMountConnectionSelect").value = "";
    setDbMountMessage(i18n.t("db_unmounted") || "连接已解绑", "green");
    await refreshProfile();
    await refreshDbMountTarget();
  } catch (e) {
    setDbMountMessage(e.message, "red");
  }
};

document.getElementById("dbMountSaveTablesBtn").onclick = async () => {
  const currentSandbox = getCurrentSandbox();
  if (!currentSandbox) return;
  const checkedBoxes = Array.from(document.querySelectorAll("#dbMountTablesList .db-table-checkbox:checked"));
  const selectedTables = checkedBoxes.map((cb) => cb.value);
  if (selectedTables.length === 0) {
    alert(i18n.t("select_one_table"));
    return;
  }
  const btn = document.getElementById("dbMountSaveTablesBtn");
  const originalText = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("processing")}`;
  try {
    await api(`/api/sandboxes/${currentSandbox.sandbox_id}/db-tables`, {
      method: "POST",
      body: JSON.stringify({ tables: selectedTables })
    });
    await refreshProfile();
    addCard(
      i18n.t("connect_db_success"),
      `<div style="color: #10b981; font-weight: 500;"><i class="fa-solid fa-circle-check"></i> ${i18n.t("connect_db_success_msg", { tables: selectedTables.join(", ") })}</div><div style="margin-top: 8px; font-size: 14px; color: #374151;">${i18n.t("connect_db_hint")}</div>`
    );
    dbMountModal.style.display = "none";
  } catch (e) {
    alert((i18n.t("save_tables_failed") || "Save tables failed") + ": " + e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalText;
  }
};

document.getElementById("dbTypeInput").onchange = () => {
  toggleDbTypeFields();
};

document.getElementById("dbTypeInput").dispatchEvent(new Event("change"));
// --- Workspace CRUD Events ---

document.getElementById("btnNewSandbox").onclick = async () => {

  const name = prompt(i18n.t("enter_new_sandbox_name"), i18n.t("default_sandbox_name"));

  if (!name) return;



  try {

    const res = await api("/api/sandboxes", {

      method: "POST",

      body: JSON.stringify({ name: name, allowed_groups: [] })

    });

    // Force the selection to be the newly created sandbox

    await refreshProfile(res.sandbox_id);

  } catch (e) {

    alert(i18n.t('create_failed') + ": " + e.message);

  }

};



document.getElementById("btnRenameSandbox").onclick = async () => {

  const currentSandboxId = sandboxSelect.value;

  if (!currentSandboxId) return;

  const currentSandbox = sandboxesData.find(s => s.sandbox_id === currentSandboxId);

  const newName = prompt(i18n.t("rename_sandbox"), currentSandbox.name);

  if (!newName || newName === currentSandbox.name) return;



  try {

    await api(`/api/sandboxes/${currentSandboxId}`, {

      method: "PUT",

      body: JSON.stringify({ name: newName })

    });

    await refreshProfile();

  } catch (e) {

    alert(i18n.t('rename_failed') + ": " + e.message);

  }

};



document.getElementById("btnDeleteSandbox").onclick = async () => {

  const currentSandboxId = sandboxSelect.value;

  if (!currentSandboxId) return;

  const currentSandbox = sandboxesData.find(s => s.sandbox_id === currentSandboxId);

  if (!confirm(i18n.t("confirm_delete_sandbox", { name: currentSandbox.name }))) return;



  try {

    await api(`/api/sandboxes/${currentSandboxId}`, { method: "DELETE" });

    sandboxSelect.value = "";

    await refreshProfile();

  } catch (e) {

    alert(i18n.t('delete_failed') + ": " + e.message);

  }

};





// Initial Load

document.title = i18n.t("app_title");

refreshProfile();



// Sidebar interactions

function setupSidebar(sidebarId, resizerId, toggleBtnId, direction) {

  const sidebar = document.getElementById(sidebarId);

  const resizer = document.getElementById(resizerId);

  const toggleBtn = document.getElementById(toggleBtnId);

  if (!sidebar || !resizer || !toggleBtn) return;



  toggleBtn.onclick = () => sidebar.classList.toggle("collapsed");



  let isResizing = false;

  let startX = 0;

  let startWidth = 0;



  resizer.addEventListener("mousedown", (e) => {

    isResizing = true;

    startX = e.clientX;

    startWidth = parseInt(window.getComputedStyle(sidebar).width, 10);

    resizer.classList.add("resizing");

    document.body.style.cursor = "col-resize";

    document.body.classList.add("no-select"); // Optional: add a class to disable selection during resize

    e.preventDefault();

  });



  window.addEventListener("mousemove", (e) => {

    if (!isResizing) return;

    

    let delta = e.clientX - startX;

    let newWidth = direction === "left" ? startWidth + delta : startWidth - delta;

    

    // Auto-collapse logic

    if (newWidth < 100) {

      sidebar.classList.add("collapsed");

      newWidth = 50; 

    } else {

      sidebar.classList.remove("collapsed");

      if (newWidth < 150) newWidth = 150;

      if (newWidth > 600) newWidth = 600;

    }

    

    sidebar.style.width = `${newWidth}px`;

  });



  window.addEventListener("mouseup", () => {

    if (isResizing) {

      isResizing = false;

      resizer.classList.remove("resizing");

      document.body.style.cursor = "default";

      document.body.classList.remove("no-select");

    }

  });

}

setupSidebar("leftSidebar", "resizerLeft", "toggleLeftBtn", "left");

setupSidebar("rightSidebar", "resizerRight", "toggleRightBtn", "right");



// Initialize global modal reference

skillModal = document.getElementById("skillModal");



// Initial data load

refreshSkills();

refreshSessions();

normalizeStaticText();

refreshProfile();
renderSkillContextSnapshot(null);

const initialSessionFromUrl = getSessionIdFromUrl();
if (initialSessionFromUrl) {
  switchSession(initialSessionFromUrl);
}



// Event listeners for skill proposal

document.getElementById("proposeSkillBtn").onclick = () => {

    // Get the last user message from the DOM

    const userBubbles = document.querySelectorAll(".user-bubble");

    const lastMsg = userBubbles.length > 0 ? userBubbles[userBubbles.length - 1].innerText : "";

    proposeSkillMetadata(lastProposalId, lastMsg);

};



async function proposeSkillMetadata(proposalId, userMessage) {

  if (!proposalId) {

    alert(i18n.t("propose_before_extract"));

    return;

  }



  const proposeBtn = document.getElementById("proposeSkillBtn");

  const originalBtnContent = proposeBtn ? proposeBtn.innerHTML : "";

  if (proposeBtn) {

    proposeBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('starting')}`;

    proposeBtn.disabled = true;

  }



  const nameInput = document.getElementById("skillNameInput");

  const descInput = document.getElementById("skillDescInput");

  const tagsInput = document.getElementById("skillTagsInput");

  const knowledgeInput = document.getElementById("skillKnowledgeInput");



  // Create thinking card in chat

  const wrapper = createAiMessageContainer();

  let accumulatedThought = i18n.t('starting') + "\n";

  updateAiCard(wrapper, i18n.t("propose_skill_title"), `<div>${i18n.t('preparing_env')}</div>`, accumulatedThought);



  const sandboxId = sandboxSelect.value;

  let distilledData = null;



  try {

    // Stage 1: Context Analysis

    accumulatedThought += i18n.t("propose_step_1") + "\n";

    updateAiCard(wrapper, i18n.t("propose_skill_title"), `<div>${i18n.t('reviewing_history')}</div>`, accumulatedThought);

    await new Promise(r => setTimeout(r, 800));



    // Stage 2: Knowledge Extraction

    accumulatedThought += i18n.t("propose_step_2") + "\n";

    updateAiCard(wrapper, i18n.t("propose_skill_title"), `<div>${i18n.t('extracting_metrics')}</div>`, accumulatedThought);

    

    // Start the API call in parallel with the structure generation thinking step

    const requestPromise = api("/api/skills/propose", {

      method: "POST",

      body: JSON.stringify({

        proposal_id: proposalId,

        message: userMessage,

        sandbox_id: sandboxId

      })

    });



    // Stage 3: Structure Generation

    accumulatedThought += i18n.t("propose_step_3") + "\n";

    updateAiCard(wrapper, i18n.t("propose_skill_title"), `<div>${i18n.t('building_structure')}</div>`, accumulatedThought);

    

    const [data] = await Promise.all([requestPromise, new Promise(r => setTimeout(r, 1200))]);

    distilledData = data;



    // Stage 4: Refinement

    accumulatedThought += i18n.t("propose_step_4") + "\n";

    updateAiCard(wrapper, i18n.t("propose_skill_title"), `<div>${i18n.t('optimizing_result')}</div>`, accumulatedThought);

    await new Promise(r => setTimeout(r, 500));



    // Populate the hidden form fields for the modal

    if (distilledData.name && nameInput) nameInput.value = distilledData.name;

    if (distilledData.description && descInput) descInput.value = distilledData.description;

    if (Array.isArray(distilledData.tags) && tagsInput) tagsInput.value = distilledData.tags.join(", ");

    if (Array.isArray(distilledData.knowledge) && knowledgeInput) knowledgeInput.value = distilledData.knowledge.join("\n");



    // Clear skill id to ensure create mode

    currentEditingSkillId = "";
    renderSkillContextSnapshot(distilledData.context_snapshot || null);

    

    // Load existing skills into overwrite dropdown

    const overwriteGroup = document.getElementById("overwriteSkillGroup");

    const overwriteSelect = document.getElementById("overwriteSkillSelect");

    if (overwriteGroup && overwriteSelect) {

      overwriteGroup.style.display = "block";

      api("/api/skills").then(res => {

          overwriteSelect.innerHTML = `<option value="">🆕 ${i18n.t('create_new_skill') || '创建全新经验'}</option>`;

          res.skills.forEach(sk => {

              overwriteSelect.innerHTML += `<option value="${sk.skill_id}">📝 ${i18n.t('overwrite_skill') || '覆盖已有经验'}: ${escapeHtml(sk.name)}${sk.version ? ` (v${sk.version})` : ''}</option>`;

          });

          overwriteSelect.value = "";

          // Automatically select if the proposed name exactly matches an existing skill

          const exactMatch = res.skills.find(sk => sk.name === distilledData.name);

          if (exactMatch) overwriteSelect.value = exactMatch.skill_id;

      }).catch(e => console.error("Failed to load skills for overwrite", e));

    }



    // Final Success State in Chat

    const successHtml = `

      <div style="background: #f0fdf4; border-radius: 8px; padding: 16px; border: 1px solid #bbf7d0; margin-bottom: 12px;">

        <div style="color: #166534; font-weight: 600; margin-bottom: 8px;"><i class="fa-solid fa-circle-check"></i> ${i18n.t('extract_complete_title')}</div>

        <div style="font-size: 13px; color: #15803d; line-height: 1.5; margin-bottom: 12px;">

          ${i18n.t('extract_complete_desc')}

        </div>

        <button class="btn btn-primary btn-block review-skill-btn" style="padding: 10px;">

          <i class="fa-solid fa-eye"></i> ${i18n.t('click_to_review')}

        </button>

      </div>

    `;

    updateAiCard(wrapper, i18n.t("extract_success"), successHtml, accumulatedThought);



    // Bind the review button

    const reviewBtn = wrapper.querySelector(".review-skill-btn");

    if (reviewBtn) {

      reviewBtn.onclick = () => {

        if (skillModal) {

            skillModal.style.display = "flex";

        } else {

            // Fallback if global init failed

            const m = document.getElementById("skillModal");

            if (m) m.style.display = "flex";

        }

      };

    }



  } catch (e) {

    console.warn("Auto propose skill metadata failed", e);

    updateAiCard(wrapper, i18n.t("extract_failed"), `<div style="color: #ef4444">${e.message}</div>`, accumulatedThought);

  } finally {

    // Restore sidebar button state

    if (proposeBtn) {

      proposeBtn.innerHTML = originalBtnContent;

      proposeBtn.disabled = false;

    }

  }

}

