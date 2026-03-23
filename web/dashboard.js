let sessionId = "";
let lastProposalId = "";
let sandboxesData = [];
let uploadedFiles = [];
let currentEditingSkillId = ""; // skill being edited, or "" for create mode
let skillModal = null; // Global reference for the skill detail modal
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
      label.title = f.is_tabular ? `${i18n.t("columns") || "列"}: ${f.columns.join(", ")}` : (i18n.t("doc_ai_hint") || "文档内容将在提问时交给AI分析");

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
});

async function refreshSkills() {
  const skills = await api("/api/skills");
  skillList.innerHTML = "";
  if (skills.skills.length === 0) {
    skillList.innerHTML = `<li class="empty-state">${i18n.t('no_skills')}</li>`;
  } else {
    skills.skills.forEach((s) => {
      const li = document.createElement("li");
      li.className = "skill-item";
      li.title = i18n.t("click_edit_skill") || "点击查看/修改经验";
      li.innerHTML = `
        <div class="skill-item-header">
          <span class="skill-item-title">${escapeHtml(s.name)}</span>
          ${s.version ? `<span class="badge" style="font-size:10px; padding:2px 4px; border-radius:4px; border:none; background:#e0e7ff; color:#4f46e5; margin-left:6px;">v${s.version}</span>` : ''}
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
  lastProposalId = "";

  cards.innerHTML = `<div style="padding:20px;color:var(--text-muted);text-align:center;"><i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t("loading_history") || "加载历史对话..."}</div>`;
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
          `• ${escapeHtml(c.text || '')} <span style="font-size:11px;color:#64748b;">（${i18n.t('confidence')} ${Math.round((c.confidence || 1) * 100)}%）</span>`
        ).join("<br/>")}</div>`;
      }

      // -- Hypotheses --
      let hypothesesHtml = "";
      if (iter.hypotheses && iter.hypotheses.length > 0) {
        hypothesesHtml = `<details style="margin-top:8px;">
          <summary style="cursor:pointer;font-size:12px;font-weight:600;color:var(--text-muted);"><i class="fa-solid fa-flask"></i> ${i18n.t("hypotheses")} (${iter.hypotheses.length})</summary>
          ${iter.hypotheses.map(h => `<div style="font-size:12px;padding:4px 0;"> • ${escapeHtml(h.text || '')}</div>`).join("")}
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
      codeBlocks += `<div style="margin-top:12px;font-weight:600;font-size:12px">${i18n.t("exec_sql") || "执行 SQL"}:</div><pre>${escapeHtml(result.sql)}</pre>`;
    }
    if (result.python_code) {
      codeBlocks += `<div style="margin-top:12px;font-weight:600;font-size:12px">${i18n.t("exec_python") || "执行 Python"}:</div>
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
      document.getElementById("questionInput").value = `${i18n.t("validate_hypothesis") || "验证猜想"}：${btn.getAttribute("data-text")}`;
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

  try {
    const token = localStorage.getItem("token");
    const headers = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = `Bearer ${token}`;

    const response = await fetch("/api/chat/iterate", {
      method: "POST",
      headers,
      body: JSON.stringify(reqBody),
    });

    if (!response.ok) throw new Error((i18n.t("request_failed") || "请求失败") + `: ${response.status}`);

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let accumulatedThought = "";
    let chartContainers = "";
    let chartIndex = 0;
    let dataRowsHtml = "";
    let finalResultData = null;
    let autoCompleted = false;
    let pendingCharts = []; // Collect specs for initialization AFTER DOM update

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
          if (data.type === "thought") {
            accumulatedThought += data.content;
            updateAiCard(wrapper, i18n.t("ai_analyzing"), `<div>${i18n.t('analyzing_desc')}</div>`, accumulatedThought);
          } else if (data.type === "result") {
            finalResultData = data.data;
          } else if (data.type === "data") {
            // Received sample data rows
            dataRowsHtml = `
              <details style="margin-top:16px;border:1px solid var(--border);border-radius:6px;overflow:hidden;">
                <summary style="background:#f8f9fa;padding:10px 16px;cursor:pointer;font-size:13px;font-weight:600;color:var(--text-main);">
                  ${i18n.t('raw_data_preview')} (${data.rows.length} ${i18n.t('rows')})
                </summary>
                <div style="padding:0">
                  ${jsonToTable(data.rows)}
                </div>
              </details>
            `;
          } else if (data.type === "chart_spec") {
            const id = `chart_${Date.now()}_${chartIndex++}`;
            chartContainers += `<div id="${id}" style="height:320px;width:100%;margin:16px 0;"></div>`;
            pendingCharts.push({ id, spec: data.data });
          } else if (data.type === "iteration_complete") {
            sessionId = data.data.session_id;
            lastProposalId = data.data.proposal_id; // For skill saving
            if (finalResultData) {
              renderIterationResult(finalResultData, wrapper, accumulatedThought, chartContainers, dataRowsHtml, pendingCharts);
            }
            autoCompleted = true;
            // Auto-refresh session list so current session appears immediately
            refreshSessions();
          } else if (data.type === "error") {
            updateAiCard(wrapper, i18n.t("error_occurred"), `<div style="color: #ef4444">${data.message}</div>`, accumulatedThought);
          }
        } catch (e) {
          console.error("JSON parse error", e, line);
        }
      }
    }

    // In case execution stream was incomplete but we had result data
    if (!autoCompleted && finalResultData) {
      renderIterationResult(finalResultData, wrapper, accumulatedThought, chartContainers, dataRowsHtml, pendingCharts);
    }

  } catch (e) {
    updateAiCard(wrapper, i18n.t("request_failed") || "请求失败", `<div style="color: #ef4444">${e.message}</div>`);
  }
}

document.getElementById("sendBtn").onclick = () => handleSend();

document.getElementById("questionInput").onkeydown = (e) => {
  if (e.key === "Enter") handleSend();
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
      addCard(i18n.t("skill_updated") || "经验已更新", `<div>${i18n.t("success_update") || "成功更新："}<strong>${escapeHtml(data.name || name)}</strong>${desc ? `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">${escapeHtml(desc)}</div>` : ''}</div>`);
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
      addCard(i18n.t("skill_saved") || "经验已沉淀", `<div>${isOverwrite ? (i18n.t('success_update') || '成功更新：') : (i18n.t('success_save') || '成功保存：')}<strong>${data.skill.name}</strong>${data.skill.version ? ` (v${data.skill.version})` : ''}${desc ? `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">${escapeHtml(desc)}</div>` : ''}</div>`);
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
    btn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('uploading') || '上传中...'}`;
    btn.disabled = true;

    const form = new FormData();
    for (let i = 0; i < input.files.length; i++) {
      form.append("files", input.files[i]);
    }

    if (sessionId) form.append("session_id", sessionId);
    const sandboxId = sandboxSelect.value;
    if (sandboxId) form.append("sandbox_id", sandboxId);

    const token = localStorage.getItem("token");
    const headers = {};
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
        addCard(i18n.t("upload_success"), `<div>${i18n.t('filename')}：${f.dataset_name}</div>${f.is_tabular ? `<div>${i18n.t('rows')}：${f.rows}</div><div>${i18n.t('cols')}：${f.columns.join(", ")}</div>` : `<div>${i18n.t('type_doc')}</div>`}<div>${i18n.t('auto_context')}</div>`);
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
const skillEditCancelLink = document.getElementById("skillEditCancelLink");

if (closeSkillModalBtn) {
  closeSkillModalBtn.onclick = () => {
    if (skillModal) skillModal.style.display = "none";
    cancelSkillEdit();
  };
}
if (skillEditCancelLink) {
  skillEditCancelLink.onclick = (e) => {
    e.preventDefault();
    if (skillModal) skillModal.style.display = "none";
    cancelSkillEdit();
  };
}

// ── External DB Connection Modal Logic ──────────────────────────────────────

const dbModal = document.getElementById("dbModal");
const openDbModalBtn = document.getElementById("openDbModalBtn");
const closeDbModalBtn = document.getElementById("closeDbModalBtn");

openDbModalBtn.onclick = () => {
  dbModal.style.display = "flex";
};

closeDbModalBtn.onclick = () => {
  dbModal.style.display = "none";
};

// Close modal when clicking outside
window.onclick = (event) => {
  if (event.target === dbModal) {
    dbModal.style.display = "none";
  }
  if (event.target === uploadModal) {
    uploadModal.style.display = "none";
  }
  if (event.target === skillModal) {
    skillModal.style.display = "none";
    cancelSkillEdit();
  }
};

function getDbFormData() {
  return {
    db_type: document.getElementById("dbTypeInput").value,
    host: document.getElementById("dbHostInput").value.trim() || "localhost",
    port: parseInt(document.getElementById("dbPortInput").value) || null,
    database: document.getElementById("dbNameInput").value.trim(),
    username: document.getElementById("dbUserInput").value.trim(),
    password: document.getElementById("dbPassInput").value
  };
}

document.getElementById("dbTestBtn").onclick = async () => {
  const sandboxId = sandboxSelect.value;
  const dbMsg = document.getElementById("dbMsg");
  if (!sandboxId) {
    dbMsg.innerHTML = `<span style="color:red">${i18n.t('select_sandbox_first')}</span>`;
    return;
  }
  const payload = getDbFormData();
  if (!payload.database) {
    dbMsg.innerHTML = `<span style="color:red">${i18n.t('enter_db_name')}</span>`;
    return;
  }

  dbMsg.innerHTML = `<span style="color:gray"><i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('testing')}</span>`;
  try {
    const res = await api(`/api/sandboxes/${sandboxId}/db-test`, {
      method: "POST",
      body: JSON.stringify(payload)
    });
    if (res.ok) {
      dbMsg.innerHTML = `<span style="color:green"><i class="fa-solid fa-check"></i> ${i18n.t('test_success')}</span>`;
      document.getElementById("dbTestBtn").className = "btn btn-outline";
      document.getElementById("dbRegisterBtn").style.display = "block";
    } else {
      dbMsg.innerHTML = `<span style="color:red"><i class="fa-solid fa-xmark"></i> ${res.error}</span>`;
    }
  } catch (e) {
    dbMsg.innerHTML = `<span style="color:red"><i class="fa-solid fa-xmark"></i> ${e.message}</span>`;
  }
};

document.getElementById("dbRegisterBtn").onclick = async () => {
  const sandboxId = sandboxSelect.value;
  const dbMsg = document.getElementById("dbMsg");
  const tableContainer = document.getElementById("dbTablesContainer");
  const tableList = document.getElementById("dbTablesList");
  const dbTestBtn = document.getElementById("dbTestBtn");
  const dbRegisterBtn = document.getElementById("dbRegisterBtn");

  if (!sandboxId) {
    dbMsg.innerHTML = `<span style="color:red">${i18n.t('select_sandbox_first')}</span>`;
    return;
  }
  const payload = getDbFormData();
  if (!payload.database) {
    dbMsg.innerHTML = `<span style="color:red">${i18n.t('enter_db_name')}</span>`;
    return;
  }

  dbMsg.innerHTML = `<span style="color:gray"><i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('connecting')}</span>`;
  tableContainer.style.display = "none";

  try {
    const res = await api(`/api/sandboxes/${sandboxId}/db-connection`, {
      method: "POST",
      body: JSON.stringify(payload)
    });
    dbMsg.innerHTML = `<span style="color:green"><i class="fa-solid fa-check"></i> ${i18n.t('connect_success_select')}</span>`;
    document.getElementById("dbPassInput").value = "";

    // Disable inputs so user just checks tables
    document.getElementById("dbTypeInput").disabled = true;
    document.getElementById("dbHostInput").disabled = true;
    document.getElementById("dbPortInput").disabled = true;
    document.getElementById("dbNameInput").disabled = true;
    document.getElementById("dbUserInput").disabled = true;
    document.getElementById("dbPassInput").disabled = true;

    // Switch buttons
    dbTestBtn.style.display = "none";
    dbRegisterBtn.style.display = "none";

    // Render the table selection checkboxes
    if (res.tables && Array.isArray(res.tables) && res.tables.length > 0) {
      dbTablesList.innerHTML = "";
      res.tables.forEach(table => {
        const div = document.createElement("div");
        div.style.marginBottom = "8px";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = table;
        cb.id = `chk_${table}`;
        cb.style.marginRight = "8px";
        cb.className = "db-table-checkbox";

        // Enforce MAX 5 check limit on UI side
        cb.onchange = () => {
          const checkedCount = document.querySelectorAll(".db-table-checkbox:checked").length;
          if (checkedCount > 5) { // MAX_SELECTED_TABLES 
            cb.checked = false;
            alert(i18n.t('max_5_tables'));
          }
        };

        const label = document.createElement("label");
        label.htmlFor = `chk_${table}`;
        label.textContent = table;
        label.style.cursor = "pointer";

        div.appendChild(cb);
        div.appendChild(label);
        dbTablesList.appendChild(div);
      });
      tableContainer.style.display = "block";
    }
  } catch (e) {
    dbMsg.innerHTML = `<span style="color:red"><i class="fa-solid fa-xmark"></i> ${e.message}</span>`;
  }
};

document.getElementById("dbSaveTablesBtn").onclick = async () => {
  const sandboxId = sandboxSelect.value;
  if (!sandboxId) return;

  const checkedBoxes = Array.from(document.querySelectorAll(".db-table-checkbox:checked"));
  const selectedTables = checkedBoxes.map(cb => cb.value);

  if (selectedTables.length === 0) {
    alert(i18n.t('select_one_table'));
    return;
  }

  const originalBtnText = document.getElementById("dbSaveTablesBtn").innerHTML;
  document.getElementById("dbSaveTablesBtn").innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${i18n.t('processing')}`;
  document.getElementById("dbSaveTablesBtn").disabled = true;

  try {
    const res = await api(`/api/sandboxes/${sandboxId}/db-tables`, {
      method: "POST",
      body: JSON.stringify({ tables: selectedTables })
    });
    // Refetch the sandbox tables to update the left sidebar immediately
    await refreshProfile();

    // Add an AI notification card to prompt the user
    addCard(i18n.t("connect_db_success"), `<div style="color: #10b981; font-weight: 500;"><i class="fa-solid fa-circle-check"></i> ${i18n.t('connect_db_success_msg', { tables: selectedTables.join(", ") })}</div><div style="margin-top: 8px; font-size: 14px; color: #374151;">${i18n.t('connect_db_hint')}</div>`);

    // Hide the table panel and modal after success
    document.getElementById("dbTablesContainer").style.display = "none";
    dbModal.style.display = "none";

    // Reset buttons
    document.getElementById("dbTestBtn").style.display = "block";
    document.getElementById("dbRegisterBtn").style.display = "none";
    document.getElementById("dbMsg").innerHTML = "";

  } catch (e) {
    alert(i18n.t('save_tables_failed') + ": " + e.message);
  } finally {
    document.getElementById("dbSaveTablesBtn").innerHTML = originalBtnText;
    document.getElementById("dbSaveTablesBtn").disabled = false;
  }
};

// Toggle inputs based on DB type (e.g. SQLite doesn't need host/port)
document.getElementById("dbTypeInput").onchange = (e) => {
  const isSqlite = e.target.value === "sqlite";
  document.getElementById("dbHostInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbPortInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbUserInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbPassInput").style.display = isSqlite ? "none" : "";
  document.getElementById("dbNameInput").placeholder = isSqlite ? i18n.t('sqlite_path_placeholder') : "DB Name";
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

// ── Initial data load ──────────────────────────────────────────────────
refreshSkills();
refreshSessions();
refreshProfile();

// ── Event listeners for Skill Proposal ──────────────────────────────────
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
    if (distilledData.tags && tagsInput) tagsInput.value = distilledData.tags.join(", ");
    if (distilledData.knowledge && knowledgeInput) knowledgeInput.value = (distilledData.knowledge || []).join("\n");

    // Clear skill id to ensure create mode
    currentEditingSkillId = "";
    
    // Load existing skills into overwrite dropdown
    const overwriteGroup = document.getElementById("overwriteSkillGroup");
    const overwriteSelect = document.getElementById("overwriteSkillSelect");
    if (overwriteGroup && overwriteSelect) {
      overwriteGroup.style.display = "block";
      api("/api/skills").then(res => {
          overwriteSelect.innerHTML = `<option value="">🆕 ${i18n.t('create_new_skill') || '创建全新经验'}</option>`;
          res.skills.forEach(sk => {
              overwriteSelect.innerHTML += `<option value="${sk.skill_id}">🔄 ${i18n.t('overwrite_skill') || '覆盖'}: ${escapeHtml(sk.name)}${sk.version ? ` (v${sk.version})` : ''}</option>`;
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
