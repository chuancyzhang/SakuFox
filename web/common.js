const API_BASE = "";

const ZH_FALLBACK = {
  app_title: "SakuFox \ud83e\udd8a - \u654f\u6377\u667a\u80fd\u6570\u636e\u5206\u6790\u5e73\u53f0",
  nav_brand: "SakuFox \ud83e\udd8a",
  nav_analysis: "\u6570\u636e\u5206\u6790",
  nav_knowledge: "\u77e5\u8bc6\u5e93\u914d\u7f6e",
  data_space: "\u6570\u636e\u5206\u6790\u7a7a\u95f4",
  new_sandbox: "\u65b0\u5efa\u7a7a\u95f4",
  rename: "\u91cd\u547d\u540d",
  delete: "\u5220\u9664",
  select_sandbox: "\u9009\u62e9\u5de5\u4f5c\u7a7a\u95f4...",
  no_sandbox: "\u6682\u65e0\u53ef\u7528\u6c99\u76d2",
  data_models: "\u6570\u636e\u6a21\u578b",
  no_tables: "\u6682\u65e0\u6570\u636e\u8868",
  datasource_mgmt: "\u6570\u636e\u6e90\u7ba1\u7406",
  connect_db: "\u5173\u8054\u5916\u90e8\u6570\u636e\u5e93",
  upload_files: "\u4e0a\u4f20\u672c\u5730\u6587\u4ef6",
  welcome_title: "\u5f00\u59cb\u6570\u636e\u63a2\u7d22",
  welcome_desc: "\u8f93\u5165\u5206\u6790\u9700\u6c42\uff0cAI \u5c06\u81ea\u52a8\u53d6\u6570\u3001\u5206\u6790\uff0c\u5e76\u8f93\u51fa\u7ed3\u8bba\u3002<br />\u4f60\u4e5f\u53ef\u4ee5\u8865\u5145\u4e1a\u52a1\u77e5\u8bc6\u6216\u6cbf\u7740\u731c\u60f3\u7ee7\u7eed\u8fed\u4ee3\uff0c\u8ba9\u5206\u6790\u9010\u6b65\u6df1\u5165\u3002",
  input_placeholder: "\u8f93\u5165\u5206\u6790\u9700\u6c42\uff0c\u6216\u8865\u5145\u4e1a\u52a1\u77e5\u8bc6\uff08\u524d\u7f00\uff1a\u77e5\u8bc6:\uff09",
  send: "\u53d1\u9001",
  save_skill: "\u6c89\u6dc0\u7ecf\u9a8c",
  extract_skill: "\u4ece\u5bf9\u8bdd\u63d0\u70bc\u7ecf\u9a8c",
  manage_skill_mounts: "\u7ba1\u7406\u6c99\u76d2\u7ecf\u9a8c\u6302\u8f7d",
  saved_skills: "\u5df2\u4fdd\u5b58\u7ecf\u9a8c",
  no_skills: "\u6682\u65e0\u6c89\u6dc0\u7ecf\u9a8c",
  mounted_skill: "\u5df2\u6302\u8f7d",
  mounted_skill_count: "\u5f53\u524d\u5de5\u4f5c\u7a7a\u95f4\u5df2\u6302\u8f7d {count} \u6761\u7ecf\u9a8c",
  skill_mount_title: "\u6302\u8f7d\u7ecf\u9a8c\u5230\u5f53\u524d\u5de5\u4f5c\u7a7a\u95f4",
  save_skill_mounts: "\u4fdd\u5b58\u6302\u8f7d",
  current_sandbox_label: "\u5f53\u524d\u5de5\u4f5c\u7a7a\u95f4",
  new_session: "\u5f00\u542f\u65b0\u5bf9\u8bdd",
  history: "\u5386\u53f2\u5bf9\u8bdd",
  no_history: "\u6682\u65e0\u5386\u53f2\u5bf9\u8bdd",
  modal_db_title: "\u5173\u8054\u5916\u90e8\u6570\u636e\u5e93",
  modal_upload_title: "\u4e0a\u4f20\u672c\u5730\u6587\u4ef6",
  modal_skill_title: "\u7ecf\u9a8c\u8be6\u60c5",
  overwrite_skill: "\u8986\u76d6\u5df2\u6709\u7ecf\u9a8c",
  create_new_skill: "\u521b\u5efa\u5168\u65b0\u7ecf\u9a8c",
  skill_name: "\u7ecf\u9a8c\u540d\u79f0\uff08\u5fc5\u586b\uff09",
  skill_desc: "\u529f\u80fd\u63cf\u8ff0",
  skill_tags: "\u6807\u7b7e\uff08\u9017\u53f7\u5206\u9694\uff09",
  skill_knowledge: "\u4e1a\u52a1\u77e5\u8bc6\uff08\u81ea\u52a8\u63d0\u53d6\uff0c\u53ef\u7f16\u8f91\uff09",
  save: "\u4fdd\u5b58\u7ecf\u9a8c",
  cancel: "\u53d6\u6d88",
  upload_label: "\u9009\u62e9\u6587\u4ef6",
  upload_help: "\u652f\u6301\u683c\u5f0f\uff1a.csv\u3001.xls(x)\u3001.txt\u3001.json\u3001.md\u3001.log<br/>\u6587\u672c\u4f1a\u4f5c\u4e3a\u80cc\u666f\u77e5\u8bc6\u63d0\u4f9b\u7ed9 AI\uff0c\u8868\u683c\u6570\u636e\u53ef\u76f4\u63a5\u53c2\u4e0e\u5206\u6790\u3002",
  start_upload: "\u5f00\u59cb\u4e0a\u4f20\u5e76\u5e94\u7528",
  test_conn: "\u6d4b\u8bd5\u5e76\u8fde\u63a5",
  get_tables: "\u83b7\u53d6\u6570\u636e\u8868",
  choose_tables: "\u8bf7\u9009\u62e9\u8981\u52a0\u5165\u6c99\u76d2\u7684\u6570\u636e\u8868\uff08\u6700\u591a 5 \u5f20\uff09",
  confirm_close: "\u786e\u8ba4\u5e76\u5173\u95ed",
  analysis_conclusion: "\u5206\u6790\u7ed3\u8bba",
  iter_conclusion: "\u8fed\u4ee3\u5206\u6790\u7ed3\u679c",
  main_conclusion: "\u4e3b\u8981\u7ed3\u8bba",
  action_suggestions: "\u843d\u5730\u52a8\u4f5c\u5efa\u8bae",
  click_hypotheses: "\u70b9\u51fb\u731c\u60f3\u7ee7\u7eed\u6df1\u6316\uff1a",
  raw_data_preview: "\u67e5\u770b\u539f\u59cb\u6570\u636e\u9884\u89c8",
  confidence: "\u7f6e\u4fe1\u5ea6",
  confidence_high: "\u9ad8",
  confidence_med: "\u4e2d",
  confidence_low: "\u4f4e",
  step: "\u6b65\u9aa4",
  hypotheses: "\u5f85\u9a8c\u8bc1\u731c\u60f3",
  iterations_count: "{count} \u8f6e",
  loading_history: "\u6b63\u5728\u52a0\u8f7d\u5386\u53f2\u5bf9\u8bdd...",
  load_failed: "\u52a0\u8f7d\u5931\u8d25",
  empty_chat: "\u7a7a\u5bf9\u8bdd",
  no_record: "\u5f53\u524d\u5bf9\u8bdd\u6682\u65e0\u8bb0\u5f55\u3002",
  ai_thinking: "AI \u601d\u8003\u4e2d...",
  ai_analyzing: "AI \u5206\u6790\u4e2d...",
  thinking_desc: "\u6b63\u5728\u89c4\u5212\u5206\u6790\u8def\u5f84...",
  analyzing_desc: "\u6b63\u5728\u6267\u884c\u67e5\u8be2\u5e76\u6574\u7406\u7ed3\u679c...",
  error_occurred: "\u5206\u6790\u51fa\u9519",
  validate_hypothesis: "\u9a8c\u8bc1\u731c\u60f3",
  using_tool: "\u4f7f\u7528\u5de5\u5177",
  none: "\u65e0",
  op_failed: "\u64cd\u4f5c\u5931\u8d25",
  feedback_recorded: "\u53cd\u9988\u5df2\u8bb0\u5f55",
  knowledge_saved: "\u4e1a\u52a1\u77e5\u8bc6\u5df2\u4fdd\u5b58",
  ai_will_ref: "AI \u4f1a\u5728\u540e\u7eed\u8fed\u4ee3\u4e2d\u81ea\u52a8\u53c2\u8003\u8fd9\u4e9b\u4fe1\u606f\u3002",
  skill_updated: "\u7ecf\u9a8c\u5df2\u66f4\u65b0",
  skill_saved: "\u7ecf\u9a8c\u5df2\u4fdd\u5b58",
  success_update: "\u66f4\u65b0\u6210\u529f\uff1a",
  success_save: "\u4fdd\u5b58\u6210\u529f\uff1a",
  select_files_first: "\u8bf7\u5148\u9009\u62e9\u6587\u4ef6",
  no_tags: "\u65e0\u6807\u7b7e",
  knowledge_count: "{count} \u6761\u77e5\u8bc6",
  skill_mount_saved: "\u7ecf\u9a8c\u6302\u8f7d\u5df2\u66f4\u65b0",
  skill_mount_saved_desc: "\u5f53\u524d\u6c99\u76d2\u5df2\u6302\u8f7d {count} \u6761\u7ecf\u9a8c",
  rows: "\u884c",
  cols: "\u5217",
  columns: "\u5217",
  no_data: "\u6682\u65e0\u6570\u636e",
  collapse: "\u6298\u53e0",
  request_failed: "\u8bf7\u6c42\u5931\u8d25"
};

const i18n = {
  lang: localStorage.getItem("lang") || "zh",
  translations: {},
  async init() {
    try {
      const res = await fetch(`/web/lang/${this.lang}.json`);
      this.translations = await res.json();
      if (this.lang === "zh") {
        this.translations = { ...ZH_FALLBACK, ...this.translations };
      }
      this.translatePage();
    } catch (e) {
      console.error("i18n init failed", e);
      if (this.lang === "zh") {
        this.translations = { ...ZH_FALLBACK };
        this.translatePage();
      }
    }
  },
  t(key, params = {}) {
    let text = this.translations[key] || key;
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(`{${k}}`, v);
    }
    return text;
  },
  translatePage() {
    document.querySelectorAll("[data-i18n], [data-i18n-title]").forEach((el) => {
      const key = el.getAttribute("data-i18n");
      const titleKey = el.getAttribute("data-i18n-title");

      if (key) {
        const text = this.t(key);
        if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
          el.placeholder = text;
        } else if (text && text !== key) {
          const icon = el.querySelector(":scope > i[class*='fa-']");
          if (icon) {
            el.innerHTML = icon.outerHTML + " " + text;
          } else {
            el.textContent = text;
          }
        }
      }

      if (titleKey) {
        const titleText = this.t(titleKey);
        if (titleText && titleText !== titleKey) {
          el.title = titleText;
        }
      }
    });
  },
  toggle() {
    this.lang = this.lang === "zh" ? "en" : "zh";
    localStorage.setItem("lang", this.lang);
    window.location.reload();
  }
};

async function api(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    "X-Language": i18n.lang,
    ...(options.headers || {})
  };
  headers.Authorization = `Bearer mock_token`;

  const res = await fetch(API_BASE + path, { ...options, headers });

  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || i18n.t("request_failed") || "\u8bf7\u6c42\u5931\u8d25");
  }

  return res.json();
}
