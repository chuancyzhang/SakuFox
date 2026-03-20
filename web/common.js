const API_BASE = "";

const i18n = {
    lang: localStorage.getItem("lang") || "zh",
    translations: {},
    async init() {
        try {
            const res = await fetch(`/web/lang/${this.lang}.json`);
            this.translations = await res.json();
            this.translatePage();
        } catch (e) {
            console.error("i18n init failed", e);
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
        document.querySelectorAll("[data-i18n], [data-i18n-title]").forEach(el => {
            const key = el.getAttribute("data-i18n");
            const titleKey = el.getAttribute("data-i18n-title");
            
            if (key) {
                const text = this.t(key);
                if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
                    el.placeholder = text;
                } else {
                    // Try to preserve internal <i> icon if it's the first child
                    const icon = el.querySelector(":scope > i.fa-solid, :scope > i.fa-brands");
                    if (icon) {
                        el.innerHTML = icon.outerHTML + " " + text;
                    } else {
                        el.textContent = text;
                    }
                }
            }

            if (titleKey) {
                el.title = this.t(titleKey);
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
    throw new Error(err.detail || i18n.t("request_failed") || "请求失败");
  }
  
  return res.json();
}
