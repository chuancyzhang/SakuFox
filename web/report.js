(function () {
  const TEXTS = {
    zh: {
      pageTitle: "\u5206\u6790\u62a5\u544a",
      back: "\u8fd4\u56de\u5bf9\u8bdd",
      exportPdf: "\u5bfc\u51fa PDF",
      loading: "\u6b63\u5728\u52a0\u8f7d\u62a5\u544a...",
      missingIteration: "\u7f3a\u5c11 iteration_id",
      loadFailed: "\u52a0\u8f7d\u62a5\u544a\u5931\u8d25",
      emptyReport: "\u62a5\u544a\u5185\u5bb9\u4e3a\u7a7a",
      renderFailed: "\u56fe\u8868\u6e32\u67d3\u5931\u8d25",
      iteration: "\u8fed\u4ee3",
    },
    en: {
      pageTitle: "Analysis Report",
      back: "Back To Chat",
      exportPdf: "Export PDF",
      loading: "Loading report...",
      missingIteration: "Missing iteration_id",
      loadFailed: "Failed to load report",
      emptyReport: "Report content is empty",
      renderFailed: "Failed to render charts",
      iteration: "Iteration",
    },
  };

  function qs(id) {
    return document.getElementById(id);
  }

  function getToken() {
    return localStorage.getItem("token");
  }

  function getQueryParam(name) {
    const params = new URLSearchParams(window.location.search);
    return params.get(name) || "";
  }

  function getLang() {
    const fromQuery = getQueryParam("lang");
    if (fromQuery === "en" || fromQuery === "zh") return fromQuery;
    const fromStorage = localStorage.getItem("lang");
    return fromStorage === "en" ? "en" : "zh";
  }

  function t(lang, key) {
    const pack = TEXTS[lang] || TEXTS.zh;
    return pack[key] || TEXTS.en[key] || key;
  }

  function showError(message) {
    const loading = qs("loading");
    const err = qs("error");
    if (loading) loading.style.display = "none";
    if (err) {
      err.style.display = "block";
      err.textContent = message;
    }
  }

  function extractHtmlFromJsonLike(rawText) {
    const text = String(rawText || "").trim();
    if (!text) return "";
    const stripFence = text.replace(/^```(?:json|html)?\s*/i, "").replace(/\s*```$/i, "").trim();
    const extractStandaloneHtml = (candidate) => {
      const match = String(candidate || "").match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
      return match ? match[0].trim() : "";
    };

    const tryParse = (candidate) => {
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

    let html = tryParse(stripFence);
    if (html) return html;

    const firstBrace = stripFence.indexOf("{");
    const lastBrace = stripFence.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      html = tryParse(stripFence.slice(firstBrace, lastBrace + 1));
      if (html) return html;
    }

    const htmlField = stripFence.match(/"html_document"\s*:\s*"([\s\S]*?)"\s*(?:,\s*"chart_bindings"|,\s*"summary"|,\s*"title"|,\s*"legacy_markdown"|\})/i);
    if (htmlField && htmlField[1]) {
      try {
        const rawHtml = JSON.parse(`"${htmlField[1]}"`).trim();
        const html = extractStandaloneHtml(rawHtml) || rawHtml;
        return hasReportRenderArtifacts(html) ? "" : html;
      } catch (_) {
        const rawHtml = htmlField[1].trim();
        const html = extractStandaloneHtml(rawHtml) || rawHtml;
        return hasReportRenderArtifacts(html) ? "" : html;
      }
    }

    const htmlBlock = stripFence.match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
    if (htmlBlock) return htmlBlock[0].trim();

    return "";
  }

  function hasReportRenderArtifacts(htmlText) {
    const raw = String(htmlText || "");
    if (!raw.trim()) return true;
    const visible = raw
      .replace(/<script[\s\S]*?<\/script>|<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, "\n");
    return /\\u[0-9a-fA-F]{4}|\\[ntr]|"html_document"\s*:|"chart_bindings"\s*:|\{\s*"title"\s*:|�|(?:Ã|Â|å|æ|ç|è|é|ä){2,}/i.test(visible)
      || /&lt;\s*(?:!doctype|\/?html|\/?body|\/?div|\/?table)/i.test(raw);
  }

  function escapeText(text) {
    return String(text || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function normalizeHtmlDocument(rawText) {
    const text = String(rawText || "").trim();
    if (!text) return "";

    const extracted = extractHtmlFromJsonLike(text);
    if (extracted && !hasReportRenderArtifacts(extracted)) return extracted;

    const htmlBlock = text.match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
    if (htmlBlock && !hasReportRenderArtifacts(htmlBlock[0])) return htmlBlock[0].trim();

    if (text.startsWith("{") || text.startsWith("[")) return "";
    if (/"html_document"\s*:|"chart_bindings"\s*:|\\u[0-9a-fA-F]{4}|&lt;\s*(?:!doctype|\/?html)/i.test(text)) return "";

    return `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Report</title><style>body{font-family:Arial,sans-serif;margin:24px;color:#111827;background:#fff;}pre{white-space:pre-wrap;line-height:1.6;}</style></head><body><pre>${escapeText(text)}</pre></body></html>`;
  }

  async function fetchReport(iterationId, lang) {
    const headers = { "Content-Type": "application/json", "X-Language": lang };
    const token = getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(`/api/reports/iterations/${encodeURIComponent(iterationId)}`, {
      method: "GET",
      headers,
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error(`${t(lang, "loadFailed")}: ${response.status}`);
    }
    return response.json();
  }

  function cloneChartOption(option) {
    if (!option || typeof option !== "object") return {};
    try {
      return structuredClone(option);
    } catch (_) {
      try {
        return JSON.parse(JSON.stringify(option));
      } catch (_) {
        return { ...option };
      }
    }
  }

  function parseCssColor(value) {
    const text = String(value || "").trim();
    const match = text.match(/^rgba?\(([^)]+)\)$/i);
    if (!match) return null;
    const parts = match[1].split(",").map((item) => parseFloat(item.trim()));
    if (parts.length < 3 || parts.some((item, index) => index < 3 && Number.isNaN(item))) return null;
    const alpha = parts.length >= 4 && !Number.isNaN(parts[3]) ? parts[3] : 1;
    if (alpha <= 0.05) return null;
    return { r: parts[0], g: parts[1], b: parts[2] };
  }

  function isDarkChartHost(host) {
    let node = host;
    const view = host?.ownerDocument?.defaultView || window;
    while (node && node.nodeType === 1) {
      const color = parseCssColor(view.getComputedStyle(node).backgroundColor);
      if (color) {
        const luminance = (0.2126 * color.r + 0.7152 * color.g + 0.0722 * color.b) / 255;
        return luminance < 0.45;
      }
      node = node.parentElement;
    }
    return false;
  }

  function asArray(value) {
    if (!value) return [];
    return Array.isArray(value) ? value : [value];
  }

  function prepareChartOption(option, darkHost) {
    const patched = cloneChartOption(option);
    const textColor = darkHost ? "#e5eefb" : "#243042";
    const mutedColor = darkHost ? "#9fb0c8" : "#64748b";
    const gridColor = darkHost ? "rgba(229,238,251,.18)" : "rgba(100,116,139,.18)";
    patched.backgroundColor = patched.backgroundColor || "transparent";
    patched.textStyle = { ...(patched.textStyle || {}), color: patched.textStyle?.color || textColor };
    patched.color = patched.color || ["#60a5fa", "#34d399", "#fbbf24", "#f472b6", "#a78bfa", "#22d3ee", "#fb7185"];
    asArray(patched.title).forEach((item) => {
      if (!item || typeof item !== "object") return;
      item.textStyle = { ...(item.textStyle || {}), color: item.textStyle?.color || textColor };
      item.subtextStyle = { ...(item.subtextStyle || {}), color: item.subtextStyle?.color || mutedColor };
    });
    asArray(patched.legend).forEach((item) => {
      if (!item || typeof item !== "object") return;
      item.textStyle = { ...(item.textStyle || {}), color: item.textStyle?.color || textColor };
    });
    ["xAxis", "yAxis", "radiusAxis", "angleAxis"].forEach((key) => {
      asArray(patched[key]).forEach((axis) => {
        if (!axis || typeof axis !== "object") return;
        axis.axisLabel = { ...(axis.axisLabel || {}), color: axis.axisLabel?.color || mutedColor };
        axis.nameTextStyle = { ...(axis.nameTextStyle || {}), color: axis.nameTextStyle?.color || mutedColor };
        axis.axisLine = {
          ...(axis.axisLine || {}),
          lineStyle: { ...(axis.axisLine?.lineStyle || {}), color: axis.axisLine?.lineStyle?.color || gridColor },
        };
        axis.splitLine = {
          ...(axis.splitLine || {}),
          lineStyle: { ...(axis.splitLine?.lineStyle || {}), color: axis.splitLine?.lineStyle?.color || gridColor },
        };
      });
    });
    asArray(patched.series).forEach((series) => {
      if (!series || typeof series !== "object") return;
      series.label = { ...(series.label || {}), color: series.label?.color || textColor };
    });
    return patched;
  }

  function makeChartMountVisible(host, mount, height) {
    host.style.setProperty("display", "block", "important");
    host.style.setProperty("position", "relative", "important");
    host.style.setProperty("width", "100%", "important");
    host.style.setProperty("min-height", `${height}px`, "important");
    host.style.setProperty("overflow", "visible", "important");
    mount.style.setProperty("display", "block", "important");
    mount.style.setProperty("position", "relative", "important");
    mount.style.setProperty("width", "100%", "important");
    mount.style.setProperty("height", `${height}px`, "important");
    mount.style.setProperty("min-height", `${height}px`, "important");
    mount.style.setProperty("opacity", "1", "important");
    mount.style.setProperty("visibility", "visible", "important");
  }

  function revealRenderedChart(mount, chart) {
    const reveal = () => {
      mount.querySelectorAll("canvas,svg").forEach((node) => {
        node.style.setProperty("display", "block", "important");
        node.style.setProperty("opacity", "1", "important");
        node.style.setProperty("visibility", "visible", "important");
      });
      try {
        chart.resize();
      } catch (_) {
        // no-op
      }
    };
    reveal();
    setTimeout(reveal, 80);
    setTimeout(reveal, 300);
  }

  function mountCharts(iframeDoc, bindings, lang) {
    if (!window.echarts || !Array.isArray(bindings)) return;
    let chartSection = null;
    const ensureSection = () => {
      if (chartSection) return chartSection;
      chartSection = iframeDoc.createElement("section");
      chartSection.style.marginTop = "22px";
      const heading = iframeDoc.createElement("h2");
      heading.textContent = lang === "en" ? "Charts" : "\u56fe\u8868";
      heading.style.margin = "0 0 10px";
      chartSection.appendChild(heading);
      const root = iframeDoc.body || iframeDoc.documentElement;
      if (root) root.appendChild(chartSection);
      return chartSection;
    };
    bindings.forEach((binding) => {
      if (!binding || typeof binding !== "object") return;
      const chartId = String(binding.chart_id || "").trim();
      const option = binding.option;
      if (!chartId || !option || typeof option !== "object") return;
      let host = iframeDoc.querySelector(`[data-chart-id="${chartId}"]`);
      if (!host) {
        const section = ensureSection();
        const block = iframeDoc.createElement("section");
        block.style.marginTop = "14px";
        const title = iframeDoc.createElement("h3");
        title.textContent = `${lang === "en" ? "Chart" : "\u56fe\u8868"}: ${chartId}`;
        title.style.margin = "0 0 8px";
        host = iframeDoc.createElement("div");
        host.setAttribute("data-chart-id", chartId);
        block.appendChild(title);
        block.appendChild(host);
        section.appendChild(block);
      }
      const height = Math.max(200, Math.min(1200, parseInt(binding.height || 360, 10) || 360));
      host.innerHTML = "";
      const mount = iframeDoc.createElement("div");
      makeChartMountVisible(host, mount, height);
      host.appendChild(mount);
      const chart = echarts.init(mount);
      chart.setOption(prepareChartOption(option, isDarkChartHost(host)), true);
      revealRenderedChart(mount, chart);
    });
  }

  function syncFrameHeight(frame) {
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

  function printFrame(frame) {
    if (!frame || !frame.contentWindow) return;
    try {
      frame.contentWindow.focus();
      frame.contentWindow.print();
    } catch (_) {
      window.print();
    }
  }

  async function init() {
    const frame = qs("reportFrame");
    const loading = qs("loading");
    const btnBack = qs("btnBack");
    const btnPrint = qs("btnPrint");
    const iterationId = getQueryParam("iteration_id");
    const printMode = getQueryParam("print") === "1";
    const lang = getLang();

    document.documentElement.lang = lang === "en" ? "en" : "zh-CN";
    document.title = t(lang, "pageTitle");
    if (loading) loading.textContent = t(lang, "loading");
    if (btnBack) btnBack.innerHTML = '<i class="fa-solid fa-arrow-left"></i> ' + t(lang, "back");
    if (btnPrint) btnPrint.innerHTML = '<i class="fa-solid fa-file-pdf"></i> ' + t(lang, "exportPdf");

    btnBack.onclick = () => {
      if (window.history.length > 1) window.history.back();
      else window.location.href = "/dashboard";
    };
    btnPrint.onclick = () => printFrame(frame);

    if (!iterationId) {
      showError(t(lang, "missingIteration"));
      return;
    }

    try {
      const data = await fetchReport(iterationId, lang);
      const htmlDocument = normalizeHtmlDocument(data.final_report_html || "");
      const chartBindings = data.final_report_chart_bindings || [];
      if (!htmlDocument) {
        showError(t(lang, "emptyReport"));
        return;
      }

      frame.onload = () => {
        try {
          const doc = frame.contentDocument;
          if (!doc) return;
          if (doc.documentElement) doc.documentElement.style.minHeight = "100%";
          if (doc.body) doc.body.style.minHeight = "100%";
          mountCharts(doc, chartBindings, lang);
          syncFrameHeight(frame);
          setTimeout(() => syncFrameHeight(frame), 250);
          setTimeout(() => syncFrameHeight(frame), 900);
          if (printMode) {
            setTimeout(() => printFrame(frame), 300);
          }
        } catch (err) {
          showError(`${t(lang, "renderFailed")}: ${err.message || err}`);
        }
      };
      frame.srcdoc = htmlDocument;
      frame.style.display = "block";
      window.addEventListener("resize", () => syncFrameHeight(frame));
      if (loading) loading.style.display = "none";
    } catch (err) {
      showError(err.message || String(err));
    }
  }

  init();
})();
