import json

import app.agent as agent_module


def _bundle_response(html_document: str, title: str = "报告标题", summary: str = "报告摘要") -> str:
    return json.dumps(
        {
            "title": title,
            "summary": summary,
            "html_document": html_document,
            "chart_bindings": [
                {
                    "chart_id": "chart_1",
                    "option": {"xAxis": {}, "yAxis": {}, "series": []},
                    "height": 320,
                }
            ],
        },
        ensure_ascii=False,
    )


def _polished_html(title: str = "报告", body: str = '<div data-chart-id="chart_1"></div>') -> str:
    return (
        "<!doctype html><html><head><style>"
        "body{margin:0;background:linear-gradient(180deg,#f8fbff,#edf2f7);font-family:Arial;color:#111827}"
        ".shell{display:grid;grid-template-columns:1fr;gap:18px;max-width:1120px;margin:auto;padding:28px}"
        ".hero{display:flex;position:relative;border-radius:24px;background:linear-gradient(135deg,#0f172a,#2563eb);box-shadow:0 24px 70px rgba(15,23,42,.2);padding:30px;color:#fff}"
        ".grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}"
        ".card{border-radius:18px;background:#fff;box-shadow:0 18px 48px rgba(15,23,42,.08);padding:22px}"
        "[data-chart-id]{min-height:320px;border-radius:18px;background:#fff}"
        "</style></head><body><main class=\"shell\">"
        f"<section class=\"hero\"><h1>{title}</h1></section><section class=\"grid\"><article class=\"card\">{body}</article></section>"
        "</main></body></html>"
    )


def _run_bundle_generation():
    return agent_module.generate_auto_analysis_report_bundle(
        message="测试自动分析",
        session_history=[],
        business_knowledge=[],
        session_patches=[],
        loop_rounds=[],
        chart_specs=[{"xAxis": {}, "yAxis": {}, "series": []}],
        final_result_rows=[],
        stop_reason="model_stopped_using_tools",
        rounds_completed=1,
        provider="openai",
    )


def test_report_bundle_accepts_standalone_ai_html_and_injects_missing_chart_placeholders(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        yield _bundle_response(_polished_html("报告", "正文"))

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    assert call_state["count"] == 1
    assert "<html" in str(bundle["html_document"]).lower()
    assert "data-chart-id=\"chart_1\"" in str(bundle["html_document"])
    assert "class=\"hero\"" in str(bundle["html_document"])


def test_report_bundle_repairs_html_fragment_before_minimal_fallback(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        if "Return valid JSON only" in user_prompt:
            yield _bundle_response("<div><h1>报告片段</h1></div>")
        else:
            assert "Structured report context" in user_prompt
            yield _polished_html("修复后报告")

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    assert call_state["count"] == 2
    html_text = str(bundle["html_document"]).lower()
    assert "<html" in html_text
    assert "修复后报告" in bundle["html_document"]
    assert "data-chart-id=\"chart_1\"" in html_text


def test_auto_report_prompt_does_not_force_fixed_sections(monkeypatch):
    captured = {}

    def fake_openai(system_prompt, user_prompt, model, config):
        captured["system_prompt"] = system_prompt
        captured["user_prompt"] = user_prompt
        yield "## 自选洞察\n- done"

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    report = agent_module.generate_auto_analysis_report(
        message="测试自动分析",
        loop_rounds=[],
        business_knowledge=[],
        stop_reason="model_stopped_using_tools",
        provider="openai",
    )

    assert "自选洞察" in report
    assert "exactly these sections" not in captured["user_prompt"]
    assert "Executive Summary" not in captured["user_prompt"]
    assert "choose the sections" in captured["user_prompt"]


def test_report_bundle_prompt_lets_html_follow_iteration_results(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 自选洞察\n- done",
    )

    captured = {}

    def fake_openai(system_prompt, user_prompt, model, config):
        captured["system_prompt"] = system_prompt
        captured["user_prompt"] = user_prompt
        yield _bundle_response(_polished_html("自选报告"))

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()

    assert "自选报告" in bundle["html_document"]
    assert "fixed report template" in captured["system_prompt"]
    assert "Structured iteration results" in captured["user_prompt"]
    assert "Available chart specs JSON" in captured["user_prompt"]
    assert "REQUIRED:" not in captured["user_prompt"]
    assert "cards, metric callouts" not in captured["user_prompt"]


def test_report_bundle_preserves_ai_title_and_summary(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    def fake_openai(system_prompt, user_prompt, model, config):
        yield _bundle_response(
            _polished_html("AI 自选标题"),
            title="AI 自选标题",
            summary="AI 自选摘要",
        )

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()

    assert bundle["title"] == "AI 自选标题"
    assert bundle["summary"] == "AI 自选摘要"


def test_report_bundle_uses_polished_fallback_after_empty_html_repairs_fail(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        if "Return valid JSON only" in user_prompt:
            yield _bundle_response("")
        else:
            yield ""

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    html_text = str(bundle["html_document"])

    assert call_state["count"] == 3
    assert "<!doctype html>" in html_text.lower()
    assert "report-shell" in html_text
    assert "report-hero" in html_text
    assert "linear-gradient" in html_text
    assert "box-shadow" in html_text


def test_report_bundle_redesigns_plain_standalone_html(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        if "Return valid JSON only" in user_prompt:
            yield _bundle_response("<!doctype html><html><head><style>body{font-family:Arial}</style></head><body><h1>分析报告</h1><hr><p>plain</p></body></html>")
        else:
            assert "plain white document" in user_prompt
            yield _polished_html("重新设计报告")

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()

    assert call_state["count"] == 2
    assert "重新设计报告" in bundle["html_document"]
    assert "class=\"hero\"" in bundle["html_document"]


def test_report_bundle_prompt_includes_iteration_warnings(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 自选洞察\n- done",
    )

    captured = {}

    def fake_openai(system_prompt, user_prompt, model, config):
        captured["user_prompt"] = user_prompt
        yield _bundle_response(_polished_html("有告警报告"))

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = agent_module.generate_auto_analysis_report_bundle(
        message="测试自动分析",
        session_history=[],
        business_knowledge=[],
        session_patches=[],
        loop_rounds=[
            {
                "round": 1,
                "prompt": "warning round",
                "result": {"steps": [], "conclusions": [], "action_items": []},
                "execution": {
                    "warning": "字段缺失",
                    "step_results": [{"warning": "字段缺失"}, {"error": "步骤失败"}],
                },
            }
        ],
        chart_specs=[{"xAxis": {}, "yAxis": {}, "series": []}],
        final_result_rows=[],
        stop_reason="model_stopped_using_tools",
        rounds_completed=1,
        provider="openai",
    )

    assert "有告警报告" in bundle["html_document"]
    assert "字段缺失" in captured["user_prompt"]
    assert "步骤失败" in captured["user_prompt"]


def test_build_polished_report_sections_skips_placeholder_only_sections():
    markdown = (
        "# 票价影响因素分析报告\n"
        "-\n\n"
        "## Executive Summary\n"
        "核心发现：票价波动主要来自维度内差异。\n\n"
        "## Key Findings\n"
        "-\n"
    )
    _, summary, rendered = agent_module._build_polished_report_sections(markdown, "自动分析报告")
    assert rendered.count('class="report-section"') == 1
    assert "Executive Summary" in rendered
    assert "Key Findings" not in rendered
    assert not summary.endswith("-")
