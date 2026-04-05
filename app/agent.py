import html
import json

import re

from typing import Generator

from pathlib import Path



import httpx



from app.config import AppConfig, load_config

from app.i18n import t, get_lang





# ── Public API ────────────────────────────────────────────────────────





def run_analysis_iteration(

    message: str,

    sandbox: dict,

    iteration_history: list[dict],

    business_knowledge: list[str],

    provider: str | None = None,

    model: str | None = None,

) -> Generator[dict, None, None]:

    """Single entry-point: AI autonomously picks tools + analyses data + outputs

    conclusions, hypotheses and action items in one shot.



    Yields:

        {"type": "thought", "content": "..."} during streaming

        {"type": "result", "data": { ... }}   final structured result

    """

    config = load_config()

    selected_provider = (provider or config.llm_provider).lower()

    if selected_provider in {"openai", "anthropic"}:

        yield from _run_iteration_by_llm(

            message=message,

            sandbox=sandbox,

            iteration_history=iteration_history,

            business_knowledge=business_knowledge,

            provider=selected_provider,

            model=model,

            config=config,

        )

    else:

        yield from _run_iteration_by_rules(

            message=message,

            sandbox=sandbox,

        )





def generate_data_insight(

    data: list[dict], sql: str, message: str, config: AppConfig

) -> Generator[str, None, None]:

    """Multi-perspective insight generation (kept from original)."""

    if not data:

        msg = t("error_no_data", default="未查询到数据，无法进行分析。")

        yield msg

        return

    preview_data = data[:20]

    count_label = t("label_total", default="共")

    preview_label = t("label_preview", default="条数据，前 20 条预览")

    data_summary = f"{count_label} {len(data)} {preview_label}：{json.dumps(preview_data, ensure_ascii=False)}"



    if config.llm_provider not in {"openai", "anthropic"}:

        report_title = t("report_title", default="### 数据分析报告\n\n")

        count_label = t("label_record_count", default="- 记录数")

        mock_msg = t("mock_msg", default="- 当前为 mock 模式，建议切换到 LLM 获取更深层商业洞察。")

        yield report_title

        yield f"{count_label}：{len(data)}\n"

        yield f"- SQL：`{sql}`\n"

        yield f"{mock_msg}\n"

        return

    question_label = t("label_user_question", default="用户问题")

    sql_label = t("label_executed_sql", default="执行 SQL")

    data_label = t("label_data_results", default="数据结果")

    instruction = t("instruction_no_code", default="请输出面向业务负责人的分析结论，不要输出任何代码。")

    user_prompt = (

        f"{question_label}: {message}\n"

        f"{sql_label}: {sql}\n"

        f"{data_label}: {data_summary}\n"

        f"{instruction}"

    )

    agents: list[tuple[str, str]] = [

        (t("insight_title_metrics"), config.insight_prompt_metrics),

        (t("insight_title_anomaly"), config.insight_prompt_anomaly),

        (t("insight_title_actions"), config.insight_prompt_actions),

    ]



    for title, system_prompt in agents:

        yield f"\n\n### {title}\n\n"

        if config.llm_provider == "openai":

            chunks = _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=None, config=config)

        else:

            chunks = _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=None, config=config)

        for chunk in chunks:

            yield chunk





# ── LLM iteration implementation ─────────────────────────────────────





def _build_iteration_user_prompt(

    message: str,

    sandbox: dict,

    iteration_history: list[dict],

    business_knowledge: list[str],

) -> str:

    """Build rich user prompt with context from past iterations and business knowledge."""

    parts: list[str] = []



    # Business knowledge accumulated from user

    is_en = get_lang() == "en"



    if business_knowledge:

        title = t("title_business_knowledge", default="【已沉淀的业务知识】")

        parts.append(title)

        for i, bk in enumerate(business_knowledge, 1):

            parts.append(f"{i}. {bk}")

        parts.append("")



    # Past iteration summaries (compact)

    if iteration_history:

        title = t("title_historic_iterations", default="【历史迭代摘要】")

        parts.append(title)

        for it in iteration_history[-5:]:  # last 5 iterations for context window

            iter_label = t("label_iteration", default="迭代")

            parts.append(f"- {iter_label} {it.get('iteration_id', '?')}: {it.get('message', '')}")

            conclusions = it.get("conclusions", [])

            if conclusions:

                for c in conclusions[:3]:

                    text = c.get("text", str(c)) if isinstance(c, dict) else str(c)

                    conf = c.get("confidence", "?") if isinstance(c, dict) else "?"

                    conclusion_label = t("label_conclusion", default="结论")

                    conf_label = t("label_confidence", default="置信度")

                    parts.append(f"  {conclusion_label}({conf_label} {conf}): {text}")

            hypotheses = it.get("hypotheses", [])

            if hypotheses:

                hypo_label = t("label_proposed_hypotheses", default="提出猜想")

                parts.append(f"  {hypo_label}: {', '.join(h.get('text', str(h)) if isinstance(h, dict) else str(h) for h in hypotheses[:3])}...")

            report_title = str(it.get("report_title", "") or "").strip()
            report_summary = str(it.get("final_report_summary", "") or "").strip()
            report_meta = it.get("report_meta", {}) or {}
            if report_title or report_summary:
                report_label = "Auto-analysis report" if is_en else "自动分析报告"
                parts.append(f"  {report_label}: {report_title or 'Untitled report'}")
                if report_summary:
                    summary_label = "Report summary" if is_en else "报告摘要"
                    parts.append(f"  {summary_label}: {report_summary[:800]}")
                meta_bits: list[str] = []
                stop_reason = str(report_meta.get("stop_reason", "") or "").strip()
                rounds_completed = report_meta.get("rounds_completed")
                if stop_reason:
                    meta_bits.append(f"stop_reason={stop_reason}")
                if rounds_completed not in (None, ""):
                    meta_bits.append(f"rounds={rounds_completed}")
                if meta_bits:
                    meta_label = "Report meta" if is_en else "报告元信息"
                    parts.append(f"  {meta_label}: {', '.join(meta_bits)}")

        parts.append("")



    # Current context: Tables, Schema, and Samples (Ground Truth)

    sandbox_id = sandbox.get("sandbox_id")

    selected_files = sandbox.get('selected_files', [])

    upload_paths = sandbox.get('upload_paths', {})



    if sandbox_id:

        from app.store import DatabaseStore

        store = DatabaseStore()

        context = store.get_sandbox_full_context(sandbox_id)

        

        # 1. Database Tables

        tables = sandbox.get("tables", [])

        if tables:

            title = t("title_sandbox_tables", default="【沙盒可用表详述 - Ground Truth】")

            parts.append(title)

            for tbl in tables:

                info = context.get(tbl, {})

                cols = info.get("columns", [])

                sample = info.get("sample", [])

                col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in cols)

                table_label = t("label_table_name", default="表名")

                column_label = t("label_columns", default="字段")

                sample_label = t("label_sample_data", default="样数据(前3行)")

                parts.append(f"{table_label}: {tbl}")

                parts.append(f"{column_label}: {col_desc or 'N/A'}")

                if sample:

                    parts.append(f"{sample_label}: {json.dumps(sample, ensure_ascii=False)}")

                parts.append("")



        # 2. Selected Uploaded Files

        if selected_files:

            title = t("title_uploaded_files", default="【已加载的本地文件详述 - Ground Truth】")

            parts.append(title)

            for fname in selected_files:

                info = context.get(fname, {})

                cols = info.get("columns", [])

                sample = info.get("sample", [])

                path = upload_paths.get(fname, t("label_unknown_path", default="未知路径"))

                

                col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in cols)

                file_label = t("label_filename", default="文件名")

                path_label = t("label_physical_path", default="实际物理路径")

                column_label = t("label_columns", default="字段")

                preview_label = t("label_content_preview", default="文件内容摘要/预览")

                sample_label = t("label_sample_data", default="样数据(前3行)")

                

                parts.append(f"{file_label}: {fname}")

                parts.append(f"{path_label}: {path}")

                if cols:

                    parts.append(f"{column_label}: {col_desc}")

                

                text_preview = info.get("text_preview")

                if text_preview:

                    parts.append(f"{preview_label}: \n{text_preview}")

                

                if sample:

                    parts.append(f"{sample_label}: {json.dumps(sample, ensure_ascii=False)}")

                parts.append("")



    question_label = t("label_user_question", default="用户问题")

    db_type_label = _describe_database_type(sandbox)
    db_type_title = t("title_database_type", default="【数据库类型】")
    db_type_label_name = t("label_database_type", default="数据库类型")
    db_type_note = t(
        "note_database_dialect",
        default="请严格按照该数据库方言编写 SQL，避免使用不支持的函数或语法。",
    )
    parts.append(db_type_title)
    parts.append(f"{db_type_label_name}: {db_type_label}")
    parts.append(db_type_note)
    parts.append("")
    parts.append(f"{question_label}: {message}")

    

    # 5. Core Instruction Constraints

    parts.append(f"\n{t('title_instruction_constraints', default='【指令约束】')}")

    

    constraint_keys = [

        "constraint_sql_python",

        "constraint_sql_injection",

        "constraint_multi_table",

        "constraint_local_files",

        "constraint_tabular_files",

        "constraint_text_files",

        "constraint_pre_injection"

    ]

    for ck in constraint_keys:

        val = t(ck)

        if val:

            parts.append(val)

    if is_en:
        parts.append(
            "- Language requirement: keep JSON keys in English, and keep all narrative values in English "
            "(conclusions.text, hypotheses.text, action_items, explanation, final_report_outline)."
        )
    else:
        parts.append(
            "- 输出语言要求：JSON 字段名保持英文，但所有文本内容必须使用简体中文"
            "（包括 conclusions.text、hypotheses.text、action_items、explanation、final_report_outline）。"
        )

    return "\n".join(parts)


def _looks_like_direct_report_output(raw: str) -> bool:
    text = raw.strip()
    if not text:
        return False
    if text.startswith("{") or text.startswith("```json"):
        return False
    markdown_markers = (
        "## ",
        "# ",
        "- ",
        "1. ",
        "Executive Summary",
        "执行摘要",
        "Key Findings",
        "关键发现",
        "Business Recommendations",
        "业务建议",
    )
    return any(marker in text for marker in markdown_markers) and len(text) >= 80


def _is_json_parse_failure_payload(parsed: dict) -> bool:
    conclusions = parsed.get("conclusions", [])
    if not isinstance(conclusions, list):
        return False
    for item in conclusions:
        text = item.get("text", "") if isinstance(item, dict) else str(item)
        if "json" in str(text).lower():
            return True
    return False


def _describe_database_type(sandbox: dict) -> str:
    """Return a human-friendly database type label for prompt context."""
    db_connection = sandbox.get("db_connection") or {}
    db_config = sandbox.get("db_config") or {}
    raw_db_type = (
        db_connection.get("db_type") if isinstance(db_connection, dict) else None
    ) or (
        db_config.get("db_type") if isinstance(db_config, dict) else None
    ) or sandbox.get("db_type") or sandbox.get("database_type") or ""

    db_type = str(raw_db_type).strip().lower()
    if not db_type:
        return "未知"

    label_map = {
        "sqlite": "SQLite",
        "mysql": "MySQL",
        "postgresql": "PostgreSQL",
        "postgres": "PostgreSQL",
        "mssql": "SQL Server",
        "sqlserver": "SQL Server",
        "oracle": "Oracle",
        "duckdb": "DuckDB",
        "clickhouse": "ClickHouse",
        "impala": "Impala",
    }
    return label_map.get(db_type, db_type.upper())





def _run_iteration_by_llm(

    message: str,

    sandbox: dict,

    iteration_history: list[dict],

    business_knowledge: list[str],

    provider: str,

    model: str | None,

    config: AppConfig,

) -> Generator[dict, None, None]:

    system_prompt = config.iteration_system_prompt

    user_prompt = _build_iteration_user_prompt(message, sandbox, iteration_history, business_knowledge)



    full_content = ""

    if provider == "openai":

        chunks = _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)

    else:

        chunks = _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)



    for chunk in chunks:

        full_content += chunk

        # Stream thoughts until JSON block starts

        if "```json" not in full_content and "{" not in full_content:

            yield {"type": "thought", "content": chunk}

        elif not full_content.strip().startswith("{") and "```json" not in full_content:

            yield {"type": "thought", "content": chunk}



    # Parse the final JSON

    parsed = _parse_bundle_json(full_content)

    if _is_json_parse_failure_payload(parsed) and _looks_like_direct_report_output(full_content):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "tools_used": [],
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "explanation": t("agent_explanation_default"),
                "final_report_outline": [],
                "direct_report": full_content.strip(),
            },
        }
        return



    # ── Extract steps (new multi-step format) ─────────────────────────

    steps = parsed.get("steps", [])

    if not isinstance(steps, list):

        steps = []



    # Backward compatibility: if no steps, build from flat sql/python_code

    if not steps:

        sql = str(parsed.get("sql", "")).strip()

        python_code = str(parsed.get("python_code", "")).strip()

        if sql:

            steps.append({"tool": "sql", "code": sql})

        if python_code:

            steps.append({"tool": "python", "code": python_code})



    # Normalize each step

    normalized_steps = []

    for s in steps:

        if isinstance(s, dict) and s.get("tool") and s.get("code"):

            tool = str(s["tool"]).strip().lower()

            if tool in ("sql", "python"):

                normalized_steps.append({"tool": tool, "code": str(s["code"]).strip()})



    # Infer tools_used from steps

    tools_used = []

    for s in normalized_steps:

        tool_name = "execute_select_sql" if s["tool"] == "sql" else "python_interpreter"

        if tool_name not in tools_used:

            tools_used.append(tool_name)



    conclusions = parsed.get("conclusions", [])

    if not isinstance(conclusions, list):

        conclusions = [{"text": str(conclusions), "confidence": 0.5}]

    # Normalize conclusion format

    normalized_conclusions = []

    for c in conclusions:

        if isinstance(c, dict):

            normalized_conclusions.append({

                "text": str(c.get("text", "")),

                "confidence": float(c.get("confidence", 0.5)),

            })

        else:

            normalized_conclusions.append({"text": str(c), "confidence": 0.5})



    hypotheses = parsed.get("hypotheses", [])

    if not isinstance(hypotheses, list):

        hypotheses = [{"id": "h1", "text": str(hypotheses)}]

    normalized_hypotheses = []

    for i, h in enumerate(hypotheses):

        if isinstance(h, dict):

            normalized_hypotheses.append({

                "id": str(h.get("id", f"h{i+1}")),

                "text": str(h.get("text", "")),

            })

        else:

            normalized_hypotheses.append({"id": f"h{i+1}", "text": str(h)})



    action_items = parsed.get("action_items", [])

    if not isinstance(action_items, list):

        action_items = [str(action_items)] if action_items else []

    action_items = [str(a) for a in action_items if str(a).strip()]

    final_report_outline = parsed.get("final_report_outline")
    if isinstance(final_report_outline, list):
        normalized_report_outline = [str(item).strip() for item in final_report_outline if str(item).strip()]
    elif isinstance(final_report_outline, str) and final_report_outline.strip():
        normalized_report_outline = [line.strip() for line in final_report_outline.splitlines() if line.strip()]
    else:
        normalized_report_outline = []



    explanation = str(parsed.get("explanation", "")) or t("agent_explanation_default")



    yield {

        "type": "result",

        "data": {

            "steps": normalized_steps,

            "tools_used": tools_used,

            "conclusions": normalized_conclusions,

            "hypotheses": normalized_hypotheses,

            "action_items": action_items,

            "explanation": explanation,

            "final_report_outline": normalized_report_outline,

            "direct_report": "",

        },

    }





def _run_iteration_by_rules(message: str, sandbox: dict) -> Generator[dict, None, None]:

    """Fallback when no LLM is configured."""

    table = (sandbox.get("tables") or [""])[0]

    if not table:

        raise RuntimeError("当前沙盒没有可用数据表")



    yield {"type": "thought", "content": t("agent_fallback_thought")}



    sql = f"SELECT * FROM {table} LIMIT 200"

    yield {

        "type": "result",

        "data": {

            "steps": [{"tool": "sql", "code": sql}],

            "tools_used": ["execute_select_sql"],

            "conclusions": [

                {"text": t("agent_fallback_conclusion", table=table), "confidence": 1.0},

            ],

            "hypotheses": [

                {"id": "h1", "text": "补充业务目标与时间范围，便于 AI 自动规划分析路径"},

                {"id": "h2", "text": "上传本地 CSV/Excel，与线上数据做联合分析"},

                {"id": "h3", "text": "配置 LLM 后开启智能迭代分析"},

            ],

            "action_items": [t("agent_fallback_item_1")],

            "explanation": t("agent_explanation_mock"),

            "final_report_outline": [],

            "direct_report": "",

        },

    }





# ── LLM protocol implementations (unchanged) ─────────────────────────





def generate_auto_analysis_report(
    message: str,
    loop_rounds: list[dict],
    business_knowledge: list[str],
    stop_reason: str,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Build a final business report from completed auto-analysis rounds."""
    config = load_config()
    is_en = get_lang() == "en"
    report_language = "English" if is_en else "简体中文"
    selected_provider = (provider or config.llm_provider).lower()
    if selected_provider not in {"openai", "anthropic"}:
        return _build_fallback_auto_report(message, loop_rounds, stop_reason)

    knowledge_block = "\n".join(f"- {item}" for item in business_knowledge[:30]) or "- N/A"
    rounds_summary = _build_loop_rounds_summary(loop_rounds)
    system_prompt = (
        "You are a senior analytics lead. Turn multi-round SQL/Python analysis traces into a concise business report. "
        "Do not invent evidence. If confidence is limited, say so explicitly."
    )
    user_prompt = (
        f"Original request:\n{message}\n\n"
        f"Stop reason:\n{stop_reason}\n\n"
        f"Business knowledge:\n{knowledge_block}\n\n"
        f"Auto-analysis rounds:\n{rounds_summary}\n\n"
        f"Write all content in {report_language}.\n\n"
        "Write the final report in Markdown with exactly these sections:\n"
        + (
            "## Executive Summary\n"
            "## Key Findings\n"
            "## Evidence And Analysis Process\n"
            "## Charts And Data Notes\n"
            "## Business Recommendations\n"
            "## Remaining Validation Questions\n"
            if is_en
            else "## 执行摘要\n"
            "## 关键发现\n"
            "## 证据与分析过程\n"
            "## 图表与数据说明\n"
            "## 业务建议\n"
            "## 待验证问题\n"
        )
        + "Avoid code unless a very short snippet is necessary."
    )
    section_template = (
        "## Executive Summary\n"
        "## Key Findings\n"
        "## Evidence And Analysis Process\n"
        "## Charts And Data Notes\n"
        "## Business Recommendations\n"
        "## Remaining Validation Questions\n"
    )
    user_prompt = (
        f"Original request:\n{message}\n\n"
        f"Stop reason:\n{stop_reason}\n\n"
        f"Business knowledge:\n{knowledge_block}\n\n"
        f"Auto-analysis rounds:\n{rounds_summary}\n\n"
        f"Write all content in {report_language}. Translate all section headings and list items into this language.\n\n"
        "Write the final report in Markdown with exactly these sections (translated when required):\n"
        f"{section_template}"
        "Avoid code unless a very short snippet is necessary."
    )
    chunks = (
        _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        if selected_provider == "openai"
        else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
    )
    content = "".join(chunks).strip()
    return content or _build_fallback_auto_report(message, loop_rounds, stop_reason)


def generate_auto_analysis_report_bundle(
    message: str,
    session_history: list[dict],
    business_knowledge: list[str],
    session_patches: list[str],
    loop_rounds: list[dict],
    chart_specs: list[dict],
    final_result_rows: list[dict],
    stop_reason: str,
    rounds_completed: int,
    provider: str | None = None,
    model: str | None = None,
) -> dict:
    lang_code = get_lang()
    report_language = "English" if lang_code == "en" else "简体中文"
    default_title = "Analysis Report" if lang_code == "en" else "\u5206\u6790\u62a5\u544a"
    fallback_markdown = generate_auto_analysis_report(
        message=message,
        loop_rounds=loop_rounds,
        business_knowledge=business_knowledge,
        stop_reason=stop_reason,
        provider=provider,
        model=model,
    )
    fallback_bundle = _build_fallback_report_bundle(fallback_markdown, chart_specs)

    config = load_config()
    selected_provider = (provider or config.llm_provider).lower()
    if selected_provider not in {"openai", "anthropic"}:
        return fallback_bundle

    knowledge_block = "\n".join(f"- {item}" for item in business_knowledge[:30]) or "- N/A"
    patches_block = "\n".join(f"- {item}" for item in session_patches[:20]) or "- N/A"
    history_lines: list[str] = []
    for it in session_history[-8:]:
        entry = f"- [{it.get('mode', 'manual')}] {it.get('message', '')}"
        report_title = str(it.get("report_title", "") or "").strip()
        report_summary = str(it.get("final_report_summary", "") or "").strip()
        if report_title:
            entry += f" | report={report_title}"
        if report_summary:
            entry += f" | summary={report_summary[:220]}"
        history_lines.append(entry)
    history_block = "\n".join(history_lines) or "- N/A"
    rounds_summary = _build_loop_rounds_summary(loop_rounds)
    rows_preview = json.dumps(final_result_rows[:20], ensure_ascii=False)
    chart_ids = [f"chart_{idx}" for idx, spec in enumerate(chart_specs[:20], start=1) if isinstance(spec, dict)]
    chart_hint = ", ".join(chart_ids) if chart_ids else "none"
    required_chart_count = min(3, len(chart_ids))
    default_chart_bindings = [
        {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
        for idx, spec in enumerate(chart_specs[:20], start=1)
        if isinstance(spec, dict)
    ]

    system_prompt = (
        "You are a principal analytics writer. Produce a complete report bundle in JSON. "
        "The html_document must be a full standalone HTML document and may include CSS but no JavaScript."
    )
    base_user_prompt = (
        "Return valid JSON only. No markdown fences.\n"
        "Schema:\n"
        "{"
        "\"title\": string, "
        "\"summary\": string, "
        "\"html_document\": string, "
        "\"chart_bindings\": [{\"chart_id\": string, \"option\": object, \"height\": number}]"
        "}\n\n"
        f"Original request:\n{message}\n\n"
        f"Stop reason: {stop_reason}\n"
        f"Rounds completed: {rounds_completed}\n\n"
        f"Business knowledge:\n{knowledge_block}\n\n"
        f"Session patches:\n{patches_block}\n\n"
        f"Session history summary:\n{history_block}\n\n"
        f"Loop rounds:\n{rounds_summary}\n\n"
        f"Final result rows preview:\n{rows_preview}\n\n"
        f"Output language requirement: {report_language}. Keep title/summary/body in this language.\n\n"
        "Chart mounting rule:\n"
        "- Place chart nodes in html_document with data-chart-id=\"...\".\n"
        f"- Available chart ids: {chart_hint}.\n"
        f"- REQUIRED: include at least {required_chart_count} chart placeholder nodes when chart ids are available.\n"
        "- chart_bindings should map chart_id to ECharts option and height."
    )
    max_attempts = 3
    last_reason = "unknown"
    for attempt in range(1, max_attempts + 1):
        retry_hint = ""
        if attempt > 1:
            retry_hint = (
                "\n\nRetry instruction:\n"
                "The previous output did not contain a qualified standalone HTML document.\n"
                f"Failure reason: {last_reason}\n"
                "Fix this and return valid JSON with html_document as a complete HTML page."
            )
        user_prompt = base_user_prompt + retry_hint
        chunks = (
            _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
            if selected_provider == "openai"
            else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        )
        raw = "".join(chunks).strip()

        candidate_bundle: dict | None = None
        ai_html_source = False
        ai_html_for_validation = ""

        direct_html = _extract_html_document(raw)
        if direct_html:
            ai_html_source = True
            ai_html_for_validation = direct_html
            html_document = _ensure_chart_placeholders(_sanitize_report_html(direct_html), default_chart_bindings)
            candidate_bundle = {
                "title": default_title,
                "summary": fallback_markdown[:500],
                "html_document": html_document,
                "chart_bindings": default_chart_bindings,
                "legacy_markdown": fallback_markdown,
            }
        else:
            parsed = _parse_report_bundle_json(raw)
            if not parsed:
                repaired = _repair_report_bundle_json(
                    raw_response=raw,
                    fallback_markdown=fallback_markdown,
                    provider=selected_provider,
                    model=model,
                    config=config,
                    report_language=report_language,
                )
                parsed = _parse_report_bundle_json(repaired) if repaired else None

            if not parsed:
                llm_html = _generate_html_document_by_llm(
                    fallback_markdown=fallback_markdown,
                    chart_specs=chart_specs,
                    provider=selected_provider,
                    model=model,
                    config=config,
                    report_language=report_language,
                )
                if llm_html:
                    ai_html_source = True
                    ai_html_for_validation = llm_html
                    html_document = _ensure_chart_placeholders(_sanitize_report_html(llm_html), default_chart_bindings)
                    candidate_bundle = {
                        "title": default_title,
                        "summary": fallback_markdown[:500],
                        "html_document": html_document,
                        "chart_bindings": default_chart_bindings,
                        "legacy_markdown": fallback_markdown,
                    }
            else:
                normalized = _normalize_report_bundle(parsed, fallback_bundle, chart_specs)
                raw_html_field = str(parsed.get("html_document", "") or "").strip()
                extracted_nested_html = _extract_html_from_json_like_text(raw_html_field)
                if extracted_nested_html:
                    raw_html_field = extracted_nested_html
                    normalized = _normalize_report_bundle(
                        {**parsed, "html_document": raw_html_field},
                        fallback_bundle,
                        chart_specs,
                    )
                if _looks_like_json_text(raw_html_field):
                    parsed_nested = _parse_report_bundle_json(raw_html_field)
                    if parsed_nested and parsed_nested.get("html_document"):
                        raw_html_field = str(parsed_nested.get("html_document", "") or "").strip()
                        normalized = _normalize_report_bundle(
                            {**parsed, "html_document": raw_html_field},
                            fallback_bundle,
                            chart_specs,
                        )

                if raw_html_field:
                    ai_html_source = True
                    ai_html_for_validation = raw_html_field

                if _looks_like_markdown_text(raw_html_field):
                    llm_html = _generate_html_document_by_llm(
                        fallback_markdown=raw_html_field,
                        chart_specs=chart_specs,
                        provider=selected_provider,
                        model=model,
                        config=config,
                        report_language=report_language,
                    )
                    if llm_html:
                        ai_html_source = True
                        ai_html_for_validation = llm_html
                        normalized["html_document"] = _sanitize_report_html(llm_html)
                    else:
                        ai_html_source = False

                normalized["html_document"] = _ensure_chart_placeholders(
                    normalized.get("html_document", ""),
                    normalized.get("chart_bindings", []) or [],
                )
                candidate_bundle = normalized

        if ai_html_source and ai_html_for_validation and not _is_standalone_html_document(ai_html_for_validation):
            upgraded_html = _generate_html_document_by_llm(
                fallback_markdown=ai_html_for_validation,
                chart_specs=chart_specs,
                provider=selected_provider,
                model=model,
                config=config,
                report_language=report_language,
            )
            if upgraded_html:
                bindings = []
                if candidate_bundle and isinstance(candidate_bundle.get("chart_bindings"), list):
                    bindings = candidate_bundle.get("chart_bindings") or []
                if not bindings:
                    bindings = default_chart_bindings
                if candidate_bundle is None:
                    candidate_bundle = {
                        "title": default_title,
                        "summary": fallback_markdown[:500],
                        "legacy_markdown": fallback_markdown,
                        "chart_bindings": bindings,
                    }
                candidate_bundle["chart_bindings"] = bindings
                candidate_bundle["html_document"] = _ensure_chart_placeholders(
                    _sanitize_report_html(upgraded_html),
                    bindings,
                )
                ai_html_for_validation = upgraded_html

        ok, reason = _is_qualified_ai_report_bundle(
            candidate_bundle,
            required_chart_count=required_chart_count,
            require_ai_html_source=ai_html_source,
            ai_html_document=ai_html_for_validation,
        )
        if ok and candidate_bundle is not None:
            return candidate_bundle
        last_reason = reason

    raise RuntimeError(
        f"AI failed to generate qualified HTML report after {max_attempts} attempts: {last_reason}"
    )


def _is_qualified_ai_report_bundle(
    bundle: dict | None,
    required_chart_count: int,
    require_ai_html_source: bool,
    ai_html_document: str,
) -> tuple[bool, str]:
    if not bundle or not isinstance(bundle, dict):
        return False, "empty bundle"
    if not require_ai_html_source:
        return False, "html_document is not generated from AI output"
    ai_html_text = str(ai_html_document or "").strip()
    if not ai_html_text:
        return False, "AI html output is empty"

    html_document = str(bundle.get("html_document", "") or "").strip()
    if not html_document:
        return False, "missing html_document"
    if not re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", html_document, re.IGNORECASE):
        return False, "html_document is not a standalone HTML document"
    if "<body" not in html_document.lower():
        return False, "html_document missing body tag"

    if required_chart_count > 0:
        placeholder_ids = set(
            re.findall(r'data-chart-id=["\']([^"\']+)["\']', ai_html_text, flags=re.IGNORECASE)
        )
        if len(placeholder_ids) < required_chart_count:
            return False, f"chart placeholders insufficient: {len(placeholder_ids)} < {required_chart_count}"

    return True, ""


def _is_standalone_html_document(text: str) -> bool:
    html_text = str(text or "").strip()
    if not html_text:
        return False
    if not re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", html_text, re.IGNORECASE):
        return False
    return "<body" in html_text.lower()


def _parse_report_bundle_json(raw: str) -> dict | None:
    text = str(raw or "").strip()
    if not text:
        return None
    candidates: list[str] = []
    if text.startswith("```"):
        fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
        if fence_match:
            candidates.append(fence_match.group(1))
    if text.startswith("{") and text.endswith("}"):
        candidates.append(text)
    object_match = re.search(r"\{.*\}", text, re.DOTALL)
    if object_match:
        candidates.append(object_match.group(0))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue
    return None


def _extract_html_document(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    json_like = _extract_html_from_json_like_text(text)
    if json_like:
        return json_like
    if "<html" in text.lower():
        match = re.search(r"<!doctype html.*</html>|<html.*</html>", text, re.IGNORECASE | re.DOTALL)
        return (match.group(0) if match else text).strip()
    fence = re.search(r"```(?:html)?\s*(<!doctype html.*</html>|<html.*</html>)\s*```", text, re.IGNORECASE | re.DOTALL)
    if fence:
        return fence.group(1).strip()
    return ""


def _extract_html_from_json_like_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    normalized = re.sub(r"^```(?:json|html)?\s*", "", text, flags=re.IGNORECASE)
    normalized = re.sub(r"\s*```$", "", normalized, flags=re.IGNORECASE).strip()

    def parse_obj(candidate: str) -> str:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return ""
        if isinstance(parsed, dict):
            html_doc = parsed.get("html_document")
            if isinstance(html_doc, str) and html_doc.strip():
                return html_doc.strip()
        return ""

    parsed_html = parse_obj(normalized)
    if parsed_html:
        return parsed_html

    first_brace = normalized.find("{")
    last_brace = normalized.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        parsed_html = parse_obj(normalized[first_brace : last_brace + 1])
        if parsed_html:
            return parsed_html

    field_match = re.search(
        r'"html_document"\s*:\s*"([\s\S]*?)"\s*(?:,\s*"chart_bindings"|,\s*"summary"|,\s*"title"|\})',
        normalized,
        flags=re.IGNORECASE,
    )
    if field_match:
        raw_val = field_match.group(1)
        try:
            return json.loads(f'"{raw_val}"').strip()
        except json.JSONDecodeError:
            return raw_val.strip()
    return ""


def _looks_like_markdown_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if "<html" in raw.lower() or "<body" in raw.lower():
        return False
    if re.search(r"<[a-zA-Z][^>]*>", raw):
        return False
    markers = ("## ", "# ", "- ", "1. ", "|---", "**")
    return any(marker in raw for marker in markers)


def _looks_like_json_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if not (raw.startswith("{") and raw.endswith("}")):
        return False
    try:
        parsed = json.loads(raw)
        return isinstance(parsed, dict)
    except json.JSONDecodeError:
        return False


def _repair_report_bundle_json(
    raw_response: str,
    fallback_markdown: str,
    provider: str,
    model: str | None,
    config: AppConfig,
    report_language: str,
) -> str:
    system_prompt = (
        "You are a strict JSON formatter. Convert the input into valid JSON only. "
        "No prose, no code fences."
    )
    user_prompt = (
        "Output exactly one JSON object with keys: title, summary, html_document, chart_bindings.\n"
        "If the input is markdown, convert it to an HTML document for html_document.\n"
        "html_document must include chart placeholders using data-chart-id when chart ids are present.\n"
        "chart_bindings can be an empty array when unavailable.\n\n"
        f"Language requirement: {report_language}.\n\n"
        f"Raw response to repair:\n{raw_response}\n\n"
        f"Fallback markdown content:\n{fallback_markdown}\n"
    )
    chunks = (
        _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        if provider == "openai"
        else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
    )
    return "".join(chunks).strip()


def _generate_html_document_by_llm(
    fallback_markdown: str,
    chart_specs: list[dict],
    provider: str,
    model: str | None,
    config: AppConfig,
    report_language: str,
) -> str:
    chart_ids = [f"chart_{idx}" for idx, spec in enumerate(chart_specs[:20], start=1) if isinstance(spec, dict)]
    chart_hint = ", ".join(chart_ids) if chart_ids else "none"
    system_prompt = (
        "You are a data-report web designer. Return a standalone HTML document only. "
        "Use semantic layout and polished CSS. Do not include JavaScript."
    )
    user_prompt = (
        "Convert the following report content into a complete HTML document.\n"
        "Requirements:\n"
        "- Return only HTML text.\n"
        "- Use clear visual hierarchy.\n"
        "- Keep content faithful to source.\n"
        f"- Use {report_language} for the whole document text.\n"
        "- You MUST include chart placeholders using available chart ids: <div data-chart-id=\"...\"></div>.\n"
        f"- REQUIRED: include at least {min(3, len(chart_ids))} chart placeholders when chart ids are available.\n"
        f"Available chart ids: {chart_hint}\n\n"
        f"Source report markdown:\n{fallback_markdown}\n"
    )
    chunks = (
        _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        if provider == "openai"
        else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
    )
    html_text = "".join(chunks).strip()
    extracted = _extract_html_document(html_text)
    if extracted:
        return extracted

    fragment = _extract_html_from_json_like_text(html_text) or html_text
    wrapped = _wrap_html_fragment_as_document(fragment)
    if wrapped:
        return wrapped
    return ""


def _wrap_html_fragment_as_document(fragment: str) -> str:
    text = str(fragment or "").strip()
    if not text:
        return ""
    text = re.sub(r"^```(?:html)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text, flags=re.IGNORECASE).strip()
    if not text:
        return ""
    if re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", text, re.IGNORECASE):
        return text
    if not re.search(r"<[a-zA-Z][^>]*>", text):
        return ""

    body_match = re.search(r"<body[^>]*>([\s\S]*?)</body>", text, re.IGNORECASE)
    body_content = body_match.group(1).strip() if body_match else text
    if not body_content:
        return ""

    html_lang = "en" if get_lang() == "en" else "zh-CN"
    report_title = "Analysis Report" if get_lang() == "en" else "分析报告"
    return (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{html.escape(report_title)}</title></head><body>{body_content}</body></html>"
    )


def _ensure_chart_placeholders(html_document: str, chart_bindings: list[dict]) -> str:
    html_text = str(html_document or "")
    if not html_text or not chart_bindings:
        return html_text
    is_en = get_lang() == "en"
    chart_label = "Chart" if is_en else "图表"
    charts_label = "Charts" if is_en else "图表"
    existing_ids = set(re.findall(r'data-chart-id=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE))
    missing_ids = [
        str(item.get("chart_id", "")).strip()
        for item in chart_bindings
        if isinstance(item, dict) and str(item.get("chart_id", "")).strip() and str(item.get("chart_id", "")).strip() not in existing_ids
    ]
    if not missing_ids:
        return html_text
    section_items = "".join(
        (
            f'<section style="margin-top:18px;">'
            f'<h3 style="margin:0 0 8px;">{chart_label} {idx}</h3>'
            f'<div data-chart-id="{html.escape(chart_id)}"></div>'
            f"</section>"
        )
        for idx, chart_id in enumerate(missing_ids, start=1)
    )
    chart_section = (
        '<section style="margin-top:22px;">'
        f'<h2 style="margin:0 0 10px;">{charts_label}</h2>'
        f"{section_items}"
        "</section>"
    )
    if "</body>" in html_text.lower():
        return re.sub(r"</body>", chart_section + "</body>", html_text, count=1, flags=re.IGNORECASE)
    return html_text + chart_section


def _normalize_report_bundle(bundle: dict, fallback_bundle: dict, chart_specs: list[dict]) -> dict:
    is_en = get_lang() == "en"
    html_lang = "en" if is_en else "zh-CN"
    title = str(bundle.get("title", "") or "").strip() or str(fallback_bundle.get("title", "Auto Analysis Report"))
    summary = str(bundle.get("summary", "") or "").strip() or str(fallback_bundle.get("summary", ""))
    raw_html = str(bundle.get("html_document", "") or "").strip()
    html_document = ""
    if raw_html:
        extracted_html = _extract_html_document(raw_html)
        if extracted_html:
            html_document = _sanitize_report_html(extracted_html)
        elif _looks_like_markdown_text(raw_html):
            html_document = _markdown_to_basic_html(raw_html)
        else:
            html_document = _sanitize_report_html(raw_html)
    if not html_document:
        html_document = str(fallback_bundle.get("html_document", ""))
    if not html_document:
        html_document = _markdown_to_basic_html(str(fallback_bundle.get("legacy_markdown", "") or ""))
    if "<html" not in html_document.lower():
        html_document = (
            f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
            f"<title>{html.escape(title)}</title></head><body>{html_document}</body></html>"
        )

    normalized_bindings: list[dict] = []
    for item in bundle.get("chart_bindings", []) or []:
        if not isinstance(item, dict):
            continue
        chart_id = str(item.get("chart_id", "") or "").strip()
        option = item.get("option")
        if not chart_id or not isinstance(option, dict):
            continue
        try:
            height = int(item.get("height", 360))
        except (TypeError, ValueError):
            height = 360
        normalized_bindings.append(
            {
                "chart_id": chart_id,
                "option": option,
                "height": min(1200, max(200, height)),
            }
        )

    if not normalized_bindings:
        normalized_bindings = [
            {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
            for idx, spec in enumerate(chart_specs[:20], start=1)
            if isinstance(spec, dict)
        ]

    return {
        "title": title,
        "summary": summary[:2000],
        "html_document": html_document,
        "chart_bindings": normalized_bindings,
        "legacy_markdown": str(fallback_bundle.get("legacy_markdown", "") or ""),
    }


def _build_fallback_report_bundle(markdown_text: str, chart_specs: list[dict]) -> dict:
    safe_markdown = str(markdown_text or "").strip()
    is_en = get_lang() == "en"
    chart_label = "Chart" if is_en else "图表"
    chart_bindings = [
        {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
        for idx, spec in enumerate(chart_specs[:20], start=1)
        if isinstance(spec, dict)
    ]
    chart_slots = "".join(
        f'<section style="margin-top:20px;"><h2 style="margin:0 0 8px;">{chart_label} {idx}</h2><div data-chart-id="chart_{idx}"></div></section>'
        for idx, _ in enumerate(chart_bindings, start=1)
    )
    html_document = _markdown_to_basic_html(safe_markdown, chart_slots)
    return {
        "title": "Analysis Report" if get_lang() == "en" else "\u5206\u6790\u62a5\u544a",
        "summary": safe_markdown[:500],
        "html_document": html_document,
        "chart_bindings": chart_bindings,
        "legacy_markdown": safe_markdown,
    }


def _markdown_to_basic_html(markdown_text: str, extra_blocks: str = "") -> str:
    is_en = get_lang() == "en"
    html_lang = "en" if is_en else "zh-CN"
    report_title = "Auto Analysis Report" if is_en else "自动分析报告"
    rendered = _render_markdown_like_html(markdown_text)
    return (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{report_title}</title>"
        "<style>body{font-family:Inter,Arial,sans-serif;margin:0;background:#f8fafc;color:#0f172a}"
        ".paper{max-width:1080px;margin:24px auto;padding:28px;background:#fff;border:1px solid #e2e8f0;border-radius:14px}"
        ".content{font-size:14px;line-height:1.7}"
        ".content h1,.content h2,.content h3{margin:18px 0 10px;line-height:1.35}"
        ".content p{margin:10px 0}"
        ".content ul,.content ol{margin:8px 0 12px 22px}"
        ".content li{margin:4px 0}"
        ".content code{padding:1px 5px;border-radius:4px;background:#e2e8f0;font-family:Consolas,monospace}"
        ".content hr{border:none;border-top:1px solid #e2e8f0;margin:18px 0}"
        "@media print{body{background:#fff}.paper{border:none;max-width:none;margin:0;padding:0}}</style>"
        "</head><body><main class=\"paper\">"
        f"<h1 style=\"margin-top:0;\">{report_title}</h1>"
        f"<div class=\"content\">{rendered}</div>"
        f"{extra_blocks}"
        "</main></body></html>"
    )


def _render_markdown_like_html(markdown_text: str) -> str:
    def inline_render(text: str) -> str:
        escaped = html.escape(text)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
        return escaped

    lines = str(markdown_text or "").splitlines()
    out: list[str] = []
    list_mode: str | None = None

    def close_list() -> None:
        nonlocal list_mode
        if list_mode == "ul":
            out.append("</ul>")
        elif list_mode == "ol":
            out.append("</ol>")
        list_mode = None

    for raw in lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            close_list()
            continue
        if stripped == "---":
            close_list()
            out.append("<hr/>")
            continue
        if stripped.startswith("### "):
            close_list()
            out.append(f"<h3>{inline_render(stripped[4:])}</h3>")
            continue
        if stripped.startswith("## "):
            close_list()
            out.append(f"<h2>{inline_render(stripped[3:])}</h2>")
            continue
        if stripped.startswith("# "):
            close_list()
            out.append(f"<h1>{inline_render(stripped[2:])}</h1>")
            continue
        if re.match(r"^\d+\.\s+", stripped):
            if list_mode != "ol":
                close_list()
                out.append("<ol>")
                list_mode = "ol"
            item = re.sub(r"^\d+\.\s+", "", stripped)
            out.append(f"<li>{inline_render(item)}</li>")
            continue
        if stripped.startswith("- "):
            if list_mode != "ul":
                close_list()
                out.append("<ul>")
                list_mode = "ul"
            out.append(f"<li>{inline_render(stripped[2:])}</li>")
            continue
        close_list()
        out.append(f"<p>{inline_render(stripped)}</p>")

    close_list()
    return "\n".join(out)


def _sanitize_report_html(document: str) -> str:
    text = str(document or "")
    text = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\son[a-z]+\s*=\s*(['\"]).*?\1", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'(href|src)\s*=\s*([\"\'])\s*javascript:[^\"\']*\2', r"\1=\2#\2", text, flags=re.IGNORECASE)
    return text


def _build_loop_rounds_summary(loop_rounds: list[dict]) -> str:
    sections: list[str] = []
    for round_item in loop_rounds:
        round_no = round_item.get("round", "?")
        result = round_item.get("result") or {}
        exec_result = round_item.get("execution") or {}
        rows = exec_result.get("rows") or []
        charts = exec_result.get("chart_specs") or []
        step_bits = []
        for step in (result.get("steps") or []):
            tool = str(step.get("tool", "")).strip()
            code = str(step.get("code", "")).strip().replace("\n", " ")
            if tool and code:
                step_bits.append(f"{tool}: {code[:180]}")
        conclusion_bits = []
        for conclusion in (result.get("conclusions") or [])[:5]:
            if isinstance(conclusion, dict):
                conclusion_bits.append(
                    f"{conclusion.get('text', '')} (confidence={conclusion.get('confidence', 0.5)})"
                )
            else:
                conclusion_bits.append(str(conclusion))
        action_bits = [str(item) for item in (result.get("action_items") or [])[:5]]
        sections.append(
            "\n".join(
                [
                    f"Round {round_no}",
                    f"Focus: {round_item.get('prompt', '')}",
                    f"Tools: {', '.join(result.get('tools_used', [])) or 'none'}",
                    f"Steps: {' | '.join(step_bits) or 'none'}",
                    f"Conclusions: {' | '.join(conclusion_bits) or 'none'}",
                    f"Actions: {' | '.join(action_bits) or 'none'}",
                    f"Rows: {len(rows)}",
                    f"Charts: {len(charts)}",
                    f"Error: {round_item.get('error') or exec_result.get('error') or 'none'}",
                ]
            )
        )
    return "\n\n".join(sections) or "No completed rounds."


def _build_fallback_auto_report(message: str, loop_rounds: list[dict], stop_reason: str) -> str:
    is_en = get_lang() == "en"
    findings: list[str] = []
    suggestions: list[str] = []
    pending: list[str] = []
    evidence: list[str] = []
    chart_count = 0
    for round_item in loop_rounds:
        result = round_item.get("result") or {}
        exec_result = round_item.get("execution") or {}
        chart_count += len(exec_result.get("chart_specs") or [])
        for conclusion in (result.get("conclusions") or [])[:2]:
            if isinstance(conclusion, dict):
                text = str(conclusion.get("text", "")).strip()
                confidence = conclusion.get("confidence", 0.5)
                if text:
                    findings.append(f"- {text} (confidence {int(float(confidence) * 100)}%)")
            else:
                text = str(conclusion).strip()
                if text:
                    findings.append(f"- {text}")
        for action in (result.get("action_items") or [])[:2]:
            text = str(action).strip()
            if text:
                suggestions.append(f"- {text}")
        for hypothesis in (result.get("hypotheses") or [])[:2]:
            text = hypothesis.get("text", "") if isinstance(hypothesis, dict) else str(hypothesis)
            text = str(text).strip()
            if text:
                pending.append(f"- {text}")
        rows_count = len(exec_result.get("rows") or [])
        evidence.append(
            f"- Round {round_item.get('round', '?')} executed {len(result.get('steps') or [])} steps and produced {rows_count} result rows"
        )

    if not findings:
        findings.append(
            "- The auto-analysis did not produce stable conclusions yet. Validate against raw data before acting."
            if is_en
            else "- 自动分析尚未形成稳定结论，建议先基于原始数据复核后再落地。"
        )
    if not suggestions:
        suggestions.append(
            "- Add tighter business constraints or a time range, then rerun one-click analysis."
            if is_en
            else "- 建议补充更明确的业务约束或时间范围后，再次执行一键分析。"
        )
    if not pending:
        pending.append("- No additional open validation questions." if is_en else "- 当前暂无新增待验证问题。")

    if is_en:
        return (
            "## Executive Summary\n"
            f"- Question: {message}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Completed Rounds: {len(loop_rounds)}\n\n"
            "## Key Findings\n"
            f"{chr(10).join(findings)}\n\n"
            "## Evidence And Analysis Process\n"
            f"{chr(10).join(evidence) if evidence else '- No execution trace available.'}\n\n"
            "## Charts And Data Notes\n"
            f"- Generated {chart_count} charts.\n\n"
            "## Business Recommendations\n"
            f"{chr(10).join(suggestions)}\n\n"
            "## Remaining Validation Questions\n"
            f"{chr(10).join(pending)}"
        )
    return (
        "## 执行摘要\n"
        f"- 问题: {message}\n"
        f"- 停止原因: {stop_reason}\n"
        f"- 完成轮次: {len(loop_rounds)}\n\n"
        "## 关键发现\n"
        f"{chr(10).join(findings)}\n\n"
        "## 证据与分析过程\n"
        f"{chr(10).join(evidence) if evidence else '- 暂无可用执行轨迹。'}\n\n"
        "## 图表与数据说明\n"
        f"- 共生成 {chart_count} 张图表。\n\n"
        "## 业务建议\n"
        f"{chr(10).join(suggestions)}\n\n"
        "## 待验证问题\n"
        f"{chr(10).join(pending)}"
    )


def _is_incomplete_stream_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "incomplete chunked read" in text
        or "peer closed connection" in text
        or "incomplete message body" in text
    )


def _call_openai_protocol(system_prompt: str, user_prompt: str, model: str | None, config: AppConfig) -> Generator[str, None, None]:

    api_key = config.openai_api_key

    if not api_key:

        raise RuntimeError("缺少 OPENAI_API_KEY")

    base_url = config.openai_base_url.rstrip("/")

    endpoint = config.openai_endpoint

    url = f"{base_url}{endpoint}" if endpoint.startswith("/") else endpoint

    payload = {

        "model": model or config.openai_model,

        "temperature": 0.2,

        "max_tokens": 8192,

        "stream": True,

        "messages": [

            {"role": "system", "content": system_prompt},

            {"role": "user", "content": user_prompt},

        ],

    }

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}



    received_any = False
    try:
        with httpx.Client(timeout=60.0) as client:

            with client.stream("POST", url, headers=headers, json=payload) as response:

                if response.status_code >= 400:

                    raise RuntimeError(f"OpenAI 协议请求失败: {response.status_code}")



                for line in response.iter_lines():

                    if line.startswith("data: "):

                        data_str = line[6:].strip()

                        if data_str == "[DONE]":

                            break

                        try:

                            data = json.loads(data_str)

                            delta = data.get("choices", [{}])[0].get("delta", {})

                            content = delta.get("content", "")

                            if content:
                                received_any = True
                                yield content

                        except json.JSONDecodeError:

                            continue
    except Exception as exc:
        if received_any and _is_incomplete_stream_error(exc):
            return
        raise





def _call_anthropic_protocol(system_prompt: str, user_prompt: str, model: str | None, config: AppConfig) -> Generator[str, None, None]:

    api_key = config.anthropic_api_key

    if not api_key:

        raise RuntimeError("缺少 ANTHROPIC_API_KEY")

    base_url = config.anthropic_base_url.rstrip("/")

    endpoint = config.anthropic_endpoint

    url = f"{base_url}{endpoint}" if endpoint.startswith("/") else endpoint

    payload = {

        "model": model or config.anthropic_model,

        "max_tokens": 4000,

        "temperature": 0.2,

        "stream": True,

        "system": system_prompt,

        "messages": [{"role": "user", "content": user_prompt}],

    }

    headers = {

        "x-api-key": api_key,

        "anthropic-version": config.anthropic_version,

        "Content-Type": "application/json",

    }



    received_any = False
    try:
        with httpx.Client(timeout=60.0) as client:

            with client.stream("POST", url, headers=headers, json=payload) as response:

                if response.status_code >= 400:

                    raise RuntimeError(f"Anthropic 协议请求失败: {response.status_code}")



                for line in response.iter_lines():

                    if line.startswith("data: "):

                        data_str = line[6:].strip()

                        try:

                            data = json.loads(data_str)

                            type_ = data.get("type")

                            if type_ == "content_block_delta":

                                delta = data.get("delta", {})

                                if delta.get("type") == "text_delta":
                                    text = delta.get("text", "")
                                    if text:
                                        received_any = True
                                        yield text

                        except json.JSONDecodeError:

                            continue
    except Exception as exc:
        if received_any and _is_incomplete_stream_error(exc):
            return
        raise





def _parse_bundle_json(raw: str) -> dict:

    """Parse LLM output as JSON. Tries multiple strategies in order."""

    text = raw.strip()



    # 1. Direct parse

    try:

        return json.loads(text)

    except json.JSONDecodeError:

        pass



    # 2. Try extracting from ```json ... ``` or ``` ... ``` block

    for fence_pattern in (r"```json\s*([\s\S]*?)```", r"```\s*([\s\S]*?)```"):

        md_match = re.search(fence_pattern, text)

        if md_match:

            try:

                return json.loads(md_match.group(1).strip())

            except json.JSONDecodeError:

                pass



    # 3. Find the outermost balanced { ... } object

    start = text.find("{")

    if start != -1:

        depth = 0

        in_str = False

        escape = False

        end = -1

        for i, ch in enumerate(text[start:], start=start):

            if escape:

                escape = False

                continue

            if ch == "\\" and in_str:

                escape = True

                continue

            if ch == '"':

                in_str = not in_str

                continue

            if in_str:

                continue

            if ch == "{":

                depth += 1

            elif ch == "}":

                depth -= 1

                if depth == 0:

                    end = i

                    break



        if end != -1:

            # Fully balanced JSON found

            candidate = text[start:end + 1]

            try:

                return json.loads(candidate)

            except json.JSONDecodeError:

                pass

        else:

            # 3b. TRUNCATION REPAIR: JSON was cut off — try to salvage what we have

            # Take everything from '{' to end of text and close open braces/brackets

            partial = text[start:]

            # Count how many levels deep we are so we can close them

            repair_depth = 0

            repair_in_str = False

            repair_escape = False

            for ch in partial:

                if repair_escape:

                    repair_escape = False

                    continue

                if ch == "\\" and repair_in_str:

                    repair_escape = True

                    continue

                if ch == '"':

                    repair_in_str = not repair_in_str

                    continue

                if repair_in_str:

                    continue

                if ch == "{":

                    repair_depth += 1

                elif ch == "}":

                    repair_depth -= 1

            # Close any open string, then add closing braces

            if repair_in_str:

                partial += '"'

            partial += "}" * max(repair_depth, 1)

            try:

                return json.loads(partial)

            except json.JSONDecodeError:

                # Even partial salvage failed — just try parsing with null conclusions appended

                try:

                    salvage = partial.rsplit(",", 1)[0] + ", \"conclusions\": [{\"text\": \"(响应被截断，步骤已执行)\", \"confidence\": 0.5}]}" + "}" * max(repair_depth - 1, 0)

                    return json.loads(salvage)

                except Exception:

                    pass



    # 4. Fallback for unescaped newlines inside strings

    # This specifically addresses the common issue where LLM outputs real \n inside "python_code"

    # A simple regex approach: replace \n with \\n if it looks like it's inside quotes.

    # While full JSON parsing with unescaped newlines is hard, we can try replacing all newlines

    # and then parse. Wait, literal newlines are universally invalid in JSON strings.

    # Let's cleanly replace unescaped real newlines into \n before JSON parsing for the whole text if it wraps.

    clean_text = raw.strip()

    # A brutal but effective heuristic for code generated in strict JSON:

    # Any actual newline character \n (or \r\n) that exists in the raw response

    # can just be converted to an escaped \\n as long as it isn't part of structural formatting.

    # To be safe, we just regex replace all real newlines with \n since the prompt instructed it anyway.

    if "\n" in clean_text:

        # Before doing a blind replace, we try to extract the JSON block again and replace \n inside it.

        # But wait, python's json.loads requires no real newlines in strings. 

        # If the LLM returned real newlines, replacing them globally with \\n might mess up indentation

        # but the JSON string parsing will succeed.

        fallback_text = clean_text.replace("\n", "\\n").replace("\r", "")

        try:

            return json.loads(fallback_text)

        except json.JSONDecodeError:

            pass

            

        # Try finding the fence again in the fallback_text

        for fence_pattern in (r"```json\s*([\s\S]*?)```", r"```\s*([\s\S]*?)```"):

            md_match = re.search(fence_pattern, fallback_text)

            if md_match:

                try:

                    return json.loads(md_match.group(1).strip())

                except json.JSONDecodeError:

                    pass



    # 5. Graceful fallback — wrap raw text in a minimal valid structure

    return {

        "sql": "",

        "python_code": "",

        "tools_used": [],

        "conclusions": [{"text": f"模型返回格式异常，无法解析为 JSON。原始输出片段：{text[:200]}", "confidence": 0.0}],

        "hypotheses": [{"id": "h1", "text": "请重试或检查 LLM 配置"}],

        "action_items": ["检查 LLM 返回是否被截断或格式有误"],

        "explanation": "JSON 解析失败，已降级为错误提示。",

    }





def generate_skill_proposal(

    message: str,

    analysis_result: dict,

    sandbox_name: str,

    provider: str | None = None,

    model: str | None = None,

) -> dict:

    """Uses LLM to summarize a successful analysis into a skill proposal."""

    from app.i18n import t, get_lang

    is_en = get_lang() == "en"

    system_prompt = "You are a business knowledge extraction expert. Please extract a reusable 'analysis skill' from the user's question, analysis process, and conclusions." if is_en else "你是一个业务知识提炼专家。请根据用户的提问、分析过程和结论，提取一个可复用的“分析经验”。"

    user_prompt_template = """

User Question: {message}

Sandbox: {sandbox_name}

Conclusions: {conclusions}

Steps: {steps}

Explanation: {explanation}



Please return a JSON object with:

1. "name": Skill name (related to context and concise)

2. "description": Detailed description of the skill

3. "tags": List of keyword tags (3-5)

4. "knowledge": Core business knowledge (rules, formulas, field meanings, etc. - be very detailed so it can be reused).



Return ONLY JSON.

""" if is_en else """

用户问题: {message}

沙盒名称: {sandbox_name}

分析结论: {conclusions}

分析步骤: {steps}

核心解释: {explanation}



请返回一个 JSON 对象，包含以下字段：

1. "name": 经验名称（与整个对话内容高度相关，并且简洁）

2. "description": 经验描述（要非常详细的描述）

3. "tags": 关键词标签列表（3-5个）

4. "knowledge": 提炼的核心业务知识（要非常详细的业务知识，包含交互流程、业务规则、指标口径、字段说明等所有知识，要让一个普通人拿到这个经验描述能直接用起来例如：某某指标计算公式、业务判定逻辑、关键字段的业务含义。每条知识点要独立且精确，可以被后续对话直接参考）



仅返回 JSON，不要任何解释文字。

"""

    steps = analysis_result.get('steps', [])
    if not steps and analysis_result.get("loop_rounds"):
        steps = []
        for round_payload in analysis_result.get("loop_rounds", []):
            for step in ((round_payload.get("result") or {}).get("steps") or []):
                if isinstance(step, dict):
                    steps.append(step)
    report_text = str(analysis_result.get("final_report_md", "")).strip()

    user_prompt = user_prompt_template.format(

        message=message,

        sandbox_name=sandbox_name,

        conclusions=json.dumps(analysis_result.get('conclusions', []), ensure_ascii=False),

        steps=json.dumps(steps, ensure_ascii=False),

        explanation=(analysis_result.get('explanation', '') or "") + (f"\n\nFinal Report:\n{report_text}" if report_text else "")

    )



    config = load_config()

    selected_provider = (provider or config.llm_provider).lower()



    full_content = ""

    try:

        if selected_provider == "openai":

            chunks = _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)

        else:

            chunks = _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)

        

        for chunk in chunks:

            full_content += chunk

    except Exception:

        pass



    # Basic cleanup and parsing

    parsed = _parse_bundle_json(full_content)

    knowledge_val = parsed.get("knowledge", [])

    if isinstance(knowledge_val, str):

        knowledge_list = [k.strip() for k in knowledge_val.split("\n") if k.strip()]

    elif isinstance(knowledge_val, list):

        knowledge_list = [str(k) for k in knowledge_val]

    else:

        knowledge_list = []



    return {

        "name": str(parsed.get("name", "")).strip(),

        "description": str(parsed.get("description", "")).strip(),

        "tags": parsed.get("tags", []) if isinstance(parsed.get("tags"), list) else [],

        "knowledge": knowledge_list,

    }

