import html
import json

import re

from typing import Generator

from pathlib import Path



import httpx



from app.config import AppConfig, format_prompt, get_prompt, load_config

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

    user_prompt = format_prompt(
        config.prompts,
        "data_insight_user",
        question_label=question_label,
        message=message,
        sql_label=sql_label,
        sql=sql,
        data_label=data_label,
        data_summary=data_summary,
        instruction=instruction,
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
    config: AppConfig | None = None,

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

        from app.store import store

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
                is_virtual_view = bool(info.get("virtual_view")) or str(info.get("type") or "") == "virtual_view"
                view_desc = str(info.get("description") or "").strip()
                source_sql_summary = str(info.get("source_sql_summary") or "").strip()

                col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in cols)

                table_label = t("label_table_name", default="表名")
                type_label = t("label_context_type", default="上下文类型")
                view_desc_label = t("label_view_description", default="视图业务描述")
                view_sql_label = t("label_view_source_sql", default="源 SQL 摘要")

                column_label = t("label_columns", default="字段")

                sample_label = t("label_sample_data", default="样数据(前3行)")

                parts.append(f"{table_label}: {tbl}")
                parts.append(f"{type_label}: {'virtual_view' if is_virtual_view else 'physical_table'}")
                if is_virtual_view and view_desc:
                    parts.append(f"{view_desc_label}: {view_desc}")
                if is_virtual_view and source_sql_summary:
                    parts.append(f"{view_sql_label}: {source_sql_summary}")

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

    cfg = config or load_config()
    parts.append(get_prompt(cfg.prompts, "iteration_user_constraints"))

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


def _normalize_iteration_payload(parsed: dict, *, include_steps: bool) -> dict:
    steps = parsed.get("steps", [])
    if not isinstance(steps, list):
        steps = []

    if include_steps and not steps:
        sql = str(parsed.get("sql", "")).strip()
        python_code = str(parsed.get("python_code", "")).strip()
        if sql:
            steps.append({"tool": "sql", "code": sql})
        if python_code:
            steps.append({"tool": "python", "code": python_code})

    normalized_steps = []
    if include_steps:
        for step in steps:
            if isinstance(step, dict) and step.get("tool") and step.get("code"):
                tool = str(step["tool"]).strip().lower()
                if tool in ("sql", "python"):
                    normalized_steps.append({"tool": tool, "code": str(step["code"]).strip()})

    tools_used = []
    for step in normalized_steps:
        tool_name = "execute_select_sql" if step["tool"] == "sql" else "python_interpreter"
        if tool_name not in tools_used:
            tools_used.append(tool_name)

    conclusions = parsed.get("conclusions", [])
    if not isinstance(conclusions, list):
        conclusions = [{"text": str(conclusions), "confidence": 0.5}]
    normalized_conclusions = []
    for conclusion in conclusions:
        if isinstance(conclusion, dict):
            try:
                confidence = float(conclusion.get("confidence", 0.5))
            except (TypeError, ValueError):
                confidence = 0.5
            normalized_conclusions.append(
                {
                    "text": str(conclusion.get("text", "")),
                    "confidence": confidence,
                }
            )
        else:
            normalized_conclusions.append({"text": str(conclusion), "confidence": 0.5})

    hypotheses = parsed.get("hypotheses", [])
    if not isinstance(hypotheses, list):
        hypotheses = [{"id": "h1", "text": str(hypotheses)}]
    normalized_hypotheses = []
    for index, hypothesis in enumerate(hypotheses):
        if isinstance(hypothesis, dict):
            normalized_hypotheses.append(
                {
                    "id": str(hypothesis.get("id", f"h{index+1}")),
                    "text": str(hypothesis.get("text", "")),
                }
            )
        else:
            normalized_hypotheses.append({"id": f"h{index+1}", "text": str(hypothesis)})

    action_items = parsed.get("action_items", [])
    if not isinstance(action_items, list):
        action_items = [action_items] if action_items else []
    action_items = [_stringify_action_item(item) for item in action_items]
    action_items = [item for item in action_items if item]

    final_report_outline = parsed.get("final_report_outline")
    if isinstance(final_report_outline, list):
        normalized_report_outline = [str(item).strip() for item in final_report_outline if str(item).strip()]
    elif isinstance(final_report_outline, str) and final_report_outline.strip():
        normalized_report_outline = [line.strip() for line in final_report_outline.splitlines() if line.strip()]
    else:
        normalized_report_outline = []

    return {
        "steps": normalized_steps if include_steps else [],
        "tools_used": tools_used if include_steps else [],
        "conclusions": normalized_conclusions,
        "hypotheses": normalized_hypotheses,
        "action_items": action_items,
        "direct_answer": str(parsed.get("direct_answer", "") or "").strip(),
        "explanation": str(parsed.get("explanation", "")) or t("agent_explanation_default"),
        "final_report_outline": normalized_report_outline,
        "direct_report": "",
        "goal": str(parsed.get("goal", "") or "").strip(),
        "observation_focus": str(parsed.get("observation_focus", "") or "").strip(),
        "continue_reason": str(parsed.get("continue_reason", "") or "").strip(),
        "stop_if": str(parsed.get("stop_if", "") or "").strip(),
        "finalize": bool(parsed.get("finalize", False)),
        "question_type": str(parsed.get("question_type", "") or "").strip(),
        "needs_clarification": bool(parsed.get("needs_clarification", False)),
        "clarification": str(parsed.get("clarification", "") or "").strip(),
    }


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

    user_prompt = _build_iteration_user_prompt(message, sandbox, iteration_history, business_knowledge, config=config)



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



    yield {

        "type": "result",

        "data": _normalize_iteration_payload(parsed, include_steps=True),

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

            "direct_answer": t("agent_fallback_conclusion", table=table),

            "explanation": t("agent_explanation_mock"),

            "final_report_outline": [],

            "direct_report": "",

            "goal": "",

            "observation_focus": "",

            "continue_reason": "",

            "stop_if": "",

            "finalize": False,
            "question_type": "data_overview",
            "needs_clarification": False,
            "clarification": "",

        },

    }





def _summarize_execution_for_reflection(execution_result: dict) -> str:
    rows = execution_result.get("rows") or []
    chart_specs = execution_result.get("chart_specs") or []
    step_results = execution_result.get("step_results") or []
    exported_vars = execution_result.get("exported_vars") or {}
    lines = [
        f"rows_count={len(rows)}",
        f"charts_count={len(chart_specs)}",
    ]
    if execution_result.get("error"):
        lines.append(f"execution_error={execution_result.get('error')}")
    if isinstance(exported_vars, dict) and exported_vars:
        lines.append(f"exported_vars={json.dumps(exported_vars, ensure_ascii=False)}")
    if rows:
        lines.append(f"rows_preview={json.dumps(rows[:8], ensure_ascii=False)}")
    if chart_specs:
        chart_preview = []
        for spec in chart_specs[:6]:
            if not isinstance(spec, dict):
                continue
            chart_preview.append(
                {
                    "title": spec.get("title") or spec.get("chart_title") or "",
                    "type": spec.get("type") or spec.get("chart_type") or "",
                    "x": spec.get("x") or spec.get("x_field") or "",
                    "y": spec.get("y") or spec.get("y_field") or "",
                }
            )
        if chart_preview:
            lines.append(f"chart_preview={json.dumps(chart_preview, ensure_ascii=False)}")
    if step_results:
        compact_steps = []
        for index, step_result in enumerate(step_results[:10], start=1):
            if not isinstance(step_result, dict):
                continue
            step_rows = step_result.get("rows") or []
            columns = step_result.get("columns")
            if not columns:
                columns = []
                for row in step_rows[:3]:
                    if isinstance(row, dict):
                        for key in row.keys():
                            if key not in columns:
                                columns.append(key)
            compact_steps.append(
                {
                    "step": index,
                    "status": step_result.get("status") or ("error" if step_result.get("error") else "success"),
                    "rows_count": step_result.get("rows_count", len(step_rows)),
                    "tables": step_result.get("tables") or [],
                    "columns": columns[:12],
                    "chart_count": step_result.get("chart_count", len(step_result.get("chart_specs") or [])),
                    "warning": step_result.get("warning") or "",
                    "error": step_result.get("error") or "",
                    "result_digest": str(step_result.get("result_digest") or "")[:800],
                }
            )
        if compact_steps:
            lines.append(f"step_results={json.dumps(compact_steps, ensure_ascii=False)}")
    return "\n".join(lines)


def synthesize_iteration_result(
    *,
    message: str,
    sandbox: dict,
    iteration_history: list[dict],
    business_knowledge: list[str],
    planned_result: dict,
    execution_result: dict,
    incremental: bool = True,
    provider: str | None = None,
    model: str | None = None,
) -> dict:
    config = load_config()
    selected_provider = (provider or config.llm_provider).lower()
    if selected_provider not in {"openai", "anthropic"}:
        return {
            "steps": [],
            "tools_used": [],
            "conclusions": planned_result.get("conclusions", []),
            "hypotheses": planned_result.get("hypotheses", []),
            "action_items": planned_result.get("action_items", []),
            "direct_answer": planned_result.get("direct_answer", ""),
            "explanation": planned_result.get("explanation", ""),
            "final_report_outline": planned_result.get("final_report_outline", []),
            "direct_report": "",
            "goal": planned_result.get("goal", ""),
            "observation_focus": planned_result.get("observation_focus", ""),
            "continue_reason": planned_result.get("continue_reason", ""),
            "stop_if": planned_result.get("stop_if", ""),
            "finalize": planned_result.get("finalize", False),
            "question_type": planned_result.get("question_type", ""),
            "needs_clarification": planned_result.get("needs_clarification", False),
            "clarification": planned_result.get("clarification", ""),
        }

    is_en = get_lang() == "en"
    report_language = "English" if is_en else "简体中文"
    history_preview = []
    known_findings: list[str] = []
    for item in iteration_history[-3:]:
        history_preview.append(
            {
                "iteration_id": item.get("iteration_id"),
                "message": item.get("message"),
                "conclusions": item.get("conclusions", [])[:3],
            }
        )
        for conclusion in item.get("conclusions", [])[:5]:
            if isinstance(conclusion, dict):
                text = str(conclusion.get("text", "")).strip()
            else:
                text = str(conclusion).strip()
            if text and text not in known_findings:
                known_findings.append(text)
    system_prompt = get_prompt(config.prompts, "reflection_system")
    mode_instruction = (
        "- This is an incremental exploration round. Output only newly discovered findings from this round's execution evidence. Do not restate prior findings unless the new evidence changes, invalidates, or sharpens them.\n"
        "- If this round only confirms old findings and adds nothing new, keep conclusions/hypotheses/action_items minimal.\n"
        "- If two or more recent rounds have already covered the same metric/entity/route/topic, set finalize=true unless this round adds a clearly new dimension.\n"
        if incremental
        else "- This is the final synthesis stage. You may combine findings across rounds into a complete final answer.\n"
    )
    user_prompt = format_prompt(
        config.prompts,
        "reflection_user",
        message=message,
        business_knowledge=json.dumps(business_knowledge[:20], ensure_ascii=False),
        history_preview=json.dumps(history_preview, ensure_ascii=False),
        known_findings=json.dumps(known_findings[:12], ensure_ascii=False),
        planner_metadata=json.dumps(
            {
                "goal": planned_result.get("goal", ""),
                "observation_focus": planned_result.get("observation_focus", ""),
                "continue_reason": planned_result.get("continue_reason", ""),
                "stop_if": planned_result.get("stop_if", ""),
                "finalize": planned_result.get("finalize", False),
            },
            ensure_ascii=False,
        ),
        executed_steps=json.dumps(planned_result.get("steps", []), ensure_ascii=False),
        execution_evidence=_summarize_execution_for_reflection(execution_result),
        available_tables=json.dumps(sandbox.get("tables", [])[:20], ensure_ascii=False),
        report_language=report_language,
        mode_instruction=mode_instruction,
    )
    chunks = (
        _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        if selected_provider == "openai"
        else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
    )
    parsed = _parse_bundle_json("".join(chunks))
    normalized = _normalize_iteration_payload(parsed, include_steps=False)
    normalized["goal"] = normalized.get("goal") or planned_result.get("goal", "")
    normalized["observation_focus"] = normalized.get("observation_focus") or planned_result.get("observation_focus", "")
    normalized["continue_reason"] = normalized.get("continue_reason") or planned_result.get("continue_reason", "")
    normalized["stop_if"] = normalized.get("stop_if") or planned_result.get("stop_if", "")
    normalized["finalize"] = bool(parsed.get("finalize", planned_result.get("finalize", False)))
    return normalized


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
    system_prompt = get_prompt(config.prompts, "auto_report_system")
    user_prompt = format_prompt(
        config.prompts,
        "auto_report_user",
        message=message,
        stop_reason=stop_reason,
        knowledge_block=knowledge_block,
        rounds_summary=rounds_summary,
        report_language=report_language,
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
    summary_rounds = _build_loop_rounds_summary(loop_rounds)
    rows_preview = json.dumps(final_result_rows[:20], ensure_ascii=False)
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
    chart_ids = [f"chart_{idx}" for idx, spec in enumerate(chart_specs[:20], start=1) if isinstance(spec, dict)]
    chart_hint = ", ".join(chart_ids) if chart_ids else "none"
    chart_specs_block = json.dumps(
        [
            {"chart_id": f"chart_{idx}", "option": spec}
            for idx, spec in enumerate(chart_specs[:20], start=1)
            if isinstance(spec, dict)
        ],
        ensure_ascii=False,
    )[:20000]
    iteration_materials: list[dict] = []
    for round_payload in loop_rounds:
        result = round_payload.get("result") or {}
        execution = round_payload.get("execution") or {}
        iteration_materials.append(
            {
                "round": round_payload.get("round"),
                "focus": round_payload.get("prompt", "")[-1200:],
                "tools_used": result.get("tools_used", []),
                "steps": result.get("steps", []),
                "conclusions": result.get("conclusions", []),
                "hypotheses": result.get("hypotheses", []),
                "action_items": result.get("action_items", []),
                "explanation": result.get("explanation", ""),
                "rows_preview": (execution.get("rows") or [])[:8],
                "row_count": len(execution.get("rows") or []),
                "chart_count": len(execution.get("chart_specs") or []),
                "warnings": _extract_execution_warnings(execution),
                "error": round_payload.get("error") or execution.get("error") or "",
            }
        )
    iteration_materials_block = json.dumps(iteration_materials, ensure_ascii=False)[:30000]

    def _merge_loop_items(key: str, unique_key: str | None = None) -> list:
        output: list = []
        seen: set[str] = set()
        for round_payload in loop_rounds:
            for item in (round_payload.get("result") or {}).get(key, []):
                if unique_key and isinstance(item, dict):
                    marker = str(item.get(unique_key, "")).strip()
                else:
                    marker = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
                if marker and marker not in seen:
                    seen.add(marker)
                    output.append(item)
        return output

    def _extract_conclusions(value: dict) -> list[dict]:
        conclusions = value.get("conclusions", [])
        if not isinstance(conclusions, list):
            conclusions = [{"text": str(conclusions), "confidence": 0.5}]
        normalized: list[dict] = []
        for item in conclusions:
            if isinstance(item, dict):
                normalized.append(
                    {
                        "text": str(item.get("text", "")).strip(),
                        "confidence": float(item.get("confidence", 0.5)),
                    }
                )
            else:
                normalized.append({"text": str(item), "confidence": 0.5})
        return [item for item in normalized if item["text"]]

    def _extract_action_items(value: dict) -> list[str]:
        items = value.get("action_items", [])
        if not isinstance(items, list):
            items = [str(items)] if items else []
        return [str(item).strip() for item in items if str(item).strip()]

    if selected_provider not in {"openai", "anthropic"}:
        fallback_bundle["conclusions"] = _extract_conclusions({"conclusions": _merge_loop_items("conclusions", unique_key="text")})
        fallback_bundle["action_items"] = _extract_action_items({"action_items": _merge_loop_items("action_items")})
        return fallback_bundle

    stage1_markdown = generate_auto_analysis_report(
        message=message,
        loop_rounds=loop_rounds,
        business_knowledge=business_knowledge,
        stop_reason=stop_reason,
        provider=provider,
        model=model,
    ).strip()
    if not stage1_markdown:
        stage1_markdown = fallback_markdown

    stage1_title = default_title
    stage1_summary = (
        _strip_markdown_to_plain_text(str(fallback_bundle.get("summary", "") or ""))
        or _strip_markdown_to_plain_text(stage1_markdown)
    )[:500]
    stage1_bundle = {
        "title": stage1_title,
        "summary": stage1_summary,
        "conclusions": _extract_conclusions({"conclusions": _merge_loop_items("conclusions", unique_key="text")}),
        "action_items": _extract_action_items({"action_items": _merge_loop_items("action_items")}),
        "legacy_markdown": stage1_markdown or fallback_markdown,
    }
    if not stage1_bundle["summary"]:
        stage1_bundle["summary"] = str(fallback_bundle.get("summary", "") or "")
    if not stage1_bundle["conclusions"]:
        stage1_bundle["conclusions"] = _extract_conclusions({"conclusions": _merge_loop_items("conclusions", unique_key="text")})
    if not stage1_bundle["action_items"]:
        stage1_bundle["action_items"] = _extract_action_items({"action_items": _merge_loop_items("action_items")})

    stage2_system_prompt = get_prompt(config.prompts, "report_bundle_system")
    stage2_user_prompt = format_prompt(
        config.prompts,
        "report_bundle_user",
        draft_title=stage1_bundle["title"],
        draft_summary=stage1_bundle["summary"],
        conclusions=json.dumps(stage1_bundle["conclusions"], ensure_ascii=False),
        action_items=json.dumps(stage1_bundle["action_items"], ensure_ascii=False),
        message=message,
        stop_reason=stop_reason,
        rounds_completed=rounds_completed,
        knowledge_block=knowledge_block,
        patches_block=patches_block,
        history_block=history_block,
        summary_rounds=summary_rounds,
        iteration_materials_block=iteration_materials_block,
        rows_preview=rows_preview,
        report_language=report_language,
        chart_hint=chart_hint,
        chart_specs_block=chart_specs_block,
    )
    stage2_chunks = (
        _call_openai_protocol(system_prompt=stage2_system_prompt, user_prompt=stage2_user_prompt, model=model, config=config)
        if selected_provider == "openai"
        else _call_anthropic_protocol(system_prompt=stage2_system_prompt, user_prompt=stage2_user_prompt, model=model, config=config)
    )
    stage2_raw = "".join(stage2_chunks).strip()
    stage2_parsed = _parse_report_bundle_json(stage2_raw)

    combined_fallback = {
        **fallback_bundle,
        "title": stage1_bundle["title"],
        "summary": stage1_bundle["summary"],
        "conclusions": stage1_bundle["conclusions"],
        "action_items": stage1_bundle["action_items"],
        "legacy_markdown": fallback_markdown,
    }
    if not stage2_parsed:
        repaired_raw = _repair_report_bundle_json(
            raw_response=stage2_raw,
            fallback_markdown=stage1_bundle["legacy_markdown"],
            provider=selected_provider,
            model=model,
            config=config,
            report_language=report_language,
        )
        stage2_parsed = _parse_report_bundle_json(repaired_raw)
    if not stage2_parsed:
        repaired_html = _generate_html_document_by_llm(
            fallback_markdown=stage1_bundle["legacy_markdown"],
            report_context={
                "title": stage1_bundle["title"],
                "summary": stage1_bundle["summary"],
                "conclusions": stage1_bundle["conclusions"],
                "action_items": stage1_bundle["action_items"],
                "original_request": message,
                "stop_reason": stop_reason,
                "rounds_completed": rounds_completed,
                "loop_rounds_summary": summary_rounds,
                "structured_iteration_results": iteration_materials,
                "final_result_rows_preview": final_result_rows[:20],
                "previous_html_attempt": stage2_raw,
            },
            chart_specs=chart_specs,
            provider=selected_provider,
            model=model,
            config=config,
            report_language=report_language,
        )
        if repaired_html:
            stage2_parsed = {
                "title": stage1_bundle["title"],
                "summary": stage1_bundle["summary"],
                "html_document": repaired_html,
                "chart_bindings": [],
            }
        else:
            return combined_fallback

    raw_ai_html = str(stage2_parsed.get("html_document", "") or "").strip()
    stage2_bundle = _normalize_report_bundle(stage2_parsed, combined_fallback, chart_specs)
    stage2_bundle["title"] = str(stage2_bundle.get("title", "") or stage1_bundle["title"] or default_title)
    stage2_bundle["summary"] = str(stage2_bundle.get("summary", "") or stage1_bundle["summary"])
    stage2_bundle["conclusions"] = stage1_bundle["conclusions"]
    stage2_bundle["action_items"] = stage1_bundle["action_items"]
    stage2_bundle["legacy_markdown"] = stage1_bundle["legacy_markdown"] or fallback_markdown

    stage2_bundle["html_document"] = _ensure_chart_placeholders(
        stage2_bundle.get("html_document", ""),
        stage2_bundle.get("chart_bindings", []),
    )
    is_qualified = bool(raw_ai_html) and _is_polished_html_document(stage2_bundle.get("html_document", ""))
    if not is_qualified:
        for _ in range(2):
            repaired_html = _generate_html_document_by_llm(
                fallback_markdown=stage1_bundle["legacy_markdown"],
                report_context={
                    "title": stage2_bundle.get("title", ""),
                    "summary": stage2_bundle.get("summary", ""),
                    "conclusions": stage1_bundle["conclusions"],
                    "action_items": stage1_bundle["action_items"],
                    "original_request": message,
                    "stop_reason": stop_reason,
                    "rounds_completed": rounds_completed,
                    "loop_rounds_summary": summary_rounds,
                    "structured_iteration_results": iteration_materials,
                    "final_result_rows_preview": final_result_rows[:20],
                    "previous_html_attempt": raw_ai_html,
                },
                chart_specs=chart_specs,
                provider=selected_provider,
                model=model,
                config=config,
                report_language=report_language,
            )
            if not repaired_html:
                continue
            stage2_bundle["html_document"] = _ensure_chart_placeholders(
                repaired_html,
                stage2_bundle.get("chart_bindings", []),
            )
            is_qualified = _is_polished_html_document(stage2_bundle.get("html_document", ""))
            if is_qualified:
                break

    if not is_qualified:
        fallback_html = _build_polished_fallback_report_html(
            stage1_bundle["legacy_markdown"] or fallback_markdown,
            title=stage2_bundle.get("title", "") or default_title,
        )
        stage2_bundle["html_document"] = _ensure_chart_placeholders(
            fallback_html,
            stage2_bundle.get("chart_bindings", []),
        )
        is_qualified = _is_standalone_html_document(stage2_bundle.get("html_document", ""))

    if not _is_standalone_html_document(stage2_bundle.get("html_document", "")):
        stage2_bundle["html_document"] = _wrap_html_fragment_as_document(stage2_bundle.get("html_document", ""))
    stage2_bundle["html_document"] = _ensure_chart_placeholders(
        stage2_bundle.get("html_document", ""),
        stage2_bundle.get("chart_bindings", []),
    )
    return stage2_bundle


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


def _is_polished_html_document(text: str) -> bool:
    html_text = str(text or "").strip()
    if not _is_standalone_html_document(html_text):
        return False
    style_blocks = re.findall(r"<style[^>]*>([\s\S]*?)</style>", html_text, flags=re.IGNORECASE)
    style_text = "\n".join(style_blocks)
    if len(style_text.strip()) < 500:
        return False
    design_signals = (
        "display:grid",
        "display: grid",
        "display:flex",
        "display: flex",
        "box-shadow",
        "border-radius",
        "linear-gradient",
        "gap:",
        "grid-template",
        "backdrop-filter",
        "position:",
    )
    signal_count = sum(1 for token in design_signals if token in style_text.lower())
    if signal_count < 4:
        return False
    body_match = re.search(r"<body[^>]*>([\s\S]*?)</body>", html_text, flags=re.IGNORECASE)
    visible_html = body_match.group(1) if body_match else html_text
    if not re.search(r'class=["\'][^"\']+["\']', visible_html, flags=re.IGNORECASE):
        return False
    plain_doc_markers = (
        "<hr",
        "<h1>分析报告</h1>",
        "<h1>Analysis Report</h1>",
    )
    if any(marker.lower() in visible_html.lower() for marker in plain_doc_markers) and signal_count < 6:
        return False
    return True


def _html_contains_markdown_artifacts(text: str) -> bool:
    html_text = str(text or "")
    if not html_text.strip():
        return False
    visible = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", " ", html_text, flags=re.IGNORECASE)
    visible = re.sub(r"<[^>]+>", "\n", visible)
    patterns = (
        r"(^|\n)\s{0,3}#{1,6}\s+\S",
        r"\*\*[^*]+\*\*",
        r"```",
        r"(^|\n)\s*\|[^|\n]+\|[^|\n]*\n\s*\|?\s*:?-{3,}:?\s*\|",
    )
    return any(re.search(pattern, visible, flags=re.MULTILINE) for pattern in patterns)


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


def _strip_markdown_to_plain_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned_lines: list[str] = []
    for line in raw.splitlines():
        current = line.strip()
        if not current:
            continue
        current = re.sub(r"^\s{0,3}#{1,6}\s*", "", current)
        current = re.sub(r"^\s*[-*+]\s+", "", current)
        current = re.sub(r"^\s*\d+\.\s+", "", current)
        current = current.replace("|", " ")
        current = re.sub(r"\*\*(.*?)\*\*", r"\1", current)
        current = re.sub(r"__(.*?)__", r"\1", current)
        current = re.sub(r"`([^`]*)`", r"\1", current)
        current = re.sub(r"\s+", " ", current).strip()
        if current:
            cleaned_lines.append(current)
    return "\n".join(cleaned_lines).strip()


def _stringify_action_item(item) -> str:
    if item is None:
        return ""
    if isinstance(item, str):
        return item.strip()
    if not isinstance(item, dict):
        return str(item).strip()

    primary = (
        str(item.get("text", "") or item.get("action", "") or item.get("title", "") or "")
        .strip()
    )
    effect = str(item.get("expected_effect", "") or item.get("impact", "") or "").strip()
    owner = str(item.get("owner", "") or "").strip()
    priority = str(item.get("priority", "") or "").strip()

    parts: list[str] = []
    if primary:
        parts.append(primary)
    if effect:
        parts.append((t("label_expected_effect", default="预期效果") if get_lang() != "en" else "Expected effect") + f": {effect}")
    if owner:
        parts.append((t("label_owner", default="负责人") if get_lang() != "en" else "Owner") + f": {owner}")
    if priority:
        parts.append((t("label_priority", default="优先级") if get_lang() != "en" else "Priority") + f": {priority}")
    if parts:
        return " | ".join(parts)
    return json.dumps(item, ensure_ascii=False, sort_keys=True)


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
    system_prompt = get_prompt(config.prompts, "report_bundle_repair_system")
    user_prompt = format_prompt(
        config.prompts,
        "report_bundle_repair_user",
        report_language=report_language,
        raw_response=raw_response,
        fallback_markdown=fallback_markdown,
    )
    chunks = (
        _call_openai_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
        if provider == "openai"
        else _call_anthropic_protocol(system_prompt=system_prompt, user_prompt=user_prompt, model=model, config=config)
    )
    return "".join(chunks).strip()


def _generate_html_document_by_llm(
    fallback_markdown: str,
    report_context: dict | None,
    chart_specs: list[dict],
    provider: str,
    model: str | None,
    config: AppConfig,
    report_language: str,
) -> str:
    chart_ids = [f"chart_{idx}" for idx, spec in enumerate(chart_specs[:20], start=1) if isinstance(spec, dict)]
    chart_hint = ", ".join(chart_ids) if chart_ids else "none"
    chart_specs_block = json.dumps(
        [
            {"chart_id": f"chart_{idx}", "option": spec}
            for idx, spec in enumerate(chart_specs[:20], start=1)
            if isinstance(spec, dict)
        ],
        ensure_ascii=False,
    )[:20000]
    context_block = json.dumps(report_context or {}, ensure_ascii=False, default=str)[:30000]
    system_prompt = get_prompt(config.prompts, "html_report_system")
    user_prompt = format_prompt(
        config.prompts,
        "html_report_user",
        report_language=report_language,
        chart_hint=chart_hint,
        chart_specs_block=chart_specs_block,
        context_block=context_block,
        fallback_markdown=fallback_markdown,
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
    existing_ids = set(re.findall(r'data-chart-id=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE))
    missing_ids = [
        str(item.get("chart_id", "")).strip()
        for item in chart_bindings
        if isinstance(item, dict) and str(item.get("chart_id", "")).strip() and str(item.get("chart_id", "")).strip() not in existing_ids
    ]
    if not missing_ids:
        return html_text
    chart_placeholders = "".join(
        f'<div data-chart-id="{html.escape(chart_id)}" style="min-height:260px"></div>'
        for chart_id in missing_ids
    )
    if "</body>" in html_text.lower():
        return re.sub(r"</body>", chart_placeholders + "</body>", html_text, count=1, flags=re.IGNORECASE)
    return html_text + chart_placeholders


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

    if not normalized_bindings and html_document:
        used_chart_ids = set(re.findall(r'data-chart-id=["\']([^"\']+)["\']', html_document, flags=re.IGNORECASE))
        normalized_bindings = [
            {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
            for idx, spec in enumerate(chart_specs[:20], start=1)
            if isinstance(spec, dict) and f"chart_{idx}" in used_chart_ids
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
    chart_bindings = [
        {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
        for idx, spec in enumerate(chart_specs[:20], start=1)
        if isinstance(spec, dict)
    ]
    chart_slots = "".join(f'<div data-chart-id="chart_{idx}" style="min-height:260px"></div>' for idx, _ in enumerate(chart_bindings, start=1))
    title = "Analysis Report" if get_lang() == "en" else "\u5206\u6790\u62a5\u544a"
    html_document = _build_polished_fallback_report_html(safe_markdown, title=title, extra_blocks=chart_slots)
    return {
        "title": title,
        "summary": safe_markdown[:500],
        "html_document": html_document,
        "chart_bindings": chart_bindings,
        "legacy_markdown": safe_markdown,
    }


def _build_polished_fallback_report_html(markdown_text: str, title: str, extra_blocks: str = "") -> str:
    is_en = get_lang() == "en"
    html_lang = "en" if is_en else "zh-CN"
    safe_title = str(title or ("Analysis Report" if is_en else "\u5206\u6790\u62a5\u544a")).strip()
    report_title, summary, rendered = _build_polished_report_sections(markdown_text, safe_title)
    body_content = rendered or _render_markdown_like_html(str(markdown_text or "").strip()) or "<p></p>"
    summary_html = f"<p>{html.escape(summary)}</p>" if summary else ""
    eyebrow = "Autonomous Analysis" if is_en else "\u4e00\u952e\u81ea\u52a8\u5206\u6790"
    return (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{html.escape(report_title)}</title>"
        "<style>:root{color-scheme:light;--ink:#111827;--muted:#64748b;--line:#dbe4ef;--paper:#f6f8fb;--surface:#ffffff;--accent:#2563eb;--accent2:#14b8a6}"
        "*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;color:var(--ink);background:radial-gradient(circle at 18% 0%,rgba(37,99,235,.16),transparent 28%),linear-gradient(180deg,#f8fbff 0%,#edf2f7 100%);line-height:1.68}"
        ".report-shell{max-width:1180px;margin:0 auto;padding:34px 22px 58px}.report-hero{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(240px,.65fr);gap:26px;align-items:stretch;margin-bottom:24px}"
        ".hero-main{background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 58%,#0f766e 100%);color:#fff;border-radius:24px;padding:34px 36px;box-shadow:0 24px 70px rgba(15,23,42,.22);position:relative;overflow:hidden}"
        ".hero-main:after{content:\"\";position:absolute;right:-80px;bottom:-100px;width:260px;height:260px;border-radius:50%;background:rgba(255,255,255,.16)}.eyebrow{font-size:12px;text-transform:uppercase;letter-spacing:.12em;font-weight:800;color:#bfdbfe;margin-bottom:12px}"
        "h1{font-size:38px;line-height:1.16;margin:0;letter-spacing:0}.hero-main p{max-width:780px;color:#dbeafe;font-size:15px;margin:18px 0 0}.hero-side{display:grid;gap:14px}.hero-note{background:rgba(255,255,255,.84);border:1px solid rgba(148,163,184,.28);border-radius:20px;padding:22px;box-shadow:0 18px 48px rgba(15,23,42,.10)}"
        ".hero-note strong{display:block;font-size:13px;color:#2563eb;margin-bottom:8px}.hero-note span{display:block;color:var(--muted);font-size:14px}.content-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:start}"
        ".report-section{background:rgba(255,255,255,.92);border:1px solid rgba(148,163,184,.28);border-radius:18px;padding:24px 26px;box-shadow:0 18px 48px rgba(15,23,42,.08);min-width:0}.report-section:first-child{grid-column:1/-1}"
        ".section-index{display:inline-flex;align-items:center;justify-content:center;height:26px;min-width:34px;padding:0 10px;border-radius:999px;background:#dbeafe;color:#1d4ed8;font-size:12px;font-weight:900;margin-bottom:12px}.report-section h2{font-size:24px;line-height:1.25;margin:0 0 14px}.section-body{font-size:15px;color:#243042;overflow-wrap:anywhere}.section-body p{margin:10px 0}.section-body ul,.section-body ol{padding-left:22px;margin:10px 0}.section-body li{margin:7px 0}"
        ".section-body table{width:100%;table-layout:fixed;border-collapse:separate;border-spacing:0;margin:16px 0;border:1px solid var(--line);border-radius:14px;overflow:hidden;background:#fff}.section-body th,.section-body td{padding:12px 14px;border-bottom:1px solid #e7edf5;text-align:left;vertical-align:top;word-break:break-word}.section-body th{background:#f1f6fd;color:#0f172a;font-weight:850}.section-body tr:last-child td{border-bottom:0}"
        ".chart-zone{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;margin-top:18px}.chart-zone>[data-chart-id],.chart-zone>div{background:#fff;border:1px solid rgba(148,163,184,.28);border-radius:18px;box-shadow:0 18px 48px rgba(15,23,42,.08);padding:12px;min-height:320px}[data-chart-id]{width:100%;min-height:320px}"
        "@media(max-width:860px){.report-shell{padding:18px 12px 36px}.report-hero,.content-grid,.chart-zone{grid-template-columns:1fr}.hero-main{padding:28px 24px;border-radius:20px}h1{font-size:30px}.report-section{padding:20px}}@media print{body{background:#fff}.report-shell{max-width:none;padding:0}.hero-main,.hero-note,.report-section,.chart-zone>[data-chart-id],.chart-zone>div{box-shadow:none;break-inside:avoid-page}}</style>"
        "</head><body><main class=\"report-shell\">"
        f"<section class=\"report-hero\"><div class=\"hero-main\"><div class=\"eyebrow\">{eyebrow}</div><h1>{html.escape(report_title)}</h1>{summary_html}</div>"
        f"<aside class=\"hero-side\"><div class=\"hero-note\"><strong>{'Evidence-based' if is_en else '基于迭代证据'}</strong><span>{'Generated from completed analysis rounds and verified execution output.' if is_en else '根据已完成的分析轮次、执行结果和可用图表整理。'}</span></div>"
        f"<div class=\"hero-note\"><strong>{'Chart-ready' if is_en else '图表可挂载'}</strong><span>{'Visual placeholders remain available for host-rendered ECharts.' if is_en else '保留图表挂载点，由页面宿主渲染 ECharts。'}</span></div></aside></section>"
        f"<section class=\"content-grid\">{body_content}</section>"
        f"{f'<section class=\"chart-zone\">{extra_blocks}</section>' if extra_blocks else ''}"
        "</main></body></html>"
    )


def _build_minimal_report_html(markdown_text: str, title: str, extra_blocks: str = "") -> str:
    is_en = get_lang() == "en"
    html_lang = "en" if is_en else "zh-CN"
    safe_title = str(title or ("Analysis Report" if is_en else "\u5206\u6790\u62a5\u544a")).strip()
    body_html = _render_markdown_like_html(str(markdown_text or "").strip())
    if not body_html:
        body_html = "<p></p>"
    return (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{html.escape(safe_title)}</title>"
        "<style>*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;color:#111827;background:#fff;line-height:1.65}"
        "main{max-width:960px;margin:0 auto;padding:32px 20px}h1{font-size:28px;line-height:1.25;margin:0 0 20px}"
        "h2,h3{line-height:1.35;margin:24px 0 10px}p,ul,ol,table{margin:12px 0}table{width:100%;border-collapse:collapse}"
        "th,td{border:1px solid #e5e7eb;padding:8px 10px;text-align:left;vertical-align:top}th{background:#f9fafb}"
        "pre{white-space:pre-wrap;overflow-wrap:anywhere}code{background:#f3f4f6;padding:1px 4px;border-radius:4px}"
        "[data-chart-id]{width:100%;min-height:260px;margin:18px 0}</style>"
        f"</head><body><main><h1>{html.escape(safe_title)}</h1>{body_html}{extra_blocks}</main></body></html>"
    )


def _build_polished_report_sections(markdown_text: str, fallback_title: str) -> tuple[str, str, str]:
    lines = str(markdown_text or "").splitlines()
    title = fallback_title
    intro_lines: list[str] = []
    sections: list[tuple[int, str, list[str]]] = []
    current_heading = ""
    current_level = 2
    current_lines: list[str] = []

    def has_meaningful_content(raw_lines: list[str]) -> bool:
        placeholder_tokens = {"-", "--", "—", "*", "n/a", "none", "无", "暂无", "待补充", "待确认"}
        for line in raw_lines:
            cleaned = str(line or "").strip()
            if not cleaned:
                continue
            cleaned = re.sub(r"^\s{0,3}#{1,6}\s*", "", cleaned)
            cleaned = re.sub(r"^\s*\d+\.\s+", "", cleaned)
            cleaned = re.sub(r"^\s*[-*+]\s*", "", cleaned).strip()
            if not cleaned:
                continue
            normalized = re.sub(r"\s+", "", cleaned).lower()
            if normalized in placeholder_tokens:
                continue
            if re.fullmatch(r"[-_*]{3,}", normalized):
                continue
            return True
        return False

    def flush_current() -> None:
        nonlocal current_heading, current_lines, current_level
        if has_meaningful_content(current_lines):
            heading = current_heading or ("Overview" if get_lang() == "en" else "概览")
            sections.append((current_level, heading, current_lines))
        current_heading = ""
        current_level = 2
        current_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# "):
            if title == fallback_title:
                title = stripped[2:].strip() or title
            else:
                flush_current()
                current_heading = stripped[2:].strip()
                current_level = 2
            continue
        if stripped.startswith("## "):
            flush_current()
            current_heading = stripped[3:].strip()
            current_level = 2
            continue
        if stripped.startswith("### "):
            flush_current()
            current_heading = stripped[4:].strip()
            current_level = 3
            continue
        if current_heading or sections:
            current_lines.append(line)
        else:
            intro_lines.append(line)

    if has_meaningful_content(intro_lines):
        sections.insert(0, (2, "Overview" if get_lang() == "en" else "概览", intro_lines))
    flush_current()
    if not sections and str(markdown_text or "").strip():
        sections.append((2, "Overview" if get_lang() == "en" else "概览", lines))

    rendered_sections: list[str] = []
    for idx, (_, heading, body_lines) in enumerate(sections, start=1):
        body_html = _render_markdown_like_html("\n".join(body_lines).strip())
        if not body_html:
            body_html = "<p>-</p>"
        rendered_sections.append(
            '<section class="report-section">'
            f'<div class="section-index">{idx:02d}</div>'
            f"<h2>{html.escape(heading)}</h2>"
            f'<div class="section-body">{body_html}</div>'
            "</section>"
        )

    summary = _strip_markdown_to_plain_text(markdown_text).replace("\n", " ")
    summary = re.sub(r"(^|\s)-(?=\s|$)", " ", summary)
    summary = re.sub(r"\s{2,}", " ", summary).strip()
    return title, (summary[:240] if summary else ""), "\n".join(rendered_sections)


def _markdown_to_basic_html(markdown_text: str, extra_blocks: str = "") -> str:
    is_en = get_lang() == "en"
    html_lang = "en" if is_en else "zh-CN"
    report_title = "Auto Analysis Report" if is_en else "自动分析报告"
    report_title, summary, rendered = _build_polished_report_sections(markdown_text, report_title)
    eyebrow = "AI Analysis Report" if is_en else "AI 分析报告"
    summary_html = f"<p>{html.escape(summary)}</p>" if summary else ""
    return (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{html.escape(report_title)}</title>"
        "<style>:root{color-scheme:light}*{box-sizing:border-box}body{font-family:Inter,Arial,sans-serif;margin:0;background:#eef3f8;color:#0f172a}"
        ".report{max-width:1180px;margin:0 auto;padding:32px 24px 48px}.hero{background:#0f172a;color:#fff;border-radius:8px;padding:34px 38px;margin-bottom:22px;position:relative;overflow:hidden}"
        ".hero:after{content:\"\";position:absolute;inset:auto -90px -120px auto;width:260px;height:260px;border-radius:50%;background:rgba(14,165,233,.22)}"
        ".eyebrow{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#7dd3fc;font-weight:700;margin-bottom:10px}.hero h1{font-size:34px;line-height:1.25;margin:0;max-width:840px}.hero p{max-width:920px;color:#dbeafe;line-height:1.7;margin:14px 0 0}"
        ".report-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:start}.report-section{background:#fff;border:1px solid #dbe5ef;border-radius:8px;padding:22px 24px;box-shadow:0 12px 34px rgba(15,23,42,.08);position:relative;overflow:hidden;min-width:0;display:flex;flex-direction:column}"
        ".report-section:before{content:\"\";position:absolute;left:0;top:0;bottom:0;width:4px;background:#0ea5e9}.report-section:first-child{grid-column:1/-1}.section-index{font-size:12px;color:#0284c7;font-weight:800;margin-bottom:8px}.report-section h2{margin:0 0 14px;font-size:22px;line-height:1.35}"
        ".section-body{font-size:15px;line-height:1.75;color:#1f2937;overflow-wrap:anywhere;word-break:break-word}.section-body>*{max-width:100%}.section-body p{margin:10px 0}.section-body ul,.section-body ol{margin:10px 0 0 22px;padding:0}.section-body li{margin:7px 0}.section-body strong{color:#0f172a}.section-body code{padding:2px 6px;border-radius:5px;background:#e2e8f0;font-family:Consolas,monospace}.section-body pre{white-space:pre-wrap;overflow-wrap:anywhere}"
        ".section-body table{width:100%;table-layout:fixed;border-collapse:separate;border-spacing:0;margin:14px 0;font-size:13px;overflow:hidden;border:1px solid #dbe5ef;border-radius:8px}.section-body th,.section-body td{padding:10px 12px;text-align:left;vertical-align:top;border-bottom:1px solid #e2e8f0;word-break:break-word;overflow-wrap:anywhere}.section-body th{background:#f1f7fb;color:#0f172a;font-weight:800}.section-body tr:last-child td{border-bottom:0}.section-body img{max-width:100%;height:auto}"
        "section[data-chart-id],div[data-chart-id]{min-height:260px}.report>section{background:#fff;border:1px solid #dbe5ef;border-radius:8px;padding:22px 24px;margin-top:18px;box-shadow:0 12px 34px rgba(15,23,42,.08)}"
        "@media(max-width:860px){.report{padding:18px 12px 32px}.hero{padding:26px 22px}.hero h1{font-size:28px}.report-grid{grid-template-columns:1fr}}@media print{body{background:#fff}.report{max-width:none;padding:0}.report-grid{grid-template-columns:1fr!important;gap:12px}.report-section{break-inside:avoid-page;page-break-inside:avoid}.hero,.report-section,.report>section{box-shadow:none;border-color:#d7dee8}}</style>"
        "</head><body><main class=\"report\">"
        f"<header class=\"hero\"><div class=\"eyebrow\">{eyebrow}</div><h1>{html.escape(report_title)}</h1>{summary_html}</header>"
        f"<div class=\"report-grid\">{rendered}</div>"
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
    table_mode = False

    def close_list() -> None:
        nonlocal list_mode
        if list_mode == "ul":
            out.append("</ul>")
        elif list_mode == "ol":
            out.append("</ol>")
        list_mode = None

    def close_table() -> None:
        nonlocal table_mode
        if table_mode:
            out.append("</tbody></table>")
        table_mode = False

    def is_table_separator(line: str) -> bool:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        return len(cells) >= 2 and all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells)

    def parse_table_cells(line: str) -> list[str]:
        return [cell.strip() for cell in line.strip().strip("|").split("|")]

    index = 0
    while index < len(lines):
        line = lines[index].rstrip()
        stripped = line.strip()
        if stripped and "|" in stripped and index + 1 < len(lines) and is_table_separator(lines[index + 1]):
            close_list()
            close_table()
            headers = parse_table_cells(stripped)
            out.append("<table><thead><tr>" + "".join(f"<th>{inline_render(cell)}</th>" for cell in headers) + "</tr></thead><tbody>")
            table_mode = True
            index += 2
            while index < len(lines):
                row_text = lines[index].strip()
                if not row_text or "|" not in row_text or is_table_separator(row_text):
                    break
                cells = parse_table_cells(row_text)
                out.append("<tr>" + "".join(f"<td>{inline_render(cell)}</td>" for cell in cells) + "</tr>")
                index += 1
            close_table()
            continue
        if not stripped:
            close_list()
            close_table()
            index += 1
            continue
        if stripped == "---":
            close_list()
            close_table()
            out.append("<hr/>")
            index += 1
            continue
        if stripped.startswith("### "):
            close_list()
            close_table()
            out.append(f"<h3>{inline_render(stripped[4:])}</h3>")
            index += 1
            continue
        if stripped.startswith("## "):
            close_list()
            close_table()
            out.append(f"<h2>{inline_render(stripped[3:])}</h2>")
            index += 1
            continue
        if stripped.startswith("# "):
            close_list()
            close_table()
            out.append(f"<h1>{inline_render(stripped[2:])}</h1>")
            index += 1
            continue
        if re.match(r"^\d+\.\s+", stripped):
            close_table()
            if list_mode != "ol":
                close_list()
                out.append("<ol>")
                list_mode = "ol"
            item = re.sub(r"^\d+\.\s+", "", stripped)
            out.append(f"<li>{inline_render(item)}</li>")
            index += 1
            continue
        if stripped.startswith("- "):
            close_table()
            if list_mode != "ul":
                close_list()
                out.append("<ul>")
                list_mode = "ul"
            out.append(f"<li>{inline_render(stripped[2:])}</li>")
            index += 1
            continue
        close_list()
        close_table()
        out.append(f"<p>{inline_render(stripped)}</p>")
        index += 1

    close_list()
    close_table()
    return "\n".join(out)


def _sanitize_report_html(document: str) -> str:
    text = str(document or "")
    text = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<link\b[^>]*rel\s*=\s*(['\"]?)stylesheet\1[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"@import\s+url\([^)]+\)\s*;?", "", text, flags=re.IGNORECASE)
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


def _extract_execution_warnings(execution: dict) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    if not isinstance(execution, dict):
        return warnings
    for candidate in (execution.get("warning"), execution.get("template_warning")):
        text = str(candidate or "").strip()
        if text and text not in seen:
            seen.add(text)
            warnings.append(text)
    for step_result in execution.get("step_results", []) or []:
        if not isinstance(step_result, dict):
            continue
        for key in ("warning", "error"):
            text = str(step_result.get(key) or "").strip()
            if text and text not in seen:
                seen.add(text)
                warnings.append(text)
    return warnings


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

    config = load_config()
    system_prompt = get_prompt(config.prompts, "skill_proposal_system")

    steps = analysis_result.get('steps', [])
    if not steps and analysis_result.get("loop_rounds"):
        steps = []
        for round_payload in analysis_result.get("loop_rounds", []):
            for step in ((round_payload.get("result") or {}).get("steps") or []):
                if isinstance(step, dict):
                    steps.append(step)
    report_text = str(analysis_result.get("final_report_md", "")).strip()

    user_prompt = format_prompt(
        config.prompts,
        "skill_proposal_user",

        message=message,

        sandbox_name=sandbox_name,

        conclusions=json.dumps(analysis_result.get('conclusions', []), ensure_ascii=False),

        steps=json.dumps(steps, ensure_ascii=False),

        explanation=(analysis_result.get('explanation', '') or "") + (f"\n\nFinal Report:\n{report_text}" if report_text else "")

    )

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

