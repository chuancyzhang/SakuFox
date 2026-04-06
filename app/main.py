import html
import json
import io
import sqlite3
import re
import uuid
from pathlib import Path

import pandas as pd
from pydantic import BaseModel
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi import Request

from app.i18n import get_lang, set_lang, t

from app.agent import (
    run_analysis_iteration,
    synthesize_iteration_result,
    generate_auto_analysis_report,
    generate_auto_analysis_report_bundle,
    generate_data_insight,
    generate_skill_proposal,
)
from app.auth import get_current_user, login_with_ldap, login_with_oauth
from app.authorization import (
    assert_sandbox_access,
    get_accessible_sandboxes,
    get_accessible_tables,
)
from app.config import load_config, MAX_SELECTED_TABLES
from app.db_connections import DbConnectionConfig, execute_external_sql, get_engine, test_connection, get_table_names
from app.models import (
    FeedbackRequest,
    AutoAnalyzeRequest,
    IterateRequest,
    LoginRequest,
    SaveSkillRequest,
    UpdateSessionRequest,
    ProposeSkillRequest,
    UpdateSkillRequest,
    CreateSandboxRequest,
    RenameSandboxRequest,
    CreateKnowledgeBaseRequest,
    UpdateKnowledgeBaseRequest,
    MountKnowledgeBasesRequest,
    MountSkillsRequest,
)
from app.notebook_kernel import create_kernel, destroy_kernel
from app.python_sandbox import run_python_pipeline
from app.skills import list_skills, save_skill_from_proposal, build_context_snapshot_for_proposal
from app.tools import execute_select_sql_with_mask
from app.store import User, store

ITERATE_MAX_ROUNDS = 8

app = FastAPI(title=t("app_title", default="SakuFox 🦊 - 敏捷智能数据分析平台"))
web_dir = Path(__file__).resolve().parent.parent / "web"
app.mount("/web", StaticFiles(directory=str(web_dir)), name="web")


@app.middleware("http")
async def i18n_middleware(request: Request, call_next):
    lang = request.headers.get("X-Language", "zh")
    set_lang(lang)
    response = await call_next(request)
    return response


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(web_dir / "dashboard.html"))


@app.get("/dashboard")
def dashboard() -> FileResponse:
    return FileResponse(str(web_dir / "dashboard.html"))


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def _collect_business_knowledge(sandbox: dict, sandbox_id: str, session_patches: list[str] | None = None) -> list[str]:
    knowledge_items: list[str] = []

    for kb_id in sandbox.get("knowledge_bases", []):
        kb = store.get_knowledge_base(kb_id)
        if kb and kb.get("content"):
            knowledge_items.append(f"[{kb.get('name')}]: {kb.get('content')}")

    knowledge_items.extend(store.get_business_knowledge(sandbox_id))

    for skill_id in sandbox.get("mounted_skills", []):
        skill = store.skills.get(skill_id)
        if not skill:
            continue
        skill_name = skill.get("name") or skill_id
        knowledge_lines = ((skill.get("layers") or {}).get("knowledge") or [])
        for line in knowledge_lines:
            text = str(line).strip()
            if text:
                knowledge_items.append(f"[{skill_name}]: {text}")

    for patch in session_patches or []:
        text = str(patch).strip()
        if text:
            knowledge_items.append(f"[Session Patch]: {text}")

    return _dedupe_keep_order(knowledge_items)


def _build_iteration_message(
    original_message: str,
    round_index: int,
    previous_round: dict | None = None,
    *,
    mode: str = "auto",
    max_rounds: int | None = None,
) -> str:
    is_en = get_lang() == "en"
    mode_label = "one-click auto-analysis" if mode == "auto" else "iterative notebook analysis"
    round_budget = max_rounds or (100 if mode == "auto" else ITERATE_MAX_ROUNDS)
    if round_index <= 1 or previous_round is None:
        if is_en:
            return (
                f"{original_message}\n\n"
                f"You are in {mode_label} mode with a maximum of {round_budget} rounds. "
                "If you still need SQL or Python tools, output steps only and keep narrative analysis empty for this planning stage. "
                "If no more tool use is needed, output empty steps and provide direct_answer, final conclusions, action items, and a report outline. "
                "Never leave Python variables, f-string fragments, or placeholders such as {top_dept} in narrative text; all narrative text must contain final concrete values. "
                "Keep all narrative fields in English."
            )
        return (
            f"{original_message}\n\n"
            f"你处于{'一键自动分析' if mode == 'auto' else '交互式 notebook 分析'}模式，最多可进行 {round_budget} 轮。"
            "如果还需要 SQL 或 Python 工具，请只输出 steps，规划阶段不要提前输出分析结论。"
            "如果不再需要工具调用，请输出空 steps，并给出 direct_answer、最终结论、行动建议和报告提纲。"
            "叙述文本里绝不能保留 Python 变量名、f-string 片段或类似 {top_dept} 的占位符，必须写成最终的具体值。"
            "JSON 的字段名保持英文，但所有结论与说明文本必须使用简体中文。"
        )

    result = previous_round.get("result") or {}
    execution = previous_round.get("execution") or {}
    no_data_label = "none" if is_en else "无"
    conclusions = "; ".join(
        str(item.get("text", "")).strip()
        for item in (result.get("conclusions") or [])[:5]
        if isinstance(item, dict) and str(item.get("text", "")).strip()
    ) or no_data_label
    actions = "; ".join(str(item).strip() for item in (result.get("action_items") or [])[:5] if str(item).strip()) or no_data_label
    rows_count = len(execution.get("rows") or [])
    charts_count = len(execution.get("chart_specs") or [])
    error_text = execution.get("error") or previous_round.get("error") or no_data_label
    warning_items = _extract_execution_warnings(execution)
    warning_text = "; ".join(warning_items[:3]) if warning_items else no_data_label
    if is_en:
        return (
            f"{original_message}\n\n"
            f"This is {mode_label} round {round_index} of up to {round_budget}. Continue from the previous round.\n"
            f"Known findings from previous rounds (context only, do not restate): {conclusions}\n"
            f"Known actions from previous rounds (context only, do not restate): {actions}\n"
            f"Previous result rows: {rows_count}; charts: {charts_count}; error: {error_text}; warnings: {warning_text}\n"
            "If more tool calls are needed, output only SQL/Python steps for this planning stage. Explore a new angle or fix the current blocker. If analysis is sufficient, output empty steps, give a direct_answer, and finalize the conclusions. "
            "Do not repeat the same findings unless new evidence changes them.\n"
            "Never leave placeholders, Python variables, or f-string fragments like {metric_name} in narrative output; replace them with final concrete values.\n"
            "Keep all narrative fields in English."
        )
    return (
        f"{original_message}\n\n"
        f"当前是{'一键自动分析' if mode == 'auto' else '交互式 notebook 分析'}第 {round_index} 轮，最多 {round_budget} 轮，请延续上一轮继续分析。\n"
        f"上一轮已知发现（仅作上下文，不要重复输出）：{conclusions}\n"
        f"上一轮已知动作建议（仅作上下文，不要重复输出）：{actions}\n"
        f"上一轮结果行数：{rows_count}；图表数：{charts_count}；错误：{error_text}；告警：{warning_text}\n"
        "如果还需要工具调用，本阶段只输出 SQL/Python steps，不要提前输出分析结论；每一轮都应探索新的角度或修复当前阻塞。"
        "如果分析已充分，请输出空 steps、给出 direct_answer，并收敛为最终结论。"
        "不要在没有新增证据的情况下重复输出上一轮已经确认的结论。"
        "叙述文本里绝不能保留 Python 变量名、f-string 片段或类似 {metric_name} 的占位符，必须写成最终的具体值。"
        "JSON 的字段名保持英文，但所有结论与说明文本必须使用简体中文。"
    )


def _iter_notebook_rounds(
    *,
    message: str,
    analysis_sandbox: dict,
    sandbox: dict,
    session_id: str,
    sandbox_id: str,
    selected_tables: list[str],
    selected_files: list[str],
    iteration_history: list[dict],
    business_knowledge: list[str],
    provider: str | None,
    model: str | None,
    max_rounds: int,
    mode: str,
) -> tuple[list[dict], str, str]:
    loop_rounds: list[dict] = []
    loop_history = list(iteration_history)
    stop_reason = "model_stopped_using_tools"
    direct_report_md = ""

    for round_index in range(1, max_rounds + 1):
        round_message = _build_iteration_message(
            original_message=message,
            round_index=round_index,
            previous_round=loop_rounds[-1] if loop_rounds else None,
            mode=mode,
            max_rounds=max_rounds,
        )
        accumulated_thought = ""
        result_data = None
        for event in run_analysis_iteration(
            message=round_message,
            sandbox=analysis_sandbox,
            iteration_history=loop_history,
            business_knowledge=business_knowledge,
            provider=provider,
            model=model,
        ):
            if event.get("type") == "thought":
                accumulated_thought += event.get("content", "")
            elif event.get("type") == "result":
                result_data = event.get("data")

        if result_data is None:
            raise RuntimeError("analysis round returned no result")

        execution_result = {"rows": [], "tables": [], "chart_specs": [], "step_results": []}
        has_tool_calls = bool(result_data.get("steps"))
        direct_report_md = str(result_data.get("direct_report", "") or "").strip()
        if not has_tool_calls and round_index == 1 and not direct_report_md:
            bootstrap_steps = _build_bootstrap_auto_steps(selected_tables, selected_files)
            if bootstrap_steps:
                result_data = {
                    **result_data,
                    "steps": bootstrap_steps,
                    "tools_used": ["execute_select_sql"] if any(s.get("tool") == "sql" for s in bootstrap_steps) else ["python_interpreter"],
                    "explanation": "system bootstrap: first round had no tool plan; injected exploration steps",
                }
                has_tool_calls = True
        if has_tool_calls:
            execution_result = _execute_analysis_steps(
                result_data=result_data,
                sandbox=sandbox,
                selected_tables=selected_tables,
                selected_files=selected_files,
                sandbox_id=sandbox_id,
                session_id=session_id,
            )
            reflected_result = synthesize_iteration_result(
                message=message,
                sandbox=analysis_sandbox,
                iteration_history=loop_history,
                business_knowledge=business_knowledge,
                planned_result=result_data,
                execution_result=execution_result,
                incremental=True,
                provider=provider,
                model=model,
            )
            result_data = {
                **reflected_result,
                "steps": result_data.get("steps", []),
                "tools_used": result_data.get("tools_used", []),
                "goal": reflected_result.get("goal") or result_data.get("goal", ""),
                "observation_focus": reflected_result.get("observation_focus") or result_data.get("observation_focus", ""),
                "continue_reason": reflected_result.get("continue_reason") or result_data.get("continue_reason", ""),
                "stop_if": reflected_result.get("stop_if") or result_data.get("stop_if", ""),
                "finalize": bool(reflected_result.get("finalize", result_data.get("finalize", False))),
            }
            result_data = _hydrate_result_templates(result_data, execution_result)
        elif _is_json_parse_failure_result(result_data):
            result_data = {
                **result_data,
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "explanation": "model stopped without additional tool calls",
            }

        unresolved_placeholders = _contains_unresolved_placeholders(
            {
                "direct_answer": result_data.get("direct_answer"),
                "explanation": result_data.get("explanation"),
                "conclusions": result_data.get("conclusions"),
                "hypotheses": result_data.get("hypotheses"),
                "action_items": result_data.get("action_items"),
            }
        )
        if unresolved_placeholders:
            execution_result["template_warning"] = "unresolved_placeholders"
            if loop_rounds and not has_tool_calls and _has_meaningful_round_output(result_data, execution_result):
                stop_reason = "unresolved_placeholders"
                break

        round_payload = {
            "round": round_index,
            "prompt": round_message,
            "thought": accumulated_thought,
            "result": result_data,
            "execution": execution_result,
            "error": execution_result.get("error"),
        }
        if (
            loop_rounds
            and _extract_execution_warnings(execution_result)
            and _warning_loop_signature(round_payload) == _warning_loop_signature(loop_rounds[-1])
        ):
            stop_reason = "repeated_warning_loop"
            break
        if mode == "auto" and _is_repeated_topic_round(round_payload, loop_rounds):
            stop_reason = "repeated_topic"
            break
        if loop_rounds and _round_signature(round_payload) == _round_signature(loop_rounds[-1]):
            stop_reason = "repeated_round"
            break
        loop_rounds.append(round_payload)
        loop_history.append(_build_auto_history_entry(round_payload))
        yield round_payload

        if execution_result.get("error"):
            stop_reason = "execution_error"
            break
        if not has_tool_calls:
            stop_reason = "model_stopped_using_tools"
            break
        if round_index >= max_rounds:
            stop_reason = "max_rounds_reached"
            break

    return {
        "loop_rounds": loop_rounds,
        "stop_reason": stop_reason,
        "direct_report_md": direct_report_md,
    }


def _run_notebook_rounds(
    *,
    message: str,
    analysis_sandbox: dict,
    sandbox: dict,
    session_id: str,
    sandbox_id: str,
    selected_tables: list[str],
    selected_files: list[str],
    iteration_history: list[dict],
    business_knowledge: list[str],
    provider: str | None,
    model: str | None,
    max_rounds: int,
    mode: str,
) -> tuple[list[dict], str, str]:
    round_iter = _iter_notebook_rounds(
        message=message,
        analysis_sandbox=analysis_sandbox,
        sandbox=sandbox,
        session_id=session_id,
        sandbox_id=sandbox_id,
        selected_tables=selected_tables,
        selected_files=selected_files,
        iteration_history=iteration_history,
        business_knowledge=business_knowledge,
        provider=provider,
        model=model,
        max_rounds=max_rounds,
        mode=mode,
    )
    loop_rounds: list[dict] = []
    stop_reason = "model_stopped_using_tools"
    direct_report_md = ""
    while True:
        try:
            round_payload = next(round_iter)
            loop_rounds.append(round_payload)
        except StopIteration as stop:
            stop_value = stop.value if isinstance(stop.value, dict) else {}
            stop_reason = str(stop_value.get("stop_reason", stop_reason) or stop_reason)
            direct_report_md = str(stop_value.get("direct_report_md", "") or "")
            break
    return loop_rounds, stop_reason, direct_report_md


def _extract_execution_warnings(execution: dict) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    if not isinstance(execution, dict):
        return warnings
    top_level_candidates = [
        execution.get("warning"),
        execution.get("template_warning"),
    ]
    for candidate in top_level_candidates:
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


def _warning_loop_signature(round_payload: dict) -> str:
    result = round_payload.get("result") or {}
    execution = round_payload.get("execution") or {}
    steps = [
        {
            "tool": str(step.get("tool", "")).strip().lower(),
            "source": str(step.get("source", "")).strip().lower(),
            "code": _normalize_round_text(str(step.get("code", "")).strip()[:400]),
        }
        for step in (result.get("steps") or [])[:12]
        if isinstance(step, dict)
    ]
    warning_texts = [_normalize_round_text(item[:400]) for item in _extract_execution_warnings(execution)]
    payload = {
        "steps": steps,
        "warnings": warning_texts,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _normalize_topic_text(value: str) -> str:
    text = _normalize_round_text(value).lower()
    text = re.sub(r"\d+(?:\.\d+)?%?", "0", text)
    text = re.sub(r"[，。、“”‘’；：,.!?;:()\[\]{}<>《》|/\\\-+*=~`\"']", " ", text)
    tokens = [
        token
        for token in re.split(r"\s+", text)
        if len(token) >= 2 and token not in {"the", "and", "with", "from", "where", "select", "group", "order", "limit"}
    ]
    return " ".join(tokens[:80])


def _round_topic_signature(round_payload: dict) -> str:
    result = round_payload.get("result") or {}
    execution = round_payload.get("execution") or {}
    conclusion_text = " ".join(
        str(item.get("text", "")).strip()
        for item in (result.get("conclusions") or [])[:5]
        if isinstance(item, dict) and str(item.get("text", "")).strip()
    )
    action_text = " ".join(str(item).strip() for item in (result.get("action_items") or [])[:5] if str(item).strip())
    step_text = " ".join(
        str(step.get("code", "")).strip()[:500]
        for step in (result.get("steps") or [])[:6]
        if isinstance(step, dict)
    )
    tables_text = " ".join(str(item).strip() for item in (execution.get("tables") or [])[:10] if str(item).strip())
    chart_text = " ".join(
        " ".join(
            str(spec.get(key, "") or "").strip()
            for key in ("title", "chart_title", "type", "chart_type", "x", "x_field", "y", "y_field")
        )
        for spec in (execution.get("chart_specs") or [])[:5]
        if isinstance(spec, dict)
    )
    return _normalize_topic_text(" ".join([conclusion_text, action_text, step_text, tables_text, chart_text]))


def _is_repeated_topic_round(current_round: dict, previous_rounds: list[dict]) -> bool:
    if len(previous_rounds) < 2:
        return False
    current_signature = _round_topic_signature(current_round)
    if not current_signature:
        return False
    recent_signatures = [_round_topic_signature(item) for item in previous_rounds[-2:]]
    if any(not signature for signature in recent_signatures):
        return False
    if current_signature == recent_signatures[-1] == recent_signatures[-2]:
        return True

    current_tokens = set(current_signature.split())
    if len(current_tokens) < 8:
        return False
    overlaps = []
    for signature in recent_signatures:
        tokens = set(signature.split())
        if not tokens:
            return False
        overlaps.append(len(current_tokens & tokens) / max(1, len(current_tokens | tokens)))
    return min(overlaps) >= 0.78


def _build_auto_history_entry(round_payload: dict) -> dict:
    result = round_payload.get("result") or {}
    execution = round_payload.get("execution") or {}
    rows_count = len(execution.get("rows") or [])
    return {
        "iteration_id": f"auto_round_{round_payload.get('round', '?')}",
        "message": f"Auto round {round_payload.get('round', '?')} rows={rows_count}",
        "conclusions": result.get("conclusions", []),
        "hypotheses": result.get("hypotheses", []),
        "warnings": _extract_execution_warnings(execution),
    }


def _round_signature(round_payload: dict) -> str:
    result = round_payload.get("result") or {}
    execution = round_payload.get("execution") or {}

    def _row_preview(row: dict) -> dict:
        if not isinstance(row, dict):
            return {"value": str(row)[:120]}
        preview = {}
        for key in sorted(row.keys())[:8]:
            value = row.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                preview[key] = value
            else:
                preview[key] = str(value)[:120]
        return preview

    conclusions = [
        {
            "text": _normalize_round_text(item.get("text", "")),
            "confidence": item.get("confidence"),
        }
        for item in (result.get("conclusions") or [])[:8]
        if isinstance(item, dict) and str(item.get("text", "")).strip()
    ]
    hypotheses = [
        {
            "text": _normalize_round_text(item.get("text", "")),
            "id": str(item.get("id", "")).strip(),
        }
        for item in (result.get("hypotheses") or [])[:8]
        if isinstance(item, dict) and str(item.get("text", "")).strip()
    ]
    actions = [_normalize_round_text(item) for item in (result.get("action_items") or [])[:8] if str(item).strip()]
    steps = [
        {
            "tool": str(step.get("tool", "")).strip().lower(),
            "source": str(step.get("source", "")).strip().lower(),
            "code": _normalize_round_text(str(step.get("code", "")).strip()[:400]),
        }
        for step in (result.get("steps") or [])[:12]
        if isinstance(step, dict)
    ]
    chart_specs = []
    for spec in (execution.get("chart_specs") or [])[:10]:
        if not isinstance(spec, dict):
            chart_specs.append(str(spec)[:200])
            continue
        chart_specs.append({
            "title": str(spec.get("title", "") or spec.get("chart_title", "") or "").strip(),
            "type": str(spec.get("type", "") or spec.get("chart_type", "") or "").strip(),
            "x": str(spec.get("x", "") or spec.get("x_field", "") or "").strip(),
            "y": str(spec.get("y", "") or spec.get("y_field", "") or "").strip(),
        })
    rows = [
        _row_preview(row)
        for row in (execution.get("rows") or [])[:5]
        if row is not None
    ]
    payload = {
        "direct_answer": _normalize_round_text(str(result.get("direct_answer", "") or "").strip()[:240]),
        "explanation": _normalize_round_text(str(result.get("explanation", "") or "").strip()[:500]),
        "direct_report": _normalize_round_text(str(result.get("direct_report", "") or "").strip()[:500]),
        "tools_used": [str(item).strip() for item in (result.get("tools_used") or [])[:8] if str(item).strip()],
        "conclusions": conclusions,
        "hypotheses": hypotheses,
        "action_items": actions,
        "steps": steps,
        "rows_count": len(execution.get("rows") or []),
        "rows_preview": rows,
        "tables": [str(item).strip() for item in (execution.get("tables") or [])[:20] if str(item).strip()],
        "chart_specs": chart_specs,
        "warnings": [_normalize_round_text(item[:400]) for item in _extract_execution_warnings(execution)],
        "template_warning": str(execution.get("template_warning", "") or "").strip(),
        "error": str(execution.get("error") or round_payload.get("error") or "").strip()[:400],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _build_iteration_context_history(iterations: list[dict]) -> list[dict]:
    context: list[dict] = []
    for it in iterations:
        report_meta = it.get("report_meta", {}) or {}
        context.append(
            {
                "iteration_id": it.get("iteration_id"),
                "mode": it.get("mode", "manual"),
                "message": str(it.get("message", "") or "")[:500],
                "conclusions": it.get("conclusions", []) or [],
                "hypotheses": it.get("hypotheses", []) or [],
                "report_title": str(it.get("report_title", "") or "")[:200],
                "final_report_summary": str(it.get("final_report_summary", "") or "")[:1200],
                "report_meta": {
                    "stop_reason": report_meta.get("stop_reason"),
                    "rounds_completed": report_meta.get("rounds_completed"),
                    "max_rounds_hit": report_meta.get("max_rounds_hit"),
                },
            }
        )
    return context


def _build_default_auto_seed_message(selected_tables: list[str], selected_files: list[str]) -> str:
    is_en = get_lang() == "en"
    table_text = ", ".join(selected_tables[:8]) if selected_tables else ("current sandbox tables" if is_en else "当前沙盒可用表")
    file_text = ", ".join(selected_files[:8]) if selected_files else ("selected uploaded files if available" if is_en else "已选择的上传文件")
    if is_en:
        return (
            "Run one-click autonomous analysis for the currently selected data assets. "
            "Start with data profiling and quality checks, then detect anomalies and latent patterns, "
            "validate key findings with SQL/Python evidence, and conclude with prioritized actionable recommendations. "
            f"Priority tables: {table_text}. Priority files: {file_text}."
        )
    return (
        "请对当前选中的数据资产执行一键自动分析："
        "先做数据概览与质量评估，再识别异常与潜在模式，"
        "用 SQL/Python 证据验证关键发现，最后给出可执行且有优先级的行动建议。"
        f"优先表：{table_text}。优先文件：{file_text}。"
    )


def _build_iteration_report_url(iteration_id: str) -> str:
    return f"/web/report.html?iteration_id={iteration_id}"


def _localize_html_bundle_runtime_error(raw_message: str) -> str:
    text = str(raw_message or "")
    if text.startswith("AI failed to generate qualified HTML report after"):
        if get_lang() == "en":
            return (
                "AI failed to generate a qualified HTML report after 3 retries. "
                "Try refining your request context and run one-click analysis again."
            )
        return "AI 连续 3 次都未生成合格的 HTML 报告，请补充更明确的上下文后重试。"
    return text


def _build_report_bundle_from_markdown(markdown_text: str, chart_specs: list[dict]) -> dict:
    is_en = get_lang() == "en"
    default_title = "Analysis Report" if is_en else "分析报告"
    html_lang = "en" if is_en else "zh-CN"
    chart_title = "Chart" if is_en else "图表"
    safe_md = str(markdown_text or "").strip()
    report_title, summary, rendered = _build_polished_report_sections(safe_md, default_title)
    eyebrow = "AI Analysis Report" if is_en else "AI 分析报告"
    summary_html = f"<p>{html.escape(summary)}</p>" if summary else ""
    chart_bindings = [
        {"chart_id": f"chart_{idx}", "option": spec, "height": 360}
        for idx, spec in enumerate(chart_specs[:20], start=1)
        if isinstance(spec, dict)
    ]
    chart_slots = "".join(
        f'<section style="margin-top:18px;"><h2 style="margin:0 0 8px;">{chart_title} {idx}</h2><div data-chart-id="chart_{idx}"></div></section>'
        for idx, _ in enumerate(chart_bindings, start=1)
    )
    html_doc = (
        f"<!doctype html><html lang=\"{html_lang}\"><head><meta charset=\"UTF-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
        f"<title>{html.escape(report_title)}</title>"
        "<style>:root{color-scheme:light}*{box-sizing:border-box}body{font-family:Inter,Arial,sans-serif;margin:0;background:#eef3f8;color:#0f172a}"
        ".report{max-width:1180px;margin:0 auto;padding:32px 24px 48px}.hero{background:#0f172a;color:#fff;border-radius:8px;padding:34px 38px;margin-bottom:22px;position:relative;overflow:hidden}"
        ".hero:after{content:\"\";position:absolute;inset:auto -90px -120px auto;width:260px;height:260px;border-radius:50%;background:rgba(14,165,233,.22)}"
        ".eyebrow{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#7dd3fc;font-weight:700;margin-bottom:10px}.hero h1{font-size:34px;line-height:1.25;margin:0;max-width:840px}.hero p{max-width:920px;color:#dbeafe;line-height:1.7;margin:14px 0 0}"
        ".report-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.report-section{background:#fff;border:1px solid #dbe5ef;border-radius:8px;padding:22px 24px;box-shadow:0 12px 34px rgba(15,23,42,.08);position:relative;overflow:hidden}"
        ".report-section:before{content:\"\";position:absolute;left:0;top:0;bottom:0;width:4px;background:#0ea5e9}.report-section:first-child{grid-column:1/-1}.section-index{font-size:12px;color:#0284c7;font-weight:800;margin-bottom:8px}.report-section h2{margin:0 0 14px;font-size:22px;line-height:1.35}"
        ".section-body{font-size:15px;line-height:1.75;color:#1f2937}.section-body p{margin:10px 0}.section-body ul,.section-body ol{margin:10px 0 0 22px;padding:0}.section-body li{margin:7px 0}.section-body strong{color:#0f172a}.section-body code{padding:2px 6px;border-radius:5px;background:#e2e8f0;font-family:Consolas,monospace}"
        ".section-body table{width:100%;border-collapse:separate;border-spacing:0;margin:14px 0;font-size:13px;overflow:hidden;border:1px solid #dbe5ef;border-radius:8px}.section-body th,.section-body td{padding:10px 12px;text-align:left;vertical-align:top;border-bottom:1px solid #e2e8f0}.section-body th{background:#f1f7fb;color:#0f172a;font-weight:800}.section-body tr:last-child td{border-bottom:0}"
        "section[data-chart-id],div[data-chart-id]{min-height:260px}.report>section{background:#fff;border:1px solid #dbe5ef;border-radius:8px;padding:22px 24px;margin-top:18px;box-shadow:0 12px 34px rgba(15,23,42,.08)}"
        "@media(max-width:860px){.report{padding:18px 12px 32px}.hero{padding:26px 22px}.hero h1{font-size:28px}.report-grid{grid-template-columns:1fr}}@media print{body{background:#fff}.report{max-width:none;padding:0}.hero,.report-section,.report>section{box-shadow:none;border-color:#d7dee8}}</style>"
        "</head><body><main class=\"report\">"
        f"<header class=\"hero\"><div class=\"eyebrow\">{eyebrow}</div><h1>{html.escape(report_title)}</h1>{summary_html}</header>"
        f"<div class=\"report-grid\">{rendered}</div>"
        f"{chart_slots}"
        "</main></body></html>"
    )
    return {
        "title": report_title,
        "summary": (summary or safe_md[:500]),
        "html_document": html_doc,
        "chart_bindings": chart_bindings,
        "legacy_markdown": safe_md,
    }


def _strip_markdown_to_plain_text(markdown_text: str) -> str:
    raw = str(markdown_text or "").strip()
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


def _build_polished_report_sections(markdown_text: str, fallback_title: str) -> tuple[str, str, str]:
    lines = str(markdown_text or "").splitlines()
    title = fallback_title
    intro_lines: list[str] = []
    sections: list[tuple[int, str, list[str]]] = []
    current_heading = ""
    current_level = 2
    current_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_heading, current_lines, current_level
        if current_heading or any(line.strip() for line in current_lines):
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

    if intro_lines:
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
    return title, (summary[:240] if summary else ""), "\n".join(rendered_sections)


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
    total = len(lines)
    while index < total:
        raw = lines[index]
        line = raw.rstrip()
        stripped = line.strip()
        if stripped and "|" in stripped and index + 1 < total and is_table_separator(lines[index + 1]):
            close_list()
            close_table()
            headers = parse_table_cells(stripped)
            out.append("<table><thead><tr>" + "".join(f"<th>{inline_render(cell)}</th>" for cell in headers) + "</tr></thead><tbody>")
            table_mode = True
            index += 2
            while index < total:
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


def _extract_html_document_from_report_text(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    normalized = re.sub(r"^```(?:json|html)?\s*", "", text, flags=re.IGNORECASE)
    normalized = re.sub(r"\s*```$", "", normalized, flags=re.IGNORECASE).strip()

    def parse_candidate(candidate: str) -> str:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return ""
        if not isinstance(parsed, dict):
            return ""
        html_doc = str(parsed.get("html_document", "") or "").strip()
        if not html_doc:
            return ""
        match = re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", html_doc, flags=re.IGNORECASE)
        return (match.group(0) if match else html_doc).strip()

    parsed_html = parse_candidate(normalized)
    if parsed_html:
        return parsed_html

    first_brace = normalized.find("{")
    last_brace = normalized.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        parsed_html = parse_candidate(normalized[first_brace:last_brace + 1])
        if parsed_html:
            return parsed_html

    field_match = re.search(
        r'"html_document"\s*:\s*"([\s\S]*?)"\s*(?:,\s*"chart_bindings"|,\s*"summary"|,\s*"title"|,\s*"legacy_markdown"|\})',
        normalized,
        flags=re.IGNORECASE,
    )
    if field_match:
        raw_value = field_match.group(1)
        try:
            decoded = json.loads(f'"{raw_value}"')
        except json.JSONDecodeError:
            decoded = raw_value
        decoded_text = str(decoded).strip()
        match = re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", decoded_text, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip()

    html_match = re.search(r"<!doctype html[\s\S]*?</html>|<html[\s\S]*?</html>", normalized, flags=re.IGNORECASE)
    if html_match:
        return html_match.group(0).strip()
    return ""


def _normalize_auto_report_bundle(report_bundle: dict, chart_specs: list[dict]) -> dict:
    normalized = dict(report_bundle or {})
    raw_html = str(normalized.get("html_document", "") or "").strip()
    html_document = _extract_html_document_from_report_text(raw_html)

    fallback_markdown = str(normalized.get("legacy_markdown", "") or "").strip()
    if not fallback_markdown and raw_html and "<html" not in raw_html.lower():
        fallback_markdown = raw_html
    if not fallback_markdown:
        fallback_markdown = str(normalized.get("summary", "") or "").strip()
    fallback_bundle = _build_report_bundle_from_markdown(fallback_markdown, chart_specs)

    if not html_document:
        html_document = str(fallback_bundle.get("html_document", "") or "")
    if "<html" not in html_document.lower():
        html_document = str(fallback_bundle.get("html_document", "") or "")

    normalized["html_document"] = html_document
    normalized["title"] = str(normalized.get("title", "") or str(fallback_bundle.get("title", "")))
    normalized["summary"] = str(normalized.get("summary", "") or str(fallback_bundle.get("summary", "")))[:500]
    normalized["legacy_markdown"] = str(normalized.get("legacy_markdown", "") or str(fallback_bundle.get("legacy_markdown", "")))
    chart_bindings = normalized.get("chart_bindings")
    if not isinstance(chart_bindings, list):
        chart_bindings = list(fallback_bundle.get("chart_bindings", []))
    normalized["chart_bindings"] = chart_bindings
    normalized["html_document"] = _ensure_chart_placeholders_in_report_html(normalized["html_document"], chart_bindings)
    return normalized


def _ensure_chart_placeholders_in_report_html(html_document: str, chart_bindings: list[dict]) -> str:
    html_text = str(html_document or "")
    if not html_text or not chart_bindings:
        return html_text
    existing_ids = set(re.findall(r'data-chart-id=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE))
    missing_ids = [
        str(item.get("chart_id", "")).strip()
        for item in chart_bindings
        if isinstance(item, dict)
        and str(item.get("chart_id", "")).strip()
        and str(item.get("chart_id", "")).strip() not in existing_ids
    ]
    if not missing_ids:
        return html_text

    is_en = get_lang() == "en"
    chart_label = "Chart" if is_en else "图表"
    charts_label = "Charts" if is_en else "图表"
    section_items = "".join(
        (
            f'<section style="margin-top:18px;">'
            f'<h3 style="margin:0 0 8px;">{chart_label} {idx}</h3>'
            f'<div data-chart-id="{html.escape(chart_id)}"></div>'
            "</section>"
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


def _build_skill_proposal_fallback(
    proposal: dict,
    requested_message: str,
    sandbox_name: str,
    suggestion: dict,
) -> dict:
    is_en = get_lang() == "en"
    sanitized = suggestion if isinstance(suggestion, dict) else {}
    output = {
        "name": str(sanitized.get("name") or "").strip(),
        "description": str(sanitized.get("description") or "").strip(),
        "tags": sanitized.get("tags") if isinstance(sanitized.get("tags"), list) else [],
        "knowledge": sanitized.get("knowledge") if isinstance(sanitized.get("knowledge"), list) else [],
    }

    message = str(requested_message or proposal.get("message") or "").strip()
    report_title = str(proposal.get("report_title") or "").strip()
    report_summary = str(proposal.get("final_report_summary") or "").strip()
    explanation = str(proposal.get("explanation") or "").strip()
    final_report_md = str(proposal.get("final_report_md") or "").strip()

    if not output["name"]:
        if report_title:
            output["name"] = report_title[:80]
        elif message:
            output["name"] = message[:50]
        else:
            output["name"] = "Auto Analysis Skill" if is_en else "自动分析经验"

    if not output["description"]:
        base_desc = report_summary or explanation or final_report_md[:500]
        if base_desc:
            output["description"] = base_desc
        else:
            output["description"] = (
                f"Reusable analysis skill distilled from sandbox {sandbox_name}."
                if is_en
                else f"从沙盒「{sandbox_name}」提炼的可复用分析经验。"
            )

    if not output["tags"]:
        tags: list[str] = []
        for table_name in (proposal.get("selected_tables") or []):
            text = str(table_name).strip()
            if text and text not in tags:
                tags.append(text)
            if len(tags) >= 4:
                break
        mode = str(proposal.get("mode") or "").strip()
        if mode and mode not in tags:
            tags.append(mode)
        output["tags"] = tags

    if not output["knowledge"]:
        knowledge_lines: list[str] = []
        for item in (proposal.get("conclusions") or [])[:5]:
            if isinstance(item, dict):
                text = str(item.get("text") or "").strip()
            else:
                text = str(item or "").strip()
            if text:
                knowledge_lines.append(text)
        for item in (proposal.get("action_items") or [])[:5]:
            text = str(item or "").strip()
            if text:
                knowledge_lines.append(text)
        if report_summary:
            knowledge_lines.append(report_summary)
        if not knowledge_lines and final_report_md:
            for line in final_report_md.splitlines():
                text = str(line).strip(" -#\t")
                if text:
                    knowledge_lines.append(text)
                if len(knowledge_lines) >= 8:
                    break
        deduped: list[str] = []
        seen: set[str] = set()
        for line in knowledge_lines:
            if line in seen:
                continue
            seen.add(line)
            deduped.append(line)
        output["knowledge"] = deduped

    return output


_PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([^}]+))?\}")


def _build_result_placeholder_context(execution_result: dict) -> dict[str, object]:
    context: dict[str, object] = {}
    rows = execution_result.get("rows") or []
    if rows and isinstance(rows[0], dict):
        first_row = rows[0]
        for key, value in first_row.items():
            if value is None:
                continue
            key_str = str(key)
            context[key_str] = value
            context[key_str.lower()] = value
    exported_vars = execution_result.get("exported_vars") or {}
    if isinstance(exported_vars, dict):
        for key, value in exported_vars.items():
            if value is None:
                continue
            key_str = str(key)
            context[key_str] = value
            context[key_str.lower()] = value
    context["rows_count"] = len(rows)
    context["row_count"] = len(rows)
    return context


def _resolve_template_placeholders(value, context: dict[str, object]):
    if isinstance(value, str):
        def replace(match: re.Match) -> str:
            raw_key = match.group(1)
            fmt = match.group(2)
            lookup_key = raw_key if raw_key in context else raw_key.lower()
            if lookup_key not in context:
                return match.group(0)
            resolved = context[lookup_key]
            if fmt:
                try:
                    return format(resolved, fmt)
                except Exception:
                    return str(resolved)
            return str(resolved)

        return _PLACEHOLDER_PATTERN.sub(replace, value)
    if isinstance(value, list):
        return [_resolve_template_placeholders(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_template_placeholders(val, context) for key, val in value.items()}
    return value


def _hydrate_result_templates(result_data: dict, execution_result: dict) -> dict:
    if not result_data:
        return result_data
    context = _build_result_placeholder_context(execution_result)
    if not context:
        return result_data
    return _resolve_template_placeholders(result_data, context)


def _contains_unresolved_placeholders(value) -> bool:
    if isinstance(value, str):
        return bool(_PLACEHOLDER_PATTERN.search(value))
    if isinstance(value, list):
        return any(_contains_unresolved_placeholders(item) for item in value)
    if isinstance(value, dict):
        return any(_contains_unresolved_placeholders(item) for item in value.values())
    return False


def _normalize_round_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = _PLACEHOLDER_PATTERN.sub("{placeholder}", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _has_meaningful_round_output(result_data: dict, execution_result: dict) -> bool:
    if not isinstance(result_data, dict):
        return False
    if str(result_data.get("direct_answer", "") or "").strip():
        return True
    if any(str(item).strip() for item in (result_data.get("action_items") or [])):
        return True
    if any(isinstance(item, dict) and str(item.get("text", "")).strip() for item in (result_data.get("conclusions") or [])):
        return True
    if any(isinstance(item, dict) and str(item.get("text", "")).strip() for item in (result_data.get("hypotheses") or [])):
        return True
    if execution_result.get("rows") or execution_result.get("chart_specs"):
        return True
    return False


def _build_bootstrap_auto_steps(selected_tables: list[str], selected_files: list[str]) -> list[dict]:
    steps: list[dict] = []
    for table_name in selected_tables[:3]:
        tbl = str(table_name).strip()
        if not tbl:
            continue
        steps.append({"tool": "sql", "code": f"SELECT * FROM {tbl} LIMIT 200"})
        steps.append({"tool": "sql", "code": f"SELECT COUNT(*) AS row_count FROM {tbl}"})
    if not steps and selected_files:
        # Let the model-driven python pipeline inspect selected local files when no table is chosen.
        steps.append(
            {
                "tool": "python",
                "code": "print('Bootstrap file exploration enabled by system fallback.')",
            }
        )
    return steps


def _merge_tools_used(loop_rounds: list[dict]) -> list[str]:
    tools: list[str] = []
    for round_payload in loop_rounds:
        for tool in (round_payload.get("result") or {}).get("tools_used", []):
            if tool not in tools:
                tools.append(tool)
    return tools


def _flatten_loop_steps(loop_rounds: list[dict]) -> list[dict]:
    steps: list[dict] = []
    for round_payload in loop_rounds:
        for step in (round_payload.get("result") or {}).get("steps", []):
            if isinstance(step, dict):
                steps.append({"tool": step.get("tool", ""), "code": step.get("code", "")})
    return steps


def _merge_structured_items(loop_rounds: list[dict], key: str, unique_key: str | None = None) -> list:
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


def _collect_all_charts(loop_rounds: list[dict]) -> list[dict]:
    charts: list[dict] = []
    for round_payload in loop_rounds:
        charts.extend((round_payload.get("execution") or {}).get("chart_specs", []))
    return charts


def _get_last_result_rows(loop_rounds: list[dict]) -> list[dict]:
    for round_payload in reversed(loop_rounds):
        rows = (round_payload.get("execution") or {}).get("rows", [])
        if rows:
            return rows
    return []


def _is_json_parse_failure_result(result_data: dict) -> bool:
    if result_data.get("steps"):
        return False
    conclusions = result_data.get("conclusions") or []
    for item in conclusions:
        text = item.get("text", "") if isinstance(item, dict) else str(item)
        if "json" in str(text).lower():
            return True
    return False


def _build_auto_iteration_payload(
    message: str,
    session_id: str,
    sandbox_id: str,
    selected_tables: list[str],
    session: dict,
    loop_rounds: list[dict],
    report_bundle: dict,
    stop_reason: str,
    max_rounds: int,
) -> dict:
    max_rounds_hit = stop_reason == "max_rounds_reached"
    report_title = str(report_bundle.get("title", "") or "")
    final_report_summary = str(report_bundle.get("summary", "") or "")
    final_report_html = str(report_bundle.get("html_document", "") or "")
    final_report_chart_bindings = report_bundle.get("chart_bindings", []) or []
    final_report_md = str(report_bundle.get("legacy_markdown", "") or "")
    return {
        "mode": "auto_analysis",
        "message": message,
        "sandbox_id": sandbox_id,
        "steps": _flatten_loop_steps(loop_rounds),
        "conclusions": _merge_structured_items(loop_rounds, "conclusions", unique_key="text"),
        "hypotheses": _merge_structured_items(loop_rounds, "hypotheses", unique_key="text"),
        "action_items": [str(item) for item in _merge_structured_items(loop_rounds, "action_items")],
        "tools_used": _merge_tools_used(loop_rounds),
        "result_rows": _get_last_result_rows(loop_rounds)[:100],
        "chart_specs": _collect_all_charts(loop_rounds),
        "loop_rounds": loop_rounds,
        "final_report_md": final_report_md,
        "report_title": report_title,
        "final_report_html": final_report_html,
        "final_report_summary": final_report_summary,
        "final_report_chart_bindings": final_report_chart_bindings,
        "report_meta": {
            "stop_reason": stop_reason,
            "rounds_completed": len(loop_rounds),
            "max_rounds_hit": max_rounds_hit,
            "report_generated": bool(final_report_html or final_report_summary),
        },
        "session_id": session_id,
        "session_patches": list(session.get("patches", [])),
    }

# ── Auth ──────────────────────────────────────────────────────────────


@app.post("/api/auth/login")
def login(req: LoginRequest):
    if req.provider == "ldap":
        token, user = login_with_ldap(req.username)
    else:
        token, user = login_with_oauth(req.oauth_token)
    return {"token": token, "user": user.__dict__}


@app.get("/api/me")
def me(user: User = Depends(get_current_user)):
    return {"user": user.__dict__}


@app.get("/api/tables")
def tables(user: User = Depends(get_current_user)):
    return {"tables": get_accessible_tables(user)}


@app.get("/api/sandboxes")
def sandboxes(user: User = Depends(get_current_user)):
    return {"sandboxes": get_accessible_sandboxes(user)}


# ── Core: iterative analysis loop ────────────────────────────────────


@app.post("/api/chat/iterate")
def iterate(req: IterateRequest, user: User = Depends(get_current_user)):
    """Single endpoint that replaces propose/select-plan/approve/execute.

    AI autonomously picks tools, runs analysis, and returns conclusions +
    hypotheses + action items.  Results are streamed as NDJSON.
    """
    try:
        sandbox = assert_sandbox_access(user, req.sandbox_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    config = load_config()
    session_id, session = store.get_or_create_session(user.user_id, req.session_id)

    # Auto-title the session from the first message and track sandbox_id
    updates = {}
    if not session.get("title"):
        updates["title"] = req.message[:40].strip()
    if not session.get("sandbox_id"):
        updates["sandbox_id"] = req.sandbox_id
    
    if updates:
        store.update_session(user.user_id, session_id, updates)
        # Update the local dict for the rest of the function logic
        session.update(updates)

    selected_tables = _resolve_selected_tables(
        requested_tables=req.selected_tables,
        sandbox=sandbox,
        user=user,
        max_selected_tables=config.max_selected_tables,
    )
    analysis_sandbox = {
        **sandbox,
        "tables": selected_tables,
        "selected_files": req.selected_files or [],
    }

    # If user picked a hypothesis from previous iteration, prepend it
    message = req.message
    if req.hypothesis_id:
        history = store.get_iteration_history(user.user_id, session_id)
        for it in reversed(history):
            for h in it.get("hypotheses", []):
                if isinstance(h, dict) and h.get("id") == req.hypothesis_id:
                    prefix = t("msg_based_on_hypothesis", default="基于上轮猜想")
                    message = f"[{prefix}: {h['text']}] {message}"
                    break

    raw_iteration_history = store.get_iteration_history(user.user_id, session_id)
    iteration_history = _build_iteration_context_history(raw_iteration_history)
    
    # Merge sandbox knowledge sources into a single context payload.
    business_knowledge = _collect_business_knowledge(sandbox, req.sandbox_id, list(session.get("patches", [])))

    def stream_generator():
        try:
            round_iter = _iter_notebook_rounds(
                message=message,
                analysis_sandbox=analysis_sandbox,
                sandbox=sandbox,
                session_id=session_id,
                sandbox_id=req.sandbox_id,
                selected_tables=selected_tables,
                selected_files=req.selected_files or [],
                iteration_history=iteration_history,
                business_knowledge=business_knowledge,
                provider=req.provider,
                model=req.model,
                max_rounds=ITERATE_MAX_ROUNDS,
                mode="iterate",
            )
            loop_rounds: list[dict] = []
            stop_reason = "model_stopped_using_tools"
            while True:
                try:
                    round_payload = next(round_iter)
                    loop_rounds.append(round_payload)
                except StopIteration as stop:
                    stop_value = stop.value if isinstance(stop.value, dict) else {}
                    stop_reason = str(stop_value.get("stop_reason", stop_reason) or stop_reason)
                    break

                round_index = int(round_payload.get("round", len(loop_rounds)) or len(loop_rounds))
                yield json.dumps({
                    "type": "loop_status",
                    "data": {
                        "round": round_index,
                        "phase": "planning",
                        "message": (
                            f"starting round {round_index}"
                            if get_lang() == "en"
                            else f"开始第 {round_index} 轮分析"
                        ),
                    },
                }, ensure_ascii=False) + "\n"
                if round_payload.get("thought"):
                    yield json.dumps({
                        "type": "loop_status",
                        "data": {
                            "round": round_index,
                            "phase": "thinking",
                            "message": round_payload.get("thought", ""),
                        },
                    }, ensure_ascii=False) + "\n"
                yield json.dumps({"type": "loop_round", "data": round_payload}, ensure_ascii=False) + "\n"
                if round_payload.get("thought"):
                    yield json.dumps({"type": "thought", "content": round_payload.get("thought", "")}, ensure_ascii=False) + "\n"
                yield json.dumps({"type": "result", "data": round_payload.get("result", {})}, ensure_ascii=False) + "\n"
                execution = round_payload.get("execution") or {}
                if execution.get("rows"):
                    yield json.dumps({"type": "data", "rows": execution.get("rows", [])[:200]}, ensure_ascii=False) + "\n"
                for idx, sr in enumerate(execution.get("step_results", [])):
                    yield json.dumps({
                        "type": "step_result",
                        "step_index": idx,
                        "data": {
                            "rows_count": len(sr.get("rows", [])),
                            "tables": sr.get("tables", []),
                            "error": sr.get("error", None),
                        },
                    }, ensure_ascii=False) + "\n"
                for spec in execution.get("chart_specs", []):
                    yield json.dumps({"type": "chart_spec", "data": spec}, ensure_ascii=False) + "\n"

            result_rows = _get_last_result_rows(loop_rounds)
            iteration_payload = {
                "mode": "manual",
                "message": message,
                "steps": _flatten_loop_steps(loop_rounds),
                "conclusions": _merge_structured_items(loop_rounds, "conclusions", unique_key="text"),
                "hypotheses": _merge_structured_items(loop_rounds, "hypotheses", unique_key="text"),
                "action_items": [str(item) for item in _merge_structured_items(loop_rounds, "action_items")],
                "tools_used": _merge_tools_used(loop_rounds),
                "result_rows": result_rows[:100],
                "chart_specs": _collect_all_charts(loop_rounds),
                "loop_rounds": loop_rounds,
                "final_report_md": "",
                "report_title": "",
                "final_report_html": "",
                "final_report_summary": "",
                "final_report_chart_bindings": [],
                "report_meta": {
                    "stop_reason": stop_reason,
                    "rounds_completed": len(loop_rounds),
                    "max_rounds_hit": stop_reason == "max_rounds_reached",
                },
            }
            iteration_id = store.append_iteration(user.user_id, session_id, iteration_payload)
            proposal_id = store.create_proposal({
                "user_id": user.user_id,
                "session_id": session_id,
                "sandbox_id": req.sandbox_id,
                "mode": "manual",
                "message": message,
                "steps": iteration_payload.get("steps", []),
                "explanation": str(((loop_rounds[-1].get("result") if loop_rounds else {}) or {}).get("explanation", "")),
                "tables": selected_tables,
                "status": "executed",
                "result_rows": result_rows,
                "chart_specs": iteration_payload.get("chart_specs", []),
                "selected_tables": selected_tables,
                "selected_files": req.selected_files or [],
                "session_patches": list(session.get("patches", [])),
                "loop_rounds": loop_rounds,
                "final_report_md": "",
                "report_title": "",
                "final_report_html": "",
                "final_report_summary": "",
                "final_report_chart_bindings": [],
                "report_meta": iteration_payload.get("report_meta", {}),
            })

            yield json.dumps({
                "type": "iteration_complete",
                "data": {
                    "iteration_id": iteration_id,
                    "session_id": session_id,
                    "proposal_id": proposal_id,
                    "result_count": len(result_rows),
                    "rounds_completed": len(loop_rounds),
                    "stop_reason": stop_reason,
                },
            }, ensure_ascii=False) + "\n"

        except RuntimeError as exc:
            localized_error = _localize_html_bundle_runtime_error(str(exc))
            yield json.dumps({"type": "error", "message": localized_error}, ensure_ascii=False) + "\n"
        except Exception as exc:
            internal_error = t("error_internal", default="服务器内部错误")
            yield json.dumps({"type": "error", "message": f"{internal_error}: {str(exc)}"}, ensure_ascii=False) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


@app.post("/api/chat/auto-analyze")
def auto_analyze(req: AutoAnalyzeRequest, user: User = Depends(get_current_user)):
    """Multi-round autonomous analysis until the model stops using tools."""
    try:
        sandbox = assert_sandbox_access(user, req.sandbox_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    config = load_config()
    session_id, session = store.get_or_create_session(user.user_id, req.session_id)
    incoming_message = str(req.message or "").strip()

    updates = {}
    if not session.get("title"):
        updates["title"] = (incoming_message or "One-click analysis")[:40]
    if not session.get("sandbox_id"):
        updates["sandbox_id"] = req.sandbox_id
    if updates:
        store.update_session(user.user_id, session_id, updates)
        session.update(updates)

    selected_tables = _resolve_selected_tables(
        requested_tables=req.selected_tables,
        sandbox=sandbox,
        user=user,
        max_selected_tables=config.max_selected_tables,
    )
    selected_files = req.selected_files or sandbox.get("selected_files", []) or []
    analysis_sandbox = {
        **sandbox,
        "tables": selected_tables,
        "selected_files": selected_files,
    }

    message = incoming_message
    historical_iterations_raw = store.get_iteration_history(user.user_id, session_id)
    historical_iterations = _build_iteration_context_history(historical_iterations_raw)
    if req.hypothesis_id:
        for it in reversed(historical_iterations_raw):
            for h in it.get("hypotheses", []):
                if isinstance(h, dict) and h.get("id") == req.hypothesis_id:
                    prefix = t("msg_based_on_hypothesis", default="基于上轮猜想")
                    message = f"[{prefix}: {h['text']}] {message}"
                    break
    if not message.strip():
        message = _build_default_auto_seed_message(selected_tables, selected_files)

    business_knowledge = _collect_business_knowledge(sandbox, req.sandbox_id, list(session.get("patches", [])))

    def stream_generator():
        try:
            round_iter = _iter_notebook_rounds(
                message=message,
                analysis_sandbox=analysis_sandbox,
                sandbox=sandbox,
                session_id=session_id,
                sandbox_id=req.sandbox_id,
                selected_tables=selected_tables,
                selected_files=selected_files,
                iteration_history=historical_iterations,
                business_knowledge=business_knowledge,
                provider=req.provider,
                model=req.model,
                max_rounds=req.max_rounds,
                mode="auto",
            )

            loop_rounds: list[dict] = []
            stop_reason = "model_stopped_using_tools"
            direct_report_md = ""
            while True:
                try:
                    round_payload = next(round_iter)
                    loop_rounds.append(round_payload)
                except StopIteration as stop:
                    stop_value = stop.value if isinstance(stop.value, dict) else {}
                    stop_reason = str(stop_value.get("stop_reason", stop_reason) or stop_reason)
                    direct_report_md = str(stop_value.get("direct_report_md", "") or "")
                    break

                round_index = int(round_payload.get("round", 0) or 0)
                yield json.dumps({
                    "type": "loop_status",
                    "data": {
                        "round": round_index,
                        "phase": "planning",
                        "message": (
                            f"starting round {round_index}"
                            if get_lang() == "en"
                            else f"开始第 {round_index} 轮分析"
                        ),
                    },
                }, ensure_ascii=False) + "\n"
                if round_payload.get("thought"):
                    yield json.dumps({
                        "type": "loop_status",
                        "data": {
                            "round": round_index,
                            "phase": "thinking",
                            "message": round_payload.get("thought", ""),
                        },
                    }, ensure_ascii=False) + "\n"
                yield json.dumps({"type": "loop_round", "data": round_payload}, ensure_ascii=False) + "\n"

            chart_specs = _collect_all_charts(loop_rounds)
            report_bundle = generate_auto_analysis_report_bundle(
                message=message,
                session_history=historical_iterations,
                business_knowledge=business_knowledge,
                session_patches=list(session.get("patches", [])),
                loop_rounds=loop_rounds,
                chart_specs=chart_specs,
                final_result_rows=_get_last_result_rows(loop_rounds),
                stop_reason=stop_reason,
                rounds_completed=len(loop_rounds),
                provider=req.provider,
                model=req.model,
            ) or {}
            if direct_report_md:
                report_bundle["legacy_markdown"] = direct_report_md
                report_bundle["summary"] = direct_report_md[:500]
            report_bundle = _normalize_auto_report_bundle(report_bundle, chart_specs)
            yield json.dumps({
                "type": "report",
                "data": {
                    "title": report_bundle.get("title", "Auto Analysis Report"),
                    "summary": report_bundle.get("summary", ""),
                    "markdown": report_bundle.get("legacy_markdown", ""),
                    "conclusions": report_bundle.get("conclusions", []),
                    "chart_bindings": report_bundle.get("chart_bindings", []),
                    "html_document": report_bundle.get("html_document", ""),
                    "stop_reason": stop_reason,
                    "rounds_completed": len(loop_rounds),
                },
            }, ensure_ascii=False) + "\n"

            iteration_payload = _build_auto_iteration_payload(
                message=message,
                session_id=session_id,
                sandbox_id=req.sandbox_id,
                selected_tables=selected_tables,
                session=session,
                loop_rounds=loop_rounds,
                report_bundle=report_bundle,
                stop_reason=stop_reason,
                max_rounds=req.max_rounds,
            )
            last_result = (loop_rounds[-1].get("result") if loop_rounds else {}) or {}

            iteration_id = store.append_iteration(user.user_id, session_id, iteration_payload)
            report_url = _build_iteration_report_url(iteration_id)
            proposal_id = store.create_proposal({
                "user_id": user.user_id,
                "session_id": session_id,
                "sandbox_id": req.sandbox_id,
                "mode": "auto_analysis",
                "message": message,
                "steps": iteration_payload.get("steps", []),
                "explanation": last_result.get("explanation", ""),
                "tables": selected_tables,
                "status": "executed",
                "result_rows": _get_last_result_rows(loop_rounds),
                "chart_specs": iteration_payload.get("chart_specs", []),
                "selected_tables": selected_tables,
                "selected_files": selected_files,
                "session_patches": list(session.get("patches", [])),
                "loop_rounds": loop_rounds,
                "final_report_md": iteration_payload.get("final_report_md", ""),
                "report_title": iteration_payload.get("report_title", ""),
                "final_report_html": iteration_payload.get("final_report_html", ""),
                "final_report_summary": iteration_payload.get("final_report_summary", ""),
                "final_report_chart_bindings": iteration_payload.get("final_report_chart_bindings", []),
                "report_meta": iteration_payload.get("report_meta", {}),
            })

            yield json.dumps({
                "type": "analysis_complete",
                "data": {
                    "iteration_id": iteration_id,
                    "session_id": session_id,
                    "proposal_id": proposal_id,
                    "stop_reason": stop_reason,
                    "rounds_completed": len(loop_rounds),
                    "max_rounds_hit": iteration_payload.get("report_meta", {}).get("max_rounds_hit", False),
                    "result_count": len(_get_last_result_rows(loop_rounds)),
                    "report_url": report_url,
                    "report_title": iteration_payload.get("report_title", ""),
                },
            }, ensure_ascii=False) + "\n"
        except RuntimeError as exc:
            localized_error = _localize_html_bundle_runtime_error(str(exc))
            yield json.dumps({"type": "error", "message": localized_error}, ensure_ascii=False) + "\n"
        except Exception as exc:
            internal_error = t("error_internal", default="服务端内部错误")
            yield json.dumps({"type": "error", "message": f"{internal_error}: {str(exc)}"}, ensure_ascii=False) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


@app.post("/api/chat/feedback")
def feedback(req: FeedbackRequest, user: User = Depends(get_current_user)):
    """Accept user feedback or business knowledge."""
    _, session = store.get_or_create_session(user.user_id, req.session_id)
    if req.is_business_knowledge:
        store.append_business_knowledge(req.sandbox_id, req.feedback)
        return {
            "session_id": req.session_id,
            "type": "business_knowledge",
            "message": t("msg_knowledge_saved", default="业务知识已沉淀，后续分析将自动参考。"),
        }
    else:
        store.append_patch(user.user_id, req.session_id, req.feedback)
        return {
            "session_id": req.session_id,
            "type": "feedback",
            "message": t("msg_feedback_saved", default="反馈已记录，下次迭代将参考。"),
        }


@app.get("/api/chat/history")
def iteration_history(session_id: str, user: User = Depends(get_current_user)):
    """Get iteration history for a session."""
    history = store.get_iteration_history(user.user_id, session_id)
    last_proposal_id = store.get_last_proposal_id(user.user_id, session_id)
    return {"session_id": session_id, "iterations": history, "last_proposal_id": last_proposal_id}


@app.get("/api/reports/iterations/{iteration_id}")
def get_iteration_report(iteration_id: str, user: User = Depends(get_current_user)):
    iteration = store.get_iteration(user.user_id, iteration_id)
    if not iteration:
        raise HTTPException(status_code=404, detail="iteration not found")
    if (iteration.get("mode") or "") != "auto_analysis":
        raise HTTPException(status_code=400, detail="iteration is not an auto-analysis report")
    normalized_report = _normalize_auto_report_bundle(
        {
            "title": iteration.get("report_title", ""),
            "summary": iteration.get("final_report_summary", ""),
            "html_document": iteration.get("final_report_html", ""),
            "chart_bindings": iteration.get("final_report_chart_bindings", []),
            "legacy_markdown": iteration.get("final_report_md", ""),
        },
        chart_specs=[],
    )
    return {
        "iteration_id": iteration.get("iteration_id"),
        "session_id": iteration.get("session_id"),
        "report_title": normalized_report.get("title", ""),
        "final_report_html": normalized_report.get("html_document", ""),
        "final_report_summary": normalized_report.get("summary", ""),
        "final_report_chart_bindings": normalized_report.get("chart_bindings", []),
        "report_meta": iteration.get("report_meta", {}),
        "created_at": iteration.get("created_at"),
    }


@app.get("/api/chat/sessions")
def list_sessions(user: User = Depends(get_current_user)):
    """List all sessions for the current user."""
    return {"sessions": store.list_sessions(user.user_id)}


@app.delete("/api/chat/sessions/{session_id}")
def delete_session(session_id: str, user: User = Depends(get_current_user)):
    ok = store.delete_session(user.user_id, session_id)
    if not ok:
        raise HTTPException(status_code=404, detail=t("error_session_not_found"))
    destroy_kernel(session_id)
    return {"deleted": session_id}


@app.patch("/api/chat/sessions/{session_id}")
def update_session(session_id: str, req: UpdateSessionRequest, user: User = Depends(get_current_user)):
    store.update_session_title(user.user_id, session_id, req.title)
    return {"session_id": session_id, "title": req.title}


# ── Insight analysis (kept) ───────────────────────────────────────────


@app.post("/api/chat/analyze")
def analyze(proposal_id: str, user: User = Depends(get_current_user)):
    proposal = store.proposals.get(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail=t("error_proposal_not_found"))
    if proposal["user_id"] != user.user_id:
        raise HTTPException(status_code=403, detail=t("error_no_permission_proposal"))

    result = proposal.get("result_rows", [])
    config = load_config()

    def stream_generator():
        try:
            for spec in proposal.get("chart_specs", []):
                yield json.dumps({"type": "chart_spec", "data": spec}) + "\n"
            insight_gen = generate_data_insight(result, proposal.get("sql", ""), proposal["message"], config)
            for chunk in insight_gen:
                yield json.dumps({"type": "insight", "content": chunk}) + "\n"
        except Exception as exc:
            yield json.dumps({"type": "error", "message": str(exc)}) + "\n"

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")


# ── Data upload ───────────────────────────────────────────────────────


from fastapi import File, Form, UploadFile
import os

@app.post("/api/data/upload")
async def upload_data(
    files: list[UploadFile] = File(...),
    sandbox_id: str = Form(...),
    session_id: str | None = Form(default=None),
    user: User = Depends(get_current_user),
):
    sid, _ = store.get_or_create_session(user.user_id, session_id)
    os.makedirs("uploads", exist_ok=True)
    
    uploaded_files_info = []

    for file in files:
        if not file.filename:
            continue
            
        content = await file.read()
        filename = file.filename
        lower = filename.lower()
        
        # Save file to disk
        file_path = os.path.abspath(os.path.join("uploads", f"{uuid.uuid4().hex[:8]}_{filename}"))
        with open(file_path, "wb") as f:
            f.write(content)
            
        rows = []
        is_tabular = False
        columns = []
        
        if lower.endswith(".csv"):
            try:
                df = pd.read_csv(io.StringIO(content.decode("utf-8")))
                is_tabular = True
            except Exception:
                pass
        elif lower.endswith(".xlsx") or lower.endswith(".xls"):
            try:
                df = pd.read_excel(io.BytesIO(content))
                is_tabular = True
            except Exception:
                pass
        
        # Determine if it's text or JSON or whatever, fallback to treating as document
        if is_tabular and 'df' in locals() and not df.empty:
            # Replace NaN/inf with None for JSON compatibility
            clean_df = df.head(5000).where(pd.notnull(df.head(5000)), None)
            rows = clean_df.to_dict(orient="records")
            columns = [str(c) for c in df.columns]

        store.add_upload(sandbox_id, filename, rows, file_path=file_path)
        
        uploaded_files_info.append({
            "dataset_name": filename,
            "rows": len(rows) if is_tabular else 0,
            "columns": columns,
            "is_tabular": is_tabular
        })

    return {"session_id": sid, "uploaded_files": uploaded_files_info}


# ── Skills ────────────────────────────────────────────────────────────


@app.post("/api/skills/save")
def save_skill(req: SaveSkillRequest, user: User = Depends(get_current_user)):
    try:
        # Extract session_id from proposal if available (for knowledge extraction)
        proposal = store.proposals.get(req.proposal_id, {})
        session_id = proposal.get("session_id")
        skill = save_skill_from_proposal(
            user=user,
            proposal_id=req.proposal_id,
            name=req.name,
            description=req.description,
            tags=req.tags,
            extra_knowledge=req.knowledge,
            table_descriptions=req.table_descriptions,
            session_id=session_id,
            overwrite_skill_id=req.overwrite_skill_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return {"skill": skill}


@app.post("/api/skills/propose")
def propose_skill(req: ProposeSkillRequest, user: User = Depends(get_current_user)):
    proposal = store.proposals.get(req.proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if proposal.get("user_id") != user.user_id:
        raise HTTPException(status_code=403, detail=t("error_no_permission_proposal", default="无权访问该提案"))
    
    snapshot: dict = {}
    try:
        snapshot = build_context_snapshot_for_proposal(user=user, proposal_id=req.proposal_id)
    except (ValueError, PermissionError):
        snapshot = {}

    source_sandbox_id = str(proposal.get("sandbox_id") or req.sandbox_id or "").strip()
    sandbox = store.sandboxes.get(source_sandbox_id)
    unnamed = t("msg_sandbox_unnamed", default="未命名沙盒")
    unknown = t("msg_sandbox_unknown", default="未知沙盒")
    sandbox_name = sandbox.get("name", unnamed) if sandbox else unknown
    
    suggestion = generate_skill_proposal(
        message=req.message,
        analysis_result=proposal,
        sandbox_name=sandbox_name
    )
    normalized_suggestion = _build_skill_proposal_fallback(
        proposal=proposal,
        requested_message=req.message,
        sandbox_name=sandbox_name,
        suggestion=suggestion,
    )
    return {**normalized_suggestion, "context_snapshot": snapshot}


@app.delete("/api/skills/{skill_id}")
def delete_skill(skill_id: str, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail=t("error_skill_not_found"))
    if skill["owner_id"] != user.user_id:
        raise HTTPException(status_code=403, detail=t("error_no_permission_skill"))
    store.delete_skill(skill_id)
    return {"deleted": skill_id}


@app.get("/api/skills/{skill_id}")
def get_skill(skill_id: str, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail=t("error_skill_not_found"))
    if skill["owner_id"] != user.user_id and not set(skill["groups"]).intersection(user.groups):
        raise HTTPException(status_code=403, detail=t("error_no_permission_skill"))
    return {"skill_id": skill_id, **skill}



@app.patch("/api/skills/{skill_id}")
def update_skill(skill_id: str, req: UpdateSkillRequest, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail=t("error_skill_not_found"))
    if skill["owner_id"] != user.user_id:
        raise HTTPException(status_code=403, detail=t("error_no_permission_skill"))
    
    updates = {}
    if req.name is not None:
        updates["name"] = req.name
    if req.description is not None:
        updates["description"] = req.description
    if req.tags is not None:
        updates["tags"] = req.tags
        
    # We need to deep copy the layers since mutating dict elements directly is complicated
    # But since we have the existing skill, we'll extract its layers to modify
    if req.knowledge is not None or req.table_descriptions is not None:
        layers = dict(skill.get("layers") or {})
        if req.knowledge is not None:
            layers["knowledge"] = req.knowledge
        if req.table_descriptions is not None:
            layers["tables"] = req.table_descriptions
        updates["layers"] = layers

    if updates:
        store.update_skill(skill_id, updates)
        skill = store.skills.get(skill_id) # reload to get updated version

    return {"skill_id": skill_id, **skill}


@app.get("/api/skills")
def skills(user: User = Depends(get_current_user)):
    return {"skills": list_skills(user)}


# ── External DB connections ────────────────────────────────────────────


class DbConnectionCreateRequest(BaseModel):
    name: str
    db_type: str                  # mysql / postgresql / sqlite / oracle / impala
    host: str = "localhost"
    port: int | None = None
    database: str
    username: str = ""
    password: str = ""


class DbConnectionUpdateRequest(BaseModel):
    name: str | None = None
    db_type: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None


class DbConnectionTestRequest(BaseModel):
    db_type: str
    host: str = "localhost"
    port: int | None = None
    database: str
    username: str = ""
    password: str = ""


class MountDbConnectionRequest(BaseModel):
    connection_id: str | None = None


class SaveTablesRequest(BaseModel):
    tables: list[str]

@app.get("/api/db-connections")
def list_db_connections(user: User = Depends(get_current_user)):
    return {"connections": store.list_db_connections()}


@app.post("/api/db-connections/test")
def test_standalone_db_connection(req: DbConnectionTestRequest, user: User = Depends(get_current_user)):
    try:
        cfg = DbConnectionConfig(
            db_type=req.db_type,
            host=req.host,
            port=req.port,
            database=req.database,
            username=req.username,
            password=req.password,
        )
        result = test_connection(cfg)
        tables = get_table_names(get_engine(cfg)) if result.get("ok") else []
        return {"ok": bool(result.get("ok")), "error": result.get("error"), "tables": tables}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "tables": []}


@app.post("/api/db-connections")
def create_db_connection(req: DbConnectionCreateRequest, user: User = Depends(get_current_user)):
    try:
        cfg = DbConnectionConfig(
            db_type=req.db_type,
            host=req.host,
            port=req.port,
            database=req.database,
            username=req.username,
            password=req.password,
        )
        result = test_connection(cfg)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=f"{t('error_db_config', default='连接配置错误')}: {result.get('error')}")

        connection = store.create_or_reuse_db_connection(
            {
                "name": req.name,
                "db_type": req.db_type,
                "host": req.host,
                "port": req.port or cfg.port,
                "database": req.database,
                "username": req.username,
                "password": req.password,
            }
        )
        tables = get_table_names(get_engine(cfg))
        return {"connection": connection, "tables": tables}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{t('error_db_config', default='连接配置错误')}: {str(exc)}")


@app.put("/api/db-connections/{connection_id}")
def update_db_connection(connection_id: str, req: DbConnectionUpdateRequest, user: User = Depends(get_current_user)):
    current = store.get_db_connection(connection_id, include_password=True)
    if not current:
        raise HTTPException(status_code=404, detail=t("error_db_connection_not_found", default="Database connection not found"))

    merged = {
        "name": req.name if req.name is not None else current["name"],
        "db_type": req.db_type if req.db_type is not None else current["db_type"],
        "host": req.host if req.host is not None else current["host"],
        "port": req.port if req.port is not None else current["port"],
        "database": req.database if req.database is not None else current["database"],
        "username": req.username if req.username is not None else current["username"],
        "password": req.password if req.password not in (None, "") else current.get("password", ""),
    }
    try:
        cfg = DbConnectionConfig(
            db_type=merged["db_type"],
            host=merged["host"],
            port=merged["port"],
            database=merged["database"],
            username=merged["username"],
            password=merged["password"],
        )
        result = test_connection(cfg)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=f"{t('error_db_config', default='连接配置错误')}: {result.get('error')}")

        updates = {
            "name": merged["name"],
            "db_type": merged["db_type"],
            "host": merged["host"],
            "port": merged["port"] or cfg.port,
            "database": merged["database"],
            "username": merged["username"],
        }
        if req.password not in (None, ""):
            updates["password"] = req.password
        connection = store.update_db_connection(connection_id, updates)
        tables = get_table_names(get_engine(cfg))
        return {"connection": connection, "tables": tables}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{t('error_db_config', default='连接配置错误')}: {str(exc)}")


@app.delete("/api/db-connections/{connection_id}")
def delete_db_connection(connection_id: str, user: User = Depends(get_current_user)):
    ok = store.delete_db_connection(connection_id)
    if not ok:
        raise HTTPException(status_code=404, detail=t("error_db_connection_not_found", default="Database connection not found"))
    return {"ok": True}


@app.put("/api/sandboxes/{sandbox_id}/db-connection")
def mount_db_connection(
    sandbox_id: str,
    req: MountDbConnectionRequest,
    user: User = Depends(get_current_user),
):
    try:
        assert_sandbox_access(user, sandbox_id)
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    if req.connection_id is None:
        sandbox = store.mount_db_connection_to_sandbox(sandbox_id=sandbox_id, connection_id=None, clear_tables=True)
        return {"sandbox_id": sandbox_id, "db_connection_id": None, "db_connection": None, "tables": [], "sandbox": sandbox}

    if not store.get_db_connection(req.connection_id):
        raise HTTPException(status_code=404, detail=t("error_db_connection_not_found", default="Database connection not found"))

    sandbox = store.mount_db_connection_to_sandbox(sandbox_id=sandbox_id, connection_id=req.connection_id, clear_tables=True)
    table_names: list[str] = []
    try:
        table_names = store.get_connection_table_names(req.connection_id)
    except Exception:
        table_names = []
    return {
        "sandbox_id": sandbox_id,
        "db_connection_id": req.connection_id,
        "db_connection": store.get_db_connection(req.connection_id),
        "tables": table_names,
        "sandbox": sandbox,
    }


@app.post("/api/sandboxes/{sandbox_id}/db-tables")
def save_sandbox_tables(
    sandbox_id: str,
    req: SaveTablesRequest,
    user: User = Depends(get_current_user),
):
    """Save the selected tables to the sandbox configuration."""
    try:
        sandbox = assert_sandbox_access(user, sandbox_id)
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    if len(req.tables) > MAX_SELECTED_TABLES:
        error_msg = t("error_max_tables", max=MAX_SELECTED_TABLES, default=f"最多只能选择 {MAX_SELECTED_TABLES} 张表")
        raise HTTPException(status_code=400, detail=error_msg)

    store.update_sandbox(sandbox_id, {
        "tables": req.tables,
        "allowed_tables": req.tables
    })
    
    return {"ok": True, "tables": req.tables}


# ── Internal helpers ──────────────────────────────────────────────────

import pandas as pd
from sqlalchemy import text


def _query_rows(sql: str, sandbox_id: str | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Preserves aliases naturally."""
    if sandbox_id:
        engine = store.get_sandbox_engine(sandbox_id)
        if engine is not None:
            return pd.read_sql(sql, engine)
    return pd.read_sql(sql, store.conn)


def _execute_analysis_steps(
    result_data: dict,
    sandbox: dict,
    selected_tables: list[str],
    selected_files: list[str] | None,
    sandbox_id: str,
    *,
    session_id: str,
) -> dict:
    kernel = create_kernel(
        session_id=session_id,
        sandbox_id=sandbox_id,
        selected_tables=selected_tables,
        selected_files=selected_files or [],
    )
    all_uploads = sandbox.get("uploads", {})
    all_upload_paths = sandbox.get("upload_paths", {})
    if selected_files is not None:
        allowed_uploads = {k: v for k, v in all_uploads.items() if k in selected_files}
        allowed_upload_paths = {k: v for k, v in all_upload_paths.items() if k in selected_files}
    else:
        allowed_uploads = all_uploads
        allowed_upload_paths = all_upload_paths

    steps = result_data.get("steps", [])
    if not isinstance(steps, list):
        steps = []

    step_results: list[dict] = []
    all_rows: list[dict] = []
    all_tables: list[str] = []
    all_chart_specs: list[dict] = []
    exported_vars: dict[str, object] = {}
    warnings: list[str] = []

    for i, step in enumerate(steps):
        tool = str(step.get("tool", "")).strip().lower()
        source = str(step.get("source", "main") or "main").strip().lower()
        code = str(step.get("code", "")).strip()
        if not code:
            step_results.append({"rows": [], "tables": [], "error": t("error_empty_code", default="空代码")})
            continue
        if tool == "sql":
            try:
                sql_result = kernel.run_sql_cell(
                    step_index=len(step_results),
                    code=code,
                    source=source,
                    main_query_df=lambda sql: _query_rows(sql, sandbox_id),
                )
                rows = sql_result.get("rows", [])
                used_tables = sql_result.get("tables", [])
                step_entry = {
                    "rows": rows,
                    "tables": used_tables,
                    "source": source,
                }
                step_results.append(step_entry)
                all_rows = rows
                for table_name in used_tables:
                    if table_name not in all_tables:
                        all_tables.append(table_name)
            except Exception as exc:
                error_msg = t("error_sql_failed", step=i + 1, default=f"SQL 执行失败 (step {i+1})") + f": {str(exc)}"
                step_results.append({"rows": [{"error": error_msg}], "tables": [], "source": source})
                return {"step_results": step_results, "error": error_msg, "rows": all_rows, "tables": all_tables, "chart_specs": all_chart_specs, "exported_vars": exported_vars}
        elif tool == "python":
            try:
                python_result = kernel.run_python_cell(
                    code=code,
                    upload_rows=allowed_uploads,
                    upload_paths=allowed_upload_paths,
                    main_query_df=lambda sql: _query_rows(sql, sandbox_id),
                    step_results=step_results,
                )
                result_rows = python_result.get("rows", [])
                result_charts = python_result.get("chart_specs", [])
                result_warning = python_result.get("warning")
                python_exported_vars = python_result.get("exported_vars") or {}
                if isinstance(python_exported_vars, dict):
                    exported_vars.update(python_exported_vars)
                step_entry = {
                    "rows": result_rows,
                    "tables": list(all_tables),
                    "chart_specs": result_charts,
                    "exported_vars": python_exported_vars if isinstance(python_exported_vars, dict) else {},
                }
                if result_warning:
                    step_entry["warning"] = result_warning
                    warnings.append(str(result_warning))
                step_results.append(step_entry)
                all_rows = result_rows
                all_chart_specs.extend(result_charts)
            except Exception as exc:
                error_msg = t("error_python_failed", step=i + 1, default=f"Python 执行失败 (step {i+1})") + f": {str(exc)}"
                step_results.append({"rows": [{"error": error_msg}], "tables": list(all_tables)})
                return {"step_results": step_results, "error": error_msg, "rows": all_rows, "tables": all_tables, "chart_specs": all_chart_specs, "exported_vars": exported_vars, "warnings": warnings}
        else:
            step_results.append({"rows": [], "tables": [], "error": t("error_unknown_tool", tool=tool, default=f"未知工具: {tool}")})

    from app.utils import sanitize_for_json

    return sanitize_for_json({
        "rows": all_rows,
        "tables": all_tables,
        "chart_specs": all_chart_specs,
        "step_results": step_results,
        "exported_vars": exported_vars,
        "warnings": warnings,
        "kernel_snapshot": kernel.snapshot(),
    })


def _resolve_selected_tables(requested_tables: list[str] | None, sandbox: dict, user: User, max_selected_tables: int) -> list[str]:
    # We already verified sandbox access in the caller via assert_sandbox_access.
    # Therefore, the user is authorized to access ALL tables registered to this sandbox.
    allowed_sandbox_tables = list(sandbox.get("tables", []))
    
    if requested_tables is None:
        return allowed_sandbox_tables[:max_selected_tables]
    normalized: list[str] = []
    for table in requested_tables:
        table_name = str(table).strip()
        if table_name and table_name not in normalized:
            normalized.append(table_name)
    if len(normalized) > max_selected_tables:
        raise HTTPException(status_code=400, detail=t("error_max_tables", max=max_selected_tables, default=f"最多可选择 {max_selected_tables} 张表"))
    denied = [table_name for table_name in normalized if table_name not in allowed_sandbox_tables]
    if denied:
        raise HTTPException(status_code=403, detail=t("error_no_permission_tables", tables=', '.join(denied), default=f"无权选择表: {', '.join(denied)}"))
    return normalized

# ── Sandbox Workspace Management ──────────────────────────────────────

@app.post("/api/sandboxes")
def create_sandbox(
    req: CreateSandboxRequest,
    user: User = Depends(get_current_user),
):
    """Create a new personal Sandbox workspace."""
    # Default to user's groups if none provided, ensuring they can see it
    groups = req.allowed_groups if req.allowed_groups else user.groups
    sandbox_id = store.create_sandbox(name=req.name, allowed_groups=groups)
    return {"sandbox_id": sandbox_id, "message": t("msg_sandbox_created")}

@app.put("/api/sandboxes/{sandbox_id}")
def rename_sandbox(
    sandbox_id: str,
    req: RenameSandboxRequest,
    user: User = Depends(get_current_user),
):
    """Rename an existing Sandbox workspace."""
    try:
        assert_sandbox_access(user, sandbox_id)
        sandbox = store.update_sandbox(sandbox_id, {"name": req.name})
        return {"sandbox_id": sandbox_id, "name": sandbox["name"], "message": t("msg_sandbox_renamed")}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

@app.delete("/api/sandboxes/{sandbox_id}")
def delete_sandbox(
    sandbox_id: str,
    user: User = Depends(get_current_user),
):
    """Delete a Sandbox workspace."""
    try:
        # Check permissions before deleting
        assert_sandbox_access(user, sandbox_id)
        store.delete_sandbox(sandbox_id)
        return {"ok": True, "message": t("msg_sandbox_deleted")}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

# ── Knowledge Bases ───────────────────────────────────────────────────

def estimate_tokens(text: str) -> int:
    if not text: return 0
    # Simple heuristic: 1 char ~ 0.75 tokens, roughly for Chinese/English mix
    return int(len(text) * 0.75)


@app.post("/api/knowledge_bases")
def create_knowledge_base(req: CreateKnowledgeBaseRequest, user: User = Depends(get_current_user)):
    data = req.model_dump(exclude_unset=True)
    if data.get("content"):
        data["token_count"] = estimate_tokens(data["content"])
    kb_id = store.create_knowledge_base(data)
    return store.get_knowledge_base(kb_id)


@app.get("/api/knowledge_bases")
def list_knowledge_bases(user: User = Depends(get_current_user)):
    return {"knowledge_bases": store.list_knowledge_bases()}


@app.patch("/api/knowledge_bases/{kb_id}")
def update_knowledge_base(kb_id: str, req: UpdateKnowledgeBaseRequest, user: User = Depends(get_current_user)):
    data = req.model_dump(exclude_unset=True)
    if "content" in data:
        data["token_count"] = estimate_tokens(data["content"] or "")
    try:
        updated = store.update_knowledge_base(kb_id, data)
        return updated
    except ValueError:
        raise HTTPException(status_code=404, detail="Knowledge base not found")


@app.delete("/api/knowledge_bases/{kb_id}")
def delete_knowledge_base(kb_id: str, user: User = Depends(get_current_user)):
    if store.delete_knowledge_base(kb_id):
        return {"deleted": kb_id}
    raise HTTPException(status_code=404, detail="Knowledge base not found")


@app.post("/api/knowledge_bases/{kb_id}/sync")
async def sync_knowledge_base(kb_id: str, user: User = Depends(get_current_user)):
    kb = store.get_knowledge_base(kb_id)
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    
    if kb.get("sync_type") != "api":
        raise HTTPException(status_code=400, detail="Not an API knowledge base")
    
    url = kb.get("api_url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing API URL")
    
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            headers = kb.get("api_headers") or {}
            params = kb.get("api_params") or {}
            method = (kb.get("api_method") or "GET").upper()
            
            if method == "POST":
                r = await client.post(url, headers=headers, json=params, timeout=10.0)
            else:
                r = await client.get(url, headers=headers, params=params, timeout=10.0)
            
            r.raise_for_status()
            content = r.text
            
            json_path = kb.get("api_json_path")
            if json_path and r.headers.get("content-type", "").startswith("application/json"):
                data = r.json()
                for key in json_path.split("."):
                    if isinstance(data, dict) and key in data:
                        data = data[key]
                    else:
                        break
                content = str(data) if data is not None else ""
            
            updated = store.update_knowledge_base(kb_id, {
                "content": content,
                "token_count": estimate_tokens(content)
            })
            return {"status": "success", "token_count": updated.get("token_count")}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sandboxes/{sandbox_id}/knowledge_bases")
def mount_knowledge_bases(sandbox_id: str, req: MountKnowledgeBasesRequest, user: User = Depends(get_current_user)):
    try:
        assert_sandbox_access(user, sandbox_id)
        knowledge_bases = _dedupe_keep_order(req.knowledge_bases)
        store.update_sandbox(sandbox_id, {"knowledge_bases": knowledge_bases})
        return {"sandbox_id": sandbox_id, "knowledge_bases": knowledge_bases}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@app.post("/api/sandboxes/{sandbox_id}/skills")
def mount_skills(sandbox_id: str, req: MountSkillsRequest, user: User = Depends(get_current_user)):
    try:
        assert_sandbox_access(user, sandbox_id)
        skill_ids = _dedupe_keep_order(req.skills)
        missing = [skill_id for skill_id in skill_ids if store.skills.get(skill_id) is None]
        if missing:
            raise HTTPException(status_code=400, detail=f"Skills not found: {', '.join(missing)}")
        store.update_sandbox(sandbox_id, {"mounted_skills": skill_ids})
        return {"sandbox_id": sandbox_id, "skills": skill_ids}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

