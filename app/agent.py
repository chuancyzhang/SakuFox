import json
import re
from typing import Generator

import httpx

from app.config import AppConfig, load_config


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
        yield "未查询到数据，无法进行分析。"
        return
    preview_data = data[:20]
    data_summary = f"共 {len(data)} 条数据，前 20 条预览：{json.dumps(preview_data, ensure_ascii=False)}"

    if config.llm_provider not in {"openai", "anthropic"}:
        yield "### 数据分析报告\n\n"
        yield f"- 记录数：{len(data)}\n"
        yield f"- SQL：`{sql}`\n"
        yield "- 当前为 mock 模式，建议切换到 LLM 获取更深层商业洞察。\n"
        return

    user_prompt = (
        f"用户问题: {message}\n"
        f"执行 SQL: {sql}\n"
        f"数据结果: {data_summary}\n"
        "请输出面向业务负责人的分析结论，不要输出任何代码。"
    )
    agents: list[tuple[str, str]] = [
        ("视角一：指标口径", config.insight_prompt_metrics),
        ("视角二：异常归因", config.insight_prompt_anomaly),
        ("视角三：经营动作", config.insight_prompt_actions),
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
    if business_knowledge:
        parts.append("【已沉淀的业务知识】")
        for i, bk in enumerate(business_knowledge, 1):
            parts.append(f"{i}. {bk}")
        parts.append("")

    # Past iteration summaries (compact)
    if iteration_history:
        parts.append("【历史迭代摘要】")
        for it in iteration_history[-5:]:  # last 5 iterations for context window
            parts.append(f"- 迭代 {it.get('iteration_id', '?')}: {it.get('message', '')}")
            conclusions = it.get("conclusions", [])
            if conclusions:
                for c in conclusions[:3]:
                    text = c.get("text", str(c)) if isinstance(c, dict) else str(c)
                    conf = c.get("confidence", "?") if isinstance(c, dict) else "?"
                    parts.append(f"  结论(置信度{conf}): {text}")
            hypotheses = it.get("hypotheses", [])
            if hypotheses:
                parts.append(f"  提出猜想: {', '.join(h.get('text', str(h)) if isinstance(h, dict) else str(h) for h in hypotheses[:3])}...")
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
            parts.append("【沙盒可用表详述 - Ground Truth】")
            for tbl in tables:
                info = context.get(tbl, {})
                cols = info.get("columns", [])
                sample = info.get("sample", [])
                col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in cols)
                parts.append(f"表名: {tbl}")
                parts.append(f"字段: {col_desc or '无法获取'}")
                if sample:
                    parts.append(f"样数据(前3行): {json.dumps(sample, ensure_ascii=False)}")
                parts.append("")

        # 2. Selected Uploaded Files
        if selected_files:
            parts.append("【已加载的本地文件详述 - Ground Truth】")
            for fname in selected_files:
                info = context.get(fname, {})
                cols = info.get("columns", [])
                sample = info.get("sample", [])
                path = upload_paths.get(fname, "未知路径")
                
                col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in cols)
                parts.append(f"文件名: {fname}")
                parts.append(f"实际物理路径: {path}")
                if cols:
                    parts.append(f"字段: {col_desc}")
                
                text_preview = info.get("text_preview")
                if text_preview:
                    parts.append(f"文件内容摘要/预览: \n{text_preview}")
                
                if sample:
                    parts.append(f"样数据(前3行): {json.dumps(sample, ensure_ascii=False)}")
                parts.append("")

    parts.append(f"用户问题: {message}")
    parts.append("【指令约束】")
    parts.append("- 请合理编排 SQL 和 Python 步骤。")
    parts.append("- SQL 结果会自动以 df0, df1... 注入 Python 变量，无需手动转换。")
    parts.append("- 如果涉及多表对比，请分别写 SQL 获取数据，然后在 Python 中合并分析。")
    parts.append("- **处理本地文件**：如果需要处理上传的文件，请直接在 Python 步骤中加载。")
    parts.append("  - 表格文件 (Excel/CSV): 使用 `pd.read_excel(uploaded_file_paths['文件名'])` 或 `pd.read_csv(...)`。")
    parts.append("  - 文本/知识文件 (TXT/JSON/MD): 使用 `Path(uploaded_file_paths['文件名']).read_text(encoding='utf-8')` 或标准 `open()`。")
    parts.append("- 系统已预先注入了 `uploaded_file_paths` (物理路径字典) 和 `uploaded_dataframes` (已预加载的表格字典)。")

    return "\n".join(parts)

    return "\n".join(parts)


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

    explanation = str(parsed.get("explanation", "")) or "已完成本轮分析。"

    yield {
        "type": "result",
        "data": {
            "steps": normalized_steps,
            "tools_used": tools_used,
            "conclusions": normalized_conclusions,
            "hypotheses": normalized_hypotheses,
            "action_items": action_items,
            "explanation": explanation,
        },
    }


def _run_iteration_by_rules(message: str, sandbox: dict) -> Generator[dict, None, None]:
    """Fallback when no LLM is configured."""
    table = (sandbox.get("tables") or [""])[0]
    if not table:
        raise RuntimeError("当前沙盒没有可用数据表")

    yield {"type": "thought", "content": "当前未启用 LLM，返回通用探查分析；建议配置 LLM 以实现 AI 自主迭代分析。"}

    sql = f"SELECT * FROM {table} LIMIT 200"
    yield {
        "type": "result",
        "data": {
            "steps": [{"tool": "sql", "code": sql}],
            "tools_used": ["execute_select_sql"],
            "conclusions": [
                {"text": f"通用数据探查：从 {table} 取样 200 行。建议配置 LLM 以获得自主分析能力。", "confidence": 1.0},
            ],
            "hypotheses": [
                {"id": "h1", "text": "补充业务目标与时间范围，便于 AI 自动规划分析路径"},
                {"id": "h2", "text": "上传本地 CSV/Excel，与线上数据做联合分析"},
                {"id": "h3", "text": "配置 LLM 后开启智能迭代分析"},
            ],
            "action_items": ["配置 LLM provider 以启用 AI 自主分析能力"],
            "explanation": "当前为 mock 模式，仅提供通用数据探查。",
        },
    }


# ── LLM protocol implementations (unchanged) ─────────────────────────


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
                            yield content
                    except json.JSONDecodeError:
                        continue


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
                                yield delta.get("text", "")
                    except json.JSONDecodeError:
                        continue


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
    selected_provider = (provider or config.llm_provider).lower()

    system_prompt = "你是一个业务知识提炼专家。请根据用户的提问、分析过程和结论，提取一个可复用的“分析技能”。"
    user_prompt = f"""
用户问题: {message}
沙盒名称: {sandbox_name}
分析结论: {json.dumps(analysis_result.get('conclusions', []), ensure_ascii=False)}
分析步骤: {json.dumps(analysis_result.get('steps', []), ensure_ascii=False)}
核心解释: {analysis_result.get('explanation', '')}

请返回一个 JSON 对象，包含以下字段：
1. "name": 技能名称（与整个对话内容高度相关，并且简洁）
2. "description": 技能描述（要非常详细的描述）
3. "tags": 关键词标签列表（3-5个）
4. "knowledge": 提炼的核心业务知识（要非常详细的业务知识，包含交互流程、业务规则、指标口径、字段说明等所有知识，要让一个普通人拿到这个技能描述能直接用起来例如：某某指标计算公式、业务判定逻辑、关键字段的业务含义。每条知识点要独立且精确，可以被后续对话直接参考）

仅返回 JSON，不要任何解释文字。
"""

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
