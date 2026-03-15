import json
import io
import uuid
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.agent import run_analysis_iteration, generate_data_insight, generate_skill_proposal
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
    IterateRequest,
    LoginRequest,
    SaveSkillRequest,
    UpdateSessionRequest,
    ProposeSkillRequest,
    UpdateSkillRequest,
    CreateSandboxRequest,
    RenameSandboxRequest,
)
from app.python_sandbox import run_python_pipeline
from app.skills import list_skills, save_skill_from_proposal
from app.tools import execute_select_sql_with_mask
from app.store import User, store

app = FastAPI(title="SakuFox 🦊 - 敏捷智能数据分析平台")
web_dir = Path(__file__).resolve().parent.parent / "web"
app.mount("/web", StaticFiles(directory=str(web_dir)), name="web")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(web_dir / "dashboard.html"))


@app.get("/dashboard")
def dashboard() -> FileResponse:
    return FileResponse(str(web_dir / "dashboard.html"))


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
    if not session.get("title"):
        title = req.message[:40].strip()
        store.update_session_title(user.user_id, session_id, title)
    if not session.get("sandbox_id"):
        session["sandbox_id"] = req.sandbox_id

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
                    message = f"[基于上轮猜想: {h['text']}] {message}"
                    break

    iteration_history = store.get_iteration_history(user.user_id, session_id)
    business_knowledge = store.get_business_knowledge(req.sandbox_id)

    def stream_generator():
        try:
            result_data = None
            for event in run_analysis_iteration(
                message=message,
                sandbox=analysis_sandbox,
                iteration_history=iteration_history,
                business_knowledge=business_knowledge,
                provider=req.provider,
                model=req.model,
            ):
                if event.get("type") == "result":
                    result_data = event["data"]
                yield json.dumps(event, ensure_ascii=False) + "\n"

            # Auto-execute: run SQL + Python if present
            if result_data:
                all_uploads = sandbox.get("uploads", {})
                all_upload_paths = sandbox.get("upload_paths", {})
                if req.selected_files is not None:
                    allowed_uploads = {k: v for k, v in all_uploads.items() if k in req.selected_files}
                    allowed_upload_paths = {k: v for k, v in all_upload_paths.items() if k in req.selected_files}
                else:
                    allowed_uploads = all_uploads
                    allowed_upload_paths = all_upload_paths

                exec_result = _auto_execute(
                    result_data=result_data,
                    allowed_tables=selected_tables,
                    upload_rows=allowed_uploads,
                    upload_paths=allowed_upload_paths,
                    sandbox_id=req.sandbox_id,
                )
                # Emit data rows
                if exec_result["rows"]:
                    yield json.dumps({"type": "data", "rows": exec_result["rows"][:200]}, ensure_ascii=False) + "\n"
                # Emit per-step results
                for idx, sr in enumerate(exec_result.get("step_results", [])):
                    yield json.dumps({
                        "type": "step_result",
                        "step_index": idx,
                        "data": {
                            "rows_count": len(sr.get("rows", [])),
                            "tables": sr.get("tables", []),
                            "error": sr.get("error", None),
                        },
                    }, ensure_ascii=False) + "\n"
                # Emit chart specs
                for spec in exec_result.get("chart_specs", []):
                    yield json.dumps({"type": "chart_spec", "data": spec}, ensure_ascii=False) + "\n"

                # Save iteration
                iteration_id = store.append_iteration(user.user_id, session_id, {
                    "message": message,
                    "steps": result_data.get("steps", []),
                    "conclusions": result_data.get("conclusions", []),
                    "hypotheses": result_data.get("hypotheses", []),
                    "action_items": result_data.get("action_items", []),
                    "tools_used": result_data.get("tools_used", []),
                    "result_rows": exec_result["rows"][:100],  # store compact
                    "chart_specs": exec_result.get("chart_specs", []),
                })

                # Also create a proposal record for skill-saving compatibility
                proposal_id = store.create_proposal({
                    "user_id": user.user_id,
                    "session_id": session_id,
                    "sandbox_id": req.sandbox_id,
                    "message": message,
                    "steps": result_data.get("steps", []),
                    "explanation": result_data.get("explanation", ""),
                    "tables": selected_tables,
                    "status": "executed",
                    "result_rows": exec_result["rows"],
                    "chart_specs": exec_result.get("chart_specs", []),
                    "selected_tables": selected_tables,
                    "session_patches": list(session.get("patches", [])),
                })

                # Emit final metadata
                yield json.dumps({
                    "type": "iteration_complete",
                    "data": {
                        "iteration_id": iteration_id,
                        "session_id": session_id,
                        "proposal_id": proposal_id,
                        "result_count": len(exec_result["rows"]),
                    },
                }, ensure_ascii=False) + "\n"

        except RuntimeError as exc:
            yield json.dumps({"type": "error", "message": str(exc)}, ensure_ascii=False) + "\n"
        except Exception as exc:
            yield json.dumps({"type": "error", "message": f"服务器内部错误: {str(exc)}"}, ensure_ascii=False) + "\n"

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
            "message": "业务知识已沉淀，后续分析将自动参考。",
        }
    else:
        store.append_patch(user.user_id, req.session_id, req.feedback)
        return {
            "session_id": req.session_id,
            "type": "feedback",
            "message": "反馈已记录，下次迭代将参考。",
        }


@app.get("/api/chat/history")
def iteration_history(session_id: str, user: User = Depends(get_current_user)):
    """Get iteration history for a session."""
    history = store.get_iteration_history(user.user_id, session_id)
    return {"session_id": session_id, "iterations": history}


@app.get("/api/chat/sessions")
def list_sessions(user: User = Depends(get_current_user)):
    """List all sessions for the current user."""
    return {"sessions": store.list_sessions(user.user_id)}


@app.delete("/api/chat/sessions/{session_id}")
def delete_session(session_id: str, user: User = Depends(get_current_user)):
    ok = store.delete_session(user.user_id, session_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
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
        raise HTTPException(status_code=404, detail="提案不存在")
    if proposal["user_id"] != user.user_id:
        raise HTTPException(status_code=403, detail="无权分析该提案")

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
            rows = df.head(5000).to_dict(orient="records")
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
    
    sandbox = store.sandboxes.get(req.sandbox_id)
    sandbox_name = sandbox.get("name", "未命名沙盒") if sandbox else "未知沙盒"
    
    # We use the final iteration's result as the source for summarization
    # Assuming proposal object contains the analysis result data
    suggestion = generate_skill_proposal(
        message=req.message,
        analysis_result=proposal, # proposal often is the dict representation of the final result
        sandbox_name=sandbox_name
    )
    return suggestion


@app.delete("/api/skills/{skill_id}")
def delete_skill(skill_id: str, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")
    if skill["owner_id"] != user.user_id:
        raise HTTPException(status_code=403, detail="无权删除他人技能")
    store.delete_skill(skill_id)
    return {"deleted": skill_id}


@app.get("/api/skills/{skill_id}")
def get_skill(skill_id: str, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")
    if skill["owner_id"] != user.user_id and not set(skill["groups"]).intersection(user.groups):
        raise HTTPException(status_code=403, detail="无权查看该技能")
    return {"skill_id": skill_id, **skill}



@app.patch("/api/skills/{skill_id}")
def update_skill(skill_id: str, req: UpdateSkillRequest, user: User = Depends(get_current_user)):
    skill = store.skills.get(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")
    if skill["owner_id"] != user.user_id:
        raise HTTPException(status_code=403, detail="无权修改他人技能")
    with store._lock:
        if req.name is not None:
            skill["name"] = req.name
        if req.description is not None:
            skill["description"] = req.description
        if req.tags is not None:
            skill["tags"] = req.tags
        if req.knowledge is not None:
            skill.setdefault("layers", {})["knowledge"] = req.knowledge
        if req.table_descriptions is not None:
            skill.setdefault("layers", {})["tables"] = req.table_descriptions
    return {"skill_id": skill_id, **skill}


@app.get("/api/skills")
def skills(user: User = Depends(get_current_user)):
    return {"skills": list_skills(user)}


# ── External DB connections ────────────────────────────────────────────


class DbConnectionRequest(SaveSkillRequest.__class__):
    pass


from pydantic import BaseModel  # noqa: E402 (already imported at top-level; needed here for inline model)

class RegisterDbRequest(BaseModel):
    db_type: str                  # mysql / postgresql / sqlite / oracle / impala
    host: str = "localhost"
    port: int | None = None
    database: str
    username: str = ""
    password: str = ""


class SaveTablesRequest(BaseModel):
    tables: list[str]

@app.post("/api/sandboxes/{sandbox_id}/db-connection")
def register_db_connection(
    sandbox_id: str,
    req: RegisterDbRequest,
    user: User = Depends(get_current_user),
):
    """Register an external database connection to a sandbox."""
    try:
        sandbox = assert_sandbox_access(user, sandbox_id)
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    try:
        cfg = DbConnectionConfig(
            db_type=req.db_type,
            host=req.host,
            port=req.port,
            database=req.database,
            username=req.username,
            password=req.password,
        )
        engine = get_engine(cfg)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"连接配置错误: {str(exc)}")

    db_config = {
        "db_type": req.db_type,
        "host": req.host,
        "port": req.port or cfg.port,
        "database": req.database,
        "username": req.username,
    }
    
    table_names = []
    try:
        table_names = get_table_names(engine)
    except Exception as exc:
        pass # If we fail to get tables for some reason, just return empty list

    store.register_sandbox_db(sandbox_id, engine, db_config)
    return {"sandbox_id": sandbox_id, "db_config": db_config, "tables": table_names, "message": "数据库连接已注册，下次迭代将自动使用外部数据库"}


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
        raise HTTPException(status_code=400, detail=f"最多只能选择 {MAX_SELECTED_TABLES} 张表")

    store.update_sandbox(sandbox_id, {
        "tables": req.tables,
        "allowed_tables": req.tables
    })
    
    return {"ok": True, "tables": req.tables}


@app.post("/api/sandboxes/{sandbox_id}/db-test")
def test_db_connection(
    sandbox_id: str,
    req: RegisterDbRequest,
    user: User = Depends(get_current_user),
):
    """Test an external database connection without registering it."""
    try:
        assert_sandbox_access(user, sandbox_id)
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))
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
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    return result


# ── Internal helpers ──────────────────────────────────────────────────


def _query_rows(sql: str, sandbox_id: str | None = None) -> list[dict]:
    """Execute SQL against external DB if configured, else built-in SQLite."""
    if sandbox_id:
        engine = store.get_sandbox_engine(sandbox_id)
        if engine is not None:
            return execute_external_sql(engine, sql)
    cur = store.conn.cursor()
    cur.execute(sql)
    return [dict(r) for r in cur.fetchall()]


def _auto_execute(result_data: dict, allowed_tables: list[str], upload_rows: dict[str, list[dict]], upload_paths: dict[str, str], sandbox_id: str | None = None) -> dict:
    """Execute a multi-step pipeline from an iteration result.

    The result_data contains a `steps` array. Each step is either:
      {"tool": "sql", "code": "SELECT ..."}
      {"tool": "python", "code": "..."}

    Steps are executed sequentially. Results from prior steps are passed
    to subsequent Python steps via the `step_results` list.
    """
    steps = result_data.get("steps", [])
    if not isinstance(steps, list):
        steps = []

    step_results: list[dict] = []
    all_rows: list[dict] = []
    all_tables: list[str] = []
    all_chart_specs: list[dict] = []
    last_sql_rows: list[dict] = []  # for df backward compat

    for i, step in enumerate(steps):
        tool = step.get("tool", "").lower()
        code = step.get("code", "").strip()
        if not code:
            step_results.append({"rows": [], "tables": [], "error": "空代码"})
            continue

        if tool == "sql":
            try:
                rows, used_tables = execute_select_sql_with_mask(
                    sql=code,
                    allowed_tables=allowed_tables,
                    query_executor=lambda s: _query_rows(s, sandbox_id),
                )
                step_results.append({"rows": rows, "tables": used_tables})
                all_rows = rows  # last SQL result becomes the primary rows
                last_sql_rows = rows
                all_tables.extend(t for t in used_tables if t not in all_tables)
            except Exception as exc:
                step_results.append({"rows": [{"error": f"SQL 执行失败 (step {i+1}): {str(exc)}"}], "tables": []})
                all_rows = step_results[-1]["rows"]

        elif tool == "python":
            def sql_tool(s: str) -> list[dict]:
                rows, _ = execute_select_sql_with_mask(
                    sql=s,
                    allowed_tables=allowed_tables,
                    query_executor=lambda x: _query_rows(x, sandbox_id),
                )
                return rows

            try:
                python_result = run_python_pipeline(
                    python_code=code,
                    seed_rows=last_sql_rows,
                    upload_rows=upload_rows,
                    upload_paths=upload_paths,
                    sql_tool=sql_tool,
                    step_results=step_results,
                )
                result_rows = python_result["rows"]
                result_charts = python_result.get("chart_specs", [])
                step_results.append({"rows": result_rows, "tables": all_tables, "chart_specs": result_charts})
                all_rows = result_rows
                all_chart_specs.extend(result_charts)
            except Exception as exc:
                step_results.append({"rows": [{"error": f"Python 执行失败 (step {i+1}): {str(exc)}"}], "tables": all_tables})
                all_rows = step_results[-1]["rows"]
        else:
            step_results.append({"rows": [], "tables": [], "error": f"未知工具: {tool}"})

    return {"rows": all_rows, "tables": all_tables, "chart_specs": all_chart_specs, "step_results": step_results}


def _resolve_selected_tables(requested_tables: list[str] | None, sandbox: dict, user: User, max_selected_tables: int) -> list[str]:
    # We already verified sandbox access in the caller via assert_sandbox_access.
    # Therefore, the user is authorized to access ALL tables registered to this sandbox.
    allowed_sandbox_tables = list(sandbox.get("tables", []))
    
    if requested_tables is None:
        return allowed_sandbox_tables[:max_selected_tables]
    normalized: list[str] = []
    for table in requested_tables:
        t = str(table).strip()
        if t and t not in normalized:
            normalized.append(t)
    if len(normalized) > max_selected_tables:
        raise HTTPException(status_code=400, detail=f"最多可选择 {max_selected_tables} 张表")
    denied = [t for t in normalized if t not in allowed_sandbox_tables]
    if denied:
        raise HTTPException(status_code=403, detail=f"无权选择表: {', '.join(denied)}")
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
    return {"sandbox_id": sandbox_id, "message": "工作空间创建成功"}

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
        return {"sandbox_id": sandbox_id, "name": sandbox["name"], "message": "重命名成功"}
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
        return {"ok": True, "message": "工作空间已删除"}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))
