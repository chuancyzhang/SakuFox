import json
import io
import uuid
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi import Request

from app.i18n import set_lang, t

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
    CreateKnowledgeBaseRequest,
    UpdateKnowledgeBaseRequest,
    MountKnowledgeBasesRequest
)
from app.python_sandbox import run_python_pipeline
from app.skills import list_skills, save_skill_from_proposal
from app.tools import execute_select_sql_with_mask
from app.store import User, store

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

    iteration_history = store.get_iteration_history(user.user_id, session_id)
    
    # Merge global knowledge bases and sandbox-specific business knowledge
    business_knowledge = []
    kb_ids = sandbox.get("knowledge_bases", [])
    for kb_id in kb_ids:
        kb = store.get_knowledge_base(kb_id)
        if kb and kb.get("content"):
            business_knowledge.append(f"[{kb.get('name')}]: {kb.get('content')}")
            
    # Append local business knowledge
    business_knowledge.extend(store.get_business_knowledge(req.sandbox_id))

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
                # Sanitize for JSON compatibility (handles NaN/Inf)
                from app.utils import sanitize_for_json
                exec_result = sanitize_for_json(exec_result)

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
            internal_error = t("error_internal", default="服务器内部错误")
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


@app.get("/api/chat/sessions")
def list_sessions(user: User = Depends(get_current_user)):
    """List all sessions for the current user."""
    return {"sessions": store.list_sessions(user.user_id)}


@app.delete("/api/chat/sessions/{session_id}")
def delete_session(session_id: str, user: User = Depends(get_current_user)):
    ok = store.delete_session(user.user_id, session_id)
    if not ok:
        raise HTTPException(status_code=404, detail=t("error_session_not_found"))
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
    
    sandbox = store.sandboxes.get(req.sandbox_id)
    unnamed = t("msg_sandbox_unnamed", default="未命名沙盒")
    unknown = t("msg_sandbox_unknown", default="未知沙盒")
    sandbox_name = sandbox.get("name", unnamed) if sandbox else unknown
    
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
        raise HTTPException(status_code=400, detail=f"{t('error_db_config', default='连接配置错误')}: {str(exc)}")

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
    return {"sandbox_id": sandbox_id, "db_config": db_config, "tables": table_names, "message": t("msg_db_registered", default="数据库连接已注册，下次迭代将自动使用外部数据库")}


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

import pandas as pd
from sqlalchemy import text

def _query_rows(sql: str, sandbox_id: str | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Preserves aliases naturally."""
    if sandbox_id:
        engine = store.get_sandbox_engine(sandbox_id)
        if engine is not None:
            # External engine
            return pd.read_sql(sql, engine)
    
    # Internal context
    return pd.read_sql(sql, store.conn)


def _auto_execute(result_data: dict, allowed_tables: list[str], upload_rows: dict[str, list[dict]], upload_paths: dict[str, str], sandbox_id: str | None = None) -> dict:
    """
    Seamless execution engine (notebook-like):
    - Shared namespace across all steps.
    - Fail-fast: stop on first error.
    - Implicit dfN binding.
    """
    steps = result_data.get("steps", [])
    if not isinstance(steps, list):
        steps = []

    shared_namespace: dict = {}
    step_results: list[dict] = []
    all_rows: list[dict] = []
    all_tables: list[str] = []
    all_chart_specs: list[dict] = []

    for i, step in enumerate(steps):
        tool = step.get("tool", "").lower()
        code = step.get("code", "").strip()
        if not code:
            step_results.append({"rows": [], "tables": [], "error": t("error_empty_code", default="空代码")})
            continue

        if tool == "sql":
            try:
                # Use a wrapper that returns DataFrame
                def df_query_executor(s):
                    return _query_rows(s, sandbox_id).to_dict(orient="records")

                rows, used_tables = execute_select_sql_with_mask(
                    sql=code,
                    allowed_tables=allowed_tables,
                    query_executor=df_query_executor,
                )
                
                step_results.append({"rows": rows, "tables": used_tables})
                
                # Bind to namespace as df{i} and df
                step_df = pd.DataFrame(rows)
                shared_namespace[f"df{i}"] = step_df
                shared_namespace["df"] = step_df
                
                all_rows = rows
                all_tables.extend(t for t in used_tables if t not in all_tables)
            except Exception as exc:
                error_msg = t("error_sql_failed", step=i+1, default=f"SQL 执行失败 (step {i+1})") + f": {str(exc)}"
                step_results.append({"rows": [{"error": error_msg}], "tables": []})
                return {"step_results": step_results, "error": error_msg}

        elif tool == "python":
            try:
                def sql_tool(s: str) -> list[dict]:
                    return _query_rows(s, sandbox_id).to_dict(orient="records")

                python_result = run_python_pipeline(
                    python_code=code,
                    shared_namespace=shared_namespace,
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
                error_msg = t("error_python_failed", step=i+1, default=f"Python 执行失败 (step {i+1})") + f": {str(exc)}"
                step_results.append({"rows": [{"error": error_msg}], "tables": all_tables})
                return {"step_results": step_results, "error": error_msg}
        else:
            step_results.append({"rows": [], "tables": [], "error": t("error_unknown_tool", tool=tool, default=f"未知工具: {tool}")})

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
        raise HTTPException(status_code=400, detail=t("error_max_tables", max=max_selected_tables, default=f"最多可选择 {max_selected_tables} 张表"))
    denied = [t for t in normalized if t not in allowed_sandbox_tables]
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
        store.update_sandbox(sandbox_id, {"knowledge_bases": req.knowledge_bases})
        return {"sandbox_id": sandbox_id, "knowledge_bases": req.knowledge_bases}
    except (ValueError, PermissionError) as exc:
        raise HTTPException(status_code=403, detail=str(exc))

