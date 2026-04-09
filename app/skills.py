from datetime import datetime, timezone

from app.authorization import filter_tables_by_user
from app.store import User, store
from app.i18n import t


def _dedupe_non_empty(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _build_context_snapshot(user: User, proposal: dict, session_id: str | None) -> dict:
    sandbox_id = str(proposal.get("sandbox_id") or "").strip()
    sandbox = store.get_sandbox(sandbox_id) if sandbox_id else None
    sandbox = sandbox or {}

    session_data = store.get_session(user.user_id, session_id) if session_id else None
    session_data = session_data or {}
    iterations = session_data.get("iterations") or []
    last_iteration_id = ""
    if iterations:
        last_iteration_id = str((iterations[-1] or {}).get("iteration_id") or "").strip()

    session_title = str(session_data.get("title") or "").strip()
    sandbox_name = str(sandbox.get("name") or "").strip()
    mode = str(proposal.get("mode") or "manual").strip() or "manual"
    message = str(proposal.get("message") or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    db_cfg = sandbox.get("db_connection")
    if not isinstance(db_cfg, dict):
        db_cfg = {}

    selected_tables = _dedupe_non_empty([str(item) for item in (proposal.get("selected_tables") or [])])
    sandbox_tables = _dedupe_non_empty([str(item) for item in (sandbox.get("tables") or [])])

    selected_files = _dedupe_non_empty([str(item) for item in (proposal.get("selected_files") or [])])
    selected_files_set = set(selected_files)
    upload_paths = sandbox.get("upload_paths") or {}
    uploads = sandbox.get("uploads") or {}
    file_names = _dedupe_non_empty(
        selected_files
        + [str(name) for name in upload_paths.keys()]
        + [str(name) for name in uploads.keys()]
    )
    files = [
        {
            "name": name,
            "path": str(upload_paths.get(name) or "").strip(),
            "selected": name in selected_files_set,
        }
        for name in file_names
    ]

    mounted_skill_ids = _dedupe_non_empty([str(item) for item in (sandbox.get("mounted_skills") or [])])
    mounted_skills: list[dict] = []
    for skill_id in mounted_skill_ids:
        skill = store.skills.get(skill_id)
        mounted_skills.append(
            {
                "skill_id": skill_id,
                "name": str((skill or {}).get("name") or skill_id).strip(),
                "version": (skill or {}).get("version"),
            }
        )

    kb_ids = _dedupe_non_empty([str(item) for item in (sandbox.get("knowledge_bases") or [])])
    knowledge_bases: list[dict] = []
    for kb_id in kb_ids:
        kb = store.get_knowledge_base(kb_id)
        knowledge_bases.append(
            {
                "id": kb_id,
                "name": str((kb or {}).get("name") or kb_id).strip(),
                "sync_type": str((kb or {}).get("sync_type") or "").strip(),
            }
        )

    virtual_views_raw = sandbox.get("virtual_views") or []
    virtual_views: list[dict] = []
    for view in virtual_views_raw:
        if not isinstance(view, dict):
            continue
        view_name = str(view.get("name") or "").strip()
        if not view_name:
            continue
        virtual_views.append(
            {
                "view_id": str(view.get("view_id") or "").strip(),
                "name": view_name,
                "description": str(view.get("description") or "").strip(),
                "source_run_id": str(view.get("source_run_id") or "").strip(),
                "columns": view.get("columns") or [],
                "sample_rows": (view.get("sample_rows") or [])[:3],
                "source_sql_summary": str(view.get("sql") or "").replace("\n", " ").strip()[:320],
            }
        )

    session_patches = _dedupe_non_empty([str(item) for item in (proposal.get("session_patches") or [])])
    sandbox_business_knowledge = _dedupe_non_empty(store.get_business_knowledge(sandbox_id) if sandbox_id else [])
    report_meta = proposal.get("report_meta") or {}

    source = {
        "session_id": session_id or "",
        "session_title": session_title,
        "proposal_id": str(proposal.get("proposal_id") or "").strip(),
        "sandbox_id": sandbox_id,
        "sandbox_name": sandbox_name,
        "mode": mode,
        "message": message,
        "saved_at": now_iso,
    }
    if last_iteration_id:
        source["last_iteration_id"] = last_iteration_id

    return {
        "source": source,
        "conversation_link": {
            "dashboard_path": f"/web/dashboard.html?session_id={session_id}" if session_id else "",
        },
        "database": {
            "db_type": db_cfg.get("db_type"),
            "host": db_cfg.get("host"),
            "port": db_cfg.get("port"),
            "database": db_cfg.get("database"),
            "username": db_cfg.get("username"),
        },
        "tables": {
            "selected_tables": selected_tables,
            "sandbox_tables": sandbox_tables,
        },
        "mounted_skills": mounted_skills,
        "knowledge_bases": knowledge_bases,
        "virtual_views": virtual_views,
        "files": files,
        "context_sources": {
            "selected_tables": bool(selected_tables),
            "selected_files": bool(selected_files),
            "mounted_skills": bool(mounted_skills),
            "knowledge_bases": bool(knowledge_bases),
            "virtual_views": bool(virtual_views),
            "session_patches": bool(session_patches),
            "sandbox_business_knowledge": bool(sandbox_business_knowledge),
        },
        "session_patches": session_patches,
        "report": {
            "report_title": str(proposal.get("report_title") or "").strip(),
            "stop_reason": str(report_meta.get("stop_reason") or "").strip(),
            "rounds_completed": report_meta.get("rounds_completed"),
            "max_rounds_hit": bool(report_meta.get("max_rounds_hit", False)),
        },
    }


def save_skill_from_proposal(
    user: User,
    proposal_id: str,
    name: str,
    description: str | None = None,
    tags: list[str] | None = None,
    extra_knowledge: list[str] | None = None,
    table_descriptions: list[dict] | None = None,
    session_id: str | None = None,
    overwrite_skill_id: str | None = None,
) -> dict:
    proposal = store.proposals.get(proposal_id)
    if not proposal:
        raise ValueError(t("error_proposal_not_found", default="提案不存在"))
    if proposal["user_id"] != user.user_id:
        raise PermissionError(t("error_no_permission_save_proposal", default="仅可保存自己的提案"))
    if proposal["status"] != "executed":
        raise ValueError(t("error_not_executed_proposal", default="仅可保存已执行提案"))

    sandbox = store.get_sandbox(str(proposal.get("sandbox_id") or "").strip()) or {}
    inherited_tables = filter_tables_by_user(user, proposal["tables"])
    sandbox_virtual_view_names = {
        str(view.get("name") or "").strip()
        for view in (sandbox.get("virtual_views") or [])
        if isinstance(view, dict) and str(view.get("name") or "").strip()
    }
    sandbox_virtual_view_map = {
        str(view.get("name") or "").strip(): view
        for view in (sandbox.get("virtual_views") or [])
        if isinstance(view, dict) and str(view.get("name") or "").strip()
    }
    inherited_tables = _dedupe_non_empty(inherited_tables + [
        str(table_name)
        for table_name in (proposal.get("tables") or [])
        if str(table_name).strip() in sandbox_virtual_view_names
    ])
    steps = proposal.get("steps", [])
    if not steps:
        for round_payload in proposal.get("loop_rounds", []):
            for step in ((round_payload.get("result") or {}).get("steps") or []):
                if isinstance(step, dict):
                    steps.append(step)

    session_knowledge: list[str] = []
    if session_id:
        session_knowledge = store.get_session_knowledge(user.user_id, session_id)
    knowledge_layer = list(dict.fromkeys(session_knowledge + (extra_knowledge or [])))

    table_descs: list[dict] = list(table_descriptions or [])
    existing_desc_tables = {td["table"] for td in table_descs}
    for table_name in inherited_tables:
        if table_name not in existing_desc_tables:
            virtual_view_info = sandbox_virtual_view_map.get(table_name, {})
            table_descs.append(
                {
                    "table": table_name,
                    "description": str(virtual_view_info.get("description") or ""),
                }
            )

    sql_template = ""
    for step in steps:
        if step.get("tool") == "sql":
            sql_template = step.get("code", "")
            break

    context_snapshot = _build_context_snapshot(user=user, proposal=proposal, session_id=session_id)

    payload = {
        "name": name,
        "description": description or "",
        "tags": tags or [],
        "layers": {
            "knowledge": knowledge_layer,
            "tables": table_descs,
            "steps": steps,
            "context_snapshot": context_snapshot,
        },
        "sql_template": sql_template,
        "inherited_tables": inherited_tables,
        "session_patches": list(proposal.get("session_patches", [])),
    }

    if overwrite_skill_id:
        existing_skill = store.skills.get(overwrite_skill_id)
        if not existing_skill:
            raise ValueError(t("error_skill_not_found", default="要覆盖的经验不存在"))
        if existing_skill["owner_id"] != user.user_id:
            raise PermissionError(t("error_no_permission_skill", default="无权修改该经验"))

        history_entry = {
            "version": existing_skill.get("version", 1),
            "name": existing_skill.get("name"),
            "description": existing_skill.get("description"),
            "tags": existing_skill.get("tags"),
            "layers": existing_skill.get("layers"),
            "updated_at": existing_skill.get("updated_at") or existing_skill.get("created_at"),
        }

        history = existing_skill.get("history") or []
        history.append(history_entry)

        payload["version"] = existing_skill.get("version", 1) + 1
        payload["history"] = history
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()

        store.update_skill(overwrite_skill_id, payload)

        return {
            "skill_id": overwrite_skill_id,
            **payload,
            "owner_id": user.user_id,
            "created_at": existing_skill.get("created_at"),
        }

    payload["owner_id"] = user.user_id
    payload["owner_name"] = user.display_name
    payload["groups"] = user.groups
    skill_id = store.create_skill(payload)
    return {"skill_id": skill_id, **payload, "version": 1, "history": []}


def list_skills(user: User) -> list[dict]:
    output = []
    for skill_id, item in store.skills.items():
        if item["owner_id"] == user.user_id or set(item["groups"]).intersection(user.groups):
            output.append({"skill_id": skill_id, **item})
    output.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return output


def build_context_snapshot_for_proposal(user: User, proposal_id: str) -> dict:
    proposal = store.proposals.get(proposal_id)
    if not proposal:
        raise ValueError(t("error_proposal_not_found", default="提案不存在"))
    if proposal.get("user_id") != user.user_id:
        raise PermissionError(t("error_no_permission_proposal", default="无权访问该提案"))
    session_id = str(proposal.get("session_id") or "").strip() or None
    return _build_context_snapshot(user=user, proposal=proposal, session_id=session_id)
