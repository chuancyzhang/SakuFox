from datetime import datetime, timezone

from app.authorization import filter_tables_by_user
from app.store import User, store
from app.i18n import t


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

    inherited_tables = filter_tables_by_user(user, proposal["tables"])
    steps = proposal.get("steps", [])
    if not steps:
        for round_payload in proposal.get("loop_rounds", []):
            for step in ((round_payload.get("result") or {}).get("steps") or []):
                if isinstance(step, dict):
                    steps.append(step)

    # ── Layer 1: Knowledge (business rules / patches) ──────────────────
    session_knowledge: list[str] = []
    if session_id:
        session_knowledge = store.get_session_knowledge(user.user_id, session_id)
    knowledge_layer = list(dict.fromkeys(session_knowledge + (extra_knowledge or [])))

    # ── Layer 2: Table context ─────────────────────────────────────────
    table_descs: list[dict] = list(table_descriptions or [])
    existing_desc_tables = {td["table"] for td in table_descs}
    for t in inherited_tables:
        if t not in existing_desc_tables:
            table_descs.append({"table": t, "description": ""})

    # ── Layer 3: Analysis steps ────────────────────────────────────────
    sql_template = ""
    for s in steps:
        if s.get("tool") == "sql":
            sql_template = s.get("code", "")
            break

    payload = {
        "name": name,
        "description": description or "",
        "tags": tags or [],
        "layers": {
            "knowledge": knowledge_layer,
            "tables": table_descs,
            "steps": steps,
        },
        # Backward-compat
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
        
        # Build history entry
        history_entry = {
            "version": existing_skill.get("version", 1),
            "name": existing_skill.get("name"),
            "description": existing_skill.get("description"),
            "tags": existing_skill.get("tags"),
            "layers": existing_skill.get("layers"),
            "updated_at": existing_skill.get("updated_at") or existing_skill.get("created_at")
        }
        
        history = existing_skill.get("history") or []
        history.append(history_entry)
        
        payload["version"] = existing_skill.get("version", 1) + 1
        payload["history"] = history
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        store.update_skill(overwrite_skill_id, payload)
        
        return {"skill_id": overwrite_skill_id, **payload, "owner_id": user.user_id, "created_at": existing_skill.get("created_at")}
    else:
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
