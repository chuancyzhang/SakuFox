from datetime import datetime, timezone

from app.authorization import filter_tables_by_user
from app.store import User, store


def save_skill_from_proposal(
    user: User,
    proposal_id: str,
    name: str,
    description: str | None = None,
    tags: list[str] | None = None,
    extra_knowledge: list[str] | None = None,
    table_descriptions: list[dict] | None = None,
    session_id: str | None = None,
) -> dict:
    proposal = store.proposals.get(proposal_id)
    if not proposal:
        raise ValueError("提案不存在")
    if proposal["user_id"] != user.user_id:
        raise PermissionError("仅可保存自己的提案")
    if proposal["status"] != "executed":
        raise ValueError("仅可保存已执行提案")

    inherited_tables = filter_tables_by_user(user, proposal["tables"])
    steps = proposal.get("steps", [])

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
        "owner_id": user.user_id,
        "owner_name": user.display_name,
        "groups": user.groups,
        "layers": {
            "knowledge": knowledge_layer,
            "tables": table_descs,
            "steps": steps,
        },
        # Backward-compat
        "sql_template": sql_template,
        "inherited_tables": inherited_tables,
        "session_patches": list(proposal.get("session_patches", [])),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    skill_id = store.create_skill(payload)
    return {"skill_id": skill_id, **payload}


def list_skills(user: User) -> list[dict]:
    output = []
    for skill_id, item in store.skills.items():
        if item["owner_id"] == user.user_id or set(item["groups"]).intersection(user.groups):
            output.append({"skill_id": skill_id, **item})
    output.sort(key=lambda x: x["created_at"], reverse=True)
    return output
