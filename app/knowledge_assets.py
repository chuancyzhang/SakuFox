from __future__ import annotations

import json
import math
import mimetypes
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
from sqlalchemy import delete, desc, select
from sklearn.feature_extraction.text import HashingVectorizer

from app.db_models import DBKnowledgeAsset, DBKnowledgeChunk, DBKnowledgeIndexJob


_VECTORIZER = HashingVectorizer(
    n_features=128,
    alternate_sign=False,
    norm="l2",
    analyzer="char_wb",
    ngram_range=(2, 4),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dedupe_non_empty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _hash_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _extract_keywords(text: str, limit: int = 40) -> list[str]:
    raw = str(text or "").lower()
    ascii_words = re.findall(r"[a-z0-9_]{2,}", raw)
    cjk_chars = re.findall(r"[\u4e00-\u9fff]", str(text or ""))
    cjk_tokens: list[str] = []
    if cjk_chars:
        cjk_tokens.extend(cjk_chars[:limit])
        cjk_tokens.extend("".join(cjk_chars[idx : idx + 2]) for idx in range(max(len(cjk_chars) - 1, 0)))
    return _dedupe_non_empty(ascii_words + cjk_tokens)[:limit]


def _embed_text(text: str) -> list[float]:
    normalized = str(text or "").strip() or "empty"
    try:
        vector = _VECTORIZER.transform([normalized]).toarray()[0]
        return [round(float(value), 6) for value in vector.tolist()]
    except Exception:
        return []


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    numerator = sum(float(x) * float(y) for x, y in zip(a, b))
    denom_a = math.sqrt(sum(float(x) * float(x) for x in a))
    denom_b = math.sqrt(sum(float(y) * float(y) for y in b))
    if denom_a <= 0 or denom_b <= 0:
        return 0.0
    return float(numerator / (denom_a * denom_b))


def _chunk_text(text: str, chunk_size: int = 900, overlap: int = 180) -> list[str]:
    normalized = str(text or "").strip()
    if not normalized:
        return []
    if len(normalized) <= chunk_size:
        return [normalized]
    output: list[str] = []
    cursor = 0
    step = max(chunk_size - overlap, 1)
    while cursor < len(normalized):
        chunk = normalized[cursor : cursor + chunk_size].strip()
        if chunk:
            output.append(chunk)
        cursor += step
    return output


def _asset_locator(asset_id: str) -> str:
    return f"asset://{asset_id}"


def _guess_content_type(source_path: str, fallback: str = "text/plain") -> str:
    guess = mimetypes.guess_type(str(source_path or ""))[0]
    return str(guess or fallback)


def _read_text_file(path: str) -> str:
    file_path = Path(str(path or ""))
    if not file_path.exists():
        return ""
    for encoding in ("utf-8", "utf-8-sig", "gbk", "latin-1"):
        try:
            return file_path.read_text(encoding=encoding)
        except Exception:
            continue
    try:
        return file_path.read_bytes().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _build_upload_asset_content(filename: str, rows: list[dict], source_path: str) -> tuple[str, str]:
    suffix = Path(str(source_path or filename or "")).suffix.lower()
    text_extensions = {".txt", ".md", ".json", ".log", ".csv", ".yaml", ".yml", ".sql"}
    if suffix in text_extensions:
        content = _read_text_file(source_path)
        if content:
            return content, _guess_content_type(source_path)
    if suffix in {".xlsx", ".xls"} and Path(str(source_path or "")).exists():
        try:
            frame = pd.read_excel(source_path)
            return frame.head(500).to_json(orient="records", force_ascii=False, indent=2), "application/vnd.ms-excel"
        except Exception:
            pass
    if rows:
        try:
            return json.dumps(rows[:1000], ensure_ascii=False, indent=2), "application/json"
        except Exception:
            return str(rows[:1000]), "application/json"
    text = _read_text_file(source_path)
    if text:
        return text, _guess_content_type(source_path)
    return "", _guess_content_type(source_path)


def _build_skill_asset_content(skill: dict) -> str:
    layers = skill.get("layers") or {}
    knowledge_lines = [str(item).strip() for item in (layers.get("knowledge") or []) if str(item).strip()]
    table_lines = []
    for table in (layers.get("tables") or []):
        if not isinstance(table, dict):
            continue
        table_name = str(table.get("table") or "").strip()
        table_desc = str(table.get("description") or "").strip()
        if table_name:
            table_lines.append(f"- {table_name}: {table_desc}")
    snapshot = layers.get("context_snapshot") or {}
    source = snapshot.get("source") or {}
    sections = [
        f"# {skill.get('name') or skill.get('skill_id')}",
        "",
        str(skill.get("description") or "").strip(),
        "",
        "## Knowledge",
        "\n".join(f"- {line}" for line in knowledge_lines) or "- N/A",
        "",
        "## Tables",
        "\n".join(table_lines) or "- N/A",
        "",
        "## Source",
        f"- sandbox_id: {source.get('sandbox_id', '')}",
        f"- sandbox_name: {source.get('sandbox_name', '')}",
        f"- session_id: {source.get('session_id', '')}",
        f"- proposal_id: {source.get('proposal_id', '')}",
    ]
    return "\n".join(sections).strip()


def _preview(content: str) -> str:
    text = str(content or "").strip()
    if len(text) > 500:
        return text[:500].rstrip() + "..."
    return text


def _record_index_job(store: Any, *, asset_id: str | None, scope: str, status: str, message: str, stats: dict | None = None) -> None:
    now = _now_iso()
    with store.SessionFactory() as sess:
        sess.add(
            DBKnowledgeIndexJob(
                job_id=f"kij_{uuid.uuid4().hex[:12]}",
                asset_id=asset_id,
                scope=scope,
                status=status,
                message=message,
                stats=stats or {},
                created_at=now,
                updated_at=now,
            )
        )
        sess.commit()


def _upsert_asset_record(store: Any, *, asset_type: str, title: str, description: str, source_type: str, source_ref: str, source_path: str, sandbox_id: str | None, owner_id: str | None, permissions: list[str], content_type: str, content_preview: str, content_hash: str, metadata_json: dict | None = None) -> str:
    now = _now_iso()
    sandbox_key = str(sandbox_id or "")
    with store.SessionFactory() as sess:
        rows = sess.execute(
            select(DBKnowledgeAsset).where(
                DBKnowledgeAsset.source_type == source_type,
                DBKnowledgeAsset.source_ref == source_ref,
            )
        ).scalars().all()
        asset = None
        for row in rows:
            if str(row.sandbox_id or "") == sandbox_key:
                asset = row
                break
        if asset is None:
            asset = DBKnowledgeAsset(asset_id=f"asset_{uuid.uuid4().hex[:12]}", created_at=now)
            sess.add(asset)
        asset.asset_type = asset_type
        asset.title = title
        asset.description = description
        asset.source_type = source_type
        asset.source_ref = source_ref
        asset.source_path = source_path
        asset.sandbox_id = sandbox_id
        asset.owner_id = owner_id
        asset.permissions = permissions
        asset.status = "active"
        asset.content_type = content_type
        asset.content_hash = content_hash
        asset.content_preview = content_preview
        asset.metadata_json = metadata_json or {}
        asset.updated_at = now
        sess.commit()
        return str(asset.asset_id)


def _replace_asset_chunks(store: Any, *, asset_id: str, chunks: list[str], source_ref: str, source_path: str, content_hash: str) -> dict[str, int]:
    now = _now_iso()
    with store.SessionFactory() as sess:
        existing = sess.execute(
            select(DBKnowledgeChunk)
            .where(DBKnowledgeChunk.asset_id == asset_id)
            .order_by(desc(DBKnowledgeChunk.index_version))
        ).scalars().first()
        next_version = int(existing.index_version or 0) + 1 if existing else 1
        sess.execute(delete(DBKnowledgeChunk).where(DBKnowledgeChunk.asset_id == asset_id))
        for idx, chunk in enumerate(chunks):
            sess.add(
                DBKnowledgeChunk(
                    chunk_id=f"kc_{uuid.uuid4().hex[:12]}",
                    asset_id=asset_id,
                    chunk_index=idx,
                    chunk_text=chunk,
                    keywords=_extract_keywords(chunk),
                    embedding=_embed_text(chunk),
                    source_ref=source_ref,
                    source_path=source_path,
                    full_document_locator=_asset_locator(asset_id),
                    content_hash=content_hash,
                    index_version=next_version,
                    metadata_json={"char_count": len(chunk)},
                    created_at=now,
                    updated_at=now,
                )
            )
        sess.commit()
        return {"chunk_count": len(chunks), "index_version": next_version}


def _upsert_kb_asset(store: Any, kb: dict) -> str | None:
    if not kb:
        return None
    content = str(kb.get("content") or "").strip()
    content_hash = _hash_text(content)
    asset_id = _upsert_asset_record(
        store,
        asset_type="enterprise_kb",
        title=str(kb.get("name") or kb.get("id") or "").strip() or "Untitled KB",
        description=str(kb.get("description") or "").strip(),
        source_type="knowledge_base",
        source_ref=str(kb.get("id") or "").strip(),
        source_path="",
        sandbox_id=None,
        owner_id=None,
        permissions=[],
        content_type="text/markdown",
        content_preview=_preview(content),
        content_hash=content_hash,
        metadata_json={"sync_type": kb.get("sync_type", "manual")},
    )
    _replace_asset_chunks(store, asset_id=asset_id, chunks=_chunk_text(content or str(kb.get("description") or "").strip()), source_ref=str(kb.get("id") or "").strip(), source_path="", content_hash=content_hash)
    return asset_id


def _upsert_upload_asset(store: Any, sandbox_id: str, filename: str, rows: list[dict], source_path: str) -> str:
    sandbox = store.get_sandbox(sandbox_id) or {}
    content, content_type = _build_upload_asset_content(filename, rows, source_path)
    content_hash = _hash_text(content or json.dumps(rows[:50], ensure_ascii=False))
    asset_id = _upsert_asset_record(
        store,
        asset_type="uploaded_file",
        title=str(filename or "").strip(),
        description=f"Uploaded to {sandbox.get('name') or sandbox_id}",
        source_type="upload",
        source_ref=str(filename or "").strip(),
        source_path=str(source_path or ""),
        sandbox_id=sandbox_id,
        owner_id=None,
        permissions=list(sandbox.get("allowed_groups") or []),
        content_type=content_type,
        content_preview=_preview(content or json.dumps(rows[:3], ensure_ascii=False)),
        content_hash=content_hash,
        metadata_json={"row_count": len(rows), "sandbox_name": sandbox.get("name", "")},
    )
    _replace_asset_chunks(store, asset_id=asset_id, chunks=_chunk_text(content or json.dumps(rows[:200], ensure_ascii=False)), source_ref=str(filename or "").strip(), source_path=str(source_path or ""), content_hash=content_hash)
    return asset_id


def _upsert_skill_asset(store: Any, skill: dict) -> str | None:
    if not skill:
        return None
    content = _build_skill_asset_content(skill)
    content_hash = _hash_text(content)
    layers = skill.get("layers") or {}
    sandbox_id = str(((layers.get("context_snapshot") or {}).get("source") or {}).get("sandbox_id") or "").strip() or None
    asset_id = _upsert_asset_record(
        store,
        asset_type="experience",
        title=str(skill.get("name") or skill.get("skill_id") or "").strip() or "Untitled Experience",
        description=str(skill.get("description") or "").strip(),
        source_type="skill",
        source_ref=str(skill.get("skill_id") or "").strip(),
        source_path="",
        sandbox_id=sandbox_id,
        owner_id=str(skill.get("owner_id") or "").strip() or None,
        permissions=list(layers.get("groups") or skill.get("groups") or []),
        content_type="text/markdown",
        content_preview=_preview(content),
        content_hash=content_hash,
        metadata_json={"version": skill.get("version"), "tags": skill.get("tags") or []},
    )
    _replace_asset_chunks(store, asset_id=asset_id, chunks=_chunk_text(content), source_ref=str(skill.get("skill_id") or "").strip(), source_path="", content_hash=content_hash)
    return asset_id


def refresh_knowledge_assets(store: Any) -> None:
    kb_records = store.list_knowledge_bases()
    sandbox_records = store.list_sandboxes()
    skill_records = store.list_skills()
    live_keys: set[tuple[str, str, str]] = set()

    for kb in kb_records:
        ref = str(kb.get("id") or "").strip()
        if not ref:
            continue
        live_keys.add(("knowledge_base", ref, ""))
        _upsert_kb_asset(store, kb)

    for sandbox in sandbox_records:
        sandbox_id = str(sandbox.get("sandbox_id") or "").strip()
        uploads = sandbox.get("uploads") or {}
        upload_paths = sandbox.get("upload_paths") or {}
        for filename, rows in uploads.items():
            ref = str(filename or "").strip()
            if not ref:
                continue
            live_keys.add(("upload", ref, sandbox_id))
            _upsert_upload_asset(store, sandbox_id=sandbox_id, filename=ref, rows=rows if isinstance(rows, list) else [], source_path=str(upload_paths.get(ref) or ""))

    for skill in skill_records:
        ref = str(skill.get("skill_id") or "").strip()
        if not ref:
            continue
        sandbox_id = str(((skill.get("layers") or {}).get("context_snapshot") or {}).get("source", {}).get("sandbox_id") or "")
        live_keys.add(("skill", ref, sandbox_id))
        _upsert_skill_asset(store, skill)

    with store.SessionFactory() as sess:
        assets = sess.execute(select(DBKnowledgeAsset)).scalars().all()
        stale_ids: list[str] = []
        for asset in assets:
            key = (
                str(asset.source_type or ""),
                str(asset.source_ref or ""),
                "" if str(asset.source_type or "") == "knowledge_base" else str(asset.sandbox_id or ""),
            )
            if key not in live_keys:
                stale_ids.append(str(asset.asset_id))
        if stale_ids:
            sess.execute(delete(DBKnowledgeChunk).where(DBKnowledgeChunk.asset_id.in_(stale_ids)))
            sess.execute(delete(DBKnowledgeAsset).where(DBKnowledgeAsset.asset_id.in_(stale_ids)))
            sess.commit()


def get_asset_mounted_sandboxes(store: Any, asset_payload: dict) -> list[dict]:
    mounted: list[dict] = []
    for sandbox in store.list_sandboxes():
        sandbox_id = str(sandbox.get("sandbox_id") or "").strip()
        if asset_payload["source_type"] == "knowledge_base":
            if asset_payload["source_ref"] not in (sandbox.get("knowledge_bases") or []):
                continue
        elif asset_payload["source_type"] == "skill":
            if asset_payload["source_ref"] not in (sandbox.get("mounted_skills") or []):
                continue
        elif asset_payload["source_type"] == "upload":
            if sandbox_id != str(asset_payload.get("sandbox_id") or ""):
                continue
        else:
            continue
        mounted.append(
            {
                "sandbox_id": sandbox_id,
                "name": sandbox.get("name") or sandbox_id,
                "allowed_groups": sandbox.get("allowed_groups") or [],
            }
        )
    return mounted


def _asset_to_dict(store: Any, asset_row: DBKnowledgeAsset) -> dict[str, Any]:
    with store.SessionFactory() as sess:
        chunk_rows = sess.execute(select(DBKnowledgeChunk).where(DBKnowledgeChunk.asset_id == asset_row.asset_id)).scalars().all()
        jobs = sess.execute(select(DBKnowledgeIndexJob).where(DBKnowledgeIndexJob.asset_id == asset_row.asset_id).order_by(desc(DBKnowledgeIndexJob.created_at))).scalars().all()
    payload = store._sanitize_json(
        {
            "asset_id": asset_row.asset_id,
            "asset_type": asset_row.asset_type,
            "title": asset_row.title,
            "description": asset_row.description or "",
            "source_type": asset_row.source_type,
            "source_ref": asset_row.source_ref,
            "source_path": asset_row.source_path or "",
            "sandbox_id": asset_row.sandbox_id,
            "owner_id": asset_row.owner_id,
            "permissions": asset_row.permissions or [],
            "status": asset_row.status,
            "content_type": asset_row.content_type or "text/plain",
            "content_hash": asset_row.content_hash or "",
            "content_preview": asset_row.content_preview or "",
            "metadata": asset_row.metadata_json or {},
            "created_at": asset_row.created_at,
            "updated_at": asset_row.updated_at,
            "mounted_sandboxes": [],
            "chunk_count": len(chunk_rows),
            "embedding_count": sum(1 for chunk in chunk_rows if chunk.embedding),
            "index_version": max([int(chunk.index_version or 1) for chunk in chunk_rows], default=0),
            "index_status": jobs[0].status if jobs else ("success" if chunk_rows else "empty"),
            "last_indexed_at": jobs[0].updated_at if jobs else asset_row.updated_at,
            "last_error": jobs[0].message if jobs and jobs[0].status == "failed" else "",
            "full_document_locator": _asset_locator(str(asset_row.asset_id)),
        }
    )
    payload["mounted_sandboxes"] = get_asset_mounted_sandboxes(store, payload)
    return payload


def _can_user_view_asset(store: Any, asset: dict, user_id: str, user_groups: list[str]) -> bool:
    asset_type = str(asset.get("asset_type") or "")
    if asset_type == "uploaded_file":
        sandbox = store.get_sandbox(str(asset.get("sandbox_id") or ""))
        if not sandbox:
            return False
        return bool(set(sandbox.get("allowed_groups") or []).intersection(user_groups))
    if asset_type == "experience":
        owner_id = str(asset.get("owner_id") or "").strip()
        permissions = set(asset.get("permissions") or [])
        return owner_id == user_id or not permissions or bool(permissions.intersection(user_groups))
    return True


def list_knowledge_assets(store: Any, user_id: str | None = None, user_groups: list[str] | None = None) -> list[dict]:
    refresh_knowledge_assets(store)
    with store.SessionFactory() as sess:
        rows = sess.execute(select(DBKnowledgeAsset).order_by(desc(DBKnowledgeAsset.updated_at))).scalars().all()
    assets = [_asset_to_dict(store, row) for row in rows]
    if user_id is None:
        return assets
    return [asset for asset in assets if _can_user_view_asset(store, asset, user_id, list(user_groups or []))]


def get_knowledge_asset(store: Any, asset_id: str) -> dict | None:
    with store.SessionFactory() as sess:
        asset = sess.get(DBKnowledgeAsset, asset_id)
    if not asset:
        return None
    return _asset_to_dict(store, asset)


def get_knowledge_index_jobs(store: Any, asset_id: str | None = None, limit: int = 50) -> list[dict]:
    with store.SessionFactory() as sess:
        stmt = select(DBKnowledgeIndexJob)
        if asset_id:
            stmt = stmt.where(DBKnowledgeIndexJob.asset_id == asset_id)
        rows = sess.execute(stmt.order_by(desc(DBKnowledgeIndexJob.created_at))).scalars().all()
    return [
        {
            "job_id": row.job_id,
            "asset_id": row.asset_id,
            "scope": row.scope,
            "status": row.status,
            "message": row.message,
            "stats": row.stats or {},
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in rows[:limit]
    ]


def get_knowledge_index_overview(store: Any, user_id: str, user_groups: list[str]) -> dict[str, Any]:
    assets = list_knowledge_assets(store, user_id=user_id, user_groups=user_groups)
    jobs = get_knowledge_index_jobs(store, limit=200)
    indexed = sum(1 for asset in assets if int(asset.get("chunk_count") or 0) > 0)
    failed = sum(1 for asset in assets if str(asset.get("index_status") or "") == "failed")
    running = sum(1 for job in jobs if str(job.get("status") or "") == "running")
    return {
        "asset_count": len(assets),
        "indexed_asset_count": indexed,
        "failed_asset_count": failed,
        "running_job_count": running,
        "chunk_count": sum(int(asset.get("chunk_count") or 0) for asset in assets),
        "embedding_count": sum(int(asset.get("embedding_count") or 0) for asset in assets),
        "coverage_ratio": round((indexed / len(assets)), 4) if assets else 0.0,
        "latest_job_at": jobs[0]["updated_at"] if jobs else "",
    }


def get_knowledge_index_asset_detail(store: Any, asset_id: str) -> dict[str, Any] | None:
    asset = get_knowledge_asset(store, asset_id)
    if not asset:
        return None
    with store.SessionFactory() as sess:
        chunks = sess.execute(select(DBKnowledgeChunk).where(DBKnowledgeChunk.asset_id == asset_id).order_by(DBKnowledgeChunk.chunk_index)).scalars().all()
    asset["chunks"] = [
        {
            "chunk_id": chunk.chunk_id,
            "chunk_index": chunk.chunk_index,
            "chunk_text": chunk.chunk_text,
            "keywords": chunk.keywords or [],
            "source_path": chunk.source_path or "",
            "full_document_locator": chunk.full_document_locator,
            "index_version": chunk.index_version,
        }
        for chunk in chunks
    ]
    asset["jobs"] = get_knowledge_index_jobs(store, asset_id=asset_id, limit=20)
    return asset


def read_knowledge_asset(store: Any, asset_id: str, mode: str = "full", cursor: str | None = None, limit: int = 12000) -> dict[str, Any]:
    asset = get_knowledge_asset(store, asset_id)
    if not asset:
        raise ValueError("Knowledge asset not found")
    source_path = str(asset.get("source_path") or "")
    content = ""
    if asset.get("source_type") == "knowledge_base":
        kb = store.get_knowledge_base(str(asset.get("source_ref") or ""))
        content = str((kb or {}).get("content") or "")
    elif asset.get("source_type") == "skill":
        skill = store.skills.get(str(asset.get("source_ref") or ""))
        content = _build_skill_asset_content(skill or {})
    elif asset.get("source_type") == "upload":
        sandbox = store.get_sandbox(str(asset.get("sandbox_id") or "")) or {}
        filename = str(asset.get("source_ref") or "")
        rows = (sandbox.get("uploads") or {}).get(filename, [])
        content, _ = _build_upload_asset_content(filename, rows, source_path)
    else:
        content = str(asset.get("content_preview") or "")

    if str(mode or "full").lower() == "preview":
        limit = min(limit, 2500)

    truncated = False
    next_cursor = ""
    parsed_cursor = int(str(cursor or "0") or "0")
    if asset.get("content_type") == "application/vnd.ms-excel":
        sandbox = store.get_sandbox(str(asset.get("sandbox_id") or "")) or {}
        filename = str(asset.get("source_ref") or "")
        rows = (sandbox.get("uploads") or {}).get(filename, [])
        page_size = max(min(limit, 200), 1)
        page_rows = rows[parsed_cursor : parsed_cursor + page_size]
        content = json.dumps(page_rows, ensure_ascii=False, indent=2)
        if parsed_cursor + page_size < len(rows):
            truncated = True
            next_cursor = str(parsed_cursor + page_size)
    else:
        start = max(parsed_cursor, 0)
        end = start + max(limit, 1)
        if len(content) > end:
            truncated = True
            next_cursor = str(end)
        content = content[start:end]

    return {
        "asset_id": asset["asset_id"],
        "title": asset["title"],
        "content": content,
        "content_type": asset.get("content_type") or "text/plain",
        "source_path": source_path,
        "truncated": truncated,
        "next_cursor": next_cursor,
        "full_document_locator": asset.get("full_document_locator", _asset_locator(asset_id)),
    }


def read_knowledge_source(store: Any, locator: str, mode: str = "full", cursor: str | None = None, limit: int = 12000) -> dict[str, Any]:
    parsed = urlparse(str(locator or ""))
    if parsed.scheme != "asset":
        raise ValueError("Unsupported knowledge locator")
    asset_id = parsed.netloc or parsed.path.lstrip("/")
    query = parse_qs(parsed.query)
    read_mode = str(query.get("mode", [mode])[0] or mode)
    read_cursor = str(query.get("cursor", [cursor or ""])[0] or cursor or "")
    return read_knowledge_asset(store, asset_id=asset_id, mode=read_mode, cursor=read_cursor, limit=limit)


def rebuild_knowledge_index(store: Any, *, asset_id: str | None = None, asset_type: str | None = None, sandbox_id: str | None = None) -> dict[str, Any]:
    _record_index_job(store, asset_id=asset_id, scope="asset" if asset_id else "all", status="running", message="Reindex started", stats={})
    refresh_knowledge_assets(store)
    assets = list_knowledge_assets(store)
    if asset_id:
        assets = [asset for asset in assets if asset["asset_id"] == asset_id]
    if asset_type:
        assets = [asset for asset in assets if asset["asset_type"] == asset_type]
    if sandbox_id:
        assets = [asset for asset in assets if str(asset.get("sandbox_id") or "") == str(sandbox_id)]
    _record_index_job(store, asset_id=asset_id, scope="asset" if asset_id else "all", status="success", message="Reindex finished", stats={"asset_count": len(assets)})
    return {"asset_count": len(assets), "assets": assets}


def _resolve_runtime_asset_ids(store: Any, sandbox_id: str) -> set[str]:
    sandbox = store.get_sandbox(sandbox_id) or {}
    if not sandbox:
        return set()
    allowed: set[str] = set()
    for asset in list_knowledge_assets(store):
        if asset["source_type"] == "knowledge_base" and asset["source_ref"] in (sandbox.get("knowledge_bases") or []):
            allowed.add(asset["asset_id"])
        elif asset["source_type"] == "skill" and asset["source_ref"] in (sandbox.get("mounted_skills") or []):
            allowed.add(asset["asset_id"])
        elif asset["source_type"] == "upload" and str(asset.get("sandbox_id") or "") == sandbox_id:
            allowed.add(asset["asset_id"])
    return allowed


def search_knowledge_index(store: Any, query: str, sandbox_id: str, top_k: int = 5) -> list[dict[str, Any]]:
    refresh_knowledge_assets(store)
    query_text = str(query or "").strip()
    if not query_text:
        return []
    allowed_ids = _resolve_runtime_asset_ids(store, sandbox_id)
    if not allowed_ids:
        return []
    query_keywords = set(_extract_keywords(query_text))
    query_embedding = _embed_text(query_text)
    with store.SessionFactory() as sess:
        chunk_rows = sess.execute(select(DBKnowledgeChunk)).scalars().all()
    asset_map = {asset["asset_id"]: asset for asset in list_knowledge_assets(store)}
    scored: list[dict[str, Any]] = []
    for chunk in chunk_rows:
        if chunk.asset_id not in allowed_ids:
            continue
        asset = asset_map.get(chunk.asset_id)
        if not asset:
            continue
        chunk_keywords = set(chunk.keywords or [])
        overlap = len(query_keywords.intersection(chunk_keywords))
        keyword_score = overlap / max(len(query_keywords), 1)
        vector_score = _cosine_similarity(query_embedding, list(chunk.embedding or []))
        score = round((0.35 * keyword_score) + (0.65 * max(vector_score, 0.0)), 6)
        if score <= 0 and overlap <= 0:
            continue
        scored.append(
            {
                "asset_id": chunk.asset_id,
                "asset_type": asset["asset_type"],
                "title": asset["title"],
                "chunk_id": chunk.chunk_id,
                "snippet": (chunk.chunk_text or "")[:400],
                "score": score,
                "source_ref": chunk.source_ref,
                "source_path": chunk.source_path or "",
                "full_document_locator": chunk.full_document_locator,
                "keyword_score": round(keyword_score, 6),
                "vector_score": round(vector_score, 6),
            }
        )
    scored.sort(key=lambda item: (item["score"], item["keyword_score"], item["vector_score"]), reverse=True)
    return scored[: max(min(int(top_k or 5), 20), 1)]


def update_asset_mounts(store: Any, asset_id: str, sandbox_ids: list[str]) -> dict[str, Any]:
    asset = get_knowledge_asset(store, asset_id)
    if not asset:
        raise ValueError("Knowledge asset not found")
    if asset["source_type"] not in {"knowledge_base", "skill"}:
        raise ValueError("Only enterprise knowledge and experience assets can be mounted")
    desired = {str(item).strip() for item in (sandbox_ids or []) if str(item).strip()}
    for sandbox in store.list_sandboxes():
        sandbox_id = str(sandbox.get("sandbox_id") or "").strip()
        if asset["source_type"] == "knowledge_base":
            values = [item for item in (sandbox.get("knowledge_bases") or []) if item != asset["source_ref"]]
            if sandbox_id in desired:
                values.append(asset["source_ref"])
            store.update_sandbox(sandbox_id, {"knowledge_bases": _dedupe_non_empty(values)})
        else:
            values = [item for item in (sandbox.get("mounted_skills") or []) if item != asset["source_ref"]]
            if sandbox_id in desired:
                values.append(asset["source_ref"])
            store.update_sandbox(sandbox_id, {"mounted_skills": _dedupe_non_empty(values)})
    return get_knowledge_asset(store, asset_id) or {}


def publish_experience_asset(store: Any, skill_id: str, name: str | None = None, description: str | None = None) -> dict[str, Any]:
    skill = store.skills.get(skill_id)
    if not skill:
        raise ValueError("Skill not found")
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = str(name).strip() or skill.get("name")
    if description is not None:
        updates["description"] = str(description).strip()
    if updates:
        skill = store.update_skill(skill_id, updates)
    _upsert_skill_asset(store, skill)
    asset = next((item for item in list_knowledge_assets(store) if item["source_type"] == "skill" and item["source_ref"] == skill_id), None)
    if not asset:
        raise ValueError("Failed to publish experience asset")
    return asset
