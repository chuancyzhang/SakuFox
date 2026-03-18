import csv
import sqlite3
import threading
import uuid
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, select, delete, update
from sqlalchemy.orm import sessionmaker, Session as SQLASession

from app.config import load_config
from app.db_models import Base, DBUser, DBSandbox, DBSession, DBIteration, DBSkill, DBProposal

@dataclass
class User:
    user_id: str
    username: str
    display_name: str
    groups: list[str]
    provider: str

class DatabaseStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        config = load_config()
        self.engine = create_engine(config.db_url, pool_pre_ping=True)
        self.SessionFactory = sessionmaker(bind=self.engine)
        
        # In-memory caches for ephemeral data or auth tokens
        # We COULD persist tokens, but usually they are fine in memory for this kind of app
        # unless we want full session persistence across restarts.
        # User wants "persistence", so let's put core metadata in DB.
        self.tokens: dict[str, User] = {} 
        
        # Static mock data (until we have a real user management UI)
        self.ldap_users: dict[str, dict] = {
            "alice": {"display_name": "Alice", "groups": ["finance"]},
            "bob": {"display_name": "Bob", "groups": ["marketing"]},
            "carol": {"display_name": "Carol", "groups": ["data", "marketing"]},
        }
        self.oauth_tokens: dict[str, str] = {
            "oauth_finance_alice": "alice",
            "oauth_marketing_bob": "bob",
            "oauth_data_carol": "carol",
        }

        # Keep the internal SQLite connection for the "sandbox demo" data
        self.db_engines: dict[str, object] = {}  # sandbox_id -> SQLAlchemy Engine
        
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        # Initialize database tables
        Base.metadata.create_all(self.engine)
        self._seed_sandbox_data()
        self._init_default_sandbox()

    def _seed_sandbox_data(self) -> None:
        """Seed the demo table in-memory."""
        cur = self.conn.cursor()
        # Check if table exists to avoid errors on multiple init
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tutorial_flights'")
        if cur.fetchone():
            return

        cur.execute(
            """
            CREATE TABLE tutorial_flights(
              department TEXT,
              cost REAL,
              travel_class TEXT,
              ticket_type TEXT,
              airline TEXT,
              travel_date TEXT,
              origin_country TEXT,
              destination_country TEXT,
              origin_region TEXT,
              destination_region TEXT,
              distance INTEGER
            )
            """
        )
        csv_path = Path(__file__).resolve().parent / "data" / "tutorial_flights.csv"
        rows: list[tuple] = []
        if csv_path.exists():
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append(
                        (
                            r["Department"],
                            float(r["Cost"]),
                            r["Travel Class"],
                            r["Ticket Single or Return"],
                            r["Airline"],
                            r["Travel Date"],
                            r["Origin Country"],
                            r["Destination Country"],
                            r["Origin Region"],
                            r["Destination Region"],
                            int(float(r["Distance"])),
                        )
                    )
            cur.executemany(
                "INSERT INTO tutorial_flights VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            self.conn.commit()

    def _init_default_sandbox(self) -> None:
        """Ensure the default sandbox entry exists in the metadata DB and has the right tables."""
        demo_db_path = Path(__file__).resolve().parent.parent / "superset_demo.db"
        using_prebuilt = demo_db_path.exists()
        
        with self.SessionFactory() as sess:
            sb = sess.execute(select(DBSandbox).where(DBSandbox.sandbox_id == "sb_flights_overview")).scalar_one_or_none()
            tables = ["tutorial_flights", "video_game_sales"] if using_prebuilt else ["tutorial_flights"]
            
            if not sb:
                new_sb = DBSandbox(
                    sandbox_id="sb_flights_overview",
                    name="Superset 样例沙盒",
                    tables=tables,
                    allowed_groups=["finance", "marketing", "data", "admin"],
                    business_knowledge=[],
                    uploads={},
                    upload_paths={}
                )
                sess.add(new_sb)
                sess.commit()
            elif set(sb.tables) != set(tables):
                sb.tables = tables
                sess.commit()

        # If using pre-built DB, register it as the engine for this sandbox
        if using_prebuilt:
            engine = create_engine(f"sqlite:///{demo_db_path}", pool_pre_ping=True)
            db_config = {
                "db_type": "sqlite",
                "database": str(demo_db_path)
            }
            self.register_sandbox_db("sb_flights_overview", engine, db_config)

    # ── Token / Auth ──────────────────────────────────────────────────

    def issue_token(self, user: User) -> str:
        token = f"tk_{uuid.uuid4().hex}"
        with self._lock:
            self.tokens[token] = user
        return token

    def get_user_by_token(self, token: str) -> User | None:
        return self.tokens.get(token)

    # ── Session ───────────────────────────────────────────────────────

    def get_or_create_session(self, user_id: str, session_id: str | None) -> tuple[str, dict]:
        with self.SessionFactory() as sess:
            if session_id:
                db_sess = sess.execute(
                    select(DBSession).where(DBSession.session_id == session_id, DBSession.user_id == user_id)
                ).scalar_one_or_none()
                if db_sess:
                    # Return as dict for compatibility
                    return session_id, self._session_to_dict(db_sess, sess)
            
            new_id = session_id or f"ss_{uuid.uuid4().hex[:12]}"
            new_db_sess = DBSession(
                session_id=new_id,
                user_id=user_id,
                title="",
                sandbox_id="",
                created_at=datetime.now(timezone.utc).isoformat(),
                patches=[]
            )
            sess.add(new_db_sess)
            sess.commit()
            return new_id, self._session_to_dict(new_db_sess, sess)

    def _session_to_dict(self, db_sess: DBSession, sql_sess: SQLASession) -> dict:
        # Fetch iterations for this session
        iters = sql_sess.execute(
            select(DBIteration).where(DBIteration.session_id == db_sess.session_id).order_by(DBIteration.created_at)
        ).scalars().all()
        
        return {
            "session_id": db_sess.session_id,
            "title": db_sess.title,
            "sandbox_id": db_sess.sandbox_id,
            "created_at": db_sess.created_at,
            "patches": db_sess.patches or [],
            "iterations": [self._iter_to_dict(it) for it in iters],
            # Other fields expected by main.py
            "uploads": {}, # Sandboxes handle uploads globally for now in this app
            "upload_paths": {},
            "business_knowledge": [] # Sandbox level
        }

    def _iter_to_dict(self, it: DBIteration) -> dict:
        return {
            "iteration_id": it.iteration_id,
            "message": it.message,
            "steps": it.steps,
            "conclusions": it.conclusions,
            "hypotheses": it.hypotheses,
            "action_items": it.action_items,
            "tools_used": it.tools_used,
            "result_rows": it.result_rows,
            "chart_specs": it.chart_specs,
            "created_at": it.created_at
        }

    def append_patch(self, user_id: str, session_id: str, patch: str) -> None:
        with self.SessionFactory() as sess:
            db_sess = sess.get(DBSession, session_id)
            if db_sess:
                patches = list(db_sess.patches or [])
                patches.append(patch)
                db_sess.patches = patches
                sess.commit()

    def add_upload(self, sandbox_id: str, name: str, rows: list[dict], file_path: str = "") -> None:
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if sb:
                uploads = dict(sb.uploads or {})
                uploads[name] = rows
                sb.uploads = uploads
                if file_path:
                    paths = dict(sb.upload_paths or {})
                    paths[name] = file_path
                    sb.upload_paths = paths
                sess.commit()

    # ── Iteration tracking ────────────────────────────────────────────

    def append_iteration(self, user_id: str, session_id: str, iteration: dict) -> str:
        iteration_id = f"iter_{uuid.uuid4().hex[:12]}"
        created_at = iteration.get("created_at") or datetime.now(timezone.utc).isoformat()
        
        with self.SessionFactory() as sess:
            new_it = DBIteration(
                iteration_id=iteration_id,
                session_id=session_id,
                user_id=user_id,
                message=iteration.get("message", ""),
                steps=iteration.get("steps", []),
                conclusions=iteration.get("conclusions", []),
                hypotheses=iteration.get("hypotheses", []),
                action_items=iteration.get("action_items", []),
                tools_used=iteration.get("tools_used", []),
                result_rows=iteration.get("result_rows", []),
                chart_specs=iteration.get("chart_specs", []),
                created_at=created_at
            )
            sess.add(new_it)
            
            # Also update session's sandbox_id if not set (first iteration)
            db_sess = sess.get(DBSession, session_id)
            if db_sess and iteration.get("sandbox_id"):
                 db_sess.sandbox_id = iteration["sandbox_id"]
            
            sess.commit()
        return iteration_id

    def get_iteration_history(self, user_id: str, session_id: str) -> list[dict]:
        with self.SessionFactory() as sess:
            iters = sess.execute(
                select(DBIteration).where(DBIteration.session_id == session_id).order_by(DBIteration.created_at)
            ).scalars().all()
            return [self._iter_to_dict(it) for it in iters]

    # ── Business knowledge ────────────────────────────────────────────

    def append_business_knowledge(self, sandbox_id: str, knowledge: str) -> None:
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if sb:
                bk = list(sb.business_knowledge or [])
                bk.append({
                    "text": knowledge,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
                sb.business_knowledge = bk
                sess.commit()

    def get_business_knowledge(self, sandbox_id: str) -> list[str]:
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if not sb:
                return []
            return [item["text"] for item in (sb.business_knowledge or [])]

    # ── Proposals ────────────────

    def create_proposal(self, data: dict) -> str:
        proposal_id = f"pp_{uuid.uuid4().hex[:12]}"
        with self.SessionFactory() as sess:
            new_p = DBProposal(
                proposal_id=proposal_id,
                user_id=data.get("user_id"),
                session_id=data.get("session_id"),
                sandbox_id=data.get("sandbox_id"),
                message=data.get("message"),
                steps=data.get("steps"),
                explanation=data.get("explanation"),
                tables=data.get("tables"),
                status=data.get("status", "pending"),
                result_rows=data.get("result_rows"),
                chart_specs=data.get("chart_specs"),
                selected_tables=data.get("selected_tables"),
                session_patches=data.get("session_patches"),
                created_at=datetime.now(timezone.utc).isoformat()
            )
            sess.add(new_p)
            sess.commit()
        return proposal_id

    def update_proposal(self, proposal_id: str, updates: dict) -> dict:
        with self.SessionFactory() as sess:
             p = sess.get(DBProposal, proposal_id)
             if p:
                 # Update dynamically
                 for k, v in updates.items():
                     if hasattr(p, k):
                         setattr(p, k, v)
                 sess.commit()
                 return {"proposal_id": p.proposal_id, "status": p.status} # Minimal return
             return {}

    def get_proposal(self, proposal_id: str) -> dict | None:
        with self.SessionFactory() as sess:
            p = sess.get(DBProposal, proposal_id)
            if p:
                return {
                    "proposal_id": p.proposal_id,
                    "user_id": p.user_id,
                    "session_id": p.session_id,
                    "message": p.message,
                    "result_rows": p.result_rows,
                    "chart_specs": p.chart_specs,
                    "tables": p.tables or [],
                    "steps": p.steps or [],
                    "explanation": p.explanation or "",
                    "sql": "", # Compatibility
                    "status": p.status
                }
            return None

    def list_sessions(self, user_id: str) -> list[dict]:
        with self.SessionFactory() as sess:
            db_sessions = sess.execute(
                select(DBSession).where(DBSession.user_id == user_id).order_by(DBSession.created_at.desc())
            ).scalars().all()
            
            result = []
            for s in db_sessions:
                # Count iterations
                count = sess.query(DBIteration).filter(DBIteration.session_id == s.session_id).count()
                result.append({
                    "session_id": s.session_id,
                    "title": s.title or "新对话",
                    "sandbox_id": s.sandbox_id,
                    "iteration_count": count,
                    "created_at": s.created_at,
                })
            return result

    def delete_session(self, user_id: str, session_id: str) -> bool:
        with self.SessionFactory() as sess:
            sess.execute(delete(DBIteration).where(DBIteration.session_id == session_id))
            sess.execute(delete(DBProposal).where(DBProposal.session_id == session_id))
            result = sess.execute(delete(DBSession).where(DBSession.session_id == session_id, DBSession.user_id == user_id))
            sess.commit()
            return result.rowcount > 0

    def update_session(self, user_id: str, session_id: str, updates: dict) -> None:
        with self.SessionFactory() as sess:
            db_sess = sess.get(DBSession, session_id)
            if db_sess and db_sess.user_id == user_id:
                for k, v in updates.items():
                    if hasattr(db_sess, k):
                        setattr(db_sess, k, v)
                sess.commit()

    def update_session_title(self, user_id: str, session_id: str, title: str) -> None:
        self.update_session(user_id, session_id, {"title": title})

    def get_session_knowledge(self, user_id: str, session_id: str) -> list[str]:
        with self.SessionFactory() as sess:
            db_sess = sess.get(DBSession, session_id)
            if db_sess and db_sess.user_id == user_id:
                return db_sess.patches or []
            return []

    # ── Skills ────────────────────────────────────────────────────────

    def create_skill(self, data: dict) -> str:
        skill_id = f"sk_{uuid.uuid4().hex[:12]}"
        with self.SessionFactory() as sess:
            new_sk = DBSkill(
                skill_id=skill_id,
                owner_id=data.get("owner_id"),
                name=data.get("name"),
                description=data.get("description"),
                tags=data.get("tags"),
                layers=data.get("layers"),
                created_at=datetime.now(timezone.utc).isoformat()
            )
            sess.add(new_sk)
            sess.commit()
        return skill_id

    def delete_skill(self, skill_id: str) -> bool:
        with self.SessionFactory() as sess:
            result = sess.execute(delete(DBSkill).where(DBSkill.skill_id == skill_id))
            sess.commit()
            return result.rowcount > 0

    def list_skills(self) -> list[dict]:
        with self.SessionFactory() as sess:
            skills = sess.execute(select(DBSkill)).scalars().all()
            return [{
                "skill_id": sk.skill_id,
                "owner_id": sk.owner_id,
                "name": sk.name,
                "description": sk.description,
                "tags": sk.tags,
                "layers": sk.layers,
                "created_at": sk.created_at
            } for sk in skills]

    # ── External DB engines ───────────────────────────────────────────

    def register_sandbox_db(self, sandbox_id: str, engine: object, db_config: dict) -> None:
        with self._lock:
            self.db_engines[sandbox_id] = engine # Keep engine in memory (non-serializable)
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if sb:
                sb.db_config = db_config
                sess.commit()

    def get_sandbox_engine(self, sandbox_id: str) -> object | None:
        return self.db_engines.get(sandbox_id)

    def get_sandbox_full_context(self, sandbox_id: str) -> dict[str, dict]:
        """Get schema and samples for all tables in a sandbox."""
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if not sb:
                return {}
            
            tables = sb.tables or []
            context = {}
            
            # Check for external engine
            engine = self.get_sandbox_engine(sandbox_id)
            
            for table in tables:
                if engine:
                    from app.db_connections import get_table_columns_info, get_sample_data
                    try:
                        columns = get_table_columns_info(engine, table)
                        sample = get_sample_data(engine, table)
                        context[table] = {"columns": columns, "sample": sample}
                    except Exception:
                        context[table] = {"columns": [], "sample": [], "error": "无法获取元数据"}
                else:
                    # Fallback to internal SQLite
                    try:
                        columns = self._get_sqlite_table_columns(table)
                        sample = self._get_sqlite_table_sample(table)
                        context[table] = {"columns": columns, "sample": sample}
                    except Exception:
                        context[table] = {"columns": [], "sample": [], "error": "无法获取元数据"}
            return context

    def _get_sqlite_table_columns(self, table_name: str) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        return [{"name": r["name"], "type": r["type"]} for r in cur.fetchall()]

    def _get_sqlite_table_sample(self, table_name: str, limit: int = 3) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        return [dict(r) for r in cur.fetchall()]

    # ── Sandbox CRUD ───────────────────────────────────────────
    
    def create_sandbox(self, name: str, allowed_groups: list[str]) -> str:
        sandbox_id = f"sb_{uuid.uuid4().hex[:12]}"
        with self.SessionFactory() as sess:
            new_sb = DBSandbox(
                sandbox_id=sandbox_id,
                name=name,
                tables=[],
                allowed_groups=allowed_groups,
                business_knowledge=[],
                uploads={},
                upload_paths={}
            )
            sess.add(new_sb)
            sess.commit()
        return sandbox_id

    def get_sandbox(self, sandbox_id: str) -> dict | None:
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if sb:
                return {
                    "sandbox_id": sb.sandbox_id,
                    "name": sb.name,
                    "tables": sb.tables or [],
                    "allowed_groups": sb.allowed_groups or [],
                    "business_knowledge": sb.business_knowledge or [],
                    "uploads": sb.uploads or {},
                    "upload_paths": sb.upload_paths or {},
                    "db_connection": sb.db_config
                }
            return None

    def update_sandbox(self, sandbox_id: str, updates: dict) -> dict:
        with self.SessionFactory() as sess:
            sb = sess.get(DBSandbox, sandbox_id)
            if sb:
                for k, v in updates.items():
                    if hasattr(sb, k):
                        setattr(sb, k, v)
                sess.commit()
                return self.get_sandbox(sandbox_id)
            raise ValueError("Sandbox not found")
            
    def delete_sandbox(self, sandbox_id: str) -> bool:
        with self.SessionFactory() as sess:
            result = sess.execute(delete(DBSandbox).where(DBSandbox.sandbox_id == sandbox_id))
            sess.commit()
            if result.rowcount > 0:
                with self._lock:
                    if sandbox_id in self.db_engines:
                        del self.db_engines[sandbox_id]
                return True
            return False

    def list_sandboxes(self) -> list[dict]:
        with self.SessionFactory() as sess:
            sbs = sess.execute(select(DBSandbox)).scalars().all()
            return [self.get_sandbox(sb.sandbox_id) for sb in sbs]

    @property
    def sandboxes(self):
        """Interface for compatibility with dict-based access."""
        outer = self
        class SandboxProxy:
            def get(self, sid, default=None):
                res = outer.get_sandbox(sid)
                return res if res is not None else default
            def __contains__(self, sid): return outer.get_sandbox(sid) is not None
            def items(self):
                sbs = outer.list_sandboxes()
                return [(sb["sandbox_id"], sb) for sb in sbs]
            def __getitem__(self, sid):
                res = outer.get_sandbox(sid)
                if not res: raise KeyError(sid)
                return res
        return SandboxProxy()

    @property
    def skills(self):
        """Interface for compatibility with dict-based access."""
        outer = self
        class SkillProxy:
            def get(self, skid, default=None):
                with outer.SessionFactory() as sess:
                    sk = sess.get(DBSkill, skid)
                    if not sk: return default
                    return {
                        "skill_id": sk.skill_id,
                        "owner_id": sk.owner_id,
                        "name": sk.name,
                        "description": sk.description,
                        "tags": sk.tags,
                        "layers": sk.layers,
                        "groups": (sk.layers or {}).get("groups", []),
                        "created_at": sk.created_at
                    } if sk else None
            def items(self):
                sks = outer.list_skills()
                return [(sk["skill_id"], sk) for sk in sks]
            def __getitem__(self, skid):
                res = self.get(skid)
                if not res: raise KeyError(skid)
                return res
        return SkillProxy()

    @property
    def proposals(self):
        """Interface for compatibility with dict-based access."""
        outer = self
        class ProposalProxy:
            def get(self, pid, default=None):
                res = outer.get_proposal(pid)
                return res if res is not None else default
            def __getitem__(self, pid):
                res = outer.get_proposal(pid)
                if not res: raise KeyError(pid)
                return res
        return ProposalProxy()

    @property
    def tables(self):
        """Interface for compatibility with table config access."""
        # For now, tables config is mostly static or derived from sandboxes.
        # InMemoryStore had a hardcoded dict. Let's provide a proxy that 
        # looks at the default sandbox or returns a default config.
        outer = self
        class TableProxy:
            def get(self, tname):
                # Hardcoded for tutorial_flights as per old seed_data
                if tname == "tutorial_flights":
                    return {
                        "allowed_groups": ["finance", "marketing", "data", "admin"],
                        "sensitive_fields": [],
                    }
                return None
            def items(self):
                # Return tutorial_flights by default
                return [("tutorial_flights", self.get("tutorial_flights"))]
        return TableProxy()

store = DatabaseStore()
