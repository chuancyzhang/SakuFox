import csv
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class User:
    user_id: str
    username: str
    display_name: str
    groups: list[str]
    provider: str


class InMemoryStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.tokens: dict[str, User] = {}
        self.oauth_tokens: dict[str, str] = {
            "oauth_finance_alice": "alice",
            "oauth_marketing_bob": "bob",
            "oauth_data_carol": "carol",
        }
        self.ldap_users: dict[str, dict] = {
            "alice": {"display_name": "Alice", "groups": ["finance"]},
            "bob": {"display_name": "Bob", "groups": ["marketing"]},
            "carol": {"display_name": "Carol", "groups": ["data", "marketing"]},
        }
        self.tables: dict[str, dict] = {
            "tutorial_flights": {
                "allowed_groups": ["finance", "marketing", "data", "admin"],
                "sensitive_fields": [],
            },
        }
        self.sandboxes: dict[str, dict] = {
            "sb_flights_overview": {
                "name": "Superset 航班样例沙盒",
                "tables": ["tutorial_flights"],
                "metadata": {
                    "main_metric": "SUM(cost)",
                    "main_dimension": "department, travel_date",
                },
                "allowed_groups": ["finance", "marketing", "data", "admin"],
                "business_knowledge": [],
                "uploads": {},
                "upload_paths": {},
            },
        }
        self.session_context: dict[str, dict[str, dict]] = {}
        self.proposals: dict[str, dict] = {}
        self.skills: dict[str, dict] = {}
        self.db_engines: dict[str, object] = {}  # sandbox_id -> SQLAlchemy Engine
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._seed_data()

    def _seed_data(self) -> None:
        cur = self.conn.cursor()
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
        with self._lock:
            user_sessions = self.session_context.setdefault(user_id, {})
            if session_id and session_id in user_sessions:
                return session_id, user_sessions[session_id]
            new_id = session_id or f"ss_{uuid.uuid4().hex[:12]}"
            user_sessions[new_id] = {
                "patches": [],
                "uploads": {},
                "upload_paths": {},
                "iterations": [],
                "business_knowledge": [],
                "title": "",  # auto-filled from first message
                "sandbox_id": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            return new_id, user_sessions[new_id]

    def append_patch(self, user_id: str, session_id: str, patch: str) -> None:
        with self._lock:
            session = self.session_context[user_id][session_id]
            session["patches"].append(patch)

    def add_upload(self, sandbox_id: str, name: str, rows: list[dict], file_path: str = "") -> None:
        with self._lock:
            if sandbox_id in self.sandboxes:
                sandbox = self.sandboxes[sandbox_id]
                sandbox.setdefault("uploads", {})[name] = rows
                if file_path:
                    sandbox.setdefault("upload_paths", {})[name] = file_path

    # ── Iteration tracking ────────────────────────────────────────────

    def append_iteration(self, user_id: str, session_id: str, iteration: dict) -> str:
        """Save one analysis iteration and return its iteration_id."""
        iteration_id = f"iter_{uuid.uuid4().hex[:12]}"
        iteration["iteration_id"] = iteration_id
        iteration.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        with self._lock:
            session = self.session_context[user_id][session_id]
            session["iterations"].append(iteration)
        return iteration_id

    def get_iteration_history(self, user_id: str, session_id: str) -> list[dict]:
        """Return all past iterations for context building."""
        with self._lock:
            session = self.session_context.get(user_id, {}).get(session_id)
            if not session:
                return []
            return list(session.get("iterations", []))

    # ── Business knowledge ────────────────────────────────────────────

    def append_business_knowledge(self, sandbox_id: str, knowledge: str) -> None:
        """Persist a piece of business knowledge the user shared into the sandbox."""
        with self._lock:
            if sandbox_id in self.sandboxes:
                sandbox = self.sandboxes[sandbox_id]
                bk = sandbox.setdefault("business_knowledge", [])
                bk.append({
                    "text": knowledge,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })

    def get_business_knowledge(self, sandbox_id: str) -> list[str]:
        """Return accumulated business knowledge texts for the sandbox."""
        with self._lock:
            sandbox = self.sandboxes.get(sandbox_id)
            if not sandbox:
                return []
            return [item["text"] for item in sandbox.get("business_knowledge", [])]

    # ── Proposals (kept for skill saving compatibility) ────────────────

    def create_proposal(self, data: dict) -> str:
        proposal_id = f"pp_{uuid.uuid4().hex[:12]}"
        with self._lock:
            self.proposals[proposal_id] = {"status": "pending", **data}
        return proposal_id

    def update_proposal(self, proposal_id: str, updates: dict) -> dict:
        with self._lock:
            self.proposals[proposal_id].update(updates)
            return self.proposals[proposal_id]

    def list_sessions(self, user_id: str) -> list[dict]:
        """Return all sessions for a user, newest first."""
        with self._lock:
            user_sessions = self.session_context.get(user_id, {})
            result = []
            for sid, s in user_sessions.items():
                result.append({
                    "session_id": sid,
                    "title": s.get("title") or "新对话",
                    "sandbox_id": s.get("sandbox_id", ""),
                    "iteration_count": len(s.get("iterations", [])),
                    "created_at": s.get("created_at", ""),
                })
            result.sort(key=lambda x: x["created_at"], reverse=True)
            return result

    def delete_session(self, user_id: str, session_id: str) -> bool:
        with self._lock:
            user_sessions = self.session_context.get(user_id, {})
            if session_id in user_sessions:
                del user_sessions[session_id]
                return True
            return False

    def update_session_title(self, user_id: str, session_id: str, title: str) -> None:
        with self._lock:
            user_sessions = self.session_context.get(user_id, {})
            if session_id in user_sessions:
                user_sessions[session_id]["title"] = title

    def get_session_knowledge(self, user_id: str, session_id: str) -> list[str]:
        """Return patches/feedback from a session for skill knowledge extraction."""
        with self._lock:
            session = self.session_context.get(user_id, {}).get(session_id, {})
            return list(session.get("patches", []))

    # ── Skills ────────────────────────────────────────────────────────

    def create_skill(self, data: dict) -> str:
        skill_id = f"sk_{uuid.uuid4().hex[:12]}"
        with self._lock:
            self.skills[skill_id] = data
        return skill_id

    def delete_skill(self, skill_id: str) -> bool:
        with self._lock:
            if skill_id in self.skills:
                del self.skills[skill_id]
                return True
            return False

    # ── External DB engines ───────────────────────────────────────────

    def register_sandbox_db(self, sandbox_id: str, engine: object, db_config: dict) -> None:
        """Attach an external SQLAlchemy Engine to a sandbox."""
        with self._lock:
            self.db_engines[sandbox_id] = engine
            if sandbox_id in self.sandboxes:
                self.sandboxes[sandbox_id]["db_connection"] = db_config

    def get_sandbox_engine(self, sandbox_id: str) -> object | None:
        """Return the registered engine for a sandbox, or None if using built-in SQLite."""
        return self.db_engines.get(sandbox_id)

    # ── Sandbox CRUD ───────────────────────────────────────────
    
    def create_sandbox(self, name: str, allowed_groups: list[str]) -> str:
        sandbox_id = f"sb_{uuid.uuid4().hex[:12]}"
        with self._lock:
            self.sandboxes[sandbox_id] = {
                "name": name,
                "tables": [],
                "allowed_groups": allowed_groups,
                "business_knowledge": [],
                "uploads": {},
                "upload_paths": {},
            }
        return sandbox_id
        
    def update_sandbox(self, sandbox_id: str, updates: dict) -> dict:
        with self._lock:
            if sandbox_id in self.sandboxes:
                self.sandboxes[sandbox_id].update(updates)
                return self.sandboxes[sandbox_id]
            raise ValueError("Sandbox not found")
            
    def delete_sandbox(self, sandbox_id: str) -> bool:
        with self._lock:
            if sandbox_id in self.sandboxes:
                del self.sandboxes[sandbox_id]
                if sandbox_id in self.db_engines:
                    del self.db_engines[sandbox_id]
                return True
            return False


store = InMemoryStore()
