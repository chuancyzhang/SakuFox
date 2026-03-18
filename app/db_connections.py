"""
Multi-database connection layer.

Supports: MySQL, PostgreSQL, SQLite (file), Oracle, Impala (Hive dialect).
Uses SQLAlchemy 2.x as the unified connection engine so callers don't need
per-driver boilerplate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.engine import Engine
    _HAS_SQLALCHEMY = True
except ImportError:
    _HAS_SQLALCHEMY = False


SUPPORTED_DB_TYPES = {"mysql", "postgresql", "sqlite", "oracle", "impala"}

DEFAULT_PORTS: dict[str, int] = {
    "mysql": 3306,
    "postgresql": 5432,
    "oracle": 1521,
    "impala": 21050,
}


@dataclass
class DbConnectionConfig:
    db_type: str                          # mysql / postgresql / sqlite / oracle / impala
    database: str                         # DB name or file path for sqlite
    host: str = "localhost"
    port: int | None = None               # None → use default
    username: str = ""
    password: str = ""
    extra_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.db_type = self.db_type.lower()
        if self.db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"不支持的数据库类型: {self.db_type}，支持: {', '.join(sorted(SUPPORTED_DB_TYPES))}")
        if self.port is None:
            self.port = DEFAULT_PORTS.get(self.db_type)


def _build_url(cfg: DbConnectionConfig) -> str:
    """Build a SQLAlchemy connection URL from config."""
    db_type = cfg.db_type
    if db_type == "sqlite":
        # sqlite:///path/to/file.db  (absolute: sqlite:////abs/path)
        return f"sqlite:///{cfg.database}"

    driver_map = {
        "mysql": "mysql+pymysql",
        "postgresql": "postgresql+psycopg2",
        "oracle": "oracle+cx_oracle",
        "impala": "hive",        # requires PyHive; graceful error below
    }
    dialect = driver_map.get(db_type, db_type)
    port_part = f":{cfg.port}" if cfg.port else ""
    return f"{dialect}://{cfg.username}:{cfg.password}@{cfg.host}{port_part}/{cfg.database}"


def get_engine(cfg: DbConnectionConfig) -> "Engine":
    """Create (and cache internally by SQLAlchemy) an Engine for the given config."""
    if not _HAS_SQLALCHEMY:
        raise RuntimeError("SQLAlchemy 未安装，请运行 pip install SQLAlchemy")
    url = _build_url(cfg)
    connect_args = cfg.extra_kwargs.get("connect_args", {})
    # Impala / Hive special-casing
    if cfg.db_type == "impala":
        connect_args.setdefault("auth_mechanism", "NOSASL")
    return create_engine(url, connect_args=connect_args, pool_pre_ping=True)


def execute_external_sql(engine: "Engine", sql: str) -> list[dict]:
    """Execute a SELECT on an external DB and return rows as list[dict]."""
    if not _HAS_SQLALCHEMY:
        raise RuntimeError("SQLAlchemy 未安装")
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def test_connection(cfg: DbConnectionConfig) -> dict:
    """Test connectivity. Returns {"ok": bool, "error": str|None}."""
    try:
        engine = get_engine(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "error": None}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def get_table_names(engine: "Engine") -> list[str]:
    """Inspect the database and return a list of physical table names."""
    if not _HAS_SQLALCHEMY:
        raise RuntimeError("SQLAlchemy 未安装")
    inspector = inspect(engine)
    return inspector.get_table_names()


def get_table_columns_info(engine: "Engine", table_name: str) -> list[dict]:
    """Inspect the database and return column info for a table."""
    if not _HAS_SQLALCHEMY:
        raise RuntimeError("SQLAlchemy 未安装")
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return [{"name": c["name"], "type": str(c["type"])} for c in columns]


def get_sample_data(engine: "Engine", table_name: str, limit: int = 3) -> list[dict]:
    """Fetch sample rows from a table."""
    if not _HAS_SQLALCHEMY:
        raise RuntimeError("SQLAlchemy 未安装")
    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
    return execute_external_sql(engine, sql)
