from __future__ import annotations

import os
import re
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from app.i18n import t
from app.python_sandbox import run_python_pipeline
from app.sql_guard import enforce_select_only, enforce_table_whitelist
from app.tools import execute_select_sql, execute_select_sql_with_mask


MAX_RESULT_ROWS = 50_000
MAX_RESULT_BYTES = 32 * 1024 * 1024
MAX_SCRATCH_TABLES = 24
SCRATCH_FILE_SWITCH_BYTES = 8 * 1024 * 1024
KERNEL_TTL_SECONDS = 60 * 60
SAFE_TABLE_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def _estimate_frame_bytes(frame: pd.DataFrame) -> int:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return 0
    try:
        return int(frame.memory_usage(deep=True).sum())
    except Exception:
        return 0


def _safe_table_name(name: str) -> str:
    normalized = SAFE_TABLE_NAME_RE.sub("_", str(name or "").strip())
    normalized = normalized.strip("_")
    return normalized or "scratch_table"


@dataclass
class NotebookKernel:
    kernel_id: str
    sandbox_id: str
    allowed_tables: list[str]
    selected_files: list[str]
    shared_namespace: dict = field(default_factory=dict)
    round_summaries: list[dict] = field(default_factory=list)
    resource_usage: dict = field(default_factory=lambda: {"peak_rows": 0, "peak_bytes": 0})
    scratch_mode: str = "memory"
    scratch_path: str = ""
    last_touch_ts: float = field(default_factory=time.time)
    cell_index: int = 0
    _scratch_conn: sqlite3.Connection | None = None

    def __post_init__(self) -> None:
        self._scratch_conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._scratch_conn.row_factory = sqlite3.Row

    @property
    def scratch_conn(self) -> sqlite3.Connection:
        if self._scratch_conn is None:
            self.__post_init__()
        assert self._scratch_conn is not None
        return self._scratch_conn

    def touch(self) -> None:
        self.last_touch_ts = time.time()

    def snapshot(self) -> dict:
        return {
            "kernel_id": self.kernel_id,
            "sandbox_id": self.sandbox_id,
            "allowed_tables": list(self.allowed_tables),
            "selected_files": list(self.selected_files),
            "scratch_mode": self.scratch_mode,
            "scratch_tables": self.list_temp_tables(),
            "resource_usage": dict(self.resource_usage),
        }

    def destroy(self) -> None:
        conn = self._scratch_conn
        self._scratch_conn = None
        if conn is not None:
            conn.close()
        if self.scratch_path:
            try:
                Path(self.scratch_path).unlink(missing_ok=True)
            except Exception:
                pass

    def list_temp_tables(self) -> list[str]:
        rows = self.scratch_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        return [str(row["name"]) for row in rows]

    def describe_table(self, name: str, source: str) -> dict:
        safe_name = _safe_table_name(name)
        if source == "scratch":
            rows = self.scratch_conn.execute(f"PRAGMA table_info({safe_name})").fetchall()
            columns = [{"name": str(row["name"]), "type": str(row["type"])} for row in rows]
            count_row = self.scratch_conn.execute(f"SELECT COUNT(*) AS row_count FROM {safe_name}").fetchone()
            return {"name": safe_name, "source": "scratch", "columns": columns, "row_count": int(count_row["row_count"])}
        raise ValueError(t("error_unknown_tool", tool=source, default=f"未知来源: {source}"))

    def _switch_to_file_backed_scratch(self) -> None:
        if self.scratch_mode == "file":
            return
        fd, path = tempfile.mkstemp(prefix=f"{self.kernel_id}_", suffix=".db")
        os.close(fd)
        file_conn = sqlite3.connect(path, check_same_thread=False)
        file_conn.row_factory = sqlite3.Row
        self.scratch_conn.backup(file_conn)
        old_conn = self._scratch_conn
        self._scratch_conn = file_conn
        self.scratch_mode = "file"
        self.scratch_path = path
        if old_conn is not None:
            old_conn.close()

    def _guard_frame_size(self, frame: pd.DataFrame, context: str) -> None:
        rows = len(frame.index)
        bytes_size = _estimate_frame_bytes(frame)
        self.resource_usage["peak_rows"] = max(int(self.resource_usage.get("peak_rows", 0)), rows)
        self.resource_usage["peak_bytes"] = max(int(self.resource_usage.get("peak_bytes", 0)), bytes_size)
        if rows > MAX_RESULT_ROWS:
            raise RuntimeError(
                t(
                    "error_python_exec",
                    exc=f"{context} rows exceeded limit ({rows}>{MAX_RESULT_ROWS})",
                    default=f"{context} rows exceeded limit ({rows}>{MAX_RESULT_ROWS})",
                )
            )
        if bytes_size > MAX_RESULT_BYTES:
            raise RuntimeError(
                t(
                    "error_python_exec",
                    exc=f"{context} memory exceeded limit ({bytes_size}>{MAX_RESULT_BYTES})",
                    default=f"{context} memory exceeded limit ({bytes_size}>{MAX_RESULT_BYTES})",
                )
            )

    def publish_df(self, name: str, df: pd.DataFrame, replace: bool = True) -> str:
        if not isinstance(df, pd.DataFrame):
            raise RuntimeError("publish_df requires a pandas DataFrame")
        safe_name = _safe_table_name(name)
        self._guard_frame_size(df, f"scratch table {safe_name}")
        if len(self.list_temp_tables()) >= MAX_SCRATCH_TABLES and safe_name not in self.list_temp_tables():
            raise RuntimeError(f"scratch table limit exceeded ({MAX_SCRATCH_TABLES})")
        if _estimate_frame_bytes(df) >= SCRATCH_FILE_SWITCH_BYTES:
            self._switch_to_file_backed_scratch()
        if_exists = "replace" if replace else "fail"
        df.to_sql(safe_name, self.scratch_conn, index=False, if_exists=if_exists)
        return safe_name

    def query_scratch_rows(self, sql: str) -> list[dict]:
        enforce_select_only(sql)
        enforce_table_whitelist(sql, self.list_temp_tables())
        frame = pd.read_sql(sql, self.scratch_conn)
        self._guard_frame_size(frame, "scratch query")
        return frame.to_dict(orient="records")

    def run_sql_cell(
        self,
        *,
        step_index: int,
        code: str,
        source: str,
        main_query_df: Callable[[str], pd.DataFrame],
    ) -> dict:
        self.touch()
        if source == "scratch":
            rows, used_tables = execute_select_sql(
                sql=code,
                allowed_tables=self.list_temp_tables(),
                query_executor=self.query_scratch_rows,
            )
        else:
            def df_query_executor(sql: str) -> list[dict]:
                frame = main_query_df(sql)
                self._guard_frame_size(frame, "main query")
                return frame.to_dict(orient="records")

            rows, used_tables = execute_select_sql_with_mask(
                sql=code,
                allowed_tables=self.allowed_tables,
                query_executor=df_query_executor,
            )
        frame = pd.DataFrame(rows)
        self.shared_namespace[f"df{self.cell_index}"] = frame
        self.shared_namespace["df"] = frame
        self.cell_index += 1
        return {
            "rows": rows,
            "tables": used_tables,
            "dataframe": frame,
            "source": source,
        }

    def run_python_cell(
        self,
        *,
        code: str,
        upload_rows: dict[str, list[dict]],
        upload_paths: dict[str, str],
        main_query_df: Callable[[str], pd.DataFrame],
        step_results: list[dict],
    ) -> dict:
        self.touch()

        # Re-bind round-local df aliases so Python `df0/df1/...` always map to
        # SQL outputs from the current analysis round (step order), not stale
        # session-level historical cells.
        round_sql_frames: list[pd.DataFrame] = []
        for step_result in step_results:
            if not isinstance(step_result, dict):
                continue
            if step_result.get("error"):
                continue
            rows = step_result.get("rows")
            if not isinstance(rows, list):
                continue
            round_sql_frames.append(pd.DataFrame(rows))
        for idx, frame in enumerate(round_sql_frames):
            self.shared_namespace[f"df{idx}"] = frame
        if round_sql_frames:
            self.shared_namespace["df"] = round_sql_frames[-1]

        def execute_select_sql_helper(sql: str, source: str = "main") -> list[dict]:
            if str(source).strip().lower() == "scratch":
                rows = self.query_scratch_rows(sql)
                self.shared_namespace["last_sql_rows"] = rows
                self.shared_namespace["last_sql_df"] = pd.DataFrame(rows)
                self.shared_namespace["df"] = self.shared_namespace["last_sql_df"]
                return rows
            rows, _ = execute_select_sql_with_mask(
                sql=sql,
                allowed_tables=self.allowed_tables,
                query_executor=lambda s: main_query_df(s).to_dict(orient="records"),
            )
            frame = pd.DataFrame(rows)
            self._guard_frame_size(frame, "main query")
            self.shared_namespace["last_sql_rows"] = rows
            self.shared_namespace["last_sql_df"] = pd.DataFrame(rows)
            self.shared_namespace["df"] = self.shared_namespace["last_sql_df"]
            return rows

        def execute_select_df_helper(sql: str, source: str = "main") -> pd.DataFrame:
            rows = execute_select_sql_helper(sql, source=source)
            return pd.DataFrame(rows)

        def publish_df_helper(name: str, df: pd.DataFrame, replace: bool = True) -> str:
            return self.publish_df(name, df, replace=replace)

        def list_temp_tables_helper() -> list[str]:
            return self.list_temp_tables()

        def describe_table_helper(name: str, source: str = "scratch") -> dict:
            return self.describe_table(name, source)

        result = run_python_pipeline(
            python_code=code,
            shared_namespace=self.shared_namespace,
            upload_rows=upload_rows,
            upload_paths=upload_paths,
            sql_tool=lambda sql: execute_select_sql_helper(sql, source="main"),
            step_results=step_results,
            extra_globals={
                "execute_select_sql": execute_select_sql_helper,
                "execute_select_df": execute_select_df_helper,
                "publish_df": publish_df_helper,
                "list_temp_tables": list_temp_tables_helper,
                "describe_table": describe_table_helper,
                "last_sql_rows": self.shared_namespace.get("last_sql_rows", []),
                "last_sql_df": self.shared_namespace.get("last_sql_df", pd.DataFrame()),
                "df": self.shared_namespace.get("df", pd.DataFrame()),
            },
        )
        final_rows = result.get("rows", [])
        self.shared_namespace[f"df{self.cell_index}"] = pd.DataFrame(final_rows)
        self.shared_namespace["df"] = self.shared_namespace[f"df{self.cell_index}"]
        self.cell_index += 1
        return result


_KERNELS: dict[str, NotebookKernel] = {}


def sweep_expired_kernels() -> None:
    now = time.time()
    expired = [kernel_id for kernel_id, kernel in _KERNELS.items() if now - kernel.last_touch_ts > KERNEL_TTL_SECONDS]
    for kernel_id in expired:
        destroy_kernel(kernel_id)


def create_kernel(session_id: str, sandbox_id: str, selected_tables: list[str], selected_files: list[str]) -> NotebookKernel:
    sweep_expired_kernels()
    kernel = _KERNELS.get(session_id)
    if kernel is None or kernel.sandbox_id != sandbox_id:
        if kernel is not None:
            kernel.destroy()
        kernel = NotebookKernel(
            kernel_id=session_id,
            sandbox_id=sandbox_id,
            allowed_tables=list(selected_tables),
            selected_files=list(selected_files),
        )
        _KERNELS[session_id] = kernel
    else:
        kernel.allowed_tables = list(selected_tables)
        kernel.selected_files = list(selected_files)
        kernel.touch()
    return kernel


def get_kernel_snapshot(kernel_id: str) -> dict:
    kernel = _KERNELS.get(kernel_id)
    return kernel.snapshot() if kernel else {}


def destroy_kernel(kernel_id: str) -> None:
    kernel = _KERNELS.pop(kernel_id, None)
    if kernel is not None:
        kernel.destroy()
