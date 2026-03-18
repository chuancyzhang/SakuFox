import re
from collections.abc import Callable

from app.authorization import get_sensitive_fields
from app.sql_guard import apply_mask_to_rows, enforce_select_only, enforce_table_whitelist


def execute_select_sql(
    sql: str,
    allowed_tables: list[str],
    query_executor: Callable[[str], list[dict]],
) -> tuple[list[dict], list[str]]:
    """Execute SQL and return rows. Normalization is now handled by the executor (e.g., pd.read_sql)."""
    enforce_select_only(sql)
    used_tables = enforce_table_whitelist(sql, allowed_tables)
    rows = query_executor(sql)
    return rows, used_tables


def execute_select_sql_with_mask(
    sql: str,
    allowed_tables: list[str],
    query_executor: Callable[[str], list[dict]],
) -> tuple[list[dict], list[str]]:
    rows, used_tables = execute_select_sql(sql=sql, allowed_tables=allowed_tables, query_executor=query_executor)
    sensitive_fields = get_sensitive_fields(used_tables)
    return apply_mask_to_rows(rows, sensitive_fields), used_tables
