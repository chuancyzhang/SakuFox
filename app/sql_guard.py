import re
from app.i18n import t


FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke)\b", re.I)
TABLE_PAT = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.I)
SELECT_PAT = re.compile(r"^\s*select\b", re.I)


def extract_tables(sql: str) -> list[str]:
    return sorted(set(TABLE_PAT.findall(sql)))


def enforce_select_only(sql: str) -> None:
    if not SELECT_PAT.search(sql):
        raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))
    if FORBIDDEN.search(sql):
        raise ValueError(t("error_danger_op", default="SQL 包含危险操作"))


def enforce_table_whitelist(sql: str, allowed_tables: list[str]) -> list[str]:
    tables = extract_tables(sql)
    denied = [t for t in tables if t not in allowed_tables]
    if denied:
        raise PermissionError(t("error_denied_tables", tables=','.join(denied), default=f"越权访问表: {','.join(denied)}"))
    return tables


def apply_mask_to_rows(rows: list[dict], sensitive_fields: dict[str, list[str]]) -> list[dict]:
    sensitive_cols = set()
    for fields in sensitive_fields.values():
        sensitive_cols.update(fields)
    masked_rows = []
    for row in rows:
        out = dict(row)
        for col in row.keys():
            if col in sensitive_cols and row[col] is not None:
                value = str(row[col])
                if len(value) <= 4:
                    out[col] = "****"
                else:
                    out[col] = value[:2] + "*" * (len(value) - 4) + value[-2:]
        masked_rows.append(out)
    return masked_rows
