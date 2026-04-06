"""
Safe Python sandbox for AI-generated analysis code.

Pre-injects common data science libraries (pandas, numpy, json, math, re,
datetime, collections) so AI code can reference them directly without using
import statements.  A safe subset of __builtins__ is exposed so the sandbox
stays safe while avoiding "__import__ not found" errors.
"""
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime as _datetime, date as _date, timedelta as _timedelta
from io import BytesIO as _BytesIO, StringIO as _StringIO
from pathlib import Path as _Path
import json as _json
import math as _math
import re as _re
import io as _io

import numpy as _np
import pandas as pd
from app.i18n import t

# sklearn — imported lazily so missing package gives a clear error at sandbox call time
try:
    from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge, Lasso
    from sklearn.ensemble import (
        RandomForestClassifier, RandomForestRegressor,
        GradientBoostingClassifier, GradientBoostingRegressor,
    )
    from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
    from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.metrics import (
        accuracy_score, f1_score, precision_score, recall_score,
        mean_squared_error, mean_absolute_error, r2_score,
        classification_report, confusion_matrix,
    )
    from sklearn.decomposition import PCA
    from sklearn.pipeline import Pipeline
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False
    # Placeholders so local_vars injection doesn't fail
    (LinearRegression, LogisticRegression, Ridge, Lasso,
     RandomForestClassifier, RandomForestRegressor,
     GradientBoostingClassifier, GradientBoostingRegressor,
     KMeans, DBSCAN, AgglomerativeClustering,
     StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder,
     train_test_split, cross_val_score,
     accuracy_score, f1_score, precision_score, recall_score,
     mean_squared_error, mean_absolute_error, r2_score,
     classification_report, confusion_matrix,
     PCA, Pipeline) = (None,) * 27


# ── Safe builtins exposed to the sandbox ─────────────────────────────────────
ALLOWED_BUILTINS = {
    # built-in types
    "bool": bool,
    "bytes": bytes,
    "complex": complex,
    "dict": dict,
    "float": float,
    "frozenset": frozenset,
    "int": int,
    "list": list,
    "set": set,
    "str": str,
    "tuple": tuple,
    "type": type,
    # itertools-like
    "abs": abs,
    "all": all,
    "any": any,
    "enumerate": enumerate,
    "filter": filter,
    "map": map,
    "max": max,
    "min": min,
    "pow": pow,
    "range": range,
    "reversed": reversed,
    "round": round,
    "sorted": sorted,
    "sum": sum,
    "zip": zip,
    # I/O
    "print": print,
    "repr": repr,
    "open": open,
    # inspection
    "callable": callable,
    "dir": dir,
    "getattr": getattr,
    "hasattr": hasattr,
    "isinstance": isinstance,
    "issubclass": issubclass,
    "len": len,
    # exceptions
    "Exception": Exception,
    "ValueError": ValueError,
    "KeyError": KeyError,
    "TypeError": TypeError,
    "IndexError": IndexError,
    "RuntimeError": RuntimeError,
    "StopIteration": StopIteration,
    "NotImplementedError": NotImplementedError,
    # import
    "__import__": __import__,
    # misc
    "hash": hash,
    "id": id,
    "iter": iter,
    "next": next,
    "object": object,
    "property": property,
    "staticmethod": staticmethod,
    "classmethod": classmethod,
    "super": super,
    "vars": vars,
}


def _safe_first_row(frame: pd.DataFrame, default: dict | None = None) -> dict | None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return default
    try:
        return frame.iloc[0].to_dict()
    except Exception:
        return default


def _safe_get_value(frame: pd.DataFrame, column: str, default=None, row_index: int = 0):
    if not isinstance(frame, pd.DataFrame) or not column:
        return default
    if column not in frame.columns:
        return default
    if row_index < 0 or row_index >= len(frame):
        return default
    try:
        return frame.iloc[row_index][column]
    except Exception:
        return default


def _safe_has_columns(frame: pd.DataFrame, *columns: str) -> bool:
    if not isinstance(frame, pd.DataFrame):
        return False
    return all(column in frame.columns for column in columns)


def _normalize_python_result(frame: pd.DataFrame | list | None) -> tuple[list[dict], pd.DataFrame]:
    if isinstance(frame, list):
        frame = pd.DataFrame(frame)
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame()
    return frame.to_dict(orient="records"), frame


def _is_json_safe_scalar(value) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _export_analysis_scalars(shared_namespace: dict) -> dict[str, object]:
    exported: dict[str, object] = {}
    blocked_names = {
        "__builtins__",
        "pd",
        "pandas",
        "np",
        "numpy",
        "json",
        "math",
        "re",
        "io",
        "Path",
        "StringIO",
        "BytesIO",
        "datetime",
        "date",
        "timedelta",
        "Counter",
        "defaultdict",
        "uploaded_dataframes",
        "uploaded_file_paths",
        "execute_select_sql",
        "execute_select_df",
        "publish_df",
        "list_temp_tables",
        "describe_table",
        "safe_first_row",
        "safe_get_value",
        "safe_has_columns",
        "chart_specs",
        "step_results",
        "final_df",
        "df",
        "last_sql_df",
        "last_sql_rows",
        "insight_hints",
    }
    for key, value in shared_namespace.items():
        if key.startswith("_") or key in blocked_names:
            continue
        if _is_json_safe_scalar(value):
            text = str(value).strip() if isinstance(value, str) else ""
            if isinstance(value, str) and (not text or len(text) > 500):
                continue
            exported[key] = value
            continue
        if isinstance(value, _np.generic):
            exported[key] = value.item()
            continue
        if isinstance(value, (list, tuple)) and len(value) <= 10 and all(_is_json_safe_scalar(item) for item in value):
            exported[key] = list(value)
            continue
    return exported


def run_python_pipeline(
    python_code: str,
    shared_namespace: dict,
    upload_rows: dict[str, list[dict]],
    upload_paths: dict[str, str],
    sql_tool: Callable[[str], list[dict]],
    step_results: list[dict] | None = None,
    extra_globals: dict | None = None,
) -> dict:
    """
    Execute Python code within a persistent shared namespace.
    """
    uploads_df = {name: pd.DataFrame(rows) for name, rows in upload_rows.items()}

    def sdk_execute_select_sql(sql: str) -> list[dict]:
        return sql_tool(sql)

    # Initialize namespace if empty (first step)
    if "pd" not in shared_namespace:
        shared_namespace.update({
            "pd": pd,
            "pandas": pd,
            "np": _np,
            "numpy": _np,
            "json": _json,
            "math": _math,
            "re": _re,
            "io": _io,
            "Path": _Path,
            "StringIO": _StringIO,
            "BytesIO": _BytesIO,
            "datetime": _datetime,
            "date": _date,
            "timedelta": _timedelta,
            "Counter": Counter,
            "defaultdict": defaultdict,
            "LinearRegression": LinearRegression,
            "LogisticRegression": LogisticRegression,
            "Ridge": Ridge,
            "Lasso": Lasso,
            "RandomForestClassifier": RandomForestClassifier,
            "RandomForestRegressor": RandomForestRegressor,
            "GradientBoostingClassifier": GradientBoostingClassifier,
            "GradientBoostingRegressor": GradientBoostingRegressor,
            "KMeans": KMeans,
            "DBSCAN": DBSCAN,
            "AgglomerativeClustering": AgglomerativeClustering,
            "StandardScaler": StandardScaler,
            "MinMaxScaler": MinMaxScaler,
            "LabelEncoder": LabelEncoder,
            "OneHotEncoder": OneHotEncoder,
            "train_test_split": train_test_split,
            "cross_val_score": cross_val_score,
            "accuracy_score": accuracy_score,
            "f1_score": f1_score,
            "precision_score": precision_score,
            "recall_score": recall_score,
            "mean_squared_error": mean_squared_error,
            "mean_absolute_error": mean_absolute_error,
            "r2_score": r2_score,
            "classification_report": classification_report,
            "confusion_matrix": confusion_matrix,
            "PCA": PCA,
            "Pipeline": Pipeline,
            "uploaded_dataframes": uploads_df,
            "uploaded_file_paths": upload_paths,
            "execute_select_sql": sdk_execute_select_sql,
            "safe_first_row": _safe_first_row,
            "safe_get_value": _safe_get_value,
            "safe_has_columns": _safe_has_columns,
            "chart_specs": [],
            "insight_hints": [],
            "final_df": pd.DataFrame(),
        })

    # Ensure step_results is always available and fresh
    shared_namespace["step_results"] = step_results or []
    if extra_globals:
        shared_namespace.update(extra_globals)
    
    # Ensure __builtins__ is available but safe
    if "__builtins__" not in shared_namespace:
        shared_namespace["__builtins__"] = ALLOWED_BUILTINS

    try:
        # EXECUTION: use the shared_namespace as BOTH globals and locals to allow persistence.
        # In Python, when globals and locals are the same object, 
        # variable assignments are correctly persisted in that object.
        exec(python_code, shared_namespace)
        
        # Post-process: find the best result
        final_df = shared_namespace.get("final_df")
        if not isinstance(final_df, (pd.DataFrame, list)):
            # If final_df not set or not a DF/list, try to use 'df'
            if isinstance(shared_namespace.get("df"), pd.DataFrame):
                final_df = shared_namespace["df"]
            elif isinstance(shared_namespace.get("df"), list):
                final_df = pd.DataFrame(shared_namespace["df"])
            else:
                final_df = pd.DataFrame() # Fallback

        if isinstance(final_df, list):
            final_df = pd.DataFrame(final_df)

        rows = final_df.to_dict(orient="records")
        
        # Consolidate charts and insights
        chart_specs = shared_namespace.get("chart_specs", [])
        if not isinstance(chart_specs, list): chart_specs = []
        for spec in chart_specs:
            if isinstance(spec, dict) and "engine" not in spec:
                spec["engine"] = "echarts"

        insight_hints = shared_namespace.get("insight_hints", [])
        if not isinstance(insight_hints, list): insight_hints = []

        return {
            "rows": rows,
            "chart_specs": chart_specs,
            "insight_hints": [str(x) for x in insight_hints],
            "exported_vars": _export_analysis_scalars(shared_namespace),
        }

    except (KeyError, IndexError) as exc:
        # Build helpful diagnostic message and degrade gracefully instead of
        # aborting the whole one-click analysis round.
        available_vars = sorted([k for k, v in shared_namespace.items() if not k.startswith("__")])
        df_cols = list(shared_namespace["df"].columns) if isinstance(shared_namespace.get("df"), pd.DataFrame) else []
        msg = t(
            "error_python_access",
            exc=str(exc),
            vars=available_vars,
            cols=df_cols,
            default=(
                f"访问数据时出错: {str(exc)}\n"
                f"可用变量: {available_vars}\n"
                f"当前 df 字段: {df_cols}\n"
                "建议先检查 DataFrame 是否为空，或使用 safe_first_row / safe_get_value / safe_has_columns。"
            ),
        )
        fallback_source = shared_namespace.get("final_df")
        if fallback_source is None or not isinstance(fallback_source, (pd.DataFrame, list)):
            fallback_source = shared_namespace.get("df")
        fallback_rows, _ = _normalize_python_result(fallback_source)
        chart_specs = shared_namespace.get("chart_specs", [])
        if not isinstance(chart_specs, list):
            chart_specs = []
        return {
            "rows": fallback_rows,
            "chart_specs": chart_specs,
            "insight_hints": [str(x) for x in shared_namespace.get("insight_hints", []) if str(x).strip()],
            "warning": msg,
            "exported_vars": _export_analysis_scalars(shared_namespace),
        }
    except Exception as exc:
        raise RuntimeError(t("error_python_exec", exc=str(exc), default=f"Python 执行出错: {str(exc)}")) from exc
