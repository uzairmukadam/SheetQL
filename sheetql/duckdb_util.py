"""Small DuckDB helpers shared by the CLI and interactive engine."""

from __future__ import annotations

import os
from typing import Dict, List

import duckdb


def quote_duckdb_identifier(name: str) -> str:
    """Double-quote an identifier and escape embedded quotes (SQL standard)."""
    if not name:
        raise ValueError("identifier must be non-empty")
    escaped = str(name).replace('"', '""')
    return f'"{escaped}"'


def apply_performance_pragmas(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Best-effort DuckDB settings for throughput (ignored if unsupported).

    - Disables strict insertion-order preservation (faster scans when ORDER BY absent).
    - Sets thread count to CPU count when DuckDB accepts it.
    """
    try:
        conn.execute("SET preserve_insertion_order = false;")
    except Exception:
        pass
    n = os.cpu_count()
    if n and n > 0:
        try:
            conn.execute(f"SET threads TO {n};")
        except Exception:
            pass


def fetch_columns_by_table(
    conn: duckdb.DuckDBPyConnection, table_names: List[str]
) -> Dict[str, List[str]]:
    """
    Return column names per table using one information_schema query.

    Falls back to per-table DESCRIBE if the bulk query is not supported.
    """
    result: Dict[str, List[str]] = {t: [] for t in table_names}
    if not table_names:
        return result

    try:
        in_list = ", ".join(quote_duckdb_identifier(t) for t in table_names)
        rows = conn.execute(f"""
            SELECT table_name, column_name, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name IN ({in_list})
            ORDER BY table_name, ordinal_position
            """).fetchall()
    except Exception:
        for t in table_names:
            try:
                d = conn.execute(f"DESCRIBE {quote_duckdb_identifier(t)}").fetchdf()
                result[t] = d["column_name"].tolist()
            except Exception:
                result[t] = []
        return result

    for tn, col, _pos in rows:
        tn_s = str(tn)
        if tn_s in result:
            result[tn_s].append(str(col))
    return result


def rename_relation(conn: duckdb.DuckDBPyConnection, old: str, new: str) -> None:
    """
    Rename a table or view using the correct DDL.

    Registered pandas frames appear as BASE TABLE; file-backed objects are VIEWs.
    """
    q_old = quote_duckdb_identifier(old)
    q_new = quote_duckdb_identifier(new)
    try:
        type_df = conn.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
            [old],
        ).fetchdf()
        obj_type = type_df.iloc[0]["table_type"] if not type_df.empty else "VIEW"
    except Exception:
        obj_type = "VIEW"

    if obj_type == "VIEW":
        conn.execute(f"ALTER VIEW {q_old} RENAME TO {q_new}")
    else:
        conn.execute(f"ALTER TABLE {q_old} RENAME TO {q_new}")


__all__ = [
    "apply_performance_pragmas",
    "fetch_columns_by_table",
    "quote_duckdb_identifier",
    "rename_relation",
]
