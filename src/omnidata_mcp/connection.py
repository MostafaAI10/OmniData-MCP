"""
DuckDB connection management for the OmniData MCP server.

DuckDB has no built-in statement timeout, so `run_with_timeout`
enforces one manually: the query runs on a worker thread, and if it
doesn't finish within `settings.query_timeout_seconds`, we call
`connection.interrupt()` to cancel it inside DuckDB and raise
TimeoutError back to the caller.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import duckdb

from omnidata_mcp.config import settings

_connection: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Return the process-wide DuckDB connection, creating it on first
    use. A single connection is reused across tool calls -- DuckDB
    connections are not designed to be opened per-query.
    """
    global _connection
    with _lock:
        if _connection is None:
            db_path = settings.duckdb_path
            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            _connection = duckdb.connect(db_path)
        return _connection


class QueryTimeoutError(TimeoutError):
    """Raised when a query exceeds settings.query_timeout_seconds."""


def run_with_timeout(sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
    """
    Execute `sql` against the shared connection, enforcing the
    configured query timeout. Returns the DuckDB relation so the
    caller can call .fetchall() / .fetchdf() / .description on it.
    """
    conn = get_connection()
    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _worker() -> None:
        try:
            result["relation"] = conn.execute(sql, params or [])
        except BaseException as exc:  # noqa: BLE001 - re-raised on the main thread
            error["exc"] = exc

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join(timeout=settings.query_timeout_seconds)

    if thread.is_alive():
        conn.interrupt()
        thread.join(timeout=2)
        raise QueryTimeoutError(
            f"Query exceeded the {settings.query_timeout_seconds}s timeout and was cancelled."
        )

    if "exc" in error:
        raise error["exc"]

    return result["relation"]
