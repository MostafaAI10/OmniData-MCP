"""
Query safety guardrails for the OmniData DuckDB server.

DuckDB doesn't have Postgres-style read-only roles, so we enforce
"read-only" ourselves at the statement level: every query an LLM
sends through `run_sql_query` is validated here before it ever
touches the database connection.

Policy (see project README "Design decisions"):
  - Exactly one statement per call (no ';'-separated chains).
  - The statement must start with SELECT, WITH, EXPLAIN, DESCRIBE,
    or SHOW.
  - The statement must not contain any data-/schema-mutating or
    system keyword, anywhere -- this catches attempts to smuggle a
    mutation inside a CTE, subquery, or comment.
"""

from __future__ import annotations

import re

_ALLOWED_START_KEYWORDS = {"select", "with", "explain", "describe", "show"}

# Anything on this list is rejected outright, regardless of where it
# appears in the statement (start, subquery, CTE body, etc).
_BLOCKED_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "create",
    "attach", "detach", "copy", "pragma", "install", "load",
    "export", "import", "call", "set", "reset", "vacuum",
    "checkpoint", "truncate", "grant", "revoke", "merge",
}

_COMMENT_PATTERN = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_WORD_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class UnsafeQueryError(ValueError):
    """Raised when a query fails the read-only safety policy."""


def strip_comments(sql: str) -> str:
    return _COMMENT_PATTERN.sub(" ", sql)


def validate_read_only_sql(sql: str) -> str:
    """
    Validate that `sql` is a single, read-only statement.

    Returns the cleaned (comment-stripped, trimmed) statement on
    success. Raises UnsafeQueryError with a clear reason on failure.
    """
    if not sql or not sql.strip():
        raise UnsafeQueryError("Query is empty.")

    cleaned = strip_comments(sql).strip()
    # Allow (and drop) a single trailing semicolon.
    cleaned = re.sub(r";\s*$", "", cleaned).strip()

    if ";" in cleaned:
        raise UnsafeQueryError(
            "Only a single SQL statement is allowed per call "
            "(no ';'-separated multi-statement queries)."
        )

    words = [w.lower() for w in _WORD_PATTERN.findall(cleaned)]
    if not words:
        raise UnsafeQueryError("Query does not contain a recognizable SQL statement.")

    if words[0] not in _ALLOWED_START_KEYWORDS:
        raise UnsafeQueryError(
            f"Query must start with one of "
            f"{sorted(_ALLOWED_START_KEYWORDS)} -- this server is read-only."
        )

    blocked_found = sorted(set(words) & _BLOCKED_KEYWORDS)
    if blocked_found:
        raise UnsafeQueryError(
            f"Query contains disallowed keyword(s): {blocked_found}. "
            "This server only permits read-only SELECT/WITH/EXPLAIN/"
            "DESCRIBE/SHOW statements."
        )

    return cleaned


def has_limit_clause(sql: str) -> bool:
    """Heuristic check for a top-level LIMIT clause."""
    return re.search(r"\blimit\b", sql, re.IGNORECASE) is not None
