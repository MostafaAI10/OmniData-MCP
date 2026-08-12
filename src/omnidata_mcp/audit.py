"""
Audit logging and consistent error handling for OmniData MCP tools.

Every tool is wrapped with @audited, which:
  1. Logs one JSON line per call (tool name, args summary, duration,
     outcome) to settings.audit_log_path -- never raw dataset rows,
     only the query/operation metadata, consistent with this
     project's governance principle that raw data stays local.
  2. Normalizes error handling: any unhandled exception inside a tool
     (e.g. an unknown-dataset ValueError) is caught here and converted
     into the same {"error": "..."} shape the tools already use for
     their own validation failures, instead of surfacing as a raw,
     unhandled MCP protocol error.
"""

from __future__ import annotations

import functools
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from omnidata_mcp.config import settings

_log_lock = threading.Lock()

# Fields safe to log in full; anything else in kwargs is truncated hard.
# These are queries/specs the user's own LLM client sent -- not raw
# dataset rows -- so logging them is consistent with the project's
# "only metadata leaves the local environment" principle, and since
# the log itself never leaves the local machine either.
_MAX_FIELD_LEN = 500


def _summarize_args(kwargs: dict[str, Any]) -> dict[str, Any]:
    summary = {}
    for key, value in kwargs.items():
        text = repr(value)
        if len(text) > _MAX_FIELD_LEN:
            text = text[:_MAX_FIELD_LEN] + f"...<truncated, {len(text)} chars total>"
        summary[key] = text
    return summary


def _write_log_line(entry: dict[str, Any]) -> None:
    path = Path(settings.audit_log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, default=str)
    with _log_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def audited(func: Callable) -> Callable:
    """
    Decorator for MCP tool functions. Apply it *below* @mcp.tool() so
    mcp.tool() introspects this wrapper (functools.wraps preserves the
    original signature via __wrapped__, which inspect.signature()
    follows by default -- verified this doesn't break tool schema
    generation before relying on it).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        status = "success"
        error_message = None
        try:
            result = func(*args, **kwargs)
            if isinstance(result, dict) and "error" in result:
                status = "error"
                error_message = result["error"]
            return result
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error_message = str(exc)
            # Normalize to the same error shape the tools use for their
            # own validation failures, instead of an unhandled exception
            # surfacing as a raw MCP protocol-level error.
            return {"error": error_message}
        finally:
            duration_ms = round((time.monotonic() - start) * 1000, 1)
            try:
                _write_log_line(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "tool": func.__name__,
                        "args": _summarize_args(kwargs),
                        "duration_ms": duration_ms,
                        "status": status,
                        "error": error_message,
                    }
                )
            except Exception:  # noqa: BLE001
                # Audit logging must never break the tool call itself.
                pass

    return wrapper
