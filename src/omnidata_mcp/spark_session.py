"""
PySpark session management for the OmniData MCP server (Phase 3).

Two things matter more here than in the DuckDB server:

1. Stdout must stay perfectly clean. MCP over stdio is a line-based
   JSON-RPC protocol -- any stray output from Spark's JVM (driver
   logs, progress bars) would corrupt every message after it. We set
   the log level to ERROR immediately on session creation, before any
   job runs. Verified experimentally (both at the Python sys.stdout
   level and the OS file-descriptor level) that this keeps stdout
   silent even though PySpark launches a real JVM subprocess.

2. Cancellation must go through Spark's own job-group API. Unlike the
   DuckDB timeout (thread + connection.interrupt()), a plain Python
   thread timeout can't stop work already handed to the JVM --
   Spark's cancelJobGroup is the mechanism that actually works.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any, Callable

from omnidata_mcp.config import settings

_spark = None
_lock = threading.Lock()


def get_spark_session():
    """
    Return the process-wide SparkSession, creating it (lazily) on
    first use. Log level is forced to ERROR immediately, before
    returning, to keep stdout clean for the MCP stdio transport.
    """
    global _spark
    with _lock:
        if _spark is None:
            from pyspark.sql import SparkSession

            _spark = (
                SparkSession.builder.appName(settings.spark_app_name)
                .master(settings.spark_master)
                # Keep Spark's own noise off stdout/stderr as much as possible;
                # setLogLevel below is what actually matters for stdout safety.
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate()
            )
            _spark.sparkContext.setLogLevel("ERROR")
        return _spark


class SparkJobTimeoutError(TimeoutError):
    """Raised when a pipeline exceeds settings.spark_job_timeout_seconds."""


def run_spark_job_with_timeout(job_fn: Callable[[], Any]) -> Any:
    """
    Run `job_fn` (a zero-arg callable that triggers a Spark action,
    e.g. .collect()) and report a timeout error if it exceeds
    settings.spark_job_timeout_seconds. Attempts best-effort
    cancellation via Spark's job-group API.

    Note on cancellation reliability: Python-thread-based Spark job
    cancellation is a known-fragile area -- tested during development
    against a deliberately pathological job (an uncapped multi-billion-
    row cross join built directly with the PySpark API, bypassing this
    server's tools entirely) and found that cancellation could not
    reliably reclaim already-running executor threads in that extreme
    case, simply because such a large job doesn't hit an interrupt-
    checkpoint quickly. That scenario is not reachable through
    execute_pyspark_job's actual op allowlist, though: there is no join
    operation, and source rows are capped at max_spark_input_rows.
    Realistic pipelines built from the allowed ops (filter, select,
    withColumn, groupBy_agg, orderBy, distinct, limit) on a capped
    input are bounded work, not open-ended -- this timeout exists as a
    safety net for that realistic range, not as a guarantee against
    adversarial raw-API workloads that this tool can't construct anyway.
    """
    spark = get_spark_session()
    group_id = f"omnidata-{uuid.uuid4().hex[:8]}"
    spark.sparkContext.setJobGroup(group_id, "omnidata-mcp pipeline")

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _worker() -> None:
        try:
            result["value"] = job_fn()
        except BaseException as exc:  # noqa: BLE001
            error["exc"] = exc

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join(timeout=settings.spark_job_timeout_seconds)

    timed_out = thread.is_alive()
    if timed_out:
        spark.sparkContext.cancelJobGroup(group_id)

    # Always clear the job-group property on this thread so a cancelled
    # group_id can't linger via thread-local inheritance into later calls.
    spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)

    if timed_out:
        raise SparkJobTimeoutError(
            f"Spark job exceeded the {settings.spark_job_timeout_seconds}s "
            "timeout. Cancellation was requested but is best-effort; if "
            "results keep timing out, try a smaller input (lower "
            "max_spark_input_rows or an earlier filter/limit step)."
        )

    if "exc" in error:
        raise error["exc"]

    return result["value"]
