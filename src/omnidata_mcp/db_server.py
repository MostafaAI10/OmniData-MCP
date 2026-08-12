"""
OmniData MCP -- Database Server (Phase 1: real DuckDB tools)

Entry point for the DuckDB / SQL side of the suite. Every tool here
follows the governance principle from the project overview: raw data
never leaves this process. Only bounded, structured results (rows
capped at settings.max_row_limit, summarized profiles, schema
metadata) are ever returned to the calling LLM.

Tools:
    health_check     -- connectivity + config sanity check
    list_datasets    -- discover tables/views in the database
    get_schema       -- column names/types for one dataset
    get_row_count    -- exact row count for one dataset
    run_sql_query    -- bounded, read-only SQL (SELECT/WITH/EXPLAIN/
                         DESCRIBE/SHOW only; auto-LIMIT; timeout-enforced)
    get_data_profile -- per-column stats via DuckDB's SUMMARIZE
"""

from __future__ import annotations

import decimal
import uuid
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.mcpserver import Image, MCPServer

from omnidata_mcp.charts import ChartBuildError, ChartType, build_chart_png
from omnidata_mcp.config import settings
from omnidata_mcp.connection import QueryTimeoutError, get_connection, run_with_timeout
from omnidata_mcp.query_safety import UnsafeQueryError, has_limit_clause, validate_read_only_sql
from omnidata_mcp.spark_pipeline import PipelineError, apply_pipeline, validate_pipeline
from omnidata_mcp.spark_session import SparkJobTimeoutError, get_spark_session, run_spark_job_with_timeout
from omnidata_mcp.audit import audited

# `MCPServer` is the mcp-sdk 2.0.0 successor to the earlier `FastMCP` class
# (same decorator-based API: @mcp.tool(), mcp.run()).
mcp = MCPServer("omnidata-db-server")


def _quote_identifier(name: str) -> str:
    """Safely quote a SQL identifier (double-quote + escape internal quotes)."""
    return '"' + name.replace('"', '""') + '"'


def _known_dataset_names() -> set[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return {r[0] for r in rows}


def _require_known_dataset(name: str) -> None:
    if name not in _known_dataset_names():
        raise ValueError(
            f"Unknown dataset '{name}'. Call list_datasets to see available tables/views."
        )


@mcp.tool()
@audited
def health_check() -> dict:
    """
    Verify the OmniData database server is running, can reach DuckDB,
    and report its current guardrail configuration (row limits,
    timeout, DB path).

    Use this first to confirm the MCP connection is alive.
    """
    try:
        get_connection().execute("SELECT 1").fetchall()
        db_status = "connected"
    except Exception as exc:  # noqa: BLE001
        db_status = f"error: {exc}"

    return {
        "status": "ok",
        "server": "omnidata-db-server",
        "phase": "1 (DuckDB tools)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duckdb_status": db_status,
        "config": {
            "duckdb_path": settings.duckdb_path,
            "default_row_limit": settings.default_row_limit,
            "max_row_limit": settings.max_row_limit,
            "query_timeout_seconds": settings.query_timeout_seconds,
        },
    }


@mcp.tool()
@audited
def list_datasets() -> dict:
    """
    List every table and view available in the database, with its
    type and column count. Call this first when exploring an unknown
    database -- run_sql_query needs real table names to work with.
    """
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchall()

    datasets = []
    for name, table_type in rows:
        col_count = conn.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ?",
            [name],
        ).fetchone()[0]
        datasets.append({"name": name, "type": table_type, "column_count": col_count})

    return {"datasets": datasets, "count": len(datasets)}


@mcp.tool()
@audited
def get_schema(dataset: str) -> dict:
    """
    Get the column names, types, and nullability for one dataset
    (table or view). Use list_datasets first to get valid names.

    Args:
        dataset: Exact table or view name, as returned by list_datasets.
    """
    _require_known_dataset(dataset)
    conn = get_connection()
    rows = conn.execute(f"DESCRIBE {_quote_identifier(dataset)}").fetchall()
    columns = [
        {
            "name": r[0],
            "type": r[1],
            "nullable": r[2] == "YES" if len(r) > 2 else None,
        }
        for r in rows
    ]
    return {"dataset": dataset, "columns": columns}


@mcp.tool()
@audited
def get_row_count(dataset: str) -> dict:
    """
    Get the exact row count for one dataset. Use this before
    run_sql_query on a large table to know how much data you're
    sampling from with a LIMIT.

    Args:
        dataset: Exact table or view name, as returned by list_datasets.
    """
    _require_known_dataset(dataset)
    conn = get_connection()
    count = conn.execute(f"SELECT count(*) FROM {_quote_identifier(dataset)}").fetchone()[0]
    return {"dataset": dataset, "row_count": count}


@mcp.tool()
@audited
def run_sql_query(sql: str, row_limit: int | None = None) -> dict:
    """
    Execute a read-only SQL query and return structured results.

    Safety guardrails (see README "Design decisions"):
      - Only SELECT / WITH / EXPLAIN / DESCRIBE / SHOW statements are
        permitted; DDL/DML and system commands are rejected.
      - Only one statement per call.
      - If the query has no LIMIT clause, one is auto-injected using
        `row_limit` (or the server default). Results are always
        hard-capped at the server's max_row_limit, regardless of what
        you request, to keep responses bounded.
      - The query is cancelled if it runs longer than the server's
        configured timeout.

    Use get_row_count first if you need to know the true size of a
    table beyond what this capped result shows.

    Args:
        sql: A single read-only SQL statement.
        row_limit: Desired max rows (capped at the server's
            max_row_limit). Ignored if your query already has LIMIT.
    """
    try:
        cleaned = validate_read_only_sql(sql)
    except UnsafeQueryError as exc:
        return {"error": str(exc), "sql": sql}

    effective_limit = min(row_limit or settings.default_row_limit, settings.max_row_limit)
    truncated = False
    query_to_run = cleaned
    if not has_limit_clause(cleaned):
        query_to_run = f"{cleaned} LIMIT {effective_limit}"
        truncated = True

    try:
        relation = run_with_timeout(query_to_run)
        columns = [d[0] for d in relation.description]
        rows = relation.fetchall()
    except QueryTimeoutError as exc:
        return {"error": str(exc), "sql": cleaned}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Query failed: {exc}", "sql": cleaned}

    # Hard cap the returned payload regardless of what the query produced.
    hard_capped = False
    if len(rows) > settings.max_row_limit:
        rows = rows[: settings.max_row_limit]
        hard_capped = True

    return {
        "sql_executed": query_to_run,
        "columns": columns,
        "rows": [list(r) for r in rows],
        "row_count_returned": len(rows),
        "limit_auto_injected": truncated,
        "hard_capped": hard_capped,
    }


@mcp.tool()
@audited
def get_data_profile(dataset: str) -> dict:
    """
    Generate summary statistics for every column in a dataset: type,
    null percentage, approximate distinct count, min/max, and (for
    numeric columns) mean/stddev/quartiles. Uses DuckDB's built-in
    SUMMARIZE, so it runs efficiently even on large tables.

    Args:
        dataset: Exact table or view name, as returned by list_datasets.
    """
    _require_known_dataset(dataset)
    conn = get_connection()
    try:
        relation = run_with_timeout(f"SUMMARIZE {_quote_identifier(dataset)}")
        columns = [d[0] for d in relation.description]
        rows = relation.fetchall()
    except QueryTimeoutError as exc:
        return {"error": str(exc), "dataset": dataset}

    profile = [dict(zip(columns, row)) for row in rows]
    return {"dataset": dataset, "profile": profile}


@mcp.tool()
@audited
def generate_chart(
    sql: str,
    chart_type: ChartType,
    x_column: str,
    y_column: str,
    series_column: str | None = None,
    title: str | None = None,
):
    """
    Run a read-only SQL query and render the result as a chart image
    (bar, line, or scatter). Subject to the same read-only safety
    guardrails as run_sql_query.

    Returns an inline chart image on success. On failure (bad SQL,
    missing column, empty result, oversized render), returns an error
    dict instead -- check for an "error" key if the result isn't an
    image. (No static return-type annotation here: the mcp SDK's
    output-schema generation can't handle Image inside a Union type.)

    Args:
        sql: A single read-only SQL statement producing the data to
            chart. Aggregate/group the data yourself for cleaner
            charts (e.g. GROUP BY category).
        chart_type: "bar", "line", or "scatter".
        x_column: Column name (from the query result) for the x-axis.
        y_column: Column name (from the query result) for the y-axis.
        series_column: Optional column to split into multiple series/
            traces (e.g. one line per region).
        title: Optional chart title. Defaults to "{y_column} by {x_column}".
    """
    try:
        cleaned = validate_read_only_sql(sql)
    except UnsafeQueryError as exc:
        return {"error": str(exc), "sql": sql}

    query_to_run = cleaned
    if not has_limit_clause(cleaned):
        query_to_run = f"{cleaned} LIMIT {settings.max_chart_rows}"

    try:
        relation = run_with_timeout(query_to_run)
        columns = [d[0] for d in relation.description]
        rows = [list(r) for r in relation.fetchall()]
    except QueryTimeoutError as exc:
        return {"error": str(exc), "sql": cleaned}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Query failed: {exc}", "sql": cleaned}

    try:
        png_bytes = build_chart_png(
            columns=columns,
            rows=rows,
            chart_type=chart_type,
            x_column=x_column,
            y_column=y_column,
            series_column=series_column,
            title=title,
        )
    except ChartBuildError as exc:
        return {"error": str(exc), "sql_executed": query_to_run, "columns": columns}

    # Always save a real file too: some MCP clients (e.g. Claude Desktop
    # with locally-installed unpacked extensions) don't render inline
    # image content blocks, so a file the user can open directly is the
    # dependable path rather than the only path.
    output_dir = Path(settings.chart_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"chart_{timestamp}_{uuid.uuid4().hex[:8]}.png"
    filepath = output_dir / filename
    filepath.write_bytes(png_bytes)

    return [
        f"Chart saved to {filepath.resolve()} ({len(png_bytes)} bytes). "
        "An inline preview follows if your client supports it.",
        Image(data=png_bytes, format="png"),
    ]


def _duckdb_value_to_python(value):
    """Coerce DuckDB result values into types Spark's schema inference handles cleanly."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


@mcp.tool()
@audited
def execute_pyspark_job(source_dataset: str, operations: list[dict], row_limit: int | None = None) -> dict:
    """
    Run a declarative PySpark transformation pipeline against a DuckDB
    dataset -- for heavier aggregations/transformations than
    run_sql_query is meant for. NOT arbitrary code execution: each
    step must be one of a fixed set of operations, validated before
    running.

    Supported operations (each a dict with an "op" key):
      {"op": "filter", "condition": "<column expression>"}
        e.g. {"op": "filter", "condition": "revenue > 100"}
      {"op": "select", "columns": ["a", "b"]}
      {"op": "withColumn", "name": "new_col", "expression": "<column expression>"}
        e.g. {"op": "withColumn", "name": "margin", "expression": "revenue - cost"}
      {"op": "groupBy_agg", "group_by": ["a"], "aggregations": {"b": "sum"}}
        aggregations map column -> function; functions:
        sum, avg, mean, count, min, max, stddev, variance
      {"op": "orderBy", "columns": ["a"], "ascending": true}
      {"op": "distinct"}
      {"op": "limit", "n": 100}

    Steps run in the order given. A final row cap is always applied
    to the output regardless of what the pipeline itself requests.

    Args:
        source_dataset: Exact table/view name, as returned by list_datasets.
        operations: Ordered list of pipeline steps (see above).
        row_limit: Desired max rows returned (capped at the server's
            max_row_limit).
    """
    _require_known_dataset(source_dataset)

    try:
        validate_pipeline(operations)
    except PipelineError as exc:
        return {"error": str(exc)}

    conn = get_connection()
    try:
        relation = conn.execute(
            f"SELECT * FROM {_quote_identifier(source_dataset)} "
            f"LIMIT {settings.max_spark_input_rows}"
        )
        columns = [d[0] for d in relation.description]
        rows = [
            tuple(_duckdb_value_to_python(v) for v in row) for row in relation.fetchall()
        ]
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Failed to load source_dataset '{source_dataset}': {exc}"}

    if not rows:
        return {"error": f"Dataset '{source_dataset}' has no rows to process."}

    try:
        spark = get_spark_session()
        df = spark.createDataFrame(rows, schema=columns)
        df = apply_pipeline(df, operations)

        effective_limit = min(row_limit or settings.default_row_limit, settings.max_row_limit)
        df = df.limit(effective_limit)
        out_columns = df.columns

        result_rows = run_spark_job_with_timeout(df.collect)
    except SparkJobTimeoutError as exc:
        return {"error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Pipeline execution failed: {exc}"}

    return {
        "source_dataset": source_dataset,
        "columns": out_columns,
        "rows": [list(r) for r in result_rows],
        "row_count_returned": len(result_rows),
        "source_rows_loaded": len(rows),
        "note": (
            f"source_rows_loaded reflects rows pulled from DuckDB into Spark "
            f"(capped at {settings.max_spark_input_rows}); row_count_returned "
            "is after your pipeline's own transformations and the server's output cap."
        ),
    }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
