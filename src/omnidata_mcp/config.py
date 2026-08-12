"""
Central configuration for the OmniData MCP server suite.

All servers (DuckDB, PySpark, etc.) import their settings from here so
that guardrails like row limits and query timeouts stay consistent
across the whole suite, and so credentials always come from the
environment rather than being hardcoded anywhere.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class OmniDataSettings(BaseSettings):
    """
    Settings are loaded from environment variables and/or a `.env` file
    in the project root. See `.env.example` for the full list of
    supported keys.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="OMNIDATA_",
        extra="ignore",
    )

    # --- DuckDB / SQL server ---------------------------------------------
    duckdb_path: str = "data/omnidata.duckdb"
    """Path to the DuckDB database file. Use ':memory:' for an ephemeral DB."""

    default_row_limit: int = 100
    """Rows returned when a query doesn't specify its own LIMIT."""

    max_row_limit: int = 1000
    """Hard cap on rows returned in a single tool response, regardless of LIMIT."""

    query_timeout_seconds: int = 10
    """Statement timeout enforced on every SQL query."""

    # --- PySpark server -----------------------------------------------------
    spark_master: str = "local[*]"
    """Spark master URL. Defaults to local multi-core execution."""

    spark_app_name: str = "omnidata-mcp"

    spark_job_timeout_seconds: int = 30
    """Max time a single execute_pyspark_job pipeline may run before cancellation."""

    max_spark_input_rows: int = 200_000
    """Cap on rows pulled from a DuckDB source table into Spark for a job."""

    max_spark_pipeline_steps: int = 20
    """Cap on operations per pipeline -- keeps jobs auditable and bounded."""

    # --- Visualization --------------------------------------------------
    max_chart_bytes: int = 1_500_000
    """Reject/downsize chart payloads larger than this (PNG bytes)."""

    max_chart_rows: int = 500
    """Cap rows plotted per chart -- keeps charts readable and payloads small."""

    chart_width: int = 900
    chart_height: int = 550

    chart_output_dir: str = "data/charts"
    """Every generated chart is also saved here as a real PNG file --
    a fallback for MCP clients that don't render inline tool images."""

    audit_log_path: str = "data/audit.log"
    """Append-only JSON-lines log of every tool call (tool name, args
    summary, duration, outcome). Never logs raw dataset rows -- only
    the query/operation metadata, consistent with this project's
    governance principle that raw data stays local."""


settings = OmniDataSettings()
