# Changelog

All notable changes to OmniData MCP are documented here, phase by phase.

## Phase 4 -- Hardening & polish

- **Audit logging**: every tool call is now logged as a JSON line to
  `data/audit.log` (tool name, argument summary, duration, outcome).
  Query/operation metadata only -- raw dataset rows are never logged,
  consistent with the project's core governance principle.
- **Consistent error handling**: all 8 tools now return the same
  `{"error": "..."}` shape on failure. Previously, `get_schema`,
  `get_row_count`, and `execute_pyspark_job` let an unknown-dataset
  check raise an unhandled exception instead, surfacing as a raw MCP
  protocol-level error rather than a clean, structured response. Fixed
  via a shared `@audited` decorator that normalizes both logging and
  error handling in one place.
- Added `LICENSE` (MIT) and this changelog.
- README rewritten as a complete reference: full tool table, security
  model, architecture reconciliation (see below), troubleshooting
  appendix consolidating real issues hit during development.

## Phase 3 -- PySpark server

- Added `execute_pyspark_job`: declarative transformation pipelines
  (filter / select / withColumn / groupBy_agg / orderBy / distinct /
  limit) against a DuckDB source table, executed via a lazily
  initialized `SparkSession`. Deliberately not an arbitrary-code-exec
  tool -- pipelines are validated against a fixed op allowlist, same
  philosophy as the SQL safety layer.
- Verified (both at the Python and OS file-descriptor level) that
  Spark's JVM logging never leaks onto stdout, which would corrupt the
  MCP stdio protocol.
- Found and fixed a real bug during testing: an earlier version
  attempted true job cancellation via `cancelJobGroup` on timeout,
  which was shown to be unreliable across Python-thread boundaries in
  extreme cases. Redesigned as a wall-clock timeout with best-effort
  cancellation, documented honestly rather than presented as a
  stronger guarantee than it is.

## Phase 2 -- Visualization

- Added `generate_chart`: bar/line/scatter charts (with optional
  multi-series grouping) rendered via Plotly, from any read-only
  query result.
- Pinned `kaleido==0.2.1` deliberately (self-contained renderer) after
  discovering `kaleido>=1.0` requires a separate Chrome install.
- Every chart is saved to `data/charts/` as a real PNG file *and*
  returned as an inline MCP image content block -- the file is the
  reliable path, since Claude Desktop does not currently render inline
  images from locally-installed unpacked extensions.

## Phase 1 -- DuckDB tools

- Added `list_datasets`, `get_schema`, `get_row_count`,
  `run_sql_query`, `get_data_profile`.
- Read-only SQL safety layer: single-statement only, allowlisted
  statement types (SELECT/WITH/EXPLAIN/DESCRIBE/SHOW), auto-injected
  `LIMIT`, hard row cap, and a thread-based query timeout (DuckDB has
  no native statement timeout).
- Verified against 14 attack/edge cases (multi-statement injection,
  comment-hidden DDL, `ATTACH` smuggling, etc.) and a genuinely slow
  query to confirm the timeout actually cancels work, not just exists
  as unused code.

## Phase 0 -- Scaffolding

- `uv`-managed project, `mcp` SDK 2.0.0 (`MCPServer`, the renamed
  successor to the earlier `FastMCP` class), `health_check` tool.
- Verified end-to-end over real stdio with an actual MCP client, not
  just a direct function call.
