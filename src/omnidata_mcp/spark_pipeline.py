"""
Declarative PySpark pipeline execution for execute_pyspark_job.

Deliberately NOT an arbitrary-code-exec tool: an LLM-provided pipeline
is a JSON list of steps from a fixed, allowlisted set of operations,
each validated before touching Spark. This mirrors query_safety.py's
philosophy for the DuckDB server -- bounded, auditable operations
rather than unrestricted code.

Supported ops:
    {"op": "filter", "condition": "<column expression>"}
    {"op": "select", "columns": ["a", "b"]}
    {"op": "withColumn", "name": "new_col", "expression": "<column expression>"}
    {"op": "groupBy_agg", "group_by": ["a"], "aggregations": {"b": "sum"}}
    {"op": "orderBy", "columns": ["a"], "ascending": true}
    {"op": "distinct"}
    {"op": "limit", "n": 100}
"""

from __future__ import annotations

import re
from typing import Any

from omnidata_mcp.config import settings

_ALLOWED_OPS = {"filter", "select", "withColumn", "groupBy_agg", "orderBy", "distinct", "limit"}
_ALLOWED_AGG_FUNCS = {"sum", "avg", "mean", "count", "min", "max", "stddev", "variance"}

# Same defense-in-depth spirit as query_safety.py, applied to expr()
# strings -- expr() can only build column expressions (not statements),
# so this is a belt-and-suspenders check, not the primary safety layer.
_BLOCKED_IN_EXPR = re.compile(
    r"\b(drop|delete|insert|update|create|alter|truncate|exec|system)\b", re.IGNORECASE
)


class PipelineError(ValueError):
    """Raised for invalid pipeline specs (bad op, missing field, unsafe expression)."""


def _check_expr_safety(expression: str, field_name: str) -> None:
    if _BLOCKED_IN_EXPR.search(expression):
        raise PipelineError(f"{field_name} contains a disallowed keyword: {expression!r}")


def validate_pipeline(operations: list[dict[str, Any]]) -> None:
    if len(operations) > settings.max_spark_pipeline_steps:
        raise PipelineError(
            f"Pipeline has {len(operations)} steps, exceeding the "
            f"{settings.max_spark_pipeline_steps}-step limit."
        )
    for i, step in enumerate(operations):
        op = step.get("op")
        if op not in _ALLOWED_OPS:
            raise PipelineError(f"Step {i}: unknown op {op!r}. Allowed: {sorted(_ALLOWED_OPS)}")

        if op == "filter":
            if "condition" not in step:
                raise PipelineError(f"Step {i} (filter): missing 'condition'.")
            _check_expr_safety(step["condition"], f"Step {i} condition")
        elif op == "select":
            if not step.get("columns"):
                raise PipelineError(f"Step {i} (select): missing/empty 'columns'.")
        elif op == "withColumn":
            if "name" not in step or "expression" not in step:
                raise PipelineError(f"Step {i} (withColumn): needs 'name' and 'expression'.")
            _check_expr_safety(step["expression"], f"Step {i} expression")
        elif op == "groupBy_agg":
            if not step.get("group_by") or not step.get("aggregations"):
                raise PipelineError(f"Step {i} (groupBy_agg): needs 'group_by' and 'aggregations'.")
            bad_funcs = set(step["aggregations"].values()) - _ALLOWED_AGG_FUNCS
            if bad_funcs:
                raise PipelineError(
                    f"Step {i} (groupBy_agg): unsupported aggregation function(s) "
                    f"{bad_funcs}. Allowed: {sorted(_ALLOWED_AGG_FUNCS)}"
                )
        elif op == "orderBy":
            if not step.get("columns"):
                raise PipelineError(f"Step {i} (orderBy): missing/empty 'columns'.")
        elif op == "limit":
            n = step.get("n")
            if not isinstance(n, int) or n <= 0:
                raise PipelineError(f"Step {i} (limit): 'n' must be a positive integer.")


def apply_pipeline(df, operations: list[dict[str, Any]]):
    """
    Apply a validated list of operations to a Spark DataFrame,
    returning the transformed DataFrame. Call validate_pipeline()
    first -- this function assumes the spec is already checked.
    """
    from pyspark.sql import functions as F

    for step in operations:
        op = step["op"]
        if op == "filter":
            df = df.filter(F.expr(step["condition"]))
        elif op == "select":
            df = df.select(*step["columns"])
        elif op == "withColumn":
            df = df.withColumn(step["name"], F.expr(step["expression"]))
        elif op == "groupBy_agg":
            agg_exprs = [
                getattr(F, func)(col).alias(f"{func}_{col}")
                for col, func in step["aggregations"].items()
            ]
            df = df.groupBy(*step["group_by"]).agg(*agg_exprs)
        elif op == "orderBy":
            df = df.orderBy(*step["columns"], ascending=step.get("ascending", True))
        elif op == "distinct":
            df = df.distinct()
        elif op == "limit":
            df = df.limit(step["n"])
    return df
