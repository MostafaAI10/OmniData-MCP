"""
Chart building for the OmniData MCP server (Phase 2).

Charts are built with Plotly and rendered to PNG bytes via Kaleido --
no pandas dependency, no browser required. Callers pass already-fetched
query columns/rows (see db_server.generate_chart), so this module has
no knowledge of SQL or the database connection.
"""

from __future__ import annotations

from typing import Any, Literal

import plotly.graph_objects as go

from omnidata_mcp.config import settings

ChartType = Literal["bar", "line", "scatter"]


class ChartBuildError(ValueError):
    """Raised for chart-spec problems (missing column, empty data, etc)."""


def _column_index(columns: list[str], name: str, role: str) -> int:
    try:
        return columns.index(name)
    except ValueError as exc:
        raise ChartBuildError(
            f"{role} column '{name}' not found in query result columns {columns}."
        ) from exc


def build_chart_png(
    columns: list[str],
    rows: list[list[Any]],
    chart_type: ChartType,
    x_column: str,
    y_column: str,
    series_column: str | None = None,
    title: str | None = None,
) -> bytes:
    """
    Build a chart from tabular query results and render it to PNG bytes.

    Args:
        columns: Column names, as returned by the query.
        rows: Row data, as returned by the query.
        chart_type: One of "bar", "line", "scatter".
        x_column, y_column: Which columns to plot.
        series_column: Optional column to split the data into multiple
            traces/series (e.g. one line per region).
        title: Optional chart title.
    """
    if not rows:
        raise ChartBuildError("Query returned no rows -- nothing to chart.")

    x_idx = _column_index(columns, x_column, "x_column")
    y_idx = _column_index(columns, y_column, "y_column")
    series_idx = _column_index(columns, series_column, "series_column") if series_column else None

    capped_rows = rows[: settings.max_chart_rows]
    fig = go.Figure()

    trace_kind = {"bar": go.Bar, "line": go.Scatter, "scatter": go.Scatter}[chart_type]
    trace_kwargs: dict[str, Any] = {}
    if chart_type == "line":
        trace_kwargs["mode"] = "lines+markers"
    elif chart_type == "scatter":
        trace_kwargs["mode"] = "markers"

    if series_idx is not None:
        series_groups: dict[Any, list[list[Any]]] = {}
        for row in capped_rows:
            series_groups.setdefault(row[series_idx], []).append(row)
        for series_value, group_rows in series_groups.items():
            fig.add_trace(
                trace_kind(
                    x=[r[x_idx] for r in group_rows],
                    y=[r[y_idx] for r in group_rows],
                    name=str(series_value),
                    **trace_kwargs,
                )
            )
    else:
        fig.add_trace(
            trace_kind(
                x=[r[x_idx] for r in capped_rows],
                y=[r[y_idx] for r in capped_rows],
                name=y_column,
                **trace_kwargs,
            )
        )

    fig.update_layout(
        title=title or f"{y_column} by {x_column}",
        xaxis_title=x_column,
        yaxis_title=y_column,
        template="plotly_white",
        legend_title=series_column if series_idx is not None else None,
    )

    png_bytes = fig.to_image(
        format="png", width=settings.chart_width, height=settings.chart_height
    )

    if len(png_bytes) > settings.max_chart_bytes:
        # Retry once at reduced dimensions rather than failing outright.
        png_bytes = fig.to_image(
            format="png",
            width=int(settings.chart_width * 0.7),
            height=int(settings.chart_height * 0.7),
        )
        if len(png_bytes) > settings.max_chart_bytes:
            raise ChartBuildError(
                f"Rendered chart ({len(png_bytes)} bytes) exceeds the "
                f"{settings.max_chart_bytes}-byte limit even after downsizing. "
                "Try charting fewer rows or a simpler chart type."
            )

    return png_bytes
