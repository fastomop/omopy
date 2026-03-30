"""Plot functions for summarised results.

Provides :func:`scatter_plot`, :func:`bar_plot`, and :func:`box_plot`
using Plotly as the rendering backend.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult
from omopy.vis._format import tidy_result
from omopy.vis._style import PlotStyle, customise_text, default_plot_style

__all__ = [
    "bar_plot",
    "box_plot",
    "scatter_plot",
]

PlotType = Literal["plotly"]

# ── scatter_plot ──────────────────────────────────────────────────────────


def scatter_plot(
    result: SummarisedResult | pl.DataFrame,
    *,
    x: str,
    y: str,
    line: bool = False,
    point: bool = True,
    ribbon: bool = False,
    y_min: str | None = None,
    y_max: str | None = None,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: PlotStyle | None = None,
    group: str | None = None,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
) -> Any:
    """Create a scatter/line/ribbon plot from results.

    Args:
        result: A :class:`SummarisedResult` (auto-tidied) or a
            :class:`~polars.DataFrame`.
        x: Column name for x-axis.
        y: Column name or estimate name for y-axis.
        line: Whether to connect points with lines.
        point: Whether to show points.
        ribbon: Whether to show a ribbon (requires *y_min* and *y_max*).
        y_min: Column/estimate for ribbon lower bound.
        y_max: Column/estimate for ribbon upper bound.
        facet: Column(s) for faceting.  A single string creates a
            single-row facet; a list of two creates a grid (row, col).
        colour: Column for colour aesthetic.
        style: Plot style configuration.
        group: Column for grouping (defaults to *colour*).
        title: Plot title.
        x_title: X-axis title. If ``None``, derived from *x*.
        y_title: Y-axis title. If ``None``, derived from *y*.

    Returns:
        A ``plotly.graph_objects.Figure``.
    """
    import plotly.express as px
    import plotly.graph_objects as go

    if style is None:
        style = default_plot_style()

    df = _prepare_plot_data(result)

    # Ensure x and y columns exist
    _validate_plot_columns(df, x=x, y=y, colour=colour, facet=facet)

    # Convert to pandas for plotly
    pdf = df.to_pandas()

    # Ensure y is numeric
    pdf[y] = _to_numeric(pdf[y])

    # Determine facets
    facet_row, facet_col = _parse_facets(facet)

    if group is None:
        group = colour

    fig = go.Figure()

    if ribbon and y_min and y_max:
        # Add ribbon traces
        pdf[y_min] = _to_numeric(pdf[y_min])
        pdf[y_max] = _to_numeric(pdf[y_max])

        groups = _get_groups(pdf, colour)
        for i, (name, grp) in enumerate(groups):
            color = style.color_palette[i % len(style.color_palette)]
            grp = grp.sort_values(x)
            fig.add_trace(
                go.Scatter(
                    x=grp[x],
                    y=grp[y_max],
                    mode="lines",
                    line=dict(width=0),
                    showlegend=False,
                    name=str(name),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=grp[x],
                    y=grp[y_min],
                    mode="lines",
                    line=dict(width=0),
                    fill="tonexty",
                    fillcolor=_with_opacity(color, 0.2),
                    showlegend=False,
                    name=str(name),
                )
            )

    if point and not line:
        fig_scatter = px.scatter(
            pdf,
            x=x,
            y=y,
            color=colour,
            facet_row=facet_row,
            facet_col=facet_col,
            color_discrete_sequence=style.color_palette,
            title=title,
        )
        for trace in fig_scatter.data:
            fig.add_trace(trace)
        if fig_scatter.layout.annotations:
            fig.update_layout(annotations=fig_scatter.layout.annotations)
    elif line and point:
        fig_line = px.line(
            pdf,
            x=x,
            y=y,
            color=colour,
            facet_row=facet_row,
            facet_col=facet_col,
            color_discrete_sequence=style.color_palette,
            title=title,
            markers=True,
        )
        for trace in fig_line.data:
            fig.add_trace(trace)
        if fig_line.layout.annotations:
            fig.update_layout(annotations=fig_line.layout.annotations)
    elif line:
        fig_line = px.line(
            pdf,
            x=x,
            y=y,
            color=colour,
            facet_row=facet_row,
            facet_col=facet_col,
            color_discrete_sequence=style.color_palette,
            title=title,
        )
        for trace in fig_line.data:
            fig.add_trace(trace)
        if fig_line.layout.annotations:
            fig.update_layout(annotations=fig_line.layout.annotations)

    # Apply layout styling
    fig = _apply_plot_style(
        fig,
        style,
        title=title,
        x_title=x_title or customise_text(x),
        y_title=y_title or customise_text(y),
    )

    return fig


# ── bar_plot ──────────────────────────────────────────────────────────────


def bar_plot(
    result: SummarisedResult | pl.DataFrame,
    *,
    x: str,
    y: str,
    position: Literal["dodge", "stack"] = "dodge",
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: PlotStyle | None = None,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
) -> Any:
    """Create a bar chart from results.

    Args:
        result: A :class:`SummarisedResult` (auto-tidied) or a
            :class:`~polars.DataFrame`.
        x: Column name for x-axis (categories).
        y: Column name or estimate for y-axis (bar height).
        position: Bar positioning: ``"dodge"`` (side-by-side) or
            ``"stack"`` (stacked).
        facet: Column(s) for faceting.
        colour: Column for colour grouping.
        style: Plot style configuration.
        title: Plot title.
        x_title: X-axis title.
        y_title: Y-axis title.

    Returns:
        A ``plotly.graph_objects.Figure``.
    """
    import plotly.express as px

    if style is None:
        style = default_plot_style()

    df = _prepare_plot_data(result)
    _validate_plot_columns(df, x=x, y=y, colour=colour, facet=facet)

    pdf = df.to_pandas()
    pdf[y] = _to_numeric(pdf[y])

    facet_row, facet_col = _parse_facets(facet)
    barmode = "group" if position == "dodge" else "stack"

    fig = px.bar(
        pdf,
        x=x,
        y=y,
        color=colour,
        facet_row=facet_row,
        facet_col=facet_col,
        color_discrete_sequence=style.color_palette,
        title=title,
        barmode=barmode,
    )

    fig = _apply_plot_style(
        fig,
        style,
        title=title,
        x_title=x_title or customise_text(x),
        y_title=y_title or customise_text(y),
    )

    return fig


# ── box_plot ──────────────────────────────────────────────────────────────


def box_plot(
    result: SummarisedResult | pl.DataFrame,
    *,
    x: str,
    lower: str = "q25",
    middle: str = "median",
    upper: str = "q75",
    y_min: str = "min",
    y_max: str = "max",
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: PlotStyle | None = None,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
) -> Any:
    """Create a box plot from pre-computed summary statistics.

    Unlike traditional box plots, this renders from pre-computed
    quantiles (as found in summarised results), not raw data.

    Args:
        result: A :class:`SummarisedResult` (auto-tidied) or a
            :class:`~polars.DataFrame` with columns for each statistic.
        x: Column for x-axis categories.
        lower: Column/estimate name for Q1 (25th percentile).
        middle: Column/estimate name for median.
        upper: Column/estimate name for Q3 (75th percentile).
        y_min: Column/estimate name for whisker minimum.
        y_max: Column/estimate name for whisker maximum.
        facet: Column(s) for faceting.
        colour: Column for colour grouping.
        style: Plot style configuration.
        title: Plot title.
        x_title: X-axis title.
        y_title: Y-axis title.

    Returns:
        A ``plotly.graph_objects.Figure``.
    """
    import plotly.graph_objects as go

    if style is None:
        style = default_plot_style()

    df = _prepare_plot_data(result)

    # For box plots, we need the statistic columns to exist
    required = [x, lower, middle, upper, y_min, y_max]
    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"Box plot requires columns: {missing} not found. Available: {df.columns}"
        raise ValueError(msg)

    pdf = df.to_pandas()
    for col in [lower, middle, upper, y_min, y_max]:
        pdf[col] = _to_numeric(pdf[col])

    groups = _get_groups(pdf, colour)
    fig = go.Figure()

    for i, (name, grp) in enumerate(groups):
        color = style.color_palette[i % len(style.color_palette)]
        fig.add_trace(
            go.Box(
                x=grp[x],
                lowerfence=grp[y_min],
                q1=grp[lower],
                median=grp[middle],
                q3=grp[upper],
                upperfence=grp[y_max],
                name=str(name),
                marker_color=color,
                fillcolor=_with_opacity(color, 0.3),
            )
        )

    fig = _apply_plot_style(
        fig,
        style,
        title=title,
        x_title=x_title or customise_text(x),
        y_title=y_title or "Value",
    )

    return fig


# ── Internal helpers ──────────────────────────────────────────────────────


def _prepare_plot_data(result: SummarisedResult | pl.DataFrame) -> pl.DataFrame:
    """Tidy a SummarisedResult for plotting, or return DataFrame as-is."""
    if isinstance(result, SummarisedResult):
        # Tidy: add settings, split name-level pairs, pivot estimates
        df = result.tidy()
        # Also pivot estimates wide
        pivoted = result.pivot_estimates()
        # Merge tidy columns (settings, split pairs) with pivoted estimates
        # Use tidy directly since it includes everything except pivoted estimates
        # Actually, for plotting, we need estimate columns as separate columns
        # Let's use split_all + pivot_estimates
        df = result.add_settings()
        df = SummarisedResult._split_name_level(df, "group_name", "group_level")
        df = SummarisedResult._split_name_level(df, "strata_name", "strata_level")
        df = SummarisedResult._split_name_level(df, "additional_name", "additional_level")

        # Pivot estimate_name/estimate_value to wide
        if "estimate_name" in df.columns and "estimate_value" in df.columns:
            key_cols = [
                c
                for c in df.columns
                if c not in ("estimate_name", "estimate_type", "estimate_value")
            ]
            try:
                df = df.pivot(
                    on="estimate_name",
                    index=key_cols,
                    values="estimate_value",
                    aggregate_function="first",
                )
            except Exception:
                pass  # If pivot fails, keep as-is

        return df
    return result


def _validate_plot_columns(
    df: pl.DataFrame,
    *,
    x: str,
    y: str,
    colour: str | None = None,
    facet: str | list[str] | None = None,
) -> None:
    """Validate that required columns exist in the DataFrame."""
    required = [x, y]
    if colour:
        required.append(colour)
    if facet:
        if isinstance(facet, str):
            required.append(facet)
        else:
            required.extend(facet)

    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"Plot requires columns {missing} but they are not in the data. Available columns: {df.columns}"
        raise ValueError(msg)


def _parse_facets(facet: str | list[str] | None) -> tuple[str | None, str | None]:
    """Parse facet spec into (facet_row, facet_col)."""
    if facet is None:
        return None, None
    if isinstance(facet, str):
        return None, facet
    if len(facet) == 1:
        return None, facet[0]
    return facet[0], facet[1]


def _get_groups(pdf: Any, colour: str | None) -> list[tuple[str, Any]]:
    """Split a pandas DataFrame by colour groups."""
    if colour is None:
        return [("all", pdf)]
    return [(name, group) for name, group in pdf.groupby(colour, sort=True)]


def _to_numeric(series: Any) -> Any:
    """Coerce a pandas Series to numeric, ignoring errors."""
    import pandas as pd

    return pd.to_numeric(series, errors="coerce")


def _with_opacity(hex_color: str, opacity: float) -> str:
    """Convert a hex colour to an rgba string with given opacity."""
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 6:
        r, g, b = int(hex_color[:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        return f"rgba({r},{g},{b},{opacity})"
    return hex_color


def _apply_plot_style(
    fig: Any,
    style: PlotStyle,
    *,
    title: str | None,
    x_title: str,
    y_title: str,
) -> Any:
    """Apply PlotStyle to a plotly Figure."""
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        font=dict(
            family=style.font_family,
            size=style.font_size,
            color=style.text_color,
        ),
        plot_bgcolor=style.background_color,
        paper_bgcolor=style.background_color,
        showlegend=style.show_legend,
    )

    # Grid styling
    fig.update_xaxes(
        gridcolor=style.grid_color,
        showgrid=True,
    )
    fig.update_yaxes(
        gridcolor=style.grid_color,
        showgrid=True,
    )

    return fig
