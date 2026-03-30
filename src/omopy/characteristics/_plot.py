"""Plot rendering functions for cohort characteristics results.

Each function takes a :class:`SummarisedResult` produced by one of the
``summarise_*`` functions and renders it as a Plotly figure via
``omopy.vis``.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "plot_characteristics",
    "plot_cohort_count",
    "plot_cohort_attrition",
    "plot_cohort_timing",
    "plot_cohort_overlap",
    "plot_large_scale_characteristics",
    "plot_compared_large_scale_characteristics",
]


# ===================================================================
# plot_characteristics
# ===================================================================


def plot_characteristics(
    result: SummarisedResult,
    *,
    plot_type: Literal["barplot", "scatterplot", "boxplot"] = "barplot",
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot characteristics results.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_characteristics"``.
    plot_type
        ``"barplot"``, ``"scatterplot"``, or ``"boxplot"``.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot, scatter_plot, box_plot

    # Filter out density estimates
    data = result.data.filter(~pl.col("estimate_name").str.starts_with("density_"))
    result = SummarisedResult(data, settings=result.settings)

    if plot_type == "barplot":
        # Determine y from the available estimate_name
        tidy = result.tidy()
        est_names = tidy["estimate_name"].unique().to_list()
        y_col = est_names[0] if len(est_names) == 1 else "count"

        return bar_plot(
            result,
            x="variable_name",
            y=y_col,
            facet=facet,
            colour=colour,
            style=style,
        )
    elif plot_type == "scatterplot":
        tidy = result.tidy()
        est_names = tidy["estimate_name"].unique().to_list()
        y_col = est_names[0] if len(est_names) == 1 else "count"

        return scatter_plot(
            result,
            x="variable_name",
            y=y_col,
            point=True,
            line=False,
            facet=facet,
            colour=colour,
            style=style,
        )
    elif plot_type == "boxplot":
        return box_plot(
            result,
            x="variable_name",
            facet=facet,
            colour=colour,
            style=style,
        )
    else:
        msg = f"Unknown plot_type: {plot_type!r}. Expected 'barplot', 'scatterplot', or 'boxplot'."
        raise ValueError(msg)


# ===================================================================
# plot_cohort_count
# ===================================================================


def plot_cohort_count(
    result: SummarisedResult,
    *,
    x: str | None = None,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot cohort counts as a bar chart.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_count"``.
    x
        Column for x-axis. Defaults to ``"cohort_name"``.
    facet
        Column(s) for faceting. Defaults to ``["cdm_name"]``.
    colour
        Column for colour grouping.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    if x is None:
        x = "cohort_name"
    if facet is None:
        facet = ["cdm_name"]

    return bar_plot(
        result,
        x=x,
        y="count",
        facet=facet,
        colour=colour,
        style=style,
    )


# ===================================================================
# plot_cohort_attrition
# ===================================================================


def plot_cohort_attrition(
    result: SummarisedResult,
    *,
    show: list[str] | None = None,
) -> Any:
    """Render an attrition flowchart as a Plotly figure.

    Unlike the R version which uses DiagrammeR, this renders a
    simplified vertical flowchart using Plotly shapes and annotations.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_cohort_attrition"``.
    show
        Which counts to display: ``["subjects"]``, ``["records"]``,
        or ``["subjects", "records"]`` (default).

    Returns
    -------
    plotly.graph_objects.Figure
        A flowchart figure.
    """
    import plotly.graph_objects as go

    if show is None:
        show = ["subjects", "records"]

    tidy = result.tidy()
    if len(tidy) == 0:
        return go.Figure()

    # Group by cohort_name
    cohort_names = (
        tidy["cohort_name"].unique().to_list() if "cohort_name" in tidy.columns else [""]
    )

    fig = go.Figure()

    for col_idx, cname in enumerate(cohort_names):
        if "cohort_name" in tidy.columns:
            c_data = tidy.filter(pl.col("cohort_name") == cname)
        else:
            c_data = tidy

        # Get unique reasons ordered by reason_id
        if "reason_id" in c_data.columns:
            reasons = c_data.select("reason", "reason_id").unique().sort("reason_id")
        else:
            reasons = c_data.select("reason").unique()

        x_offset = col_idx * 3.0
        y = 0.0

        for _, reason_row in enumerate(reasons.iter_rows(named=True)):
            reason = reason_row["reason"]
            r_data = (
                c_data.filter(pl.col("reason") == reason) if "reason" in c_data.columns else c_data
            )

            # Build label
            parts = [reason]
            for metric in show:
                var_name = f"number_{metric}"
                val_rows = r_data.filter(pl.col("variable_name") == var_name)
                if len(val_rows) > 0:
                    val = val_rows["estimate_value"].to_list()[0]
                    parts.append(f"{metric.capitalize()}: {val}")

                excl_name = f"excluded_{metric}"
                excl_rows = r_data.filter(pl.col("variable_name") == excl_name)
                if len(excl_rows) > 0:
                    val = excl_rows["estimate_value"].to_list()[0]
                    if val != "0":
                        parts.append(f"Excluded {metric}: {val}")

            label = "<br>".join(parts)

            # Add box annotation
            fig.add_annotation(
                x=x_offset,
                y=-y,
                text=label,
                showarrow=False,
                bordercolor="black",
                borderwidth=1,
                borderpad=8,
                bgcolor="lightblue",
                font=dict(size=10),
            )

            # Add arrow to next box
            if y > 0:
                fig.add_annotation(
                    x=x_offset,
                    y=-y + 0.8,
                    ax=x_offset,
                    ay=-y + 0.2,
                    arrowhead=2,
                    arrowsize=1,
                    arrowcolor="black",
                    showarrow=True,
                )

            y += 1.5

    # Layout
    fig.update_layout(
        title=f"Cohort Attrition",
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
        width=400 * len(cohort_names),
        height=max(400, int(y * 100)),
    )

    return fig


# ===================================================================
# plot_cohort_timing
# ===================================================================


def plot_cohort_timing(
    result: SummarisedResult,
    *,
    plot_type: Literal["boxplot", "densityplot"] = "boxplot",
    time_scale: Literal["days", "years"] = "days",
    unique_combinations: bool = True,
    facet: str | list[str] | None = None,
    colour: str | list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Plot cohort timing distributions.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_timing"``.
    plot_type
        ``"boxplot"`` or ``"densityplot"``.
    time_scale
        ``"days"`` or ``"years"`` (divides by 365.25).
    unique_combinations
        If ``True``, show only unique cohort pairs.
    facet
        Column(s) for faceting. Defaults to
        ``["cdm_name", "cohort_name_reference"]``.
    colour
        Column for colour grouping. Defaults to
        ``"cohort_name_comparator"``.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import box_plot, scatter_plot
    from omopy.characteristics._table import _filter_unique_pairs

    if facet is None:
        facet = ["cdm_name", "cohort_name_reference"]
    if colour is None:
        colour = "cohort_name_comparator"

    # Filter to timing variable only
    data = result.data.filter(pl.col("variable_name") == "Days between cohort entries")

    if unique_combinations:
        data = _filter_unique_pairs(data)

    # Optionally convert to years
    if time_scale == "years":
        numeric_ests = {"min", "q25", "median", "q75", "max", "mean", "sd"}
        rows: list[dict] = []
        for row in data.iter_rows(named=True):
            r = dict(row)
            if r["estimate_name"] in numeric_ests and r["estimate_value"] != "NA":
                try:
                    val = float(r["estimate_value"]) / 365.25
                    r["estimate_value"] = f"{val:.2f}"
                except ValueError, TypeError:
                    pass
            rows.append(r)
        data = pl.DataFrame(rows) if rows else data

    result = SummarisedResult(data, settings=result.settings)

    if plot_type == "boxplot":
        return box_plot(
            result,
            x="cohort_name_comparator",
            facet=facet,
            colour=colour,
            style=style,
        )
    elif plot_type == "densityplot":
        return scatter_plot(
            result,
            x="density_x",
            y="density_y",
            line=True,
            point=False,
            facet=facet,
            colour=colour,
            style=style,
        )
    else:
        msg = f"Unknown plot_type: {plot_type!r}"
        raise ValueError(msg)


# ===================================================================
# plot_cohort_overlap
# ===================================================================


def plot_cohort_overlap(
    result: SummarisedResult,
    *,
    unique_combinations: bool = True,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot cohort overlap as a stacked bar chart.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_overlap"``.
    unique_combinations
        If ``True``, show only unique cohort pairs.
    facet
        Column(s) for faceting. Defaults to
        ``["cdm_name", "cohort_name_reference"]``.
    colour
        Column for colour grouping. Defaults to ``"variable_name"``.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot
    from omopy.characteristics._table import _filter_unique_pairs

    if facet is None:
        facet = ["cdm_name", "cohort_name_reference"]
    if colour is None:
        colour = "variable_name"

    # Filter to percentage estimates
    data = result.data.filter(pl.col("estimate_name") == "percentage")

    if unique_combinations:
        data = _filter_unique_pairs(data)

    result = SummarisedResult(data, settings=result.settings)

    return bar_plot(
        result,
        x="cohort_name_comparator",
        y="percentage",
        position="stack",
        facet=facet,
        colour=colour,
        style=style,
    )


# ===================================================================
# plot_large_scale_characteristics
# ===================================================================


def plot_large_scale_characteristics(
    result: SummarisedResult,
    *,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot large-scale characteristics as a scatter plot.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_large_scale_characteristics"``.
    facet
        Column(s) for faceting. Defaults to
        ``["cdm_name", "cohort_name"]``.
    colour
        Column for colour grouping. Defaults to ``"variable_level"``.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import scatter_plot

    if facet is None:
        facet = ["cdm_name", "cohort_name"]
    if colour is None:
        colour = "variable_level"

    # Filter to percentage estimates
    data = result.data.filter(pl.col("estimate_name") == "percentage")
    result = SummarisedResult(data, settings=result.settings)

    return scatter_plot(
        result,
        x="variable_name",
        y="percentage",
        point=True,
        line=False,
        facet=facet,
        colour=colour,
        style=style,
    )


# ===================================================================
# plot_compared_large_scale_characteristics
# ===================================================================


def plot_compared_large_scale_characteristics(
    result: SummarisedResult,
    *,
    colour: str,
    reference: str | None = None,
    facet: str | list[str] | None = None,
    missings: float | None = 0.0,
    style: Any | None = None,
) -> Any:
    """Plot compared large-scale characteristics.

    Shows a scatter plot where x is the reference group's percentage
    and y is each comparison group's percentage, with a diagonal
    reference line.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_large_scale_characteristics"``.
    colour
        **Required.** Column to colour by (e.g. ``"cohort_name"``).
    reference
        Level of ``colour`` to use as reference (x-axis). Defaults to
        the first alphabetical level.
    facet
        Column(s) for faceting.
    missings
        Replace missing percentages with this value. ``None`` = drop.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import scatter_plot
    import plotly.graph_objects as go

    # Tidy and filter to percentage
    tidy = result.tidy()
    pct_data = tidy.filter(pl.col("estimate_name") == "percentage")

    if len(pct_data) == 0:
        return go.Figure()

    # Cast estimate_value to float
    pct_data = pct_data.with_columns(
        pl.col("estimate_value").cast(pl.Float64, strict=False).alias("percentage")
    )

    if colour not in pct_data.columns:
        msg = f"colour column {colour!r} not found in result"
        raise ValueError(msg)

    # Determine reference
    levels = sorted(pct_data[colour].unique().to_list())
    if reference is None:
        reference = levels[0]

    # Split reference vs comparators
    ref_data = pct_data.filter(pl.col(colour) == reference)
    comp_data = pct_data.filter(pl.col(colour) != reference)

    # Build join key (all columns except colour, percentage, estimate_value)
    join_cols = [
        c
        for c in pct_data.columns
        if c not in {colour, "percentage", "estimate_value", "estimate_name", "estimate_type"}
    ]

    # Join reference percentage
    ref_slim = ref_data.select(join_cols + [pl.col("percentage").alias("reference_percentage")])
    merged = comp_data.join(ref_slim, on=join_cols, how="left")

    if missings is not None:
        merged = merged.with_columns(
            pl.col("reference_percentage").fill_null(missings),
            pl.col("percentage").fill_null(missings),
        )
    else:
        merged = merged.drop_nulls(["reference_percentage", "percentage"])

    fig = scatter_plot(
        merged,
        x="reference_percentage",
        y="percentage",
        point=True,
        line=False,
        facet=facet,
        colour=colour,
        style=style,
    )

    # Add diagonal reference line
    fig.add_shape(
        type="line",
        x0=0,
        y0=0,
        x1=100,
        y1=100,
        line=dict(dash="dash", color="gray"),
    )

    return fig
