"""Plot rendering for survival results.

Implements ``plot_survival()`` and ``available_survival_grouping()``.

Creates Kaplan-Meier / cumulative incidence plots using plotly,
optionally with confidence interval ribbons and risk tables.

This is the Python equivalent of R's ``plotSurvival()`` and
``availableSurvivalGrouping()`` from the CohortSurvival package.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "available_survival_grouping",
    "plot_survival",
]


def available_survival_grouping(
    result: SummarisedResult,
    *,
    varying: bool = False,
) -> list[str]:
    """List columns available for faceting or colouring survival plots.

    Parameters
    ----------
    result
        A SummarisedResult from a survival estimation function.
    varying
        If ``True``, only return columns with more than one unique value.

    Returns
    -------
    list[str]
        Column names available for grouping.
    """
    available: list[str] = []

    settings = result.settings
    data = result.data

    # Check settings columns
    settings_cols = [
        c for c in settings.columns
        if c not in ("result_id", "result_type", "package_name", "package_version")
    ]
    for col in settings_cols:
        vals = settings[col].unique()
        if not varying or len(vals) > 1:
            available.append(col)

    # Check group columns
    group_names = data["group_name"].unique().to_list()
    for gn in group_names:
        if gn and gn != OVERALL:
            for name in gn.split(NAME_LEVEL_SEP):
                name = name.strip()
                if name not in available:
                    if not varying:
                        available.append(name)
                    else:
                        # Check if there are multiple levels
                        levels = data.filter(pl.col("group_name") == gn)["group_level"].unique()
                        if len(levels) > 1:
                            available.append(name)

    # Check strata columns
    strata_names = data["strata_name"].unique().to_list()
    for sn in strata_names:
        if sn and sn != OVERALL and sn != "reason":
            for name in sn.split(NAME_LEVEL_SEP):
                name = name.strip()
                if name not in available:
                    if not varying:
                        available.append(name)
                    else:
                        levels = data.filter(pl.col("strata_name") == sn)["strata_level"].unique()
                        if len(levels) > 1:
                            available.append(name)

    return available


def plot_survival(
    result: SummarisedResult,
    *,
    ribbon: bool = True,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    cumulative_failure: bool = False,
    risk_table: bool = False,
    risk_interval: int = 30,
    log_log: bool = False,
    time_scale: str = "days",
    style: dict[str, Any] | None = None,
) -> Any:
    """Create a survival curve plot.

    Generates a Kaplan-Meier survival curve or cumulative incidence/failure
    plot from a SummarisedResult.

    Parameters
    ----------
    result
        SummarisedResult from a survival estimation function.
    ribbon
        Show 95% confidence interval as a shaded ribbon.
    facet
        Column(s) to facet by.
    colour
        Column to colour by.
    cumulative_failure
        If ``True``, plot 1 - S(t) (cumulative failure) instead of S(t).
    risk_table
        If ``True``, add a risk table below the plot.
    risk_interval
        Interval for risk table display (in days).
    log_log
        If ``True``, use log(-log(S(t))) y-axis scale.
    time_scale
        Time axis scale: ``"days"``, ``"months"``, or ``"years"``.
    style
        Additional plotly style overrides.

    Returns
    -------
    plotly.graph_objects.Figure
        A plotly figure with the survival plot.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    from omopy.survival._result import as_survival_result

    surv_result = as_survival_result(result)
    estimates = surv_result["estimates"]

    if len(estimates) == 0:
        fig = go.Figure()
        fig.update_layout(title="No survival data available")
        return fig

    time_divisor = _time_divisor(time_scale)
    time_label = f"Time ({time_scale})"
    y_label = "Cumulative failure" if cumulative_failure else "Survival probability"

    # Determine grouping
    group_col = colour or "variable_level"
    groups = _get_plot_groups(estimates, group_col)

    # Create figure with optional risk table subplot
    if risk_table:
        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.75, 0.25],
            vertical_spacing=0.08,
            shared_xaxes=True,
        )
    else:
        fig = go.Figure()

    colours = _default_colours()

    for i, (group_label, group_df) in enumerate(groups):
        colour_hex = colours[i % len(colours)]

        # Get time and estimate columns
        if "additional_level" in group_df.columns:
            try:
                time_vals = group_df["additional_level"].cast(pl.Float64) / time_divisor
            except Exception:
                time_vals = pl.Series(range(len(group_df))) / time_divisor
        else:
            time_vals = pl.Series(range(len(group_df))) / time_divisor

        if "estimate" in group_df.columns:
            est_vals = group_df["estimate"].cast(pl.Float64)
        else:
            continue

        if cumulative_failure:
            est_vals = 1.0 - est_vals

        if log_log:
            import numpy as np
            est_np = est_vals.to_numpy()
            # log(-log(S(t)))
            with np.errstate(divide="ignore", invalid="ignore"):
                est_vals = pl.Series(np.log(-np.log(np.clip(est_np, 1e-10, 1 - 1e-10))))
            y_label = "log(-log(S(t)))"

        time_list = time_vals.to_list()
        est_list = est_vals.to_list()

        # Main line
        trace_kwargs: dict[str, Any] = {
            "x": time_list,
            "y": est_list,
            "name": str(group_label),
            "line": {"color": colour_hex, "shape": "hv"},
            "mode": "lines",
        }

        if risk_table:
            fig.add_trace(go.Scatter(**trace_kwargs), row=1, col=1)
        else:
            fig.add_trace(go.Scatter(**trace_kwargs))

        # Ribbon (confidence interval)
        if ribbon and "estimate_95CI_lower" in group_df.columns:
            try:
                lower = group_df["estimate_95CI_lower"].cast(pl.Float64)
                upper = group_df["estimate_95CI_upper"].cast(pl.Float64)
                if cumulative_failure:
                    lower, upper = 1.0 - upper, 1.0 - lower

                ribbon_trace = go.Scatter(
                    x=time_list + time_list[::-1],
                    y=upper.to_list() + lower.reverse().to_list(),
                    fill="toself",
                    fillcolor=colour_hex.replace(")", ", 0.15)").replace("rgb", "rgba")
                    if "rgb" in colour_hex
                    else f"rgba({_hex_to_rgb(colour_hex)}, 0.15)",
                    line={"color": "rgba(0,0,0,0)"},
                    showlegend=False,
                    name=f"{group_label} 95% CI",
                )
                if risk_table:
                    fig.add_trace(ribbon_trace, row=1, col=1)
                else:
                    fig.add_trace(ribbon_trace)
            except Exception:
                pass

    # Risk table subplot
    if risk_table:
        events_data = surv_result.get("events", pl.DataFrame())
        if len(events_data) > 0 and "n_risk_count" in events_data.columns:
            try:
                if "additional_level" in events_data.columns:
                    rt_times = events_data["additional_level"].str.split(" &&& ").list.first().cast(pl.Float64) / time_divisor
                else:
                    rt_times = pl.Series(range(len(events_data))) / time_divisor

                n_risk = events_data["n_risk_count"].cast(pl.Int64)
                fig.add_trace(
                    go.Scatter(
                        x=rt_times.to_list(),
                        y=[0] * len(rt_times),
                        text=n_risk.cast(pl.Utf8).to_list(),
                        mode="text",
                        textposition="middle center",
                        showlegend=False,
                    ),
                    row=2, col=1,
                )
                fig.update_yaxes(
                    visible=False, row=2, col=1,
                )
                fig.update_xaxes(title_text=time_label, row=2, col=1)
            except Exception:
                pass

    # Layout
    layout_kwargs: dict[str, Any] = {
        "title": "Survival Analysis",
        "template": "plotly_white",
        "legend": {"title": group_col.replace("_", " ").title()},
    }

    if not risk_table:
        layout_kwargs["xaxis_title"] = time_label
        layout_kwargs["yaxis_title"] = y_label
    else:
        fig.update_xaxes(title_text=time_label, row=1, col=1)
        fig.update_yaxes(title_text=y_label, row=1, col=1)

    if not cumulative_failure and not log_log:
        if not risk_table:
            layout_kwargs["yaxis_range"] = [0, 1.05]
        else:
            fig.update_yaxes(range=[0, 1.05], row=1, col=1)

    if style:
        layout_kwargs.update(style)

    fig.update_layout(**layout_kwargs)
    return fig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_plot_groups(
    estimates: pl.DataFrame,
    group_col: str,
) -> list[tuple[str, pl.DataFrame]]:
    """Split estimates DataFrame by group column."""
    if group_col in estimates.columns:
        unique_vals = estimates[group_col].unique().sort().to_list()
        return [
            (str(val), estimates.filter(pl.col(group_col) == val))
            for val in unique_vals
        ]
    else:
        return [("overall", estimates)]


def _time_divisor(time_scale: str) -> float:
    """Get divisor for converting days to time_scale."""
    if time_scale == "days":
        return 1.0
    elif time_scale == "months":
        return 30.4375
    elif time_scale == "years":
        return 365.25
    return 1.0


def _default_colours() -> list[str]:
    """Default colour palette for survival plots."""
    return [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf",
    ]


def _hex_to_rgb(hex_color: str) -> str:
    """Convert hex colour to RGB string for rgba()."""
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"{r}, {g}, {b}"
