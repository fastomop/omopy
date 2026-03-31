"""Plot rendering for pregnancy results.

Produces Plotly figures from a :class:`SummarisedResult` containing
pregnancy episode statistics.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = ["plot_pregnancies"]


def plot_pregnancies(
    result: SummarisedResult,
    *,
    type: str = "outcome",
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot pregnancy results.

    Parameters
    ----------
    result
        A :class:`SummarisedResult` from :func:`summarise_pregnancies`.
    type
        ``"outcome"`` for outcome category bar chart,
        ``"source"`` for source distribution,
        ``"duration"`` for episode duration,
        ``"precision"`` for precision distribution.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure | polars.DataFrame
        A Plotly figure, or a tidy DataFrame if Plotly is unavailable.
    """
    tidy = result.tidy()

    # Filter to relevant variable
    variable_map = {
        "outcome": "Outcome category",
        "source": "Episode source",
        "duration": "Episode duration (days)",
        "precision": "Start date precision",
    }

    var_name = variable_map.get(type, "Outcome category")
    filtered = tidy.filter(pl.col("variable_name") == var_name)

    if filtered.height == 0:
        # Return the full tidy frame if no matching variable
        return tidy

    try:
        from omopy.vis import bar_plot

        # For count-based variables, produce bar charts
        if type in ("outcome", "source", "precision"):
            count_data = filtered.filter(pl.col("estimate_name") == "count")
            if count_data.height == 0:
                return filtered

            return bar_plot(
                result,
                x="variable_level",
                y="count",
                facet=facet,
                colour=colour,
                style=style,
            )
        elif type == "duration":
            return bar_plot(
                result,
                x="estimate_name",
                y="estimate_value",
                facet=facet,
                colour=colour,
                style=style,
            )
        else:
            return bar_plot(
                result,
                x="variable_level",
                y="count",
                facet=facet,
                colour=colour,
                style=style,
            )
    except ImportError, Exception:
        # Fallback: return tidy DataFrame
        return filtered
