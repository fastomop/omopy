"""Plot rendering functions for incidence and prevalence results.

Provides line/ribbon plots for incidence and prevalence trends,
bar plots for population summaries, and helper functions for
determining available grouping variables.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "available_incidence_grouping",
    "available_prevalence_grouping",
    "plot_incidence",
    "plot_incidence_population",
    "plot_prevalence",
    "plot_prevalence_population",
]


# ---------------------------------------------------------------------------
# Grouping helpers
# ---------------------------------------------------------------------------


def available_incidence_grouping(
    result: SummarisedResult,
    *,
    varying: bool = False,
) -> list[str]:
    """List variables available for grouping/faceting incidence plots.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    varying
        If ``True``, only return variables with more than one unique value.

    Returns
    -------
    list[str]
        Available grouping variable names.
    """
    return _available_grouping(result, "incidence", varying=varying)


def available_prevalence_grouping(
    result: SummarisedResult,
    *,
    varying: bool = False,
) -> list[str]:
    """List variables available for grouping/faceting prevalence plots.

    Parameters
    ----------
    result
        A SummarisedResult from a prevalence estimation function.
    varying
        If ``True``, only return variables with more than one unique value.

    Returns
    -------
    list[str]
        Available grouping variable names.
    """
    return _available_grouping(result, ("point_prevalence", "period_prevalence"), varying=varying)


def _available_grouping(
    result: SummarisedResult,
    result_type: str | tuple[str, ...],
    *,
    varying: bool,
) -> list[str]:
    """Extract available grouping variables from a SummarisedResult."""
    if isinstance(result_type, str):
        result_type = (result_type,)

    candidates: list[str] = []

    # From settings
    settings = result.settings
    matching = settings.filter(pl.col("result_type").is_in(list(result_type)))
    standard = {"result_id", "result_type", "package_name", "package_version"}
    for c in matching.columns:
        if c not in standard:
            candidates.append(c)

    # From group/strata columns
    data = result.data
    for name_col in ("group_name", "strata_name"):
        if name_col in data.columns:
            for val in data[name_col].unique().to_list():
                if val and val != OVERALL:
                    for part in val.split(NAME_LEVEL_SEP):
                        part = part.strip()
                        if part and part != OVERALL and part not in candidates:
                            candidates.append(part)

    if varying and not matching.is_empty():
        candidates = [
            c for c in candidates if c in matching.columns and matching[c].n_unique() > 1
        ]

    return candidates


# ---------------------------------------------------------------------------
# Line/ribbon plot: incidence
# ---------------------------------------------------------------------------


def plot_incidence(
    result: SummarisedResult,
    *,
    x: str = "variable_level",
    y: str = "incidence_100000_pys",
    line: bool = True,
    point: bool = True,
    ribbon: bool = True,
    y_min: str | None = "incidence_100000_pys_95ci_lower",
    y_max: str | None = "incidence_100000_pys_95ci_upper",
    facet: str | list[str] | None = None,
    colour: str | None = None,
) -> Any:
    """Plot incidence rates as a line plot with confidence ribbons.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    x
        Column for x-axis (default: interval labels).
    y
        Estimate name for y-axis.
    line, point, ribbon
        Display elements.
    y_min, y_max
        Estimate names for CI ribbon bounds.
    facet
        Faceting column(s).
    colour
        Colour grouping column.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import scatter_plot

    # Filter to incidence rows
    data = result.data.filter(pl.col("variable_name") == "incidence")
    result_filtered = SummarisedResult(data, settings=result.settings)

    return scatter_plot(
        result_filtered,
        x=x,
        y=y,
        line=line,
        point=point,
        ribbon=ribbon,
        y_min=y_min if ribbon else None,
        y_max=y_max if ribbon else None,
        facet=facet,
        colour=colour,
        title="Incidence Rate (per 100,000 person-years)",
        y_title="Incidence rate per 100,000 PY",
    )


# ---------------------------------------------------------------------------
# Line/ribbon plot: prevalence
# ---------------------------------------------------------------------------


def plot_prevalence(
    result: SummarisedResult,
    *,
    x: str = "variable_level",
    y: str = "prevalence",
    line: bool = True,
    point: bool = True,
    ribbon: bool = True,
    y_min: str | None = "prevalence_95ci_lower",
    y_max: str | None = "prevalence_95ci_upper",
    facet: str | list[str] | None = None,
    colour: str | None = None,
) -> Any:
    """Plot prevalence proportions as a line plot with confidence ribbons.

    Parameters
    ----------
    result
        A SummarisedResult from a prevalence estimation function.
    x
        Column for x-axis.
    y
        Estimate name for y-axis.
    line, point, ribbon
        Display elements.
    y_min, y_max
        Estimate names for CI ribbon bounds.
    facet
        Faceting column(s).
    colour
        Colour grouping column.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import scatter_plot

    data = result.data.filter(
        pl.col("variable_name").is_in(["point_prevalence", "period_prevalence"])
    )
    result_filtered = SummarisedResult(data, settings=result.settings)

    return scatter_plot(
        result_filtered,
        x=x,
        y=y,
        line=line,
        point=point,
        ribbon=ribbon,
        y_min=y_min if ribbon else None,
        y_max=y_max if ribbon else None,
        facet=facet,
        colour=colour,
        title="Prevalence",
        y_title="Prevalence",
    )


# ---------------------------------------------------------------------------
# Population bar plots
# ---------------------------------------------------------------------------


def plot_incidence_population(
    result: SummarisedResult,
    *,
    x: str = "variable_level",
    y: str = "n_persons",
    facet: str | list[str] | None = None,
    colour: str | None = None,
) -> Any:
    """Bar plot of denominator population counts from incidence results.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    x
        Column for x-axis.
    y
        Estimate name for y-axis.
    facet
        Faceting column(s).
    colour
        Colour grouping column.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    data = result.data.filter(pl.col("variable_name") == "incidence")
    result_filtered = SummarisedResult(data, settings=result.settings)

    return bar_plot(
        result_filtered,
        x=x,
        y=y,
        facet=facet,
        colour=colour,
        title="Incidence Analysis Population",
        y_title="Number of persons",
    )


def plot_prevalence_population(
    result: SummarisedResult,
    *,
    x: str = "variable_level",
    y: str = "n_persons",
    facet: str | list[str] | None = None,
    colour: str | None = None,
) -> Any:
    """Bar plot of denominator population counts from prevalence results.

    Parameters
    ----------
    result
        A SummarisedResult from a prevalence estimation function.
    x
        Column for x-axis.
    y
        Estimate name for y-axis.
    facet
        Faceting column(s).
    colour
        Colour grouping column.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    data = result.data.filter(
        pl.col("variable_name").is_in(["point_prevalence", "period_prevalence"])
    )
    result_filtered = SummarisedResult(data, settings=result.settings)

    return bar_plot(
        result_filtered,
        x=x,
        y=y,
        facet=facet,
        colour=colour,
        title="Prevalence Analysis Population",
        y_title="Number of persons",
    )
