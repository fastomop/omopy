"""Plot rendering functions for drug utilisation results.

Each function takes a :class:`SummarisedResult` produced by one of the
``summarise_*`` functions and renders it as a Plotly figure via
``omopy.vis``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "plot_drug_restart",
    "plot_drug_utilisation",
    "plot_indication",
    "plot_proportion_of_patients_covered",
    "plot_treatment",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_result_type(
    result: SummarisedResult,
    result_type: str,
) -> SummarisedResult:
    """Filter a SummarisedResult to rows matching the given result_type."""
    settings = result.settings
    matching_ids = settings.filter(pl.col("result_type") == result_type)[
        "result_id"
    ].to_list()

    if not matching_ids:
        return result

    data = result.data.filter(pl.col("result_id").is_in(matching_ids))
    filtered_settings = settings.filter(pl.col("result_id").is_in(matching_ids))
    return SummarisedResult(data, settings=filtered_settings)


# ===================================================================
# plot_drug_utilisation
# ===================================================================


def plot_drug_utilisation(
    result: SummarisedResult,
    *,
    plot_type: str = "boxplot",
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot drug utilisation summary statistics.

    Creates box plots or bar charts of drug utilisation metrics
    (number exposures, days exposed, etc.) across cohorts.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_drug_utilisation"``.
    plot_type
        ``"boxplot"`` or ``"barplot"``.
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
    from omopy.vis import bar_plot, box_plot

    result = _filter_result_type(result, "summarise_drug_utilisation")

    # Filter out count rows — only show metric distributions
    data = result.data.filter(
        ~pl.col("variable_name").is_in(["Number records", "Number subjects"])
    )
    result = SummarisedResult(data, settings=result.settings)

    if plot_type == "boxplot":
        return box_plot(
            result,
            x="variable_name",
            facet=facet or ["cohort_name"],
            colour=colour,
            style=style,
        )
    elif plot_type == "barplot":
        return bar_plot(
            result,
            x="variable_name",
            y="mean",
            facet=facet or ["cohort_name"],
            colour=colour,
            style=style,
            title="Drug Utilisation",
            y_title="Mean value",
        )
    else:
        msg = f"Unknown plot_type: {plot_type!r}. Expected 'boxplot' or 'barplot'."
        raise ValueError(msg)


# ===================================================================
# plot_indication
# ===================================================================


def plot_indication(
    result: SummarisedResult,
    *,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot indication prevalence as a bar chart.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_indication"``.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping. Defaults to
        ``"variable_level"`` (indication name).
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    result = _filter_result_type(result, "summarise_indication")

    # Filter to percentage estimates for plotting
    data = result.data.filter(pl.col("estimate_name") == "percentage")
    result = SummarisedResult(data, settings=result.settings)

    if colour is None:
        colour = "variable_level"

    return bar_plot(
        result,
        x="variable_name",
        y="percentage",
        facet=facet or ["cohort_name"],
        colour=colour,
        style=style,
        title="Indication",
        y_title="Percentage (%)",
    )


# ===================================================================
# plot_treatment
# ===================================================================


def plot_treatment(
    result: SummarisedResult,
    *,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot treatment prevalence as a bar chart.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_treatment"``.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping. Defaults to
        ``"variable_level"`` (treatment name).
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    result = _filter_result_type(result, "summarise_treatment")

    data = result.data.filter(pl.col("estimate_name") == "percentage")
    result = SummarisedResult(data, settings=result.settings)

    if colour is None:
        colour = "variable_level"

    return bar_plot(
        result,
        x="variable_name",
        y="percentage",
        facet=facet or ["cohort_name"],
        colour=colour,
        style=style,
        title="Treatment",
        y_title="Percentage (%)",
    )


# ===================================================================
# plot_drug_restart
# ===================================================================


def plot_drug_restart(
    result: SummarisedResult,
    *,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    style: Any | None = None,
) -> Any:
    """Plot drug restart classification as a stacked bar chart.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_drug_restart"``.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping. Defaults to
        ``"variable_level"`` (restart category).
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import bar_plot

    result = _filter_result_type(result, "summarise_drug_restart")

    data = result.data.filter(pl.col("estimate_name") == "percentage")
    result = SummarisedResult(data, settings=result.settings)

    if colour is None:
        colour = "variable_level"

    return bar_plot(
        result,
        x="variable_name",
        y="percentage",
        position="stack",
        facet=facet or ["cohort_name"],
        colour=colour,
        style=style,
        title="Drug Restart",
        y_title="Percentage (%)",
    )


# ===================================================================
# plot_proportion_of_patients_covered
# ===================================================================


def plot_proportion_of_patients_covered(
    result: SummarisedResult,
    *,
    facet: str | list[str] | None = None,
    colour: str | None = None,
    ribbon: bool = True,
    style: Any | None = None,
) -> Any:
    """Plot proportion of patients covered (PPC) over time.

    Shows the PPC curve with confidence ribbon (if enabled) over the
    follow-up period.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_proportion_of_patients_covered"``.
    facet
        Column(s) for faceting.
    colour
        Column for colour grouping.
    ribbon
        Show 95% confidence interval ribbon.
    style
        A ``PlotStyle`` for styling.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    from omopy.vis import scatter_plot

    result = _filter_result_type(result, "summarise_proportion_of_patients_covered")

    # Filter to ppc estimates
    data = result.data.filter(
        pl.col("estimate_name").is_in(["ppc", "ppc_lower", "ppc_upper"])
    )
    result = SummarisedResult(data, settings=result.settings)

    return scatter_plot(
        result,
        x="time",
        y="ppc",
        line=True,
        point=False,
        ribbon=ribbon,
        y_min="ppc_lower" if ribbon else None,
        y_max="ppc_upper" if ribbon else None,
        facet=facet,
        colour=colour or "cohort_name",
        style=style,
        title="Proportion of Patients Covered",
        y_title="PPC",
        x_title="Days from index date",
    )
