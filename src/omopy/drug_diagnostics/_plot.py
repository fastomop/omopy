"""Plot functions for drug diagnostics.

Provides bar charts for categorical checks (type, route, sig) and
box/violin plots for quantile checks (exposure_duration, days_supply,
quantity, days_between).
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics import SummarisedResult

__all__ = ["plot_drug_diagnostics"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_result_type(
    result: SummarisedResult,
    result_type: str,
) -> SummarisedResult:
    """Filter a SummarisedResult to a specific result_type."""
    matching_ids = result.settings.filter(
        pl.col("result_type") == result_type
    )["result_id"].to_list()

    if not matching_ids:
        return result

    data_dtype = result.data["result_id"].dtype
    if data_dtype == pl.Utf8:
        matching_ids = [str(x) for x in matching_ids]

    data = result.data.filter(pl.col("result_id").is_in(matching_ids))
    settings = result.settings.filter(pl.col("result_id").is_in(matching_ids))
    return SummarisedResult(data, settings=settings)


_CATEGORICAL_CHECKS = {"type", "route", "sig", "source_concept"}
_QUANTILE_CHECKS = {"exposure_duration", "days_supply", "quantity", "days_between"}


def _plot_categorical(
    result: SummarisedResult,
    *,
    check_name: str,
    title: str,
    colour: str | None,
) -> Any:
    """Create a bar chart for a categorical check."""
    import plotly.express as px

    data = result.data.filter(
        (pl.col("variable_name") == check_name)
        & (pl.col("estimate_name") == "count")
    )

    if data.height == 0:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    plot_df = data.select(
        pl.col("group_level").alias("ingredient"),
        pl.col("variable_level").alias("category"),
        pl.col("estimate_value").cast(pl.Float64).alias("count"),
    ).to_pandas()

    fig = px.bar(
        plot_df,
        x="category",
        y="count",
        color="ingredient" if plot_df["ingredient"].nunique() > 1 else None,
        title=title,
        labels={"category": check_name.replace("_", " ").title(), "count": "Count"},
        barmode="group",
    )

    if colour:
        fig.update_traces(marker_color=colour)

    fig.update_layout(xaxis_tickangle=-45)
    return fig


def _plot_quantile(
    result: SummarisedResult,
    *,
    check_name: str,
    title: str,
    colour: str | None,
) -> Any:
    """Create a box plot for a quantile check using precomputed stats."""
    import plotly.graph_objects as go

    data = result.data.filter(pl.col("variable_name") == check_name)

    if data.height == 0:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # Extract stats per ingredient
    fig = go.Figure()

    # Group by ingredient (from group_level)
    ingredients = data.select("group_level").unique().to_series().to_list()

    for ingredient in ingredients:
        ing_data = data.filter(pl.col("group_level") == ingredient)

        stats: dict[str, float | None] = {}
        for row in ing_data.iter_rows(named=True):
            est = row["variable_level"]
            val = row["estimate_value"]
            if val != "NA":
                try:
                    stats[est] = float(val)
                except (ValueError, TypeError):
                    stats[est] = None

        # Extract ingredient name (second part of "id &&& name")
        parts = str(ingredient).split(" &&& ")
        label = parts[-1] if len(parts) > 1 else str(ingredient)

        if any(v is not None for v in stats.values()):
            fig.add_trace(
                go.Box(
                    name=label,
                    q1=[stats.get("q25")],
                    median=[stats.get("median")],
                    q3=[stats.get("q75")],
                    lowerfence=[stats.get("min")],
                    upperfence=[stats.get("max")],
                    mean=[stats.get("mean")],
                    boxpoints=False,
                    marker_color=colour,
                )
            )

    fig.update_layout(
        title=title,
        yaxis_title="Value",
    )

    return fig


def _plot_missing(
    result: SummarisedResult,
    *,
    title: str,
    colour: str | None,
) -> Any:
    """Create a bar chart of missing value proportions."""
    import plotly.express as px

    data = result.data.filter(
        (pl.col("variable_name") == "missing")
        & (pl.col("estimate_name") == "proportion_missing")
    )

    if data.height == 0:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    plot_df = data.select(
        pl.col("group_level").alias("ingredient"),
        pl.col("variable_level").alias("column"),
        pl.col("estimate_value").cast(pl.Float64).alias("proportion_missing"),
    ).to_pandas()

    fig = px.bar(
        plot_df,
        x="column",
        y="proportion_missing",
        color="ingredient" if plot_df["ingredient"].nunique() > 1 else None,
        title=title,
        labels={"column": "Column", "proportion_missing": "Proportion Missing"},
        barmode="group",
    )

    if colour:
        fig.update_traces(marker_color=colour)

    fig.update_layout(xaxis_tickangle=-45, yaxis_range=[0, 1])
    return fig


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plot_drug_diagnostics(
    result: SummarisedResult,
    *,
    check: str = "missing",
    facet: str | None = None,
    colour: str | None = None,
    title: str | None = None,
    style: Any | None = None,
) -> Any:
    """Create a plot for drug diagnostics results.

    Generates bar charts for categorical checks and box plots for
    quantile-based checks.

    Parameters
    ----------
    result
        A ``SummarisedResult`` from :func:`summarise_drug_diagnostics`.
    check
        Which check to plot. One of:
        ``"missing"``, ``"exposure_duration"``, ``"type"``, ``"route"``,
        ``"source_concept"``, ``"sig"``, ``"quantity"``, ``"days_supply"``,
        ``"days_between"``.
    facet
        Column to facet by (currently unused, reserved for future use).
    colour
        Override colour for all bars/boxes.
    title
        Chart title. Defaults to a descriptive title based on the check.
    style
        Optional plot style configuration (reserved for future use).

    Returns
    -------
    plotly.graph_objects.Figure
        Interactive plotly figure.

    Raises
    ------
    ValueError
        If ``check`` is not a valid plottable check name.
    """
    plottable = _CATEGORICAL_CHECKS | _QUANTILE_CHECKS | {"missing", "verbatim_end_date", "dose", "diagnostics_summary"}
    if check not in plottable:
        msg = f"Cannot plot check '{check}'. Plottable checks: {sorted(plottable)}"
        raise ValueError(msg)

    result = _filter_result_type(result, f"drug_diagnostics_{check}")

    default_titles = {
        "missing": "Drug Exposure Missing Values",
        "exposure_duration": "Exposure Duration Distribution",
        "type": "Drug Type Frequencies",
        "route": "Route Frequencies",
        "source_concept": "Source Concept Mapping",
        "days_supply": "Days Supply Distribution",
        "verbatim_end_date": "Verbatim End Date Comparison",
        "dose": "Dose Coverage",
        "sig": "Sig (Instruction) Frequencies",
        "quantity": "Quantity Distribution",
        "days_between": "Days Between Consecutive Records",
        "diagnostics_summary": "Diagnostics Summary",
    }

    if title is None:
        title = default_titles.get(check, f"Drug Diagnostics: {check}")

    if check == "missing":
        return _plot_missing(result, title=title, colour=colour)
    elif check in _CATEGORICAL_CHECKS:
        return _plot_categorical(result, check_name=check, title=title, colour=colour)
    elif check in _QUANTILE_CHECKS:
        return _plot_quantile(result, check_name=check, title=title, colour=colour)
    else:
        # For checks without specialised plots, use categorical as fallback
        return _plot_categorical(result, check_name=check, title=title, colour=colour)
