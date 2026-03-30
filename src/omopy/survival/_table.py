"""Table rendering for survival results.

Implements ``table_survival()``, ``table_survival_events()``,
``table_survival_attrition()``, and ``options_table_survival()``.

These functions format SummarisedResult data into publication-ready
tables using ``omopy.vis.vis_omop_table()``.

This is the Python equivalent of R's ``tableSurvival()``,
``tableSurvivalEvents()``, ``tableSurvivalAttrition()``, and
``optionsTableSurvival()`` from the CohortSurvival package.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "options_table_survival",
    "table_survival",
    "table_survival_attrition",
    "table_survival_events",
]


def options_table_survival() -> dict[str, Any]:
    """Return default options for ``table_survival()``.

    Returns
    -------
    dict
        Dictionary of default option values.
    """
    return {
        "header": "estimate",
        "group_column": ["target_cohort"],
        "hide": ["result_id", "estimate_type", "cdm_name"],
        "rename": {
            "variable_level": "Outcome",
            "strata_level": "Strata",
        },
        "estimates": {
            "median_survival": "Median survival: {median_survival}",
            "restricted_mean_survival": "RMST: {restricted_mean_survival}",
        },
    }


def table_survival(
    result: SummarisedResult,
    *,
    times: list[int] | None = None,
    time_scale: str = "days",
    header: str | list[str] = "estimate",
    estimates: list[str] | None = None,
    type: str = "gt",
    **kwargs: Any,
) -> Any:
    """Create a summary table of survival results.

    Shows median survival, restricted mean survival, and optionally
    survival estimates at specific time points.

    Parameters
    ----------
    result
        SummarisedResult from a survival estimation function.
    times
        Optional list of time points (in days) to include estimates for.
    time_scale
        Scale for display: ``"days"``, ``"months"``, or ``"years"``.
    header
        Column(s) to use as header.
    estimates
        Which summary estimates to include. Default: median + RMST.
    type
        Output type (``"gt"`` for great-tables).

    Returns
    -------
    great_tables.GT or pl.DataFrame
        Rendered table.
    """
    from omopy.survival._result import as_survival_result

    surv_result = as_survival_result(result)
    summary = surv_result["summary"]

    if len(summary) == 0:
        return summary

    # Apply time scale conversion
    time_divisor = _time_divisor(time_scale)

    # Build display table from summary data
    display_cols = ["group_level", "strata_level", "variable_level"]
    available = [c for c in display_cols if c in summary.columns]

    # Select estimate columns
    if estimates is None:
        estimates = ["median_survival", "restricted_mean_survival"]

    est_cols = [c for c in estimates if c in summary.columns]
    if not est_cols:
        est_cols = [
            c
            for c in summary.columns
            if c not in display_cols
            and c
            not in (
                "result_id",
                "cdm_name",
                "estimate_type",
                "additional_name",
                "additional_level",
                "group_name",
                "strata_name",
                "variable_name",
            )
        ]

    out = summary.select(*available, *est_cols)

    # Convert numeric columns by time scale
    if time_divisor != 1.0:
        for col in est_cols:
            if "survival" in col or "mean" in col:
                try:
                    out = out.with_columns(
                        pl.col(col).cast(pl.Float64).truediv(time_divisor).alias(col)
                    )
                except Exception:
                    pass

    # Add time-point estimates if requested
    if times is not None:
        estimates_wide = surv_result["estimates"]
        if len(estimates_wide) > 0:
            for t in times:
                t_str = str(t)
                # Filter estimates at this time point
                if "additional_level" in estimates_wide.columns:
                    at_time = estimates_wide.filter(pl.col("additional_level") == t_str)
                    if len(at_time) > 0 and "estimate" in at_time.columns:
                        label = (
                            f"S(t={t})" if time_scale == "days" else f"S(t={t / time_divisor:.1f})"
                        )
                        # Join with out
                        merge_cols = [c for c in available if c in at_time.columns]
                        if merge_cols:
                            at_time = at_time.select(*merge_cols, pl.col("estimate").alias(label))
                            out = out.join(at_time, on=merge_cols, how="left")

    # Rename columns
    rename_map = {
        "group_level": "Target cohort",
        "strata_level": "Strata",
        "variable_level": "Outcome",
    }
    for old, new in rename_map.items():
        if old in out.columns:
            out = out.rename({old: new})

    # Filter out 'overall' strata label if it's uninformative
    if "Strata" in out.columns:
        if out["Strata"].n_unique() == 1 and out["Strata"][0] == OVERALL:
            out = out.drop("Strata")

    if type == "gt":
        try:
            from omopy.vis._table import vis_table

            return vis_table(out)
        except ImportError:
            return out

    return out


def table_survival_events(
    result: SummarisedResult,
    *,
    event_gap: int | None = None,
    header: str | list[str] = "estimate",
    type: str = "gt",
    **kwargs: Any,
) -> Any:
    """Create a risk table showing events per time interval.

    Parameters
    ----------
    result
        SummarisedResult from a survival estimation function.
    event_gap
        Override the event gap interval. ``None`` uses the original.
    header
        Column(s) to use as header.
    type
        Output type.

    Returns
    -------
    great_tables.GT or pl.DataFrame
    """
    from omopy.survival._result import as_survival_result

    surv_result = as_survival_result(result)
    events = surv_result["events"]

    if len(events) == 0:
        return events

    # Extract display columns
    display_cols = [
        c
        for c in events.columns
        if c
        not in (
            "result_id",
            "cdm_name",
            "estimate_type",
            "group_name",
            "strata_name",
            "variable_name",
            "additional_name",
        )
    ]

    out = events.select([c for c in display_cols if c in events.columns])

    # Parse the additional_level to extract time
    if "additional_level" in out.columns:
        # Split "time &&& eventgap" format
        out = out.with_columns(
            pl.col("additional_level").str.split(" &&& ").list.first().alias("Time (days)")
        ).drop("additional_level")

    # Rename columns
    rename_map = {
        "group_level": "Target cohort",
        "strata_level": "Strata",
        "variable_level": "Outcome",
        "n_risk_count": "N at risk",
        "n_events_count": "N events",
        "n_censor_count": "N censored",
    }
    for old, new in rename_map.items():
        if old in out.columns:
            out = out.rename({old: new})

    if "Strata" in out.columns:
        if out["Strata"].n_unique() == 1 and out["Strata"][0] == OVERALL:
            out = out.drop("Strata")

    if type == "gt":
        try:
            from omopy.vis._table import vis_table

            return vis_table(out)
        except ImportError:
            return out

    return out


def table_survival_attrition(
    result: SummarisedResult,
    *,
    type: str = "gt",
    **kwargs: Any,
) -> Any:
    """Create an attrition table.

    Parameters
    ----------
    result
        SummarisedResult from a survival estimation function.
    type
        Output type.

    Returns
    -------
    great_tables.GT or pl.DataFrame
    """
    from omopy.survival._result import as_survival_result

    surv_result = as_survival_result(result)
    attrition = surv_result["attrition"]

    if len(attrition) == 0:
        return attrition

    # Build display
    display_cols = [
        c
        for c in attrition.columns
        if c
        not in (
            "result_id",
            "cdm_name",
            "estimate_type",
            "group_name",
            "strata_name",
            "variable_name",
            "additional_name",
        )
    ]

    out = attrition.select([c for c in display_cols if c in attrition.columns])

    rename_map = {
        "group_level": "Target cohort",
        "strata_level": "Reason",
        "variable_level": "Outcome",
        "number_records": "N records",
        "excluded_records": "N excluded",
        "additional_level": "Step",
    }
    for old, new in rename_map.items():
        if old in out.columns:
            out = out.rename({old: new})

    if type == "gt":
        try:
            from omopy.vis._table import vis_table

            return vis_table(out)
        except ImportError:
            return out

    return out


def _time_divisor(time_scale: str) -> float:
    """Get divisor for converting days to time_scale."""
    if time_scale == "days":
        return 1.0
    elif time_scale == "months":
        return 30.4375  # average days per month
    elif time_scale == "years":
        return 365.25
    else:
        return 1.0
