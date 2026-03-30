"""Table rendering functions for incidence and prevalence results.

Each function takes a :class:`SummarisedResult` from an estimation
function and renders it via ``omopy.vis.vis_omop_table()``.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "options_table_incidence",
    "options_table_prevalence",
    "table_incidence",
    "table_incidence_attrition",
    "table_prevalence",
    "table_prevalence_attrition",
]


# ---------------------------------------------------------------------------
# Options (default configurations)
# ---------------------------------------------------------------------------


def options_table_incidence() -> dict[str, Any]:
    """Return default table options for incidence results.

    Returns
    -------
    dict
        Default options for :func:`table_incidence`.
    """
    return {
        "header": ["cdm_name"],
        "group_column": ["denominator_cohort_name", "outcome_cohort_name"],
        "estimate_name": {
            "N": "<n_persons>",
            "Person-years": "<person_years>",
            "Events": "<n_events>",
            "IR [95% CI]": "<incidence_100000_pys> [<incidence_100000_pys_95ci_lower> - <incidence_100000_pys_95ci_upper>]",
        },
        "hide": ["result_id", "estimate_type"],
        "rename": {
            "CDM name": "cdm_name",
            "Denominator": "denominator_cohort_name",
            "Outcome": "outcome_cohort_name",
        },
    }


def options_table_prevalence() -> dict[str, Any]:
    """Return default table options for prevalence results.

    Returns
    -------
    dict
        Default options for :func:`table_prevalence`.
    """
    return {
        "header": ["cdm_name"],
        "group_column": ["denominator_cohort_name", "outcome_cohort_name"],
        "estimate_name": {
            "N": "<n_persons>",
            "Cases": "<n_cases>",
            "Prevalence [95% CI]": "<prevalence> [<prevalence_95ci_lower> - <prevalence_95ci_upper>]",
        },
        "hide": ["result_id", "estimate_type"],
        "rename": {
            "CDM name": "cdm_name",
            "Denominator": "denominator_cohort_name",
            "Outcome": "outcome_cohort_name",
        },
    }


# ---------------------------------------------------------------------------
# Table functions
# ---------------------------------------------------------------------------


def table_incidence(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    settings_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    """Render an incidence results table.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    type
        ``"gt"`` for great_tables, ``"polars"`` for DataFrame.
    header
        Columns to pivot into header.
    group_column
        Row grouping columns.
    settings_column
        Settings columns to include.
    hide
        Columns to hide.
    style
        Table style configuration.
    options
        Override options from :func:`options_table_incidence`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    opts = options_table_incidence()
    if options:
        opts.update(options)

    # Filter to incidence results
    result = _filter_result_type(result, "incidence")

    return vis_omop_table(
        result,
        estimate_name=opts.get("estimate_name"),
        header=header or opts.get("header"),
        group_column=group_column or opts.get("group_column"),
        settings_columns=settings_column,
        hide=hide or opts.get("hide"),
        type=type,
        style=style,
        rename=opts.get("rename"),
    )


def table_prevalence(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    settings_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    """Render a prevalence results table.

    Parameters
    ----------
    result
        A SummarisedResult from a prevalence estimation function.
    type
        ``"gt"`` for great_tables, ``"polars"`` for DataFrame.
    header
        Columns to pivot into header.
    group_column
        Row grouping columns.
    settings_column
        Settings columns to include.
    hide
        Columns to hide.
    style
        Table style configuration.
    options
        Override options from :func:`options_table_prevalence`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    opts = options_table_prevalence()
    if options:
        opts.update(options)

    result = _filter_result_type(result, ("point_prevalence", "period_prevalence"))

    return vis_omop_table(
        result,
        estimate_name=opts.get("estimate_name"),
        header=header or opts.get("header"),
        group_column=group_column or opts.get("group_column"),
        settings_columns=settings_column,
        hide=hide or opts.get("hide"),
        type=type,
        style=style,
        rename=opts.get("rename"),
    )


def table_incidence_attrition(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    settings_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render an attrition table for incidence analyses.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    type, header, group_column, settings_column, hide, style
        Table rendering options.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, "incidence")
    result = _filter_attrition_rows(result)

    return vis_omop_table(
        result,
        header=header or ["cdm_name"],
        group_column=group_column or [],
        settings_columns=settings_column,
        hide=hide,
        type=type,
        style=style,
    )


def table_prevalence_attrition(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    settings_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render an attrition table for prevalence analyses.

    Parameters
    ----------
    result
        A SummarisedResult from a prevalence estimation function.
    type, header, group_column, settings_column, hide, style
        Table rendering options.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, ("point_prevalence", "period_prevalence"))
    result = _filter_attrition_rows(result)

    return vis_omop_table(
        result,
        header=header or ["cdm_name"],
        group_column=group_column or [],
        settings_columns=settings_column,
        hide=hide,
        type=type,
        style=style,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_result_type(
    result: SummarisedResult, result_type: str | tuple[str, ...]
) -> SummarisedResult:
    """Filter a SummarisedResult to rows matching the given result_type(s)."""
    if isinstance(result_type, str):
        result_type = (result_type,)

    settings = result.settings
    matching_ids = settings.filter(pl.col("result_type").is_in(list(result_type)))[
        "result_id"
    ].to_list()

    if not matching_ids:
        return result

    data = result.data.filter(pl.col("result_id").is_in(matching_ids))
    filtered_settings = settings.filter(pl.col("result_id").is_in(matching_ids))
    return SummarisedResult(data, settings=filtered_settings)


def _filter_attrition_rows(result: SummarisedResult) -> SummarisedResult:
    """Filter to attrition-related rows only.

    Attrition data is stored in the denominator cohort's attrition,
    not in the SummarisedResult directly. This is a pass-through for
    now — in practice, attrition tables display from the denominator
    CohortTable metadata rather than the SummarisedResult.
    """
    # For SummarisedResult, we can filter to rows with attrition info
    # if stored, or return the full result if attrition is tracked elsewhere
    return result
