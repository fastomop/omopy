"""Table rendering functions for cohort characteristics results.

Each function takes a :class:`SummarisedResult` produced by one of the
``summarise_*`` functions and renders it as a formatted table via
``omopy.vis``.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "table_characteristics",
    "table_cohort_count",
    "table_cohort_attrition",
    "table_cohort_timing",
    "table_cohort_overlap",
    "table_top_large_scale_characteristics",
    "table_large_scale_characteristics",
    "available_table_columns",
]


def _settings_columns(result: SummarisedResult) -> list[str]:
    """Return settings column names (excluding the standard 4)."""
    standard = {"result_id", "result_type", "package_name", "package_version"}
    return [c for c in result.settings.columns if c not in standard]


def _strata_columns(result: SummarisedResult) -> list[str]:
    """Return unique strata column names from the result."""
    from omopy.generics._types import NAME_LEVEL_SEP, OVERALL

    names = result.data["strata_name"].unique().to_list()
    cols: list[str] = []
    for name in names:
        if name == OVERALL:
            continue
        for part in name.split(NAME_LEVEL_SEP):
            part = part.strip()
            if part and part != OVERALL and part not in cols:
                cols.append(part)
    return cols


def _additional_columns(result: SummarisedResult) -> list[str]:
    """Return unique additional column names from the result."""
    from omopy.generics._types import NAME_LEVEL_SEP, OVERALL

    names = result.data["additional_name"].unique().to_list()
    cols: list[str] = []
    for name in names:
        if name == OVERALL:
            continue
        for part in name.split(NAME_LEVEL_SEP):
            part = part.strip()
            if part and part != OVERALL and part not in cols:
                cols.append(part)
    return cols


# ===================================================================
# table_characteristics
# ===================================================================


def table_characteristics(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render a characteristics table.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_characteristics"``.
    type
        Output format: ``"gt"`` for great_tables, ``"polars"`` for DataFrame.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name", "cohort_name"]``.
    group_column
        Columns for row grouping.
    hide
        Columns to hide.
    style
        A ``TableStyle`` for styling.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = []
    if hide is None:
        hide = _additional_columns(result) + _settings_columns(result)

    # Filter out density estimates
    data = result.data.filter(~pl.col("estimate_name").str.starts_with("density_"))
    result = SummarisedResult(data, settings=result.settings)

    return vis_omop_table(
        result,
        estimate_name={
            "N (%)": "<count> (<percentage>%)",
            "N": "<count>",
            "Median [Q25 - Q75]": "<median> [<q25> - <q75>]",
            "Mean (SD)": "<mean> (<sd>)",
            "Range": "<min> to <max>",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_cohort_count
# ===================================================================


def table_cohort_count(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render a cohort count table.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_count"``.
    type, header, group_column, hide, style
        See :func:`table_characteristics`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    if header is None:
        header = ["cohort_name"]
    if group_column is None:
        group_column = []
    if hide is None:
        hide = ["variable_level"] + _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={"N": "<count>"},
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_cohort_attrition
# ===================================================================


def table_cohort_attrition(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render a cohort attrition table.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_attrition"``.
    type, header, group_column, hide, style
        See :func:`table_characteristics`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    if header is None:
        header = ["variable_name"]
    if group_column is None:
        group_column = ["cdm_name", "cohort_name"]
    if hide is None:
        hide = ["variable_level", "reason_id", "estimate_name"] + _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={"N": "<count>"},
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_cohort_timing
# ===================================================================


def table_cohort_timing(
    result: SummarisedResult,
    *,
    time_scale: Literal["days", "years"] = "days",
    unique_combinations: bool = True,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render a cohort timing table.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_timing"``.
    time_scale
        ``"days"`` or ``"years"`` (divides by 365.25).
    unique_combinations
        If ``True``, show only unique cohort pairs (A→B but not B→A).
    type, header, group_column, hide, style
        See :func:`table_characteristics`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table
    from omopy.generics._types import NAME_LEVEL_SEP

    # Filter density estimates
    data = result.data.filter(~pl.col("estimate_name").str.starts_with("density_"))

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
                except (ValueError, TypeError):
                    pass
            rows.append(r)
        data = pl.DataFrame(rows)

    # Optionally filter to unique combinations
    if unique_combinations:
        data = _filter_unique_pairs(data)

    result = SummarisedResult(data, settings=result.settings)

    if header is None:
        header = _strata_columns(result) or ["strata_name"]
    if group_column is None:
        group_column = ["cdm_name"]
    if hide is None:
        hide = ["variable_level"] + _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={
            "N": "<count>",
            "Mean (SD)": "<mean> (<sd>)",
            "Median [Q25 - Q75]": "<median> [<q25> - <q75>]",
            "Range": "<min> to <max>",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_cohort_overlap
# ===================================================================


def table_cohort_overlap(
    result: SummarisedResult,
    *,
    unique_combinations: bool = True,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render a cohort overlap table.

    Parameters
    ----------
    result
        A SummarisedResult with ``result_type="summarise_cohort_overlap"``.
    unique_combinations
        If ``True``, show only unique cohort pairs.
    type, header, group_column, hide, style
        See :func:`table_characteristics`.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    data = result.data
    if unique_combinations:
        data = _filter_unique_pairs(data)
    result = SummarisedResult(data, settings=result.settings)

    if header is None:
        header = ["variable_name"]
    if group_column is None:
        group_column = ["cdm_name"]
    if hide is None:
        hide = ["variable_level"] + _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={"N (%)": "<count> (<percentage>%)"},
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_top_large_scale_characteristics
# ===================================================================


def table_top_large_scale_characteristics(
    result: SummarisedResult,
    *,
    top_concepts: int = 10,
    type: Literal["gt", "polars"] | None = None,
    style: Any | None = None,
) -> Any:
    """Render the top N most frequent concepts as a table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_large_scale_characteristics"``.
    top_concepts
        Number of top concepts per group to display.
    type
        Output format.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_table

    # Tidy and extract percentages
    tidy = result.tidy()

    # Filter to percentage estimates for ranking
    pct_data = tidy.filter(pl.col("estimate_name") == "percentage")

    if len(pct_data) == 0:
        return pl.DataFrame()

    # Sort by percentage (descending) and take top N
    pct_data = pct_data.with_columns(
        pl.col("estimate_value").cast(pl.Float64, strict=False).alias("_pct")
    )
    pct_data = pct_data.sort("_pct", descending=True).head(top_concepts)

    # Build display DataFrame
    display_cols = [c for c in pct_data.columns if c not in (
        "estimate_name", "estimate_type", "estimate_value", "_pct",
        "result_id", "result_type", "package_name", "package_version",
    )]
    display = pct_data.select(display_cols + ["_pct"]).rename({"_pct": "Frequency (%)"})

    return vis_table(
        display,
        type=type,
        style=style,
    )


# ===================================================================
# table_large_scale_characteristics
# ===================================================================


def table_large_scale_characteristics(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render the full large-scale characteristics table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_large_scale_characteristics"``.
    type
        Output format.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_table

    # Tidy and filter to percentage
    tidy = result.tidy()
    pct_data = tidy.filter(pl.col("estimate_name") == "percentage")

    if len(pct_data) == 0:
        return pl.DataFrame()

    # Select display columns
    exclude = {
        "estimate_name", "estimate_type", "result_id",
        "result_type", "package_name", "package_version",
    }
    if hide:
        exclude.update(hide)

    display_cols = [c for c in pct_data.columns if c not in exclude]
    display = pct_data.select(display_cols)

    return vis_table(
        display,
        type=type,
        style=style,
    )


# ===================================================================
# available_table_columns
# ===================================================================


def available_table_columns(result: SummarisedResult) -> list[str]:
    """Return columns available for table customisation.

    Parameters
    ----------
    result
        Any characteristics SummarisedResult.

    Returns
    -------
    list[str]
        Column names from ``cdm_name``, group, strata, additional,
        and settings columns.
    """
    cols = ["cdm_name"]

    # Group columns
    from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
    for name in result.data["group_name"].unique().to_list():
        if name != OVERALL:
            for part in name.split(NAME_LEVEL_SEP):
                part = part.strip()
                if part and part != OVERALL and part not in cols:
                    cols.append(part)

    cols.extend(_strata_columns(result))
    cols.extend(_additional_columns(result))
    cols.extend(_settings_columns(result))

    return cols


# ===================================================================
# Helpers
# ===================================================================


def _filter_unique_pairs(data: pl.DataFrame) -> pl.DataFrame:
    """Filter to unique cohort pairs (alphabetically ordered)."""
    from omopy.generics._types import NAME_LEVEL_SEP

    if "group_level" not in data.columns:
        return data

    # Parse group_level into ref/comp and keep only ordered pairs
    def _keep_row(group_level: str) -> bool:
        parts = group_level.split(NAME_LEVEL_SEP)
        if len(parts) != 2:
            return True
        return parts[0].strip() <= parts[1].strip()

    mask = data["group_level"].map_elements(_keep_row, return_dtype=pl.Boolean)
    return data.filter(mask)
