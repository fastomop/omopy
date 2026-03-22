"""Table rendering functions for drug utilisation results.

Each function takes a :class:`SummarisedResult` produced by one of the
``summarise_*`` functions in this module and renders it as a formatted
table via ``omopy.vis``.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "table_drug_utilisation",
    "table_indication",
    "table_treatment",
    "table_drug_restart",
    "table_dose_coverage",
    "table_proportion_of_patients_covered",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings_columns(result: SummarisedResult) -> list[str]:
    """Return settings column names (excluding the standard 4)."""
    standard = {"result_id", "result_type", "package_name", "package_version"}
    return [c for c in result.settings.columns if c not in standard]


def _additional_columns(result: SummarisedResult) -> list[str]:
    """Return unique additional column names from the result."""
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


def _filter_result_type(
    result: SummarisedResult, result_type: str,
) -> SummarisedResult:
    """Filter a SummarisedResult to rows matching the given result_type."""
    settings = result.settings
    matching_ids = settings.filter(
        pl.col("result_type") == result_type
    )["result_id"].to_list()

    if not matching_ids:
        return result

    data = result.data.filter(pl.col("result_id").is_in(matching_ids))
    filtered_settings = settings.filter(pl.col("result_id").is_in(matching_ids))
    return SummarisedResult(data, settings=filtered_settings)


# ===================================================================
# table_drug_utilisation
# ===================================================================


def table_drug_utilisation(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render a drug utilisation summary table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_drug_utilisation"``.
    type
        Output format: ``"gt"`` for great_tables, ``"polars"`` for
        DataFrame.
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

    result = _filter_result_type(result, "summarise_drug_utilisation")

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = []
    if hide is None:
        hide = _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={
            "N": "<count>",
            "Mean (SD)": "<mean> (<sd>)",
            "Median [Q25 - Q75]": "<median> [<q25> - <q75>]",
            "Range": "<min> to <max>",
            "Missing N (%)": "<count_missing> (<percentage_missing>%)",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_indication
# ===================================================================


def table_indication(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render an indication summary table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_indication"``.
    type
        Output format.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name", "cohort_name"]``.
    group_column
        Row grouping columns.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, "summarise_indication")

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = ["variable_name"]
    if hide is None:
        hide = _settings_columns(result)

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
# table_treatment
# ===================================================================


def table_treatment(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render a treatment summary table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_treatment"``.
    type
        Output format.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name", "cohort_name"]``.
    group_column
        Row grouping columns.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, "summarise_treatment")

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = ["variable_name"]
    if hide is None:
        hide = _settings_columns(result)

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
# table_drug_restart
# ===================================================================


def table_drug_restart(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render a drug restart summary table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_drug_restart"``.
    type
        Output format.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name", "cohort_name"]``.
    group_column
        Row grouping columns.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, "summarise_drug_restart")

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = ["variable_name"]
    if hide is None:
        hide = _settings_columns(result)

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
# table_dose_coverage
# ===================================================================


def table_dose_coverage(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render a dose coverage summary table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_dose_coverage"``.
    type
        Output format.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name"]``.
    group_column
        Row grouping columns. Defaults to ``["ingredient_name"]``.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(result, "summarise_dose_coverage")

    if header is None:
        header = ["cdm_name"]
    if group_column is None:
        group_column = ["ingredient_name"]
    if hide is None:
        hide = _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={
            "N": "<count>",
            "Mean (SD)": "<mean> (<sd>)",
            "Median [Q25 - Q75]": "<median> [<q25> - <q75>]",
            "Range": "<min> to <max>",
            "Missing N (%)": "<count_missing> (<percentage_missing>%)",
            "N (%)": "<count> (<percentage>%)",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )


# ===================================================================
# table_proportion_of_patients_covered
# ===================================================================


def table_proportion_of_patients_covered(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Render a proportion of patients covered (PPC) table.

    Parameters
    ----------
    result
        A SummarisedResult with
        ``result_type="summarise_proportion_of_patients_covered"``.
    type
        Output format.
    header
        Columns to pivot into header. Defaults to
        ``["cdm_name", "cohort_name"]``.
    group_column
        Row grouping columns.
    hide
        Columns to hide.
    style
        Table style.

    Returns
    -------
    great_tables.GT or polars.DataFrame
    """
    from omopy.vis import vis_omop_table

    result = _filter_result_type(
        result, "summarise_proportion_of_patients_covered"
    )

    if header is None:
        header = ["cdm_name", "cohort_name"]
    if group_column is None:
        group_column = []
    if hide is None:
        hide = _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={
            "PPC [95% CI]": "<ppc> [<ppc_lower> - <ppc_upper>]",
            "N covered / N at risk": "<outcome_count> / <denominator_count>",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
        rename={"CDM name": "cdm_name"},
    )
