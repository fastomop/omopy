"""Table formatting functions for drug diagnostics.

Thin wrappers around ``omopy.vis.vis_omop_table()`` with domain-specific
defaults for drug exposure diagnostics results.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics import SummarisedResult

__all__ = ["table_drug_diagnostics"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_result_type(
    result: SummarisedResult,
    result_type: str,
) -> SummarisedResult:
    """Filter a SummarisedResult to a specific result_type."""
    matching_ids = result.settings.filter(pl.col("result_type") == result_type)[
        "result_id"
    ].to_list()

    if not matching_ids:
        return result

    data_dtype = result.data["result_id"].dtype
    if data_dtype == pl.Utf8:
        matching_ids = [str(x) for x in matching_ids]

    data = result.data.filter(pl.col("result_id").is_in(matching_ids))
    settings = result.settings.filter(pl.col("result_id").is_in(matching_ids))
    return SummarisedResult(data, settings=settings)


def _settings_columns(result: SummarisedResult) -> list[str]:
    """Get non-required settings column names to hide."""
    required = {"result_id", "result_type", "package_name", "package_version"}
    return [c for c in result.settings.columns if c not in required]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def table_drug_diagnostics(
    result: SummarisedResult,
    *,
    check: str | None = None,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
) -> Any:
    """Format drug diagnostics results as a display-ready table.

    Parameters
    ----------
    result
        A ``SummarisedResult`` from :func:`summarise_drug_diagnostics`.
    check
        Specific check to display (e.g. ``"missing"``, ``"exposure_duration"``).
        If ``None``, all checks are included.
    type
        Output format: ``"gt"`` for ``great_tables.GT``, ``"polars"`` for
        a Polars DataFrame. Default is ``"polars"``.
    header
        Columns to use as multi-level headers.
    group_column
        Columns to use for row grouping.
    hide
        Columns to hide from the output.
    style
        Optional ``TableStyle`` for customisation.

    Returns
    -------
    great_tables.GT | polars.DataFrame
        Formatted table.
    """
    from omopy.vis import vis_omop_table

    if check is not None:
        result = _filter_result_type(result, f"drug_diagnostics_{check}")

    if header is None:
        header = ["cdm_name"]
    if group_column is None:
        group_column = ["variable_name"]
    if hide is None:
        hide = _settings_columns(result)

    return vis_omop_table(
        result,
        estimate_name={
            "N": "<count>",
            "%": "<proportion>%",
        },
        header=header,
        group_column=group_column,
        hide=hide,
        type=type,
        style=style,
    )
