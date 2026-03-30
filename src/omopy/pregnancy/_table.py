"""Table rendering for pregnancy results.

Wraps :func:`omopy.vis.vis_omop_table` to produce a formatted table
from a :class:`SummarisedResult` containing pregnancy episode statistics.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = ["table_pregnancies"]


def table_pregnancies(
    result: SummarisedResult,
    *,
    type: Literal["gt", "polars"] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    hide: list[str] | None = None,
    style: Any | None = None,
    **options: Any,
) -> Any:
    """Render pregnancy results as a formatted table.

    Parameters
    ----------
    result
        A :class:`SummarisedResult` from :func:`summarise_pregnancies`.
    type
        ``"gt"`` for a great_tables table, ``"polars"`` for a raw DataFrame.
    header
        Columns to use as header grouping.
    group_column
        Columns for row grouping.
    hide
        Columns to hide.
    style
        A ``TableStyle`` for customisation.
    **options
        Additional options forwarded to ``vis_omop_table``.

    Returns
    -------
    great_tables.GT | polars.DataFrame
    """
    if type == "polars":
        return result.tidy()

    try:
        from omopy.vis import vis_omop_table

        return vis_omop_table(
            result,
            header=header or ["strata_name", "strata_level"],
            group_column=group_column,
            hide=hide,
            style=style,
            **options,
        )
    except ImportError:
        # Fallback: return tidy DataFrame
        return result.tidy()
