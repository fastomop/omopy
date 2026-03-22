"""Categories — bin numeric variables into named groups.

This is the Python equivalent of R's ``addCategories()`` from
the PatientProfiles package.
"""

from __future__ import annotations

import math
from typing import Any

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_table import CdmTable
from omopy.profiles._demographics import _get_ibis_table

__all__ = ["add_categories"]


def add_categories(
    x: CdmTable,
    variable: str,
    categories: dict[str, list[tuple[float, float]]] | dict[str, dict[str, tuple[float, float]]],
    *,
    missing_category_value: str = "None",
    overlap: bool = False,
) -> CdmTable:
    """Add categorical columns by binning a numeric variable.

    Parameters
    ----------
    x
        Input CDM table.
    variable
        Column name to categorize.
    categories
        Mapping of output column name to category definitions. Each
        category definition is either:

        - A list of ``(lower, upper)`` tuples (auto-labelled).
        - A dict mapping label to ``(lower, upper)`` tuple.

    missing_category_value
        Value for rows where the variable is NULL.
    overlap
        If ``True``, allow overlapping ranges.

    Returns
    -------
    CdmTable
        Input table with new categorical columns.

    Examples
    --------
    >>> add_categories(
    ...     x, "age",
    ...     {"age_group": {"young": (0, 17), "adult": (18, 64), "senior": (65, float("inf"))}},
    ... )
    """
    tbl = _get_ibis_table(x)

    new_cols = {}
    for col_name, cat_def in categories.items():
        if isinstance(cat_def, list):
            # Auto-label: list of (lo, hi) tuples
            labelled = {_auto_label(lo, hi): (lo, hi) for lo, hi in cat_def}
        elif isinstance(cat_def, dict):
            labelled = cat_def
        else:
            msg = f"Category definition must be dict or list, got {type(cat_def)}"
            raise TypeError(msg)

        if not overlap:
            _check_no_overlap(labelled)

        new_cols[col_name] = _build_case_expr(tbl[variable], labelled, missing_category_value)

    tbl = tbl.mutate(**new_cols)
    return x._with_data(tbl)


def _auto_label(lo: float, hi: float) -> str:
    """Generate auto-label for a range."""
    if math.isinf(lo) and lo < 0 and math.isinf(hi) and hi > 0:
        return "any"
    if math.isinf(lo) and lo < 0:
        return f"{int(hi)} or below"
    if math.isinf(hi) and hi > 0:
        return f"{int(lo)} or above"
    return f"{int(lo)} to {int(hi)}"


def _check_no_overlap(ranges: dict[str, tuple[float, float]]) -> None:
    """Check that ranges don't overlap."""
    sorted_ranges = sorted(ranges.values(), key=lambda x: x[0])
    for i in range(1, len(sorted_ranges)):
        prev_hi = sorted_ranges[i - 1][1]
        curr_lo = sorted_ranges[i][0]
        if curr_lo <= prev_hi and not math.isinf(prev_hi):
            msg = f"Overlapping ranges: previous upper {prev_hi} >= current lower {curr_lo}"
            raise ValueError(msg)


def _build_case_expr(
    col: ir.Column,
    ranges: dict[str, tuple[float, float]],
    missing_value: str,
) -> ir.Column:
    """Build a CASE WHEN expression for categorization."""
    cases: list[tuple[ir.BooleanValue, str]] = []

    for label, (lo, hi) in ranges.items():
        if math.isinf(lo) and lo < 0 and math.isinf(hi) and hi > 0:
            cases.append((col.notnull(), label))
        elif math.isinf(lo) and lo < 0:
            cases.append((col <= ibis.literal(hi), label))
        elif math.isinf(hi) and hi > 0:
            cases.append((col >= ibis.literal(lo), label))
        else:
            cases.append((
                (col >= ibis.literal(lo)) & (col <= ibis.literal(hi)),
                label,
            ))

    return ibis.cases(*cases, else_=missing_value)
