"""Result conversion functions for incidence and prevalence.

Implements ``as_incidence_result()`` and ``as_prevalence_result()``
which convert SummarisedResult objects into tidy DataFrames.
"""

from __future__ import annotations

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "as_incidence_result",
    "as_prevalence_result",
]


def as_incidence_result(
    result: SummarisedResult,
    *,
    metadata: bool = False,
) -> pl.DataFrame:
    """Convert a summarised result to a tidy incidence DataFrame.

    Pivots the long-form SummarisedResult into a wide DataFrame with
    one row per interval and columns for each estimate.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_incidence`.
    metadata
        If ``True``, include settings metadata columns.

    Returns
    -------
    pl.DataFrame
        Wide-form incidence results.
    """
    return _to_tidy(result, variable_name="incidence", metadata=metadata)


def as_prevalence_result(
    result: SummarisedResult,
    *,
    metadata: bool = False,
) -> pl.DataFrame:
    """Convert a summarised result to a tidy prevalence DataFrame.

    Pivots the long-form SummarisedResult into a wide DataFrame with
    one row per interval and columns for each estimate.

    Parameters
    ----------
    result
        A SummarisedResult from :func:`estimate_point_prevalence` or
        :func:`estimate_period_prevalence`.
    metadata
        If ``True``, include settings metadata columns.

    Returns
    -------
    pl.DataFrame
        Wide-form prevalence results.
    """
    return _to_tidy(
        result,
        variable_name=("point_prevalence", "period_prevalence"),
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _to_tidy(
    result: SummarisedResult,
    variable_name: str | tuple[str, ...],
    metadata: bool,
) -> pl.DataFrame:
    """Convert SummarisedResult to wide-form tidy DataFrame."""
    data = result.data

    if isinstance(variable_name, str):
        variable_name = (variable_name,)

    # Filter to the relevant rows
    data = data.filter(pl.col("variable_name").is_in(list(variable_name)))

    if data.is_empty():
        return data

    # Parse group columns
    data = _split_name_level(data, "group_name", "group_level")
    data = _split_name_level(data, "strata_name", "strata_level")
    data = _split_name_level(data, "additional_name", "additional_level")

    # Pivot estimate columns to wide
    index_cols = [
        c
        for c in data.columns
        if c not in ("estimate_name", "estimate_type", "estimate_value")
    ]

    wide = data.pivot(
        on="estimate_name",
        index=index_cols,
        values="estimate_value",
    )

    # Drop the raw name/level columns that were split
    drop_cols = [
        "group_name",
        "group_level",
        "strata_name",
        "strata_level",
        "additional_name",
        "additional_level",
    ]
    wide = wide.drop([c for c in drop_cols if c in wide.columns])

    # Cast numeric columns
    for col in wide.columns:
        if col in ("n_persons", "n_events", "n_cases", "person_days"):
            wide = wide.with_columns(pl.col(col).cast(pl.Int64, strict=False))
        elif col in (
            "person_years",
            "incidence_100000_pys",
            "incidence_100000_pys_95ci_lower",
            "incidence_100000_pys_95ci_upper",
            "prevalence",
            "prevalence_95ci_lower",
            "prevalence_95ci_upper",
        ):
            wide = wide.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    if metadata:
        settings = result.settings
        wide = wide.join(settings, on="result_id", how="left")

    return wide


def _split_name_level(df: pl.DataFrame, name_col: str, level_col: str) -> pl.DataFrame:
    """Split a compound name/level column pair into individual columns.

    E.g., ``group_name = "a &&& b"``, ``group_level = "x &&& y"``
    becomes columns ``a = "x"`` and ``b = "y"``.
    """
    if name_col not in df.columns or level_col not in df.columns:
        return df

    # Get unique name patterns
    name_patterns = df.select(name_col).unique().to_series().to_list()

    for pattern in name_patterns:
        if pattern == "overall" or pattern is None:
            continue
        names = pattern.split(NAME_LEVEL_SEP)
        for i, n in enumerate(names):
            n = n.strip()
            if n and n != "overall" and n not in df.columns:
                # Extract the i-th component from level_col
                df = df.with_columns(
                    pl.when(pl.col(name_col) == pattern)
                    .then(pl.col(level_col).str.split(NAME_LEVEL_SEP).list.get(i))
                    .otherwise(pl.lit(None))
                    .alias(n)
                )

    return df
