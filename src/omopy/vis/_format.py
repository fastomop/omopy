"""Formatting functions for summarised results.

This module provides the format pipeline:

1. :func:`format_estimate_value` — numeric formatting (decimals, big marks)
2. :func:`format_estimate_name` — combine estimates into display strings
3. :func:`format_header` — pivot columns into multi-level column headers
4. :func:`format_min_cell_count` — replace suppressed values with ``<N``
"""

from __future__ import annotations

import re
from typing import Literal

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.summarised_result import SUMMARISED_RESULT_COLUMNS, SummarisedResult

__all__ = [
    "format_estimate_name",
    "format_estimate_value",
    "format_header",
    "format_min_cell_count",
]

# Default decimal places per estimate type
DEFAULT_DECIMALS: dict[str, int] = {
    "integer": 0,
    "numeric": 2,
    "percentage": 1,
    "proportion": 3,
    "date": 0,
    "character": 0,
    "logical": 0,
}

HEADER_DELIM = "\n"

# ── format_estimate_value ─────────────────────────────────────────────────


def format_estimate_value(
    result: SummarisedResult,
    *,
    decimals: dict[str, int] | None = None,
    decimal_mark: str = ".",
    big_mark: str = ",",
) -> SummarisedResult:
    """Format numeric precision of ``estimate_value`` based on ``estimate_type``.

    For each row, rounds the value to the number of decimal places
    specified by the row's ``estimate_type`` and applies decimal/big marks.

    Args:
        result: A :class:`SummarisedResult`.
        decimals: Mapping of estimate_type -> number of decimals.
            Defaults to ``{integer: 0, numeric: 2, percentage: 1, proportion: 3}``.
        decimal_mark: Character to use as decimal separator (default ``"."``).
        big_mark: Thousands separator (default ``","``). Use ``""`` for none.

    Returns:
        A new :class:`SummarisedResult` with formatted ``estimate_value``.
    """
    dec = {**DEFAULT_DECIMALS, **(decimals or {})}

    df = result.data
    types = df["estimate_type"].to_list()
    values = df["estimate_value"].to_list()

    formatted: list[str] = []
    for val, etype in zip(values, types, strict=True):
        formatted.append(_format_single_value(val, etype, dec, decimal_mark, big_mark))

    new_df = df.with_columns(pl.Series("estimate_value", formatted))
    return SummarisedResult(new_df, settings=result.settings)


def _format_single_value(
    val: str | None,
    etype: str | None,
    decimals: dict[str, int],
    decimal_mark: str,
    big_mark: str,
) -> str:
    """Format a single estimate value string."""
    if val is None or val == "" or val == "-":
        return val or ""

    # Non-numeric types: return as-is
    if etype in ("character", "logical", "date", None):
        return val

    try:
        num = float(val)
    except ValueError, TypeError:
        return val

    n_dec = decimals.get(etype or "numeric", 2)

    # Round
    if n_dec == 0:
        int_val = round(num)
        formatted = _apply_big_mark(str(int_val), big_mark)
    else:
        rounded = round(num, n_dec)
        # Format with fixed decimal places
        formatted = f"{rounded:.{n_dec}f}"
        if big_mark:
            int_part, dec_part = formatted.split(".")
            int_part = _apply_big_mark(int_part, big_mark)
            formatted = f"{int_part}.{dec_part}"

    # Apply decimal mark
    if decimal_mark != ".":
        formatted = formatted.replace(".", decimal_mark)

    return formatted


def _apply_big_mark(int_str: str, big_mark: str) -> str:
    """Insert thousands separator into an integer string."""
    if not big_mark or len(int_str) <= 3:
        return int_str

    negative = int_str.startswith("-")
    digits = int_str.lstrip("-")
    # Insert separator every 3 digits from the right
    parts: list[str] = []
    while len(digits) > 3:
        parts.append(digits[-3:])
        digits = digits[:-3]
    parts.append(digits)
    result = big_mark.join(reversed(parts))
    return f"-{result}" if negative else result


# ── format_estimate_name ──────────────────────────────────────────────────


def format_estimate_name(
    result: SummarisedResult,
    *,
    estimate_name: dict[str, str] | None = None,
    keep_not_formatted: bool = True,
    use_format_order: bool = True,
) -> SummarisedResult:
    """Combine/rename estimate values using template patterns.

    Each key in *estimate_name* is a display label; each value is a pattern
    containing ``<estimate_name>`` placeholders, e.g.::

        {"N (%)": "<count> (<percentage>%)"}

    Rows whose ``estimate_name`` appears in a pattern are merged; the
    resulting row gets the display label as its ``estimate_name`` and the
    interpolated string as ``estimate_value``.

    Args:
        result: A :class:`SummarisedResult`.
        estimate_name: Mapping of ``display_label -> pattern``.
            Patterns use ``<name>`` to reference estimate values by their
            ``estimate_name``.  If ``None``, returns *result* unchanged.
        keep_not_formatted: Whether to keep rows whose ``estimate_name``
            was not matched by any pattern.
        use_format_order: If ``True``, output rows follow the order of
            *estimate_name* keys; otherwise, data order is preserved.

    Returns:
        A new :class:`SummarisedResult` with combined estimates.
    """
    if estimate_name is None or len(estimate_name) == 0:
        return result

    df = result.data

    # Key columns for grouping (everything except the 3 estimate columns)
    key_cols = [
        c
        for c in SUMMARISED_RESULT_COLUMNS
        if c not in ("estimate_name", "estimate_type", "estimate_value")
    ]

    # Build a lookup: for each key-group, map estimate_name -> estimate_value
    groups = df.group_by(key_cols, maintain_order=True)

    result_rows: list[dict[str, str | int]] = []
    used_estimate_names: set[str] = set()

    for group_keys, group_df in groups:
        # Build estimate lookup for this group
        est_lookup: dict[str, str] = {}
        est_type_lookup: dict[str, str] = {}
        for row in group_df.iter_rows(named=True):
            est_lookup[row["estimate_name"]] = row["estimate_value"]
            est_type_lookup[row["estimate_name"]] = row["estimate_type"]

        # Build key dict
        if isinstance(group_keys, tuple):
            key_dict = dict(zip(key_cols, group_keys, strict=True))
        else:
            key_dict = {key_cols[0]: group_keys}

        # Apply format patterns
        for display_label, pattern in estimate_name.items():
            # Find all <name> placeholders in the pattern
            placeholders = re.findall(r"<(\w+)>", pattern)
            if not placeholders:
                continue

            # Check if all required estimates exist
            if not all(p in est_lookup for p in placeholders):
                continue

            # Interpolate
            formatted_value = pattern
            for ph in placeholders:
                formatted_value = formatted_value.replace(f"<{ph}>", est_lookup[ph])
                used_estimate_names.add(ph)

            row = {
                **key_dict,
                "estimate_name": display_label,
                "estimate_type": "character",
                "estimate_value": formatted_value,
            }
            result_rows.append(row)

        # Keep unformatted rows if requested
        if keep_not_formatted:
            for row in group_df.iter_rows(named=True):
                if row["estimate_name"] not in used_estimate_names:
                    result_rows.append(row)

    if not result_rows:
        # Return empty result with same schema
        return SummarisedResult(
            df.clear(),
            settings=result.settings,
        )

    new_df = pl.DataFrame(result_rows, schema=df.schema)

    if use_format_order:
        # Sort by the order of estimate_name keys
        order = {label: i for i, label in enumerate(estimate_name)}
        # Add unformatted at the end
        max_order = len(order)
        new_df = new_df.with_columns(
            pl.col("estimate_name").replace_strict(order, default=max_order).alias("_sort_order")
        )
        new_df = new_df.sort("_sort_order").drop("_sort_order")

    return SummarisedResult(new_df, settings=result.settings)


# ── format_header ─────────────────────────────────────────────────────────


def format_header(
    result: pl.DataFrame | SummarisedResult,
    header: list[str],
    *,
    delim: str = HEADER_DELIM,
    include_header_name: bool = True,
    include_header_key: bool = True,
) -> pl.DataFrame:
    """Pivot columns into multi-level column headers.

    Takes a DataFrame (or SummarisedResult's data) and pivots specified
    columns so their unique values become part of the column names,
    enabling multi-level headers in formatted tables.

    The column names in the output encode header metadata using *delim* as
    a separator. For example, pivoting ``cohort_name`` with values
    ``["cohort_1", "cohort_2"]`` produces columns like::

        "[header]cohort_name\\n[header_level]cohort_1"

    Args:
        result: The data to pivot. If a :class:`SummarisedResult`, uses
            its :attr:`data` attribute. Should be a "tidy" DataFrame
            (i.e., after ``split_all()`` + ``pivot_estimates()``).
        header: Column names to pivot into headers.  May also contain
            label strings (not actual column names) that will be inserted
            as header group labels.
        delim: Delimiter between header levels (default newline).
        include_header_name: Include the column name in the header.
        include_header_key: Include ``[header]``/``[header_level]`` keys.

    Returns:
        A pivoted :class:`~polars.DataFrame` with encoded column names.
    """
    if isinstance(result, SummarisedResult):
        df = result.data
    else:
        df = result

    if not header:
        return df

    # Separate actual column names from label strings
    actual_cols = [h for h in header if h in df.columns]
    label_strings = [h for h in header if h not in df.columns]

    if not actual_cols:
        return df

    # Non-header columns become the index
    index_cols = [c for c in df.columns if c not in actual_cols and c != "estimate_value"]

    # If estimate_value is not present, nothing to pivot
    if "estimate_value" not in df.columns:
        return df

    # Build pivot: for each combination of header column values,
    # create a new column with the estimate_value
    for col in actual_cols:
        unique_vals = sorted(df[col].drop_nulls().unique().to_list())
        remaining_cols = [c for c in df.columns if c != col and c != "estimate_value"]

        pivoted_parts: list[pl.DataFrame] = []
        for val in unique_vals:
            subset = df.filter(pl.col(col) == val).drop(col)
            # Rename estimate_value to encoded column name
            encoded_name = _encode_header_name(
                col, str(val), label_strings, delim, include_header_name, include_header_key
            )
            subset = subset.rename({"estimate_value": encoded_name})
            pivoted_parts.append(subset)

        if not pivoted_parts:
            return df

        # Join all pivot parts on the remaining columns
        result_df = pivoted_parts[0]
        join_cols = [c for c in remaining_cols if c in result_df.columns]
        for part in pivoted_parts[1:]:
            result_df = result_df.join(part, on=join_cols, how="outer_coalesce")

        df = result_df

    return df


def _encode_header_name(
    col_name: str,
    value: str,
    label_strings: list[str],
    delim: str,
    include_name: bool,
    include_key: bool,
) -> str:
    """Build an encoded multi-level column header string."""
    parts: list[str] = []
    if label_strings:
        for label in label_strings:
            if include_key:
                parts.append(f"[header]{label}")
            else:
                parts.append(label)
    if include_name:
        if include_key:
            parts.append(f"[header_name]{col_name}")
        else:
            parts.append(col_name)
    if include_key:
        parts.append(f"[header_level]{value}")
    else:
        parts.append(value)
    return delim.join(parts)


def parse_header_keys(col_name: str, *, delim: str = HEADER_DELIM) -> dict[str, str]:
    """Parse encoded header keys from a column name.

    Returns a dict with possible keys: ``header``, ``header_name``, ``header_level``.
    """
    parts = col_name.split(delim)
    result: dict[str, str] = {}
    for part in parts:
        for key in ("header", "header_name", "header_level"):
            prefix = f"[{key}]"
            if part.startswith(prefix):
                result[key] = part[len(prefix) :]
                break
    return result


# ── format_min_cell_count ─────────────────────────────────────────────────


def format_min_cell_count(
    result: SummarisedResult,
) -> SummarisedResult:
    """Replace suppressed count values with ``<N`` display strings.

    Reads ``min_cell_count`` from the result's settings. Rows where
    ``estimate_value`` is ``"-"`` (the suppression sentinel) are replaced
    with ``"<N"`` where *N* is the minimum cell count.

    Args:
        result: A :class:`SummarisedResult` (typically after
            :meth:`SummarisedResult.suppress`).

    Returns:
        A new :class:`SummarisedResult` with formatted suppression labels.
    """
    settings = result.settings
    df = result.data

    # Get min_cell_count from settings (default 5)
    min_counts: dict[int, int] = {}
    if "min_cell_count" in settings.columns:
        for row in settings.iter_rows(named=True):
            try:
                min_counts[row["result_id"]] = int(row["min_cell_count"])
            except ValueError, TypeError:
                min_counts[row["result_id"]] = 5
    else:
        for rid in df["result_id"].unique().to_list():
            min_counts[rid] = 5

    # Replace "-" with "<N"
    result_ids = df["result_id"].to_list()
    values = df["estimate_value"].to_list()
    new_values = [
        f"<{min_counts.get(rid, 5)}" if val == "-" else val
        for val, rid in zip(values, result_ids, strict=True)
    ]

    new_df = df.with_columns(pl.Series("estimate_value", new_values))
    return SummarisedResult(new_df, settings=settings)


# ── tidy helper ───────────────────────────────────────────────────────────


def tidy_result(result: SummarisedResult) -> pl.DataFrame:
    """Convert a :class:`SummarisedResult` to a tidy DataFrame.

    Equivalent to R's ``tidy(<summarised_result>)``:
    1. Add settings columns
    2. Split all name-level pair columns
    3. Keep estimate columns for downstream pivoting

    Args:
        result: The summarised result to tidy.

    Returns:
        A wide :class:`~polars.DataFrame` with individual columns for
        group, strata, and additional variables.
    """
    return result.tidy()


def tidy_columns(result: SummarisedResult) -> list[str]:
    """Return the column names available after tidying.

    Args:
        result: The summarised result.

    Returns:
        List of column names that :func:`tidy_result` would produce.
    """
    return result.tidy().columns
