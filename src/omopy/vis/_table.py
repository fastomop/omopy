"""Table rendering for summarised results.

Provides high-level :func:`vis_omop_table` (for ``SummarisedResult``) and
lower-level :func:`vis_table` and :func:`format_table` for rendering
DataFrames as formatted tables using ``great_tables``.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl

from omopy.generics.summarised_result import SummarisedResult
from omopy.vis._format import (
    format_estimate_name,
    format_estimate_value,
    format_min_cell_count,
    parse_header_keys,
)
from omopy.vis._style import TableStyle, default_table_style

__all__ = [
    "format_table",
    "vis_omop_table",
    "vis_table",
]

TableType = Literal["gt", "polars"]


# ── vis_omop_table ────────────────────────────────────────────────────────


def vis_omop_table(
    result: SummarisedResult,
    *,
    estimate_name: dict[str, str] | None = None,
    header: list[str] | None = None,
    settings_columns: list[str] | None = None,
    group_column: list[str] | None = None,
    rename: dict[str, str] | None = None,
    type: TableType | None = None,
    hide: list[str] | None = None,
    column_order: list[str] | None = None,
    style: TableStyle | None = None,
    show_min_cell_count: bool = True,
    decimals: dict[str, int] | None = None,
    decimal_mark: str = ".",
    big_mark: str = ",",
    title: str | None = None,
    subtitle: str | None = None,
) -> Any:
    """Create a formatted table from a :class:`SummarisedResult`.

    This is the main high-level entry point, equivalent to R's
    ``visOmopTable()``.  It executes the full format pipeline:

    1. Format estimate values (numeric precision)
    2. Show min cell count markers if suppressed
    3. Format estimate names (combine estimates)
    4. Split name-level pairs and add settings
    5. Apply header pivoting
    6. Render to table

    Args:
        result: A :class:`SummarisedResult`.
        estimate_name: Mapping of ``display_label -> pattern`` for
            combining estimates (e.g., ``{"N (%)": "<count> (<percentage>%)"}``)
        header: Columns to pivot into multi-level column headers.
        settings_columns: Settings columns to include in output.
        group_column: Columns to use for row grouping.
        rename: Column rename mapping (``{display_name: column_name}``).
        type: Output type: ``"gt"`` for great_tables, ``"polars"`` for
            plain DataFrame. Defaults to ``"gt"`` if great_tables is
            available, ``"polars"`` otherwise.
        hide: Columns to exclude from output.
        column_order: Explicit column ordering.
        style: Table style configuration.
        show_min_cell_count: Show ``<N`` for suppressed counts.
        decimals: Override decimal places per estimate type.
        decimal_mark: Decimal separator character.
        big_mark: Thousands separator character.
        title: Table title.
        subtitle: Table subtitle.

    Returns:
        A ``great_tables.GT`` object (if type="gt") or a
        :class:`~polars.DataFrame` (if type="polars").
    """
    # 1. Format estimate values
    result = format_estimate_value(
        result, decimals=decimals, decimal_mark=decimal_mark, big_mark=big_mark
    )

    # 2. Format min cell count
    if show_min_cell_count:
        result = format_min_cell_count(result)

    # 3. Format estimate names
    if estimate_name:
        result = format_estimate_name(result, estimate_name=estimate_name)

    # 4. Build tidy DataFrame
    df = result.data

    # Add settings columns
    if settings_columns:
        settings = result.settings
        cols_to_add = [
            c for c in settings_columns if c in settings.columns and c != "result_id"
        ]
        if cols_to_add:
            join_df = settings.select(["result_id", *cols_to_add])
            df = df.join(join_df, on="result_id", how="left")

    # Split name-level pairs into separate columns
    df = SummarisedResult._split_name_level(df, "group_name", "group_level")
    df = SummarisedResult._split_name_level(df, "strata_name", "strata_level")
    df = SummarisedResult._split_name_level(df, "additional_name", "additional_level")

    # Default hidden columns
    always_hide = {"result_id", "estimate_type"}
    hide_set = always_hide | set(hide or [])
    df = df.drop([c for c in hide_set if c in df.columns])

    # 5. Header pivoting
    if header:
        actual_header = [h for h in header if h in df.columns]
        if actual_header:
            df = _pivot_for_header(df, actual_header)

    # 6. Column ordering
    if column_order:
        ordered_cols = [c for c in column_order if c in df.columns]
        remaining = [c for c in df.columns if c not in ordered_cols]
        df = df.select(ordered_cols + remaining)

    # 7. Rename columns
    if rename:
        # rename is {display_name: column_name}
        reverse_map = {v: k for k, v in rename.items() if v in df.columns}
        if reverse_map:
            df = df.rename(reverse_map)

    # 8. Render
    return format_table(
        df,
        type=type,
        style=style,
        group_column=group_column,
        title=title,
        subtitle=subtitle,
    )


# ── vis_table ─────────────────────────────────────────────────────────────


def vis_table(
    result: pl.DataFrame,
    *,
    estimate_name: dict[str, str] | None = None,
    header: list[str] | None = None,
    group_column: list[str] | None = None,
    rename: dict[str, str] | None = None,
    type: TableType | None = None,
    hide: list[str] | None = None,
    style: TableStyle | None = None,
    title: str | None = None,
    subtitle: str | None = None,
) -> Any:
    """Create a formatted table from any DataFrame.

    A lower-level function than :func:`vis_omop_table` — operates on any
    :class:`~polars.DataFrame`, not just :class:`SummarisedResult`.

    Args:
        result: Any Polars DataFrame.
        estimate_name: Not applicable for plain DataFrames (ignored).
        header: Columns to pivot into headers.
        group_column: Columns to use for row grouping.
        rename: Column rename mapping (``{display_name: column_name}``).
        type: Output type.
        hide: Columns to exclude.
        style: Table style configuration.
        title: Table title.
        subtitle: Table subtitle.

    Returns:
        A ``great_tables.GT`` object or :class:`~polars.DataFrame`.
    """
    df = result

    # Hide columns
    if hide:
        df = df.drop([c for c in hide if c in df.columns])

    # Header pivoting
    if header:
        actual_header = [h for h in header if h in df.columns]
        if actual_header:
            df = _pivot_for_header(df, actual_header)

    # Rename
    if rename:
        reverse_map = {v: k for k, v in rename.items() if v in df.columns}
        if reverse_map:
            df = df.rename(reverse_map)

    return format_table(
        df,
        type=type,
        style=style,
        group_column=group_column,
        title=title,
        subtitle=subtitle,
    )


# ── format_table ──────────────────────────────────────────────────────────


def format_table(
    x: pl.DataFrame,
    *,
    type: TableType | None = None,
    style: TableStyle | None = None,
    na: str | None = None,
    title: str | None = None,
    subtitle: str | None = None,
    group_column: list[str] | None = None,
    group_as_column: bool = False,
    merge: str = "all_columns",
) -> Any:
    """Render a prepared DataFrame to a table object.

    This is the low-level rendering function.  Typically called via
    :func:`vis_omop_table` or :func:`vis_table`, but can be used
    directly for maximum control.

    Args:
        x: A :class:`~polars.DataFrame` to render.
        type: Output type (``"gt"`` or ``"polars"``).
        style: Table style configuration.
        na: String to display for missing values.
        title: Table title.
        subtitle: Table subtitle.
        group_column: Columns for row grouping.
        group_as_column: If ``True``, show groups as a column;
            if ``False``, show as spanning rows.
        merge: Column merge strategy (``"all_columns"`` or ``"none"``).

    Returns:
        A ``great_tables.GT`` object or :class:`~polars.DataFrame`.
    """
    if style is None:
        style = default_table_style()

    if na is None:
        na = style.na_display

    if type is None:
        type = _detect_table_type()

    if type == "polars":
        return _to_polars_table(x, na=na)
    else:
        return _to_gt_table(
            x,
            style=style,
            na=na,
            title=title,
            subtitle=subtitle,
            group_column=group_column,
            group_as_column=group_as_column,
        )


# ── Internal helpers ──────────────────────────────────────────────────────


def _detect_table_type() -> TableType:
    """Detect the best available table type."""
    try:
        import great_tables  # noqa: F401

        return "gt"
    except ImportError:
        return "polars"


def _to_polars_table(df: pl.DataFrame, *, na: str) -> pl.DataFrame:
    """Return a DataFrame with all columns cast to string and nulls replaced by *na*."""
    # Cast all columns to Utf8 so fill_null with a string works uniformly
    df = df.cast({col: pl.Utf8 for col in df.columns})
    return df.fill_null(na)


def _to_gt_table(
    df: pl.DataFrame,
    *,
    style: TableStyle,
    na: str,
    title: str | None,
    subtitle: str | None,
    group_column: list[str] | None,
    group_as_column: bool,
) -> Any:
    """Render a DataFrame as a great_tables.GT object."""
    import great_tables as gt

    # Replace nulls with na string for display
    display_df = df.fill_null(na)

    # Convert to pandas for great_tables (it requires pandas)
    pdf = display_df.to_pandas()

    # Build GT object
    rowname_col = None
    groupname_col = None

    if group_column and len(group_column) > 0:
        if not group_as_column:
            candidate = group_column[0]
            # Only use as groupname if the column actually exists in the DataFrame
            if candidate in pdf.columns:
                groupname_col = candidate
        else:
            groupname_col = None

    tbl = gt.GT(pdf, groupname_col=groupname_col, rowname_col=rowname_col)

    # Apply title/subtitle
    if title or subtitle:
        tbl = tbl.tab_header(title=title or "", subtitle=subtitle or "")

    # Parse multi-level headers from encoded column names
    tbl = _apply_spanner_headers(tbl, df.columns)

    # Apply style
    tbl = _apply_table_style(tbl, style)

    return tbl


def _apply_spanner_headers(tbl: Any, columns: list[str]) -> Any:
    """Parse encoded column names and create spanner headers."""

    spanners: dict[str, list[str]] = {}

    for col in columns:
        parsed = parse_header_keys(col)
        if "header_name" in parsed or "header" in parsed:
            # This column has spanner info
            spanner_label = parsed.get("header_name", parsed.get("header", ""))
            if spanner_label:
                if spanner_label not in spanners:
                    spanners[spanner_label] = []
                spanners[spanner_label].append(col)

            # Rename the column to just the level value
            level = parsed.get("header_level", col)
            tbl = tbl.cols_label(**{col: level})

    # Add spanner headers
    for label, cols in spanners.items():
        tbl = tbl.tab_spanner(label=label, columns=cols)

    return tbl


def _apply_table_style(tbl: Any, style: TableStyle) -> Any:
    """Apply TableStyle to a GT object."""
    import great_tables as gt

    # Header styling
    tbl = tbl.tab_style(
        style=gt.style.fill(color=style.header_background),
        locations=gt.loc.column_labels(),
    )
    tbl = tbl.tab_style(
        style=gt.style.text(color=style.header_color, weight="bold"),
        locations=gt.loc.column_labels(),
    )

    # Body font
    tbl = tbl.tab_style(
        style=gt.style.text(font=style.font_family, size=f"{style.font_size}px"),
        locations=gt.loc.body(),
    )

    # Stripe rows
    if style.stripe:
        tbl = tbl.opt_row_striping()

    return tbl


def _pivot_for_header(df: pl.DataFrame, header_cols: list[str]) -> pl.DataFrame:
    """Pivot columns into wide format for header display.

    For each column in *header_cols*, unique values become new columns.
    The ``estimate_value`` column is spread across these new columns.
    If ``estimate_value`` is not present, joins the first suitable value
    column.
    """
    value_col = "estimate_value"
    if value_col not in df.columns:
        # Look for a suitable value column
        for candidate in df.columns:
            if candidate not in header_cols:
                value_col = candidate
                break
        else:
            return df

    for col in header_cols:
        if col not in df.columns:
            continue

        index_cols = [c for c in df.columns if c != col and c != value_col]
        unique_vals = sorted(
            [v for v in df[col].drop_nulls().unique().to_list() if v is not None]
        )

        if not unique_vals:
            continue

        # Manual pivot: filter + rename + join
        parts: list[pl.DataFrame] = []
        for val in unique_vals:
            subset = df.filter(pl.col(col) == val).drop(col)
            new_name = f"[header_name]{col}\n[header_level]{val}"
            if value_col in subset.columns:
                subset = subset.rename({value_col: new_name})
            parts.append(subset)

        if not parts:
            continue

        result_df = parts[0]
        join_cols = [c for c in index_cols if c in result_df.columns and c != col]
        if join_cols:
            for part in parts[1:]:
                result_df = result_df.join(part, on=join_cols, how="outer_coalesce")
        else:
            # No join cols — just concat horizontally
            result_df = pl.concat(parts, how="horizontal")

        df = result_df
        value_col = None  # After first pivot, value_col is consumed
        if value_col is None:
            break  # Only support one level of header pivoting for now

    return df
