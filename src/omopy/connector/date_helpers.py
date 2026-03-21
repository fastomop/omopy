"""Dialect-aware date arithmetic helpers for Ibis expressions.

Provides ``dateadd``, ``datediff``, and ``datepart`` that work on Ibis
table expressions across all backends (DuckDB, PostgreSQL, etc.).

Unlike the R CDMConnector which generates raw SQL strings per dialect,
we leverage Ibis's built-in date operations which already handle dialect
translation. These helpers provide a convenient, CDM-oriented API.

This is the Python equivalent of R's ``dateadd.R``.
"""

from __future__ import annotations

from typing import Any, Literal

import ibis
import ibis.expr.types as ir
import polars as pl

__all__ = ["dateadd", "datediff", "datepart"]

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

IntervalUnit = Literal["day", "year", "month"]
DatePartUnit = Literal["day", "month", "year"]


# ---------------------------------------------------------------------------
# dateadd — add days/months/years to a date column
# ---------------------------------------------------------------------------


def dateadd(
    expr: ir.Column | ir.Table,
    date_col: str,
    number: int | str,
    *,
    interval: IntervalUnit = "day",
) -> ir.Column | ir.Table:
    """Add a time interval to a date column.

    Works on both Ibis table expressions and individual columns. If *expr*
    is a Table, returns a new Table with a computed column added. If it's
    a Column (date expression), returns a new date Column.

    Parameters
    ----------
    expr
        An Ibis Table or date Column expression.
    date_col
        Name of the date column to modify.
    number
        Number of units to add (can be negative). If a string, it is
        interpreted as a column name containing the number.
    interval
        The unit of time: ``"day"`` (default), ``"month"``, or ``"year"``.

    Returns
    -------
    ir.Column
        A new date column expression with the interval added.

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table({"start_date": "date", "days_offset": "int64"}, name="events")
    >>> # Add 30 days to start_date
    >>> new_date = dateadd(t, "start_date", 30)
    >>> # Add a column-based number of years
    >>> new_date = dateadd(t, "start_date", "days_offset", interval="day")
    """
    if interval not in ("day", "month", "year"):
        msg = f"interval must be 'day', 'month', or 'year', got {interval!r}"
        raise ValueError(msg)

    if isinstance(expr, ir.Table):
        date_expr = expr[date_col]
    else:
        date_expr = expr

    # Resolve the number: literal int or column reference
    if isinstance(number, str):
        if not isinstance(expr, ir.Table):
            msg = "When number is a column name, expr must be an Ibis Table"
            raise TypeError(msg)
        num_expr = expr[number]
    else:
        num_expr = ibis.literal(number)

    # Use Ibis interval arithmetic (handles dialect translation)
    if interval == "day":
        return date_expr + num_expr.cast("int64") * ibis.interval(days=1)
    elif interval == "month":
        return date_expr + num_expr.cast("int64") * ibis.interval(months=1)
    else:  # year
        return date_expr + num_expr.cast("int64") * ibis.interval(years=1)


# ---------------------------------------------------------------------------
# datediff — compute difference between two date columns
# ---------------------------------------------------------------------------


def datediff(
    table: ir.Table,
    start_col: str,
    end_col: str,
    *,
    interval: IntervalUnit = "day",
) -> ir.Column:
    """Compute the difference between two date columns.

    Parameters
    ----------
    table
        An Ibis Table expression containing both date columns.
    start_col
        Name of the start date column.
    end_col
        Name of the end date column.
    interval
        The unit for the difference: ``"day"`` (default), ``"month"``,
        or ``"year"``.

    Returns
    -------
    ir.Column
        An integer column expression with the difference in the
        specified units.

    Notes
    -----
    For ``"day"`` interval, this returns ``end_col - start_col`` in whole
    days. For ``"month"`` and ``"year"``, it uses calendar-based
    computation (matching R's ``clock::date_count_between``):

    - Months: ``(year_end * 12 + month_end) - (year_start * 12 + month_start)``,
      adjusted for day-of-month.
    - Years: ``year_end - year_start``, adjusted for month+day.

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table(
    ...     {"start_date": "date", "end_date": "date"}, name="periods"
    ... )
    >>> days_diff = datediff(t, "start_date", "end_date")
    >>> years_diff = datediff(t, "start_date", "end_date", interval="year")
    """
    if interval not in ("day", "month", "year"):
        msg = f"interval must be 'day', 'month', or 'year', got {interval!r}"
        raise ValueError(msg)

    start = table[start_col]
    end = table[end_col]

    if interval == "day":
        # Date subtraction yields interval; cast to int64 gives days
        return (end - start).cast("int64")

    # For month/year, extract components and compute calendar difference
    # This matches R's floor-based approach:
    # month diff = floor((y2*1200 + m2*100 + d2 - (y1*1200 + m1*100 + d1)) / 100)
    # year diff  = floor((y2*10000 + m2*100 + d2 - (y1*10000 + m1*100 + d1)) / 10000)
    y1 = start.year().cast("int64")
    m1 = start.month().cast("int64")
    d1 = start.day().cast("int64")
    y2 = end.year().cast("int64")
    m2 = end.month().cast("int64")
    d2 = end.day().cast("int64")

    if interval == "month":
        return (
            (y2 * ibis.literal(1200) + m2 * ibis.literal(100) + d2)
            - (y1 * ibis.literal(1200) + m1 * ibis.literal(100) + d1)
        ) / ibis.literal(100)
    else:  # year
        return (
            (y2 * ibis.literal(10000) + m2 * ibis.literal(100) + d2)
            - (y1 * ibis.literal(10000) + m1 * ibis.literal(100) + d1)
        ) / ibis.literal(10000)


# ---------------------------------------------------------------------------
# datepart — extract day/month/year from a date column
# ---------------------------------------------------------------------------


def datepart(
    table: ir.Table,
    date_col: str,
    part: DatePartUnit = "year",
) -> ir.Column:
    """Extract a part (day, month, year) from a date column.

    Parameters
    ----------
    table
        An Ibis Table expression.
    date_col
        Name of the date column.
    part
        The part to extract: ``"year"`` (default), ``"month"``, or
        ``"day"``.

    Returns
    -------
    ir.Column
        An integer column expression with the extracted part.

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table({"birth_date": "date"}, name="person")
    >>> birth_year = datepart(t, "birth_date", "year")
    >>> birth_month = datepart(t, "birth_date", "month")
    """
    if part not in ("day", "month", "year"):
        msg = f"part must be 'day', 'month', or 'year', got {part!r}"
        raise ValueError(msg)

    col = table[date_col]
    if part == "year":
        return col.year()
    elif part == "month":
        return col.month()
    else:  # day
        return col.day()


# ---------------------------------------------------------------------------
# Polars-compatible variants (for local CdmTables backed by Polars)
# ---------------------------------------------------------------------------


def dateadd_polars(
    df: pl.DataFrame | pl.LazyFrame,
    date_col: str,
    number: int | str,
    *,
    interval: IntervalUnit = "day",
    result_col: str | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Add a time interval to a date column in a Polars DataFrame.

    Parameters
    ----------
    df
        A Polars DataFrame or LazyFrame.
    date_col
        Name of the date column.
    number
        Number of units to add, or name of a column containing the number.
    interval
        The unit: ``"day"`` (default), ``"month"``, or ``"year"``.
    result_col
        Name for the result column. Defaults to ``date_col`` (in-place).

    Returns
    -------
    pl.DataFrame | pl.LazyFrame
        The DataFrame with the new/modified date column.
    """
    if interval not in ("day", "month", "year"):
        msg = f"interval must be 'day', 'month', or 'year', got {interval!r}"
        raise ValueError(msg)

    out_col = result_col or date_col

    if isinstance(number, str):
        num_expr = pl.col(number)
    else:
        num_expr = pl.lit(number)

    if interval == "day":
        duration = pl.duration(days=num_expr)
    elif interval == "month":
        duration = pl.duration(days=num_expr * 30)  # Approximation
        # Polars has offset_by for exact month arithmetic
        return df.with_columns(
            pl.col(date_col)
            .dt.offset_by(pl.format("{}mo", num_expr))
            .alias(out_col)
        ) if isinstance(number, int) else df.with_columns(
            (pl.col(date_col) + duration).alias(out_col)
        )
    else:  # year
        if isinstance(number, int):
            return df.with_columns(
                pl.col(date_col)
                .dt.offset_by(f"{number}y")
                .alias(out_col)
            )
        else:
            duration = pl.duration(days=num_expr * 365)

    return df.with_columns((pl.col(date_col) + duration).alias(out_col))


def datediff_polars(
    df: pl.DataFrame | pl.LazyFrame,
    start_col: str,
    end_col: str,
    *,
    interval: IntervalUnit = "day",
    result_col: str = "date_diff",
) -> pl.DataFrame | pl.LazyFrame:
    """Compute date difference in a Polars DataFrame.

    Parameters
    ----------
    df
        A Polars DataFrame or LazyFrame.
    start_col
        Name of the start date column.
    end_col
        Name of the end date column.
    interval
        The unit: ``"day"`` (default), ``"month"``, or ``"year"``.
    result_col
        Name for the result column (default ``"date_diff"``).

    Returns
    -------
    pl.DataFrame | pl.LazyFrame
        The DataFrame with a new integer column containing the difference.
    """
    if interval not in ("day", "month", "year"):
        msg = f"interval must be 'day', 'month', or 'year', got {interval!r}"
        raise ValueError(msg)

    if interval == "day":
        return df.with_columns(
            (pl.col(end_col) - pl.col(start_col)).dt.total_days().alias(result_col)
        )
    elif interval == "month":
        # Calendar month difference (matches R behavior)
        # Cast all parts to Int64 to avoid i8 overflow (month is i8 in Polars)
        y1 = pl.col(start_col).dt.year().cast(pl.Int64)
        m1 = pl.col(start_col).dt.month().cast(pl.Int64)
        d1 = pl.col(start_col).dt.day().cast(pl.Int64)
        y2 = pl.col(end_col).dt.year().cast(pl.Int64)
        m2 = pl.col(end_col).dt.month().cast(pl.Int64)
        d2 = pl.col(end_col).dt.day().cast(pl.Int64)
        return df.with_columns(
            (
                (y2 * 1200 + m2 * 100 + d2 - (y1 * 1200 + m1 * 100 + d1)) // 100
            ).alias(result_col)
        )
    else:  # year
        # Cast all parts to Int64 to avoid i8 overflow
        y1 = pl.col(start_col).dt.year().cast(pl.Int64)
        m1 = pl.col(start_col).dt.month().cast(pl.Int64)
        d1 = pl.col(start_col).dt.day().cast(pl.Int64)
        y2 = pl.col(end_col).dt.year().cast(pl.Int64)
        m2 = pl.col(end_col).dt.month().cast(pl.Int64)
        d2 = pl.col(end_col).dt.day().cast(pl.Int64)
        return df.with_columns(
            (
                (y2 * 10000 + m2 * 100 + d2 - (y1 * 10000 + m1 * 100 + d1)) // 10000
            ).alias(result_col)
        )
