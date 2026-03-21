"""DBMS-independent quantile estimation.

Provides ``summarise_quantile()`` which computes quantiles using a
cumulative-sum approach that works on any database backend (including
those without native ``PERCENTILE_CONT``).

Equivalent to R's ``summariseQuantile2()``.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir
import polars as pl

__all__ = ["summarise_quantile"]


def summarise_quantile(
    data: ir.Table | pl.DataFrame | pl.LazyFrame,
    columns: str | list[str],
    probs: list[float],
    *,
    group_by: str | list[str] | None = None,
) -> ir.Table | pl.DataFrame:
    """Compute quantiles for one or more numeric columns.

    Uses a cumulative-sum / inverse-CDF approach (equivalent to
    ``quantile(type=1)`` in R) that generates pure SQL and works on all
    database backends.

    Parameters
    ----------
    data
        An Ibis table, Polars DataFrame, or Polars LazyFrame.
    columns
        Column name(s) to compute quantiles for.
    probs
        Probability values in [0, 1].
    group_by
        Optional grouping column(s).

    Returns
    -------
    ir.Table | pl.DataFrame
        One row per group (or a single row if ungrouped), with columns
        named ``q{pct:02d}_{col}`` (e.g. ``q25_age``, ``q50_age``).

    Raises
    ------
    ValueError
        If *probs* contains values outside [0, 1], or *columns* is empty.
    """
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        msg = "columns must not be empty"
        raise ValueError(msg)

    probs = sorted(set(probs))
    for p in probs:
        if not (0.0 <= p <= 1.0):
            msg = f"All probs must be in [0, 1], got {p}"
            raise ValueError(msg)

    if isinstance(group_by, str):
        group_by = [group_by]

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        return _quantile_polars(data, columns, probs, group_by)
    else:
        return _quantile_ibis(data, columns, probs, group_by)


# ---------------------------------------------------------------------------
# Ibis implementation
# ---------------------------------------------------------------------------


def _quantile_ibis_single(
    data: ir.Table,
    col: str,
    probs: list[float],
    group_by: list[str] | None,
) -> ir.Table:
    """Compute quantiles for a single column via Ibis."""
    group_cols = group_by or []

    # Step 1: group by (group_cols + value_col), count occurrences
    agg_cols = [*group_cols, col]
    grouped = data.group_by(agg_cols).agg(n__=data.count())

    # Step 2: window cumsum ordered by the value column
    if group_cols:
        window = ibis.window(
            group_by=[grouped[g] for g in group_cols],
            order_by=grouped[col],
        )
    else:
        window = ibis.window(order_by=grouped[col])

    with_cum = grouped.mutate(
        accumulated=grouped["n__"].sum().over(window),
        total=grouped["n__"].sum().over(
            ibis.window(group_by=[grouped[g] for g in group_cols])
            if group_cols
            else ibis.window()
        ),
    )

    # Step 3: for each prob, compute min(value where accumulated >= prob * total)
    quant_exprs: dict[str, ir.Scalar] = {}
    for p in probs:
        pct = int(p * 100)
        col_name = f"q{pct:02d}_{col}"
        # min(case when accumulated >= p*total then value else null end)
        quant_exprs[col_name] = (
            ibis.cases(
                (with_cum["accumulated"] >= p * with_cum["total"], with_cum[col]),
                else_=ibis.null(),
            )
            .min()
        )

    if group_cols:
        result = with_cum.group_by(group_cols).agg(**quant_exprs)
    else:
        result = with_cum.aggregate(**quant_exprs)

    return result


def _quantile_ibis(
    data: ir.Table,
    columns: list[str],
    probs: list[float],
    group_by: list[str] | None,
) -> ir.Table:
    """Compute quantiles for multiple columns via Ibis, joined by group keys."""
    results = []
    for col in columns:
        results.append(_quantile_ibis_single(data, col, probs, group_by))

    if len(results) == 1:
        return results[0]

    # Join all single-column results on group keys
    combined = results[0]
    if group_by:
        for r in results[1:]:
            combined = combined.join(r, group_by)
    else:
        for r in results[1:]:
            combined = combined.cross_join(r)

    return combined


# ---------------------------------------------------------------------------
# Polars implementation
# ---------------------------------------------------------------------------


def _quantile_polars_single(
    data: pl.DataFrame,
    col: str,
    probs: list[float],
    group_by: list[str] | None,
) -> pl.DataFrame:
    """Compute quantiles for a single column via Polars."""
    group_cols = group_by or []

    # Step 1: count per (group_cols + value)
    agg_key = [*group_cols, col]
    counted = data.group_by(agg_key).agg(pl.len().alias("n__"))

    # Step 2: sort by value within groups, cumsum, total
    if group_cols:
        counted = counted.sort(col)
        counted = counted.with_columns(
            pl.col("n__")
            .cum_sum()
            .over(group_cols)
            .alias("accumulated"),
            pl.col("n__").sum().over(group_cols).alias("total"),
        )
    else:
        counted = counted.sort(col)
        counted = counted.with_columns(
            pl.col("n__").cum_sum().alias("accumulated"),
            pl.col("n__").sum().alias("total"),
        )

    # Step 3: for each prob, find min value where accumulated >= prob * total
    quant_exprs: list[pl.Expr] = []
    for p in probs:
        pct = int(p * 100)
        col_name = f"q{pct:02d}_{col}"
        quant_exprs.append(
            pl.when(pl.col("accumulated") >= p * pl.col("total"))
            .then(pl.col(col))
            .otherwise(None)
            .min()
            .alias(col_name)
        )

    if group_cols:
        result = counted.group_by(group_cols).agg(quant_exprs)
    else:
        result = counted.select(quant_exprs)

    return result


def _quantile_polars(
    data: pl.DataFrame | pl.LazyFrame,
    columns: list[str],
    probs: list[float],
    group_by: list[str] | None,
) -> pl.DataFrame:
    """Compute quantiles for multiple columns via Polars."""
    if isinstance(data, pl.LazyFrame):
        data = data.collect()

    results = []
    for col in columns:
        results.append(_quantile_polars_single(data, col, probs, group_by))

    if len(results) == 1:
        return results[0]

    # Join on group keys
    combined = results[0]
    if group_by:
        for r in results[1:]:
            combined = combined.join(r, on=group_by, how="full", coalesce=True)
    else:
        for r in results[1:]:
            combined = combined.hstack(r)

    return combined
