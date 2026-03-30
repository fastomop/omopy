"""Daily dose calculation from drug_strength patterns.

Computes daily dose values for drug_exposure records by matching against
drug_strength table patterns and applying the appropriate formula.

This is the Python equivalent of R's ``addDailyDose()`` and the internal
dose calculation helpers (``drugStrengthPattern``, ``standardUnits``,
``applyFormula``).
"""

from __future__ import annotations

from typing import Any

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.connector.db_source import DbSource
from omopy.drug._data.patterns import (
    PATTERNS,
    FIXED_AMOUNT,
    CONCENTRATION,
    TIME_BASED_DENOM,
    TIME_BASED_NO_DENOM,
    AMOUNT_UNIT_CONVERSIONS,
    NUMERATOR_UNIT_CONVERSIONS,
    DENOMINATOR_UNIT_CONVERSIONS,
)

__all__ = ["add_daily_dose", "pattern_table"]


def add_daily_dose(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    ingredient_concept_id: int,
    name: str | None = None,
) -> CdmTable:
    """Add daily dose and unit columns to a drug exposure table.

    Joins the input table with ``drug_strength``, matches against known
    drug strength patterns, standardises units, and applies the appropriate
    dose calculation formula.

    The input table must have the following columns:
    ``drug_concept_id``, ``drug_exposure_start_date``,
    ``drug_exposure_end_date``, ``quantity``.

    Parameters
    ----------
    x
        A CdmTable (typically drug_exposure or a subset thereof).
    cdm
        The CdmReference. If ``None``, uses ``x.cdm``.
    ingredient_concept_id
        The concept ID of the active ingredient to compute dose for.
        Used to filter ``drug_strength`` to the relevant ingredient.
    name
        Unused (kept for API compatibility).

    Returns
    -------
    CdmTable
        The input table with added ``daily_dose`` (Float64) and ``unit``
        (Utf8) columns.
    """
    cdm = cdm or x.cdm
    if cdm is None:
        msg = "CdmReference is required for daily dose calculation"
        raise ValueError(msg)

    tbl = _get_ibis_or_memtable(x)
    source = cdm.cdm_source

    if not isinstance(source, DbSource):
        msg = "Daily dose calculation requires a database-backed CDM"
        raise TypeError(msg)

    con = source.connection
    catalog = source._catalog
    schema = source.cdm_schema

    # Get drug_strength for the specified ingredient
    drug_strength = con.table("drug_strength", database=(catalog, schema))

    # Filter drug_strength to the specified ingredient
    ds = drug_strength.filter(
        drug_strength.ingredient_concept_id.cast("int64") == ingredient_concept_id
    )

    # Build pattern indicators for matching
    ds = ds.mutate(
        _amount_numeric=ibis.cases(
            (ds.amount_value.notnull(), 1),
            else_=0,
        ),
        _numerator_numeric=ibis.cases(
            (ds.numerator_value.notnull(), 1),
            else_=0,
        ),
        _denominator_numeric=ibis.cases(
            (ds.denominator_value.notnull(), 1),
            else_=0,
        ),
    )

    # Upload patterns as a temp table
    patterns_arrow = _patterns_to_arrow()
    tmp_patterns = f"__omopy_dose_patterns"
    con.con.register(tmp_patterns, patterns_arrow)

    try:
        patterns_tbl = con.table(tmp_patterns)

        # Join drug_strength with patterns on the 6 matching columns
        matched = _join_with_patterns(ds, patterns_tbl)

        # Join input table with matched drug_strength
        result = _join_exposure_with_strength(tbl, matched)

        # Standardise units
        result = _standardise_units(result)

        # Compute days exposed
        result = result.mutate(
            _days_exposed=(
                (result.drug_exposure_end_date - result.drug_exposure_start_date).cast("int64") + 1
            ),
        )

        # Apply formula
        result = _apply_formula(result)

        # Select original columns + daily_dose + unit
        orig_cols = tbl.columns
        out = result.select(*orig_cols, "daily_dose", "unit")

        return x._with_data(out)
    finally:
        try:
            con.con.unregister(tmp_patterns)
        except Exception:
            pass


def pattern_table(cdm: CdmReference) -> pl.DataFrame:
    """Inspect the drug_strength table and return matched patterns.

    Joins ``drug_strength`` with the known pattern table and returns a
    summary of which patterns are present and whether formulas are valid.

    Parameters
    ----------
    cdm
        A database-backed CdmReference.

    Returns
    -------
    pl.DataFrame
        DataFrame with pattern_id, formula_name, unit, and counts.
    """
    if not isinstance(cdm, CdmReference):
        msg = "cdm must be a CdmReference instance"
        raise TypeError(msg)
    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "pattern_table requires a database-backed CDM"
        raise TypeError(msg)

    con = source.connection
    catalog = source._catalog
    schema = source.cdm_schema

    drug_strength = con.table("drug_strength", database=(catalog, schema))

    # Build pattern indicators
    ds = drug_strength.mutate(
        _amount_numeric=ibis.cases(
            (drug_strength.amount_value.notnull(), 1),
            else_=0,
        ),
        _numerator_numeric=ibis.cases(
            (drug_strength.numerator_value.notnull(), 1),
            else_=0,
        ),
        _denominator_numeric=ibis.cases(
            (drug_strength.denominator_value.notnull(), 1),
            else_=0,
        ),
    )

    patterns_arrow = _patterns_to_arrow()
    tmp_patterns = "__omopy_pattern_tbl"
    con.con.register(tmp_patterns, patterns_arrow)

    try:
        patterns_tbl = con.table(tmp_patterns)

        matched = _join_with_patterns(ds, patterns_tbl)

        # Aggregate
        summary = (
            matched.group_by("pattern_id", "formula_name", "unit")
            .agg(n=matched.count())
            .order_by("pattern_id")
        )

        arrow = summary.to_pyarrow()
        return pl.from_arrow(arrow)
    finally:
        try:
            con.con.unregister(tmp_patterns)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_ibis_or_memtable(x: CdmTable | Any) -> ir.Table:
    """Get an Ibis table from a CdmTable (Ibis, Polars, or LazyFrame)."""
    if isinstance(x, CdmTable):
        data = x.data
    else:
        data = x

    if isinstance(data, ir.Table):
        return data
    elif isinstance(data, pl.DataFrame):
        return ibis.memtable(data.to_arrow())
    elif isinstance(data, pl.LazyFrame):
        return ibis.memtable(data.collect().to_arrow())
    else:
        return ibis.memtable(data)


def _patterns_to_arrow() -> Any:
    """Convert the patterns data to a PyArrow table for DB registration."""
    import pyarrow as pa

    rows = {
        "pattern_id": [],
        "p_amount_numeric": [],
        "p_amount_unit_concept_id": [],
        "p_numerator_numeric": [],
        "p_numerator_unit_concept_id": [],
        "p_denominator_numeric": [],
        "p_denominator_unit_concept_id": [],
        "formula_name": [],
        "unit": [],
    }

    for p in PATTERNS:
        rows["pattern_id"].append(p.pattern_id)
        rows["p_amount_numeric"].append(p.amount_numeric)
        rows["p_amount_unit_concept_id"].append(p.amount_unit_concept_id)
        rows["p_numerator_numeric"].append(p.numerator_numeric)
        rows["p_numerator_unit_concept_id"].append(p.numerator_unit_concept_id)
        rows["p_denominator_numeric"].append(p.denominator_numeric)
        rows["p_denominator_unit_concept_id"].append(p.denominator_unit_concept_id)
        rows["formula_name"].append(p.formula_name)
        rows["unit"].append(p.unit)

    return pa.table(
        {
            "pattern_id": pa.array(rows["pattern_id"], type=pa.int32()),
            "p_amount_numeric": pa.array(rows["p_amount_numeric"], type=pa.int32()),
            "p_amount_unit_concept_id": pa.array(
                rows["p_amount_unit_concept_id"], type=pa.int64()
            ),
            "p_numerator_numeric": pa.array(rows["p_numerator_numeric"], type=pa.int32()),
            "p_numerator_unit_concept_id": pa.array(
                rows["p_numerator_unit_concept_id"], type=pa.int64()
            ),
            "p_denominator_numeric": pa.array(rows["p_denominator_numeric"], type=pa.int32()),
            "p_denominator_unit_concept_id": pa.array(
                rows["p_denominator_unit_concept_id"], type=pa.int64()
            ),
            "formula_name": rows["formula_name"],
            "unit": rows["unit"],
        }
    )


def _join_with_patterns(ds: ir.Table, patterns_tbl: ir.Table) -> ir.Table:
    """Join drug_strength records with the pattern lookup table.

    Uses a left join on the 6 pattern-matching columns. For columns that
    can be NULL in the pattern, the join condition uses ``IS NOT DISTINCT FROM``
    semantics (NULLs match NULLs).
    """
    # Build join predicates with NULL-safe equality
    predicates = [
        ds._amount_numeric == patterns_tbl.p_amount_numeric,
        ds._numerator_numeric == patterns_tbl.p_numerator_numeric,
        ds._denominator_numeric == patterns_tbl.p_denominator_numeric,
    ]

    # For unit concept IDs, we need NULL-safe comparison
    # (NULL pattern matches NULL drug_strength)
    predicates.append(
        (ds.amount_unit_concept_id.isnull() & patterns_tbl.p_amount_unit_concept_id.isnull())
        | (ds.amount_unit_concept_id.cast("int64") == patterns_tbl.p_amount_unit_concept_id)
    )
    predicates.append(
        (ds.numerator_unit_concept_id.isnull() & patterns_tbl.p_numerator_unit_concept_id.isnull())
        | (ds.numerator_unit_concept_id.cast("int64") == patterns_tbl.p_numerator_unit_concept_id)
    )
    predicates.append(
        (
            ds.denominator_unit_concept_id.isnull()
            & patterns_tbl.p_denominator_unit_concept_id.isnull()
        )
        | (
            ds.denominator_unit_concept_id.cast("int64")
            == patterns_tbl.p_denominator_unit_concept_id
        )
    )

    combined = ibis.literal(True)
    for pred in predicates:
        combined = combined & pred

    matched = ds.left_join(patterns_tbl, combined)

    return matched


def _join_exposure_with_strength(
    exposure: ir.Table,
    strength: ir.Table,
) -> ir.Table:
    """Join drug exposure records with matched drug_strength patterns."""
    # Select relevant columns from strength
    strength_cols = strength.select(
        ds_drug_concept_id=strength.drug_concept_id.cast("int64"),
        amount_value=strength.amount_value,
        numerator_value=strength.numerator_value,
        denominator_value=strength.denominator_value,
        amount_unit_concept_id=strength.amount_unit_concept_id,
        numerator_unit_concept_id=strength.numerator_unit_concept_id,
        denominator_unit_concept_id=strength.denominator_unit_concept_id,
        pattern_id=strength.pattern_id,
        formula_name=strength.formula_name,
        unit=strength.unit,
    )

    result = exposure.left_join(
        strength_cols,
        exposure.drug_concept_id.cast("int64") == strength_cols.ds_drug_concept_id,
    )

    return result


def _standardise_units(tbl: ir.Table) -> ir.Table:
    """Standardise drug strength values to base units.

    - Micrograms → milligrams (÷ 1000)
    - Mega-IU → IU (÷ 1,000,000)
    - Liters → milliliters (× 1000)
    """
    from omopy.drug._data.patterns import MICROGRAM, MEGA_INTERNATIONAL_UNIT, LITER

    # Standardise amount_value
    tbl = tbl.mutate(
        amount_value=ibis.cases(
            (
                tbl.amount_unit_concept_id.cast("int64") == MICROGRAM,
                tbl.amount_value / 1000.0,
            ),
            else_=tbl.amount_value,
        ),
    )

    # Standardise numerator_value
    tbl = tbl.mutate(
        numerator_value=ibis.cases(
            (
                tbl.numerator_unit_concept_id.cast("int64") == MICROGRAM,
                tbl.numerator_value / 1000.0,
            ),
            (
                tbl.numerator_unit_concept_id.cast("int64") == MEGA_INTERNATIONAL_UNIT,
                tbl.numerator_value / 1_000_000.0,
            ),
            else_=tbl.numerator_value,
        ),
    )

    # Standardise denominator_value
    tbl = tbl.mutate(
        denominator_value=ibis.cases(
            (
                tbl.denominator_unit_concept_id.cast("int64") == LITER,
                tbl.denominator_value * 1000.0,
            ),
            else_=tbl.denominator_value,
        ),
    )

    return tbl


def _apply_formula(tbl: ir.Table) -> ir.Table:
    """Apply dose calculation formulas based on matched pattern.

    Formulas:
    - Fixed amount: amount_value * quantity / days_exposed
    - Concentration: numerator_value * quantity / days_exposed
    - Time-based with denominator: if denom > 24 then num * 24 / denom else num
    - Time-based no denominator: numerator_value * 24
    """
    # Validity checks: set dose to NULL when inputs are invalid
    valid_qty = tbl.quantity.notnull() & (tbl.quantity > 0)
    valid_days = tbl._days_exposed > 0

    fixed_amount_dose = ibis.cases(
        (
            valid_qty & valid_days & tbl.amount_value.notnull() & (tbl.amount_value > 0),
            tbl.amount_value * tbl.quantity / tbl._days_exposed.cast("float64"),
        ),
        else_=ibis.null(),
    )

    concentration_dose = ibis.cases(
        (
            valid_qty & valid_days & tbl.numerator_value.notnull() & (tbl.numerator_value > 0),
            tbl.numerator_value * tbl.quantity / tbl._days_exposed.cast("float64"),
        ),
        else_=ibis.null(),
    )

    time_denom_dose = ibis.cases(
        (
            tbl.numerator_value.notnull()
            & (tbl.numerator_value > 0)
            & tbl.denominator_value.notnull()
            & (tbl.denominator_value > 0)
            & (tbl.denominator_value > 24),
            tbl.numerator_value * 24.0 / tbl.denominator_value,
        ),
        (
            tbl.numerator_value.notnull()
            & (tbl.numerator_value > 0)
            & tbl.denominator_value.notnull()
            & (tbl.denominator_value > 0),
            tbl.numerator_value,
        ),
        else_=ibis.null(),
    )

    time_no_denom_dose = ibis.cases(
        (
            tbl.numerator_value.notnull() & (tbl.numerator_value > 0),
            tbl.numerator_value * 24.0,
        ),
        else_=ibis.null(),
    )

    tbl = tbl.mutate(
        daily_dose=ibis.cases(
            (tbl.formula_name == FIXED_AMOUNT, fixed_amount_dose),
            (tbl.formula_name == CONCENTRATION, concentration_dose),
            (tbl.formula_name == TIME_BASED_DENOM, time_denom_dose),
            (tbl.formula_name == TIME_BASED_NO_DENOM, time_no_denom_dose),
            else_=ibis.null(),
        ).cast("float64"),
    )

    # Set unit to NULL if daily_dose is NULL
    tbl = tbl.mutate(
        unit=ibis.cases(
            (tbl.daily_dose.isnull(), ibis.null()),
            else_=tbl.unit,
        ),
    )

    return tbl
