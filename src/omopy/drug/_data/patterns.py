"""Drug strength pattern data for daily dose calculation.

This module contains the pattern matching table and unit conversion rules
needed to compute daily doses from OMOP drug_strength records. Ported from
the DARWIN-EU DrugUtilisation R package's internal ``sysdata.rda``.

The patterns match combinations of amount/numerator/denominator presence
and unit concept IDs to one of four dose calculation formulas.
"""

from __future__ import annotations

from typing import NamedTuple


class _Pattern(NamedTuple):
    """A drug strength pattern row."""

    pattern_id: int
    amount_numeric: int  # 1 if amount_value expected, 0 otherwise
    amount_unit_concept_id: int | None
    numerator_numeric: int  # 1 if numerator_value expected, 0 otherwise
    numerator_unit_concept_id: int | None
    denominator_numeric: int  # 1 if denominator_value expected, 0 otherwise
    denominator_unit_concept_id: int | None
    formula_name: str
    unit: str  # output unit for the computed daily dose


# ---------------------------------------------------------------------------
# Unit concept IDs (OMOP standard vocabulary)
# ---------------------------------------------------------------------------
HOUR = 8505
UNIT = 8510
LITER = 8519
MILLIGRAM = 8576
MILLILITER = 8587
INTERNATIONAL_UNIT = 8718
MEGA_INTERNATIONAL_UNIT = 9439
SQUARE_CENTIMETER = 9483
MILLIEQUIVALENT = 9551
MICROGRAM = 9655
ACTUATION = 45744809

# ---------------------------------------------------------------------------
# Formula names
# ---------------------------------------------------------------------------
FIXED_AMOUNT = "fixed amount formulation"
CONCENTRATION = "concentration formulation"
TIME_BASED_DENOM = "time based with denominator"
TIME_BASED_NO_DENOM = "time based no denominator"

# ---------------------------------------------------------------------------
# The 41 drug strength patterns
# ---------------------------------------------------------------------------
PATTERNS: tuple[_Pattern, ...] = (
    # Time-based with denominator (1-3): numerator + denominator, denom unit = hour
    _Pattern(1, 0, None, 1, MICROGRAM, 1, HOUR, TIME_BASED_DENOM, "milliequivalent"),
    _Pattern(2, 0, None, 1, MILLIGRAM, 1, HOUR, TIME_BASED_DENOM, "milligram"),
    _Pattern(3, 0, None, 1, UNIT, 1, HOUR, TIME_BASED_DENOM, "international unit"),
    # Time-based no denominator (4-5): numerator, denom unit = hour but no value
    _Pattern(4, 0, None, 1, MICROGRAM, 0, HOUR, TIME_BASED_NO_DENOM, "milligram"),
    _Pattern(5, 0, None, 1, MILLIGRAM, 0, HOUR, TIME_BASED_NO_DENOM, "milliliter"),
    # Fixed amount formulation (6-11): only amount present
    _Pattern(6, 1, INTERNATIONAL_UNIT, 0, None, 0, None, FIXED_AMOUNT, "international unit"),
    _Pattern(7, 1, MICROGRAM, 0, None, 0, None, FIXED_AMOUNT, "milligram"),
    _Pattern(8, 1, MILLIEQUIVALENT, 0, None, 0, None, FIXED_AMOUNT, "milliequivalent"),
    _Pattern(9, 1, MILLIGRAM, 0, None, 0, None, FIXED_AMOUNT, "milligram"),
    _Pattern(10, 1, MILLILITER, 0, None, 0, None, FIXED_AMOUNT, "milliliter"),
    _Pattern(11, 1, UNIT, 0, None, 0, None, FIXED_AMOUNT, "international unit"),
    # Concentration with denominator value (12-25)
    _Pattern(
        12, 0, None, 1, INTERNATIONAL_UNIT, 1, MILLIGRAM, CONCENTRATION, "international unit"
    ),
    _Pattern(
        13, 0, None, 1, INTERNATIONAL_UNIT, 1, MILLILITER, CONCENTRATION, "international unit"
    ),
    _Pattern(14, 0, None, 1, MILLIEQUIVALENT, 1, MILLILITER, CONCENTRATION, "milliequivalent"),
    _Pattern(15, 0, None, 1, MILLIGRAM, 1, ACTUATION, CONCENTRATION, "milligram"),
    _Pattern(16, 0, None, 1, MILLIGRAM, 1, LITER, CONCENTRATION, "milligram"),
    _Pattern(17, 0, None, 1, MILLIGRAM, 1, MILLIGRAM, CONCENTRATION, "milligram"),
    _Pattern(18, 0, None, 1, MILLIGRAM, 1, MILLILITER, CONCENTRATION, "milligram"),
    _Pattern(19, 0, None, 1, MILLIGRAM, 1, SQUARE_CENTIMETER, CONCENTRATION, "milligram"),
    _Pattern(20, 0, None, 1, MILLILITER, 1, MILLIGRAM, CONCENTRATION, "milliliter"),
    _Pattern(21, 0, None, 1, MILLILITER, 1, MILLILITER, CONCENTRATION, "milliliter"),
    _Pattern(22, 0, None, 1, UNIT, 1, ACTUATION, CONCENTRATION, "international unit"),
    _Pattern(23, 0, None, 1, UNIT, 1, MILLIGRAM, CONCENTRATION, "international unit"),
    _Pattern(24, 0, None, 1, UNIT, 1, MILLILITER, CONCENTRATION, "international unit"),
    _Pattern(25, 0, None, 1, UNIT, 1, SQUARE_CENTIMETER, CONCENTRATION, "international unit"),
    # Concentration without denominator value (26-41)
    _Pattern(
        26, 0, None, 1, INTERNATIONAL_UNIT, 0, MILLIGRAM, CONCENTRATION, "international unit"
    ),
    _Pattern(
        27, 0, None, 1, INTERNATIONAL_UNIT, 0, MILLILITER, CONCENTRATION, "international unit"
    ),
    _Pattern(
        28, 0, None, 1, MEGA_INTERNATIONAL_UNIT, 0, MILLILITER, CONCENTRATION, "international unit"
    ),
    _Pattern(29, 0, None, 1, MILLIEQUIVALENT, 0, MILLIGRAM, CONCENTRATION, "milliequivalent"),
    _Pattern(30, 0, None, 1, MILLIEQUIVALENT, 0, MILLILITER, CONCENTRATION, "milliequivalent"),
    _Pattern(31, 0, None, 1, MILLIGRAM, 0, ACTUATION, CONCENTRATION, "milligram"),
    _Pattern(32, 0, None, 1, MILLIGRAM, 0, LITER, CONCENTRATION, "milligram"),
    _Pattern(33, 0, None, 1, MILLIGRAM, 0, MILLIGRAM, CONCENTRATION, "milligram"),
    _Pattern(34, 0, None, 1, MILLIGRAM, 0, MILLILITER, CONCENTRATION, "milligram"),
    _Pattern(35, 0, None, 1, MILLIGRAM, 0, SQUARE_CENTIMETER, CONCENTRATION, "milligram"),
    _Pattern(36, 0, None, 1, MILLILITER, 0, MILLIGRAM, CONCENTRATION, "milliliter"),
    _Pattern(37, 0, None, 1, MILLILITER, 0, MILLILITER, CONCENTRATION, "milliliter"),
    _Pattern(38, 0, None, 1, UNIT, 0, ACTUATION, CONCENTRATION, "international unit"),
    _Pattern(39, 0, None, 1, UNIT, 0, MILLIGRAM, CONCENTRATION, "international unit"),
    _Pattern(40, 0, None, 1, UNIT, 0, MILLILITER, CONCENTRATION, "international unit"),
    _Pattern(41, 0, None, 1, UNIT, 0, SQUARE_CENTIMETER, CONCENTRATION, "international unit"),
)


# ---------------------------------------------------------------------------
# Unit standardisation: convert to base units before applying formulas
# ---------------------------------------------------------------------------
# {source_concept_id: (target_concept_id, multiplier)}
AMOUNT_UNIT_CONVERSIONS: dict[int, tuple[int, float]] = {
    MICROGRAM: (MILLIGRAM, 1e-3),  # μg → mg
}

NUMERATOR_UNIT_CONVERSIONS: dict[int, tuple[int, float]] = {
    MICROGRAM: (MILLIGRAM, 1e-3),  # μg → mg
    MEGA_INTERNATIONAL_UNIT: (INTERNATIONAL_UNIT, 1e-6),  # M IU → IU (÷ 1e6)
}

DENOMINATOR_UNIT_CONVERSIONS: dict[int, tuple[int, float]] = {
    LITER: (MILLILITER, 1e3),  # L → mL
}


def get_patterns_as_dicts() -> list[dict]:
    """Return pattern data as a list of dicts (for constructing Polars/Arrow tables)."""
    return [p._asdict() for p in PATTERNS]
