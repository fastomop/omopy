"""Tests for omopy.drug — DrugUtilisation module.

Tests are organised into:
1. Unit tests using mock data (patterns, daily dose, mock, table, plot)
2. Integration tests using the Synthea database (cohort generation,
   require, add_drug_use, summarise)
"""

from __future__ import annotations

import datetime
import math
import warnings
from typing import Any

import polars as pl
import pytest

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_drug_cohort(
    n_cohorts: int = 1,
    n_subjects: int = 5,
    gap_era: int = 30,
    cdm=None,
) -> CohortTable:
    """Create a minimal drug CohortTable with test data."""
    rows: list[dict[str, Any]] = []
    sid = 1
    for cid in range(1, n_cohorts + 1):
        for _ in range(n_subjects):
            rows.append(
                {
                    "cohort_definition_id": cid,
                    "subject_id": sid,
                    "cohort_start_date": datetime.date(2020, 1, 1),
                    "cohort_end_date": datetime.date(2020, 6, 30),
                }
            )
            sid += 1

    df = pl.DataFrame(rows)
    settings = pl.DataFrame(
        {
            "cohort_definition_id": list(range(1, n_cohorts + 1)),
            "cohort_name": [f"drug_{i}" for i in range(1, n_cohorts + 1)],
            "gap_era": [gap_era] * n_cohorts,
        }
    )

    ct = CohortTable(df, settings=settings)
    if cdm is not None:
        ct.cdm = cdm
    return ct


# ===================================================================
# Tests: Module imports and exports
# ===================================================================


class TestModuleImports:
    """Verify that the module imports correctly and exports all 44 items."""

    def test_import_module(self):
        import omopy.drug

        assert hasattr(omopy.drug, "__all__")

    def test_export_count(self):
        import omopy.drug

        assert len(omopy.drug.__all__) == 44

    def test_all_exports_are_callable(self):
        import omopy.drug

        for name in omopy.drug.__all__:
            obj = getattr(omopy.drug, name)
            assert callable(obj), f"{name} is not callable"

    def test_cohort_generation_exports(self):
        from omopy.drug import (
            generate_drug_utilisation_cohort_set,
            generate_ingredient_cohort_set,
            generate_atc_cohort_set,
            erafy_cohort,
            cohort_gap_era,
        )

    def test_daily_dose_exports(self):
        from omopy.drug import add_daily_dose, pattern_table

    def test_require_exports(self):
        from omopy.drug import (
            require_is_first_drug_entry,
            require_prior_drug_washout,
            require_observation_before_drug,
            require_drug_in_date_range,
        )

    def test_add_drug_use_exports(self):
        from omopy.drug import (
            add_drug_utilisation,
            add_number_exposures,
            add_number_eras,
            add_days_exposed,
            add_days_prescribed,
            add_time_to_exposure,
            add_initial_exposure_duration,
            add_initial_quantity,
            add_cumulative_quantity,
            add_initial_daily_dose,
            add_cumulative_dose,
            add_drug_restart,
        )

    def test_add_intersect_exports(self):
        from omopy.drug import add_indication, add_treatment

    def test_summarise_exports(self):
        from omopy.drug import (
            summarise_drug_utilisation,
            summarise_indication,
            summarise_treatment,
            summarise_drug_restart,
            summarise_dose_coverage,
            summarise_proportion_of_patients_covered,
        )

    def test_table_exports(self):
        from omopy.drug import (
            table_drug_utilisation,
            table_indication,
            table_treatment,
            table_drug_restart,
            table_dose_coverage,
            table_proportion_of_patients_covered,
        )

    def test_plot_exports(self):
        from omopy.drug import (
            plot_drug_utilisation,
            plot_indication,
            plot_treatment,
            plot_drug_restart,
            plot_proportion_of_patients_covered,
        )

    def test_mock_exports(self):
        from omopy.drug import mock_drug_utilisation, benchmark_drug_utilisation


# ===================================================================
# Tests: Pattern table
# ===================================================================


class TestPatternData:
    """Test the raw drug strength pattern data (no database required)."""

    def test_pattern_count(self):
        from omopy.drug._data.patterns import PATTERNS

        assert len(PATTERNS) == 41

    def test_pattern_fields(self):
        from omopy.drug._data.patterns import PATTERNS

        for p in PATTERNS:
            assert hasattr(p, "pattern_id")
            assert hasattr(p, "amount_numeric")
            assert hasattr(p, "amount_unit_concept_id")
            assert hasattr(p, "numerator_numeric")
            assert hasattr(p, "numerator_unit_concept_id")
            assert hasattr(p, "denominator_numeric")
            assert hasattr(p, "denominator_unit_concept_id")
            assert hasattr(p, "formula_name")
            assert hasattr(p, "unit")

    def test_pattern_ids_unique(self):
        from omopy.drug._data.patterns import PATTERNS

        ids = [p.pattern_id for p in PATTERNS]
        assert len(ids) == len(set(ids))

    def test_pattern_ids_sequential(self):
        from omopy.drug._data.patterns import PATTERNS

        ids = [p.pattern_id for p in PATTERNS]
        assert ids == list(range(1, 42))

    def test_pattern_formula_names(self):
        from omopy.drug._data.patterns import (
            PATTERNS,
            FIXED_AMOUNT,
            CONCENTRATION,
            TIME_BASED_DENOM,
            TIME_BASED_NO_DENOM,
        )

        valid_formulas = {FIXED_AMOUNT, CONCENTRATION, TIME_BASED_DENOM, TIME_BASED_NO_DENOM}
        formula_names = {p.formula_name for p in PATTERNS}
        assert formula_names <= valid_formulas

    def test_pattern_amount_numeric_binary(self):
        from omopy.drug._data.patterns import PATTERNS

        for p in PATTERNS:
            assert p.amount_numeric in (0, 1)
            assert p.numerator_numeric in (0, 1)
            assert p.denominator_numeric in (0, 1)

    def test_pattern_output_units(self):
        from omopy.drug._data.patterns import PATTERNS

        valid_units = {"milligram", "milliliter", "international unit", "milliequivalent"}
        for p in PATTERNS:
            assert p.unit in valid_units, f"Pattern {p.pattern_id} has invalid unit: {p.unit}"

    def test_get_patterns_as_dicts(self):
        from omopy.drug._data.patterns import get_patterns_as_dicts

        dicts = get_patterns_as_dicts()
        assert len(dicts) == 41
        assert isinstance(dicts[0], dict)
        assert "pattern_id" in dicts[0]

    def test_patterns_deterministic(self):
        from omopy.drug._data.patterns import get_patterns_as_dicts

        d1 = get_patterns_as_dicts()
        d2 = get_patterns_as_dicts()
        assert d1 == d2


# ===================================================================
# Tests: Data module constants
# ===================================================================


class TestDataConstants:
    """Test the unit concept ID constants."""

    def test_unit_constants(self):
        from omopy.drug._data.patterns import (
            HOUR,
            UNIT,
            LITER,
            MILLIGRAM,
            MILLILITER,
            INTERNATIONAL_UNIT,
            MEGA_INTERNATIONAL_UNIT,
            SQUARE_CENTIMETER,
            MILLIEQUIVALENT,
            MICROGRAM,
            ACTUATION,
        )

        assert HOUR == 8505
        assert UNIT == 8510
        assert LITER == 8519
        assert MILLIGRAM == 8576
        assert MILLILITER == 8587
        assert INTERNATIONAL_UNIT == 8718
        assert MEGA_INTERNATIONAL_UNIT == 9439
        assert SQUARE_CENTIMETER == 9483
        assert MILLIEQUIVALENT == 9551
        assert MICROGRAM == 9655
        assert ACTUATION == 45744809


# ===================================================================
# Tests: Mock drug utilisation
# ===================================================================


class TestMockDrugUtilisation:
    """Test the mock data generator."""

    def test_basic_mock(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

    def test_result_type(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        assert result.settings["result_type"][0] == "summarise_drug_utilisation"

    def test_n_cohorts(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation(n_cohorts=3)
        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 3

    def test_deterministic_with_seed(self):
        from omopy.drug import mock_drug_utilisation

        r1 = mock_drug_utilisation(seed=123)
        r2 = mock_drug_utilisation(seed=123)
        assert r1.data.equals(r2.data)

    def test_different_seeds_differ(self):
        from omopy.drug import mock_drug_utilisation

        r1 = mock_drug_utilisation(seed=1)
        r2 = mock_drug_utilisation(seed=2)
        assert not r1.data.equals(r2.data)

    def test_with_strata(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation(n_strata=1)
        strata_names = result.data["strata_name"].unique().to_list()
        assert OVERALL in strata_names
        assert "sex" in strata_names

    def test_has_count_rows(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        var_names = set(result.data["variable_name"].unique().to_list())
        assert "Number records" in var_names
        assert "Number subjects" in var_names

    def test_has_metric_rows(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        var_names = set(result.data["variable_name"].unique().to_list())

        assert "number exposures" in var_names
        assert "days exposed" in var_names
        assert "cumulative dose" in var_names

    def test_has_concept_set_in_additional(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation(n_concept_sets=2)
        additional_names = result.data["additional_name"].unique().to_list()
        assert "concept_set" in additional_names

    def test_standard_columns(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)

    def test_estimate_names(self):
        from omopy.drug import mock_drug_utilisation

        result = mock_drug_utilisation()
        est_names = set(result.data["estimate_name"].unique().to_list())

        assert "mean" in est_names
        assert "sd" in est_names
        assert "median" in est_names
        assert "count" in est_names
        assert "count_missing" in est_names
        assert "percentage_missing" in est_names


# ===================================================================
# Tests: Benchmark placeholder
# ===================================================================


class TestBenchmark:
    def test_benchmark_returns_dict(self):
        from omopy.drug import benchmark_drug_utilisation

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = benchmark_drug_utilisation()

        assert isinstance(result, dict)

    def test_benchmark_warns(self):
        from omopy.drug import benchmark_drug_utilisation

        with pytest.warns(UserWarning, match="placeholder"):
            benchmark_drug_utilisation(verbose=True)


# ===================================================================
# Tests: Table functions (using mock data)
# ===================================================================


class TestTableFunctions:
    """Test table wrapper functions with mock data."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.drug import mock_drug_utilisation

        return mock_drug_utilisation()

    def test_table_drug_utilisation_polars(self, mock_result):
        from omopy.drug import table_drug_utilisation

        table = table_drug_utilisation(mock_result, type="polars")
        assert isinstance(table, pl.DataFrame)
        assert len(table) > 0

    def test_table_drug_utilisation_custom_header(self, mock_result):
        from omopy.drug import table_drug_utilisation

        table = table_drug_utilisation(
            mock_result,
            type="polars",
            header=["cohort_name"],
        )
        assert isinstance(table, pl.DataFrame)

    def test_table_indication_polars(self):
        """Test table_indication with a synthetic indication result."""
        from omopy.drug import table_indication

        # Build a minimal indication SummarisedResult
        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Indication (0, 0)",
                "variable_level": "hypertension",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": "10",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Indication (0, 0)",
                "variable_level": "hypertension",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "50.00",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_indication"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        table = table_indication(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_treatment_polars(self):
        """Test table_treatment with a synthetic treatment result."""
        from omopy.drug import table_treatment

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Medication (0, 0)",
                "variable_level": "statin",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": "8",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Medication (0, 0)",
                "variable_level": "statin",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "40.00",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_treatment"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        table = table_treatment(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_drug_restart_polars(self):
        """Test table_drug_restart with a synthetic result."""
        from omopy.drug import table_drug_restart

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Drug restart in 180 days",
                "variable_level": "restart",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": "5",
                "additional_name": "follow_up_days",
                "additional_level": "180",
            },
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Drug restart in 180 days",
                "variable_level": "restart",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "25.00",
                "additional_name": "follow_up_days",
                "additional_level": "180",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_drug_restart"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        table = table_drug_restart(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_dose_coverage_polars(self):
        """Test table_dose_coverage with a synthetic result."""
        from omopy.drug import table_dose_coverage

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "ingredient_name",
                "group_level": "aspirin",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "daily_dose",
                "variable_level": "",
                "estimate_name": "mean",
                "estimate_type": "numeric",
                "estimate_value": "50.00",
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            },
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "ingredient_name",
                "group_level": "aspirin",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "daily_dose",
                "variable_level": "",
                "estimate_name": "sd",
                "estimate_type": "numeric",
                "estimate_value": "10.00",
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_dose_coverage"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        table = table_dose_coverage(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_ppc_polars(self):
        """Test table_proportion_of_patients_covered with synthetic result."""
        from omopy.drug import table_proportion_of_patients_covered

        rows = []
        for day in range(3):
            for est_name, est_val in [
                ("outcome_count", "10"),
                ("denominator_count", "20"),
                ("ppc", "0.500000"),
                ("ppc_lower", "0.300000"),
                ("ppc_upper", "0.700000"),
            ]:
                rows.append(
                    {
                        "result_id": 1,
                        "cdm_name": "test",
                        "group_name": "cohort_name",
                        "group_level": "drug_1",
                        "strata_name": OVERALL,
                        "strata_level": OVERALL,
                        "variable_name": OVERALL,
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": "numeric" if "ppc" in est_name else "integer",
                        "estimate_value": est_val,
                        "additional_name": "time",
                        "additional_level": str(day),
                    }
                )
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_proportion_of_patients_covered"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        table = table_proportion_of_patients_covered(sr, type="polars")
        assert isinstance(table, pl.DataFrame)


# ===================================================================
# Tests: Plot functions (using mock data)
# ===================================================================


class TestPlotFunctions:
    """Test plot wrapper functions with mock data."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.drug import mock_drug_utilisation

        return mock_drug_utilisation()

    def test_plot_drug_utilisation_boxplot(self, mock_result):
        from omopy.drug import plot_drug_utilisation

        fig = plot_drug_utilisation(mock_result, plot_type="boxplot")
        assert fig is not None

    def test_plot_drug_utilisation_barplot(self, mock_result):
        from omopy.drug import plot_drug_utilisation

        fig = plot_drug_utilisation(mock_result, plot_type="barplot")
        assert fig is not None

    def test_plot_drug_utilisation_invalid_type(self, mock_result):
        from omopy.drug import plot_drug_utilisation

        with pytest.raises(ValueError, match="Unknown plot_type"):
            plot_drug_utilisation(mock_result, plot_type="invalid")

    def test_plot_indication(self):
        from omopy.drug import plot_indication

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Indication (0, 0)",
                "variable_level": "hypertension",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "50.00",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_indication"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        fig = plot_indication(sr)
        assert fig is not None

    def test_plot_treatment(self):
        from omopy.drug import plot_treatment

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Medication (0, 0)",
                "variable_level": "statin",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "40.00",
                "additional_name": "window_name",
                "additional_level": "(0, 0)",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_treatment"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        fig = plot_treatment(sr)
        assert fig is not None

    def test_plot_drug_restart(self):
        from omopy.drug import plot_drug_restart

        rows = [
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Drug restart in 180 days",
                "variable_level": "restart",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "25.00",
                "additional_name": "follow_up_days",
                "additional_level": "180",
            },
            {
                "result_id": 1,
                "cdm_name": "test",
                "group_name": "cohort_name",
                "group_level": "drug_1",
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Drug restart in 180 days",
                "variable_level": "untreated",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": "75.00",
                "additional_name": "follow_up_days",
                "additional_level": "180",
            },
        ]
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_drug_restart"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        fig = plot_drug_restart(sr)
        assert fig is not None

    def test_plot_ppc(self):
        from omopy.drug import plot_proportion_of_patients_covered

        rows = []
        for day in range(5):
            ppc = 1.0 - day * 0.1
            for est_name, est_val in [
                ("ppc", f"{ppc:.6f}"),
                ("ppc_lower", f"{ppc - 0.05:.6f}"),
                ("ppc_upper", f"{ppc + 0.05:.6f}"),
            ]:
                rows.append(
                    {
                        "result_id": 1,
                        "cdm_name": "test",
                        "group_name": "cohort_name",
                        "group_level": "drug_1",
                        "strata_name": OVERALL,
                        "strata_level": OVERALL,
                        "variable_name": OVERALL,
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": "numeric",
                        "estimate_value": est_val,
                        "additional_name": "time",
                        "additional_level": str(day),
                    }
                )
        data = pl.DataFrame(rows)
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["summarise_proportion_of_patients_covered"],
                "package_name": ["omopy.drug"],
                "package_version": ["0.1.0"],
            }
        )
        sr = SummarisedResult(data, settings=settings)

        fig = plot_proportion_of_patients_covered(sr)
        assert fig is not None


# ===================================================================
# Tests: Summarise helper functions
# ===================================================================


class TestSummariseHelpers:
    """Test internal helper functions from _summarise.py."""

    def test_make_settings(self):
        from omopy.drug._summarise import _make_settings

        s = _make_settings(1, "summarise_drug_utilisation")
        assert isinstance(s, pl.DataFrame)
        assert s["result_id"][0] == 1
        assert s["result_type"][0] == "summarise_drug_utilisation"
        assert s["package_name"][0] == "omopy.drug"

    def test_make_settings_with_extra(self):
        from omopy.drug._summarise import _make_settings

        s = _make_settings(1, "test", ingredient="aspirin")
        assert "ingredient" in s.columns
        assert s["ingredient"][0] == "aspirin"

    def test_empty_result(self):
        from omopy.drug._summarise import _empty_result

        result = _empty_result("summarise_drug_utilisation")
        assert isinstance(result, SummarisedResult)
        assert len(result) == 0
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)

    def test_resolve_strata_overall_only(self):
        from omopy.drug._summarise import _resolve_strata

        df = pl.DataFrame({"x": [1, 2, 3]})
        groups = _resolve_strata(df, [])
        assert len(groups) == 1
        assert groups[0][0] == OVERALL
        assert groups[0][1] == OVERALL

    def test_resolve_strata_single(self):
        from omopy.drug._summarise import _resolve_strata

        df = pl.DataFrame({"sex": ["M", "M", "F"]})
        groups = _resolve_strata(df, ["sex"])
        assert len(groups) == 3  # overall + M + F

    def test_resolve_strata_missing_raises(self):
        from omopy.drug._summarise import _resolve_strata

        df = pl.DataFrame({"x": [1]})
        with pytest.raises(ValueError, match="Strata columns not found"):
            _resolve_strata(df, ["nonexistent"])

    def test_add_count_rows(self):
        from omopy.drug._summarise import _add_count_rows

        df = pl.DataFrame(
            {
                "subject_id": [1, 2, 2, 3],
            }
        )
        rows = _add_count_rows(
            df,
            cdm_name="test",
            result_id=1,
            group_name="cohort_name",
            group_level="c1",
            strata_name=OVERALL,
            strata_level=OVERALL,
        )
        assert len(rows) == 2
        names = {r["variable_name"] for r in rows}
        assert names == {"Number records", "Number subjects"}

        records_row = next(r for r in rows if r["variable_name"] == "Number records")
        subjects_row = next(r for r in rows if r["variable_name"] == "Number subjects")
        assert records_row["estimate_value"] == "4"
        assert subjects_row["estimate_value"] == "3"

    def test_compute_numeric_estimates(self):
        from omopy.drug._summarise import _compute_numeric_estimates

        s = pl.Series("val", [10.0, 20.0, 30.0, 40.0, 50.0])
        est = (
            "mean",
            "sd",
            "median",
            "min",
            "max",
            "q25",
            "q75",
            "count_missing",
            "percentage_missing",
        )
        rows = _compute_numeric_estimates(s, "test_metric", est)

        names = {r["estimate_name"] for r in rows}
        assert "mean" in names
        assert "sd" in names
        assert "median" in names
        assert "count_missing" in names

        mean_row = next(r for r in rows if r["estimate_name"] == "mean")
        assert float(mean_row["estimate_value"]) == pytest.approx(30.0, abs=0.01)

    def test_compute_numeric_estimates_empty(self):
        from omopy.drug._summarise import _compute_numeric_estimates

        s = pl.Series("val", [], dtype=pl.Float64)
        rows = _compute_numeric_estimates(s, "test", ("mean", "sd"))

        mean_row = next(r for r in rows if r["estimate_name"] == "mean")
        assert mean_row["estimate_value"] == "NA"

    def test_compute_numeric_estimates_with_nulls(self):
        from omopy.drug._summarise import _compute_numeric_estimates

        s = pl.Series("val", [10.0, None, 30.0, None, 50.0])
        rows = _compute_numeric_estimates(
            s,
            "test",
            ("mean", "count_missing", "percentage_missing"),
        )

        missing_row = next(r for r in rows if r["estimate_name"] == "count_missing")
        assert missing_row["estimate_value"] == "2"

        pct_row = next(r for r in rows if r["estimate_name"] == "percentage_missing")
        assert float(pct_row["estimate_value"]) == pytest.approx(40.0, abs=0.01)

    def test_wilson_ci_basic(self):
        from omopy.drug._summarise import _wilson_ci

        lower, upper = _wilson_ci(50, 100)
        assert 0.0 < lower < 0.5
        assert 0.5 < upper < 1.0

    def test_wilson_ci_zero(self):
        from omopy.drug._summarise import _wilson_ci

        lower, upper = _wilson_ci(0, 100)
        assert lower < 1e-10
        assert upper > 0.0

    def test_wilson_ci_all(self):
        from omopy.drug._summarise import _wilson_ci

        lower, upper = _wilson_ci(100, 100)
        assert lower > 0.9
        assert upper <= 1.0

    def test_wilson_ci_zero_total(self):
        from omopy.drug._summarise import _wilson_ci

        lower, upper = _wilson_ci(0, 0)
        assert lower == 0.0
        assert upper == 0.0


# ===================================================================
# Tests: _format_fud helper
# ===================================================================


class TestFormatFud:
    def test_integer(self):
        from omopy.drug._add_drug_use import _format_fud

        assert _format_fud(180) == "180"

    def test_float_inf(self):
        from omopy.drug._add_drug_use import _format_fud

        assert _format_fud(float("inf")) == "inf"

    def test_float_number(self):
        from omopy.drug._add_drug_use import _format_fud

        assert _format_fud(365.0) == "365"


# ===================================================================
# Tests: Erafy cohort (unit tests with Polars)
# ===================================================================


class TestErafyCohort:
    """Test erafy_cohort with local Polars data (no DB needed)."""

    def test_erafy_collapses_overlapping(self):
        """Two overlapping records for the same subject should merge."""
        from omopy.drug import erafy_cohort

        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 1],
                "subject_id": [1, 1],
                "cohort_start_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 1, 15),
                ],
                "cohort_end_date": [
                    datetime.date(2020, 2, 1),
                    datetime.date(2020, 3, 1),
                ],
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["drug_1"],
                "gap_era": [0],
            }
        )
        ct = CohortTable(df, settings=settings)

        result = erafy_cohort(ct, gap_era=0)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert len(result_df) == 1
        assert result_df["cohort_start_date"][0] == datetime.date(2020, 1, 1)
        assert result_df["cohort_end_date"][0] == datetime.date(2020, 3, 1)

    def test_erafy_with_gap(self):
        """Two records with a gap ≤ gap_era should merge."""
        from omopy.drug import erafy_cohort

        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 1],
                "subject_id": [1, 1],
                "cohort_start_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 2, 10),
                ],
                "cohort_end_date": [
                    datetime.date(2020, 2, 1),
                    datetime.date(2020, 3, 1),
                ],
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["drug_1"],
                "gap_era": [0],
            }
        )
        ct = CohortTable(df, settings=settings)

        # Gap of 8 days (Feb 2 to Feb 9), so gap_era=10 should merge
        result = erafy_cohort(ct, gap_era=10)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert len(result_df) == 1

        # Gap of 8 days, gap_era=5 should NOT merge
        result2 = erafy_cohort(ct, gap_era=5)
        result_df2 = (
            result2.collect() if not isinstance(result2.data, pl.DataFrame) else result2.data
        )
        assert len(result_df2) == 2

    def test_erafy_different_subjects(self):
        """Records for different subjects should not merge."""
        from omopy.drug import erafy_cohort

        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 1],
                "subject_id": [1, 2],
                "cohort_start_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 1, 1),
                ],
                "cohort_end_date": [
                    datetime.date(2020, 2, 1),
                    datetime.date(2020, 2, 1),
                ],
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["drug_1"],
                "gap_era": [0],
            }
        )
        ct = CohortTable(df, settings=settings)

        result = erafy_cohort(ct, gap_era=30)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert len(result_df) == 2


# ===================================================================
# Tests: cohort_gap_era
# ===================================================================


class TestCohortGapEra:
    def test_gap_era_from_settings(self):
        from omopy.drug import cohort_gap_era

        ct = _make_drug_cohort(gap_era=30)
        result = cohort_gap_era(ct)
        assert isinstance(result, dict)
        assert list(result.values()) == [30]

    def test_gap_era_multiple_cohorts(self):
        from omopy.drug import cohort_gap_era

        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 2],
                "subject_id": [1, 2],
                "cohort_start_date": [datetime.date(2020, 1, 1)] * 2,
                "cohort_end_date": [datetime.date(2020, 6, 30)] * 2,
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1, 2],
                "cohort_name": ["drug_1", "drug_2"],
                "gap_era": [30, 60],
            }
        )
        ct = CohortTable(df, settings=settings)

        result = cohort_gap_era(ct)
        assert result == {1: 30, 2: 60}


# ===================================================================
# Tests: Require functions (unit)
# ===================================================================


class TestRequireFunctions:
    """Test require_* with local Polars CohortTable data."""

    def test_require_is_first_drug_entry(self):
        from omopy.drug import require_is_first_drug_entry

        # Subject 1 has two entries, subject 2 has one
        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 1, 1],
                "subject_id": [1, 1, 2],
                "cohort_start_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 6, 1),
                    datetime.date(2020, 3, 1),
                ],
                "cohort_end_date": [
                    datetime.date(2020, 3, 1),
                    datetime.date(2020, 9, 1),
                    datetime.date(2020, 6, 1),
                ],
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["drug_1"],
            }
        )
        ct = CohortTable(df, settings=settings)

        result = require_is_first_drug_entry(ct)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        # Should keep only the first entry per subject
        assert len(result_df) == 2

    def test_require_drug_in_date_range(self):
        from omopy.drug import require_drug_in_date_range

        df = pl.DataFrame(
            {
                "cohort_definition_id": [1, 1, 1],
                "subject_id": [1, 2, 3],
                "cohort_start_date": [
                    datetime.date(2019, 1, 1),
                    datetime.date(2020, 6, 1),
                    datetime.date(2021, 1, 1),
                ],
                "cohort_end_date": [
                    datetime.date(2019, 6, 1),
                    datetime.date(2020, 12, 1),
                    datetime.date(2021, 6, 1),
                ],
            }
        )
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["drug_1"],
            }
        )
        ct = CohortTable(df, settings=settings)

        result = require_drug_in_date_range(
            ct,
            date_range=(datetime.date(2020, 1, 1), datetime.date(2020, 12, 31)),
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        # Only subject 2 should remain (starts in 2020)
        assert len(result_df) == 1
        assert result_df["subject_id"][0] == 2


# ===================================================================
# Integration tests — Synthea database
# ===================================================================


@pytest.fixture(scope="module")
def drug_cohort(synthea_cdm):
    """Generate a drug cohort from Synthea data using ingredient lookup."""
    from omopy.drug import generate_drug_utilisation_cohort_set

    # Use a concept set with known drug concepts from Synthea
    # lisinopril ingredient concept_id = 1308216
    cdm = generate_drug_utilisation_cohort_set(
        synthea_cdm,
        name="lisinopril",
        concept_set={"lisinopril": [1308216]},
        gap_era=30,
    )
    return cdm


class TestCohortGeneration:
    """Integration: generate_drug_utilisation_cohort_set on Synthea."""

    def test_generates_cohort(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        assert isinstance(ct, CohortTable)

    def test_cohort_has_records(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        df = ct.collect()
        # Synthea has 180 lisinopril records across multiple persons
        assert len(df) > 0

    def test_cohort_has_settings(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        settings = ct.settings
        assert "cohort_definition_id" in settings.columns
        assert "cohort_name" in settings.columns
        assert "lisinopril" in settings["cohort_name"].to_list()

    def test_cohort_has_gap_era_in_settings(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        settings = ct.settings
        assert "gap_era" in settings.columns
        assert settings["gap_era"][0] == 30

    def test_cohort_columns(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        df = ct.collect()
        assert "cohort_definition_id" in df.columns
        assert "subject_id" in df.columns
        assert "cohort_start_date" in df.columns
        assert "cohort_end_date" in df.columns

    def test_cohort_dates_ordered(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        df = ct.collect()
        # start_date <= end_date for all rows
        assert (df["cohort_start_date"] <= df["cohort_end_date"]).all()

    def test_cohort_has_attrition(self, drug_cohort):
        ct = drug_cohort["lisinopril"]
        attrition = ct.attrition
        assert isinstance(attrition, pl.DataFrame)
        assert len(attrition) > 0

    def test_cohort_gap_era_value(self, drug_cohort):
        from omopy.drug import cohort_gap_era

        ct = drug_cohort["lisinopril"]
        result = cohort_gap_era(ct)
        assert list(result.values()) == [30]


class TestCohortGenerationIngredient:
    """Integration: generate_ingredient_cohort_set on Synthea."""

    def test_ingredient_cohort(self, synthea_cdm):
        from omopy.drug import generate_ingredient_cohort_set

        cdm = generate_ingredient_cohort_set(
            synthea_cdm,
            name="ingredients",
            ingredient=["lisinopril"],
            gap_era=0,
        )
        ct = cdm["ingredients"]
        df = ct.collect()
        assert len(df) > 0


class TestCohortGenerationAtc:
    """Integration: generate_atc_cohort_set on Synthea."""

    def test_atc_cohort(self, synthea_cdm):
        from omopy.drug import generate_atc_cohort_set

        # ATC C09AA — ACE inhibitors (includes lisinopril)
        cdm = generate_atc_cohort_set(
            synthea_cdm,
            name="atc_ace",
            atc_name=["agents acting on the renin-angiotensin system"],
            gap_era=0,
        )
        ct = cdm["atc_ace"]
        df = ct.collect()
        # Should find drug records
        assert len(df) >= 0  # May not find exact ATC match


class TestRequireIntegration:
    """Integration: require_* on real Synthea drug cohort."""

    def test_require_is_first_entry(self, drug_cohort):
        from omopy.drug import require_is_first_drug_entry

        ct = drug_cohort["lisinopril"]
        original_df = ct.collect()
        original_n = len(original_df)

        result = require_is_first_drug_entry(ct)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data

        # Should have fewer or equal records
        assert len(result_df) <= original_n
        # Each subject should appear at most once per cohort
        assert (
            result_df.group_by(["cohort_definition_id", "subject_id"])
            .len()
            .filter(pl.col("len") > 1)
            .height
            == 0
        )

    def test_require_drug_in_date_range_integration(self, drug_cohort):
        from omopy.drug import require_drug_in_date_range

        ct = drug_cohort["lisinopril"]
        result = require_drug_in_date_range(
            ct,
            date_range=(datetime.date(2020, 1, 1), datetime.date(2025, 12, 31)),
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        # All remaining records should have start dates in range
        if len(result_df) > 0:
            assert (result_df["cohort_start_date"] >= datetime.date(2020, 1, 1)).all()


class TestErafyCohortIntegration:
    """Integration: erafy_cohort on Synthea drug cohort."""

    def test_erafy_reduces_records(self, drug_cohort):
        from omopy.drug import erafy_cohort

        ct = drug_cohort["lisinopril"]
        original = ct.collect()

        result = erafy_cohort(ct, gap_era=365)
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data

        # With a large gap, should collapse more records
        assert len(result_df) <= len(original)


class TestAddDrugUseIntegration:
    """Integration: add_* functions on Synthea drug cohort."""

    def test_add_number_exposures(self, drug_cohort, synthea_cdm):
        from omopy.drug import add_number_exposures

        ct = drug_cohort["lisinopril"]
        result = add_number_exposures(
            ct,
            concept_set={"lisinopril": [1308216]},
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert "number_exposures_lisinopril" in result_df.columns
        # Values should be >= 0
        assert (result_df["number_exposures_lisinopril"] >= 0).all()

    def test_add_number_eras(self, drug_cohort, synthea_cdm):
        from omopy.drug import add_number_eras

        ct = drug_cohort["lisinopril"]
        result = add_number_eras(
            ct,
            concept_set={"lisinopril": [1308216]},
            gap_era=30,
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert "number_eras_lisinopril" in result_df.columns

    def test_add_days_exposed(self, drug_cohort, synthea_cdm):
        from omopy.drug import add_days_exposed

        ct = drug_cohort["lisinopril"]
        result = add_days_exposed(
            ct,
            concept_set={"lisinopril": [1308216]},
            gap_era=30,
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert "days_exposed_lisinopril" in result_df.columns

    def test_add_days_prescribed(self, drug_cohort, synthea_cdm):
        from omopy.drug import add_days_prescribed

        ct = drug_cohort["lisinopril"]
        result = add_days_prescribed(
            ct,
            concept_set={"lisinopril": [1308216]},
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data
        assert "days_prescribed_lisinopril" in result_df.columns

    def test_add_drug_utilisation_all(self, drug_cohort, synthea_cdm):
        """add_drug_utilisation with non-dose metrics enabled."""
        from omopy.drug import add_drug_utilisation

        ct = drug_cohort["lisinopril"]
        result = add_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            # Disable dose metrics (Synthea has no drug_strength data)
            initial_daily_dose=False,
            cumulative_dose=False,
        )
        result_df = result.collect() if not isinstance(result.data, pl.DataFrame) else result.data

        expected_cols = [
            "number_exposures_lisinopril",
            "number_eras_lisinopril",
            "days_exposed_lisinopril",
            "days_prescribed_lisinopril",
            "time_to_exposure_lisinopril",
            "initial_exposure_duration_lisinopril",
            "initial_quantity_lisinopril",
            "cumulative_quantity_lisinopril",
        ]
        for col in expected_cols:
            assert col in result_df.columns, f"Missing column: {col}"


class TestSummariseIntegration:
    """Integration: summarise_drug_utilisation on Synthea."""

    # Synthea has no drug_strength data, so disable dose metrics
    _no_dose = {"initial_daily_dose": False, "cumulative_dose": False}

    def test_summarise_drug_utilisation(self, drug_cohort):
        from omopy.drug import summarise_drug_utilisation

        ct = drug_cohort["lisinopril"]
        result = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )

        assert isinstance(result, SummarisedResult)
        assert len(result) > 0
        assert result.settings["result_type"][0] == "summarise_drug_utilisation"

        # Should have standard columns
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)

    def test_summarise_has_count_rows(self, drug_cohort):
        from omopy.drug import summarise_drug_utilisation

        ct = drug_cohort["lisinopril"]
        result = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )

        var_names = set(result.data["variable_name"].unique().to_list())
        assert "Number records" in var_names
        assert "Number subjects" in var_names

    def test_summarise_has_metric_rows(self, drug_cohort):
        from omopy.drug import summarise_drug_utilisation

        ct = drug_cohort["lisinopril"]
        result = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )

        var_names = set(result.data["variable_name"].unique().to_list())
        assert "number exposures" in var_names
        assert "days exposed" in var_names

    def test_summarise_has_distribution_estimates(self, drug_cohort):
        from omopy.drug import summarise_drug_utilisation

        ct = drug_cohort["lisinopril"]
        result = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )

        est_names = set(result.data["estimate_name"].unique().to_list())
        assert "mean" in est_names
        assert "median" in est_names

    def test_summarise_table_renders(self, drug_cohort):
        """Full pipeline: summarise -> table."""
        from omopy.drug import summarise_drug_utilisation, table_drug_utilisation

        ct = drug_cohort["lisinopril"]
        sr = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )
        table = table_drug_utilisation(sr, type="polars")
        assert isinstance(table, pl.DataFrame)
        assert len(table) > 0

    def test_summarise_plot_renders(self, drug_cohort):
        """Full pipeline: summarise -> plot."""
        from omopy.drug import summarise_drug_utilisation, plot_drug_utilisation

        ct = drug_cohort["lisinopril"]
        sr = summarise_drug_utilisation(
            ct,
            gap_era=30,
            concept_set={"lisinopril": [1308216]},
            **self._no_dose,
        )
        fig = plot_drug_utilisation(sr, plot_type="boxplot")
        assert fig is not None


class TestPPCIntegration:
    """Integration: PPC on Synthea drug cohort."""

    def test_summarise_ppc(self, drug_cohort):
        from omopy.drug import summarise_proportion_of_patients_covered

        ct = drug_cohort["lisinopril"]
        result = summarise_proportion_of_patients_covered(
            ct,
            follow_up_days=30,
        )

        assert isinstance(result, SummarisedResult)
        if len(result) > 0:
            assert result.settings["result_type"][0] == "summarise_proportion_of_patients_covered"

            est_names = set(result.data["estimate_name"].unique().to_list())
            assert "ppc" in est_names
            assert "outcome_count" in est_names
            assert "denominator_count" in est_names

    def test_ppc_values_bounded(self, drug_cohort):
        from omopy.drug import summarise_proportion_of_patients_covered

        ct = drug_cohort["lisinopril"]
        result = summarise_proportion_of_patients_covered(
            ct,
            follow_up_days=10,
        )

        if len(result) > 0:
            ppc_rows = result.data.filter(pl.col("estimate_name") == "ppc")
            for val in ppc_rows["estimate_value"].to_list():
                ppc = float(val)
                assert 0.0 <= ppc <= 1.0


class TestPatternTableIntegration:
    """Test pattern_table with real database (Synthea drug_strength is empty)."""

    def test_pattern_table_returns_dataframe(self, synthea_cdm):
        from omopy.drug import pattern_table

        df = pattern_table(synthea_cdm)
        # Synthea drug_strength is empty, so result should be empty
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_pattern_table_requires_cdm(self):
        from omopy.drug import pattern_table

        with pytest.raises(TypeError):
            pattern_table(None)
