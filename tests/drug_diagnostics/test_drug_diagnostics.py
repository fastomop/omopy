"""Unit tests for omopy.drug_diagnostics — mock-based, no database required."""

from __future__ import annotations

import polars as pl
import pytest

from omopy.drug_diagnostics import (
    AVAILABLE_CHECKS,
    DiagnosticsResult,
    mock_drug_exposure,
    summarise_drug_diagnostics,
)
from omopy.drug_diagnostics._checks import (
    _MISSING_COLUMNS,
    _QUANTILE_NAMES,
    _check_days_between,
    _check_days_supply,
    _check_dose_from_records,
    _check_exposure_duration,
    _check_missing,
    _check_quantity,
    _check_route,
    _check_sig,
    _check_source_concept,
    _check_type,
    _check_verbatim_end_date,
    _check_diagnostics_summary,
    _quantile_stats,
    _obscure_df,
)
from omopy.generics import SummarisedResult


# =====================================================================
# Test AVAILABLE_CHECKS constant
# =====================================================================


class TestAvailableChecks:
    """Tests for the AVAILABLE_CHECKS constant."""

    def test_is_tuple(self):
        assert isinstance(AVAILABLE_CHECKS, tuple)

    def test_has_12_checks(self):
        assert len(AVAILABLE_CHECKS) == 12

    def test_expected_checks_present(self):
        expected = {
            "missing",
            "exposure_duration",
            "type",
            "route",
            "source_concept",
            "days_supply",
            "verbatim_end_date",
            "dose",
            "sig",
            "quantity",
            "days_between",
            "diagnostics_summary",
        }
        assert set(AVAILABLE_CHECKS) == expected

    def test_all_strings(self):
        for c in AVAILABLE_CHECKS:
            assert isinstance(c, str)


# =====================================================================
# Test DiagnosticsResult model
# =====================================================================


class TestDiagnosticsResult:
    """Tests for the DiagnosticsResult Pydantic model."""

    def test_basic_construction(self):
        result = DiagnosticsResult(
            results={"missing": pl.DataFrame({"a": [1]})},
            checks_performed=("missing",),
            ingredient_concepts={1: "Test"},
        )
        assert "missing" in result
        assert result["missing"].height == 1

    def test_dict_like_access(self):
        df = pl.DataFrame({"x": [1, 2, 3]})
        result = DiagnosticsResult(
            results={"test": df},
            checks_performed=("test",),
            ingredient_concepts={1: "Test"},
        )
        assert "test" in result
        assert list(result.keys()) == ["test"]
        assert len(list(result.values())) == 1
        assert len(list(result.items())) == 1

    def test_repr(self):
        result = DiagnosticsResult(
            results={"a": pl.DataFrame({"x": [1]}), "b": pl.DataFrame({"y": [1, 2]})},
            checks_performed=("a", "b"),
            ingredient_concepts={1: "Test"},
            execution_time_seconds=1.234,
        )
        r = repr(result)
        assert "checks=2" in r
        assert "ingredients=1" in r
        assert "total_rows=3" in r
        assert "1.2s" in r

    def test_validates_results_type(self):
        with pytest.raises(Exception):
            DiagnosticsResult(
                results={"bad": "not_a_dataframe"},  # type: ignore[arg-type]
                checks_performed=("bad",),
                ingredient_concepts={1: "Test"},
            )


# =====================================================================
# Test _quantile_stats helper
# =====================================================================


class TestQuantileStats:
    """Tests for the _quantile_stats helper."""

    def test_basic_stats(self):
        s = pl.Series("x", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        stats = _quantile_stats(s)
        assert stats["median"] == pytest.approx(5.5, abs=0.5)
        assert stats["mean"] == pytest.approx(5.5, abs=0.01)
        assert stats["count"] == 10
        assert stats["count_missing"] == 0
        assert stats["min"] == pytest.approx(1.0)
        assert stats["max"] == pytest.approx(10.0)

    def test_all_nulls(self):
        s = pl.Series("x", [None, None, None], dtype=pl.Float64)
        stats = _quantile_stats(s)
        assert stats["median"] is None
        assert stats["mean"] is None
        assert stats["count"] == 0
        assert stats["count_missing"] == 3

    def test_empty_series(self):
        s = pl.Series("x", [], dtype=pl.Float64)
        stats = _quantile_stats(s)
        assert stats["count"] == 0
        assert stats["count_missing"] == 0

    def test_with_prefix(self):
        s = pl.Series("x", [1.0, 2.0, 3.0])
        stats = _quantile_stats(s, name_prefix="dur")
        assert "dur_median" in stats
        assert "dur_mean" in stats
        assert "dur_count" in stats

    def test_single_value(self):
        s = pl.Series("x", [42.0])
        stats = _quantile_stats(s)
        assert stats["median"] == pytest.approx(42.0)
        assert stats["mean"] == pytest.approx(42.0)
        assert stats["sd"] == 0.0
        assert stats["count"] == 1


# =====================================================================
# Test _obscure_df helper
# =====================================================================


class TestObscureDf:
    """Tests for the _obscure_df helper."""

    def test_obscures_small_counts(self):
        df = pl.DataFrame(
            {
                "name": ["a", "b", "c"],
                "count": [3, 10, 1],
            }
        )
        result = _obscure_df(df, min_cell_count=5, count_columns=["count"])
        assert result["count"][0] is None  # 3 < 5
        assert result["count"][1] == 10
        assert result["count"][2] is None  # 1 < 5
        assert result["result_obscured"][0] == True  # noqa: E712
        assert result["result_obscured"][1] == False  # noqa: E712

    def test_no_obscuration_when_disabled(self):
        df = pl.DataFrame({"count": [1, 2, 3]})
        result = _obscure_df(df, min_cell_count=0, count_columns=["count"])
        assert result["count"].to_list() == [1, 2, 3]
        assert result["result_obscured"].to_list() == [False, False, False]

    def test_skips_missing_columns(self):
        df = pl.DataFrame({"a": [1, 2]})
        result = _obscure_df(df, min_cell_count=5, count_columns=["nonexistent"])
        assert "result_obscured" in result.columns


# =====================================================================
# Test individual check implementations
# =====================================================================


class TestCheckMissing:
    """Tests for _check_missing."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "drug_exposure_id": [1, 2, 3],
                "person_id": [10, 20, 30],
                "drug_concept_id": [100, 200, 300],
                "drug_exposure_start_date": [None, "2020-01-01", "2020-02-01"],
                "drug_exposure_end_date": ["2020-01-10", None, "2020-02-10"],
                "drug_type_concept_id": [32817, 32817, 32817],
                "stop_reason": [None, None, None],
                "refills": [None, None, None],
                "quantity": [10.0, None, 30.0],
                "days_supply": [10, 20, None],
                "sig": [None, None, None],
                "route_concept_id": [0, 0, 0],
                "route_source_value": [None, None, None],
                "dose_unit_source_value": [None, None, None],
                "verbatim_end_date": [None, None, "2020-02-10"],
            }
        )
        result = _check_missing(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == len(_MISSING_COLUMNS)
        assert "proportion_missing" in result.columns
        assert all(result["n_records"] == 3)

        # Check specific columns
        start_row = result.filter(pl.col("variable") == "drug_exposure_start_date")
        assert start_row["n_missing"][0] == 1

    def test_empty_df(self):
        df = pl.DataFrame(
            schema={
                "drug_exposure_id": pl.Int64,
                "person_id": pl.Int64,
            }
        )
        result = _check_missing(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 0


class TestCheckExposureDuration:
    """Tests for _check_exposure_duration."""

    def test_basic(self):
        import datetime as dt

        df = pl.DataFrame(
            {
                "drug_exposure_start_date": [
                    dt.date(2020, 1, 1),
                    dt.date(2020, 2, 1),
                    dt.date(2020, 3, 1),
                ],
                "drug_exposure_end_date": [
                    dt.date(2020, 1, 10),
                    dt.date(2020, 2, 28),
                    dt.date(2020, 3, 5),
                ],
            }
        )
        result = _check_exposure_duration(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert result["n_records"][0] == 3
        assert result["n_negative_duration"][0] == 0
        assert result["duration_count"][0] == 3

    def test_negative_duration(self):
        import datetime as dt

        df = pl.DataFrame(
            {
                "drug_exposure_start_date": [dt.date(2020, 1, 10), dt.date(2020, 2, 1)],
                "drug_exposure_end_date": [dt.date(2020, 1, 1), dt.date(2020, 2, 28)],
            }
        )
        result = _check_exposure_duration(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result["n_negative_duration"][0] == 1

    def test_empty_df(self):
        df = pl.DataFrame(schema={"drug_exposure_start_date": pl.Date})
        result = _check_exposure_duration(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 0


class TestCheckType:
    """Tests for _check_type."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "drug_type_concept_id": [32817, 32817, 32818],
            }
        )
        concept_df = pl.DataFrame(
            {
                "concept_id": [32817, 32818],
                "concept_name": ["EHR", "EHR administration"],
            }
        )
        result = _check_type(df, concept_df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 2
        assert "drug_type" in result.columns
        assert result["count"].sum() == 3

    def test_no_concept_df(self):
        df = pl.DataFrame({"drug_type_concept_id": [32817, 32817]})
        result = _check_type(df, None, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert result["drug_type"][0] == "Unknown"


class TestCheckRoute:
    """Tests for _check_route."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "route_concept_id": [4128794, 4128794, 0],
            }
        )
        concept_df = pl.DataFrame(
            {
                "concept_id": [4128794, 0],
                "concept_name": ["Oral", "No matching concept"],
            }
        )
        result = _check_route(df, concept_df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 2


class TestCheckSourceConcept:
    """Tests for _check_source_concept."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "drug_concept_id": [100, 100, 200],
                "drug_source_concept_id": [1001, 1001, 1002],
                "drug_source_value": ["tab100mg", "tab100mg", "cap200mg"],
            }
        )
        result = _check_source_concept(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 2
        assert "drug_source_value" in result.columns


class TestCheckDaysSupply:
    """Tests for _check_days_supply."""

    def test_basic(self):
        import datetime as dt

        df = pl.DataFrame(
            {
                "drug_exposure_start_date": [dt.date(2020, 1, 1), dt.date(2020, 2, 1)],
                "drug_exposure_end_date": [dt.date(2020, 1, 10), dt.date(2020, 2, 28)],
                "days_supply": [10, 28],
            }
        )
        result = _check_days_supply(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert "days_supply_median" in result.columns
        assert result["n_records"][0] == 2
        assert result["n_days_supply_match_date_diff"][0] == 2  # both match


class TestCheckVerbatimEndDate:
    """Tests for _check_verbatim_end_date."""

    def test_basic(self):
        import datetime as dt

        df = pl.DataFrame(
            {
                "drug_exposure_end_date": [
                    dt.date(2020, 1, 10),
                    dt.date(2020, 2, 28),
                    dt.date(2020, 3, 5),
                ],
                "verbatim_end_date": [dt.date(2020, 1, 10), None, dt.date(2020, 3, 1)],
            }
        )
        result = _check_verbatim_end_date(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert result["n_verbatim_end_date_missing"][0] == 1
        assert result["n_verbatim_end_date_equal"][0] == 1
        assert result["n_verbatim_end_date_differ"][0] == 1


class TestCheckDoseFromRecords:
    """Tests for _check_dose_from_records."""

    def test_with_matching_strength(self):
        df = pl.DataFrame(
            {
                "drug_concept_id": [100, 200, 300],
            }
        )
        strength = pl.DataFrame(
            {
                "drug_concept_id": [100, 200],
            }
        )
        result = _check_dose_from_records(
            df,
            strength,
            ingredient_concept_id=1,
            ingredient_name="Test",
        )
        assert result["n_with_dose"][0] == 2
        assert result["n_without_dose"][0] == 1

    def test_no_strength_table(self):
        df = pl.DataFrame({"drug_concept_id": [100, 200]})
        result = _check_dose_from_records(
            df,
            None,
            ingredient_concept_id=1,
            ingredient_name="Test",
        )
        assert result["n_with_dose"][0] == 0
        assert result["n_without_dose"][0] == 2


class TestCheckSig:
    """Tests for _check_sig."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "sig": ["Take 1 daily", None, "Take 1 daily"],
            }
        )
        result = _check_sig(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 2  # "Take 1 daily" and "<missing>"
        assert result["count"].sum() == 3


class TestCheckQuantity:
    """Tests for _check_quantity."""

    def test_basic(self):
        df = pl.DataFrame(
            {
                "quantity": [10.0, 20.0, 30.0, None],
            }
        )
        result = _check_quantity(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert result["quantity_count"][0] == 3
        assert result["quantity_count_missing"][0] == 1

    def test_all_null(self):
        df = pl.DataFrame(
            {
                "quantity": [None, None, None],
            },
            schema={"quantity": pl.Float64},
        )
        result = _check_quantity(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result["quantity_count"][0] == 0
        assert result["quantity_count_missing"][0] == 3


class TestCheckDaysBetween:
    """Tests for _check_days_between."""

    def test_basic(self):
        import datetime as dt

        df = pl.DataFrame(
            {
                "person_id": [1, 1, 1, 2, 2],
                "drug_exposure_start_date": [
                    dt.date(2020, 1, 1),
                    dt.date(2020, 2, 1),
                    dt.date(2020, 4, 1),
                    dt.date(2020, 3, 1),
                    dt.date(2020, 6, 1),
                ],
            }
        )
        result = _check_days_between(df, ingredient_concept_id=1, ingredient_name="Test")
        assert result.height == 1
        assert result["n_persons"][0] == 2
        assert result["n_persons_multiple_records"][0] == 2
        assert result["days_between_count"][0] == 3  # 3 gaps total


class TestCheckDiagnosticsSummary:
    """Tests for _check_diagnostics_summary."""

    def test_basic(self):
        check_results = {
            "missing": pl.DataFrame(
                {
                    "proportion_missing": [0.1, 0.2, 0.3],
                }
            ),
            "exposure_duration": pl.DataFrame(
                {
                    "duration_median": [30.0],
                    "n_negative_duration": [2],
                }
            ),
        }
        result = _check_diagnostics_summary(
            check_results,
            ingredient_concept_id=1,
            ingredient_name="Test",
            n_records=100,
            n_sample=100,
            n_persons=50,
        )
        assert result.height == 1
        assert result["n_records"][0] == 100
        assert result["mean_proportion_missing"][0] == pytest.approx(0.2, abs=0.01)
        assert result["median_duration_days"][0] == 30.0


# =====================================================================
# Test mock_drug_exposure
# =====================================================================


class TestMockDrugExposure:
    """Tests for mock_drug_exposure."""

    def test_basic(self):
        result = mock_drug_exposure()
        assert isinstance(result, DiagnosticsResult)
        assert len(result.checks_performed) == 12
        assert len(result.ingredient_concepts) == 2

    def test_all_checks_present(self):
        result = mock_drug_exposure()
        for check in AVAILABLE_CHECKS:
            assert check in result, f"Check '{check}' missing from mock result"
            assert isinstance(result[check], pl.DataFrame)

    def test_custom_params(self):
        result = mock_drug_exposure(
            n_ingredients=3,
            n_records_per_ingredient=50,
            seed=123,
            include_checks=["missing", "type"],
        )
        assert len(result.ingredient_concepts) == 3
        assert len(result.checks_performed) == 2
        assert "missing" in result
        assert "type" in result
        assert "route" not in result

    def test_reproducibility(self):
        r1 = mock_drug_exposure(seed=42)
        r2 = mock_drug_exposure(seed=42)
        for check in r1.keys():
            assert r1[check].equals(r2[check]), f"Check '{check}' not reproducible"

    def test_missing_check_structure(self):
        result = mock_drug_exposure(include_checks=["missing"])
        df = result["missing"]
        assert "variable" in df.columns
        assert "n_missing" in df.columns
        assert "proportion_missing" in df.columns
        assert df.height > 0

    def test_exposure_duration_structure(self):
        result = mock_drug_exposure(include_checks=["exposure_duration"])
        df = result["exposure_duration"]
        assert "duration_median" in df.columns
        assert "n_negative_duration" in df.columns

    def test_type_check_structure(self):
        result = mock_drug_exposure(include_checks=["type"])
        df = result["type"]
        assert "drug_type_concept_id" in df.columns
        assert "drug_type" in df.columns
        assert "count" in df.columns

    def test_diagnostics_summary_structure(self):
        result = mock_drug_exposure(include_checks=["diagnostics_summary"])
        df = result["diagnostics_summary"]
        assert "n_records" in df.columns
        assert "n_persons" in df.columns


# =====================================================================
# Test summarise_drug_diagnostics
# =====================================================================


class TestSummariseDrugDiagnostics:
    """Tests for summarise_drug_diagnostics."""

    def test_basic(self):
        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        assert isinstance(sr, SummarisedResult)
        assert sr.data.height > 0
        assert sr.settings.height > 0

    def test_settings_have_result_types(self):
        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        result_types = sr.settings["result_type"].to_list()
        for check in AVAILABLE_CHECKS:
            expected = f"drug_diagnostics_{check}"
            assert expected in result_types, f"Missing result_type: {expected}"

    def test_all_13_columns(self):
        from omopy.generics.summarised_result import SUMMARISED_RESULT_COLUMNS

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in sr.data.columns, f"Missing column: {col}"

    def test_single_check(self):
        diag = mock_drug_exposure(include_checks=["missing"])
        sr = summarise_drug_diagnostics(diag)
        assert sr.settings.height == 1
        assert sr.settings["result_type"][0] == "drug_diagnostics_missing"

    def test_empty_result(self):
        diag = DiagnosticsResult(
            results={"missing": pl.DataFrame(schema={"x": pl.Int64})},
            checks_performed=("missing",),
            ingredient_concepts={1: "Test"},
        )
        # Should not raise
        sr = summarise_drug_diagnostics(diag)
        assert isinstance(sr, SummarisedResult)

    def test_validates_input_type(self):
        with pytest.raises(TypeError, match="Expected DiagnosticsResult"):
            summarise_drug_diagnostics("not_a_result")  # type: ignore[arg-type]


# =====================================================================
# Test table_drug_diagnostics
# =====================================================================


class TestTableDrugDiagnostics:
    """Tests for table_drug_diagnostics."""

    def test_returns_polars_by_default(self):
        from omopy.drug_diagnostics import table_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        table = table_drug_diagnostics(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_filter_by_check(self):
        from omopy.drug_diagnostics import table_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        table = table_drug_diagnostics(sr, check="missing", type="polars")
        assert isinstance(table, pl.DataFrame)


# =====================================================================
# Test plot_drug_diagnostics
# =====================================================================


class TestPlotDrugDiagnostics:
    """Tests for plot_drug_diagnostics."""

    def test_missing_plot(self):
        from omopy.drug_diagnostics import plot_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        fig = plot_drug_diagnostics(sr, check="missing")
        assert hasattr(fig, "update_layout")  # plotly Figure

    def test_categorical_plot(self):
        from omopy.drug_diagnostics import plot_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        fig = plot_drug_diagnostics(sr, check="type")
        assert hasattr(fig, "update_layout")

    def test_quantile_plot(self):
        from omopy.drug_diagnostics import plot_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        fig = plot_drug_diagnostics(sr, check="exposure_duration")
        assert hasattr(fig, "update_layout")

    def test_invalid_check_raises(self):
        from omopy.drug_diagnostics import plot_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        with pytest.raises(ValueError, match="Cannot plot check"):
            plot_drug_diagnostics(sr, check="nonexistent")

    def test_custom_title(self):
        from omopy.drug_diagnostics import plot_drug_diagnostics

        diag = mock_drug_exposure()
        sr = summarise_drug_diagnostics(diag)
        fig = plot_drug_diagnostics(sr, check="missing", title="Custom Title")
        assert fig.layout.title.text == "Custom Title"
