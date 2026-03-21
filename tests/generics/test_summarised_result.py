"""Tests for omopy.generics.summarised_result — SummarisedResult."""

import polars as pl
import pytest

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.summarised_result import (
    SETTINGS_REQUIRED_COLUMNS,
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_result_data(n: int = 10) -> pl.DataFrame:
    """Create a minimal valid SummarisedResult DataFrame."""
    return pl.DataFrame({
        "result_id": [1] * n,
        "cdm_name": ["test_cdm"] * n,
        "group_name": [OVERALL] * n,
        "group_level": [OVERALL] * n,
        "strata_name": [OVERALL] * n,
        "strata_level": [OVERALL] * n,
        "variable_name": ["number subjects"] * (n // 2) + ["age"] * (n - n // 2),
        "variable_level": [None] * n,
        "estimate_name": ["count"] * (n // 2) + ["mean"] * (n - n // 2),
        "estimate_type": ["integer"] * (n // 2) + ["numeric"] * (n - n // 2),
        "estimate_value": [str(i * 10) for i in range(n)],
        "additional_name": [OVERALL] * n,
        "additional_level": [OVERALL] * n,
    })


def _sample_settings() -> pl.DataFrame:
    return pl.DataFrame({
        "result_id": [1],
        "result_type": ["test_analysis"],
        "package_name": ["omopy"],
        "package_version": ["0.1.0"],
    })


def _multi_group_data() -> pl.DataFrame:
    """Create data with non-trivial group_name/group_level pairs."""
    rows = [
        (1, "test", "age &&& sex", "50 &&& female", OVERALL, OVERALL,
         "number subjects", None, "count", "integer", "100", OVERALL, OVERALL),
        (1, "test", "age &&& sex", "60 &&& male", OVERALL, OVERALL,
         "number subjects", None, "count", "integer", "200", OVERALL, OVERALL),
        (1, "test", OVERALL, OVERALL, OVERALL, OVERALL,
         "number subjects", None, "count", "integer", "300", OVERALL, OVERALL),
    ]
    return pl.DataFrame(
        rows,
        schema=list(SUMMARISED_RESULT_COLUMNS),
        orient="row",
    )


# ---------------------------------------------------------------------------
# SummarisedResult basics
# ---------------------------------------------------------------------------


class TestSummarisedResultBasics:
    def test_creation(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        assert len(sr) == 10

    def test_creation_default_settings(self):
        sr = SummarisedResult(_sample_result_data())
        assert "result_id" in sr.settings.columns
        assert "result_type" in sr.settings.columns

    def test_missing_columns_raises(self):
        df = pl.DataFrame({"result_id": [1], "cdm_name": ["test"]})
        with pytest.raises(ValueError, match="missing required columns"):
            SummarisedResult(df)

    def test_settings_missing_columns_raises(self):
        data = _sample_result_data()
        bad_settings = pl.DataFrame({"result_id": [1]})
        with pytest.raises(ValueError, match="missing required columns"):
            SummarisedResult(data, settings=bad_settings)

    def test_data_property(self):
        data = _sample_result_data()
        sr = SummarisedResult(data, settings=_sample_settings())
        assert sr.data is data

    def test_settings_property(self):
        settings = _sample_settings()
        sr = SummarisedResult(_sample_result_data(), settings=settings)
        assert sr.settings is settings

    def test_settings_setter(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        new_settings = _sample_settings().with_columns(
            pl.lit("new_type").alias("result_type"),
        )
        sr.settings = new_settings
        assert sr.settings["result_type"][0] == "new_type"

    def test_settings_setter_invalid(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        with pytest.raises(ValueError):
            sr.settings = pl.DataFrame({"result_id": [1]})

    def test_repr(self):
        sr = SummarisedResult(_sample_result_data())
        r = repr(sr)
        assert "SummarisedResult" in r
        assert "10 rows" in r


# ---------------------------------------------------------------------------
# Suppression
# ---------------------------------------------------------------------------


class TestSuppression:
    def test_suppress_below_threshold(self):
        data = _sample_result_data(6)
        sr = SummarisedResult(data, settings=_sample_settings())
        # Data has "number subjects" rows with values "0", "10", "20"
        # With min_cell_count=15: "10" matches 0 < 10 < 15, so row idx=1 triggers suppression.
        # All "number subjects" rows with that combo get suppressed.
        suppressed = sr.suppress(min_cell_count=15)
        vals = suppressed.data["estimate_value"].to_list()
        assert "-" in vals

    def test_suppress_zero_threshold(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        result = sr.suppress(min_cell_count=0)
        # No suppression with threshold < 1
        assert "-" not in result.data["estimate_value"].to_list()

    def test_suppress_no_counts(self):
        """When no count variables exist, nothing is suppressed."""
        data = _sample_result_data()
        # Replace all variable_names to be non-count
        data = data.with_columns(pl.lit("some_metric").alias("variable_name"))
        sr = SummarisedResult(data, settings=_sample_settings())
        suppressed = sr.suppress(min_cell_count=1000)
        assert "-" not in suppressed.data["estimate_value"].to_list()


# ---------------------------------------------------------------------------
# Split name-level pairs
# ---------------------------------------------------------------------------


class TestSplitNameLevel:
    def test_split_group_overall(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.split_group()
        assert "group_name" not in df.columns
        assert "group_level" not in df.columns

    def test_split_group_with_values(self):
        data = _multi_group_data()
        sr = SummarisedResult(data, settings=_sample_settings())
        df = sr.split_group()
        assert "group_name" not in df.columns
        # Should have new columns "age" and "sex"
        assert "age" in df.columns
        assert "sex" in df.columns

    def test_split_strata(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.split_strata()
        assert "strata_name" not in df.columns
        assert "strata_level" not in df.columns

    def test_split_additional(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.split_additional()
        assert "additional_name" not in df.columns
        assert "additional_level" not in df.columns

    def test_split_all(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.split_all()
        for col in ("group_name", "group_level", "strata_name",
                     "strata_level", "additional_name", "additional_level"):
            assert col not in df.columns


# ---------------------------------------------------------------------------
# Unite name-level pairs
# ---------------------------------------------------------------------------


class TestUniteNameLevel:
    def test_unite_group_empty(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        result = sr.unite_group([])
        assert result.data["group_name"][0] == OVERALL
        assert result.data["group_level"][0] == OVERALL

    def test_unite_strata(self):
        data = _sample_result_data().with_columns(
            pl.lit("50").alias("age"),
            pl.lit("female").alias("sex"),
        )
        sr = SummarisedResult(data, settings=_sample_settings())
        result = sr.unite_strata(["age", "sex"])
        assert result.data["strata_name"][0] == "age &&& sex"
        assert "50" in result.data["strata_level"][0]


# ---------------------------------------------------------------------------
# Add settings
# ---------------------------------------------------------------------------


class TestAddSettings:
    def test_add_all_settings(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.add_settings()
        assert "result_type" in df.columns
        assert "package_name" in df.columns

    def test_add_specific_settings(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.add_settings(columns=["result_type"])
        assert "result_type" in df.columns
        # package_name should NOT be joined (not requested)
        assert "package_name" not in df.columns


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    def test_filter_settings(self):
        settings = pl.DataFrame({
            "result_id": [1, 2],
            "result_type": ["analysis_a", "analysis_b"],
            "package_name": ["omopy", "omopy"],
            "package_version": ["0.1.0", "0.1.0"],
        })
        data = _sample_result_data()
        # Add some result_id=2 rows, ensuring matching schema
        extra = _sample_result_data(5).with_columns(
            pl.lit(2).cast(pl.Int64).alias("result_id"),
        )
        data = pl.concat([data, extra])
        sr = SummarisedResult(data, settings=settings)

        filtered = sr.filter_settings(result_type="analysis_a")
        assert all(filtered.data["result_id"] == 1)

    def test_filter_settings_list(self):
        settings = pl.DataFrame({
            "result_id": [1, 2],
            "result_type": ["a", "b"],
            "package_name": ["omopy", "omopy"],
            "package_version": ["0.1.0", "0.1.0"],
        })
        data = _sample_result_data()
        extra = _sample_result_data(5).with_columns(
            pl.lit(2).cast(pl.Int64).alias("result_id"),
        )
        data = pl.concat([data, extra])
        sr = SummarisedResult(data, settings=settings)

        filtered = sr.filter_settings(result_type=["a", "b"])
        assert set(filtered.data["result_id"].unique().to_list()) == {1, 2}


# ---------------------------------------------------------------------------
# Pivot estimates
# ---------------------------------------------------------------------------


class TestPivotEstimates:
    def test_pivot(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        pivoted = sr.pivot_estimates()
        # "count" and "mean" should become columns
        assert "count" in pivoted.columns or "mean" in pivoted.columns


# ---------------------------------------------------------------------------
# Tidy
# ---------------------------------------------------------------------------


class TestTidy:
    def test_tidy(self):
        sr = SummarisedResult(_sample_result_data(), settings=_sample_settings())
        df = sr.tidy()
        # Should have settings columns joined
        assert "result_type" in df.columns
        # Name-level columns should be split (removed)
        assert "group_name" not in df.columns
