"""Tests for omopy.vis._format — formatting pipeline functions."""

import polars as pl
import pytest

from omopy.generics.summarised_result import SummarisedResult
from omopy.vis import mock_summarised_result
from omopy.vis._format import (
    _apply_big_mark,
    _format_single_value,
    format_estimate_name,
    format_estimate_value,
    format_header,
    format_min_cell_count,
    parse_header_keys,
    tidy_columns,
    tidy_result,
)


@pytest.fixture()
def sr() -> SummarisedResult:
    return mock_summarised_result()


# ── format_estimate_value ─────────────────────────────────────────────────


class TestFormatEstimateValue:
    def test_returns_summarised_result(self, sr: SummarisedResult):
        result = format_estimate_value(sr)
        assert isinstance(result, SummarisedResult)

    def test_integer_no_decimals(self, sr: SummarisedResult):
        result = format_estimate_value(sr)
        # Find a count (integer) row
        count_rows = result.data.filter(pl.col("estimate_name") == "count")
        for val in count_rows["estimate_value"].to_list():
            assert "." not in val  # integers should not have decimal point

    def test_numeric_two_decimals(self, sr: SummarisedResult):
        result = format_estimate_value(sr)
        mean_rows = result.data.filter(pl.col("estimate_name") == "mean")
        for val in mean_rows["estimate_value"].to_list():
            if "." in val:
                parts = val.split(".")
                assert len(parts[1]) == 2

    def test_percentage_one_decimal(self, sr: SummarisedResult):
        result = format_estimate_value(sr)
        pct_rows = result.data.filter(pl.col("estimate_name") == "percentage")
        for val in pct_rows["estimate_value"].to_list():
            if "." in val:
                parts = val.split(".")
                assert len(parts[1]) == 1

    def test_custom_decimals(self, sr: SummarisedResult):
        result = format_estimate_value(sr, decimals={"integer": 2, "numeric": 4})
        count_rows = result.data.filter(pl.col("estimate_name") == "count")
        for val in count_rows["estimate_value"].to_list():
            if "." in val:
                parts = val.split(".")
                assert len(parts[1]) == 2

    def test_big_mark(self):
        # Create a result with a large number
        data = pl.DataFrame(
            {
                "result_id": [1],
                "cdm_name": ["test"],
                "group_name": ["overall"],
                "group_level": ["overall"],
                "strata_name": ["overall"],
                "strata_level": ["overall"],
                "variable_name": ["population"],
                "variable_level": ["pop"],
                "estimate_name": ["count"],
                "estimate_type": ["integer"],
                "estimate_value": ["1234567"],
                "additional_name": ["overall"],
                "additional_level": ["overall"],
            }
        )
        sr = SummarisedResult(data)
        result = format_estimate_value(sr, big_mark=",")
        val = result.data["estimate_value"][0]
        assert val == "1,234,567"

    def test_no_big_mark(self):
        data = pl.DataFrame(
            {
                "result_id": [1],
                "cdm_name": ["test"],
                "group_name": ["overall"],
                "group_level": ["overall"],
                "strata_name": ["overall"],
                "strata_level": ["overall"],
                "variable_name": ["population"],
                "variable_level": ["pop"],
                "estimate_name": ["count"],
                "estimate_type": ["integer"],
                "estimate_value": ["1234567"],
                "additional_name": ["overall"],
                "additional_level": ["overall"],
            }
        )
        sr = SummarisedResult(data)
        result = format_estimate_value(sr, big_mark="")
        val = result.data["estimate_value"][0]
        assert val == "1234567"

    def test_decimal_mark(self):
        data = pl.DataFrame(
            {
                "result_id": [1],
                "cdm_name": ["test"],
                "group_name": ["overall"],
                "group_level": ["overall"],
                "strata_name": ["overall"],
                "strata_level": ["overall"],
                "variable_name": ["age"],
                "variable_level": ["age"],
                "estimate_name": ["mean"],
                "estimate_type": ["numeric"],
                "estimate_value": ["45.67"],
                "additional_name": ["overall"],
                "additional_level": ["overall"],
            }
        )
        sr = SummarisedResult(data)
        result = format_estimate_value(sr, decimal_mark=",", big_mark=".")
        val = result.data["estimate_value"][0]
        assert "," in val  # comma as decimal mark

    def test_suppressed_values_preserved(self):
        data = pl.DataFrame(
            {
                "result_id": [1],
                "cdm_name": ["test"],
                "group_name": ["overall"],
                "group_level": ["overall"],
                "strata_name": ["overall"],
                "strata_level": ["overall"],
                "variable_name": ["count"],
                "variable_level": ["count"],
                "estimate_name": ["count"],
                "estimate_type": ["integer"],
                "estimate_value": ["-"],
                "additional_name": ["overall"],
                "additional_level": ["overall"],
            }
        )
        sr = SummarisedResult(data)
        result = format_estimate_value(sr)
        assert result.data["estimate_value"][0] == "-"

    def test_preserves_row_count(self, sr: SummarisedResult):
        result = format_estimate_value(sr)
        assert len(result) == len(sr)


class TestApplyBigMark:
    def test_short_number(self):
        assert _apply_big_mark("123", ",") == "123"

    def test_thousands(self):
        assert _apply_big_mark("1234", ",") == "1,234"

    def test_millions(self):
        assert _apply_big_mark("1234567", ",") == "1,234,567"

    def test_negative(self):
        assert _apply_big_mark("-1234567", ",") == "-1,234,567"

    def test_no_mark(self):
        assert _apply_big_mark("1234567", "") == "1234567"


class TestFormatSingleValue:
    def test_none_returns_empty(self):
        assert _format_single_value(None, "integer", {}, ".", ",") == ""

    def test_dash_returns_dash(self):
        assert _format_single_value("-", "integer", {}, ".", ",") == "-"

    def test_character_passthrough(self):
        assert _format_single_value("hello", "character", {}, ".", ",") == "hello"

    def test_integer(self):
        result = _format_single_value("42", "integer", {"integer": 0}, ".", ",")
        assert result == "42"


# ── format_estimate_name ──────────────────────────────────────────────────


class TestFormatEstimateName:
    def test_returns_summarised_result(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name={"N": "<count>"})
        assert isinstance(result, SummarisedResult)

    def test_simple_rename(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name={"N": "<count>"})
        names = result.data["estimate_name"].unique().to_list()
        assert "N" in names

    def test_combined_pattern(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name={"N (%)": "<count> (<percentage>%)"})
        names = result.data["estimate_name"].unique().to_list()
        assert "N (%)" in names
        # Check that the value contains both pieces
        combined = result.data.filter(pl.col("estimate_name") == "N (%)")
        for val in combined["estimate_value"].to_list():
            assert "(" in val
            assert "%" in val

    def test_keep_not_formatted(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name={"N": "<count>"}, keep_not_formatted=True)
        names = set(result.data["estimate_name"].unique().to_list())
        # mean, sd should still be there
        assert "mean" in names or "sd" in names

    def test_drop_not_formatted(self, sr: SummarisedResult):
        result = format_estimate_name(
            sr,
            estimate_name={"N (%)": "<count> (<percentage>%)"},
            keep_not_formatted=False,
        )
        names = set(result.data["estimate_name"].unique().to_list())
        # Only formatted names should remain
        assert "count" not in names
        assert "percentage" not in names

    def test_none_returns_unchanged(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name=None)
        assert result.data.equals(sr.data)

    def test_empty_dict_returns_unchanged(self, sr: SummarisedResult):
        result = format_estimate_name(sr, estimate_name={})
        assert result.data.equals(sr.data)


# ── format_header ─────────────────────────────────────────────────────────


class TestFormatHeader:
    def test_empty_header_returns_unchanged(self):
        df = pl.DataFrame({"a": [1, 2], "estimate_value": ["x", "y"]})
        result = format_header(df, [])
        assert result.equals(df)

    def test_nonexistent_column_ignored(self):
        df = pl.DataFrame({"a": [1, 2], "estimate_value": ["x", "y"]})
        result = format_header(df, ["nonexistent"])
        assert result.equals(df)


# ── format_min_cell_count ─────────────────────────────────────────────────


class TestFormatMinCellCount:
    def test_replaces_dash_with_less_than(self):
        data = pl.DataFrame(
            {
                "result_id": [1, 1],
                "cdm_name": ["test", "test"],
                "group_name": ["overall", "overall"],
                "group_level": ["overall", "overall"],
                "strata_name": ["overall", "overall"],
                "strata_level": ["overall", "overall"],
                "variable_name": ["n", "n"],
                "variable_level": ["n", "n"],
                "estimate_name": ["count", "pct"],
                "estimate_type": ["integer", "percentage"],
                "estimate_value": ["-", "50.0"],
                "additional_name": ["overall", "overall"],
                "additional_level": ["overall", "overall"],
            }
        )
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["test"],
                "package_name": ["omopy"],
                "package_version": ["0.1.0"],
                "min_cell_count": ["5"],
            }
        )
        sr = SummarisedResult(data, settings=settings)
        result = format_min_cell_count(sr)
        values = result.data["estimate_value"].to_list()
        assert values[0] == "<5"
        assert values[1] == "50.0"

    def test_custom_min_cell_count(self):
        data = pl.DataFrame(
            {
                "result_id": [1],
                "cdm_name": ["test"],
                "group_name": ["overall"],
                "group_level": ["overall"],
                "strata_name": ["overall"],
                "strata_level": ["overall"],
                "variable_name": ["n"],
                "variable_level": ["n"],
                "estimate_name": ["count"],
                "estimate_type": ["integer"],
                "estimate_value": ["-"],
                "additional_name": ["overall"],
                "additional_level": ["overall"],
            }
        )
        settings = pl.DataFrame(
            {
                "result_id": [1],
                "result_type": ["test"],
                "package_name": ["omopy"],
                "package_version": ["0.1.0"],
                "min_cell_count": ["10"],
            }
        )
        sr = SummarisedResult(data, settings=settings)
        result = format_min_cell_count(sr)
        assert result.data["estimate_value"][0] == "<10"

    def test_no_suppressed_values(self, sr: SummarisedResult):
        result = format_min_cell_count(sr)
        # None of the mock values should be "-", so no changes
        assert result.data.equals(sr.data)


# ── parse_header_keys ─────────────────────────────────────────────────────


class TestParseHeaderKeys:
    def test_header_level(self):
        result = parse_header_keys("[header_level]cohort_1")
        assert result == {"header_level": "cohort_1"}

    def test_header_name_and_level(self):
        result = parse_header_keys("[header_name]cohort_name\n[header_level]cohort_1")
        assert result == {"header_name": "cohort_name", "header_level": "cohort_1"}

    def test_full_header(self):
        result = parse_header_keys(
            "[header]Results\n[header_name]cohort_name\n[header_level]cohort_1"
        )
        assert result == {
            "header": "Results",
            "header_name": "cohort_name",
            "header_level": "cohort_1",
        }

    def test_plain_column(self):
        result = parse_header_keys("age")
        assert result == {}


# ── tidy helpers ──────────────────────────────────────────────────────────


class TestTidyHelpers:
    def test_tidy_result_returns_dataframe(self, sr: SummarisedResult):
        result = tidy_result(sr)
        assert isinstance(result, pl.DataFrame)

    def test_tidy_columns_returns_list(self, sr: SummarisedResult):
        cols = tidy_columns(sr)
        assert isinstance(cols, list)
        assert len(cols) > 0

    def test_tidy_splits_group(self, sr: SummarisedResult):
        result = tidy_result(sr)
        assert "group_name" not in result.columns
        assert "cohort_name" in result.columns
