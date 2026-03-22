"""Tests for omopy.vis._table — table rendering functions."""

import polars as pl
import pytest

from omopy.generics.summarised_result import SummarisedResult
from omopy.vis import mock_summarised_result
from omopy.vis._style import TableStyle
from omopy.vis._table import (
    format_table,
    vis_omop_table,
    vis_table,
)


@pytest.fixture()
def sr() -> SummarisedResult:
    return mock_summarised_result()


# ── vis_omop_table ────────────────────────────────────────────────────────


class TestVisOmopTable:
    def test_returns_gt_object(self, sr: SummarisedResult):
        import great_tables
        tbl = vis_omop_table(sr, type="gt")
        assert isinstance(tbl, great_tables.GT)

    def test_returns_polars_dataframe(self, sr: SummarisedResult):
        tbl = vis_omop_table(sr, type="polars")
        assert isinstance(tbl, pl.DataFrame)

    def test_polars_no_nulls(self, sr: SummarisedResult):
        tbl = vis_omop_table(sr, type="polars")
        # Should have na replaced with en-dash
        for col in tbl.columns:
            assert tbl[col].null_count() == 0

    def test_estimate_name_formatting(self, sr: SummarisedResult):
        tbl = vis_omop_table(
            sr,
            estimate_name={"N (%)": "<count> (<percentage>%)"},
            type="polars",
        )
        assert isinstance(tbl, pl.DataFrame)

    def test_hide_columns(self, sr: SummarisedResult):
        tbl = vis_omop_table(sr, hide=["cdm_name"], type="polars")
        assert "cdm_name" not in tbl.columns

    def test_result_id_always_hidden(self, sr: SummarisedResult):
        tbl = vis_omop_table(sr, type="polars")
        assert "result_id" not in tbl.columns

    def test_estimate_type_always_hidden(self, sr: SummarisedResult):
        tbl = vis_omop_table(sr, type="polars")
        assert "estimate_type" not in tbl.columns

    def test_rename_columns(self, sr: SummarisedResult):
        tbl = vis_omop_table(
            sr, rename={"Database": "cdm_name"}, type="polars"
        )
        assert "Database" in tbl.columns
        assert "cdm_name" not in tbl.columns

    def test_with_title(self, sr: SummarisedResult):
        import great_tables
        tbl = vis_omop_table(sr, type="gt", title="My Table")
        assert isinstance(tbl, great_tables.GT)

    def test_settings_columns(self, sr: SummarisedResult):
        tbl = vis_omop_table(
            sr, settings_columns=["result_type"], type="polars"
        )
        assert "result_type" in tbl.columns

    def test_decimal_formatting(self, sr: SummarisedResult):
        tbl = vis_omop_table(
            sr, decimals={"numeric": 4}, type="polars"
        )
        assert isinstance(tbl, pl.DataFrame)

    def test_custom_style(self, sr: SummarisedResult):
        style = TableStyle(header_background="#ff0000")
        tbl = vis_omop_table(sr, type="gt", style=style)
        import great_tables
        assert isinstance(tbl, great_tables.GT)


# ── vis_table ─────────────────────────────────────────────────────────────


class TestVisTable:
    def test_returns_polars(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = vis_table(df, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_returns_gt(self):
        import great_tables
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = vis_table(df, type="gt")
        assert isinstance(result, great_tables.GT)

    def test_hide_columns(self):
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = vis_table(df, hide=["c"], type="polars")
        assert "c" not in result.columns

    def test_rename(self):
        df = pl.DataFrame({"a": [1], "b": [2]})
        result = vis_table(df, rename={"Alpha": "a"}, type="polars")
        assert "Alpha" in result.columns


# ── format_table ──────────────────────────────────────────────────────────


class TestFormatTable:
    def test_polars_output(self):
        df = pl.DataFrame({"x": [1, None, 3]})
        result = format_table(df, type="polars")
        assert isinstance(result, pl.DataFrame)
        # All columns are cast to string, null replaced with en-dash
        vals = result["x"].to_list()
        assert "\u2013" in vals
        assert "1" in vals
        assert "3" in vals

    def test_gt_output(self):
        import great_tables
        df = pl.DataFrame({"x": [1, 2, 3]})
        result = format_table(df, type="gt")
        assert isinstance(result, great_tables.GT)

    def test_custom_na(self):
        df = pl.DataFrame({"x": [1, None]})
        result = format_table(df, type="polars", na="N/A")
        vals = result["x"].to_list()
        assert "N/A" in vals

    def test_with_title_subtitle(self):
        import great_tables
        df = pl.DataFrame({"x": [1]})
        result = format_table(df, type="gt", title="Title", subtitle="Sub")
        assert isinstance(result, great_tables.GT)

    def test_group_column(self):
        import great_tables
        df = pl.DataFrame({"group": ["a", "a", "b"], "val": [1, 2, 3]})
        result = format_table(df, type="gt", group_column=["group"])
        assert isinstance(result, great_tables.GT)

    def test_auto_detect_type(self):
        df = pl.DataFrame({"x": [1]})
        result = format_table(df)
        # Should auto-detect gt since it's installed
        import great_tables
        assert isinstance(result, great_tables.GT)
