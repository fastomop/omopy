"""Tests for omopy.generics.cdm_table — CdmTable wrapper."""

import polars as pl
import pytest

from omopy.generics.cdm_table import CdmTable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "gender_concept_id": [8507, 8532, 8507],
            "year_of_birth": [1990, 1985, 2000],
        }
    )


# ---------------------------------------------------------------------------
# CdmTable basics
# ---------------------------------------------------------------------------


class TestCdmTableBasics:
    def test_creation(self):
        df = _sample_df()
        tbl = CdmTable(df, tbl_name="person")
        assert tbl.tbl_name == "person"
        assert tbl.tbl_source == "local"
        assert tbl.cdm is None

    def test_columns(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        assert tbl.columns == ["person_id", "gender_concept_id", "year_of_birth"]

    def test_schema(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        schema = tbl.schema
        assert "person_id" in schema

    def test_data_access(self):
        df = _sample_df()
        tbl = CdmTable(df, tbl_name="person")
        assert tbl.data is df

    def test_count(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        assert tbl.count() == 3

    def test_len(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        assert len(tbl) == 3

    def test_repr(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        r = repr(tbl)
        assert "person" in r
        assert "local" in r

    def test_custom_source(self):
        tbl = CdmTable(_sample_df(), tbl_name="person", tbl_source="duckdb")
        assert tbl.tbl_source == "duckdb"

    def test_cdm_setter(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        assert tbl.cdm is None
        # Set to a sentinel value (not a real CdmReference in this test)
        tbl.cdm = "fake_cdm"  # type: ignore[assignment]
        assert tbl.cdm == "fake_cdm"


# ---------------------------------------------------------------------------
# CdmTable with LazyFrame
# ---------------------------------------------------------------------------


class TestCdmTableLazy:
    def test_lazy_columns(self):
        lf = _sample_df().lazy()
        tbl = CdmTable(lf, tbl_name="person")
        assert tbl.columns == ["person_id", "gender_concept_id", "year_of_birth"]

    def test_collect(self):
        lf = _sample_df().lazy()
        tbl = CdmTable(lf, tbl_name="person")
        result = tbl.collect()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_collect_df_passthrough(self):
        df = _sample_df()
        tbl = CdmTable(df, tbl_name="person")
        result = tbl.collect()
        assert result is df  # no copy for DataFrame


# ---------------------------------------------------------------------------
# CdmTable transformations
# ---------------------------------------------------------------------------


class TestCdmTableTransformations:
    def test_filter_preserves_metadata(self):
        tbl = CdmTable(_sample_df(), tbl_name="person", tbl_source="duckdb")
        filtered = tbl.filter(pl.col("year_of_birth") > 1990)
        assert isinstance(filtered, CdmTable)
        assert filtered.tbl_name == "person"
        assert filtered.tbl_source == "duckdb"
        assert filtered.count() == 1  # only person 3 (year 2000)

    def test_select_preserves_metadata(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        selected = tbl.select("person_id", "year_of_birth")
        assert isinstance(selected, CdmTable)
        assert selected.tbl_name == "person"
        assert selected.columns == ["person_id", "year_of_birth"]

    def test_rename_preserves_metadata(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        renamed = tbl.rename({"person_id": "pid"})
        assert isinstance(renamed, CdmTable)
        assert renamed.tbl_name == "person"
        assert "pid" in renamed.columns

    def test_head_preserves_metadata(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        h = tbl.head(2)
        assert isinstance(h, CdmTable)
        assert h.tbl_name == "person"
        assert h.count() == 2

    def test_join_preserves_metadata(self):
        tbl = CdmTable(_sample_df(), tbl_name="person")
        other = pl.DataFrame({"person_id": [1, 2], "extra": ["a", "b"]})
        joined = tbl.join(other, on="person_id", how="inner")
        assert isinstance(joined, CdmTable)
        assert joined.tbl_name == "person"
        assert "extra" in joined.columns
        assert joined.count() == 2

    def test_join_with_cdm_table(self):
        tbl1 = CdmTable(_sample_df(), tbl_name="person")
        tbl2 = CdmTable(
            pl.DataFrame({"person_id": [1], "obs": ["x"]}),
            tbl_name="obs",
        )
        joined = tbl1.join(tbl2, on="person_id", how="inner")
        assert joined.tbl_name == "person"  # preserves left table's name
        assert joined.count() == 1

    def test_filter_lazy(self):
        lf = _sample_df().lazy()
        tbl = CdmTable(lf, tbl_name="person")
        filtered = tbl.filter(pl.col("year_of_birth") > 1990)
        assert isinstance(filtered.data, pl.LazyFrame)
        result = filtered.collect()
        assert len(result) == 1

    def test_with_data_preserves_cdm_ref(self):
        tbl = CdmTable(_sample_df(), tbl_name="person", cdm="fake_cdm")  # type: ignore[arg-type]
        new_tbl = tbl._with_data(pl.DataFrame({"person_id": [1]}))
        assert new_tbl.cdm == "fake_cdm"
