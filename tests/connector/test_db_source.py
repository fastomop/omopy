"""Tests for DbSource — database-backed CDM source."""

from __future__ import annotations

import pytest

from omopy.generics._types import CdmVersion


class TestDbSourceCreation:
    """Test DbSource instantiation and auto-detection."""

    def test_creation(self, synthea_db_source):
        from omopy.connector.db_source import DbSource
        assert isinstance(synthea_db_source, DbSource)

    def test_source_type_is_duckdb(self, synthea_db_source):
        assert synthea_db_source.source_type == "duckdb"

    def test_cdm_schema(self, synthea_db_source):
        assert synthea_db_source.cdm_schema == "base"

    def test_auto_detect_version(self, synthea_db_source):
        assert synthea_db_source.cdm_version == CdmVersion.V5_4

    def test_auto_detect_name(self, synthea_db_source):
        assert synthea_db_source.cdm_name == "dbt-synthea"

    def test_repr(self, synthea_db_source):
        r = repr(synthea_db_source)
        assert "duckdb" in r
        assert "base" in r
        assert "5.4" in r


class TestDbSourceListTables:
    """Test table listing."""

    def test_list_tables_returns_list(self, synthea_db_source):
        tables = synthea_db_source.list_tables()
        assert isinstance(tables, list)

    def test_list_tables_has_person(self, synthea_db_source):
        tables = synthea_db_source.list_tables()
        assert "person" in tables

    def test_list_tables_has_standard_cdm(self, synthea_db_source):
        tables = synthea_db_source.list_tables()
        expected = [
            "person", "observation_period", "visit_occurrence",
            "condition_occurrence", "drug_exposure", "measurement",
        ]
        for name in expected:
            assert name in tables, f"Expected '{name}' in tables"

    def test_list_tables_count(self, synthea_db_source):
        tables = synthea_db_source.list_tables()
        assert len(tables) == 36  # All 36 tables in the base schema

    def test_list_tables_sorted(self, synthea_db_source):
        tables = synthea_db_source.list_tables()
        assert tables == sorted(tables)


class TestDbSourceReadTable:
    """Test lazy table reading."""

    def test_read_table_returns_cdm_table(self, synthea_db_source):
        from omopy.generics.cdm_table import CdmTable
        tbl = synthea_db_source.read_table("person")
        assert isinstance(tbl, CdmTable)

    def test_read_table_is_lazy(self, synthea_db_source):
        import ibis
        tbl = synthea_db_source.read_table("person")
        # The underlying data should be an Ibis table, not materialised
        assert isinstance(tbl.data, ibis.expr.types.Table)

    def test_read_table_tbl_name(self, synthea_db_source):
        tbl = synthea_db_source.read_table("person")
        assert tbl.tbl_name == "person"

    def test_read_table_tbl_source(self, synthea_db_source):
        tbl = synthea_db_source.read_table("person")
        assert tbl.tbl_source == "duckdb"

    def test_read_table_columns(self, synthea_db_source):
        tbl = synthea_db_source.read_table("person")
        cols = tbl.columns
        assert "person_id" in cols
        assert "gender_concept_id" in cols
        assert "year_of_birth" in cols

    def test_read_table_nonexistent_raises(self, synthea_db_source):
        with pytest.raises(KeyError, match="not_a_real_table"):
            synthea_db_source.read_table("not_a_real_table")

    def test_read_table_collect(self, synthea_db_source):
        import polars as pl
        tbl = synthea_db_source.read_table("person")
        df = tbl.collect()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 27

    def test_read_table_count_no_materialise(self, synthea_db_source):
        """count() should use database COUNT(*), not materialise the full table."""
        tbl = synthea_db_source.read_table("person")
        assert tbl.count() == 27


class TestDbSourceCdmSourceProtocol:
    """Verify DbSource satisfies the CdmSource protocol."""

    def test_implements_protocol(self, synthea_db_source):
        from omopy.generics.cdm_reference import CdmSource
        assert isinstance(synthea_db_source, CdmSource)
