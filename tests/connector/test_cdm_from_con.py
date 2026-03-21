"""Tests for cdm_from_con — the primary CDM connection factory."""

from __future__ import annotations

import polars as pl
import pytest

from omopy.generics._types import CdmVersion
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable


class TestCdmFromCon:
    """Test cdm_from_con() with the Synthea test database."""

    def test_returns_cdm_reference(self, synthea_cdm):
        assert isinstance(synthea_cdm, CdmReference)

    def test_cdm_version(self, synthea_cdm):
        assert synthea_cdm.cdm_version == CdmVersion.V5_4

    def test_cdm_name(self, synthea_cdm):
        assert synthea_cdm.cdm_name == "dbt-synthea"

    def test_has_person_table(self, synthea_cdm):
        assert "person" in synthea_cdm

    def test_has_standard_tables(self, synthea_cdm):
        expected = [
            "person", "observation_period", "visit_occurrence",
            "condition_occurrence", "drug_exposure",
        ]
        for name in expected:
            assert name in synthea_cdm, f"Expected '{name}' in CDM"

    def test_table_count(self, synthea_cdm):
        # Should have all standard CDM tables that exist in the database
        assert len(synthea_cdm) > 20

    def test_tables_are_cdm_table(self, synthea_cdm):
        for name in synthea_cdm:
            assert isinstance(synthea_cdm[name], CdmTable)

    def test_tables_are_lazy(self, synthea_cdm):
        import ibis
        person = synthea_cdm["person"]
        assert isinstance(person.data, ibis.expr.types.Table)

    def test_source_is_db_source(self, synthea_cdm):
        from omopy.connector.db_source import DbSource
        assert isinstance(synthea_cdm.cdm_source, DbSource)

    def test_repr(self, synthea_cdm):
        r = repr(synthea_cdm)
        assert "dbt-synthea" in r
        assert "duckdb" in r


class TestCdmFromConWithPath:
    """Test cdm_from_con() with a file path (DuckDB shorthand)."""

    def test_from_path_string(self):
        from pathlib import Path
        from omopy.connector.cdm_from_con import cdm_from_con

        db_path = Path(__file__).resolve().parent.parent.parent / "data" / "synthea.duckdb"
        if not db_path.exists():
            pytest.skip("Synthea DB not found")

        cdm = cdm_from_con(str(db_path), cdm_schema="base")
        assert isinstance(cdm, CdmReference)
        assert "person" in cdm
        assert cdm["person"].count() == 27

    def test_from_path_object(self):
        from pathlib import Path
        from omopy.connector.cdm_from_con import cdm_from_con

        db_path = Path(__file__).resolve().parent.parent.parent / "data" / "synthea.duckdb"
        if not db_path.exists():
            pytest.skip("Synthea DB not found")

        cdm = cdm_from_con(db_path, cdm_schema="base")
        assert isinstance(cdm, CdmReference)
        assert cdm.cdm_name == "dbt-synthea"


class TestCdmFromConAutoDetect:
    """Test schema auto-detection."""

    def test_auto_detect_schema(self, synthea_con):
        from omopy.connector.cdm_from_con import cdm_from_con
        # Don't pass cdm_schema — should auto-detect "base"
        cdm = cdm_from_con(synthea_con)
        assert "person" in cdm

    def test_explicit_wrong_schema_raises(self, synthea_con):
        from omopy.connector.cdm_from_con import cdm_from_con
        with pytest.raises(ValueError, match="person"):
            cdm_from_con(synthea_con, cdm_schema="main")


class TestCdmFromConTableSelection:
    """Test selective table loading."""

    def test_specific_tables(self, synthea_con):
        from omopy.connector.cdm_from_con import cdm_from_con
        cdm = cdm_from_con(
            synthea_con,
            cdm_schema="base",
            cdm_tables=["person", "observation_period"],
        )
        assert len(cdm) == 2
        assert "person" in cdm
        assert "observation_period" in cdm
        assert "drug_exposure" not in cdm

    def test_nonexistent_table_ignored(self, synthea_con):
        from omopy.connector.cdm_from_con import cdm_from_con
        cdm = cdm_from_con(
            synthea_con,
            cdm_schema="base",
            cdm_tables=["person", "nonexistent_table"],
        )
        assert len(cdm) == 1
        assert "person" in cdm


class TestCdmTableOperations:
    """Test CdmTable operations against the live database."""

    def test_collect_person(self, synthea_cdm):
        df = synthea_cdm["person"].collect()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 27
        assert "person_id" in df.columns

    def test_count_person(self, synthea_cdm):
        assert synthea_cdm["person"].count() == 27

    def test_count_visit_occurrence(self, synthea_cdm):
        assert synthea_cdm["visit_occurrence"].count() == 599

    def test_count_condition_occurrence(self, synthea_cdm):
        assert synthea_cdm["condition_occurrence"].count() == 59

    def test_columns(self, synthea_cdm):
        cols = synthea_cdm["person"].columns
        assert "person_id" in cols
        assert "gender_concept_id" in cols
        assert "year_of_birth" in cols

    def test_filter(self, synthea_cdm):
        import ibis
        person = synthea_cdm["person"]
        males = person.filter(person.data.gender_concept_id == 8507)
        assert isinstance(males, CdmTable)
        assert males.tbl_name == "person"
        count = males.count()
        assert 0 < count < 27

    def test_select(self, synthea_cdm):
        person = synthea_cdm["person"]
        subset = person.select("person_id", "year_of_birth")
        assert subset.columns == ["person_id", "year_of_birth"]
        df = subset.collect()
        assert len(df) == 27

    def test_head(self, synthea_cdm):
        person = synthea_cdm["person"]
        top5 = person.head(5)
        assert top5.count() == 5

    def test_join(self, synthea_cdm):
        person = synthea_cdm["person"]
        obs = synthea_cdm["observation_period"]
        joined = person.join(obs, on="person_id", how="inner")
        assert joined.count() == 27  # 1:1 relationship in Synthea
        assert "person_id" in joined.columns

    def test_schema(self, synthea_cdm):
        schema = synthea_cdm["person"].schema
        assert "person_id" in schema

    def test_collect_dtypes(self, synthea_cdm):
        """Verify that collected DataFrame has sensible Polars dtypes."""
        df = synthea_cdm["person"].collect()
        # person_id should be integer
        assert df["person_id"].dtype in (pl.Int32, pl.Int64)
        # year_of_birth should be integer
        assert df["year_of_birth"].dtype in (pl.Int32, pl.Int64)


class TestCdmSnapshot:
    """Test CDM snapshot with database-backed tables."""

    def test_snapshot(self, synthea_cdm):
        snap = synthea_cdm.snapshot()
        assert snap["cdm_name"] == "dbt-synthea"
        assert snap["cdm_version"] == "5.4"
        assert snap["source_type"] == "duckdb"
        assert "person" in snap["tables"]
        assert snap["tables"]["person"]["nrows"] == 27
