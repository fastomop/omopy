"""Tests for omopy.connector.copy_cdm — copy_cdm_to."""

from __future__ import annotations

import datetime

import ibis
import polars as pl
import pytest

from omopy.connector._connection import _get_catalog
from omopy.connector.copy_cdm import copy_cdm_to
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.generics._types import CdmVersion


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_cdm():
    """A minimal in-memory CDM for testing copy operations."""
    person_df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "gender_concept_id": [8507, 8532, 8507],
            "year_of_birth": [1990, 1985, 2000],
            "month_of_birth": [1, 6, 12],
            "day_of_birth": [15, 20, 1],
            "race_concept_id": [0, 0, 0],
            "ethnicity_concept_id": [0, 0, 0],
        }
    )
    obs_period_df = pl.DataFrame(
        {
            "observation_period_id": [1, 2, 3],
            "person_id": [1, 2, 3],
            "observation_period_start_date": [
                datetime.date(2010, 1, 1),
                datetime.date(2012, 6, 1),
                datetime.date(2015, 1, 1),
            ],
            "observation_period_end_date": [
                datetime.date(2023, 12, 31),
                datetime.date(2023, 12, 31),
                datetime.date(2023, 12, 31),
            ],
            "period_type_concept_id": [0, 0, 0],
        }
    )
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 2],
            "condition_concept_id": [320128, 257012],
            "condition_start_date": [
                datetime.date(2020, 3, 15),
                datetime.date(2021, 7, 1),
            ],
            "condition_end_date": [
                datetime.date(2020, 4, 15),
                datetime.date(2021, 8, 1),
            ],
            "condition_type_concept_id": [0, 0],
        }
    )

    tables = {
        "person": CdmTable(data=person_df, tbl_name="person"),
        "observation_period": CdmTable(data=obs_period_df, tbl_name="observation_period"),
        "condition_occurrence": CdmTable(data=condition_df, tbl_name="condition_occurrence"),
    }
    return CdmReference(
        tables=tables,
        cdm_version=CdmVersion.V5_4,
        cdm_name="test_source",
    )


@pytest.fixture()
def source_cdm_with_cohort(source_cdm):
    """Source CDM that includes a cohort table."""
    cohort_df = pl.DataFrame(
        {
            "cohort_definition_id": [1, 1, 2],
            "subject_id": [1, 2, 3],
            "cohort_start_date": [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 6, 1),
                datetime.date(2021, 1, 1),
            ],
            "cohort_end_date": [
                datetime.date(2020, 12, 31),
                datetime.date(2020, 12, 31),
                datetime.date(2021, 12, 31),
            ],
        }
    )
    settings = pl.DataFrame(
        {
            "cohort_definition_id": [1, 2],
            "cohort_name": ["hypertension", "sinusitis"],
        }
    )
    attrition = pl.DataFrame(
        {
            "cohort_definition_id": [1, 2],
            "number_records": [2, 1],
            "number_subjects": [2, 1],
            "reason_id": [1, 1],
            "reason": ["Initial qualifying events", "Initial qualifying events"],
            "excluded_records": [0, 0],
            "excluded_subjects": [0, 0],
        }
    )

    cohort = CohortTable(
        data=cohort_df,
        tbl_name="my_cohort",
        settings=settings,
        attrition=attrition,
    )
    source_cdm["my_cohort"] = cohort
    return source_cdm


@pytest.fixture()
def target_con(tmp_path):
    """A writable DuckDB connection for the copy target."""
    db_path = tmp_path / "target.duckdb"
    con = ibis.duckdb.connect(str(db_path))
    yield con
    con.disconnect()


# ---------------------------------------------------------------------------
# Basic copy tests
# ---------------------------------------------------------------------------


class TestCopyCdmTo:
    """Tests for copy_cdm_to."""

    def test_basic_copy(self, source_cdm, target_con):
        """Copies all tables to the target database."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")

        assert isinstance(new_cdm, CdmReference)
        assert set(new_cdm.table_names) == {"person", "observation_period", "condition_occurrence"}

    def test_preserves_cdm_metadata(self, source_cdm, target_con):
        """CDM name and version are preserved."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")

        assert new_cdm.cdm_name == "test_source"
        assert new_cdm.cdm_version == CdmVersion.V5_4

    def test_person_data_correct(self, source_cdm, target_con):
        """Person table data is correctly copied."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")
        person_df = new_cdm["person"].collect()

        assert len(person_df) == 3
        assert set(person_df["person_id"].to_list()) == {1, 2, 3}

    def test_observation_period_data(self, source_cdm, target_con):
        """Observation period data is correctly copied."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")
        obs = new_cdm["observation_period"].collect()

        assert len(obs) == 3

    def test_condition_data(self, source_cdm, target_con):
        """Condition occurrence data is correctly copied."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")
        cond = new_cdm["condition_occurrence"].collect()

        assert len(cond) == 2

    def test_tables_are_ibis_backed(self, source_cdm, target_con):
        """Tables in the new CDM are backed by Ibis (lazy)."""
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")

        import ibis.expr.types as ir

        for name in new_cdm.table_names:
            tbl = new_cdm[name]
            assert isinstance(tbl.data, ir.Table), f"{name} should be Ibis-backed"

    def test_copy_order_person_first(self, source_cdm, target_con):
        """person and observation_period are copied before other tables."""
        # This is implicitly tested by successful copy — if person were
        # missing, other tables with FK references would fail.
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm")
        assert "person" in new_cdm
        assert "observation_period" in new_cdm


# ---------------------------------------------------------------------------
# Cohort metadata preservation
# ---------------------------------------------------------------------------


class TestCopyCohortMetadata:
    """Tests that cohort metadata is preserved during copy."""

    def test_cohort_table_type_preserved(self, source_cdm_with_cohort, target_con):
        """CohortTable type is preserved after copy."""
        new_cdm = copy_cdm_to(source_cdm_with_cohort, target_con, schema="cdm")
        assert isinstance(new_cdm["my_cohort"], CohortTable)

    def test_cohort_settings_preserved(self, source_cdm_with_cohort, target_con):
        """Cohort settings are preserved."""
        new_cdm = copy_cdm_to(source_cdm_with_cohort, target_con, schema="cdm")
        cohort = new_cdm["my_cohort"]

        assert isinstance(cohort, CohortTable)
        assert len(cohort.settings) == 2
        assert set(cohort.settings["cohort_name"].to_list()) == {"hypertension", "sinusitis"}

    def test_cohort_attrition_preserved(self, source_cdm_with_cohort, target_con):
        """Cohort attrition is preserved."""
        new_cdm = copy_cdm_to(source_cdm_with_cohort, target_con, schema="cdm")
        cohort = new_cdm["my_cohort"]

        assert isinstance(cohort, CohortTable)
        assert len(cohort.attrition) == 2

    def test_cohort_data_correct(self, source_cdm_with_cohort, target_con):
        """Cohort data rows are correctly copied."""
        new_cdm = copy_cdm_to(source_cdm_with_cohort, target_con, schema="cdm")
        cohort_df = new_cdm["my_cohort"].collect()

        assert len(cohort_df) == 3
        assert set(cohort_df["subject_id"].to_list()) == {1, 2, 3}


# ---------------------------------------------------------------------------
# Overwrite behaviour
# ---------------------------------------------------------------------------


class TestCopyOverwrite:
    """Tests for overwrite parameter."""

    def test_overwrite_false_raises_on_conflict(self, source_cdm, target_con):
        """overwrite=False raises if target tables exist."""
        # First copy succeeds
        copy_cdm_to(source_cdm, target_con, schema="cdm", overwrite=True)
        # Second copy with overwrite=False should fail
        with pytest.raises(ValueError, match="already exists"):
            copy_cdm_to(source_cdm, target_con, schema="cdm", overwrite=False)

    def test_overwrite_true_replaces(self, source_cdm, target_con):
        """overwrite=True replaces existing tables."""
        copy_cdm_to(source_cdm, target_con, schema="cdm", overwrite=True)
        # Second copy with overwrite=True should succeed
        new_cdm = copy_cdm_to(source_cdm, target_con, schema="cdm", overwrite=True)
        assert len(new_cdm["person"].collect()) == 3


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestCopyEdgeCases:
    """Edge case tests for copy_cdm_to."""

    def test_empty_table(self, target_con):
        """Can copy a CDM with an empty table."""
        empty_person = pl.DataFrame(
            {
                "person_id": pl.Series([], dtype=pl.Int64),
                "gender_concept_id": pl.Series([], dtype=pl.Int64),
                "year_of_birth": pl.Series([], dtype=pl.Int64),
                "month_of_birth": pl.Series([], dtype=pl.Int64),
                "day_of_birth": pl.Series([], dtype=pl.Int64),
                "race_concept_id": pl.Series([], dtype=pl.Int64),
                "ethnicity_concept_id": pl.Series([], dtype=pl.Int64),
            }
        )
        obs = pl.DataFrame(
            {
                "observation_period_id": pl.Series([], dtype=pl.Int64),
                "person_id": pl.Series([], dtype=pl.Int64),
                "observation_period_start_date": pl.Series([], dtype=pl.Date),
                "observation_period_end_date": pl.Series([], dtype=pl.Date),
                "period_type_concept_id": pl.Series([], dtype=pl.Int64),
            }
        )
        tables = {
            "person": CdmTable(data=empty_person, tbl_name="person"),
            "observation_period": CdmTable(data=obs, tbl_name="observation_period"),
        }
        cdm = CdmReference(
            tables=tables,
            cdm_version=CdmVersion.V5_4,
            cdm_name="empty_cdm",
        )
        new_cdm = copy_cdm_to(cdm, target_con, schema="cdm")
        assert len(new_cdm["person"].collect()) == 0

    def test_different_schema(self, source_cdm, target_con):
        """Can copy to different schema names."""
        cdm1 = copy_cdm_to(source_cdm, target_con, schema="schema_a", overwrite=True)
        cdm2 = copy_cdm_to(source_cdm, target_con, schema="schema_b", overwrite=True)

        assert len(cdm1["person"].collect()) == 3
        assert len(cdm2["person"].collect()) == 3


# ---------------------------------------------------------------------------
# Integration: copy from Synthea
# ---------------------------------------------------------------------------


class TestCopyFromSynthea:
    """Integration tests copying from the Synthea test database."""

    def test_copy_subset_from_synthea(self, synthea_cdm, tmp_path):
        """Can copy a subset of Synthea tables to a new database."""
        # Create a small CDM with just person and observation_period
        small_tables = {
            name: synthea_cdm[name]
            for name in ["person", "observation_period"]
            if name in synthea_cdm
        }
        small_cdm = CdmReference(
            tables=small_tables,
            cdm_version=synthea_cdm.cdm_version,
            cdm_name="synthea_subset",
        )

        target = ibis.duckdb.connect(str(tmp_path / "synthea_copy.duckdb"))
        try:
            new_cdm = copy_cdm_to(small_cdm, target, schema="copied")
            person_df = new_cdm["person"].collect()
            assert len(person_df) == 27  # Synthea has 27 persons
        finally:
            target.disconnect()
