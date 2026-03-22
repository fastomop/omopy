"""Tests for omopy.profiles._columns — column name helpers."""

from __future__ import annotations

import pytest

from omopy.profiles._columns import (
    end_date_column,
    person_id_column,
    source_concept_id_column,
    standard_concept_id_column,
    start_date_column,
)


# ---------------------------------------------------------------------------
# start_date_column
# ---------------------------------------------------------------------------


class TestStartDateColumn:
    def test_condition_occurrence(self):
        assert start_date_column("condition_occurrence") == "condition_start_date"

    def test_drug_exposure(self):
        assert start_date_column("drug_exposure") == "drug_exposure_start_date"

    def test_procedure_occurrence(self):
        assert start_date_column("procedure_occurrence") == "procedure_date"

    def test_observation(self):
        assert start_date_column("observation") == "observation_date"

    def test_measurement(self):
        assert start_date_column("measurement") == "measurement_date"

    def test_visit_occurrence(self):
        assert start_date_column("visit_occurrence") == "visit_start_date"

    def test_device_exposure(self):
        assert start_date_column("device_exposure") == "device_exposure_start_date"

    def test_death(self):
        assert start_date_column("death") == "death_date"

    def test_observation_period(self):
        assert start_date_column("observation_period") == "observation_period_start_date"

    def test_cohort_default(self):
        """Non-OMOP tables fall back to cohort defaults."""
        assert start_date_column("my_cohort") == "cohort_start_date"

    def test_unknown_table(self):
        assert start_date_column("nonexistent_table") == "cohort_start_date"


# ---------------------------------------------------------------------------
# end_date_column
# ---------------------------------------------------------------------------


class TestEndDateColumn:
    def test_condition_occurrence(self):
        assert end_date_column("condition_occurrence") == "condition_end_date"

    def test_drug_exposure(self):
        assert end_date_column("drug_exposure") == "drug_exposure_end_date"

    def test_procedure_occurrence(self):
        """Procedure has same start and end date column."""
        assert end_date_column("procedure_occurrence") == "procedure_date"

    def test_observation(self):
        assert end_date_column("observation") == "observation_date"

    def test_death(self):
        assert end_date_column("death") == "death_date"

    def test_cohort_default(self):
        assert end_date_column("my_cohort") == "cohort_end_date"


# ---------------------------------------------------------------------------
# standard_concept_id_column
# ---------------------------------------------------------------------------


class TestStandardConceptIdColumn:
    def test_condition_occurrence(self):
        assert standard_concept_id_column("condition_occurrence") == "condition_concept_id"

    def test_drug_exposure(self):
        assert standard_concept_id_column("drug_exposure") == "drug_concept_id"

    def test_death(self):
        assert standard_concept_id_column("death") == "cause_concept_id"

    def test_cohort_default(self):
        assert standard_concept_id_column("my_cohort") == "cohort_definition_id"


# ---------------------------------------------------------------------------
# source_concept_id_column
# ---------------------------------------------------------------------------


class TestSourceConceptIdColumn:
    def test_condition_occurrence(self):
        assert source_concept_id_column("condition_occurrence") == "condition_source_concept_id"

    def test_drug_exposure(self):
        assert source_concept_id_column("drug_exposure") == "drug_source_concept_id"

    def test_cohort_default(self):
        assert source_concept_id_column("my_cohort") == "cohort_definition_id"


# ---------------------------------------------------------------------------
# person_id_column
# ---------------------------------------------------------------------------


class TestPersonIdColumn:
    def test_person_id(self):
        assert person_id_column(["person_id", "start_date"]) == "person_id"

    def test_subject_id(self):
        assert person_id_column(["subject_id", "start_date"]) == "subject_id"

    def test_person_id_preferred(self):
        """person_id takes priority over subject_id."""
        assert person_id_column(["person_id", "subject_id"]) == "person_id"

    def test_neither_raises(self):
        with pytest.raises(ValueError, match="no person identifier"):
            person_id_column(["id", "name"])

    def test_tuple_input(self):
        assert person_id_column(("person_id", "date")) == "person_id"
