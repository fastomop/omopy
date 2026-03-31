"""Tests for omopy.generics.cohort_table — CohortTable."""

import polars as pl
import pytest

from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import COHORT_REQUIRED_COLUMNS, CohortTable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cohort_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cohort_definition_id": [1, 1, 2, 2, 2],
            "subject_id": [101, 102, 101, 103, 104],
            "cohort_start_date": ["2020-01-01"] * 5,
            "cohort_end_date": ["2020-12-31"] * 5,
        }
    )


# ---------------------------------------------------------------------------
# CohortTable basics
# ---------------------------------------------------------------------------


class TestCohortTableBasics:
    def test_creation(self):
        ct = CohortTable(_cohort_df())
        assert ct.tbl_name == "cohort"  # default
        assert ct.tbl_source == "local"

    def test_required_columns_present(self):
        for col in COHORT_REQUIRED_COLUMNS:
            assert col in CohortTable(_cohort_df()).columns

    def test_missing_required_column_raises(self):
        df = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "subject_id": [101],
                # missing start_date and end_date
            }
        )
        with pytest.raises(ValueError, match="missing required columns"):
            CohortTable(df)

    def test_default_settings(self):
        ct = CohortTable(_cohort_df())
        settings = ct.settings
        assert "cohort_definition_id" in settings.columns
        assert "cohort_name" in settings.columns
        ids = settings["cohort_definition_id"].to_list()
        assert sorted(ids) == [1, 2]

    def test_default_attrition(self):
        ct = CohortTable(_cohort_df())
        attrition = ct.attrition
        assert "cohort_definition_id" in attrition.columns
        assert len(attrition) == 0  # empty by default

    def test_default_cohort_codelist(self):
        ct = CohortTable(_cohort_df())
        codelist = ct.cohort_codelist
        assert "cohort_definition_id" in codelist.columns
        assert len(codelist) == 0

    def test_cohort_ids(self):
        ct = CohortTable(_cohort_df())
        assert sorted(ct.cohort_ids) == [1, 2]

    def test_cohort_names(self):
        ct = CohortTable(_cohort_df())
        names = ct.cohort_names
        assert len(names) == 2
        assert all(isinstance(n, str) for n in names)

    def test_custom_settings(self):
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1, 2],
                "cohort_name": ["Diabetes", "Hypertension"],
            }
        )
        ct = CohortTable(_cohort_df(), settings=settings)
        assert ct.cohort_names == ["Diabetes", "Hypertension"]


# ---------------------------------------------------------------------------
# CohortTable.cohort_count
# ---------------------------------------------------------------------------


class TestCohortCount:
    def test_cohort_count(self):
        ct = CohortTable(_cohort_df())
        counts = ct.cohort_count()
        assert "number_records" in counts.columns
        assert "number_subjects" in counts.columns

        # cohort 1: 2 records, 2 subjects
        c1 = counts.filter(pl.col("cohort_definition_id") == 1)
        assert c1["number_records"].item() == 2
        assert c1["number_subjects"].item() == 2

        # cohort 2: 3 records, 3 subjects
        c2 = counts.filter(pl.col("cohort_definition_id") == 2)
        assert c2["number_records"].item() == 3
        assert c2["number_subjects"].item() == 3


# ---------------------------------------------------------------------------
# CohortTable settings setter validation
# ---------------------------------------------------------------------------


class TestCohortTableSettingsSetter:
    def test_valid_settings(self):
        ct = CohortTable(_cohort_df())
        new_settings = pl.DataFrame(
            {
                "cohort_definition_id": [1, 2],
                "cohort_name": ["A", "B"],
            }
        )
        ct.settings = new_settings
        assert ct.cohort_names == ["A", "B"]

    def test_settings_missing_id_raises(self):
        ct = CohortTable(_cohort_df())
        with pytest.raises(ValueError, match="cohort_definition_id"):
            ct.settings = pl.DataFrame({"cohort_name": ["A"]})

    def test_settings_missing_name_raises(self):
        ct = CohortTable(_cohort_df())
        with pytest.raises(ValueError, match="cohort_name"):
            ct.settings = pl.DataFrame({"cohort_definition_id": [1]})


# ---------------------------------------------------------------------------
# CohortTable transformation metadata preservation
# ---------------------------------------------------------------------------


class TestCohortTableTransformations:
    def test_filter_preserves_cohort_metadata(self):
        settings = pl.DataFrame(
            {
                "cohort_definition_id": [1, 2],
                "cohort_name": ["A", "B"],
            }
        )
        ct = CohortTable(_cohort_df(), settings=settings)
        filtered = ct.filter(pl.col("cohort_definition_id") == 1)
        assert isinstance(filtered, CohortTable)
        assert filtered.settings is settings  # same settings object

    def test_select_losing_cohort_id_downgrades(self):
        ct = CohortTable(_cohort_df())
        selected = ct.select("subject_id")  # drops cohort_definition_id
        # Should downgrade to CdmTable since cohort_definition_id is gone
        assert isinstance(selected, CdmTable)

    def test_head_preserves(self):
        ct = CohortTable(_cohort_df())
        h = ct.head(2)
        assert isinstance(h, CohortTable)

    def test_repr(self):
        ct = CohortTable(_cohort_df())
        r = repr(ct)
        assert "CohortTable" in r
        assert "cohort" in r
