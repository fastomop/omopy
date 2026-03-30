"""Tests for omopy.profiles._demographics — demographics engine.

Uses the Synthea test database (27 persons, CDM v5.4).
"""

from __future__ import annotations

import polars as pl
import pytest

from omopy.profiles import (
    add_age,
    add_date_of_birth,
    add_demographics,
    add_future_observation,
    add_in_observation,
    add_prior_observation,
    add_sex,
)


# ---------------------------------------------------------------------------
# add_sex
# ---------------------------------------------------------------------------


class TestAddSex:
    def test_adds_sex_column(self, synthea_cdm):
        """add_sex should add a 'sex' column to the person table."""
        person = synthea_cdm["person"]
        result = add_sex(person, synthea_cdm)
        df = result.collect()
        assert "sex" in df.columns
        # All values should be Male, Female, or None
        values = set(df["sex"].to_list())
        assert values <= {"Male", "Female", "None"}

    def test_correct_sex_counts(self, synthea_cdm):
        """Synthea has 14 males and 13 females."""
        person = synthea_cdm["person"]
        result = add_sex(person, synthea_cdm)
        df = result.collect()
        counts = df.group_by("sex").len().sort("sex")
        sex_map = dict(zip(counts["sex"].to_list(), counts["len"].to_list()))
        assert sex_map.get("Male") == 14
        assert sex_map.get("Female") == 13

    def test_custom_sex_name(self, synthea_cdm):
        person = synthea_cdm["person"]
        result = add_sex(person, synthea_cdm, sex_name="gender")
        df = result.collect()
        assert "gender" in df.columns
        assert "sex" not in df.columns

    def test_custom_missing_value(self, synthea_cdm):
        """Test custom missing sex value (all Synthea persons have sex)."""
        person = synthea_cdm["person"]
        result = add_sex(person, synthea_cdm, missing_sex_value="Unknown")
        df = result.collect()
        # No unknowns expected in Synthea
        assert "Unknown" not in df["sex"].to_list()


# ---------------------------------------------------------------------------
# add_date_of_birth
# ---------------------------------------------------------------------------


class TestAddDateOfBirth:
    def test_adds_dob_column(self, synthea_cdm):
        person = synthea_cdm["person"]
        result = add_date_of_birth(person, synthea_cdm)
        df = result.collect()
        assert "date_of_birth" in df.columns

    def test_dob_not_null_for_all(self, synthea_cdm):
        """All 27 Synthea persons should have a date of birth."""
        person = synthea_cdm["person"]
        result = add_date_of_birth(person, synthea_cdm)
        df = result.collect()
        assert df["date_of_birth"].null_count() == 0
        assert len(df) == 27

    def test_custom_name(self, synthea_cdm):
        person = synthea_cdm["person"]
        result = add_date_of_birth(person, synthea_cdm, date_of_birth_name="birth_date")
        df = result.collect()
        assert "birth_date" in df.columns

    def test_year_matches_person_table(self, synthea_cdm):
        """Birth year from constructed DoB should match year_of_birth."""
        person = synthea_cdm["person"]
        result = add_date_of_birth(person, synthea_cdm)
        df = result.collect()
        years = df["date_of_birth"].dt.year().to_list()
        expected = df["year_of_birth"].to_list()
        for y, e in zip(years, expected):
            assert y == e


# ---------------------------------------------------------------------------
# add_age
# ---------------------------------------------------------------------------


class TestAddAge:
    def test_adds_age_column(self, synthea_cdm):
        """Person table doesn't have cohort_start_date, so use a table with dates."""
        # Use observation_period which has person_id and observation_period_start_date
        obs = synthea_cdm["observation_period"]
        result = add_age(obs, synthea_cdm, index_date="observation_period_start_date")
        df = result.collect()
        assert "age" in df.columns

    def test_age_non_negative(self, synthea_cdm):
        """Ages at observation start should be non-negative."""
        obs = synthea_cdm["observation_period"]
        result = add_age(obs, synthea_cdm, index_date="observation_period_start_date")
        df = result.collect()
        ages = df["age"].drop_nulls()
        assert (ages >= 0).all()

    def test_age_reasonable_range(self, synthea_cdm):
        """Ages should be between 0 and 150."""
        obs = synthea_cdm["observation_period"]
        result = add_age(obs, synthea_cdm, index_date="observation_period_start_date")
        df = result.collect()
        ages = df["age"].drop_nulls()
        assert (ages >= 0).all()
        assert (ages <= 150).all()

    def test_age_unit_months(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_age(
            obs, synthea_cdm, index_date="observation_period_start_date", age_unit="months"
        )
        df = result.collect()
        ages = df["age"].drop_nulls()
        # Monthly ages should be >= 0 and generally > 12 for adults
        assert (ages >= 0).all()

    def test_age_unit_days(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_age(
            obs, synthea_cdm, index_date="observation_period_start_date", age_unit="days"
        )
        df = result.collect()
        ages = df["age"].drop_nulls()
        assert (ages >= 0).all()
        # Day ages should be larger numbers
        assert ages.max() > 365

    def test_custom_age_name(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_age(
            obs, synthea_cdm, index_date="observation_period_start_date", age_name="patient_age"
        )
        df = result.collect()
        assert "patient_age" in df.columns
        assert "age" not in df.columns

    def test_age_group_list(self, synthea_cdm):
        """Test age grouping with a list of ranges."""
        obs = synthea_cdm["observation_period"]
        result = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            age_group=[(0, 17), (18, 64), (65, float("inf"))],
        )
        df = result.collect()
        assert "age" in df.columns
        assert "age_group" in df.columns
        group_values = set(df["age_group"].drop_nulls().to_list())
        # Should only contain expected labels (some may be absent)
        assert group_values <= {"0 to 17", "18 to 64", "65 or above", "None"}

    def test_age_group_dict(self, synthea_cdm):
        """Test age grouping with a dict of label -> range."""
        obs = synthea_cdm["observation_period"]
        result = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            age_group={"child": (0, 17), "adult": (18, 64), "senior": (65, float("inf"))},
        )
        df = result.collect()
        assert "age_group" in df.columns
        group_values = set(df["age_group"].drop_nulls().to_list())
        assert group_values <= {"child", "adult", "senior", "None"}


# ---------------------------------------------------------------------------
# add_prior_observation / add_future_observation
# ---------------------------------------------------------------------------


class TestAddPriorObservation:
    def test_adds_prior_obs_days(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_prior_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "prior_observation" in df.columns
        # At observation start, prior observation should be 0
        values = df["prior_observation"].drop_nulls()
        assert (values >= 0).all()
        # At the very start, prior obs should be exactly 0
        assert 0 in values.to_list()

    def test_prior_obs_date_type(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_prior_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            prior_observation_type="date",
        )
        df = result.collect()
        assert "prior_observation" in df.columns
        # Should be a date column
        assert df["prior_observation"].dtype in (pl.Date, pl.Datetime)


class TestAddFutureObservation:
    def test_adds_future_obs_days(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_future_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "future_observation" in df.columns
        values = df["future_observation"].drop_nulls()
        # At observation start, future observation should be > 0
        assert (values >= 0).all()

    def test_future_obs_date_type(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_future_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            future_observation_type="date",
        )
        df = result.collect()
        assert "future_observation" in df.columns
        assert df["future_observation"].dtype in (pl.Date, pl.Datetime)


# ---------------------------------------------------------------------------
# add_demographics (combined)
# ---------------------------------------------------------------------------


class TestAddDemographics:
    def test_all_demographics(self, synthea_cdm):
        """Add all demographics at once."""
        obs = synthea_cdm["observation_period"]
        result = add_demographics(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            age=True,
            sex=True,
            prior_observation=True,
            future_observation=True,
            date_of_birth=True,
        )
        df = result.collect()
        assert "age" in df.columns
        assert "sex" in df.columns
        assert "prior_observation" in df.columns
        assert "future_observation" in df.columns
        assert "date_of_birth" in df.columns
        assert len(df) == 27

    def test_preserves_original_columns(self, synthea_cdm):
        """Original columns should not be lost."""
        obs = synthea_cdm["observation_period"]
        orig_cols = obs.columns
        result = add_demographics(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        for col in orig_cols:
            assert col in df.columns

    def test_no_demographics_requested(self, synthea_cdm):
        """When nothing is requested, table should be unchanged."""
        obs = synthea_cdm["observation_period"]
        result = add_demographics(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            age=False,
            sex=False,
            prior_observation=False,
            future_observation=False,
            date_of_birth=False,
        )
        df = result.collect()
        assert len(df) == 27

    def test_uses_table_cdm_reference(self, synthea_cdm):
        """Should work without explicit cdm parameter if table has cdm backref."""
        obs = synthea_cdm["observation_period"]
        # obs.cdm should be set by CdmReference
        result = add_sex(obs)  # No cdm argument
        df = result.collect()
        assert "sex" in df.columns

    def test_returns_cdm_table(self, synthea_cdm):
        """Result should be a CdmTable, not raw Ibis."""
        from omopy.generics.cdm_table import CdmTable

        obs = synthea_cdm["observation_period"]
        result = add_sex(obs, synthea_cdm)
        assert isinstance(result, CdmTable)

    def test_preserves_tbl_name(self, synthea_cdm):
        """CdmTable metadata should be preserved."""
        obs = synthea_cdm["observation_period"]
        result = add_sex(obs, synthea_cdm)
        assert result.tbl_name == obs.tbl_name

    def test_no_cdm_raises(self):
        """Should raise if no CDM reference is available."""
        from omopy.generics.cdm_table import CdmTable
        import ibis

        t = ibis.table({"person_id": "int64", "start_date": "date"}, name="test")
        tbl = CdmTable(t, tbl_name="test")
        with pytest.raises(ValueError, match="No CDM reference"):
            add_sex(tbl)


# ---------------------------------------------------------------------------
# add_in_observation
# ---------------------------------------------------------------------------


class TestAddInObservation:
    def test_at_index_date(self, synthea_cdm):
        """With window (0,0), everyone at obs start should be in observation."""
        obs = synthea_cdm["observation_period"]
        result = add_in_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "in_observation" in df.columns
        # All rows should be in observation at their own start date
        assert (df["in_observation"] == 1).all()

    def test_wide_window(self, synthea_cdm):
        """With infinite window, check overlap with observation period."""
        obs = synthea_cdm["observation_period"]
        result = add_in_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(float("-inf"), float("inf")),
        )
        df = result.collect()
        assert "in_observation" in df.columns
        # All should be 1 since they match their own obs period
        assert (df["in_observation"] == 1).all()

    def test_custom_name_style(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_in_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            name_style="in_obs_{window_name}",
            window=[(0, 0), (0, 365)],
        )
        df = result.collect()
        assert "in_obs_0_to_0" in df.columns
        assert "in_obs_0_to_365" in df.columns
