"""Tests for omopy.incidence module.

Covers:
- Module import and exports
- Mock data generation
- Denominator cohort generation (general + target-based)
- Incidence estimation
- Point prevalence estimation
- Period prevalence estimation
- Result conversion
- Table and plot functions
- Confidence intervals
- Calendar interval generation
- Integration tests against Synthea database
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

import omopy  # noqa: F401 — triggers CPython 3.14 typing compat shim
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import SummarisedResult

# ===================================================================
# Module imports
# ===================================================================


class TestModuleExports:
    """Verify the module exports are complete and importable."""

    def test_all_exports_count(self):
        import omopy.incidence

        assert len(omopy.incidence.__all__) == 21

    def test_import_denominator(self):
        from omopy.incidence import (
            generate_denominator_cohort_set,
            generate_target_denominator_cohort_set,
        )

        assert callable(generate_denominator_cohort_set)
        assert callable(generate_target_denominator_cohort_set)

    def test_import_estimate(self):
        from omopy.incidence import (
            estimate_incidence,
            estimate_period_prevalence,
            estimate_point_prevalence,
        )

        assert callable(estimate_incidence)
        assert callable(estimate_period_prevalence)
        assert callable(estimate_point_prevalence)

    def test_import_result(self):
        from omopy.incidence import as_incidence_result, as_prevalence_result

        assert callable(as_incidence_result)
        assert callable(as_prevalence_result)

    def test_import_table(self):
        from omopy.incidence import (
            options_table_incidence,
            options_table_prevalence,
            table_incidence,
            table_incidence_attrition,
            table_prevalence,
            table_prevalence_attrition,
        )

        assert callable(table_incidence)
        assert callable(table_prevalence)
        assert callable(table_incidence_attrition)
        assert callable(table_prevalence_attrition)
        assert callable(options_table_incidence)
        assert callable(options_table_prevalence)

    def test_import_plot(self):
        from omopy.incidence import (
            available_incidence_grouping,
            available_prevalence_grouping,
            plot_incidence,
            plot_incidence_population,
            plot_prevalence,
            plot_prevalence_population,
        )

        assert callable(plot_incidence)
        assert callable(plot_prevalence)
        assert callable(plot_incidence_population)
        assert callable(plot_prevalence_population)
        assert callable(available_incidence_grouping)
        assert callable(available_prevalence_grouping)

    def test_import_mock(self):
        from omopy.incidence import (
            benchmark_incidence_prevalence,
            mock_incidence_prevalence,
        )

        assert callable(mock_incidence_prevalence)
        assert callable(benchmark_incidence_prevalence)


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture()
def mock_cdm():
    """Create a mock CDM for testing."""
    from omopy.incidence import mock_incidence_prevalence

    return mock_incidence_prevalence(sample_size=50, seed=42)


@pytest.fixture()
def mock_cdm_with_denom(mock_cdm):
    """Create a mock CDM with denominator cohort generated."""
    from omopy.incidence import generate_denominator_cohort_set

    return generate_denominator_cohort_set(mock_cdm, name="denominator")


# ===================================================================
# Mock data generation
# ===================================================================


class TestMock:
    """Tests for mock_incidence_prevalence."""

    def test_basic_mock(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm = mock_incidence_prevalence(sample_size=20, seed=42)
        assert isinstance(cdm, CdmReference)
        assert "person" in cdm.table_names
        assert "observation_period" in cdm.table_names
        assert "target" in cdm.table_names
        assert "outcome" in cdm.table_names

    def test_mock_person_count(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm = mock_incidence_prevalence(sample_size=30, seed=1)
        person = cdm["person"].collect()
        assert len(person) == 30

    def test_mock_outcome_prevalence(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm = mock_incidence_prevalence(
            sample_size=1000, outcome_prevalence=0.5, seed=99
        )
        outcome = cdm["outcome"].collect()
        # With 50% prevalence and 1000 persons, expect ~500 outcomes
        # Allow wide range due to randomness
        assert 300 < len(outcome) < 700

    def test_mock_cohort_tables(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm = mock_incidence_prevalence(sample_size=10, seed=42)
        target = cdm["target"]
        assert isinstance(target, CohortTable)
        assert target.settings["cohort_name"][0] == "target"

    def test_mock_deterministic(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm1 = mock_incidence_prevalence(sample_size=10, seed=42)
        cdm2 = mock_incidence_prevalence(sample_size=10, seed=42)
        p1 = cdm1["person"].collect()
        p2 = cdm2["person"].collect()
        assert p1.equals(p2)

    def test_mock_custom_dates(self):
        from omopy.incidence import mock_incidence_prevalence

        cdm = mock_incidence_prevalence(
            sample_size=10,
            seed=42,
            study_start=datetime.date(2015, 1, 1),
            study_end=datetime.date(2018, 12, 31),
        )
        obs = cdm["observation_period"].collect()
        assert obs["observation_period_start_date"].min() >= datetime.date(2015, 1, 1)
        assert obs["observation_period_end_date"].max() <= datetime.date(2018, 12, 31)


# ===================================================================
# Denominator generation
# ===================================================================


class TestDenominatorGeneration:
    """Tests for generate_denominator_cohort_set."""

    def test_basic_denominator(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm)
        assert "denominator" in cdm.table_names
        denom = cdm["denominator"]
        assert isinstance(denom, CohortTable)
        assert len(denom.cohort_ids) == 1  # default: one age group, Both, 0 prior

    def test_denominator_data_shape(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm)
        denom = cdm["denominator"].collect()
        assert set(denom.columns) == {
            "cohort_definition_id",
            "subject_id",
            "cohort_start_date",
            "cohort_end_date",
        }
        assert not denom.is_empty()

    def test_denominator_custom_name(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm, name="my_denom")
        assert "my_denom" in cdm.table_names

    def test_denominator_age_groups(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            age_group=[(0, 40), (41, 80), (81, 150)],
        )
        denom = cdm["denominator"]
        assert len(denom.cohort_ids) == 3
        settings = denom.settings
        assert "age_group" in settings.columns

    def test_denominator_sex_filter(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            sex=["Male", "Female"],
        )
        denom = cdm["denominator"]
        assert len(denom.cohort_ids) == 2

    def test_denominator_sex_male_only(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm, sex="Male")
        denom = cdm["denominator"].collect()
        # All subjects should be male
        person = mock_cdm["person"].collect()
        male_ids = set(
            person.filter(pl.col("gender_concept_id") == 8507)["person_id"].to_list()
        )
        denom_ids = set(denom["subject_id"].to_list())
        assert denom_ids <= male_ids

    def test_denominator_interactions(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            age_group=[(0, 50), (51, 150)],
            sex=["Male", "Female"],
            requirement_interactions=True,
        )
        denom = cdm["denominator"]
        # 2 age groups × 2 sexes = 4 cohorts
        assert len(denom.cohort_ids) == 4

    def test_denominator_no_interactions(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            age_group=[(0, 50), (51, 150)],
            sex=["Male", "Female"],
            requirement_interactions=False,
        )
        denom = cdm["denominator"]
        # Non-interaction mode: 2 age + 2 sex = up to 4, minus defaults overlap
        assert len(denom.cohort_ids) >= 2

    def test_denominator_prior_observation(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            days_prior_observation=365,
        )
        denom = cdm["denominator"]
        assert len(denom.cohort_ids) == 1

    def test_denominator_study_window(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            mock_cdm,
            cohort_date_range=(datetime.date(2015, 1, 1), datetime.date(2018, 12, 31)),
        )
        denom = cdm["denominator"].collect()
        if not denom.is_empty():
            assert denom["cohort_start_date"].min() >= datetime.date(2015, 1, 1)
            assert denom["cohort_end_date"].max() <= datetime.date(2018, 12, 31)

    def test_denominator_attrition(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm)
        denom = cdm["denominator"]
        attrition = denom.attrition
        assert not attrition.is_empty()
        assert "reason" in attrition.columns
        assert "number_records" in attrition.columns

    def test_denominator_settings_columns(self, mock_cdm):
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(mock_cdm)
        settings = cdm["denominator"].settings
        assert "cohort_definition_id" in settings.columns
        assert "cohort_name" in settings.columns
        assert "age_group" in settings.columns
        assert "sex" in settings.columns
        assert "days_prior_observation" in settings.columns


# ===================================================================
# Target denominator generation
# ===================================================================


class TestTargetDenominatorGeneration:
    """Tests for generate_target_denominator_cohort_set."""

    def test_basic_target_denominator(self, mock_cdm):
        from omopy.incidence import generate_target_denominator_cohort_set

        cdm = generate_target_denominator_cohort_set(
            mock_cdm,
            target_cohort_table="target",
        )
        assert "denominator" in cdm.table_names
        denom = cdm["denominator"]
        assert isinstance(denom, CohortTable)

    def test_target_denominator_not_cohort(self, mock_cdm):
        from omopy.incidence import generate_target_denominator_cohort_set

        with pytest.raises(TypeError, match="not a CohortTable"):
            generate_target_denominator_cohort_set(
                mock_cdm,
                target_cohort_table="person",
            )

    def test_target_denominator_time_at_risk(self, mock_cdm):
        from omopy.incidence import generate_target_denominator_cohort_set

        cdm = generate_target_denominator_cohort_set(
            mock_cdm,
            target_cohort_table="target",
            time_at_risk=(0, 365),
        )
        denom = cdm["denominator"].collect()
        if not denom.is_empty():
            # Duration should be at most 365 days
            durations = (
                denom["cohort_end_date"] - denom["cohort_start_date"]
            ).dt.total_days()
            assert durations.max() <= 365

    def test_target_denominator_settings(self, mock_cdm):
        from omopy.incidence import generate_target_denominator_cohort_set

        cdm = generate_target_denominator_cohort_set(
            mock_cdm,
            target_cohort_table="target",
        )
        settings = cdm["denominator"].settings
        assert "target_cohort_table" in settings.columns

    def test_target_denominator_infinite_tar(self, mock_cdm):
        from omopy.incidence import generate_target_denominator_cohort_set

        cdm = generate_target_denominator_cohort_set(
            mock_cdm,
            target_cohort_table="target",
            time_at_risk=(0, float("inf")),
        )
        denom = cdm["denominator"]
        assert isinstance(denom, CohortTable)


# ===================================================================
# Incidence estimation
# ===================================================================


class TestEstimateIncidence:
    """Tests for estimate_incidence."""

    def test_basic_incidence(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        assert isinstance(result, SummarisedResult)
        assert not result.data.is_empty()

    def test_incidence_has_rates(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        est_names = result.data["estimate_name"].unique().to_list()
        assert "incidence_100000_pys" in est_names
        assert "n_events" in est_names
        assert "person_years" in est_names
        assert "n_persons" in est_names

    def test_incidence_has_ci(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        est_names = result.data["estimate_name"].unique().to_list()
        assert "incidence_100000_pys_95ci_lower" in est_names
        assert "incidence_100000_pys_95ci_upper" in est_names

    def test_incidence_settings(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        settings = result.settings
        assert "result_type" in settings.columns
        assert settings["result_type"][0] == "incidence"

    def test_incidence_overall_interval(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            interval="overall",
        )
        labels = result.data["variable_level"].unique().to_list()
        assert "overall" in labels

    def test_incidence_monthly_interval(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            interval="months",
        )
        # Should have multiple intervals
        labels = result.data["variable_level"].unique().to_list()
        assert len(labels) > 1

    def test_incidence_quarterly_interval(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            interval="quarters",
        )
        labels = result.data["variable_level"].unique().to_list()
        assert any("Q" in label for label in labels)

    def test_incidence_no_complete_intervals(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            complete_database_intervals=False,
        )
        assert isinstance(result, SummarisedResult)

    def test_incidence_group_names(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence

        result = estimate_incidence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        # Group should contain denominator and outcome cohort names
        groups = result.data["group_name"].unique().to_list()
        assert len(groups) >= 1
        assert "denominator_cohort_name" in groups[0]


# ===================================================================
# Point prevalence
# ===================================================================


class TestEstimatePointPrevalence:
    """Tests for estimate_point_prevalence."""

    def test_basic_point_prevalence(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence

        result = estimate_point_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        assert isinstance(result, SummarisedResult)

    def test_point_prevalence_estimates(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence

        result = estimate_point_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        if not result.data.is_empty():
            est_names = result.data["estimate_name"].unique().to_list()
            assert "prevalence" in est_names
            assert "n_persons" in est_names
            assert "n_cases" in est_names
            assert "prevalence_95ci_lower" in est_names
            assert "prevalence_95ci_upper" in est_names

    def test_point_prevalence_settings(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence

        result = estimate_point_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        settings = result.settings
        assert settings["result_type"][0] == "point_prevalence"
        assert settings["time_point"][0] == "start"

    def test_point_prevalence_time_point_middle(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence

        result = estimate_point_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            time_point="middle",
        )
        assert isinstance(result, SummarisedResult)

    def test_point_prevalence_overall(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence

        result = estimate_point_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            interval="overall",
        )
        if not result.data.is_empty():
            labels = result.data["variable_level"].unique().to_list()
            assert "overall" in labels


# ===================================================================
# Period prevalence
# ===================================================================


class TestEstimatePeriodPrevalence:
    """Tests for estimate_period_prevalence."""

    def test_basic_period_prevalence(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_period_prevalence

        result = estimate_period_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        assert isinstance(result, SummarisedResult)

    def test_period_prevalence_estimates(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_period_prevalence

        result = estimate_period_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        if not result.data.is_empty():
            est_names = result.data["estimate_name"].unique().to_list()
            assert "prevalence" in est_names

    def test_period_prevalence_full_contribution(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_period_prevalence

        result = estimate_period_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
            full_contribution=True,
        )
        assert isinstance(result, SummarisedResult)

    def test_period_prevalence_settings(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_period_prevalence

        result = estimate_period_prevalence(
            mock_cdm_with_denom,
            "denominator",
            "outcome",
        )
        settings = result.settings
        assert settings["result_type"][0] == "period_prevalence"


# ===================================================================
# Result conversion
# ===================================================================


class TestResultConversion:
    """Tests for as_incidence_result and as_prevalence_result."""

    def test_as_incidence_result(self, mock_cdm_with_denom):
        from omopy.incidence import as_incidence_result, estimate_incidence

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        tidy = as_incidence_result(sr)
        assert isinstance(tidy, pl.DataFrame)
        if not tidy.is_empty():
            assert "incidence_100000_pys" in tidy.columns or len(tidy.columns) > 0

    def test_as_prevalence_result(self, mock_cdm_with_denom):
        from omopy.incidence import as_prevalence_result, estimate_point_prevalence

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        tidy = as_prevalence_result(sr)
        assert isinstance(tidy, pl.DataFrame)

    def test_as_incidence_result_with_metadata(self, mock_cdm_with_denom):
        from omopy.incidence import as_incidence_result, estimate_incidence

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        tidy = as_incidence_result(sr, metadata=True)
        assert isinstance(tidy, pl.DataFrame)


# ===================================================================
# Table functions
# ===================================================================


class TestTableFunctions:
    """Tests for table rendering functions."""

    def test_options_table_incidence(self):
        from omopy.incidence import options_table_incidence

        opts = options_table_incidence()
        assert isinstance(opts, dict)
        assert "header" in opts
        assert "estimate_name" in opts

    def test_options_table_prevalence(self):
        from omopy.incidence import options_table_prevalence

        opts = options_table_prevalence()
        assert isinstance(opts, dict)
        assert "header" in opts

    def test_table_incidence_polars(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence, table_incidence

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        result = table_incidence(sr, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_table_prevalence_polars(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence, table_prevalence

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        result = table_prevalence(sr, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_table_incidence_attrition_polars(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence, table_incidence_attrition

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        result = table_incidence_attrition(sr, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_table_prevalence_attrition_polars(self, mock_cdm_with_denom):
        from omopy.incidence import (
            estimate_point_prevalence,
            table_prevalence_attrition,
        )

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        result = table_prevalence_attrition(sr, type="polars")
        assert isinstance(result, pl.DataFrame)


# ===================================================================
# Plot functions
# ===================================================================


class TestPlotFunctions:
    """Tests for plot rendering functions."""

    def test_plot_incidence(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence, plot_incidence

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        fig = plot_incidence(sr)
        assert fig is not None
        assert hasattr(fig, "to_json")  # plotly Figure

    def test_plot_prevalence(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_point_prevalence, plot_prevalence

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        fig = plot_prevalence(sr)
        assert fig is not None

    def test_plot_incidence_population(self, mock_cdm_with_denom):
        from omopy.incidence import estimate_incidence, plot_incidence_population

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        fig = plot_incidence_population(sr)
        assert fig is not None

    def test_plot_prevalence_population(self, mock_cdm_with_denom):
        from omopy.incidence import (
            estimate_point_prevalence,
            plot_prevalence_population,
        )

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        fig = plot_prevalence_population(sr)
        assert fig is not None

    def test_available_incidence_grouping(self, mock_cdm_with_denom):
        from omopy.incidence import (
            available_incidence_grouping,
            estimate_incidence,
        )

        sr = estimate_incidence(mock_cdm_with_denom, "denominator", "outcome")
        groups = available_incidence_grouping(sr)
        assert isinstance(groups, list)

    def test_available_prevalence_grouping(self, mock_cdm_with_denom):
        from omopy.incidence import (
            available_prevalence_grouping,
            estimate_point_prevalence,
        )

        sr = estimate_point_prevalence(mock_cdm_with_denom, "denominator", "outcome")
        groups = available_prevalence_grouping(sr)
        assert isinstance(groups, list)


# ===================================================================
# Confidence intervals (unit tests)
# ===================================================================


class TestConfidenceIntervals:
    """Unit tests for CI computation."""

    def test_poisson_ci_zero_events(self):
        from omopy.incidence._estimate import _poisson_ci

        lower, upper = _poisson_ci(0, 100.0)
        assert lower == 0.0
        assert upper > 0.0

    def test_poisson_ci_positive_events(self):
        from omopy.incidence._estimate import _poisson_ci

        lower, upper = _poisson_ci(10, 100.0)
        assert lower > 0.0
        assert upper > lower
        # Rate: 10/100 * 100,000 = 10,000
        ir = 10 / 100 * 100_000
        assert lower < ir < upper

    def test_poisson_ci_zero_person_years(self):
        from omopy.incidence._estimate import _poisson_ci

        lower, upper = _poisson_ci(5, 0.0)
        assert lower == 0.0
        assert upper == 0.0

    def test_wilson_ci_zero(self):
        from omopy.incidence._estimate import _wilson_ci

        lower, upper = _wilson_ci(0, 100)
        assert lower < 1e-10  # essentially 0
        assert upper > 0.0

    def test_wilson_ci_all(self):
        from omopy.incidence._estimate import _wilson_ci

        lower, upper = _wilson_ci(100, 100)
        assert lower > 0.0
        assert upper == 1.0

    def test_wilson_ci_normal(self):
        from omopy.incidence._estimate import _wilson_ci

        lower, upper = _wilson_ci(50, 100)
        assert lower < 0.5 < upper
        assert lower >= 0.0
        assert upper <= 1.0

    def test_wilson_ci_empty_population(self):
        from omopy.incidence._estimate import _wilson_ci

        lower, upper = _wilson_ci(0, 0)
        assert lower == 0.0
        assert upper == 0.0


# ===================================================================
# Calendar interval generation (unit tests)
# ===================================================================


class TestCalendarIntervals:
    """Unit tests for interval generation utilities."""

    def test_yearly_intervals(self):
        from omopy.incidence._estimate import _generate_intervals

        denom = pl.DataFrame(
            {
                "cohort_start_date": [datetime.date(2015, 6, 1)],
                "cohort_end_date": [datetime.date(2018, 3, 15)],
            }
        )
        intervals = _generate_intervals(denom, "years")
        assert len(intervals) == 4  # 2015, 2016, 2017, 2018
        assert intervals["interval_label"][0] == "2015"

    def test_quarterly_intervals(self):
        from omopy.incidence._estimate import _generate_intervals

        denom = pl.DataFrame(
            {
                "cohort_start_date": [datetime.date(2020, 1, 1)],
                "cohort_end_date": [datetime.date(2020, 12, 31)],
            }
        )
        intervals = _generate_intervals(denom, "quarters")
        assert len(intervals) == 4
        labels = intervals["interval_label"].to_list()
        assert "2020 Q1" in labels
        assert "2020 Q4" in labels

    def test_monthly_intervals(self):
        from omopy.incidence._estimate import _generate_intervals

        denom = pl.DataFrame(
            {
                "cohort_start_date": [datetime.date(2020, 1, 1)],
                "cohort_end_date": [datetime.date(2020, 6, 30)],
            }
        )
        intervals = _generate_intervals(denom, "months")
        assert len(intervals) == 6

    def test_weekly_intervals(self):
        from omopy.incidence._estimate import _generate_intervals

        denom = pl.DataFrame(
            {
                "cohort_start_date": [datetime.date(2020, 1, 1)],
                "cohort_end_date": [datetime.date(2020, 1, 31)],
            }
        )
        intervals = _generate_intervals(denom, "weeks")
        assert len(intervals) >= 4

    def test_overall_interval(self):
        from omopy.incidence._estimate import _generate_intervals

        denom = pl.DataFrame(
            {
                "cohort_start_date": [datetime.date(2015, 1, 1)],
                "cohort_end_date": [datetime.date(2020, 12, 31)],
            }
        )
        intervals = _generate_intervals(denom, "overall")
        assert len(intervals) == 1
        assert intervals["interval_label"][0] == "overall"


# ===================================================================
# Washout logic (unit tests)
# ===================================================================


class TestWashout:
    """Unit tests for outcome washout logic."""

    def test_first_event_only(self):
        from omopy.incidence._estimate import _apply_washout

        events = pl.DataFrame(
            {
                "person_id": [1, 1, 2, 2, 2],
                "outcome_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 6, 1),
                    datetime.date(2020, 3, 1),
                    datetime.date(2020, 7, 1),
                    datetime.date(2020, 12, 1),
                ],
            }
        )
        result = _apply_washout(events, float("inf"), repeated_events=False)
        assert len(result) == 2  # one per person

    def test_repeated_events_finite_washout(self):
        from omopy.incidence._estimate import _apply_washout

        events = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "outcome_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 1, 15),  # 14 days later
                    datetime.date(2020, 4, 1),  # 76 days later
                ],
            }
        )
        # Washout of 30 days
        result = _apply_washout(events, 30, repeated_events=True)
        # First event kept, second excluded (14 < 30), third kept (76 > 30)
        assert len(result) == 2

    def test_empty_events(self):
        from omopy.incidence._estimate import _apply_washout

        events = pl.DataFrame(schema={"person_id": pl.Int64, "outcome_date": pl.Date})
        result = _apply_washout(events, float("inf"), repeated_events=False)
        assert result.is_empty()


# ===================================================================
# Benchmark
# ===================================================================


class TestBenchmark:
    """Tests for benchmark_incidence_prevalence."""

    def test_benchmark_runs(self, mock_cdm):
        from omopy.incidence import benchmark_incidence_prevalence

        results = benchmark_incidence_prevalence(mock_cdm)
        assert isinstance(results, dict)
        assert "denominator_generation" in results
        assert results["denominator_generation"] > 0


# ===================================================================
# Integration tests against Synthea database
# ===================================================================


class TestSyntheaIntegration:
    """Integration tests using the Synthea database."""

    def test_denominator_from_synthea(self, synthea_cdm):
        """Generate denominator from real OMOP data."""
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(synthea_cdm, name="denom")
        denom = cdm["denom"]
        assert isinstance(denom, CohortTable)
        assert not denom.collect().is_empty()
        # Should have subjects from the 27-person database
        assert denom.collect()["subject_id"].n_unique() <= 27

    def test_denominator_age_stratified(self, synthea_cdm):
        """Age-stratified denominator from Synthea."""
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            synthea_cdm,
            name="denom",
            age_group=[(0, 40), (41, 65), (66, 150)],
        )
        denom = cdm["denom"]
        assert len(denom.cohort_ids) == 3

    def test_denominator_sex_stratified(self, synthea_cdm):
        """Sex-stratified denominator from Synthea."""
        from omopy.incidence import generate_denominator_cohort_set

        cdm = generate_denominator_cohort_set(
            synthea_cdm,
            name="denom",
            sex=["Male", "Female"],
        )
        denom = cdm["denom"]
        assert len(denom.cohort_ids) == 2

    def test_incidence_on_conditions(self, synthea_cdm):
        """Estimate incidence using Synthea data.

        Uses a manually constructed outcome cohort from conditions.
        """
        from omopy.incidence import (
            estimate_incidence,
            generate_denominator_cohort_set,
        )

        # Generate denominator
        cdm = generate_denominator_cohort_set(synthea_cdm, name="denom")

        # Create a simple outcome cohort from condition_occurrence
        cond = cdm["condition_occurrence"].collect()
        if cond.is_empty():
            pytest.skip("No condition data in Synthea database")

        # Use first condition for each person as the outcome
        outcome_df = (
            cond.select(
                pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"),
                pl.col("person_id").cast(pl.Int64).alias("subject_id"),
                pl.col("condition_start_date").alias("cohort_start_date"),
                pl.col("condition_end_date").alias("cohort_end_date"),
            )
            .sort("subject_id", "cohort_start_date")
            .group_by("subject_id")
            .first()
            .with_columns(pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"))
        )
        # Fill null end dates
        outcome_df = outcome_df.with_columns(
            pl.col("cohort_end_date").fill_null(pl.col("cohort_start_date"))
        )

        outcome_settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["first_condition"],
            }
        ).cast({"cohort_definition_id": pl.Int64})

        cdm["outcome"] = CohortTable(
            outcome_df,
            tbl_name="outcome",
            settings=outcome_settings,
        )

        result = estimate_incidence(cdm, "denom", "outcome", interval="overall")
        assert isinstance(result, SummarisedResult)
        assert not result.data.is_empty()

        # Should have incidence rate
        ir_rows = result.data.filter(pl.col("estimate_name") == "incidence_100000_pys")
        assert not ir_rows.is_empty()
        ir_value = float(ir_rows["estimate_value"][0])
        assert ir_value > 0

    def test_prevalence_on_conditions(self, synthea_cdm):
        """Estimate point prevalence using Synthea data."""
        from omopy.incidence import (
            estimate_point_prevalence,
            generate_denominator_cohort_set,
        )

        cdm = generate_denominator_cohort_set(synthea_cdm, name="denom")

        # Create outcome cohort from conditions (all conditions, not just first)
        cond = cdm["condition_occurrence"].collect()
        if cond.is_empty():
            pytest.skip("No condition data in Synthea database")

        outcome_df = cond.select(
            pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"),
            pl.col("person_id").cast(pl.Int64).alias("subject_id"),
            pl.col("condition_start_date").alias("cohort_start_date"),
            pl.col("condition_end_date")
            .fill_null(pl.col("condition_start_date") + pl.duration(days=30))
            .alias("cohort_end_date"),
        )

        outcome_settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["conditions"],
            }
        ).cast({"cohort_definition_id": pl.Int64})

        cdm["outcome"] = CohortTable(
            outcome_df,
            tbl_name="outcome",
            settings=outcome_settings,
        )

        result = estimate_point_prevalence(cdm, "denom", "outcome", interval="years")
        assert isinstance(result, SummarisedResult)

    def test_period_prevalence_on_conditions(self, synthea_cdm):
        """Estimate period prevalence using Synthea data."""
        from omopy.incidence import (
            estimate_period_prevalence,
            generate_denominator_cohort_set,
        )

        cdm = generate_denominator_cohort_set(synthea_cdm, name="denom")

        cond = cdm["condition_occurrence"].collect()
        if cond.is_empty():
            pytest.skip("No condition data in Synthea database")

        outcome_df = cond.select(
            pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"),
            pl.col("person_id").cast(pl.Int64).alias("subject_id"),
            pl.col("condition_start_date").alias("cohort_start_date"),
            pl.col("condition_end_date")
            .fill_null(pl.col("condition_start_date") + pl.duration(days=30))
            .alias("cohort_end_date"),
        )

        outcome_settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["conditions"],
            }
        ).cast({"cohort_definition_id": pl.Int64})

        cdm["outcome"] = CohortTable(
            outcome_df,
            tbl_name="outcome",
            settings=outcome_settings,
        )

        result = estimate_period_prevalence(cdm, "denom", "outcome", interval="years")
        assert isinstance(result, SummarisedResult)

    def test_result_conversion_on_synthea(self, synthea_cdm):
        """Test result conversion with real data."""
        from omopy.incidence import (
            as_incidence_result,
            estimate_incidence,
            generate_denominator_cohort_set,
        )

        cdm = generate_denominator_cohort_set(synthea_cdm, name="denom")

        cond = cdm["condition_occurrence"].collect()
        if cond.is_empty():
            pytest.skip("No condition data")

        outcome_df = (
            cond.select(
                pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"),
                pl.col("person_id").cast(pl.Int64).alias("subject_id"),
                pl.col("condition_start_date").alias("cohort_start_date"),
                pl.col("condition_end_date")
                .fill_null(pl.col("condition_start_date"))
                .alias("cohort_end_date"),
            )
            .sort("subject_id", "cohort_start_date")
            .group_by("subject_id")
            .first()
            .with_columns(pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"))
        )

        outcome_settings = pl.DataFrame(
            {
                "cohort_definition_id": [1],
                "cohort_name": ["first_condition"],
            }
        ).cast({"cohort_definition_id": pl.Int64})

        cdm["outcome"] = CohortTable(
            outcome_df,
            tbl_name="outcome",
            settings=outcome_settings,
        )

        sr = estimate_incidence(cdm, "denom", "outcome", interval="overall")
        tidy = as_incidence_result(sr)
        assert isinstance(tidy, pl.DataFrame)
        assert not tidy.is_empty()
