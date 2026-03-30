"""Tests for omopy.survival — Cohort survival analysis module.

Tests cover:
- Module imports and exports
- Mock data generation
- add_cohort_survival() function
- Single-event (Kaplan-Meier) survival estimation
- Competing risk (Aalen-Johansen) survival estimation
- Result conversion (as_survival_result)
- Table rendering functions
- Plot rendering function
- Available grouping introspection
- Edge cases and parameter variations
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

import omopy
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture()
def mock_cdm():
    """Create a mock CDM for survival testing."""
    from omopy.survival import mock_survival

    return mock_survival(n_persons=100, seed=42)


@pytest.fixture()
def mock_cdm_small():
    """Small mock CDM for quick tests."""
    from omopy.survival import mock_survival

    return mock_survival(n_persons=30, seed=123)


@pytest.fixture()
def mock_cdm_no_events():
    """Mock CDM with no events."""
    from omopy.survival import mock_survival

    return mock_survival(n_persons=50, seed=99, event_rate=0.0, competing_rate=0.0)


@pytest.fixture()
def single_event_result(mock_cdm):
    """Pre-computed single event survival result."""
    from omopy.survival import estimate_single_event_survival

    return estimate_single_event_survival(
        mock_cdm,
        target_cohort_table="target",
        outcome_cohort_table="outcome",
        estimate_gap=30,
    )


@pytest.fixture()
def competing_risk_result(mock_cdm):
    """Pre-computed competing risk result."""
    from omopy.survival import estimate_competing_risk_survival

    return estimate_competing_risk_survival(
        mock_cdm,
        target_cohort_table="target",
        outcome_cohort_table="outcome",
        competing_outcome_cohort_table="competing",
        estimate_gap=30,
    )


# ===================================================================
# Module imports
# ===================================================================


class TestModuleImports:
    """Verify all exports are importable and callable."""

    def test_import_module(self):
        import omopy.survival

        assert hasattr(omopy.survival, "__all__")

    def test_export_count(self):
        import omopy.survival

        assert len(omopy.survival.__all__) == 11

    def test_all_exports_callable(self):
        import omopy.survival

        for name in omopy.survival.__all__:
            obj = getattr(omopy.survival, name)
            assert callable(obj), f"{name} is not callable"

    def test_individual_imports(self):
        from omopy.survival import (
            add_cohort_survival,
            as_survival_result,
            available_survival_grouping,
            estimate_competing_risk_survival,
            estimate_single_event_survival,
            mock_survival,
            options_table_survival,
            plot_survival,
            table_survival,
            table_survival_attrition,
            table_survival_events,
        )

        assert callable(estimate_single_event_survival)
        assert callable(estimate_competing_risk_survival)
        assert callable(add_cohort_survival)
        assert callable(as_survival_result)
        assert callable(table_survival)
        assert callable(table_survival_events)
        assert callable(table_survival_attrition)
        assert callable(options_table_survival)
        assert callable(plot_survival)
        assert callable(available_survival_grouping)
        assert callable(mock_survival)


# ===================================================================
# Mock data
# ===================================================================


class TestMockSurvival:
    """Test the mock data generator."""

    def test_returns_cdm_reference(self):
        from omopy.survival import mock_survival

        cdm = mock_survival(n_persons=20, seed=1)
        assert isinstance(cdm, CdmReference)

    def test_has_required_tables(self, mock_cdm):
        assert "person" in mock_cdm.table_names
        assert "observation_period" in mock_cdm.table_names
        assert "target" in mock_cdm.table_names
        assert "outcome" in mock_cdm.table_names
        assert "competing" in mock_cdm.table_names

    def test_target_is_cohort_table(self, mock_cdm):
        target = mock_cdm["target"]
        assert isinstance(target, CohortTable)
        assert target.settings["cohort_name"][0] == "target"

    def test_outcome_is_cohort_table(self, mock_cdm):
        outcome = mock_cdm["outcome"]
        assert isinstance(outcome, CohortTable)
        assert outcome.settings["cohort_name"][0] == "outcome"

    def test_competing_is_cohort_table(self, mock_cdm):
        competing = mock_cdm["competing"]
        assert isinstance(competing, CohortTable)
        assert competing.settings["cohort_name"][0] == "competing"

    def test_person_count(self, mock_cdm):
        person = mock_cdm["person"].collect()
        assert len(person) == 100

    def test_observation_period_count(self, mock_cdm):
        obs = mock_cdm["observation_period"].collect()
        assert len(obs) == 100

    def test_target_has_strata_columns(self, mock_cdm):
        target = mock_cdm["target"].collect()
        assert "sex" in target.columns
        assert "age_group" in target.columns

    def test_deterministic(self):
        from omopy.survival import mock_survival

        cdm1 = mock_survival(n_persons=20, seed=42)
        cdm2 = mock_survival(n_persons=20, seed=42)
        t1 = cdm1["target"].collect()
        t2 = cdm2["target"].collect()
        assert t1.equals(t2)

    def test_no_strata(self):
        from omopy.survival import mock_survival

        cdm = mock_survival(n_persons=20, seed=42, include_strata=False)
        target = cdm["target"].collect()
        assert "sex" not in target.columns
        assert "age_group" not in target.columns

    def test_custom_names(self):
        from omopy.survival import mock_survival

        cdm = mock_survival(
            n_persons=20,
            seed=42,
            target_name="exposure",
            outcome_name="death",
            competing_name="transplant",
        )
        assert "exposure" in cdm.table_names
        assert "death" in cdm.table_names
        assert "transplant" in cdm.table_names

    def test_no_events_mock(self, mock_cdm_no_events):
        outcome = mock_cdm_no_events["outcome"].collect()
        assert len(outcome) == 0


# ===================================================================
# add_cohort_survival
# ===================================================================


class TestAddCohortSurvival:
    """Test the add_cohort_survival function."""

    def test_basic_usage(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        target = mock_cdm["target"]
        result = add_cohort_survival(
            target,
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
        )
        df = result.collect()
        assert "time" in df.columns
        assert "status" in df.columns

    def test_custom_column_names(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
            time_column="follow_up_time",
            status_column="event_flag",
        )
        df = result.collect()
        assert "follow_up_time" in df.columns
        assert "event_flag" in df.columns

    def test_status_values(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
        )
        df = result.collect()
        # Non-null status values should be 0 or 1
        non_null = df.filter(pl.col("status").is_not_null())
        status_vals = non_null["status"].unique().sort().to_list()
        assert all(v in (0, 1) for v in status_vals)

    def test_time_non_negative(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
        )
        df = result.collect()
        non_null = df.filter(pl.col("time").is_not_null())
        assert (non_null["time"] >= 0).all()

    def test_preserves_original_columns(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        target = mock_cdm["target"]
        original_cols = set(target.collect().columns)
        result = add_cohort_survival(
            target,
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
        )
        result_cols = set(result.collect().columns)
        # Original columns should still be there
        assert original_cols.issubset(result_cols)

    def test_censor_on_cohort_exit(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
            censor_on_cohort_exit=True,
        )
        df = result.collect()
        assert "time" in df.columns
        assert "status" in df.columns

    def test_follow_up_cap(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
            follow_up_days=365,
        )
        df = result.collect()
        non_null = df.filter(pl.col("time").is_not_null())
        assert (non_null["time"] <= 365).all()

    def test_finite_washout(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
            outcome_washout=180,
        )
        df = result.collect()
        assert "time" in df.columns

    def test_returns_cohort_table(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table="outcome",
            outcome_cohort_id=1,
        )
        # Should still be a CdmTable
        assert isinstance(result, CdmTable)

    def test_cohort_table_object_as_outcome(self, mock_cdm):
        from omopy.survival import add_cohort_survival

        outcome_ct = mock_cdm["outcome"]
        result = add_cohort_survival(
            mock_cdm["target"],
            mock_cdm,
            outcome_cohort_table=outcome_ct,
            outcome_cohort_id=1,
        )
        df = result.collect()
        assert "time" in df.columns
        assert "status" in df.columns


# ===================================================================
# Single-event survival estimation
# ===================================================================


class TestSingleEventSurvival:
    """Test estimate_single_event_survival."""

    def test_basic_estimation(self, single_event_result):
        assert isinstance(single_event_result, SummarisedResult)
        assert len(single_event_result) > 0

    def test_result_has_all_columns(self, single_event_result):
        data = single_event_result.data
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in data.columns, f"Missing column: {col}"

    def test_settings_present(self, single_event_result):
        settings = single_event_result.settings
        assert len(settings) > 0
        assert "result_type" in settings.columns
        assert "analysis_type" in settings.columns
        assert settings["analysis_type"][0] == "single_event"

    def test_has_estimate_rows(self, single_event_result):
        data = single_event_result.data
        estimate_rows = data.filter(pl.col("estimate_name") == "estimate")
        assert len(estimate_rows) > 0

    def test_has_summary_rows(self, single_event_result):
        data = single_event_result.data
        summary_rows = data.filter(pl.col("estimate_name") == "number_records")
        assert len(summary_rows) > 0

    def test_has_attrition_rows(self, single_event_result):
        data = single_event_result.data
        attrition_rows = data.filter(pl.col("strata_name").str.contains("reason"))
        assert len(attrition_rows) > 0

    def test_estimates_between_0_and_1(self, single_event_result):
        data = single_event_result.data
        surv_rows = data.filter(pl.col("estimate_name") == "estimate")
        vals = surv_rows["estimate_value"].cast(pl.Float64)
        assert (vals >= 0).all()
        assert (vals <= 1.0001).all()  # small floating point tolerance

    def test_survival_monotonically_decreasing(self, single_event_result):
        data = single_event_result.data
        surv_rows = data.filter(
            (pl.col("estimate_name") == "estimate") & (pl.col("strata_name") == "overall")
        )
        if len(surv_rows) > 1:
            vals = surv_rows["estimate_value"].cast(pl.Float64).to_list()
            # Should be non-increasing
            for i in range(1, len(vals)):
                assert vals[i] <= vals[i - 1] + 1e-10

    def test_with_strata(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            strata=["sex"],
            estimate_gap=60,
        )
        data = result.data
        strata_names = data["strata_name"].unique().to_list()
        assert "sex" in strata_names or any("sex" in s for s in strata_names if s)

    def test_with_follow_up_cap(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            follow_up_days=365,
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

    def test_censor_on_cohort_exit(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            censor_on_cohort_exit=True,
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

    def test_restricted_mean(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            restricted_mean_follow_up=365,
            estimate_gap=30,
        )
        data = result.data
        rmst_rows = data.filter(pl.col("estimate_name") == "restricted_mean_survival")
        assert len(rmst_rows) > 0

    def test_custom_event_gap(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            event_gap=60,
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)

    def test_minimum_survival_days(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            minimum_survival_days=30,
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)

    def test_group_column_content(self, single_event_result):
        data = single_event_result.data
        # group_name should be "target_cohort"
        group_names = data["group_name"].unique().to_list()
        assert "target_cohort" in group_names


# ===================================================================
# Competing risk survival estimation
# ===================================================================


class TestCompetingRiskSurvival:
    """Test estimate_competing_risk_survival."""

    def test_basic_estimation(self, competing_risk_result):
        assert isinstance(competing_risk_result, SummarisedResult)
        assert len(competing_risk_result) > 0

    def test_result_has_all_columns(self, competing_risk_result):
        data = competing_risk_result.data
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in data.columns

    def test_settings_analysis_type(self, competing_risk_result):
        settings = competing_risk_result.settings
        assert settings["analysis_type"][0] == "competing_risk"

    def test_has_estimates(self, competing_risk_result):
        data = competing_risk_result.data
        est_rows = data.filter(pl.col("estimate_name") == "estimate")
        assert len(est_rows) > 0

    def test_cif_between_0_and_1(self, competing_risk_result):
        data = competing_risk_result.data
        cif_rows = data.filter(pl.col("estimate_name") == "estimate")
        vals = cif_rows["estimate_value"].cast(pl.Float64)
        assert (vals >= -0.001).all()  # small tolerance
        assert (vals <= 1.001).all()

    def test_cif_monotonically_increasing(self, competing_risk_result):
        data = competing_risk_result.data
        cif_rows = data.filter(
            (pl.col("estimate_name") == "estimate") & (pl.col("strata_name") == "overall")
        )
        if len(cif_rows) > 1:
            vals = cif_rows["estimate_value"].cast(pl.Float64).to_list()
            # CIF should be non-decreasing
            for i in range(1, len(vals)):
                assert vals[i] >= vals[i - 1] - 1e-10

    def test_has_competing_outcome_in_settings(self, competing_risk_result):
        settings = competing_risk_result.settings
        assert "competing_outcome_cohort_name" in settings.columns


# ===================================================================
# Result conversion
# ===================================================================


class TestAsSurvivalResult:
    """Test the as_survival_result conversion function."""

    def test_basic_conversion(self, single_event_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(single_event_result)
        assert isinstance(result, dict)
        assert "estimates" in result
        assert "events" in result
        assert "summary" in result
        assert "attrition" in result

    def test_estimates_df(self, single_event_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(single_event_result)
        assert isinstance(result["estimates"], pl.DataFrame)
        assert len(result["estimates"]) > 0

    def test_events_df(self, single_event_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(single_event_result)
        assert isinstance(result["events"], pl.DataFrame)

    def test_summary_df(self, single_event_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(single_event_result)
        assert isinstance(result["summary"], pl.DataFrame)
        assert len(result["summary"]) > 0

    def test_attrition_df(self, single_event_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(single_event_result)
        assert isinstance(result["attrition"], pl.DataFrame)
        assert len(result["attrition"]) > 0

    def test_competing_risk_conversion(self, competing_risk_result):
        from omopy.survival import as_survival_result

        result = as_survival_result(competing_risk_result)
        assert isinstance(result, dict)
        assert len(result["estimates"]) > 0


# ===================================================================
# Table functions
# ===================================================================


class TestTableFunctions:
    """Test table rendering functions."""

    def test_options_table_survival(self):
        from omopy.survival import options_table_survival

        opts = options_table_survival()
        assert isinstance(opts, dict)
        assert "header" in opts
        assert "estimates" in opts

    def test_table_survival_returns_data(self, single_event_result):
        from omopy.survival import table_survival

        result = table_survival(single_event_result, type="polars")
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

    def test_table_survival_with_times(self, single_event_result):
        from omopy.survival import table_survival

        result = table_survival(
            single_event_result,
            times=[30, 90, 180],
            type="polars",
        )
        assert isinstance(result, pl.DataFrame)

    def test_table_survival_time_scale(self, single_event_result):
        from omopy.survival import table_survival

        result = table_survival(
            single_event_result,
            time_scale="months",
            type="polars",
        )
        assert isinstance(result, pl.DataFrame)

    def test_table_survival_events_returns_data(self, single_event_result):
        from omopy.survival import table_survival_events

        result = table_survival_events(single_event_result, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_table_survival_attrition_returns_data(self, single_event_result):
        from omopy.survival import table_survival_attrition

        result = table_survival_attrition(single_event_result, type="polars")
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0


# ===================================================================
# Plot function
# ===================================================================


class TestPlotFunction:
    """Test the plot_survival function."""

    def test_basic_plot(self, single_event_result):
        from omopy.survival import plot_survival

        fig = plot_survival(single_event_result)
        assert fig is not None
        # Should have data
        assert len(fig.data) > 0

    def test_plot_no_ribbon(self, single_event_result):
        from omopy.survival import plot_survival

        fig = plot_survival(single_event_result, ribbon=False)
        assert fig is not None

    def test_plot_cumulative_failure(self, single_event_result):
        from omopy.survival import plot_survival

        fig = plot_survival(single_event_result, cumulative_failure=True)
        assert fig is not None

    def test_plot_time_scale_months(self, single_event_result):
        from omopy.survival import plot_survival

        fig = plot_survival(single_event_result, time_scale="months")
        assert fig is not None

    def test_plot_time_scale_years(self, single_event_result):
        from omopy.survival import plot_survival

        fig = plot_survival(single_event_result, time_scale="years")
        assert fig is not None

    def test_plot_competing_risk(self, competing_risk_result):
        from omopy.survival import plot_survival

        fig = plot_survival(competing_risk_result)
        assert fig is not None


# ===================================================================
# Available grouping
# ===================================================================


class TestAvailableGrouping:
    """Test available_survival_grouping."""

    def test_basic(self, single_event_result):
        from omopy.survival import available_survival_grouping

        grouping = available_survival_grouping(single_event_result)
        assert isinstance(grouping, list)

    def test_varying_only(self, single_event_result):
        from omopy.survival import available_survival_grouping

        grouping = available_survival_grouping(single_event_result, varying=True)
        assert isinstance(grouping, list)

    def test_returns_settings_columns(self, single_event_result):
        from omopy.survival import available_survival_grouping

        grouping = available_survival_grouping(single_event_result)
        # Should include some settings columns
        assert len(grouping) > 0


# ===================================================================
# Edge cases
# ===================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_no_events_still_produces_result(self, mock_cdm_no_events):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm_no_events,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)
        # Should have data (all censored)
        assert len(result) > 0

    def test_small_sample(self, mock_cdm_small):
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm_small,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=30,
        )
        assert isinstance(result, SummarisedResult)

    def test_result_ids_match_settings(self, single_event_result):
        data = single_event_result.data
        settings = single_event_result.settings
        data_ids = set(data["result_id"].unique().to_list())
        settings_ids = set(settings["result_id"].to_list())
        assert data_ids.issubset(settings_ids)

    def test_cdm_name_in_result(self, single_event_result):
        data = single_event_result.data
        cdm_names = data["cdm_name"].unique().to_list()
        assert "mock_survival" in cdm_names

    def test_estimate_gap_1(self, mock_cdm_small):
        """Test with estimate_gap=1 (fine granularity)."""
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm_small,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            follow_up_days=100,
            estimate_gap=1,
        )
        assert isinstance(result, SummarisedResult)
        # Should have many estimate rows
        est_rows = result.data.filter(pl.col("estimate_name") == "estimate")
        assert len(est_rows) >= 50  # at least ~100 timepoints

    def test_large_estimate_gap(self, mock_cdm_small):
        """Test with large estimate_gap."""
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm_small,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=365,
        )
        assert isinstance(result, SummarisedResult)

    def test_multiple_strata(self, mock_cdm):
        """Test with multiple strata columns."""
        from omopy.survival import estimate_single_event_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            strata=["sex", "age_group"],
            estimate_gap=60,
        )
        data = result.data
        # Should have overall + multiple strata combinations
        strata_names = data["strata_name"].unique().to_list()
        assert "overall" in strata_names
        assert len(strata_names) > 1


# ===================================================================
# Integration: full pipeline
# ===================================================================


class TestFullPipeline:
    """Test complete workflows end-to-end."""

    def test_estimate_then_table(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival, table_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=30,
        )
        table = table_survival(result, type="polars")
        assert isinstance(table, pl.DataFrame)
        assert len(table) > 0

    def test_estimate_then_plot(self, mock_cdm):
        from omopy.survival import estimate_single_event_survival, plot_survival

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=30,
        )
        fig = plot_survival(result)
        assert fig is not None
        assert len(fig.data) > 0

    def test_estimate_then_convert_then_table(self, mock_cdm):
        from omopy.survival import (
            as_survival_result,
            estimate_single_event_survival,
            table_survival,
        )

        result = estimate_single_event_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            estimate_gap=30,
        )
        converted = as_survival_result(result)
        assert "estimates" in converted
        assert "summary" in converted

    def test_competing_risk_full_pipeline(self, mock_cdm):
        from omopy.survival import (
            estimate_competing_risk_survival,
            plot_survival,
            table_survival,
        )

        result = estimate_competing_risk_survival(
            mock_cdm,
            target_cohort_table="target",
            outcome_cohort_table="outcome",
            competing_outcome_cohort_table="competing",
            estimate_gap=30,
        )
        table = table_survival(result, type="polars")
        fig = plot_survival(result)
        assert isinstance(table, pl.DataFrame)
        assert fig is not None
