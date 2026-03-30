"""Tests for omopy.vis._mock — mock data generators."""

import polars as pl
import pytest

from omopy.generics.summarised_result import SUMMARISED_RESULT_COLUMNS, SummarisedResult
from omopy.vis import mock_summarised_result


class TestMockSummarisedResult:
    """Tests for mock_summarised_result()."""

    def test_returns_summarised_result(self):
        sr = mock_summarised_result()
        assert isinstance(sr, SummarisedResult)

    def test_has_required_columns(self):
        sr = mock_summarised_result()
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in sr.data.columns

    def test_default_shape(self):
        sr = mock_summarised_result()
        # 2 cohorts * 3 strata * 5 rows_per_combination = 30
        assert len(sr) == 30

    def test_custom_n_cohorts(self):
        sr = mock_summarised_result(n_cohorts=3)
        # 3 cohorts * 3 strata * 5 = 45
        assert len(sr) == 45

    def test_custom_n_strata(self):
        sr = mock_summarised_result(n_strata=1)
        # 2 cohorts * 1 stratum * 5 = 10
        assert len(sr) == 10

    def test_large_strata(self):
        sr = mock_summarised_result(n_strata=9)
        # 2 cohorts * 9 strata * 5 = 90
        assert len(sr) == 90

    def test_settings_present(self):
        sr = mock_summarised_result()
        assert sr.settings is not None
        assert "result_id" in sr.settings.columns
        assert "result_type" in sr.settings.columns

    def test_cohort_names(self):
        sr = mock_summarised_result(n_cohorts=3)
        cohorts = (
            sr.data.filter(pl.col("group_name") == "cohort_name")["group_level"]
            .unique()
            .sort()
            .to_list()
        )
        assert cohorts == ["cohort_1", "cohort_2", "cohort_3"]

    def test_estimate_types(self):
        sr = mock_summarised_result()
        types = set(sr.data["estimate_type"].unique().to_list())
        assert "integer" in types
        assert "numeric" in types
        assert "percentage" in types

    def test_estimate_names(self):
        sr = mock_summarised_result()
        names = set(sr.data["estimate_name"].unique().to_list())
        assert "count" in names
        assert "mean" in names
        assert "sd" in names
        assert "percentage" in names

    def test_variable_names(self):
        sr = mock_summarised_result()
        variables = set(sr.data["variable_name"].unique().to_list())
        assert "number subjects" in variables
        assert "age" in variables
        assert "Medications" in variables

    def test_reproducible_seed(self):
        sr1 = mock_summarised_result()
        sr2 = mock_summarised_result()
        assert sr1.data.equals(sr2.data)
