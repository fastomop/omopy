"""Tests for omopy.profiles._cohort_intersect — cohort intersection functions.

Uses the Synthea test database (27 persons, CDM v5.4).
Generates a hypertension cohort (concept 320128, 6 persons) for testing.
"""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

from omopy.connector import cdm_from_con, generate_concept_cohort_set
from omopy.generics.codelist import Codelist
from omopy.profiles import (
    add_cohort_intersect_count,
    add_cohort_intersect_date,
    add_cohort_intersect_days,
    add_cohort_intersect_flag,
)


@pytest.fixture(scope="module")
def cdm():
    db = pathlib.Path(__file__).resolve().parents[2] / "data" / "synthea.duckdb"
    if not db.exists():
        pytest.skip(f"Synthea database not found at {db}")
    return cdm_from_con(db, cdm_schema="base")


@pytest.fixture(scope="module")
def cdm_with_cohort(cdm):
    """CDM with a hypertension cohort attached."""
    cs = Codelist({"hypertension": [320128]})
    return generate_concept_cohort_set(cdm, cs, "my_cohort")


class TestAddCohortIntersectFlag:
    def test_flag_basic(self, cdm_with_cohort):
        """Flag observation periods for overlap with hypertension cohort."""
        obs = cdm_with_cohort["observation_period"]
        result = add_cohort_intersect_flag(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "hypertension_0_to_inf" in df.columns
        flags = df["hypertension_0_to_inf"]
        assert flags.max() == 1
        assert flags.min() == 0

    def test_preserves_row_count(self, cdm_with_cohort):
        obs = cdm_with_cohort["observation_period"]
        orig = obs.count()
        result = add_cohort_intersect_flag(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
        )
        assert result.count() == orig

    def test_multiple_windows(self, cdm_with_cohort):
        obs = cdm_with_cohort["observation_period"]
        result = add_cohort_intersect_flag(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
            window=[(0, 365), (366, float("inf"))],
        )
        df = result.collect()
        assert "hypertension_0_to_365" in df.columns
        assert "hypertension_366_to_inf" in df.columns


class TestAddCohortIntersectCount:
    def test_count_basic(self, cdm_with_cohort):
        obs = cdm_with_cohort["observation_period"]
        result = add_cohort_intersect_count(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "hypertension_0_to_inf" in df.columns
        counts = df["hypertension_0_to_inf"]
        assert counts.min() >= 0


class TestAddCohortIntersectDate:
    def test_date_basic(self, cdm_with_cohort):
        obs = cdm_with_cohort["observation_period"]
        result = add_cohort_intersect_date(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
            order="first",
        )
        df = result.collect()
        assert "hypertension_0_to_inf" in df.columns
        dates = df.filter(pl.col("hypertension_0_to_inf").is_not_null())
        if len(dates) > 0:
            assert dates["hypertension_0_to_inf"].dtype in (pl.Date, pl.Datetime)


class TestAddCohortIntersectDays:
    def test_days_basic(self, cdm_with_cohort):
        obs = cdm_with_cohort["observation_period"]
        result = add_cohort_intersect_days(
            obs, "my_cohort", cdm_with_cohort,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
            order="first",
        )
        df = result.collect()
        assert "hypertension_0_to_inf" in df.columns
        days = df["hypertension_0_to_inf"].drop_nulls()
        if len(days) > 0:
            assert (days >= 0).all()
