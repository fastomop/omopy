"""Tests for omopy.profiles._death — death functions.

The Synthea database may or may not have a death table. Tests handle both.
"""

from __future__ import annotations

import polars as pl
import pytest

from omopy.profiles import add_death_date, add_death_days, add_death_flag


class TestAddDeathFlag:
    def test_adds_death_flag(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_flag(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "death" in df.columns
        # Should be 0 or 1
        values = set(df["death"].to_list())
        assert values <= {0, 1}

    def test_preserves_row_count(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        orig = obs.count()
        result = add_death_flag(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        assert result.count() == orig

    def test_custom_name(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_flag(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            death_flag_name="is_dead",
        )
        df = result.collect()
        assert "is_dead" in df.columns


class TestAddDeathDate:
    def test_adds_death_date(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_date(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "date_of_death" in df.columns

    def test_custom_name(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_date(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            death_date_name="death_dt",
        )
        df = result.collect()
        assert "death_dt" in df.columns


class TestAddDeathDays:
    def test_adds_death_days(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_days(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        assert "days_to_death" in df.columns

    def test_custom_name(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_death_days(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
            death_days_name="days_until_death",
        )
        df = result.collect()
        assert "days_until_death" in df.columns
