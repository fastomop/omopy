"""Tests for omopy.profiles._table_intersect — table intersection functions.

Uses the Synthea test database (27 persons, CDM v5.4).
Key table counts: condition_occurrence=59, drug_exposure=663,
visit_occurrence=599, observation_period=27.
"""

from __future__ import annotations

import polars as pl

from omopy.profiles import (
    add_table_intersect_count,
    add_table_intersect_date,
    add_table_intersect_days,
    add_table_intersect_flag,
)


class TestAddTableIntersectFlag:
    def test_condition_flag(self, synthea_cdm):
        """Flag whether person has conditions after observation start."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_flag(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "condition_occurrence_0_to_inf" in df.columns
        flags = df["condition_occurrence_0_to_inf"]
        # Should have some 1s (persons with conditions) and some 0s
        assert flags.max() == 1
        assert flags.min() == 0

    def test_visit_flag(self, synthea_cdm):
        """Flag whether person has visits."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_flag(
            obs,
            "visit_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "visit_occurrence_0_to_inf" in df.columns
        # Most persons should have visits in Synthea
        assert df["visit_occurrence_0_to_inf"].sum() > 0

    def test_multiple_windows(self, synthea_cdm):
        """Flag with multiple time windows."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_flag(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=[(0, 365), (366, float("inf"))],
        )
        df = result.collect()
        assert "condition_occurrence_0_to_365" in df.columns
        assert "condition_occurrence_366_to_inf" in df.columns

    def test_preserves_row_count(self, synthea_cdm):
        """Row count should not change (no duplication)."""
        obs = synthea_cdm["observation_period"]
        orig_count = obs.count()
        result = add_table_intersect_flag(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        assert result.count() == orig_count


class TestAddTableIntersectCount:
    def test_condition_count(self, synthea_cdm):
        """Count conditions after observation start."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_count(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "condition_occurrence_0_to_inf" in df.columns
        counts = df["condition_occurrence_0_to_inf"]
        # Total conditions should sum to at most 59 (the total records)
        assert counts.sum() <= 59
        assert counts.min() >= 0

    def test_drug_count(self, synthea_cdm):
        """Count drug exposures."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_count(
            obs,
            "drug_exposure",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
        )
        df = result.collect()
        assert "drug_exposure_0_to_inf" in df.columns
        assert df["drug_exposure_0_to_inf"].sum() > 0


class TestAddTableIntersectDate:
    def test_first_condition_date(self, synthea_cdm):
        """Get date of first condition after observation start."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_date(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
            order="first",
        )
        df = result.collect()
        assert "condition_occurrence_0_to_inf" in df.columns
        # Non-null entries should be dates >= obs start
        dates = df.filter(pl.col("condition_occurrence_0_to_inf").is_not_null())
        if len(dates) > 0:
            assert dates["condition_occurrence_0_to_inf"].dtype in (
                pl.Date,
                pl.Datetime,
            )


class TestAddTableIntersectDays:
    def test_days_to_first_condition(self, synthea_cdm):
        """Get days to first condition."""
        obs = synthea_cdm["observation_period"]
        result = add_table_intersect_days(
            obs,
            "condition_occurrence",
            synthea_cdm,
            index_date="observation_period_start_date",
            window=(0, float("inf")),
            order="first",
        )
        df = result.collect()
        assert "condition_occurrence_0_to_inf" in df.columns
        days = df["condition_occurrence_0_to_inf"].drop_nulls()
        # Days should be >= 0 (conditions on or after index)
        if len(days) > 0:
            assert (days >= 0).all()
