"""Tests for omopy.profiles._categories — categorization."""

from __future__ import annotations

import pytest

from omopy.profiles import add_age, add_categories


class TestAddCategories:
    def test_age_groups(self, synthea_cdm):
        """Categorize age into groups."""
        obs = synthea_cdm["observation_period"]
        # First add age
        with_age = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        # Then categorize
        result = add_categories(
            with_age,
            "age",
            {
                "age_group": {
                    "young": (0, 17),
                    "adult": (18, 64),
                    "senior": (65, float("inf")),
                }
            },
        )
        df = result.collect()
        assert "age_group" in df.columns
        values = set(df["age_group"].drop_nulls().to_list())
        assert values <= {"young", "adult", "senior", "None"}

    def test_auto_labels(self, synthea_cdm):
        """Test auto-labelling with list input."""
        obs = synthea_cdm["observation_period"]
        with_age = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        result = add_categories(
            with_age,
            "age",
            {"age_group": [(0, 17), (18, 64), (65, float("inf"))]},
        )
        df = result.collect()
        assert "age_group" in df.columns
        values = set(df["age_group"].drop_nulls().to_list())
        assert values <= {"0 to 17", "18 to 64", "65 or above", "None"}

    def test_overlap_raises(self, synthea_cdm):
        """Overlapping ranges should raise without overlap=True."""
        obs = synthea_cdm["observation_period"]
        with_age = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        with pytest.raises(ValueError, match="Overlapping"):
            add_categories(
                with_age,
                "age",
                {"age_group": {"a": (0, 20), "b": (15, 30)}},
            )

    def test_overlap_allowed(self, synthea_cdm):
        """With overlap=True, overlapping ranges are OK."""
        obs = synthea_cdm["observation_period"]
        with_age = add_age(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        result = add_categories(
            with_age,
            "age",
            {"age_group": {"a": (0, 20), "b": (15, 30)}},
            overlap=True,
        )
        df = result.collect()
        assert "age_group" in df.columns
