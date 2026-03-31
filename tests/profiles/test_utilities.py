"""Tests for omopy.profiles._utilities — utility functions."""

from __future__ import annotations

from omopy.profiles import (
    add_cdm_name,
    add_concept_name,
    filter_in_observation,
)


class TestAddConceptName:
    def test_adds_concept_name(self, synthea_cdm):
        """Should add concept_name for concept_id columns."""
        cond = synthea_cdm["condition_occurrence"]
        result = add_concept_name(cond, synthea_cdm, column="condition_concept_id")
        df = result.collect()
        assert "condition_concept_id_name" in df.columns
        # Should have some non-null names
        names = df["condition_concept_id_name"].drop_nulls()
        assert len(names) > 0

    def test_custom_name_style(self, synthea_cdm):
        cond = synthea_cdm["condition_occurrence"]
        result = add_concept_name(
            cond,
            synthea_cdm,
            column="condition_concept_id",
            name_style="{column}_label",
        )
        df = result.collect()
        assert "condition_concept_id_label" in df.columns


class TestAddCdmName:
    def test_adds_cdm_name(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = add_cdm_name(obs, synthea_cdm)
        df = result.collect()
        assert "cdm_name" in df.columns
        # All values should be the CDM name
        values = set(df["cdm_name"].to_list())
        assert values == {synthea_cdm.cdm_name}


class TestFilterInObservation:
    def test_filters_correctly(self, synthea_cdm):
        """Observation periods should all pass since they define observation."""
        obs = synthea_cdm["observation_period"]
        result = filter_in_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        # Should keep all rows (obs start is always in its own obs period)
        assert result.count() == obs.count()

    def test_preserves_columns(self, synthea_cdm):
        obs = synthea_cdm["observation_period"]
        result = filter_in_observation(
            obs,
            synthea_cdm,
            index_date="observation_period_start_date",
        )
        df = result.collect()
        for col in obs.columns:
            assert col in df.columns
