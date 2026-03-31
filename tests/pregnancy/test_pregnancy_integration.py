"""Integration tests for omopy.pregnancy — HIPPS pregnancy identification.

Tests the full pipeline against mock CDM and optionally against the
Synthea database.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from omopy.pregnancy import (
    PregnancyResult,
    identify_pregnancies,
    mock_pregnancy_cdm,
    plot_pregnancies,
    summarise_pregnancies,
    table_pregnancies,
    validate_episodes,
)

SYNTHEA_DB = Path(__file__).resolve().parent.parent.parent / "data" / "synthea.duckdb"


# ---------------------------------------------------------------------------
# Mock CDM integration tests
# ---------------------------------------------------------------------------


class TestMockCdmPipeline:
    """Run the full pipeline against mock_pregnancy_cdm()."""

    def test_identify_from_mock(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        assert isinstance(result, PregnancyResult)
        assert result.n_episodes >= 0
        assert result.cdm_name == "mock_pregnancy"

    def test_identify_returns_episodes(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        assert isinstance(result.episodes, pl.DataFrame)
        if result.n_episodes > 0:
            assert "person_id" in result.episodes.columns
            assert "category" in result.episodes.columns
            assert "episode_start_date" in result.episodes.columns
            assert "episode_end_date" in result.episodes.columns

    def test_identify_returns_intermediate(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        assert isinstance(result.hip_episodes, pl.DataFrame)
        assert isinstance(result.pps_episodes, pl.DataFrame)
        assert isinstance(result.merged_episodes, pl.DataFrame)

    def test_identify_with_date_filters(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(
            cdm,
            start_date=datetime.date(2018, 1, 1),
            end_date=datetime.date(2022, 12, 31),
        )
        assert isinstance(result, PregnancyResult)

    def test_identify_with_small_n(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=5)
        result = identify_pregnancies(cdm)
        assert isinstance(result, PregnancyResult)

    def test_full_pipeline_mock(self):
        """Full pipeline: identify -> summarise -> table -> plot."""
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        sr = summarise_pregnancies(result)
        tbl = table_pregnancies(sr, type="polars")
        fig = plot_pregnancies(sr, type="outcome")

        assert isinstance(tbl, pl.DataFrame)
        assert fig is not None

    def test_summarise_with_strata(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        sr = summarise_pregnancies(result, strata=["category"])

        if result.n_episodes > 0:
            strata_names = sr.data["strata_name"].unique().to_list()
            assert "category" in strata_names or "overall" in strata_names

    def test_validate_mock_episodes(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        result = identify_pregnancies(cdm)
        if result.n_episodes > 0:
            report = validate_episodes(result.episodes)
            assert "check" in report.columns

    def test_settings_preserved(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=10)
        result = identify_pregnancies(
            cdm,
            start_date=datetime.date(2018, 1, 1),
            just_gestation=False,
        )
        assert result.settings["start_date"] == "2018-01-01"
        assert result.settings["just_gestation"] is False

    def test_n_persons_matches(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=10)
        result = identify_pregnancies(cdm)
        assert result.n_persons_input >= 0
        # n_persons_input counts persons with any pregnancy-related record
        # It should not exceed n_persons in mock CDM
        assert result.n_persons_input <= 10


# ---------------------------------------------------------------------------
# Synthea database integration tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not SYNTHEA_DB.exists(), reason="Synthea DB not available")
class TestSyntheaCdmPipeline:
    """Run the full pipeline against the Synthea database.

    Synthea may have very few or no pregnancy concepts. The tests verify
    that the pipeline runs without errors.
    """

    def test_identify_from_synthea(self, synthea_cdm):
        result = identify_pregnancies(synthea_cdm)
        assert isinstance(result, PregnancyResult)
        assert result.n_episodes >= 0

    def test_summarise_from_synthea(self, synthea_cdm):
        result = identify_pregnancies(synthea_cdm)
        sr = summarise_pregnancies(result)
        assert len(sr) > 0

    def test_table_from_synthea(self, synthea_cdm):
        result = identify_pregnancies(synthea_cdm)
        sr = summarise_pregnancies(result)
        tbl = table_pregnancies(sr, type="polars")
        assert isinstance(tbl, pl.DataFrame)

    def test_plot_from_synthea(self, synthea_cdm):
        result = identify_pregnancies(synthea_cdm)
        sr = summarise_pregnancies(result)
        fig = plot_pregnancies(sr, type="outcome")
        assert fig is not None

    def test_validate_synthea_episodes(self, synthea_cdm):
        result = identify_pregnancies(synthea_cdm)
        if result.n_episodes > 0:
            report = validate_episodes(result.episodes)
            assert "check" in report.columns

    def test_synthea_with_date_range(self, synthea_cdm):
        result = identify_pregnancies(
            synthea_cdm,
            start_date=datetime.date(2010, 1, 1),
            end_date=datetime.date(2020, 12, 31),
        )
        assert isinstance(result, PregnancyResult)
