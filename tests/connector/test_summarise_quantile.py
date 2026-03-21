"""Tests for summarise_quantile()."""

from __future__ import annotations

import pytest
import polars as pl

from omopy.connector.summarise_quantile import summarise_quantile


class TestSummariseQuantilePolars:
    """Tests using Polars DataFrames."""

    @pytest.fixture
    def sample_data(self):
        return pl.DataFrame({
            "group": ["A", "A", "A", "A", "B", "B", "B", "B"],
            "value": [1, 2, 3, 4, 10, 20, 30, 40],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        })

    def test_single_column_no_group(self, sample_data):
        result = summarise_quantile(
            sample_data, "value", probs=[0.0, 0.5, 1.0]
        )
        assert isinstance(result, pl.DataFrame)
        assert "q00_value" in result.columns
        assert "q50_value" in result.columns
        assert "q100_value" in result.columns
        assert len(result) == 1

    def test_min_and_max(self, sample_data):
        result = summarise_quantile(
            sample_data, "value", probs=[0.0, 1.0]
        )
        assert result["q00_value"][0] == 1
        assert result["q100_value"][0] == 40

    def test_grouped_quantile(self, sample_data):
        result = summarise_quantile(
            sample_data, "value", probs=[0.5], group_by="group"
        )
        assert len(result) == 2
        assert "group" in result.columns
        assert "q50_value" in result.columns

    def test_multiple_columns(self, sample_data):
        result = summarise_quantile(
            sample_data, ["value", "weight"], probs=[0.25, 0.75]
        )
        assert "q25_value" in result.columns
        assert "q75_value" in result.columns
        assert "q25_weight" in result.columns
        assert "q75_weight" in result.columns

    def test_invalid_probs_raises(self, sample_data):
        with pytest.raises(ValueError):
            summarise_quantile(sample_data, "value", probs=[1.5])

    def test_empty_columns_raises(self, sample_data):
        with pytest.raises(ValueError, match="columns must not be empty"):
            summarise_quantile(sample_data, [], probs=[0.5])

    def test_lazy_frame(self, sample_data):
        """LazyFrame should also work (collected internally)."""
        lazy = sample_data.lazy()
        result = summarise_quantile(lazy, "value", probs=[0.5])
        assert isinstance(result, pl.DataFrame)
        assert "q50_value" in result.columns

    def test_sorted_probs(self, sample_data):
        """Probs should be sorted regardless of input order."""
        result = summarise_quantile(
            sample_data, "value", probs=[1.0, 0.0, 0.5]
        )
        assert "q00_value" in result.columns
        assert "q50_value" in result.columns
        assert "q100_value" in result.columns

    def test_duplicate_probs(self, sample_data):
        """Duplicate probs should produce deduplicated columns."""
        result = summarise_quantile(
            sample_data, "value", probs=[0.5, 0.5]
        )
        assert result.columns.count("q50_value") == 1


class TestSummariseQuantileIbis:
    """Tests using Ibis (Synthea DB)."""

    def test_ibis_single_column(self, synthea_cdm):
        """Compute quantiles on person.year_of_birth via Ibis."""
        person_data = synthea_cdm["person"].data
        result = summarise_quantile(
            person_data, "year_of_birth", probs=[0.0, 0.5, 1.0]
        )
        df = result.execute()
        assert len(df) == 1
        assert "q00_year_of_birth" in df.columns
        assert "q50_year_of_birth" in df.columns
        assert "q100_year_of_birth" in df.columns

    def test_ibis_grouped(self, synthea_cdm):
        """Compute quantiles grouped by gender."""
        person_data = synthea_cdm["person"].data
        result = summarise_quantile(
            person_data,
            "year_of_birth",
            probs=[0.5],
            group_by="gender_concept_id",
        )
        df = result.execute()
        assert len(df) >= 2  # male + female
        assert "gender_concept_id" in df.columns
        assert "q50_year_of_birth" in df.columns

    def test_ibis_min_max(self, synthea_cdm):
        """p0 should be min, p100 should be max."""
        person_data = synthea_cdm["person"].data
        result = summarise_quantile(
            person_data, "year_of_birth", probs=[0.0, 1.0]
        )
        df = result.execute()
        min_yob = df["q00_year_of_birth"].iloc[0]
        max_yob = df["q100_year_of_birth"].iloc[0]
        assert min_yob <= max_yob
