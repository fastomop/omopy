"""Tests for omopy.connector.cdm_subset — cdm_subset_cohort() and cdm_sample().

Tests against the Synthea DuckDB test database (data/synthea.duckdb).

Key test data:
- 27 persons, 27 observation periods
- condition_occurrence: 59 rows
- drug_exposure: 663 rows
- Hypertension cohort (320128): 6 subjects [5, 15, 16, 17, 19, 21]
"""

from __future__ import annotations

import pytest
import polars as pl

from omopy.connector import (
    cdm_from_con,
    cdm_sample,
    cdm_subset_cohort,
    generate_concept_cohort_set,
)
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cdm():
    """Module-scoped CDM for subset tests."""
    from pathlib import Path

    db = Path(__file__).resolve().parent.parent.parent / "data" / "synthea.duckdb"
    if not db.exists():
        pytest.skip(f"Synthea database not found at {db}")
    return cdm_from_con(db, cdm_schema="base")


@pytest.fixture(scope="module")
def cdm_with_cohort(cdm):
    """CDM with a hypertension cohort attached."""
    cs = Codelist({"hypertension": [320128]})
    return generate_concept_cohort_set(cdm, cs, "hypertension")


# ---------------------------------------------------------------------------
# cdm_subset_cohort tests
# ---------------------------------------------------------------------------


class TestCdmSubsetCohort:
    def test_subset_returns_cdm_reference(self, cdm_with_cohort):
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        assert isinstance(subset, CdmReference)

    def test_subset_person_count(self, cdm_with_cohort):
        """Subset should have exactly 6 persons (the hypertension subjects)."""
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        assert subset["person"].count() == 6

    def test_subset_person_ids_match(self, cdm_with_cohort):
        """Person IDs in subset should match the cohort subject IDs."""
        cohort_subjects = sorted(
            cdm_with_cohort["hypertension"].collect()["subject_id"].to_list()
        )
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        subset_persons = sorted(
            subset["person"].collect()["person_id"].to_list()
        )
        assert subset_persons == cohort_subjects

    def test_subset_reduces_condition_occurrence(self, cdm_with_cohort):
        """Condition occurrence should be reduced to only cohort members."""
        full_count = cdm_with_cohort["condition_occurrence"].count()
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        subset_count = subset["condition_occurrence"].count()
        assert subset_count < full_count
        assert subset_count > 0

    def test_subset_reduces_drug_exposure(self, cdm_with_cohort):
        full_count = cdm_with_cohort["drug_exposure"].count()
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        subset_count = subset["drug_exposure"].count()
        assert subset_count < full_count
        assert subset_count > 0

    def test_subset_preserves_vocabulary_tables(self, cdm_with_cohort):
        """Tables without person_id or subject_id should pass through unchanged."""
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        # concept table has no person_id — should be unchanged
        if "concept" in subset:
            assert subset["concept"].count() == cdm_with_cohort["concept"].count()

    def test_subset_cohort_table_filtered(self, cdm_with_cohort):
        """The cohort table itself should be filtered to cohort members."""
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        if "hypertension" in subset:
            cohort_df = subset["hypertension"].collect()
            assert len(cohort_df) == 6  # all subjects are in the cohort

    def test_subset_retains_cdm_metadata(self, cdm_with_cohort):
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        assert subset.cdm_version == cdm_with_cohort.cdm_version
        assert subset.cdm_name == cdm_with_cohort.cdm_name

    def test_subset_missing_cohort_raises(self, cdm_with_cohort):
        with pytest.raises(KeyError, match="not found in CDM"):
            cdm_subset_cohort(cdm_with_cohort, "nonexistent")

    def test_subset_with_specific_cohort_id(self, cdm_with_cohort):
        """Filter to specific cohort_definition_id."""
        subset = cdm_subset_cohort(
            cdm_with_cohort, "hypertension", cohort_id=[1]
        )
        assert subset["person"].count() == 6

    def test_subset_with_empty_cohort_id_list(self, cdm_with_cohort):
        """Empty cohort_id list → no subjects → empty tables."""
        subset = cdm_subset_cohort(
            cdm_with_cohort, "hypertension", cohort_id=[999]
        )
        assert subset["person"].count() == 0

    def test_subset_observation_period_filtered(self, cdm_with_cohort):
        """Observation periods should be filtered to cohort members."""
        subset = cdm_subset_cohort(cdm_with_cohort, "hypertension")
        assert subset["observation_period"].count() == 6


# ---------------------------------------------------------------------------
# cdm_sample tests
# ---------------------------------------------------------------------------


class TestCdmSample:
    def test_sample_returns_cdm_reference(self, cdm):
        result = cdm_sample(cdm, 5)
        assert isinstance(result, CdmReference)

    def test_sample_correct_person_count(self, cdm):
        result = cdm_sample(cdm, 10)
        assert result["person"].count() == 10

    def test_sample_n_larger_than_population(self, cdm):
        """Requesting more persons than exist should return all."""
        result = cdm_sample(cdm, 100)
        assert result["person"].count() == 27

    def test_sample_n_1(self, cdm):
        result = cdm_sample(cdm, 1)
        assert result["person"].count() == 1

    def test_sample_with_seed(self, cdm):
        """Seed parameter should be accepted without error."""
        r1 = cdm_sample(cdm, 5, seed=42)
        p1 = sorted(r1["person"].collect()["person_id"].to_list())
        assert len(p1) == 5
        # Note: DuckDB setseed is per-query, so reproducibility across
        # separate cdm_sample calls is not guaranteed.

    def test_sample_reduces_clinical_tables(self, cdm):
        result = cdm_sample(cdm, 5)
        assert result["condition_occurrence"].count() <= cdm["condition_occurrence"].count()
        assert result["drug_exposure"].count() <= cdm["drug_exposure"].count()

    def test_sample_preserves_vocabulary(self, cdm):
        result = cdm_sample(cdm, 5)
        if "concept" in result:
            assert result["concept"].count() == cdm["concept"].count()

    def test_sample_retains_metadata(self, cdm):
        result = cdm_sample(cdm, 5)
        assert result.cdm_version == cdm.cdm_version
        assert result.cdm_name == cdm.cdm_name

    def test_sample_no_person_table_raises(self):
        """CDM without person table should raise."""
        empty_cdm = CdmReference()
        with pytest.raises(KeyError, match="person"):
            cdm_sample(empty_cdm, 5)

    def test_sample_non_db_cdm_raises(self):
        """Local CDM should raise TypeError."""
        from omopy.generics.cdm_table import CdmTable

        person_df = pl.DataFrame({
            "person_id": [1, 2, 3],
            "gender_concept_id": [0, 0, 0],
            "year_of_birth": [1990, 1991, 1992],
            "race_concept_id": [0, 0, 0],
            "ethnicity_concept_id": [0, 0, 0],
        })
        local_cdm = CdmReference(
            tables={"person": CdmTable(data=person_df, tbl_name="person")}
        )
        with pytest.raises(TypeError, match="DbSource"):
            cdm_sample(local_cdm, 2)
