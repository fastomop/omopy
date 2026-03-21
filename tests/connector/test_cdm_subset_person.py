"""Tests for cdm_subset(person_ids=...)."""

from __future__ import annotations

import pytest

from omopy.connector.cdm_subset import cdm_subset


class TestCdmSubsetByPersonIds:
    """Tests for cdm_subset() with explicit person IDs."""

    def test_returns_cdm_reference(self, synthea_cdm):
        from omopy.generics.cdm_reference import CdmReference

        result = cdm_subset(synthea_cdm, person_ids=[1, 2])
        assert isinstance(result, CdmReference)

    def test_person_table_filtered(self, synthea_cdm):
        """Only requested person_ids should remain."""
        # Get some actual person IDs from the test DB
        all_persons = synthea_cdm["person"].collect()
        pids = all_persons["person_id"].to_list()[:3]

        result = cdm_subset(synthea_cdm, person_ids=pids)
        filtered_persons = result["person"].collect()
        result_pids = set(filtered_persons["person_id"].to_list())
        assert result_pids == set(pids)

    def test_clinical_tables_filtered(self, synthea_cdm):
        """Clinical tables should also be filtered by person_id."""
        all_persons = synthea_cdm["person"].collect()
        pids = all_persons["person_id"].to_list()[:2]

        result = cdm_subset(synthea_cdm, person_ids=pids)

        if "observation_period" in result:
            obs = result["observation_period"].collect()
            obs_pids = set(obs["person_id"].to_list())
            assert obs_pids.issubset(set(pids))

    def test_vocab_tables_unchanged(self, synthea_cdm):
        """Vocabulary tables (no person_id) should pass through."""
        all_persons = synthea_cdm["person"].collect()
        pids = all_persons["person_id"].to_list()[:2]

        result = cdm_subset(synthea_cdm, person_ids=pids)

        if "concept" in result:
            orig_count = len(synthea_cdm["concept"].collect())
            new_count = len(result["concept"].collect())
            assert new_count == orig_count

    def test_empty_person_ids_raises(self, synthea_cdm):
        with pytest.raises(ValueError, match="must not be empty"):
            cdm_subset(synthea_cdm, person_ids=[])

    def test_single_person(self, synthea_cdm):
        all_persons = synthea_cdm["person"].collect()
        pid = all_persons["person_id"].to_list()[0]

        result = cdm_subset(synthea_cdm, person_ids=[pid])
        filtered = result["person"].collect()
        assert len(filtered) == 1
        assert filtered["person_id"][0] == pid

    def test_nonexistent_person_ids(self, synthea_cdm):
        """Person IDs not in the DB should result in empty tables."""
        result = cdm_subset(synthea_cdm, person_ids=[999999999])
        filtered = result["person"].collect()
        assert len(filtered) == 0

    def test_preserves_cdm_metadata(self, synthea_cdm):
        all_persons = synthea_cdm["person"].collect()
        pids = all_persons["person_id"].to_list()[:2]

        result = cdm_subset(synthea_cdm, person_ids=pids)
        assert result.cdm_version == synthea_cdm.cdm_version
        assert result.cdm_name == synthea_cdm.cdm_name
