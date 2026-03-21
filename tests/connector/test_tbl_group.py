"""Tests for tbl_group()."""

from __future__ import annotations

import pytest

from omopy.connector.tbl_group import tbl_group
from omopy.generics._types import CdmVersion, TableGroup


class TestTblGroup:
    """Tests for tbl_group()."""

    def test_vocab_group_returns_vocab_tables(self):
        result = tbl_group("vocab")
        assert "concept" in result
        assert "vocabulary" in result
        assert "concept_relationship" in result
        assert "concept_ancestor" in result
        # Clinical tables should NOT be in vocab
        assert "person" not in result
        assert "condition_occurrence" not in result

    def test_clinical_group(self):
        result = tbl_group("clinical")
        assert "person" in result
        assert "observation_period" in result
        assert "condition_occurrence" in result
        assert "drug_exposure" in result
        # Vocab tables should NOT be in clinical
        assert "concept" not in result

    def test_all_group_contains_everything(self):
        result = tbl_group("all")
        assert "person" in result
        assert "concept" in result
        assert "drug_era" in result
        assert len(result) > 20

    def test_derived_group(self):
        result = tbl_group("derived")
        assert "drug_era" in result
        assert "condition_era" in result
        # person is not a derived table
        assert "person" not in result

    def test_default_group(self):
        result = tbl_group("default")
        assert "person" in result
        assert "concept" in result
        assert len(result) > 10

    def test_enum_input(self):
        result = tbl_group(TableGroup.VOCAB)
        assert "concept" in result

    def test_multiple_groups(self):
        result = tbl_group(["vocab", "derived"])
        assert "concept" in result
        assert "drug_era" in result

    def test_multiple_groups_no_duplicates(self):
        result = tbl_group(["all", "vocab"])
        assert len(result) == len(set(result))

    def test_cdm_version_53(self):
        result = tbl_group("clinical", cdm_version="5.3")
        assert "person" in result

    def test_cdm_version_54(self):
        result = tbl_group("clinical", cdm_version=CdmVersion.V5_4)
        assert "person" in result

    def test_invalid_group_raises(self):
        with pytest.raises(ValueError):
            tbl_group("nonexistent")

    def test_empty_list(self):
        result = tbl_group([])
        assert result == []

    def test_returns_list_of_strings(self):
        result = tbl_group("vocab")
        assert isinstance(result, list)
        assert all(isinstance(x, str) for x in result)
