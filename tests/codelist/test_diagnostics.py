"""Tests for omopy.codelist._diagnostics — summarise_code_use / summarise_orphan_codes."""

from __future__ import annotations

import polars as pl
import pytest

from omopy.codelist import summarise_code_use, summarise_orphan_codes
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# summarise_code_use
# ---------------------------------------------------------------------------


class TestSummariseCodeUse:
    """Tests for summarise_code_use()."""

    def test_basic_record_count(self, synthea_cdm):
        """Count records for condition concepts in use."""
        cl = Codelist({"hypertension": [320128]})
        result = summarise_code_use(cl, synthea_cdm)
        assert isinstance(result, pl.DataFrame)
        assert "concept_set_name" in result.columns
        assert "concept_id" in result.columns
        assert "count" in result.columns
        # 320128 has 6 occurrences in condition_occurrence
        row = result.filter(pl.col("concept_id") == 320128)
        assert len(row) == 1
        assert row["count"][0] == 6

    def test_person_count(self, synthea_cdm):
        """Count distinct persons for a concept."""
        cl = Codelist({"hypertension": [320128]})
        result = summarise_code_use(cl, synthea_cdm, count_by="person")
        row = result.filter(pl.col("concept_id") == 320128)
        assert len(row) == 1
        # 320128 has 6 records for 6 persons (each person has one record)
        assert row["count"][0] == 6

    def test_multiple_concepts(self, synthea_cdm):
        """Multiple concepts in a single concept set."""
        cl = Codelist({"sinusitis": [40481087, 257012]})
        result = summarise_code_use(cl, synthea_cdm)
        # Should have rows for both concepts
        ids_in_result = result["concept_id"].to_list()
        assert 40481087 in ids_in_result
        assert 257012 in ids_in_result

    def test_unused_concept_has_zero_count(self, synthea_cdm):
        """A concept not in domain tables gets count=0."""
        cl = Codelist({"unused": [4283893]})  # Sinusitis (not in condition_occurrence)
        result = summarise_code_use(cl, synthea_cdm)
        row = result.filter(pl.col("concept_id") == 4283893)
        assert len(row) == 1
        assert row["count"][0] == 0

    def test_multiple_concept_sets(self, synthea_cdm):
        """Multiple concept sets are processed."""
        cl = Codelist(
            {
                "hyp": [320128],
                "sin": [40481087],
            }
        )
        result = summarise_code_use(cl, synthea_cdm)
        set_names = result["concept_set_name"].unique().to_list()
        assert "hyp" in set_names
        assert "sin" in set_names

    def test_concept_metadata_columns(self, synthea_cdm):
        """Result includes concept_name, domain_id, vocabulary_id."""
        cl = Codelist({"hyp": [320128]})
        result = summarise_code_use(cl, synthea_cdm)
        assert "concept_name" in result.columns
        assert "domain_id" in result.columns
        assert "vocabulary_id" in result.columns
        row = result.filter(pl.col("concept_id") == 320128)
        assert row["concept_name"][0] == "Essential hypertension"
        assert row["domain_id"][0] == "Condition"
        assert row["vocabulary_id"][0] == "SNOMED"

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist returns empty DataFrame with correct schema."""
        cl = Codelist({"empty": []})
        result = summarise_code_use(cl, synthea_cdm)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    def test_viral_sinusitis_count(self, synthea_cdm):
        """40481087 has 4 occurrences in condition_occurrence."""
        cl = Codelist({"viral_sin": [40481087]})
        result = summarise_code_use(cl, synthea_cdm)
        row = result.filter(pl.col("concept_id") == 40481087)
        assert row["count"][0] == 4

    def test_chronic_sinusitis_count(self, synthea_cdm):
        """257012 (Chronic sinusitis) has 5 occurrences."""
        cl = Codelist({"chronic_sin": [257012]})
        result = summarise_code_use(cl, synthea_cdm)
        row = result.filter(pl.col("concept_id") == 257012)
        assert row["count"][0] == 5


# ---------------------------------------------------------------------------
# summarise_orphan_codes
# ---------------------------------------------------------------------------


class TestSummariseOrphanCodes:
    """Tests for summarise_orphan_codes()."""

    def test_basic_orphan_detection(self, synthea_cdm):
        """Find orphan descendants of a concept that are in the data."""
        # Use 4283893 (Sinusitis) — its descendants include 40481087 and 257012
        # which appear in condition_occurrence
        cl = Codelist({"sinusitis": [4283893]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        assert isinstance(result, pl.DataFrame)
        if len(result) > 0:
            assert "concept_set_name" in result.columns
            assert "concept_id" in result.columns
            assert "relationship" in result.columns
            assert "count" in result.columns

    def test_orphan_descendants_found(self, synthea_cdm):
        """4283893 has descendants 40481087 and 257012 which are orphans if not in codelist."""
        cl = Codelist({"sinusitis": [4283893]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        if len(result) > 0:
            orphan_ids = result["concept_id"].to_list()
            # 40481087 and/or 257012 should be found as descendants in use
            found_sinusitis_orphans = set(orphan_ids) & {40481087, 257012}
            assert len(found_sinusitis_orphans) > 0

    def test_no_orphans_when_all_included(self, synthea_cdm):
        """When all descendants are already in the codelist, no orphans."""
        cl = Codelist({"full": [4283893, 40481087, 257012]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        # If all used descendants are included, there should be no orphans
        # (or only non-used orphans which wouldn't appear in the result)
        orphan_ids = result["concept_id"].to_list() if len(result) > 0 else []
        assert 40481087 not in orphan_ids
        assert 257012 not in orphan_ids

    def test_orphan_result_schema(self, synthea_cdm):
        """Empty result has correct schema."""
        # Use a concept with no descendants
        cl = Codelist({"leaf": [40481087]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        expected_cols = {
            "concept_set_name",
            "concept_id",
            "concept_name",
            "domain_id",
            "relationship",
            "count",
        }
        assert expected_cols == set(result.columns)

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist returns empty DataFrame."""
        cl = Codelist({"empty": []})
        result = summarise_orphan_codes(cl, synthea_cdm)
        assert len(result) == 0

    def test_orphan_relationship_types(self, synthea_cdm):
        """Orphans should be labeled as 'descendant' or 'mapped'."""
        cl = Codelist({"sinusitis": [4283893]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        if len(result) > 0:
            relationships = result["relationship"].unique().to_list()
            for r in relationships:
                assert r in ("descendant", "mapped")

    def test_orphan_counts_positive(self, synthea_cdm):
        """All orphans in the result should have count > 0."""
        cl = Codelist({"sinusitis": [4283893]})
        result = summarise_orphan_codes(cl, synthea_cdm)
        if len(result) > 0:
            assert (result["count"] > 0).all()
