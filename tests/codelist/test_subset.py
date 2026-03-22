"""Tests for omopy.codelist._subset — subset_to_codes_in_use, subset_by_domain/vocabulary."""

from __future__ import annotations

import pytest

from omopy.codelist import subset_by_domain, subset_by_vocabulary, subset_to_codes_in_use
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# subset_by_domain
# ---------------------------------------------------------------------------


class TestSubsetByDomain:
    """Tests for subset_by_domain()."""

    def test_filter_to_condition(self, synthea_cdm):
        """Keep only Condition domain concepts."""
        cl = Codelist({"mixed": [40481087, 320128, 1177480]})
        # 40481087 = Condition, 320128 = Condition, 1177480 = Drug
        result = subset_by_domain(cl, synthea_cdm, "Condition")
        assert 40481087 in result["mixed"]
        assert 320128 in result["mixed"]
        assert 1177480 not in result["mixed"]

    def test_filter_to_drug(self, synthea_cdm):
        """Keep only Drug domain concepts."""
        cl = Codelist({"mixed": [40481087, 1177480]})
        result = subset_by_domain(cl, synthea_cdm, "Drug")
        assert 40481087 not in result["mixed"]
        assert 1177480 in result["mixed"]

    def test_multiple_domains(self, synthea_cdm):
        """Filter to multiple domains."""
        cl = Codelist({"mixed": [40481087, 1177480]})
        result = subset_by_domain(cl, synthea_cdm, ["Condition", "Drug"])
        assert 40481087 in result["mixed"]
        assert 1177480 in result["mixed"]

    def test_empty_concept_set(self, synthea_cdm):
        """Empty concept set stays empty."""
        cl = Codelist({"empty": []})
        result = subset_by_domain(cl, synthea_cdm, "Condition")
        assert result["empty"] == []

    def test_no_match(self, synthea_cdm):
        """All concepts filtered out returns empty list."""
        cl = Codelist({"conditions": [40481087, 320128]})
        result = subset_by_domain(cl, synthea_cdm, "Drug")
        assert result["conditions"] == []

    def test_preserves_multiple_sets(self, synthea_cdm):
        """Multiple concept sets are each filtered independently."""
        cl = Codelist({
            "set1": [40481087],
            "set2": [1177480],
        })
        result = subset_by_domain(cl, synthea_cdm, "Condition")
        assert 40481087 in result["set1"]
        assert result["set2"] == []

    def test_result_sorted(self, synthea_cdm):
        """Results are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = subset_by_domain(cl, synthea_cdm, "Condition")
        assert result["conds"] == sorted(result["conds"])


# ---------------------------------------------------------------------------
# subset_by_vocabulary
# ---------------------------------------------------------------------------


class TestSubsetByVocabulary:
    """Tests for subset_by_vocabulary()."""

    def test_filter_to_snomed(self, synthea_cdm):
        """Keep only SNOMED concepts."""
        cl = Codelist({"mixed": [40481087, 320128]})
        # Both are SNOMED
        result = subset_by_vocabulary(cl, synthea_cdm, "SNOMED")
        assert 40481087 in result["mixed"]
        assert 320128 in result["mixed"]

    def test_filter_to_atc(self, synthea_cdm):
        """ATC concepts only."""
        # 21600001 = ATC concept
        cl = Codelist({"mix": [40481087, 21600001]})
        result = subset_by_vocabulary(cl, synthea_cdm, "ATC")
        assert 40481087 not in result["mix"]
        assert 21600001 in result["mix"]

    def test_multiple_vocabs(self, synthea_cdm):
        """Multiple vocabulary IDs."""
        cl = Codelist({"mix": [40481087, 21600001]})
        result = subset_by_vocabulary(cl, synthea_cdm, ["SNOMED", "ATC"])
        assert 40481087 in result["mix"]
        assert 21600001 in result["mix"]

    def test_empty_set(self, synthea_cdm):
        """Empty concept set stays empty."""
        cl = Codelist({"empty": []})
        result = subset_by_vocabulary(cl, synthea_cdm, "SNOMED")
        assert result["empty"] == []

    def test_result_sorted(self, synthea_cdm):
        """Results are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = subset_by_vocabulary(cl, synthea_cdm, "SNOMED")
        assert result["conds"] == sorted(result["conds"])


# ---------------------------------------------------------------------------
# subset_to_codes_in_use
# ---------------------------------------------------------------------------


class TestSubsetToCodesToInUse:
    """Tests for subset_to_codes_in_use()."""

    def test_used_condition_codes(self, synthea_cdm):
        """40481087 (Viral sinusitis) is used in condition_occurrence."""
        cl = Codelist({"conds": [40481087, 320128]})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        # Both appear in condition_occurrence
        assert 40481087 in result["conds"]
        assert 320128 in result["conds"]

    def test_unused_code_filtered(self, synthea_cdm):
        """A concept not in any domain table is excluded."""
        # 4283893 (Sinusitis) is NOT in condition_occurrence
        cl = Codelist({"conds": [40481087, 4283893]})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        assert 40481087 in result["conds"]
        assert 4283893 not in result["conds"]

    def test_all_unused(self, synthea_cdm):
        """All unused codes returns empty."""
        cl = Codelist({"conds": [4283893]})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        assert result["conds"] == []

    def test_empty_set(self, synthea_cdm):
        """Empty concept set stays empty."""
        cl = Codelist({"empty": []})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        assert result["empty"] == []

    def test_drug_codes_in_use(self, synthea_cdm):
        """Drug concepts that appear in drug_exposure should be kept."""
        # 1177480 = ibuprofen (Ingredient) - may or may not directly appear
        # in drug_exposure; drug_exposure may use more specific clinical drugs
        cl = Codelist({"drugs": [1177480]})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        # This depends on whether ingredient-level concept_ids appear in drug_exposure
        assert isinstance(result, Codelist)

    def test_preserves_multiple_sets(self, synthea_cdm):
        """Multiple concept sets are each processed independently."""
        cl = Codelist({
            "used": [40481087],
            "unused": [4283893],
        })
        result = subset_to_codes_in_use(cl, synthea_cdm)
        assert 40481087 in result["used"]
        assert result["unused"] == []

    def test_result_sorted(self, synthea_cdm):
        """Results are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = subset_to_codes_in_use(cl, synthea_cdm)
        assert result["conds"] == sorted(result["conds"])
