"""Tests for omopy.codelist._stratify.

Covers stratify_by_domain / vocabulary / concept_class.
"""

from __future__ import annotations

from omopy.codelist import (
    stratify_by_concept_class,
    stratify_by_domain,
    stratify_by_vocabulary,
)
from omopy.generics.codelist import Codelist

# ---------------------------------------------------------------------------
# stratify_by_domain
# ---------------------------------------------------------------------------


class TestStratifyByDomain:
    """Tests for stratify_by_domain()."""

    def test_single_domain(self, synthea_cdm):
        """All concepts in one domain → one stratified entry."""
        cl = Codelist({"conds": [40481087, 320128]})
        result = stratify_by_domain(cl, synthea_cdm)
        assert isinstance(result, Codelist)
        # Both are Condition domain
        assert "conds_condition" in result
        ids = result["conds_condition"]
        assert 40481087 in ids
        assert 320128 in ids

    def test_multiple_domains(self, synthea_cdm):
        """Concepts from different domains produce separate entries."""
        cl = Codelist({"mixed": [40481087, 1177480]})
        result = stratify_by_domain(cl, synthea_cdm)
        assert "mixed_condition" in result
        assert "mixed_drug" in result
        assert 40481087 in result["mixed_condition"]
        assert 1177480 in result["mixed_drug"]

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist produces empty result."""
        cl = Codelist({"empty": []})
        result = stratify_by_domain(cl, synthea_cdm)
        # No entries because empty input
        assert len(result) == 0

    def test_multiple_concept_sets(self, synthea_cdm):
        """Each concept set is stratified independently."""
        cl = Codelist(
            {
                "set1": [40481087],
                "set2": [1177480],
            }
        )
        result = stratify_by_domain(cl, synthea_cdm)
        assert "set1_condition" in result
        assert "set2_drug" in result

    def test_result_values_sorted(self, synthea_cdm):
        """IDs within each stratified entry are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = stratify_by_domain(cl, synthea_cdm)
        for ids in result.values():
            assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# stratify_by_vocabulary
# ---------------------------------------------------------------------------


class TestStratifyByVocabulary:
    """Tests for stratify_by_vocabulary()."""

    def test_single_vocab(self, synthea_cdm):
        """All SNOMED concepts → one entry."""
        cl = Codelist({"snomed_set": [40481087, 320128]})
        result = stratify_by_vocabulary(cl, synthea_cdm)
        assert "snomed_set_snomed" in result
        assert 40481087 in result["snomed_set_snomed"]
        assert 320128 in result["snomed_set_snomed"]

    def test_mixed_vocabs(self, synthea_cdm):
        """Mix of SNOMED and ATC concepts are split."""
        cl = Codelist({"mixed": [40481087, 21600001]})
        result = stratify_by_vocabulary(cl, synthea_cdm)
        assert "mixed_snomed" in result
        assert "mixed_atc" in result

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist."""
        cl = Codelist({"empty": []})
        result = stratify_by_vocabulary(cl, synthea_cdm)
        assert len(result) == 0

    def test_result_values_sorted(self, synthea_cdm):
        """Results are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = stratify_by_vocabulary(cl, synthea_cdm)
        for ids in result.values():
            assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# stratify_by_concept_class
# ---------------------------------------------------------------------------


class TestStratifyByConceptClass:
    """Tests for stratify_by_concept_class()."""

    def test_single_class(self, synthea_cdm):
        """All Disorder concepts → one entry."""
        cl = Codelist({"disorders": [40481087, 320128]})
        result = stratify_by_concept_class(cl, synthea_cdm)
        # Both are "Disorder" concept_class_id
        assert "disorders_disorder" in result

    def test_mixed_classes(self, synthea_cdm):
        """Different concept classes produce separate entries."""
        # 40481087 = Disorder, 1177480 = Ingredient
        cl = Codelist({"mixed": [40481087, 1177480]})
        result = stratify_by_concept_class(cl, synthea_cdm)
        assert "mixed_disorder" in result
        assert "mixed_ingredient" in result

    def test_atc_classes(self, synthea_cdm):
        """ATC concepts have specific class IDs."""
        cl = Codelist({"atc": [21600001]})
        result = stratify_by_concept_class(cl, synthea_cdm)
        # 21600001 has concept_class_id "ATC 1st"
        assert "atc_atc_1st" in result

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist."""
        cl = Codelist({"empty": []})
        result = stratify_by_concept_class(cl, synthea_cdm)
        assert len(result) == 0

    def test_result_values_sorted(self, synthea_cdm):
        """Results are sorted."""
        cl = Codelist({"conds": [320128, 40481087]})
        result = stratify_by_concept_class(cl, synthea_cdm)
        for ids in result.values():
            assert ids == sorted(ids)
