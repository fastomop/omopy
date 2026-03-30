"""Tests for omopy.codelist._search — get_candidate_codes / get_mappings."""

from __future__ import annotations

import pytest

from omopy.codelist import get_candidate_codes, get_mappings
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# get_candidate_codes
# ---------------------------------------------------------------------------


class TestGetCandidateCodes:
    """Tests for get_candidate_codes()."""

    def test_single_keyword(self, synthea_cdm):
        """Search for 'sinusitis' should find 3 SNOMED Condition concepts."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis")
        assert isinstance(cl, Codelist)
        key = next(iter(cl))
        ids = cl[key]
        # Should find: 40481087 (Viral sinusitis), 257012 (Chronic sinusitis),
        # 4283893 (Sinusitis)
        assert 40481087 in ids
        assert 257012 in ids
        assert 4283893 in ids

    def test_multiple_keywords_or(self, synthea_cdm):
        """Multiple keywords are OR-combined."""
        cl = get_candidate_codes(synthea_cdm, ["sinusitis", "hypertension"])
        key = next(iter(cl))
        ids = cl[key]
        # Should include both sinusitis and hypertension concepts
        assert 40481087 in ids  # Viral sinusitis
        assert 320128 in ids  # Essential hypertension

    def test_case_insensitive(self, synthea_cdm):
        """Search is case-insensitive."""
        cl_lower = get_candidate_codes(synthea_cdm, "sinusitis")
        cl_upper = get_candidate_codes(synthea_cdm, "SINUSITIS")
        cl_mixed = get_candidate_codes(synthea_cdm, "Sinusitis")
        key_lower = next(iter(cl_lower))
        key_upper = next(iter(cl_upper))
        key_mixed = next(iter(cl_mixed))
        assert set(cl_lower[key_lower]) == set(cl_upper[key_upper]) == set(cl_mixed[key_mixed])

    def test_domain_filter(self, synthea_cdm):
        """Filtering by domain restricts results."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", domains="Condition")
        key = next(iter(cl))
        ids = cl[key]
        assert len(ids) >= 3  # All sinusitis concepts are Condition domain

    def test_standard_concept_filter(self, synthea_cdm):
        """Filtering by standard_concept='S' keeps only standard concepts."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", standard_concept="S")
        key = next(iter(cl))
        ids = cl[key]
        # All three sinusitis concepts are standard
        assert 40481087 in ids
        assert 257012 in ids
        assert 4283893 in ids

    def test_vocabulary_filter(self, synthea_cdm):
        """Filtering by vocabulary_id."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", vocabulary_id="SNOMED")
        key = next(iter(cl))
        ids = cl[key]
        assert len(ids) >= 3

    def test_exclude_keyword(self, synthea_cdm):
        """Exclude keyword removes matching concepts."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", exclude="chronic")
        key = next(iter(cl))
        ids = cl[key]
        # 257012 (Chronic sinusitis) should be excluded
        assert 257012 not in ids
        # But Viral sinusitis and Sinusitis should remain
        assert 40481087 in ids
        assert 4283893 in ids

    def test_exclude_multiple(self, synthea_cdm):
        """Multiple exclude keywords."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", exclude=["chronic", "viral"])
        key = next(iter(cl))
        ids = cl[key]
        assert 257012 not in ids  # Chronic sinusitis excluded
        assert 40481087 not in ids  # Viral sinusitis excluded
        assert 4283893 in ids  # Just "Sinusitis" remains

    def test_custom_name(self, synthea_cdm):
        """Custom name parameter."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", name="my_sinusitis")
        assert "my_sinusitis" in cl

    def test_default_name(self, synthea_cdm):
        """Default name is first keyword lowercased."""
        cl = get_candidate_codes(synthea_cdm, "Sinusitis")
        assert "sinusitis" in cl

    def test_include_descendants(self, synthea_cdm):
        """include_descendants adds descendant concepts."""
        # Without descendants
        cl_no_desc = get_candidate_codes(synthea_cdm, "sinusitis")
        # With descendants
        cl_desc = get_candidate_codes(synthea_cdm, "sinusitis", include_descendants=True)
        key_no = next(iter(cl_no_desc))
        key_yes = next(iter(cl_desc))
        # With descendants should have at least as many concepts
        assert len(cl_desc[key_yes]) >= len(cl_no_desc[key_no])

    def test_concept_class_filter(self, synthea_cdm):
        """Filtering by concept_class_id."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis", concept_class_id="Disorder")
        key = next(iter(cl))
        ids = cl[key]
        # All sinusitis concepts are "Disorder" class
        assert 40481087 in ids

    def test_result_sorted(self, synthea_cdm):
        """Results should be sorted."""
        cl = get_candidate_codes(synthea_cdm, "sinusitis")
        key = next(iter(cl))
        ids = cl[key]
        assert ids == sorted(ids)

    def test_no_results(self, synthea_cdm):
        """Searching for a non-existent keyword returns empty list."""
        cl = get_candidate_codes(synthea_cdm, "xyzzyplugh_nonexistent_999")
        key = next(iter(cl))
        assert cl[key] == []


# ---------------------------------------------------------------------------
# get_mappings
# ---------------------------------------------------------------------------


class TestGetMappings:
    """Tests for get_mappings()."""

    def test_maps_to_self(self, synthea_cdm):
        """Standard concepts typically map to themselves."""
        cl = Codelist({"hypertension": [320128]})
        mapped = get_mappings(synthea_cdm, cl, relationship_id="Maps to")
        assert isinstance(mapped, Codelist)
        assert "hypertension" in mapped
        # 320128 maps to itself
        assert 320128 in mapped["hypertension"]

    def test_maps_to_sinusitis(self, synthea_cdm):
        """40481087 maps to itself."""
        cl = Codelist({"sinusitis": [40481087]})
        mapped = get_mappings(synthea_cdm, cl)
        assert 40481087 in mapped["sinusitis"]

    def test_multiple_concept_sets(self, synthea_cdm):
        """Each concept set is mapped independently."""
        cl = Codelist(
            {
                "hypertension": [320128],
                "sinusitis": [40481087],
            }
        )
        mapped = get_mappings(synthea_cdm, cl)
        assert "hypertension" in mapped
        assert "sinusitis" in mapped

    def test_empty_codelist(self, synthea_cdm):
        """Empty codelist input returns empty codelist."""
        cl = Codelist({"empty": []})
        mapped = get_mappings(synthea_cdm, cl)
        assert mapped["empty"] == []

    def test_custom_name_style(self, synthea_cdm):
        """name_style parameter formats output names."""
        cl = Codelist({"hyp": [320128]})
        mapped = get_mappings(synthea_cdm, cl, name_style="mapped_{concept_set_name}")
        assert "mapped_hyp" in mapped

    def test_is_a_relationship(self, synthea_cdm):
        """Using 'Is a' relationship returns parents."""
        cl = Codelist({"hypertension": [320128]})
        mapped = get_mappings(synthea_cdm, cl, relationship_id="Is a")
        # 320128 "Is a" 316866 (Hypertensive disorder)
        assert 316866 in mapped["hypertension"]
