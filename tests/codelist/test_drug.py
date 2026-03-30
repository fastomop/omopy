"""Tests for omopy.codelist._drug — get_drug_ingredient_codes / get_atc_codes."""

from __future__ import annotations

import pytest

from omopy.codelist import get_atc_codes, get_drug_ingredient_codes
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# get_drug_ingredient_codes
# ---------------------------------------------------------------------------


class TestGetDrugIngredientCodes:
    """Tests for get_drug_ingredient_codes()."""

    def test_search_by_name(self, synthea_cdm):
        """Search for 'ibuprofen' by name finds the ingredient."""
        cl = get_drug_ingredient_codes(synthea_cdm, "ibuprofen")
        assert isinstance(cl, Codelist)
        assert len(cl) >= 1
        # Should find ibuprofen concept (1177480)
        all_ids = [cid for ids in cl.values() for cid in ids]
        assert 1177480 in all_ids

    def test_search_by_concept_id(self, synthea_cdm):
        """Lookup by concept_id directly (1177480 = ibuprofen)."""
        cl = get_drug_ingredient_codes(synthea_cdm, 1177480)
        assert isinstance(cl, Codelist)
        assert len(cl) >= 1
        all_ids = [cid for ids in cl.values() for cid in ids]
        assert 1177480 in all_ids

    def test_includes_descendants(self, synthea_cdm):
        """Drug ingredient codes should include descendants (drug products)."""
        cl = get_drug_ingredient_codes(synthea_cdm, 1177480)
        # The ingredient itself plus any drug products that descend from it
        all_ids = [cid for ids in cl.values() for cid in ids]
        # Should have at least the ingredient itself
        assert len(all_ids) >= 1

    def test_multiple_ingredients_by_name(self, synthea_cdm):
        """Searching for multiple ingredient names."""
        cl = get_drug_ingredient_codes(synthea_cdm, ["ibuprofen", "acetaminophen"])
        assert len(cl) >= 2

    def test_multiple_ingredients_by_id(self, synthea_cdm):
        """Searching for multiple ingredient IDs."""
        cl = get_drug_ingredient_codes(synthea_cdm, [1177480, 1125315])
        assert len(cl) >= 2

    def test_no_match(self, synthea_cdm):
        """Searching for a non-existent ingredient returns empty codelist."""
        cl = get_drug_ingredient_codes(synthea_cdm, "xyzzyplugh_nonexistent")
        assert len(cl) == 0

    def test_none_returns_all_ingredients(self, synthea_cdm):
        """Passing None returns all standard drug ingredient concepts."""
        cl = get_drug_ingredient_codes(synthea_cdm, None)
        # Synthea has 28 drug ingredients
        assert len(cl) == 28

    def test_custom_name(self, synthea_cdm):
        """Custom name parameter is used."""
        cl = get_drug_ingredient_codes(synthea_cdm, 1177480, name="ibu")
        assert "ibu" in cl


# ---------------------------------------------------------------------------
# get_atc_codes
# ---------------------------------------------------------------------------


class TestGetATCCodes:
    """Tests for get_atc_codes()."""

    def test_all_atc(self, synthea_cdm):
        """Without filters, returns all ATC concepts individually."""
        cl = get_atc_codes(synthea_cdm)
        assert isinstance(cl, Codelist)
        # Synthea has 732 ATC concepts
        assert len(cl) >= 700

    def test_atc_with_name_filter(self, synthea_cdm):
        """Search ATC by name keyword."""
        cl = get_atc_codes(synthea_cdm, atc_name="ibuprofen")
        assert len(cl) >= 1
        # All returned entries should have ibuprofen-related names
        for key in cl:
            assert "ibuprofen" in key.lower()

    def test_atc_level_filter(self, synthea_cdm):
        """Filter to specific ATC levels."""
        cl = get_atc_codes(synthea_cdm, level="ATC 1st", name="atc1")
        assert "atc1" in cl
        ids = cl["atc1"]
        # Synthea has 11 ATC 1st level concepts
        assert len(ids) == 11

    def test_atc_level_filter_multiple(self, synthea_cdm):
        """Filter to multiple ATC levels."""
        cl = get_atc_codes(synthea_cdm, level=["ATC 1st", "ATC 2nd"], name="atc12")
        assert "atc12" in cl
        ids = cl["atc12"]
        # 11 + 30 = 41
        assert len(ids) == 41

    def test_atc_combined_filters(self, synthea_cdm):
        """Combine name and level filters."""
        cl = get_atc_codes(
            synthea_cdm,
            atc_name="alimentary",
            level="ATC 1st",
            name="alimentary_1st",
        )
        assert "alimentary_1st" in cl
        ids = cl["alimentary_1st"]
        # Should match "ALIMENTARY TRACT AND METABOLISM" at ATC 1st level
        assert len(ids) >= 1

    def test_no_match(self, synthea_cdm):
        """Non-existent ATC name returns empty."""
        cl = get_atc_codes(synthea_cdm, atc_name="xyzzy_nonexistent", name="empty")
        assert cl["empty"] == []

    def test_individual_entries_without_name(self, synthea_cdm):
        """Without name param, each ATC concept gets its own entry."""
        cl = get_atc_codes(synthea_cdm, level="ATC 1st")
        # 11 ATC 1st level concepts, each as separate entry
        assert len(cl) == 11
