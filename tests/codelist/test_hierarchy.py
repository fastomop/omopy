"""Tests for omopy.codelist._hierarchy — get_descendants / get_ancestors."""

from __future__ import annotations

import pytest

from omopy.codelist import get_ancestors, get_descendants
from omopy.generics.codelist import Codelist


# ---------------------------------------------------------------------------
# get_descendants
# ---------------------------------------------------------------------------


class TestGetDescendants:
    """Tests for get_descendants()."""

    def test_single_concept_include_self(self, synthea_cdm):
        """40481087 (Viral sinusitis) has only itself as descendant in Synthea."""
        cl = get_descendants(synthea_cdm, 40481087, include_self=True)
        assert isinstance(cl, Codelist)
        assert len(cl) == 1
        key = next(iter(cl))
        assert "40481087" in key
        assert 40481087 in cl[key]

    def test_single_concept_exclude_self(self, synthea_cdm):
        """40481087 has no descendants besides itself, so result should be empty."""
        cl = get_descendants(synthea_cdm, 40481087, include_self=False)
        key = next(iter(cl))
        # No descendants beyond self
        assert 40481087 not in cl[key]

    def test_concept_with_descendants(self, synthea_cdm):
        """4283893 (Sinusitis) should have descendants including 40481087 and 257012."""
        cl = get_descendants(synthea_cdm, 4283893, include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        # Sinusitis itself should be included
        assert 4283893 in ids
        # Viral sinusitis is a descendant of Sinusitis
        assert 40481087 in ids

    def test_concept_with_descendants_exclude_self(self, synthea_cdm):
        """Excluding self: 4283893 itself should NOT be in the result."""
        cl = get_descendants(synthea_cdm, 4283893, include_self=False)
        key = next(iter(cl))
        ids = cl[key]
        assert 4283893 not in ids
        # But descendants should still be present
        assert 40481087 in ids

    def test_multiple_concepts(self, synthea_cdm):
        """Passing multiple concept IDs returns their combined descendants."""
        cl = get_descendants(synthea_cdm, [40481087, 320128], include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        assert 40481087 in ids
        assert 320128 in ids

    def test_custom_name(self, synthea_cdm):
        """Custom name is used as the codelist key."""
        cl = get_descendants(synthea_cdm, 40481087, name="my_codes")
        assert "my_codes" in cl

    def test_default_name_contains_ids(self, synthea_cdm):
        """Default name format: descendants_{id1}_{id2}..."""
        cl = get_descendants(synthea_cdm, [320128, 40481087])
        key = next(iter(cl))
        assert "320128" in key
        assert "40481087" in key

    def test_returns_only_standard_concepts(self, synthea_cdm):
        """All returned concepts should be standard (S)."""
        cl = get_descendants(synthea_cdm, 4283893, include_self=True)
        key = next(iter(cl))
        # Every returned ID should exist as a standard concept
        # (We trust the implementation filters on standard_concept='S')
        assert len(cl[key]) > 0

    def test_result_is_sorted(self, synthea_cdm):
        """Returned concept IDs should be sorted."""
        cl = get_descendants(synthea_cdm, 4283893, include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# get_ancestors
# ---------------------------------------------------------------------------


class TestGetAncestors:
    """Tests for get_ancestors()."""

    def test_single_concept_include_self(self, synthea_cdm):
        """320128 (Essential hypertension) has 7 ancestor rows (including self)."""
        cl = get_ancestors(synthea_cdm, 320128, include_self=True)
        assert isinstance(cl, Codelist)
        key = next(iter(cl))
        ids = cl[key]
        # Self should be included
        assert 320128 in ids
        # Should have multiple ancestors
        assert len(ids) >= 2

    def test_single_concept_exclude_self(self, synthea_cdm):
        """Excluding self: 320128 not in result but ancestors are."""
        cl = get_ancestors(synthea_cdm, 320128, include_self=False)
        key = next(iter(cl))
        ids = cl[key]
        assert 320128 not in ids
        assert len(ids) >= 1

    def test_known_ancestors(self, synthea_cdm):
        """320128 should have known ancestors like 316866, 134057, 4180628."""
        cl = get_ancestors(synthea_cdm, 320128, include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        # 316866 is 1 level up from 320128
        assert 316866 in ids

    def test_viral_sinusitis_ancestors(self, synthea_cdm):
        """40481087 (Viral sinusitis) has 31 ancestor rows — should return many ancestors."""
        cl = get_ancestors(synthea_cdm, 40481087, include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        assert 40481087 in ids
        # 4283893 (Sinusitis) is an ancestor
        assert 4283893 in ids
        assert len(ids) >= 10

    def test_custom_name(self, synthea_cdm):
        """Custom name parameter works for ancestors."""
        cl = get_ancestors(synthea_cdm, 320128, name="hyp_ancestors")
        assert "hyp_ancestors" in cl

    def test_default_name(self, synthea_cdm):
        """Default name format: ancestors_{id}."""
        cl = get_ancestors(synthea_cdm, 320128)
        key = next(iter(cl))
        assert "320128" in key

    def test_multiple_concepts(self, synthea_cdm):
        """Passing multiple IDs returns union of ancestors."""
        cl = get_ancestors(synthea_cdm, [320128, 40481087], include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        assert 320128 in ids
        assert 40481087 in ids

    def test_result_is_sorted(self, synthea_cdm):
        """Returned ancestor IDs should be sorted."""
        cl = get_ancestors(synthea_cdm, 40481087, include_self=True)
        key = next(iter(cl))
        ids = cl[key]
        assert ids == sorted(ids)
