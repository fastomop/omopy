"""Tests for omopy.codelist._operations — union / intersect / compare."""

from __future__ import annotations

from omopy.codelist import compare_codelists, intersect_codelists, union_codelists
from omopy.generics.codelist import Codelist

# ---------------------------------------------------------------------------
# union_codelists
# ---------------------------------------------------------------------------


class TestUnionCodelists:
    """Tests for union_codelists()."""

    def test_union_same_name(self):
        """Union merges IDs for the same concept set name."""
        cl1 = Codelist({"a": [1, 2, 3]})
        cl2 = Codelist({"a": [3, 4, 5]})
        result = union_codelists(cl1, cl2)
        assert result["a"] == [1, 2, 3, 4, 5]

    def test_union_different_names(self):
        """Union preserves distinct names from each codelist."""
        cl1 = Codelist({"a": [1, 2]})
        cl2 = Codelist({"b": [3, 4]})
        result = union_codelists(cl1, cl2)
        assert "a" in result
        assert "b" in result
        assert result["a"] == [1, 2]
        assert result["b"] == [3, 4]

    def test_union_three_codelists(self):
        """Union of three codelists."""
        cl1 = Codelist({"x": [1]})
        cl2 = Codelist({"x": [2]})
        cl3 = Codelist({"x": [3]})
        result = union_codelists(cl1, cl2, cl3)
        assert result["x"] == [1, 2, 3]

    def test_union_empty(self):
        """Union with no arguments returns empty codelist."""
        result = union_codelists()
        assert len(result) == 0

    def test_union_single(self):
        """Union of a single codelist is itself."""
        cl = Codelist({"a": [1, 2, 3]})
        result = union_codelists(cl)
        assert result["a"] == [1, 2, 3]

    def test_union_deduplicates(self):
        """Union deduplicates IDs."""
        cl1 = Codelist({"a": [1, 1, 2]})
        cl2 = Codelist({"a": [2, 2, 3]})
        result = union_codelists(cl1, cl2)
        assert result["a"] == [1, 2, 3]

    def test_union_result_sorted(self):
        """Union result IDs are sorted."""
        cl1 = Codelist({"a": [5, 3, 1]})
        cl2 = Codelist({"a": [4, 2]})
        result = union_codelists(cl1, cl2)
        assert result["a"] == [1, 2, 3, 4, 5]

    def test_union_returns_codelist(self):
        """Union returns a Codelist instance."""
        cl1 = Codelist({"a": [1]})
        result = union_codelists(cl1)
        assert isinstance(result, Codelist)


# ---------------------------------------------------------------------------
# intersect_codelists
# ---------------------------------------------------------------------------


class TestIntersectCodelists:
    """Tests for intersect_codelists()."""

    def test_intersect_basic(self):
        """Intersect keeps only common IDs."""
        cl1 = Codelist({"a": [1, 2, 3]})
        cl2 = Codelist({"a": [2, 3, 4]})
        result = intersect_codelists(cl1, cl2)
        assert result["a"] == [2, 3]

    def test_intersect_no_overlap(self):
        """No overlapping IDs produces empty set (name not included)."""
        cl1 = Codelist({"a": [1, 2]})
        cl2 = Codelist({"a": [3, 4]})
        result = intersect_codelists(cl1, cl2)
        # No common IDs, so "a" may be absent
        assert result.get("a", []) == []

    def test_intersect_names_must_match(self):
        """Only names present in ALL codelists are kept."""
        cl1 = Codelist({"a": [1], "b": [2]})
        cl2 = Codelist({"a": [1], "c": [3]})
        result = intersect_codelists(cl1, cl2)
        assert "a" in result
        assert "b" not in result
        assert "c" not in result

    def test_intersect_three(self):
        """Intersect of three codelists."""
        cl1 = Codelist({"x": [1, 2, 3, 4]})
        cl2 = Codelist({"x": [2, 3, 4, 5]})
        cl3 = Codelist({"x": [3, 4, 5, 6]})
        result = intersect_codelists(cl1, cl2, cl3)
        assert result["x"] == [3, 4]

    def test_intersect_empty_input(self):
        """Intersect with no arguments returns empty."""
        result = intersect_codelists()
        assert len(result) == 0

    def test_intersect_result_sorted(self):
        """Intersection result is sorted."""
        cl1 = Codelist({"a": [5, 3, 1]})
        cl2 = Codelist({"a": [5, 1]})
        result = intersect_codelists(cl1, cl2)
        assert result["a"] == [1, 5]

    def test_intersect_returns_codelist(self):
        """Returns a Codelist instance."""
        cl1 = Codelist({"a": [1]})
        cl2 = Codelist({"a": [1]})
        result = intersect_codelists(cl1, cl2)
        assert isinstance(result, Codelist)


# ---------------------------------------------------------------------------
# compare_codelists
# ---------------------------------------------------------------------------


class TestCompareCodelists:
    """Tests for compare_codelists()."""

    def test_compare_basic(self):
        """Compare produces only_a, only_b, both."""
        cl_a = Codelist({"x": [1, 2, 3]})
        cl_b = Codelist({"x": [2, 3, 4]})
        result = compare_codelists(cl_a, cl_b)
        assert "x" in result
        assert result["x"]["only_a"] == [1]
        assert result["x"]["only_b"] == [4]
        assert result["x"]["both"] == [2, 3]

    def test_compare_disjoint(self):
        """Completely disjoint sets."""
        cl_a = Codelist({"x": [1, 2]})
        cl_b = Codelist({"x": [3, 4]})
        result = compare_codelists(cl_a, cl_b)
        assert result["x"]["only_a"] == [1, 2]
        assert result["x"]["only_b"] == [3, 4]
        assert result["x"]["both"] == []

    def test_compare_identical(self):
        """Identical sets."""
        cl_a = Codelist({"x": [1, 2, 3]})
        cl_b = Codelist({"x": [1, 2, 3]})
        result = compare_codelists(cl_a, cl_b)
        assert result["x"]["only_a"] == []
        assert result["x"]["only_b"] == []
        assert result["x"]["both"] == [1, 2, 3]

    def test_compare_different_names(self):
        """Names only in one codelist still appear in result."""
        cl_a = Codelist({"a": [1], "c": [5]})
        cl_b = Codelist({"b": [2], "c": [5, 6]})
        result = compare_codelists(cl_a, cl_b)
        # "a" only in cl_a
        assert result["a"]["only_a"] == [1]
        assert result["a"]["only_b"] == []
        assert result["a"]["both"] == []
        # "b" only in cl_b
        assert result["b"]["only_a"] == []
        assert result["b"]["only_b"] == [2]
        assert result["b"]["both"] == []
        # "c" in both
        assert result["c"]["only_a"] == []
        assert result["c"]["only_b"] == [6]
        assert result["c"]["both"] == [5]

    def test_compare_empty(self):
        """Compare two empty codelists."""
        result = compare_codelists(Codelist(), Codelist())
        assert len(result) == 0

    def test_compare_sorted_output(self):
        """Output lists are sorted."""
        cl_a = Codelist({"x": [5, 3, 1]})
        cl_b = Codelist({"x": [4, 2]})
        result = compare_codelists(cl_a, cl_b)
        assert result["x"]["only_a"] == [1, 3, 5]
        assert result["x"]["only_b"] == [2, 4]
