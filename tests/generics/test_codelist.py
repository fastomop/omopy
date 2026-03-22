"""Tests for omopy.generics.codelist — Codelist, ConceptEntry, ConceptSetExpression."""

import pytest
from pydantic import ValidationError

from omopy.generics.codelist import Codelist, ConceptEntry, ConceptSetExpression


# ---------------------------------------------------------------------------
# ConceptEntry
# ---------------------------------------------------------------------------


class TestConceptEntry:
    def test_creation(self):
        ce = ConceptEntry(concept_id=201826, concept_name="Type 2 diabetes")
        assert ce.concept_id == 201826
        assert ce.concept_name == "Type 2 diabetes"
        assert ce.is_excluded is False
        assert ce.include_descendants is True
        assert ce.include_mapped is False

    def test_frozen(self):
        ce = ConceptEntry(concept_id=1)
        with pytest.raises(ValidationError, match="frozen_instance"):
            ce.concept_id = 2  # type: ignore[misc]

    def test_defaults(self):
        ce = ConceptEntry(concept_id=1)
        assert ce.concept_name == ""
        assert ce.domain_id == ""
        assert ce.vocabulary_id == ""
        assert ce.concept_class_id == ""
        assert ce.standard_concept == ""
        assert ce.concept_code == ""

    def test_equality(self):
        a = ConceptEntry(concept_id=1, is_excluded=True)
        b = ConceptEntry(concept_id=1, is_excluded=True)
        assert a == b

    def test_inequality(self):
        a = ConceptEntry(concept_id=1, is_excluded=True)
        b = ConceptEntry(concept_id=1, is_excluded=False)
        assert a != b


# ---------------------------------------------------------------------------
# Codelist
# ---------------------------------------------------------------------------


class TestCodelist:
    def test_creation_from_dict(self):
        cl = Codelist({"diabetes": [201826, 442793], "hypertension": [316866]})
        assert "diabetes" in cl
        assert cl["diabetes"] == [201826, 442793]

    def test_creation_empty(self):
        cl = Codelist()
        assert len(cl) == 0

    def test_creation_kwargs(self):
        cl = Codelist(diabetes=[201826])
        assert cl["diabetes"] == [201826]

    def test_names(self):
        cl = Codelist({"a": [1], "b": [2]})
        assert set(cl.names) == {"a", "b"}

    def test_all_concept_ids(self):
        cl = Codelist({"a": [1, 2], "b": [2, 3]})
        assert cl.all_concept_ids == {1, 2, 3}

    def test_setitem_valid(self):
        cl = Codelist()
        cl["test"] = [1, 2, 3]
        assert cl["test"] == [1, 2, 3]

    def test_setitem_invalid_key(self):
        cl = Codelist()
        with pytest.raises(TypeError, match="keys must be strings"):
            cl[42] = [1]  # type: ignore[index]

    def test_setitem_invalid_value(self):
        cl = Codelist()
        with pytest.raises(TypeError, match="lists of integers"):
            cl["test"] = [1, "two"]  # type: ignore[list-item]

    def test_creation_invalid_key_type(self):
        with pytest.raises(TypeError, match="keys must be strings"):
            Codelist({42: [1]})  # type: ignore[dict-item]

    def test_creation_invalid_value_type(self):
        with pytest.raises(TypeError, match="values must be lists"):
            Codelist({"a": (1, 2)})  # type: ignore[dict-item]

    def test_creation_invalid_concept_id(self):
        with pytest.raises(TypeError, match="Concept IDs must be integers"):
            Codelist({"a": [1, "two"]})  # type: ignore[list-item]

    def test_repr(self):
        cl = Codelist({"a": [1, 2], "b": [3]})
        r = repr(cl)
        assert "2 codelist(s)" in r
        assert "3 total concept ID(s)" in r

    def test_dict_operations(self):
        cl = Codelist({"a": [1]})
        assert len(cl) == 1
        del cl["a"]
        assert len(cl) == 0

    def test_iteration(self):
        cl = Codelist({"a": [1], "b": [2]})
        keys = list(cl)
        assert set(keys) == {"a", "b"}


# ---------------------------------------------------------------------------
# ConceptSetExpression
# ---------------------------------------------------------------------------


class TestConceptSetExpression:
    def test_creation(self):
        entries = [
            ConceptEntry(concept_id=201826, include_descendants=True),
            ConceptEntry(concept_id=442793, is_excluded=True),
        ]
        cse = ConceptSetExpression({"diabetes": entries})
        assert "diabetes" in cse
        assert len(cse["diabetes"]) == 2

    def test_names(self):
        cse = ConceptSetExpression({
            "a": [ConceptEntry(concept_id=1)],
            "b": [ConceptEntry(concept_id=2)],
        })
        assert set(cse.names) == {"a", "b"}

    def test_to_codelist(self):
        entries = [
            ConceptEntry(concept_id=1, is_excluded=False),
            ConceptEntry(concept_id=2, is_excluded=True),
            ConceptEntry(concept_id=3, is_excluded=False),
        ]
        cse = ConceptSetExpression({"test": entries})
        cl = cse.to_codelist()
        assert isinstance(cl, Codelist)
        # Excluded concepts should be dropped
        assert cl["test"] == [1, 3]

    def test_to_codelist_empty(self):
        cse = ConceptSetExpression({"test": []})
        cl = cse.to_codelist()
        assert cl["test"] == []

    def test_repr(self):
        cse = ConceptSetExpression({
            "a": [ConceptEntry(concept_id=1), ConceptEntry(concept_id=2)],
        })
        r = repr(cse)
        assert "1 concept set(s)" in r
        assert "2 total entries" in r
