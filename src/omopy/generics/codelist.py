"""Codelist and ConceptSetExpression types.

Mirrors R's ``codelist`` and ``conceptSetExpression`` classes from omopgenerics.

A ``Codelist`` is a named mapping of concept set names -> lists of concept IDs.
A ``ConceptSetExpression`` is a named mapping of concept set names -> lists of
concept entries with include/exclude/descendants/mapped flags.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

__all__ = ["Codelist", "ConceptSetExpression", "ConceptEntry"]


# ---------------------------------------------------------------------------
# ConceptEntry — a single concept in a concept set expression
# ---------------------------------------------------------------------------


class ConceptEntry(BaseModel):
    """A single concept within a concept set expression.

    Matches the ATLAS JSON format::

        {
          "concept": {"CONCEPT_ID": 123, "CONCEPT_NAME": "Foo", ...},
          "isExcluded": false,
          "includeDescendants": true,
          "includeMapped": false
        }
    """

    model_config = ConfigDict(frozen=True)

    concept_id: int
    concept_name: str = ""
    domain_id: str = ""
    vocabulary_id: str = ""
    concept_class_id: str = ""
    standard_concept: str = ""
    concept_code: str = ""
    is_excluded: bool = False
    include_descendants: bool = True
    include_mapped: bool = False


# ---------------------------------------------------------------------------
# Codelist
# ---------------------------------------------------------------------------


class Codelist(dict[str, list[int]]):
    """A named collection of concept ID lists.

    Inherits from ``dict[str, list[int]]``. Keys are codelist names,
    values are lists of integer concept IDs.

    Usage::

        cl = Codelist({"diabetes": [201826, 442793], "hypertension": [316866]})
        assert "diabetes" in cl
        assert cl["diabetes"] == [201826, 442793]
    """

    def __init__(self, data: dict[str, list[int]] | None = None, /, **kwargs: list[int]) -> None:
        super().__init__()
        if data:
            self.update(data)
        if kwargs:
            self.update(kwargs)
        self._validate()

    def _validate(self) -> None:
        for name, ids in self.items():
            if not isinstance(name, str):
                msg = f"Codelist keys must be strings, got {type(name).__name__}"
                raise TypeError(msg)
            if not isinstance(ids, list):
                msg = f"Codelist values must be lists, got {type(ids).__name__} for key {name!r}"
                raise TypeError(msg)
            for i, cid in enumerate(ids):
                if not isinstance(cid, int):
                    msg = (
                        f"Concept IDs must be integers, got {type(cid).__name__} "
                        f"at {name!r}[{i}]"
                    )
                    raise TypeError(msg)

    def __setitem__(self, key: str, value: list[int]) -> None:
        if not isinstance(key, str):
            msg = f"Codelist keys must be strings, got {type(key).__name__}"
            raise TypeError(msg)
        if not isinstance(value, list) or not all(isinstance(v, int) for v in value):
            msg = f"Codelist values must be lists of integers"
            raise TypeError(msg)
        super().__setitem__(key, value)

    @property
    def names(self) -> list[str]:
        """Return codelist names."""
        return list(self.keys())

    @property
    def all_concept_ids(self) -> set[int]:
        """Return all unique concept IDs across all codelists."""
        return {cid for ids in self.values() for cid in ids}

    def __repr__(self) -> str:
        n = len(self)
        total = sum(len(v) for v in self.values())
        return f"Codelist({n} codelist(s), {total} total concept ID(s))"


# ---------------------------------------------------------------------------
# ConceptSetExpression
# ---------------------------------------------------------------------------


class ConceptSetExpression(dict[str, list[ConceptEntry]]):
    """A named collection of concept set expressions (with flags).

    Each entry includes concept metadata plus ``is_excluded``,
    ``include_descendants``, and ``include_mapped`` flags.

    Usage::

        cse = ConceptSetExpression({
            "diabetes": [
                ConceptEntry(concept_id=201826, include_descendants=True),
                ConceptEntry(concept_id=442793, is_excluded=True),
            ]
        })
    """

    def __init__(
        self,
        data: dict[str, list[ConceptEntry]] | None = None,
        /,
        **kwargs: list[ConceptEntry],
    ) -> None:
        super().__init__()
        if data:
            self.update(data)
        if kwargs:
            self.update(kwargs)

    @property
    def names(self) -> list[str]:
        return list(self.keys())

    def to_codelist(self) -> Codelist:
        """Convert to a simple Codelist (dropping flags, keeping only included concepts)."""
        result: dict[str, list[int]] = {}
        for name, entries in self.items():
            result[name] = [e.concept_id for e in entries if not e.is_excluded]
        return Codelist(result)

    def __repr__(self) -> str:
        n = len(self)
        total = sum(len(v) for v in self.values())
        return f"ConceptSetExpression({n} concept set(s), {total} total entries)"
