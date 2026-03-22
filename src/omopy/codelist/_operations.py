"""Codelist operations — union, intersect, compare codelists.

Pure in-memory set operations on Codelist objects. No database access needed.

This is the Python equivalent of R's ``unionCodelists()``,
``intersectCodelists()``, and ``compareCodelists()`` from CodelistGenerator.
"""

from __future__ import annotations

from omopy.generics.codelist import Codelist

__all__ = [
    "compare_codelists",
    "intersect_codelists",
    "union_codelists",
]


def union_codelists(*codelists: Codelist) -> Codelist:
    """Union multiple codelists, merging concept IDs per name.

    Concept sets with the same name across different codelists are merged
    (set union). Concept sets with distinct names are preserved as-is.

    Parameters
    ----------
    *codelists
        One or more Codelist objects.

    Returns
    -------
    Codelist
        Merged codelist.
    """
    result: dict[str, set[int]] = {}
    for cl in codelists:
        for name, ids in cl.items():
            if name in result:
                result[name].update(ids)
            else:
                result[name] = set(ids)
    return Codelist({k: sorted(v) for k, v in result.items()})


def intersect_codelists(*codelists: Codelist) -> Codelist:
    """Intersect multiple codelists, keeping only shared concept IDs.

    For each concept set name present in ALL input codelists, returns
    only the concept IDs that appear in every codelist's version of
    that concept set.

    Parameters
    ----------
    *codelists
        Two or more Codelist objects.

    Returns
    -------
    Codelist
        Codelist with intersected concept sets.
    """
    if not codelists:
        return Codelist()

    # Find names present in all codelists
    common_names = set(codelists[0].keys())
    for cl in codelists[1:]:
        common_names &= set(cl.keys())

    result: dict[str, list[int]] = {}
    for name in common_names:
        ids = set(codelists[0][name])
        for cl in codelists[1:]:
            ids &= set(cl[name])
        if ids:
            result[name] = sorted(ids)

    return Codelist(result)


def compare_codelists(
    codelist_a: Codelist,
    codelist_b: Codelist,
) -> dict[str, dict[str, list[int]]]:
    """Compare two codelists element-by-element.

    For each concept set name present in both codelists, computes:
    - ``only_a``: concept IDs only in codelist_a
    - ``only_b``: concept IDs only in codelist_b
    - ``both``: concept IDs in both

    Parameters
    ----------
    codelist_a, codelist_b
        Two codelists to compare.

    Returns
    -------
    dict[str, dict[str, list[int]]]
        Mapping of concept set names to comparison results.
    """
    all_names = set(codelist_a.keys()) | set(codelist_b.keys())
    result: dict[str, dict[str, list[int]]] = {}

    for name in sorted(all_names):
        ids_a = set(codelist_a.get(name, []))
        ids_b = set(codelist_b.get(name, []))
        result[name] = {
            "only_a": sorted(ids_a - ids_b),
            "only_b": sorted(ids_b - ids_a),
            "both": sorted(ids_a & ids_b),
        }

    return result
