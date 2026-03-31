"""Hierarchy traversal — get descendants and ancestors via concept_ancestor.

Uses the OMOP ``concept_ancestor`` table to traverse the concept
hierarchy. This is the Python equivalent of R's ``getDescendants()``
and ``getAncestors()`` from CodelistGenerator.
"""

from __future__ import annotations

import ibis

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "get_ancestors",
    "get_descendants",
]


def get_descendants(
    cdm: CdmReference,
    concept_id: Codelist | int | list[int],
    *,
    include_self: bool = True,
    name: str | None = None,
) -> Codelist:
    """Get all descendant concepts via the concept_ancestor table.

    Parameters
    ----------
    cdm
        CDM reference with access to ``concept_ancestor`` and ``concept``.
    concept_id
        One or more ancestor concept IDs, or a :class:`Codelist` whose
        concept IDs will be extracted automatically.
    include_self
        If True, include the input concept(s) themselves.
    name
        Name for the resulting codelist entry. Defaults to ``"descendants_{id}"``.

    Returns
    -------
    Codelist
        A codelist mapping name to descendant concept IDs (standard only).
    """
    if isinstance(concept_id, Codelist):
        ids = sorted(concept_id.all_concept_ids)
        if name is None:
            names = concept_id.names
            name = f"descendants_{'_'.join(names)}" if names else None
    elif isinstance(concept_id, int):
        ids = [concept_id]
    else:
        ids = list(concept_id)

    ca = _get_ibis_table(cdm["concept_ancestor"])
    concept = _get_ibis_table(cdm["concept"])

    # Find all descendants
    descendants = ca.filter(
        ca["ancestor_concept_id"]
        .cast("int64")
        .isin([ibis.literal(int(i)) for i in ids])
    )
    if not include_self:
        descendants = descendants.filter(descendants["min_levels_of_separation"] > 0)

    # Get the descendant concept IDs
    desc_ids = descendants.select(
        concept_id=descendants["descendant_concept_id"].cast("int64")
    )

    # Join with concept to get only standard concepts
    result = (
        desc_ids.inner_join(
            concept,
            desc_ids["concept_id"] == concept["concept_id"].cast("int64"),
        )
        .filter(lambda t: t["standard_concept"] == ibis.literal("S"))
        .select("concept_id")
        .distinct()
    )

    # Execute and collect
    result_df = result.execute()
    result_ids = sorted(result_df["concept_id"].tolist())

    if name is None:
        name = f"descendants_{'_'.join(str(i) for i in ids)}"

    return Codelist({name: result_ids})


def get_ancestors(
    cdm: CdmReference,
    concept_id: Codelist | int | list[int],
    *,
    include_self: bool = True,
    name: str | None = None,
) -> Codelist:
    """Get all ancestor concepts via the concept_ancestor table.

    Parameters
    ----------
    cdm
        CDM reference with access to ``concept_ancestor`` and ``concept``.
    concept_id
        One or more descendant concept IDs, or a :class:`Codelist` whose
        concept IDs will be extracted automatically.
    include_self
        If True, include the input concept(s) themselves.
    name
        Name for the resulting codelist entry.

    Returns
    -------
    Codelist
        A codelist mapping name to ancestor concept IDs (standard only).
    """
    if isinstance(concept_id, Codelist):
        ids = sorted(concept_id.all_concept_ids)
        if name is None:
            names = concept_id.names
            name = f"ancestors_{'_'.join(names)}" if names else None
    elif isinstance(concept_id, int):
        ids = [concept_id]
    else:
        ids = list(concept_id)

    ca = _get_ibis_table(cdm["concept_ancestor"])
    concept = _get_ibis_table(cdm["concept"])

    ancestors = ca.filter(
        ca["descendant_concept_id"]
        .cast("int64")
        .isin([ibis.literal(int(i)) for i in ids])
    )
    if not include_self:
        ancestors = ancestors.filter(ancestors["min_levels_of_separation"] > 0)

    anc_ids = ancestors.select(
        concept_id=ancestors["ancestor_concept_id"].cast("int64")
    )

    result = (
        anc_ids.inner_join(
            concept,
            anc_ids["concept_id"] == concept["concept_id"].cast("int64"),
        )
        .filter(lambda t: t["standard_concept"] == ibis.literal("S"))
        .select("concept_id")
        .distinct()
    )

    result_df = result.execute()
    result_ids = sorted(result_df["concept_id"].tolist())

    if name is None:
        name = f"ancestors_{'_'.join(str(i) for i in ids)}"

    return Codelist({name: result_ids})
