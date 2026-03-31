"""Keyword search — find candidate concepts by searching concept names.

The ``get_candidate_codes()`` function searches the OMOP ``concept``
table using keyword patterns (SQL LIKE matching) on ``concept_name``.
Optionally searches ``concept_synonym`` as well.

``get_mappings()`` finds mapped concepts via ``concept_relationship``.

This is the Python equivalent of R's ``getCandidateCodes()`` and
``getMappings()`` from CodelistGenerator.
"""

from __future__ import annotations

import ibis

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "get_candidate_codes",
    "get_mappings",
]


def get_candidate_codes(
    cdm: CdmReference,
    keywords: str | list[str],
    *,
    search_synonyms: bool = False,
    exclude: str | list[str] | None = None,
    domains: str | list[str] | None = None,
    standard_concept: str | list[str] | None = None,
    vocabulary_id: str | list[str] | None = None,
    concept_class_id: str | list[str] | None = None,
    include_descendants: bool = False,
    name: str | None = None,
) -> Codelist:
    """Search for concepts by keyword matching on concept_name.

    Parameters
    ----------
    cdm
        CDM reference with access to ``concept`` (and optionally
        ``concept_synonym``).
    keywords
        One or more keyword strings. Each is matched using SQL LIKE
        (case-insensitive, surrounded by ``%``).
    search_synonyms
        If True, also search ``concept_synonym.concept_synonym_name``.
    exclude
        Keywords to exclude (concepts matching these are removed).
    domains
        Restrict to specific domain_id(s) (e.g. ``"Condition"``).
    standard_concept
        Filter by standard_concept value(s) (``"S"``, ``"C"``, etc.).
        Defaults to ``None`` (no filter). Pass ``"S"`` for standard only.
    vocabulary_id
        Restrict to specific vocabulary_id(s).
    concept_class_id
        Restrict to specific concept_class_id(s).
    include_descendants
        If True, include descendants of matching concepts via
        ``concept_ancestor``.
    name
        Name for the codelist entry. Defaults to the first keyword.

    Returns
    -------
    Codelist
        A codelist with one entry containing matching concept IDs.
    """
    kw_list = [keywords] if isinstance(keywords, str) else list(keywords)
    concept_tbl = _get_ibis_table(cdm["concept"])

    # Build keyword LIKE filters (case-insensitive OR across keywords)
    kw_filters = []
    for kw in kw_list:
        pattern = f"%{kw.lower()}%"
        kw_filters.append(concept_tbl["concept_name"].lower().like(pattern))

    # Combine keywords with OR
    combined_filter = kw_filters[0]
    for f in kw_filters[1:]:
        combined_filter = combined_filter | f

    result = concept_tbl.filter(combined_filter)

    # Search synonyms if requested
    if search_synonyms and "concept_synonym" in cdm:
        syn_tbl = _get_ibis_table(cdm["concept_synonym"])
        # Only search if there are rows
        syn_filters = []
        for kw in kw_list:
            pattern = f"%{kw.lower()}%"
            syn_filters.append(
                syn_tbl["concept_synonym_name"].cast("string").lower().like(pattern)
            )
        syn_combined = syn_filters[0]
        for f in syn_filters[1:]:
            syn_combined = syn_combined | f

        syn_matches = (
            syn_tbl.filter(syn_combined)
            .select(concept_id=syn_tbl["concept_id"])
            .distinct()
        )

        # Get full concept rows for synonym matches
        syn_concepts = concept_tbl.inner_join(
            syn_matches,
            concept_tbl["concept_id"] == syn_matches["concept_id"],
        )

        # Union with direct keyword matches
        shared_cols = [c for c in result.columns if c in syn_concepts.columns]
        result = (
            result.select(*shared_cols)
            .union(syn_concepts.select(*shared_cols))
            .distinct()
        )

    # Apply optional filters
    if domains is not None:
        dom_list = [domains] if isinstance(domains, str) else list(domains)
        result = result.filter(result["domain_id"].isin(dom_list))

    if standard_concept is not None:
        sc_list = (
            [standard_concept]
            if isinstance(standard_concept, str)
            else list(standard_concept)
        )
        result = result.filter(result["standard_concept"].isin(sc_list))

    if vocabulary_id is not None:
        v_list = (
            [vocabulary_id] if isinstance(vocabulary_id, str) else list(vocabulary_id)
        )
        result = result.filter(result["vocabulary_id"].isin(v_list))

    if concept_class_id is not None:
        cc_list = (
            [concept_class_id]
            if isinstance(concept_class_id, str)
            else list(concept_class_id)
        )
        result = result.filter(result["concept_class_id"].isin(cc_list))

    # Apply exclude keywords
    if exclude is not None:
        excl_list = [exclude] if isinstance(exclude, str) else list(exclude)
        for excl in excl_list:
            pattern = f"%{excl.lower()}%"
            result = result.filter(~result["concept_name"].lower().like(pattern))

    # Get concept IDs
    matched_ids = result.select(
        concept_id=result["concept_id"].cast("int64")
    ).distinct()

    # Include descendants if requested
    if include_descendants and "concept_ancestor" in cdm:
        ca = _get_ibis_table(cdm["concept_ancestor"])
        desc = (
            ca.inner_join(
                matched_ids,
                ca["ancestor_concept_id"].cast("int64") == matched_ids["concept_id"],
            )
            .select(concept_id=ca["descendant_concept_id"].cast("int64"))
            .distinct()
        )

        matched_ids = matched_ids.union(desc).distinct()

    # Execute
    result_df = matched_ids.execute()
    result_ids = sorted(result_df["concept_id"].tolist())

    if name is None:
        name = kw_list[0].lower().replace(" ", "_")

    return Codelist({name: result_ids})


def get_mappings(
    cdm: CdmReference,
    codelist: Codelist,
    *,
    relationship_id: str | list[str] = "Maps to",
    name_style: str = "{concept_set_name}",
) -> Codelist:
    """Get mapped concepts via concept_relationship.

    For each concept set in the codelist, finds concepts linked via
    the specified relationship(s) in ``concept_relationship``.

    Parameters
    ----------
    cdm
        CDM reference.
    codelist
        Input codelist with concept IDs to find mappings for.
    relationship_id
        Relationship type(s) to follow (e.g. ``"Maps to"``).
    name_style
        Naming template. ``{concept_set_name}`` is replaced with the
        original concept set name.

    Returns
    -------
    Codelist
        New codelist with mapped concept IDs.
    """
    cr = _get_ibis_table(cdm["concept_relationship"])
    rel_list = (
        [relationship_id] if isinstance(relationship_id, str) else list(relationship_id)
    )

    result = Codelist()
    for set_name, concept_ids in codelist.items():
        if not concept_ids:
            result[set_name] = []
            continue

        # Filter concept_relationship to these concept IDs and relationship
        matches = (
            cr.filter(
                cr["concept_id_1"]
                .cast("int64")
                .isin([ibis.literal(int(c)) for c in concept_ids])
                & cr["relationship_id"].isin(rel_list)
            )
            .select(concept_id=cr["concept_id_2"].cast("int64"))
            .distinct()
        )

        result_df = matches.execute()
        mapped_ids = sorted(result_df["concept_id"].tolist())

        out_name = name_style.replace("{concept_set_name}", set_name)
        result[out_name] = mapped_ids

    return result
