"""Stratify codelists — split concept sets by domain, vocabulary, etc.

Takes a codelist and splits each concept set into multiple sub-sets
based on concept attributes (domain_id, vocabulary_id, concept_class_id).

This is the Python equivalent of R's ``stratifyByDomain()``,
``stratifyByVocabulary()``, etc. from CodelistGenerator.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "stratify_by_concept_class",
    "stratify_by_domain",
    "stratify_by_vocabulary",
]


def _stratify_by_attribute(
    codelist: Codelist,
    cdm: CdmReference,
    attribute: str,
) -> Codelist:
    """Internal helper: stratify a codelist by a concept attribute."""
    concept = _get_ibis_table(cdm["concept"])

    result = Codelist()
    for name, ids in codelist.items():
        if not ids:
            continue

        # Get attribute values for these concepts
        concept_attrs = concept.filter(
            concept["concept_id"].cast("int64").isin([ibis.literal(int(c)) for c in ids])
        ).select(
            concept_id=concept["concept_id"].cast("int64"),
            attr_val=concept[attribute],
        )

        df = concept_attrs.execute()

        # Group by attribute value
        groups: dict[str, list[int]] = {}
        for _, row in df.iterrows():
            val = str(row["attr_val"])
            cid = int(row["concept_id"])
            groups.setdefault(val, []).append(cid)

        for val, cids in groups.items():
            safe_val = val.lower().replace(" ", "_")
            result[f"{name}_{safe_val}"] = sorted(cids)

    return result


def stratify_by_domain(
    codelist: Codelist,
    cdm: CdmReference,
) -> Codelist:
    """Split each concept set by domain_id.

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference.

    Returns
    -------
    Codelist
        New codelist with entries like ``"{name}_{domain}"``.
    """
    return _stratify_by_attribute(codelist, cdm, "domain_id")


def stratify_by_vocabulary(
    codelist: Codelist,
    cdm: CdmReference,
) -> Codelist:
    """Split each concept set by vocabulary_id.

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference.

    Returns
    -------
    Codelist
        New codelist with entries like ``"{name}_{vocabulary}"``.
    """
    return _stratify_by_attribute(codelist, cdm, "vocabulary_id")


def stratify_by_concept_class(
    codelist: Codelist,
    cdm: CdmReference,
) -> Codelist:
    """Split each concept set by concept_class_id.

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference.

    Returns
    -------
    Codelist
        New codelist with entries like ``"{name}_{concept_class}"``.
    """
    return _stratify_by_attribute(codelist, cdm, "concept_class_id")
