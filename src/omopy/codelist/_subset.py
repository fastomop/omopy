"""Subset codelists — filter concept IDs by domain, vocabulary, or usage.

Subsets codelists by joining against the ``concept`` vocabulary table
to filter by domain_id, vocabulary_id, or by checking whether concepts
actually occur in the CDM data.

This is the Python equivalent of R's ``subsetOnDomain()``,
``subsetOnVocabulary()``, and ``subsetToCodesInUse()`` from
CodelistGenerator.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.profiles._columns import _TABLE_COLUMNS
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "subset_by_domain",
    "subset_by_vocabulary",
    "subset_to_codes_in_use",
]

# Domain → OMOP table name (same mapping used in concept intersect)
_DOMAIN_TO_TABLE: dict[str, str] = {
    "condition": "condition_occurrence",
    "drug": "drug_exposure",
    "procedure": "procedure_occurrence",
    "observation": "observation",
    "measurement": "measurement",
    "visit": "visit_occurrence",
    "device": "device_exposure",
    "specimen": "specimen",
    "episode": "episode",
}


def subset_by_domain(
    codelist: Codelist,
    cdm: CdmReference,
    domain_id: str | list[str],
) -> Codelist:
    """Subset a codelist to concepts in specific domain(s).

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference (for concept table lookup).
    domain_id
        Domain(s) to keep (e.g. ``"Condition"``, ``"Drug"``).

    Returns
    -------
    Codelist
        Codelist filtered to concepts in the specified domain(s).
    """
    domains = [domain_id] if isinstance(domain_id, str) else list(domain_id)
    concept = _get_ibis_table(cdm["concept"])

    result = Codelist()
    for name, ids in codelist.items():
        if not ids:
            result[name] = []
            continue

        matching = concept.filter(
            concept["concept_id"].cast("int64").isin(
                [ibis.literal(int(c)) for c in ids]
            )
            & concept["domain_id"].isin(domains)
        ).select(
            concept_id=concept["concept_id"].cast("int64")
        ).distinct()

        df = matching.execute()
        result[name] = sorted(df["concept_id"].tolist())

    return result


def subset_by_vocabulary(
    codelist: Codelist,
    cdm: CdmReference,
    vocabulary_id: str | list[str],
) -> Codelist:
    """Subset a codelist to concepts in specific vocabulary(ies).

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference.
    vocabulary_id
        Vocabulary ID(s) to keep (e.g. ``"SNOMED"``, ``"RxNorm"``).

    Returns
    -------
    Codelist
        Codelist filtered to concepts in the specified vocabulary(ies).
    """
    vocabs = [vocabulary_id] if isinstance(vocabulary_id, str) else list(vocabulary_id)
    concept = _get_ibis_table(cdm["concept"])

    result = Codelist()
    for name, ids in codelist.items():
        if not ids:
            result[name] = []
            continue

        matching = concept.filter(
            concept["concept_id"].cast("int64").isin(
                [ibis.literal(int(c)) for c in ids]
            )
            & concept["vocabulary_id"].isin(vocabs)
        ).select(
            concept_id=concept["concept_id"].cast("int64")
        ).distinct()

        df = matching.execute()
        result[name] = sorted(df["concept_id"].tolist())

    return result


def subset_to_codes_in_use(
    codelist: Codelist,
    cdm: CdmReference,
) -> Codelist:
    """Subset a codelist to only concepts that actually appear in the CDM.

    Checks each domain table for the presence of concept IDs.

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference with access to domain tables.

    Returns
    -------
    Codelist
        Codelist filtered to concepts found in the data.
    """
    concept = _get_ibis_table(cdm["concept"])

    # For each concept set, determine domains and check domain tables
    result = Codelist()
    for name, ids in codelist.items():
        if not ids:
            result[name] = []
            continue

        # Look up domain for each concept
        concept_domains = concept.filter(
            concept["concept_id"].cast("int64").isin(
                [ibis.literal(int(c)) for c in ids]
            )
        ).select(
            concept_id=concept["concept_id"].cast("int64"),
            domain_id=concept["domain_id"].lower(),
        )

        domain_df = concept_domains.execute()

        # Group by domain
        domain_concepts: dict[str, list[int]] = {}
        for _, row in domain_df.iterrows():
            d = row["domain_id"]
            cid = int(row["concept_id"])
            domain_concepts.setdefault(d, []).append(cid)

        found_ids: set[int] = set()
        for domain, cids in domain_concepts.items():
            table_name = _DOMAIN_TO_TABLE.get(domain)
            if table_name is None or table_name not in cdm:
                continue
            if table_name not in _TABLE_COLUMNS:
                continue

            col_info = _TABLE_COLUMNS[table_name]
            concept_col = col_info["concept_id"]
            domain_tbl = _get_ibis_table(cdm[table_name])

            used = domain_tbl.filter(
                domain_tbl[concept_col].cast("int64").isin(
                    [ibis.literal(int(c)) for c in cids]
                )
            ).select(
                concept_id=domain_tbl[concept_col].cast("int64")
            ).distinct()

            used_df = used.execute()
            found_ids.update(used_df["concept_id"].tolist())

        result[name] = sorted(found_ids & set(ids))

    return result
