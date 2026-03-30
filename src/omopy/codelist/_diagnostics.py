"""Diagnostics — summarise code usage and find orphan codes.

``summarise_code_use()`` counts how often each concept in a codelist
appears in the CDM domain tables.

``summarise_orphan_codes()`` finds related concepts (descendants,
mapped) that are used in the data but are NOT in the codelist.

This is the Python equivalent of R's ``summariseCodeUse()`` and
``summariseOrphanCodes()`` from CodelistGenerator.
"""

from __future__ import annotations

import polars as pl
import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.generics.summarised_result import SummarisedResult
from omopy.profiles._columns import _TABLE_COLUMNS
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "summarise_code_use",
    "summarise_orphan_codes",
]

# Domain → OMOP table name
_DOMAIN_TO_TABLE: dict[str, str] = {
    "condition": "condition_occurrence",
    "drug": "drug_exposure",
    "procedure": "procedure_occurrence",
    "observation": "observation",
    "measurement": "measurement",
    "visit": "visit_occurrence",
    "device": "device_exposure",
}


def summarise_code_use(
    codelist: Codelist,
    cdm: CdmReference,
    *,
    count_by: str = "record",
) -> pl.DataFrame:
    """Count usage of codelist concepts across CDM domain tables.

    For each concept in the codelist, counts how many records (or
    distinct persons) reference it in the appropriate domain table.

    Parameters
    ----------
    codelist
        Codelist to summarise.
    cdm
        CDM reference.
    count_by
        ``"record"`` for total record count or ``"person"`` for
        distinct person count.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: ``concept_set_name``, ``concept_id``,
        ``concept_name``, ``domain_id``, ``vocabulary_id``, ``count``.
    """
    concept = _get_ibis_table(cdm["concept"])

    rows: list[dict] = []
    for set_name, ids in codelist.items():
        if not ids:
            continue

        # Get concept info (domain, name, vocabulary)
        concept_info = concept.filter(
            concept["concept_id"].cast("int64").isin([ibis.literal(int(c)) for c in ids])
        ).select(
            concept_id=concept["concept_id"].cast("int64"),
            concept_name=concept["concept_name"],
            domain_id=concept["domain_id"],
            vocabulary_id=concept["vocabulary_id"],
        )

        info_df = concept_info.execute()

        # Group by domain
        domain_concepts: dict[str, list[int]] = {}
        concept_meta: dict[int, dict] = {}
        for _, row in info_df.iterrows():
            cid = int(row["concept_id"])
            domain = str(row["domain_id"]).lower()
            domain_concepts.setdefault(domain, []).append(cid)
            concept_meta[cid] = {
                "concept_name": str(row["concept_name"]),
                "domain_id": str(row["domain_id"]),
                "vocabulary_id": str(row["vocabulary_id"]),
            }

        # Count in each domain table
        for domain, cids in domain_concepts.items():
            table_name = _DOMAIN_TO_TABLE.get(domain)
            if table_name is None or table_name not in cdm:
                # No usage possible
                for cid in cids:
                    meta = concept_meta[cid]
                    rows.append(
                        {
                            "concept_set_name": set_name,
                            "concept_id": cid,
                            "concept_name": meta["concept_name"],
                            "domain_id": meta["domain_id"],
                            "vocabulary_id": meta["vocabulary_id"],
                            "count": 0,
                        }
                    )
                continue

            if table_name not in _TABLE_COLUMNS:
                continue

            col_info = _TABLE_COLUMNS[table_name]
            concept_col = col_info["concept_id"]
            domain_tbl = _get_ibis_table(cdm[table_name])

            # Count per concept_id
            if count_by == "person":
                counts = (
                    domain_tbl.filter(
                        domain_tbl[concept_col]
                        .cast("int64")
                        .isin([ibis.literal(int(c)) for c in cids])
                    )
                    .group_by(concept_id=domain_tbl[concept_col].cast("int64"))
                    .agg(count=domain_tbl["person_id"].nunique())
                )
            else:
                counts = (
                    domain_tbl.filter(
                        domain_tbl[concept_col]
                        .cast("int64")
                        .isin([ibis.literal(int(c)) for c in cids])
                    )
                    .group_by(concept_id=domain_tbl[concept_col].cast("int64"))
                    .agg(count=domain_tbl[concept_col].count())
                )

            counts_df = counts.execute()
            counted_ids: set[int] = set()
            for _, row in counts_df.iterrows():
                cid = int(row["concept_id"])
                meta = concept_meta[cid]
                rows.append(
                    {
                        "concept_set_name": set_name,
                        "concept_id": cid,
                        "concept_name": meta["concept_name"],
                        "domain_id": meta["domain_id"],
                        "vocabulary_id": meta["vocabulary_id"],
                        "count": int(row["count"]),
                    }
                )
                counted_ids.add(cid)

            # Concepts with zero count
            for cid in cids:
                if cid not in counted_ids:
                    meta = concept_meta[cid]
                    rows.append(
                        {
                            "concept_set_name": set_name,
                            "concept_id": cid,
                            "concept_name": meta["concept_name"],
                            "domain_id": meta["domain_id"],
                            "vocabulary_id": meta["vocabulary_id"],
                            "count": 0,
                        }
                    )

    if not rows:
        return pl.DataFrame(
            schema={
                "concept_set_name": pl.Utf8,
                "concept_id": pl.Int64,
                "concept_name": pl.Utf8,
                "domain_id": pl.Utf8,
                "vocabulary_id": pl.Utf8,
                "count": pl.Int64,
            }
        )

    return pl.DataFrame(rows)


def summarise_orphan_codes(
    codelist: Codelist,
    cdm: CdmReference,
) -> pl.DataFrame:
    """Find related concepts used in the data but not in the codelist.

    For each concept set, finds:
    1. Descendants not in the codelist
    2. Concepts mapped to codelist concepts but not included
    that actually appear in the CDM data.

    Parameters
    ----------
    codelist
        Input codelist.
    cdm
        CDM reference.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: ``concept_set_name``, ``concept_id``,
        ``concept_name``, ``domain_id``, ``relationship``, ``count``.
    """
    concept = _get_ibis_table(cdm["concept"])

    rows: list[dict] = []
    for set_name, ids in codelist.items():
        if not ids:
            continue

        codelist_set = set(ids)

        # 1. Find descendants not in codelist
        if "concept_ancestor" in cdm:
            ca = _get_ibis_table(cdm["concept_ancestor"])
            desc = (
                ca.filter(
                    ca["ancestor_concept_id"]
                    .cast("int64")
                    .isin([ibis.literal(int(c)) for c in ids])
                )
                .select(concept_id=ca["descendant_concept_id"].cast("int64"))
                .distinct()
            )
            desc_df = desc.execute()
            descendant_ids = set(desc_df["concept_id"].tolist()) - codelist_set
        else:
            descendant_ids = set()

        # 2. Find mapped concepts not in codelist
        if "concept_relationship" in cdm:
            cr = _get_ibis_table(cdm["concept_relationship"])
            mapped = (
                cr.filter(
                    cr["concept_id_1"].cast("int64").isin([ibis.literal(int(c)) for c in ids])
                    & (cr["relationship_id"] == ibis.literal("Maps to"))
                )
                .select(concept_id=cr["concept_id_2"].cast("int64"))
                .distinct()
            )
            mapped_df = mapped.execute()
            mapped_ids = set(mapped_df["concept_id"].tolist()) - codelist_set
        else:
            mapped_ids = set()

        # Combine orphan candidates
        orphan_candidates = descendant_ids | mapped_ids
        if not orphan_candidates:
            continue

        # Check which orphans are actually in use
        # Get concept info
        orphan_info = concept.filter(
            concept["concept_id"]
            .cast("int64")
            .isin([ibis.literal(int(c)) for c in orphan_candidates])
        ).select(
            concept_id=concept["concept_id"].cast("int64"),
            concept_name=concept["concept_name"],
            domain_id=concept["domain_id"],
        )

        orphan_df = orphan_info.execute()
        orphan_meta: dict[int, dict] = {}
        domain_orphans: dict[str, list[int]] = {}
        for _, row in orphan_df.iterrows():
            cid = int(row["concept_id"])
            domain = str(row["domain_id"]).lower()
            orphan_meta[cid] = {
                "concept_name": str(row["concept_name"]),
                "domain_id": str(row["domain_id"]),
            }
            domain_orphans.setdefault(domain, []).append(cid)

        # Check each domain table for usage
        for domain, cids in domain_orphans.items():
            table_name = _DOMAIN_TO_TABLE.get(domain)
            if table_name is None or table_name not in cdm:
                continue
            if table_name not in _TABLE_COLUMNS:
                continue

            col_info = _TABLE_COLUMNS[table_name]
            concept_col = col_info["concept_id"]
            domain_tbl = _get_ibis_table(cdm[table_name])

            counts = (
                domain_tbl.filter(
                    domain_tbl[concept_col]
                    .cast("int64")
                    .isin([ibis.literal(int(c)) for c in cids])
                )
                .group_by(concept_id=domain_tbl[concept_col].cast("int64"))
                .agg(count=domain_tbl[concept_col].count())
            )

            counts_df = counts.execute()
            for _, row in counts_df.iterrows():
                cid = int(row["concept_id"])
                count = int(row["count"])
                if count > 0:
                    meta = orphan_meta[cid]
                    relationship = "descendant" if cid in descendant_ids else "mapped"
                    rows.append(
                        {
                            "concept_set_name": set_name,
                            "concept_id": cid,
                            "concept_name": meta["concept_name"],
                            "domain_id": meta["domain_id"],
                            "relationship": relationship,
                            "count": count,
                        }
                    )

    if not rows:
        return pl.DataFrame(
            schema={
                "concept_set_name": pl.Utf8,
                "concept_id": pl.Int64,
                "concept_name": pl.Utf8,
                "domain_id": pl.Utf8,
                "relationship": pl.Utf8,
                "count": pl.Int64,
            }
        )

    return pl.DataFrame(rows)
