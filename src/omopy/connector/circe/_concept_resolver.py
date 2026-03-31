"""Concept set resolution against the database.

Resolves concept sets by expanding descendants (via ``concept_ancestor``),
mapped concepts (via ``concept_relationship``), and handling exclusions.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.connector.circe._types import ConceptItem, ConceptSet

__all__ = ["resolve_concept_sets"]


def resolve_concept_sets(
    concept_sets: tuple[ConceptSet, ...],
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
) -> dict[int, ir.Table]:
    """Resolve concept sets to tables of concept IDs.

    For each concept set, produces an Ibis Table with a single column
    ``concept_id`` (int64) containing all resolved concept IDs.

    Parameters
    ----------
    concept_sets
        Parsed concept sets from the cohort expression.
    con
        Ibis backend connection.
    catalog
        Database catalog name.
    cdm_schema
        Schema containing CDM vocabulary tables.

    Returns
    -------
    dict[int, ir.Table]
        Mapping from concept set ID to a table of resolved concept IDs.
    """
    concept_tbl = con.table("concept", database=(catalog, cdm_schema))
    ancestor_tbl = con.table("concept_ancestor", database=(catalog, cdm_schema))
    relationship_tbl = con.table("concept_relationship", database=(catalog, cdm_schema))

    result: dict[int, ir.Table] = {}

    for cs in concept_sets:
        if not cs.items:
            # Empty concept set: return empty table
            result[cs.id] = _empty_concept_id_table(con)
            continue

        # Split into included and excluded items
        included = [item for item in cs.items if not item.is_excluded]
        excluded = [item for item in cs.items if item.is_excluded]

        # Build the included concept IDs
        if included:
            include_expr = _resolve_items(
                included, concept_tbl, ancestor_tbl, relationship_tbl
            )
        else:
            include_expr = _empty_concept_id_table(con)

        # Build the excluded concept IDs
        if excluded:
            exclude_expr = _resolve_items(
                excluded, concept_tbl, ancestor_tbl, relationship_tbl
            )
            # Final = included EXCEPT excluded
            final_expr = include_expr.difference(exclude_expr)
        else:
            final_expr = include_expr

        result[cs.id] = final_expr

    return result


def _resolve_items(
    items: list[ConceptItem],
    concept_tbl: ir.Table,
    ancestor_tbl: ir.Table,
    relationship_tbl: ir.Table,
) -> ir.Table:
    """Resolve a list of concept items (all included or all excluded).

    Returns a table with a single ``concept_id`` column.
    """
    parts: list[ir.Table] = []

    for item in items:
        cid = item.concept.concept_id

        # Direct concept
        direct = concept_tbl.filter(concept_tbl.concept_id == cid).select(
            concept_tbl.concept_id
        )
        parts.append(direct)

        # Include descendants via concept_ancestor
        if item.include_descendants:
            descendants = (
                ancestor_tbl.filter(ancestor_tbl.ancestor_concept_id == cid)
                .select(ancestor_tbl.descendant_concept_id)
                .rename(concept_id="descendant_concept_id")
            )
            parts.append(descendants)

        # Include mapped concepts via concept_relationship
        if item.include_mapped:
            mapped = (
                relationship_tbl.filter(
                    (relationship_tbl.concept_id_1 == cid)
                    & (relationship_tbl.relationship_id == "Maps to")
                )
                .select(relationship_tbl.concept_id_2)
                .rename(concept_id="concept_id_2")
            )
            parts.append(mapped)

    if not parts:
        # Should not happen since items is non-empty, but be safe
        return concept_tbl.limit(0).select(concept_tbl.concept_id)

    # UNION ALL then DISTINCT
    result = parts[0]
    for p in parts[1:]:
        result = result.union(p)
    return result.distinct()


def _empty_concept_id_table(con: ibis.BaseBackend) -> ir.Table:
    """Create an empty table with a single concept_id column."""
    import pyarrow as pa

    empty = pa.table({"concept_id": pa.array([], type=pa.int64())})
    temp_name = "__omopy_empty_concepts"
    con.con.register(temp_name, empty)
    return con.table(temp_name).select("concept_id")
