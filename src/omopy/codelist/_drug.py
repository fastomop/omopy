"""Drug vocabulary functions — ingredient and ATC code lookup.

Uses ``concept_relationship`` and ``concept_ancestor`` to traverse
drug hierarchies and find ingredient or ATC codes.

This is the Python equivalent of R's ``getDrugIngredientCodes()``
and ``getATCCodes()`` from CodelistGenerator.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "get_atc_codes",
    "get_drug_ingredient_codes",
]


def get_drug_ingredient_codes(
    cdm: CdmReference,
    ingredient: str | list[str] | int | list[int] | None = None,
    *,
    name: str | None = None,
) -> Codelist:
    """Get drug concepts linked to specified ingredients.

    If ``ingredient`` is a string, searches by keyword in concept_name
    where concept_class_id = 'Ingredient'. If an integer, uses concept_id
    directly.

    Parameters
    ----------
    cdm
        CDM reference.
    ingredient
        Ingredient name(s) or concept ID(s). If None, returns all
        standard ingredient concepts.
    name
        Name for the codelist entry.

    Returns
    -------
    Codelist
        Codelist of drug ingredient concept IDs.
    """
    concept = _get_ibis_table(cdm["concept"])

    if ingredient is None:
        # All ingredient concepts
        result = concept.filter(
            (concept["concept_class_id"] == ibis.literal("Ingredient"))
            & (concept["standard_concept"] == ibis.literal("S"))
            & (concept["domain_id"] == ibis.literal("Drug"))
        ).select(
            concept_id=concept["concept_id"].cast("int64"),
            concept_name=concept["concept_name"],
        )
        result_df = result.execute()
        codelist = Codelist()
        for _, row in result_df.iterrows():
            ing_name = str(row["concept_name"]).lower().replace(" ", "_")
            codelist[ing_name] = [int(row["concept_id"])]
        return codelist

    # Normalize input
    if isinstance(ingredient, (str, int)):
        ingredients = [ingredient]
    else:
        ingredients = list(ingredient)

    codelist = Codelist()
    for ing in ingredients:
        if isinstance(ing, int):
            # Direct concept ID
            ing_ids = concept.filter(
                concept["concept_id"].cast("int64") == ibis.literal(int(ing))
            ).select(
                concept_id=concept["concept_id"].cast("int64"),
                concept_name=concept["concept_name"],
            )
        else:
            # Keyword search for ingredient
            pattern = f"%{ing.lower()}%"
            ing_ids = concept.filter(
                concept["concept_name"].lower().like(pattern)
                & (concept["concept_class_id"] == ibis.literal("Ingredient"))
                & (concept["standard_concept"] == ibis.literal("S"))
                & (concept["domain_id"] == ibis.literal("Drug"))
            ).select(
                concept_id=concept["concept_id"].cast("int64"),
                concept_name=concept["concept_name"],
            )

        ing_df = ing_ids.execute()
        for _, row in ing_df.iterrows():
            entry_name = name or str(row["concept_name"]).lower().replace(" ", "_")
            cid = int(row["concept_id"])

            # Get descendants of this ingredient via concept_ancestor
            if "concept_ancestor" in cdm:
                ca = _get_ibis_table(cdm["concept_ancestor"])
                desc = ca.filter(
                    ca["ancestor_concept_id"].cast("int64") == ibis.literal(cid)
                ).select(
                    concept_id=ca["descendant_concept_id"].cast("int64")
                )
                # Filter to standard Drug concepts
                desc_with_info = desc.inner_join(
                    concept,
                    desc["concept_id"] == concept["concept_id"].cast("int64"),
                ).filter(
                    lambda t: (t["standard_concept"] == ibis.literal("S"))
                    & (t["domain_id"] == ibis.literal("Drug"))
                ).select("concept_id").distinct()

                desc_df = desc_with_info.execute()
                all_ids = sorted(desc_df["concept_id"].tolist())
            else:
                all_ids = [cid]

            codelist[entry_name] = all_ids

    return codelist


def get_atc_codes(
    cdm: CdmReference,
    atc_name: str | None = None,
    *,
    level: str | list[str] | None = None,
    name: str | None = None,
) -> Codelist:
    """Get ATC (Anatomical Therapeutic Chemical) codes.

    Finds ATC concepts in the vocabulary and optionally their
    linked RxNorm concepts via concept_relationship.

    Parameters
    ----------
    cdm
        CDM reference.
    atc_name
        Keyword to search ATC concept names. If None, returns all.
    level
        ATC concept class(es) to filter to (e.g. ``"ATC 1st"``,
        ``"ATC 2nd"``, ``"ATC 3rd"``, ``"ATC 4th"``, ``"ATC 5th"``).
    name
        Name for the codelist entry.

    Returns
    -------
    Codelist
        Codelist of ATC concept IDs.
    """
    concept = _get_ibis_table(cdm["concept"])

    # Base filter: ATC vocabulary
    result = concept.filter(
        concept["vocabulary_id"] == ibis.literal("ATC")
    )

    if atc_name is not None:
        pattern = f"%{atc_name.lower()}%"
        result = result.filter(result["concept_name"].lower().like(pattern))

    if level is not None:
        levels = [level] if isinstance(level, str) else list(level)
        result = result.filter(result["concept_class_id"].isin(levels))

    result = result.select(
        concept_id=result["concept_id"].cast("int64"),
        concept_name=result["concept_name"],
    ).distinct()

    result_df = result.execute()

    if name is not None:
        all_ids = sorted(result_df["concept_id"].tolist())
        return Codelist({name: all_ids})

    # One entry per ATC concept
    codelist = Codelist()
    for _, row in result_df.iterrows():
        entry_name = str(row["concept_name"]).lower().replace(" ", "_")
        codelist[entry_name] = [int(row["concept_id"])]

    return codelist
