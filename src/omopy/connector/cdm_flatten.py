"""Flatten a CDM into a single longitudinal observation table.

Provides ``cdm_flatten()`` to UNION ALL clinical domain tables into a
single table with normalised columns.  Equivalent to R's
``cdmFlatten()``.
"""

from __future__ import annotations

from typing import Literal

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference

__all__ = ["cdm_flatten"]

# Domain → (concept_id_col, start_date_col, end_date_col, type_concept_id_col)
_DOMAIN_MAP: dict[str, tuple[str, str, str, str]] = {
    "condition_occurrence": (
        "condition_concept_id",
        "condition_start_date",
        "condition_end_date",
        "condition_type_concept_id",
    ),
    "drug_exposure": (
        "drug_concept_id",
        "drug_exposure_start_date",
        "drug_exposure_end_date",
        "drug_type_concept_id",
    ),
    "procedure_occurrence": (
        "procedure_concept_id",
        "procedure_date",
        "procedure_date",
        "procedure_type_concept_id",
    ),
    "measurement": (
        "measurement_concept_id",
        "measurement_date",
        "measurement_date",
        "measurement_type_concept_id",
    ),
    "visit_occurrence": (
        "visit_concept_id",
        "visit_start_date",
        "visit_end_date",
        "visit_type_concept_id",
    ),
    "death": (
        "cause_concept_id",
        "death_date",
        "death_date",
        "death_type_concept_id",
    ),
    "observation": (
        "observation_concept_id",
        "observation_date",
        "observation_date",
        "observation_type_concept_id",
    ),
}

_VALID_DOMAINS = tuple(_DOMAIN_MAP.keys())

DomainName = Literal[
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "measurement",
    "visit_occurrence",
    "death",
    "observation",
]


def cdm_flatten(
    cdm: CdmReference,
    domains: list[str] | None = None,
    *,
    include_concept_name: bool = True,
) -> ir.Table | pl.DataFrame:
    """Flatten a CDM into a single observation table.

    Each included domain table is projected to a common schema
    (``person_id``, ``observation_concept_id``, ``start_date``,
    ``end_date``, ``type_concept_id``, ``domain``) and then UNION-ALLed
    together.  Optionally joins concept names from the ``concept`` table.

    Parameters
    ----------
    cdm
        A CdmReference.
    domains
        Domains to include.  Defaults to
        ``["condition_occurrence", "drug_exposure", "procedure_occurrence"]``.
        Valid values: ``condition_occurrence``, ``drug_exposure``,
        ``procedure_occurrence``, ``measurement``, ``visit_occurrence``,
        ``death``, ``observation``.
    include_concept_name
        If ``True`` (default), join concept names from the ``concept``
        table, adding ``observation_concept_name`` and
        ``type_concept_name`` columns.

    Returns
    -------
    ir.Table | pl.DataFrame
        A lazy Ibis table (if DB-backed) or Polars DataFrame with the
        flattened observations.  Distinct rows only.

    Raises
    ------
    ValueError
        If an invalid domain is specified.
    KeyError
        If a requested domain table is not in the CDM.
    """
    if domains is None:
        domains = ["condition_occurrence", "drug_exposure", "procedure_occurrence"]

    # Validate
    for d in domains:
        if d not in _DOMAIN_MAP:
            msg = f"Invalid domain '{d}'. Valid domains: {', '.join(_VALID_DOMAINS)}"
            raise ValueError(msg)
        if d not in cdm:
            msg = f"Domain table '{d}' not found in CDM"
            raise KeyError(msg)

    # Detect if we're working with Ibis or Polars
    sample_data = cdm[domains[0]].data
    is_ibis = not isinstance(sample_data, (pl.DataFrame, pl.LazyFrame))

    if is_ibis:
        result = _flatten_ibis(cdm, domains, include_concept_name)
    else:
        result = _flatten_polars(cdm, domains, include_concept_name)

    return result


# ---------------------------------------------------------------------------
# Ibis implementation
# ---------------------------------------------------------------------------


def _flatten_ibis(
    cdm: CdmReference,
    domains: list[str],
    include_concept_name: bool,
) -> ir.Table:
    """Flatten using Ibis (database-backed)."""
    parts: list[ir.Table] = []

    for domain in domains:
        concept_col, start_col, end_col, type_col = _DOMAIN_MAP[domain]
        data = cdm[domain].data

        proj = data.select(
            person_id=data["person_id"],
            observation_concept_id=data[concept_col],
            start_date=data[start_col],
            end_date=data[end_col],
            type_concept_id=data[type_col],
        ).mutate(domain=ibis.literal(domain))

        parts.append(proj.distinct())

    # Union all parts
    result = parts[0]
    for p in parts[1:]:
        result = result.union(p)

    if include_concept_name and "concept" in cdm:
        concept_data = cdm["concept"].data

        # Join observation_concept_name
        obs_concept = concept_data.select(
            _obs_concept_id=concept_data["concept_id"],
            observation_concept_name=concept_data["concept_name"],
        )
        result = result.left_join(
            obs_concept,
            result["observation_concept_id"] == obs_concept["_obs_concept_id"],
        ).drop("_obs_concept_id")

        # Join type_concept_name
        type_concept = concept_data.select(
            _type_concept_id=concept_data["concept_id"],
            type_concept_name=concept_data["concept_name"],
        )
        result = result.left_join(
            type_concept,
            result["type_concept_id"] == type_concept["_type_concept_id"],
        ).drop("_type_concept_id")

        result = result.distinct()

    return result


# ---------------------------------------------------------------------------
# Polars implementation
# ---------------------------------------------------------------------------


def _flatten_polars(
    cdm: CdmReference,
    domains: list[str],
    include_concept_name: bool,
) -> pl.DataFrame:
    """Flatten using Polars (local data)."""
    parts: list[pl.DataFrame] = []

    for domain in domains:
        concept_col, start_col, end_col, type_col = _DOMAIN_MAP[domain]
        data = cdm[domain].data
        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        proj = data.select(
            pl.col("person_id"),
            pl.col(concept_col).alias("observation_concept_id"),
            pl.col(start_col).alias("start_date"),
            pl.col(end_col).alias("end_date"),
            pl.col(type_col).alias("type_concept_id"),
        ).with_columns(pl.lit(domain).alias("domain"))

        parts.append(proj.unique())

    result = pl.concat(parts)

    if include_concept_name and "concept" in cdm:
        concept_data = cdm["concept"].data
        if isinstance(concept_data, pl.LazyFrame):
            concept_data = concept_data.collect()

        concept_lookup = concept_data.select(
            pl.col("concept_id"),
            pl.col("concept_name"),
        )

        # Join observation_concept_name
        result = result.join(
            concept_lookup.rename(
                {
                    "concept_id": "observation_concept_id",
                    "concept_name": "observation_concept_name",
                }
            ),
            on="observation_concept_id",
            how="left",
        )

        # Join type_concept_name
        result = result.join(
            concept_lookup.rename(
                {"concept_id": "type_concept_id", "concept_name": "type_concept_name"}
            ),
            on="type_concept_id",
            how="left",
        )

        result = result.unique()

    return result
