"""Concept intersection — add flag/count/date/days/field from concept sets.

These functions resolve concept sets (lists of concept IDs) to their
domain tables (condition_occurrence, drug_exposure, etc.), build a
unified overlap table, and intersect it with the input table using the
core intersection engine.

This is the Python equivalent of R's ``addConceptIntersectFlag()``,
``addConceptIntersectCount()``, etc. from PatientProfiles.

Algorithm:
1. Look up each concept ID in the ``concept`` vocabulary table to get its
   ``domain_id`` (e.g. "Condition", "Drug").
2. For each domain, select from the domain table with canonical columns
   (person_id, start_date, end_date, concept_id).
3. Assign a ``_concept_set_id`` integer to each concept set.
4. UNION ALL the per-domain tables into a single overlap table.
5. Pass to ``_add_intersect()`` with ``filter_variable="_concept_set_id"``.
"""

from __future__ import annotations

from typing import Literal

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.codelist import Codelist
from omopy.profiles._columns import _TABLE_COLUMNS, person_id_column
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm
from omopy.profiles._intersect import _add_intersect
from omopy.profiles._windows import Window, validate_windows

__all__ = [
    "add_concept_intersect_count",
    "add_concept_intersect_date",
    "add_concept_intersect_days",
    "add_concept_intersect_field",
    "add_concept_intersect_flag",
]

# ---------------------------------------------------------------------------
# Domain ID (from concept.domain_id) → OMOP table name mapping
# ---------------------------------------------------------------------------
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


def _build_concept_overlap_table(
    cdm: CdmReference,
    concept_set: dict[str, list[int]],
) -> tuple[ir.Table, list[int], list[str]]:
    """Build a unified overlap table from concept sets across all domains.

    For each concept set, looks up concept IDs in the ``concept`` vocabulary
    table to determine which domain tables to query. Then selects matching
    rows from each domain table, assigns a ``_concept_set_id``, and unions
    everything into a single overlap table with canonical columns.

    Parameters
    ----------
    cdm
        CDM reference with access to domain tables and ``concept``.
    concept_set
        Mapping of concept set names to lists of concept IDs.

    Returns
    -------
    tuple[ir.Table, list[int], list[str]]
        (overlap_table, filter_ids, id_names) where:
        - overlap_table has columns: ``person_id``, ``_ov_start``,
          ``_ov_end``, ``_concept_set_id``
        - filter_ids are integer IDs (1, 2, ...) for each concept set
        - id_names are the concept set names
    """
    concept_tbl = _get_ibis_table(cdm["concept"])

    # Assign numeric IDs to concept sets
    set_names = list(concept_set.keys())
    set_ids = list(range(1, len(set_names) + 1))
    all_concept_ids = {cid for ids in concept_set.values() for cid in ids}

    # Look up domain_id for all concept IDs at once
    concept_lookup = concept_tbl.filter(
        concept_tbl["concept_id"].cast("int64").isin(
            [ibis.literal(int(c)) for c in all_concept_ids]
        )
    ).select(
        concept_id=concept_tbl["concept_id"].cast("int64"),
        domain_id=concept_tbl["domain_id"].lower(),
    )

    # Materialise the lookup so we can figure out which domains to query
    concept_lookup_df = concept_lookup.execute()
    concept_to_domain: dict[int, str] = {}
    for _, row in concept_lookup_df.iterrows():
        concept_to_domain[int(row["concept_id"])] = row["domain_id"]

    # Group concept IDs by (concept_set_idx, domain) → list of concept_ids
    # domain → list of (concept_set_id, concept_id) pairs
    domain_concepts: dict[str, list[tuple[int, int]]] = {}
    for set_name, set_id in zip(set_names, set_ids):
        for cid in concept_set[set_name]:
            domain = concept_to_domain.get(cid, "")
            if domain in _DOMAIN_TO_TABLE:
                table_name = _DOMAIN_TO_TABLE[domain]
                if table_name in cdm:
                    domain_concepts.setdefault(domain, []).append((set_id, cid))

    # Build per-domain sub-queries and union them
    parts: list[ir.Table] = []
    for domain, set_cid_pairs in domain_concepts.items():
        table_name = _DOMAIN_TO_TABLE[domain]
        if table_name not in _TABLE_COLUMNS:
            continue
        col_info = _TABLE_COLUMNS[table_name]
        domain_tbl = _get_ibis_table(cdm[table_name])

        concept_col = col_info["concept_id"]
        start_col = col_info["start_date"]
        end_col = col_info["end_date"]

        # Collect all concept_ids needed from this domain
        domain_cids = list({cid for _, cid in set_cid_pairs})

        # Filter domain table to only relevant concept IDs
        filtered = domain_tbl.filter(
            domain_tbl[concept_col].cast("int64").isin(
                [ibis.literal(int(c)) for c in domain_cids]
            )
        )

        # Build the CASE for concept_set_id assignment
        # Multiple concept sets may include the same concept_id
        # We need one row per (concept_set, event), so we create
        # a sub-query per concept_set that has concepts in this domain
        sets_in_domain: dict[int, list[int]] = {}
        for set_id, cid in set_cid_pairs:
            sets_in_domain.setdefault(set_id, []).append(cid)

        for set_id, cids_for_set in sets_in_domain.items():
            part = filtered.filter(
                filtered[concept_col].cast("int64").isin(
                    [ibis.literal(int(c)) for c in cids_for_set]
                )
            ).select(
                person_id=filtered["person_id"],
                _ov_start=filtered[start_col],
                _ov_end=filtered[end_col].fill_null(filtered[start_col])
                if end_col != start_col
                else filtered[start_col],
                _concept_set_id=ibis.literal(set_id),
            )
            parts.append(part)

    if not parts:
        # No concepts found in any domain — return an empty table schema
        # Create a dummy empty table by filtering an existing table to false
        any_tbl = _get_ibis_table(cdm["person"])
        empty = any_tbl.filter(ibis.literal(False)).select(
            person_id=any_tbl["person_id"],
            _ov_start=ibis.null().cast("date"),
            _ov_end=ibis.null().cast("date"),
            _concept_set_id=ibis.literal(0),
        )
        return empty, set_ids, set_names

    # UNION ALL
    result = parts[0]
    for part in parts[1:]:
        result = result.union(part)

    return result, set_ids, set_names


# ---------------------------------------------------------------------------
# Public API — 5 concept intersect functions
# ---------------------------------------------------------------------------


def add_concept_intersect_flag(
    x: CdmTable,
    concept_set: Codelist | dict[str, list[int]],
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    in_observation: bool = True,
    name_style: str = "{concept_name}_{window_name}",
) -> CdmTable:
    """Add a binary flag (0/1) per concept set and time window.

    Parameters
    ----------
    x
        Input CDM table.
    concept_set
        Mapping of concept set names to lists of concept IDs.
    cdm
        CDM reference. If None, uses ``x.cdm``.
    index_date
        Column in ``x`` containing the reference date.
    censor_date
        Optional censoring column.
    window
        Time window(s) relative to index date.
    in_observation
        Restrict to events within observation period.
    name_style
        Column naming template. ``{concept_name}`` and ``{window_name}``
        are replaced.

    Returns
    -------
    CdmTable
        Input table with flag columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    overlap, set_ids, set_names = _build_concept_overlap_table(cdm, dict(concept_set))

    return _add_intersect(
        x, cdm,
        target_table=overlap,
        target_person_col="person_id",
        target_start_date="_ov_start",
        target_end_date="_ov_end",
        value="flag",
        filter_variable="_concept_set_id",
        filter_id=set_ids,
        id_name=set_names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        name_style=name_style,
    )


def add_concept_intersect_count(
    x: CdmTable,
    concept_set: Codelist | dict[str, list[int]],
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    in_observation: bool = True,
    name_style: str = "{concept_name}_{window_name}",
) -> CdmTable:
    """Add event count per concept set and time window.

    Parameters
    ----------
    x
        Input CDM table.
    concept_set
        Mapping of concept set names to lists of concept IDs.
    cdm
        CDM reference.
    index_date
        Reference date column in ``x``.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    in_observation
        Restrict to observation period.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with count columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    overlap, set_ids, set_names = _build_concept_overlap_table(cdm, dict(concept_set))

    return _add_intersect(
        x, cdm,
        target_table=overlap,
        target_person_col="person_id",
        target_start_date="_ov_start",
        target_end_date="_ov_end",
        value="count",
        filter_variable="_concept_set_id",
        filter_id=set_ids,
        id_name=set_names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        name_style=name_style,
    )


def add_concept_intersect_date(
    x: CdmTable,
    concept_set: Codelist | dict[str, list[int]],
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{concept_name}_{window_name}",
) -> CdmTable:
    """Add the date of the first/last event per concept set and window.

    Parameters
    ----------
    x
        Input CDM table.
    concept_set
        Mapping of concept set names to lists of concept IDs.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with date columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    overlap, set_ids, set_names = _build_concept_overlap_table(cdm, dict(concept_set))

    return _add_intersect(
        x, cdm,
        target_table=overlap,
        target_person_col="person_id",
        target_start_date="_ov_start",
        target_end_date="_ov_end",
        value="date",
        filter_variable="_concept_set_id",
        filter_id=set_ids,
        id_name=set_names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=name_style,
    )


def add_concept_intersect_days(
    x: CdmTable,
    concept_set: Codelist | dict[str, list[int]],
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{concept_name}_{window_name}",
) -> CdmTable:
    """Add days from index to first/last event per concept set and window.

    Parameters
    ----------
    x
        Input CDM table.
    concept_set
        Mapping of concept set names to lists of concept IDs.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with days columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    overlap, set_ids, set_names = _build_concept_overlap_table(cdm, dict(concept_set))

    return _add_intersect(
        x, cdm,
        target_table=overlap,
        target_person_col="person_id",
        target_start_date="_ov_start",
        target_end_date="_ov_end",
        value="days",
        filter_variable="_concept_set_id",
        filter_id=set_ids,
        id_name=set_names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=name_style,
    )


def add_concept_intersect_field(
    x: CdmTable,
    concept_set: Codelist | dict[str, list[int]],
    field: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{concept_name}_{field}_{window_name}",
) -> CdmTable:
    """Add a field value from the first/last event per concept set.

    .. note::

        Field extraction for concept intersects is limited since events
        come from different domain tables. The ``field`` must be a column
        present in the unified overlap table (currently only the
        canonical columns are available).

    Parameters
    ----------
    x
        Input CDM table.
    concept_set
        Mapping of concept set names to lists of concept IDs.
    field
        Column name to extract.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template with ``{field}``.

    Returns
    -------
    CdmTable
        Input table with field columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    overlap, set_ids, set_names = _build_concept_overlap_table(cdm, dict(concept_set))

    return _add_intersect(
        x, cdm,
        target_table=overlap,
        target_person_col="person_id",
        target_start_date="_ov_start",
        target_end_date="_ov_end",
        value=field,
        filter_variable="_concept_set_id",
        filter_id=set_ids,
        id_name=set_names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=name_style,
    )
