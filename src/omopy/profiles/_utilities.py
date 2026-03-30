"""Utility functions — add_cohort_name, add_concept_name, filters.

This is the Python equivalent of R's ``addCohortName()``,
``addConceptName()``, ``filterInObservation()``, ``filterCohortId()``
from PatientProfiles.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.profiles._columns import person_id_column
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm

__all__ = [
    "add_cdm_name",
    "add_cohort_name",
    "add_concept_name",
    "filter_cohort_id",
    "filter_in_observation",
]


def add_cohort_name(
    x: CohortTable,
) -> CdmTable:
    """Add a ``cohort_name`` column from the cohort settings.

    Parameters
    ----------
    x
        A CohortTable with settings.

    Returns
    -------
    CdmTable
        The table with ``cohort_name`` added.
    """
    if not isinstance(x, CohortTable):
        msg = "add_cohort_name requires a CohortTable"
        raise TypeError(msg)

    tbl = _get_ibis_table(x)
    settings = x.settings

    # Build CASE expression from settings
    ids = settings["cohort_definition_id"].to_list()
    names = settings["cohort_name"].to_list()

    cases = [
        (tbl["cohort_definition_id"].cast("int64") == ibis.literal(int(cid)), name)
        for cid, name in zip(ids, names)
    ]

    tbl = tbl.mutate(cohort_name=ibis.cases(*cases, else_=ibis.null().cast("string")))
    return x._with_data(tbl)


def add_concept_name(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    column: str | None = None,
    name_style: str = "{column}_name",
) -> CdmTable:
    """Add concept name(s) by looking up concept IDs in the concept table.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.
    column
        Column containing concept IDs. If None, auto-detects columns
        ending in ``_concept_id``.
    name_style
        Template for new column names with ``{column}`` placeholder.

    Returns
    -------
    CdmTable
        Input table with concept name column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)
    concept = _get_ibis_table(cdm["concept"])

    if column is not None:
        columns = [column]
    else:
        columns = [c for c in tbl.columns if c.endswith("_concept_id")]

    for col in columns:
        out_name = name_style.replace("{column}", col)

        # Create a concept lookup sub-table
        concept_lookup = concept.select(
            _cid=concept["concept_id"].cast("int64"),
            _cname=concept["concept_name"],
        )

        # Left join
        tbl = tbl.left_join(concept_lookup, tbl[col].cast("int64") == concept_lookup["_cid"])
        tbl = tbl.mutate(**{out_name: tbl["_cname"]})
        # Drop helper columns
        keep = [c for c in tbl.columns if c not in ("_cid", "_cname")]
        tbl = tbl.select(*keep)

    return x._with_data(tbl)


def add_cdm_name(
    x: CdmTable,
    cdm: CdmReference | None = None,
) -> CdmTable:
    """Add a ``cdm_name`` column with the CDM source name.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.

    Returns
    -------
    CdmTable
        Input table with ``cdm_name`` column added.
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)
    tbl = tbl.mutate(cdm_name=ibis.literal(cdm.cdm_name))
    return x._with_data(tbl)


def filter_in_observation(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
) -> CdmTable:
    """Filter to rows where the index date is within an observation period.

    INNER JOINs with ``observation_period`` and filters to rows where
    ``obs_start <= index_date <= obs_end``.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.
    index_date
        Column to check against observation periods.

    Returns
    -------
    CdmTable
        Filtered table (only rows within observation).
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)
    pid = person_id_column(tbl.columns)
    obs = _get_ibis_table(cdm["observation_period"])
    orig_cols = tbl.columns

    obs_sub = obs.select(
        _fio_pid=obs["person_id"],
        _fio_start=obs["observation_period_start_date"],
        _fio_end=obs["observation_period_end_date"],
    )

    result = (
        tbl.join(obs_sub, tbl[pid] == obs_sub["_fio_pid"])
        .filter(lambda t: (t["_fio_start"] <= t[index_date]) & (t[index_date] <= t["_fio_end"]))
        .select(*orig_cols)
    )

    return x._with_data(result)


def filter_cohort_id(
    x: CohortTable,
    cohort_id: int | list[int] | None = None,
) -> CohortTable:
    """Filter a CohortTable to specified cohort definition IDs.

    Parameters
    ----------
    x
        A CohortTable.
    cohort_id
        One or more cohort_definition_id values. None = no filter.

    Returns
    -------
    CohortTable
        Filtered cohort table.
    """
    if not isinstance(x, CohortTable):
        msg = "filter_cohort_id requires a CohortTable"
        raise TypeError(msg)

    if cohort_id is None:
        return x

    if isinstance(cohort_id, int):
        cohort_id = [cohort_id]

    tbl = _get_ibis_table(x)

    # Build filter
    id_lits = [ibis.literal(int(i)) for i in cohort_id]
    tbl = tbl.filter(tbl["cohort_definition_id"].cast("int64").isin(id_lits))

    return x._with_data(tbl)
