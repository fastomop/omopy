"""Cohort intersection — add flag/count/date/days/field from cohort tables.

These functions intersect the input table with a cohort table and add
columns per cohort definition, within specified time windows.

This is the Python equivalent of R's ``addCohortIntersectFlag()``,
``addCohortIntersectCount()``, etc. from PatientProfiles.
"""

from __future__ import annotations

from typing import Literal

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm
from omopy.profiles._intersect import _add_intersect
from omopy.profiles._windows import Window, validate_windows

__all__ = [
    "add_cohort_intersect_count",
    "add_cohort_intersect_date",
    "add_cohort_intersect_days",
    "add_cohort_intersect_field",
    "add_cohort_intersect_flag",
]


def _resolve_cohort_info(
    target_cohort_table: str | CohortTable,
    cdm: CdmReference,
    target_cohort_id: list[int] | None,
) -> tuple[CohortTable, list[int], list[str]]:
    """Resolve cohort table, IDs, and names."""
    if isinstance(target_cohort_table, str):
        cohort = cdm[target_cohort_table]
        if not isinstance(cohort, CohortTable):
            msg = f"Table '{target_cohort_table}' is not a CohortTable"
            raise TypeError(msg)
    else:
        cohort = target_cohort_table

    # Get settings for ID -> name mapping
    settings = cohort.settings
    all_ids = settings["cohort_definition_id"].to_list()
    all_names = settings["cohort_name"].to_list()

    if target_cohort_id is not None:
        ids = [i for i in target_cohort_id if i in all_ids]
        names = [all_names[all_ids.index(i)] for i in ids]
    else:
        ids = all_ids
        names = all_names

    return cohort, ids, names


def add_cohort_intersect_flag(
    x: CdmTable,
    target_cohort_table: str | CohortTable,
    cdm: CdmReference | None = None,
    *,
    target_cohort_id: list[int] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    target_start_date: str = "cohort_start_date",
    target_end_date: str = "cohort_end_date",
    window: Window | list[Window] = (0, float("inf")),
    name_style: str = "{cohort_name}_{window_name}",
) -> CdmTable:
    """Add a binary flag (0/1) per cohort and time window.

    Parameters
    ----------
    x
        Input CDM table.
    target_cohort_table
        Name of a cohort table in the CDM, or a CohortTable directly.
    cdm
        CDM reference.
    target_cohort_id
        Subset of cohort IDs. None = all.
    index_date
        Reference date column in ``x``.
    censor_date
        Optional censoring column.
    target_start_date
        Start date column in cohort table.
    target_end_date
        End date column in cohort table.
    window
        Time window(s).
    name_style
        Column naming template with ``{cohort_name}`` and ``{window_name}``.

    Returns
    -------
    CdmTable
        Input table with flag columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    cohort, ids, names = _resolve_cohort_info(target_cohort_table, cdm, target_cohort_id)
    target = _get_ibis_table(cohort)

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="subject_id",
        target_start_date=target_start_date,
        target_end_date=target_end_date,
        value="flag",
        filter_variable="cohort_definition_id",
        filter_id=ids,
        id_name=names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,  # Cohort intersection doesn't use in_observation
        name_style=name_style,
    )


def add_cohort_intersect_count(
    x: CdmTable,
    target_cohort_table: str | CohortTable,
    cdm: CdmReference | None = None,
    *,
    target_cohort_id: list[int] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    target_start_date: str = "cohort_start_date",
    target_end_date: str = "cohort_end_date",
    window: Window | list[Window] = (0, float("inf")),
    name_style: str = "{cohort_name}_{window_name}",
) -> CdmTable:
    """Add event count per cohort and time window.

    Parameters
    ----------
    x
        Input CDM table.
    target_cohort_table
        Cohort table name or CohortTable.
    cdm
        CDM reference.
    target_cohort_id
        Subset of cohort IDs.
    index_date, censor_date
        Reference and censoring dates.
    target_start_date, target_end_date
        Date columns in cohort table.
    window
        Time window(s).
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with count columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    cohort, ids, names = _resolve_cohort_info(target_cohort_table, cdm, target_cohort_id)
    target = _get_ibis_table(cohort)

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="subject_id",
        target_start_date=target_start_date,
        target_end_date=target_end_date,
        value="count",
        filter_variable="cohort_definition_id",
        filter_id=ids,
        id_name=names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        name_style=name_style,
    )


def add_cohort_intersect_date(
    x: CdmTable,
    target_cohort_table: str | CohortTable,
    cdm: CdmReference | None = None,
    *,
    target_cohort_id: list[int] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    target_date: str = "cohort_start_date",
    order: Literal["first", "last"] = "first",
    window: Window | list[Window] = (0, float("inf")),
    name_style: str = "{cohort_name}_{window_name}",
) -> CdmTable:
    """Add the date of the first/last cohort event per cohort and window.

    Parameters
    ----------
    x
        Input CDM table.
    target_cohort_table
        Cohort table name or CohortTable.
    cdm
        CDM reference.
    target_cohort_id
        Subset of cohort IDs.
    index_date, censor_date
        Reference and censoring dates.
    target_date
        Date column in cohort table (point-in-time).
    order
        ``"first"`` or ``"last"`` event.
    window
        Time window(s).
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with date columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    cohort, ids, names = _resolve_cohort_info(target_cohort_table, cdm, target_cohort_id)
    target = _get_ibis_table(cohort)

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="subject_id",
        target_start_date=target_date,
        target_end_date=None,
        value="date",
        filter_variable="cohort_definition_id",
        filter_id=ids,
        id_name=names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order=order,
        name_style=name_style,
    )


def add_cohort_intersect_days(
    x: CdmTable,
    target_cohort_table: str | CohortTable,
    cdm: CdmReference | None = None,
    *,
    target_cohort_id: list[int] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    target_date: str = "cohort_start_date",
    order: Literal["first", "last"] = "first",
    window: Window | list[Window] = (0, float("inf")),
    name_style: str = "{cohort_name}_{window_name}",
) -> CdmTable:
    """Add days from index to first/last cohort event per cohort and window.

    Parameters
    ----------
    x
        Input CDM table.
    target_cohort_table
        Cohort table name or CohortTable.
    cdm
        CDM reference.
    target_cohort_id
        Subset of cohort IDs.
    index_date, censor_date
        Reference and censoring dates.
    target_date
        Date column in cohort table.
    order
        ``"first"`` or ``"last"`` event.
    window
        Time window(s).
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with days columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    cohort, ids, names = _resolve_cohort_info(target_cohort_table, cdm, target_cohort_id)
    target = _get_ibis_table(cohort)

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="subject_id",
        target_start_date=target_date,
        target_end_date=None,
        value="days",
        filter_variable="cohort_definition_id",
        filter_id=ids,
        id_name=names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order=order,
        name_style=name_style,
    )


def add_cohort_intersect_field(
    x: CdmTable,
    target_cohort_table: str | CohortTable,
    field: str,
    cdm: CdmReference | None = None,
    *,
    target_cohort_id: list[int] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    target_date: str = "cohort_start_date",
    order: Literal["first", "last"] = "first",
    window: Window | list[Window] = (0, float("inf")),
    name_style: str = "{cohort_name}_{field}_{window_name}",
) -> CdmTable:
    """Add a field value from the first/last cohort event.

    Parameters
    ----------
    x
        Input CDM table.
    target_cohort_table
        Cohort table name or CohortTable.
    field
        Column name in cohort table to extract.
    cdm
        CDM reference.
    target_cohort_id
        Subset of cohort IDs.
    index_date, censor_date
        Reference and censoring dates.
    target_date
        Date column in cohort table.
    order
        ``"first"`` or ``"last"`` event.
    window
        Time window(s).
    name_style
        Column naming template with ``{field}``.

    Returns
    -------
    CdmTable
        Input table with field columns added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    cohort, ids, names = _resolve_cohort_info(target_cohort_table, cdm, target_cohort_id)
    target = _get_ibis_table(cohort)

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="subject_id",
        target_start_date=target_date,
        target_end_date=None,
        value=field,
        filter_variable="cohort_definition_id",
        filter_id=ids,
        id_name=names,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order=order,
        name_style=name_style,
    )
