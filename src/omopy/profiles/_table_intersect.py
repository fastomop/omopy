"""Table intersection — add flag/count/date/days/field from OMOP tables.

These functions intersect the input table with a named OMOP CDM table
(e.g. ``condition_occurrence``, ``drug_exposure``) and add columns
indicating the presence, count, date, time offset, or field value of
matching events within specified time windows.

This is the Python equivalent of R's ``addTableIntersectFlag()``,
``addTableIntersectCount()``, ``addTableIntersectDate()``,
``addTableIntersectDays()``, and ``addTableIntersectField()`` from
the PatientProfiles package.
"""

from __future__ import annotations

from typing import Literal

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.profiles._columns import end_date_column, start_date_column
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm
from omopy.profiles._intersect import _add_intersect
from omopy.profiles._windows import Window, validate_windows

__all__ = [
    "add_table_intersect_count",
    "add_table_intersect_date",
    "add_table_intersect_days",
    "add_table_intersect_field",
    "add_table_intersect_flag",
]


def add_table_intersect_flag(
    x: CdmTable,
    table_name: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    target_start_date: str | None = None,
    target_end_date: str | None = None,
    in_observation: bool = True,
    name_style: str = "{table_name}_{window_name}",
) -> CdmTable:
    """Add a binary flag (0/1) for events in an OMOP table.

    Parameters
    ----------
    x
        Input CDM table.
    table_name
        Name of the OMOP table to intersect with.
    cdm
        CDM reference. If None, uses ``x.cdm``.
    index_date
        Column in ``x`` containing the reference date.
    censor_date
        Optional column in ``x`` for censoring events.
    window
        Time window(s) relative to index date.
    target_start_date
        Start date column in target. Auto-detected if None.
    target_end_date
        End date column in target. Auto-detected if None.
    in_observation
        Restrict to events within observation period.
    name_style
        Column naming template. Use ``{table_name}`` and ``{window_name}``.

    Returns
    -------
    CdmTable
        Input table with flag column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    t_start = target_start_date or start_date_column(table_name)
    t_end = target_end_date or end_date_column(table_name)
    target = _get_ibis_table(cdm[table_name])

    # Replace {table_name} in name_style with the actual table name
    ns = name_style.replace("{table_name}", table_name)

    return _add_intersect(
        x,
        cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date=t_start,
        target_end_date=t_end,
        value="flag",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        name_style=ns,
    )


def add_table_intersect_count(
    x: CdmTable,
    table_name: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    target_start_date: str | None = None,
    target_end_date: str | None = None,
    in_observation: bool = True,
    name_style: str = "{table_name}_{window_name}",
) -> CdmTable:
    """Add event count from an OMOP table.

    Parameters
    ----------
    x
        Input CDM table.
    table_name
        Name of the OMOP table to intersect with.
    cdm
        CDM reference.
    index_date
        Reference date column in ``x``.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    target_start_date
        Start date in target. Auto-detected if None.
    target_end_date
        End date in target. Auto-detected if None.
    in_observation
        Restrict to observation period.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with count column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    t_start = target_start_date or start_date_column(table_name)
    t_end = target_end_date or end_date_column(table_name)
    target = _get_ibis_table(cdm[table_name])
    ns = name_style.replace("{table_name}", table_name)

    return _add_intersect(
        x,
        cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date=t_start,
        target_end_date=t_end,
        value="count",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        name_style=ns,
    )


def add_table_intersect_date(
    x: CdmTable,
    table_name: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    target_date: str | None = None,
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{table_name}_{window_name}",
) -> CdmTable:
    """Add the date of the first/last event from an OMOP table.

    Parameters
    ----------
    x
        Input CDM table.
    table_name
        Name of the OMOP table.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    target_date
        Date column in target. Auto-detected if None.
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with date column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    t_date = target_date or start_date_column(table_name)
    target = _get_ibis_table(cdm[table_name])
    ns = name_style.replace("{table_name}", table_name)

    return _add_intersect(
        x,
        cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date=t_date,
        target_end_date=None,  # Point-in-time for date/days
        value="date",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=ns,
    )


def add_table_intersect_days(
    x: CdmTable,
    table_name: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    target_date: str | None = None,
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{table_name}_{window_name}",
) -> CdmTable:
    """Add days from index to first/last event in an OMOP table.

    Parameters
    ----------
    x
        Input CDM table.
    table_name
        Name of the OMOP table.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    target_date
        Date column in target. Auto-detected if None.
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with days column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    t_date = target_date or start_date_column(table_name)
    target = _get_ibis_table(cdm[table_name])
    ns = name_style.replace("{table_name}", table_name)

    return _add_intersect(
        x,
        cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date=t_date,
        target_end_date=None,
        value="days",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=ns,
    )


def add_table_intersect_field(
    x: CdmTable,
    table_name: str,
    field: str,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window | list[Window] = (0, float("inf")),
    target_date: str | None = None,
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{table_name}_{field}_{window_name}",
) -> CdmTable:
    """Add a field value from the first/last event in an OMOP table.

    Parameters
    ----------
    x
        Input CDM table.
    table_name
        Name of the OMOP table.
    field
        Column name in the target table to extract.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window(s).
    target_date
        Date column in target. Auto-detected if None.
    in_observation
        Restrict to observation period.
    order
        ``"first"`` or ``"last"`` event.
    name_style
        Column naming template. Use ``{field}`` placeholder.

    Returns
    -------
    CdmTable
        Input table with field column(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)
    t_date = target_date or start_date_column(table_name)
    target = _get_ibis_table(cdm[table_name])
    ns = name_style.replace("{table_name}", table_name)

    return _add_intersect(
        x,
        cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date=t_date,
        target_end_date=None,
        value=field,
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=in_observation,
        order=order,
        name_style=ns,
    )
