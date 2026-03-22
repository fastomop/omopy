"""Death functions — add death date, days to death, and death flag.

These functions delegate to the intersection engine targeting the
``death`` table in the CDM.

This is the Python equivalent of R's ``addDeathDate()``,
``addDeathDays()``, and ``addDeathFlag()`` from PatientProfiles.
"""

from __future__ import annotations

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm
from omopy.profiles._intersect import _add_intersect
from omopy.profiles._windows import Window, validate_windows

__all__ = [
    "add_death_date",
    "add_death_days",
    "add_death_flag",
]


def add_death_date(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window = (0, float("inf")),
    death_date_name: str = "date_of_death",
) -> CdmTable:
    """Add date of death column.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window relative to index date.
    death_date_name
        Output column name.

    Returns
    -------
    CdmTable
        Input table with death date column added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)

    if "death" not in cdm:
        # No death table — add NULL column
        import ibis
        tbl = _get_ibis_table(x)
        tbl = tbl.mutate(**{death_date_name: ibis.null().cast("date")})
        return x._with_data(tbl)

    target = _get_ibis_table(cdm["death"])

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date="death_date",
        target_end_date=None,
        value="date",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order="first",
        name_style=death_date_name,
    )


def add_death_days(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window = (0, float("inf")),
    death_days_name: str = "days_to_death",
) -> CdmTable:
    """Add days-to-death column.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window.
    death_days_name
        Output column name.

    Returns
    -------
    CdmTable
        Input table with days-to-death column added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)

    if "death" not in cdm:
        import ibis
        tbl = _get_ibis_table(x)
        tbl = tbl.mutate(**{death_days_name: ibis.null().cast("int64")})
        return x._with_data(tbl)

    target = _get_ibis_table(cdm["death"])

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date="death_date",
        target_end_date=None,
        value="days",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order="first",
        name_style=death_days_name,
    )


def add_death_flag(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    window: Window = (0, float("inf")),
    death_flag_name: str = "death",
) -> CdmTable:
    """Add death flag column (0/1).

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference.
    index_date
        Reference date column.
    censor_date
        Optional censoring column.
    window
        Time window.
    death_flag_name
        Output column name.

    Returns
    -------
    CdmTable
        Input table with death flag column added.
    """
    cdm = _resolve_cdm(x, cdm)
    windows = validate_windows(window)

    if "death" not in cdm:
        import ibis
        tbl = _get_ibis_table(x)
        tbl = tbl.mutate(**{death_flag_name: ibis.literal(0)})
        return x._with_data(tbl)

    target = _get_ibis_table(cdm["death"])

    return _add_intersect(
        x, cdm,
        target_table=target,
        target_person_col="person_id",
        target_start_date="death_date",
        target_end_date=None,
        value="flag",
        windows=windows,
        index_date=index_date,
        censor_date=censor_date,
        in_observation=False,
        order="first",
        name_style=death_flag_name,
    )
