"""Demographics engine — add age, sex, observation period, etc.

Provides functions to enrich an Ibis-backed CDM table with demographic
information from the ``person`` and ``observation_period`` tables.

All functions accept and return Ibis Table expressions (lazy). They work
on any table that has a ``person_id`` or ``subject_id`` column and an
index-date column.

This is the Python equivalent of R's ``addDemographics()``,
``addAge()``, ``addSex()``, ``addPriorObservation()``,
``addFutureObservation()``, ``addDateOfBirth()``, and
``addInObservation()`` from the PatientProfiles package.
"""

from __future__ import annotations

import math
from typing import Literal

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.profiles._columns import person_id_column
from omopy.profiles._windows import Window, validate_windows, window_name

__all__ = [
    "add_age",
    "add_date_of_birth",
    "add_demographics",
    "add_future_observation",
    "add_in_observation",
    "add_prior_observation",
    "add_sex",
]


# ---------------------------------------------------------------------------
# Unified demographics
# ---------------------------------------------------------------------------


def add_demographics(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    age: bool = True,
    age_name: str = "age",
    age_missing_month: int = 1,
    age_missing_day: int = 1,
    age_impose_month: bool = False,
    age_impose_day: bool = False,
    age_unit: Literal["years", "months", "days"] = "years",
    age_group: dict[str, tuple[float, float]] | list[tuple[float, float]] | None = None,
    missing_age_group_value: str = "None",
    sex: bool = True,
    sex_name: str = "sex",
    missing_sex_value: str = "None",
    prior_observation: bool = True,
    prior_observation_name: str = "prior_observation",
    prior_observation_type: Literal["days", "date"] = "days",
    future_observation: bool = True,
    future_observation_name: str = "future_observation",
    future_observation_type: Literal["days", "date"] = "days",
    date_of_birth: bool = False,
    date_of_birth_name: str = "date_of_birth",
) -> CdmTable:
    """Add demographic columns to a CDM table.

    Joins the ``person`` and ``observation_period`` tables to compute
    age, sex, prior/future observation, and date of birth for each row.

    Parameters
    ----------
    x
        Input CDM table (must have a person identifier and ``index_date``).
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    index_date
        Column name of the date to compute demographics at.
    age, sex, prior_observation, future_observation, date_of_birth
        Whether to add each column.
    age_name, sex_name, prior_observation_name,
    future_observation_name, date_of_birth_name
        Column names for the output.
    age_missing_month, age_missing_day
        Default month/day when birth month/day is missing.
    age_impose_month, age_impose_day
        Force the missing values even when actual values exist.
    age_unit
        Unit for age: ``"years"`` (default), ``"months"``, or ``"days"``.
    age_group
        Optional age-group binning. Dict mapping label to (lower, upper)
        range, or a list of (lower, upper) tuples (auto-labelled).
    missing_age_group_value
        Value for rows where age is missing.
    missing_sex_value
        Value for rows where sex is unknown.
    prior_observation_type, future_observation_type
        ``"days"`` for integer days, ``"date"`` for the actual date.

    Returns
    -------
    CdmTable
        The input table with new demographic columns.
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)
    pid = person_id_column(tbl.columns)

    # -- Observation period join (prior/future) --
    if prior_observation or future_observation:
        tbl = _add_observation_period(
            tbl,
            cdm,
            pid=pid,
            index_date=index_date,
            prior_observation=prior_observation,
            prior_observation_name=prior_observation_name,
            prior_observation_type=prior_observation_type,
            future_observation=future_observation,
            future_observation_name=future_observation_name,
            future_observation_type=future_observation_type,
        )

    # -- Person join (age, sex, date of birth) --
    need_person = age or (age_group is not None) or sex or date_of_birth
    if need_person:
        tbl = _add_person_info(
            tbl,
            cdm,
            pid=pid,
            index_date=index_date,
            age=age,
            age_name=age_name,
            age_missing_month=age_missing_month,
            age_missing_day=age_missing_day,
            age_impose_month=age_impose_month,
            age_impose_day=age_impose_day,
            age_unit=age_unit,
            age_group=age_group,
            missing_age_group_value=missing_age_group_value,
            sex=sex,
            sex_name=sex_name,
            missing_sex_value=missing_sex_value,
            date_of_birth=date_of_birth,
            date_of_birth_name=date_of_birth_name,
        )

    return x._with_data(tbl)


# ---------------------------------------------------------------------------
# Individual add_* functions
# ---------------------------------------------------------------------------


def add_age(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    age_name: str = "age",
    age_missing_month: int = 1,
    age_missing_day: int = 1,
    age_impose_month: bool = False,
    age_impose_day: bool = False,
    age_unit: Literal["years", "months", "days"] = "years",
    age_group: dict[str, tuple[float, float]] | list[tuple[float, float]] | None = None,
    missing_age_group_value: str = "None",
) -> CdmTable:
    """Add an ``age`` column computed at the index date.

    Uses the R PatientProfiles integer-arithmetic trick for year/month
    age computation.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    index_date
        Column to compute age at.
    age_name
        Output column name.
    age_missing_month, age_missing_day
        Defaults for missing birth month/day.
    age_impose_month, age_impose_day
        Force defaults even when actual values exist.
    age_unit
        ``"years"`` (default), ``"months"``, or ``"days"``.
    age_group
        Optional age-group binning (dict or list of ranges).
    missing_age_group_value
        Value for missing age groups.

    Returns
    -------
    CdmTable
        Input table with the age column added.
    """
    return add_demographics(
        x,
        cdm,
        index_date=index_date,
        age=True,
        age_name=age_name,
        age_missing_month=age_missing_month,
        age_missing_day=age_missing_day,
        age_impose_month=age_impose_month,
        age_impose_day=age_impose_day,
        age_unit=age_unit,
        age_group=age_group,
        missing_age_group_value=missing_age_group_value,
        sex=False,
        prior_observation=False,
        future_observation=False,
        date_of_birth=False,
    )


def add_sex(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    sex_name: str = "sex",
    missing_sex_value: str = "None",
) -> CdmTable:
    """Add a ``sex`` column (Male / Female / missing).

    Maps ``gender_concept_id``: 8507 → Male, 8532 → Female,
    anything else → ``missing_sex_value``.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    sex_name
        Output column name.
    missing_sex_value
        Value when sex is unknown.

    Returns
    -------
    CdmTable
        Input table with the sex column added.
    """
    return add_demographics(
        x,
        cdm,
        sex=True,
        sex_name=sex_name,
        missing_sex_value=missing_sex_value,
        age=False,
        prior_observation=False,
        future_observation=False,
        date_of_birth=False,
    )


def add_prior_observation(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    prior_observation_name: str = "prior_observation",
    prior_observation_type: Literal["days", "date"] = "days",
) -> CdmTable:
    """Add a ``prior_observation`` column.

    Computes the number of days (or actual date) from the start of the
    observation period containing the index date.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    index_date
        Column to measure from.
    prior_observation_name
        Output column name.
    prior_observation_type
        ``"days"`` for integer days, ``"date"`` for the observation start date.

    Returns
    -------
    CdmTable
        Input table with prior observation added.
    """
    return add_demographics(
        x,
        cdm,
        index_date=index_date,
        prior_observation=True,
        prior_observation_name=prior_observation_name,
        prior_observation_type=prior_observation_type,
        age=False,
        sex=False,
        future_observation=False,
        date_of_birth=False,
    )


def add_future_observation(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    future_observation_name: str = "future_observation",
    future_observation_type: Literal["days", "date"] = "days",
) -> CdmTable:
    """Add a ``future_observation`` column.

    Computes the number of days (or actual date) from the index date to
    the end of the observation period containing the index date.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    index_date
        Column to measure from.
    future_observation_name
        Output column name.
    future_observation_type
        ``"days"`` for integer days, ``"date"`` for the observation end date.

    Returns
    -------
    CdmTable
        Input table with future observation added.
    """
    return add_demographics(
        x,
        cdm,
        index_date=index_date,
        future_observation=True,
        future_observation_name=future_observation_name,
        future_observation_type=future_observation_type,
        age=False,
        sex=False,
        prior_observation=False,
        date_of_birth=False,
    )


def add_date_of_birth(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    date_of_birth_name: str = "date_of_birth",
    missing_month: int = 1,
    missing_day: int = 1,
    impose_month: bool = False,
    impose_day: bool = False,
) -> CdmTable:
    """Add a ``date_of_birth`` column constructed from the person table.

    Combines ``year_of_birth``, ``month_of_birth``, ``day_of_birth``
    from the ``person`` table into a single date column.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    date_of_birth_name
        Output column name.
    missing_month, missing_day
        Defaults when birth month/day is missing.
    impose_month, impose_day
        Force defaults even when actual values exist.

    Returns
    -------
    CdmTable
        Input table with date of birth added.
    """
    return add_demographics(
        x,
        cdm,
        date_of_birth=True,
        date_of_birth_name=date_of_birth_name,
        age_missing_month=missing_month,
        age_missing_day=missing_day,
        age_impose_month=impose_month,
        age_impose_day=impose_day,
        age=False,
        sex=False,
        prior_observation=False,
        future_observation=False,
    )


def add_in_observation(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    index_date: str = "cohort_start_date",
    window: Window = (0, 0),
    complete_interval: bool = False,
    name_style: str = "in_observation",
) -> CdmTable:
    """Add an ``in_observation`` flag (1/0) for each time window.

    Checks whether the index date (± window) falls within a person's
    observation period.

    Parameters
    ----------
    x
        Input CDM table.
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    index_date
        Column to check.
    window
        Time window relative to index date.
    complete_interval
        If ``True``, requires the observation period to completely cover
        the window. If ``False``, any overlap suffices.
    name_style
        Output column name (or template with ``{window_name}``).

    Returns
    -------
    CdmTable
        Input table with the in-observation flag(s) added.
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)
    pid = person_id_column(tbl.columns)
    windows = validate_windows(window)

    obs_period = _get_ibis_table(cdm["observation_period"])

    # Get distinct (person_id, index_date)
    orig_cols = tbl.columns

    # Build observation period join
    # Rename obs columns to avoid conflicts
    obs = obs_period.select(
        obs_pid=obs_period["person_id"],
        obs_start=obs_period["observation_period_start_date"],
        obs_end=obs_period["observation_period_end_date"],
    )

    # Join on person_id, filter to enclosing period
    joined = tbl.join(obs, tbl[pid] == obs["obs_pid"]).filter(
        lambda t: (t["obs_start"] <= t[index_date]) & (t[index_date] <= t["obs_end"])
    )

    # Compute day offsets
    joined = joined.mutate(
        _obs_start_diff=(joined["obs_start"] - joined[index_date]).cast("int64"),
        _obs_end_diff=(joined["obs_end"] - joined[index_date]).cast("int64"),
    )

    # Compute flag for each window
    new_cols = {}
    for w in windows:
        wn = window_name(w)
        col_name = (
            name_style.replace("{window_name}", wn)
            if "{window_name}" in name_style
            else name_style
        )

        lo, hi = w

        if lo == 0 and hi == 0:
            # Always in observation at index date (we already filtered)
            flag_expr = ibis.literal(1)
        elif complete_interval:
            if math.isinf(lo) or math.isinf(hi):
                # Can't completely cover an infinite window
                flag_expr = ibis.literal(0)
            else:
                # Obs period must cover [indexDate + lo, indexDate + hi]
                flag_expr = ibis.cases(
                    (
                        (joined["_obs_start_diff"] <= ibis.literal(int(lo)))
                        & (joined["_obs_end_diff"] >= ibis.literal(int(hi))),
                        1,
                    ),
                    else_=0,
                )
        else:
            # Any overlap: observation period overlaps with [indexDate+lo, indexDate+hi]
            lo_expr = ibis.literal(int(lo)) if not math.isinf(lo) else None
            hi_expr = ibis.literal(int(hi)) if not math.isinf(hi) else None

            if lo_expr is None and hi_expr is None:
                flag_expr = ibis.literal(1)
            elif lo_expr is None:
                flag_expr = ibis.cases(
                    (joined["_obs_start_diff"] <= hi_expr, 1),
                    else_=0,
                )
            elif hi_expr is None:
                flag_expr = ibis.cases(
                    (joined["_obs_end_diff"] >= lo_expr, 1),
                    else_=0,
                )
            else:
                flag_expr = ibis.cases(
                    (
                        (joined["_obs_end_diff"] >= lo_expr)
                        & (joined["_obs_start_diff"] <= hi_expr),
                        1,
                    ),
                    else_=0,
                )

        new_cols[col_name] = flag_expr

    joined = joined.mutate(**new_cols)

    # Select only original columns + new flag columns
    result = joined.select(*orig_cols, *new_cols.keys())

    # Left join back to original to handle persons not in observation
    # Use a simpler approach: just select distinct and coalesce
    # Actually, we need to handle duplicates (multiple obs periods)
    # Take max flag per original row
    group_keys = orig_cols
    agg_exprs = {col: result[col].max().name(col) for col in new_cols}
    result = result.group_by(group_keys).agg(**agg_exprs)

    # Now left join from original to get 0 for non-matching rows
    result.select(
        *[result[c] for c in orig_cols],
        *[result[c] for c in new_cols],
    )

    # Instead of a complex left join, use a different approach:
    # We already have the correct rows. Just fill missing with 0
    # by left-joining the original table.
    # But since we started from `tbl`, `result` already has all original rows
    # that had a matching observation period. We need to add back rows with no
    # observation period.

    # Simpler approach: start over with a left join pattern
    tbl_result = tbl
    for col in new_cols:
        tbl_result = tbl_result.mutate(**{col: ibis.literal(0)})

    # Drop the placeholder columns and join with actual values
    # Actually let's use the cleaner approach: left join tbl with computed result
    list(new_cols.keys())

    # Make result unique on the join keys by grouping
    # Build a minimal result table with just the keys and flags
    all_tbl_cols = tbl.columns

    # Re-do: compute flags directly with a left join approach
    tbl_with_obs = tbl.left_join(obs, tbl[pid] == obs["obs_pid"]).filter(
        lambda t: (
            t["obs_start"].isnull()
            | ((t["obs_start"] <= t[index_date]) & (t[index_date] <= t["obs_end"]))
        )
    )

    # Re-compute flags on the left-joined table (obs columns may be null)
    final_cols = {}
    for w in windows:
        wn = window_name(w)
        col_name = (
            name_style.replace("{window_name}", wn)
            if "{window_name}" in name_style
            else name_style
        )

        lo, hi = w

        if lo == 0 and hi == 0:
            # In observation at index date: obs_start is not null means we matched
            flag_expr = ibis.cases(
                (tbl_with_obs["obs_start"].notnull(), 1),
                else_=0,
            )
        elif complete_interval:
            if math.isinf(lo) or math.isinf(hi):
                flag_expr = ibis.literal(0)
            else:
                obs_start_diff = (
                    tbl_with_obs["obs_start"] - tbl_with_obs[index_date]
                ).cast("int64")
                obs_end_diff = (
                    tbl_with_obs["obs_end"] - tbl_with_obs[index_date]
                ).cast("int64")
                flag_expr = ibis.cases(
                    (
                        tbl_with_obs["obs_start"].notnull()
                        & (obs_start_diff <= ibis.literal(int(lo)))
                        & (obs_end_diff >= ibis.literal(int(hi))),
                        1,
                    ),
                    else_=0,
                )
        else:
            obs_start_diff = (
                tbl_with_obs["obs_start"] - tbl_with_obs[index_date]
            ).cast("int64")
            obs_end_diff = (tbl_with_obs["obs_end"] - tbl_with_obs[index_date]).cast(
                "int64"
            )

            lo_lit = ibis.literal(int(lo)) if not math.isinf(lo) else None
            hi_lit = ibis.literal(int(hi)) if not math.isinf(hi) else None

            if lo_lit is None and hi_lit is None:
                flag_expr = ibis.cases(
                    (tbl_with_obs["obs_start"].notnull(), 1),
                    else_=0,
                )
            elif lo_lit is None:
                flag_expr = ibis.cases(
                    (
                        tbl_with_obs["obs_start"].notnull()
                        & (obs_start_diff <= hi_lit),
                        1,
                    ),
                    else_=0,
                )
            elif hi_lit is None:
                flag_expr = ibis.cases(
                    (tbl_with_obs["obs_start"].notnull() & (obs_end_diff >= lo_lit), 1),
                    else_=0,
                )
            else:
                flag_expr = ibis.cases(
                    (
                        tbl_with_obs["obs_start"].notnull()
                        & (obs_end_diff >= lo_lit)
                        & (obs_start_diff <= hi_lit),
                        1,
                    ),
                    else_=0,
                )

        final_cols[col_name] = flag_expr

    tbl_with_obs = tbl_with_obs.mutate(**final_cols)

    # Group by original columns, take max flag (handles multiple obs periods)
    agg_dict = {col: tbl_with_obs[col].max().name(col) for col in final_cols}
    result_final = tbl_with_obs.group_by(all_tbl_cols).agg(**agg_dict)

    return x._with_data(result_final)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_cdm(x: CdmTable, cdm: CdmReference | None) -> CdmReference:
    """Get the CDM reference from arguments or the table's back-pointer."""
    if cdm is not None:
        return cdm
    if x.cdm is not None:
        return x.cdm
    msg = (
        "No CDM reference available. Either pass 'cdm=' explicitly or "
        "ensure the table has a CDM back-reference (x.cdm)."
    )
    raise ValueError(msg)


def _get_ibis_table(x: CdmTable | object) -> ir.Table:
    """Extract the Ibis table expression from a CdmTable or return as-is.

    Handles Ibis-backed tables directly. For Polars DataFrames/LazyFrames,
    converts via ``ibis.memtable()`` to create an in-memory Ibis table.
    """
    if isinstance(x, CdmTable):
        data = x.data
        if isinstance(data, ir.Table):
            return data
        # Handle Polars DataFrames (e.g. generated cohort tables)
        try:
            import polars as pl

            if isinstance(data, pl.DataFrame):
                return ibis.memtable(data.to_arrow())
            if isinstance(data, pl.LazyFrame):
                return ibis.memtable(data.collect().to_arrow())
        except Exception:
            pass
        msg = f"Expected Ibis-backed CdmTable, got {type(data).__name__}"
        raise TypeError(msg)
    if isinstance(x, ir.Table):
        return x
    msg = f"Expected CdmTable or Ibis Table, got {type(x).__name__}"
    raise TypeError(msg)


def _add_observation_period(
    tbl: ir.Table,
    cdm: CdmReference,
    *,
    pid: str,
    index_date: str,
    prior_observation: bool,
    prior_observation_name: str,
    prior_observation_type: str,
    future_observation: bool,
    future_observation_name: str,
    future_observation_type: str,
) -> ir.Table:
    """Join observation_period to compute prior/future observation."""
    obs_period = _get_ibis_table(cdm["observation_period"])

    orig_cols = tbl.columns

    # Rename obs columns to avoid conflicts
    obs = obs_period.select(
        _obs_pid=obs_period["person_id"],
        _obs_start=obs_period["observation_period_start_date"],
        _obs_end=obs_period["observation_period_end_date"],
    )

    # Left join + filter to enclosing observation period
    joined = tbl.left_join(obs, tbl[pid] == obs["_obs_pid"]).filter(
        lambda t: (
            t["_obs_start"].isnull()
            | ((t["_obs_start"] <= t[index_date]) & (t[index_date] <= t["_obs_end"]))
        )
    )

    # Compute requested columns
    new_mutate = {}
    if prior_observation:
        if prior_observation_type == "days":
            new_mutate[prior_observation_name] = (
                joined[index_date] - joined["_obs_start"]
            ).cast("int64")
        else:
            new_mutate[prior_observation_name] = joined["_obs_start"]

    if future_observation:
        if future_observation_type == "days":
            new_mutate[future_observation_name] = (
                joined["_obs_end"] - joined[index_date]
            ).cast("int64")
        else:
            new_mutate[future_observation_name] = joined["_obs_end"]

    joined = joined.mutate(**new_mutate)

    # Select original columns + new columns
    return joined.select(*orig_cols, *new_mutate.keys())


def _add_person_info(
    tbl: ir.Table,
    cdm: CdmReference,
    *,
    pid: str,
    index_date: str,
    age: bool,
    age_name: str,
    age_missing_month: int,
    age_missing_day: int,
    age_impose_month: bool,
    age_impose_day: bool,
    age_unit: str,
    age_group: dict[str, tuple[float, float]] | list[tuple[float, float]] | None,
    missing_age_group_value: str,
    sex: bool,
    sex_name: str,
    missing_sex_value: str,
    date_of_birth: bool,
    date_of_birth_name: str,
) -> ir.Table:
    """Join person table to compute age, sex, date of birth."""
    person = _get_ibis_table(cdm["person"])
    person_cols = person.columns

    orig_cols = tbl.columns

    # Build person sub-select with the columns we need
    person_select = {"_per_pid": person["person_id"]}

    need_dob = age or date_of_birth or (age_group is not None)
    if need_dob:
        person_select["_year_of_birth"] = person["year_of_birth"].cast("int64")

        # Month of birth
        if age_impose_month or "month_of_birth" not in person_cols:
            person_select["_month_of_birth"] = ibis.literal(age_missing_month).cast(
                "int64"
            )
        else:
            person_select["_month_of_birth"] = (
                person["month_of_birth"]
                .cast("int64")
                .fill_null(ibis.literal(age_missing_month).cast("int64"))
            )

        # Day of birth
        if age_impose_day or "day_of_birth" not in person_cols:
            person_select["_day_of_birth"] = ibis.literal(age_missing_day).cast("int64")
        else:
            person_select["_day_of_birth"] = (
                person["day_of_birth"]
                .cast("int64")
                .fill_null(ibis.literal(age_missing_day).cast("int64"))
            )

    if sex:
        person_select["_gender_concept_id"] = person["gender_concept_id"]

    per = person.select(**person_select)

    # Left join person
    joined = tbl.left_join(per, tbl[pid] == per["_per_pid"])

    # Build mutate expressions
    new_cols = {}

    if need_dob:
        # Construct date of birth expression
        # Use string concatenation + cast to date (portable across backends)
        dob_expr = ibis.cases(
            (
                joined["_year_of_birth"].isnull(),
                ibis.null().cast("date"),
            ),
            else_=ibis.literal("")
            .concat(
                joined["_year_of_birth"].cast("string"),
                ibis.literal("-"),
                joined["_month_of_birth"].cast("string").lpad(2, "0"),
                ibis.literal("-"),
                joined["_day_of_birth"].cast("string").lpad(2, "0"),
            )
            .cast("date"),
        )
        new_cols["_dob"] = dob_expr

    joined = joined.mutate(**new_cols)

    # Now compute age and other derived columns
    derived = {}

    if age or (age_group is not None):
        age_expr = _compute_age_expr(joined, index_date, age_unit)
        if age:
            derived[age_name] = age_expr

        if age_group is not None:
            age_groups = _normalize_age_groups(age_group)
            for group_col, ranges in age_groups.items():
                derived[group_col] = _age_group_case_expr(
                    age_expr, ranges, missing_age_group_value
                )

    if date_of_birth:
        derived[date_of_birth_name] = joined["_dob"]

    if sex:
        if missing_sex_value == "NA" or missing_sex_value is None:
            else_val = ibis.null().cast("string")
        else:
            else_val = ibis.literal(missing_sex_value)

        derived[sex_name] = ibis.cases(
            (joined["_gender_concept_id"].cast("int64") == ibis.literal(8507), "Male"),
            (
                joined["_gender_concept_id"].cast("int64") == ibis.literal(8532),
                "Female",
            ),
            else_=else_val,
        )

    joined = joined.mutate(**derived)

    # Select original columns + derived
    return joined.select(*orig_cols, *derived.keys())


def _compute_age_expr(
    tbl: ir.Table,
    index_date: str,
    age_unit: str,
) -> ir.Column:
    """Build an Ibis expression for age computation.

    Uses the R PatientProfiles integer-arithmetic trick:
    - years: floor((idx_packed - dob_packed) / 10000)
    - months: floor((idx_packed_m - dob_packed_m) / 100)
    - days: simple date difference
    """
    dob = tbl["_dob"]
    idx = tbl[index_date]

    if age_unit == "days":
        return (idx - dob).cast("int64")

    # Extract date parts — cast to int64 to avoid i8 overflow
    y_idx = idx.year().cast("int64")
    m_idx = idx.month().cast("int64")
    d_idx = idx.day().cast("int64")
    y_dob = dob.year().cast("int64")
    m_dob = dob.month().cast("int64")
    d_dob = dob.day().cast("int64")

    if age_unit == "years":
        # Pack as YYYYMMDD integers
        idx_packed = y_idx * ibis.literal(10000) + m_idx * ibis.literal(100) + d_idx
        dob_packed = y_dob * ibis.literal(10000) + m_dob * ibis.literal(100) + d_dob
        return ((idx_packed - dob_packed) / ibis.literal(10000)).floor().cast("int64")
    elif age_unit == "months":
        # Pack with 1200 factor for years
        idx_packed = y_idx * ibis.literal(1200) + m_idx * ibis.literal(100) + d_idx
        dob_packed = y_dob * ibis.literal(1200) + m_dob * ibis.literal(100) + d_dob
        return ((idx_packed - dob_packed) / ibis.literal(100)).floor().cast("int64")
    else:
        msg = f"Unknown age_unit: {age_unit!r}. Must be 'years', 'months', or 'days'."
        raise ValueError(msg)


def _normalize_age_groups(
    age_group: dict[str, tuple[float, float]] | list[tuple[float, float]],
) -> dict[str, dict[str, tuple[float, float]]]:
    """Normalize age_group into {column_name: {label: (lo, hi)}} format.

    Accepts:
    - dict mapping label -> (lo, hi): creates a single "age_group" column
    - list of (lo, hi) tuples: auto-labels like "0 to 17", "18 to 64", etc.
    """
    if isinstance(age_group, list):
        # Auto-label
        labelled: dict[str, tuple[float, float]] = {}
        for lo, hi in age_group:
            label = _auto_age_label(lo, hi)
            labelled[label] = (lo, hi)
        return {"age_group": labelled}
    elif isinstance(age_group, dict):
        # Check if values are tuples (flat dict) or nested dicts
        first_val = next(iter(age_group.values()), None)
        if isinstance(first_val, tuple):
            return {"age_group": age_group}
        elif isinstance(first_val, dict):
            return age_group  # type: ignore[return-value]
        else:
            msg = f"Unexpected age_group format: {type(first_val)}"
            raise TypeError(msg)
    else:
        msg = f"age_group must be a dict or list, got {type(age_group)}"
        raise TypeError(msg)


def _auto_age_label(lo: float, hi: float) -> str:
    """Generate an automatic label for an age range."""
    if math.isinf(lo) and math.isinf(hi):
        return "any"
    if math.isinf(lo):
        return f"{int(hi)} or below"
    if math.isinf(hi):
        return f"{int(lo)} or above"
    return f"{int(lo)} to {int(hi)}"


def _age_group_case_expr(
    age_expr: ir.Column,
    ranges: dict[str, tuple[float, float]],
    missing_value: str,
) -> ir.Column:
    """Build a CASE WHEN expression for age grouping."""
    cases: list[tuple[ir.BooleanValue, str]] = []
    for label, (lo, hi) in ranges.items():
        if math.isinf(lo) and math.isinf(hi):
            cases.append((age_expr.notnull(), label))
        elif math.isinf(lo):
            cases.append((age_expr <= ibis.literal(int(hi)), label))
        elif math.isinf(hi):
            cases.append((age_expr >= ibis.literal(int(lo)), label))
        else:
            cases.append(
                (
                    (age_expr >= ibis.literal(int(lo)))
                    & (age_expr <= ibis.literal(int(hi))),
                    label,
                )
            )

    return ibis.cases(*cases, else_=missing_value)
