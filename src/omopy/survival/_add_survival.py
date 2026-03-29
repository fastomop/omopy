"""Add survival time and status columns to a cohort table.

Implements ``add_cohort_survival()`` — the foundation function that enriches
a cohort table with ``time`` (days to event or censoring) and ``status``
(1 = event, 0 = censored) columns.

This is the Python equivalent of R's ``addCohortSurvival()`` from the
CohortSurvival package.

Algorithm (mirrors the R implementation):
1. Add ``future_observation`` (days to end of observation period) via profiles.
2. Check for outcome events in the washout period (flag via cohort intersect).
3. Get ``days_to_event`` (days from index to first outcome after index).
4. Apply censoring hierarchy: cohort exit → censor date → follow-up cap.
5. Compute ``status``: 1 if event occurred before censoring, else 0.
6. Compute ``time``: days_to_event if event, days_to_exit if censored.
7. Set time/status to NA for anyone with an event in the washout period.
"""

from __future__ import annotations

import math
from typing import Any

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm

__all__ = ["add_cohort_survival"]


def add_cohort_survival(
    x: CdmTable,
    cdm: CdmReference | None = None,
    *,
    outcome_cohort_table: str | CohortTable,
    outcome_cohort_id: int = 1,
    outcome_date_variable: str = "cohort_start_date",
    outcome_washout: int | float = float("inf"),
    censor_on_cohort_exit: bool = False,
    censor_on_date: str | None = None,
    follow_up_days: int | float = float("inf"),
    time_column: str = "time",
    status_column: str = "status",
) -> CdmTable:
    """Add survival time and status columns to a cohort table.

    Computes days-to-event/censoring for each person in the input cohort,
    relative to an outcome cohort.

    Parameters
    ----------
    x
        Input cohort table (must have ``subject_id``, ``cohort_start_date``,
        ``cohort_end_date``).
    cdm
        CDM reference. If ``None``, uses ``x.cdm``.
    outcome_cohort_table
        Name of the outcome cohort table in the CDM, or a CohortTable.
    outcome_cohort_id
        Which cohort definition ID in the outcome cohort to use.
    outcome_date_variable
        Date column in the outcome cohort for the event date.
    outcome_washout
        Number of days before index date to check for prior outcome.
        ``float('inf')`` means check the entire prior history.
    censor_on_cohort_exit
        If ``True``, censor at cohort_end_date instead of observation end.
    censor_on_date
        Column name in ``x`` containing a date to censor on.
    follow_up_days
        Maximum follow-up time in days. ``float('inf')`` = no cap.
    time_column
        Name of the output time column.
    status_column
        Name of the output status column.

    Returns
    -------
    CdmTable
        Input table with ``time_column`` and ``status_column`` added.
    """
    cdm = _resolve_cdm(x, cdm)
    tbl = _get_ibis_table(x)

    # Resolve outcome cohort
    if isinstance(outcome_cohort_table, str):
        outcome_ct = cdm[outcome_cohort_table]
        if not isinstance(outcome_ct, CohortTable):
            msg = f"Table '{outcome_cohort_table}' is not a CohortTable"
            raise TypeError(msg)
    else:
        outcome_ct = outcome_cohort_table

    outcome_tbl = _get_ibis_table(outcome_ct)

    # Filter outcome to the requested cohort_definition_id
    outcome_events = outcome_tbl.filter(
        outcome_tbl["cohort_definition_id"] == ibis.literal(outcome_cohort_id)
    )

    # --- Step 1: Add days_to_exit (future observation) ---
    # Join observation_period to get observation end date
    obs_period = _get_ibis_table(cdm["observation_period"])
    obs = obs_period.select(
        _obs_pid=obs_period["person_id"],
        _obs_end=obs_period["observation_period_end_date"],
        _obs_start=obs_period["observation_period_start_date"],
    )

    # Add a row ID for deduplication
    orig_cols = tbl.columns
    tbl = tbl.mutate(_row_id=ibis.row_number())

    # Left join to observation period (enclosing the index date)
    joined = tbl.left_join(obs, tbl["subject_id"] == obs["_obs_pid"]).filter(
        lambda t: t["_obs_start"].isnull()
        | (
            (t["_obs_start"] <= t["cohort_start_date"])
            & (t["cohort_start_date"] <= t["_obs_end"])
        )
    )

    # Compute days_to_exit based on censoring strategy
    if censor_on_cohort_exit:
        # Censor at cohort_end_date
        days_to_exit = (joined["cohort_end_date"] - joined["cohort_start_date"]).cast("int64")
    else:
        # Censor at end of observation period
        days_to_exit = (joined["_obs_end"] - joined["cohort_start_date"]).cast("int64")

    joined = joined.mutate(_days_to_exit=days_to_exit)

    # --- Step 2: Check for prior outcome in washout period ---
    if not math.isinf(outcome_washout):
        washout_days = int(outcome_washout)
        # Check if there's an outcome event in window [-washout, -1]
        prior_outcomes = outcome_events.select(
            _p_pid=outcome_events["subject_id"],
            _p_date=outcome_events[outcome_date_variable],
        )

        # Flag rows where there's a prior outcome in washout window
        # Use a left join and check for matches
        with_washout = joined.left_join(
            prior_outcomes,
            (joined["subject_id"] == prior_outcomes["_p_pid"])
            & (
                prior_outcomes["_p_date"]
                >= joined["cohort_start_date"] - ibis.interval(days=washout_days)
            )
            & (prior_outcomes["_p_date"] < joined["cohort_start_date"]),
        ).mutate(
            _has_washout_event=lambda t: t["_p_pid"].notnull()
        )

        # Group to collapse duplicates from multiple washout events
        group_cols = [c for c in joined.columns]
        with_washout = (
            with_washout.group_by(group_cols)
            .agg(_has_washout_event=with_washout["_has_washout_event"].max())
        )

        joined = with_washout
    else:
        # Infinite washout: check entire prior history
        prior_outcomes = outcome_events.select(
            _p_pid=outcome_events["subject_id"],
            _p_date=outcome_events[outcome_date_variable],
        )

        with_washout = joined.left_join(
            prior_outcomes,
            (joined["subject_id"] == prior_outcomes["_p_pid"])
            & (prior_outcomes["_p_date"] < joined["cohort_start_date"]),
        ).mutate(
            _has_washout_event=lambda t: t["_p_pid"].notnull()
        )

        group_cols = [c for c in joined.columns]
        with_washout = (
            with_washout.group_by(group_cols)
            .agg(_has_washout_event=with_washout["_has_washout_event"].max())
        )

        joined = with_washout

    # --- Step 3: Get days to first outcome event after index date ---
    future_outcomes = outcome_events.select(
        _f_pid=outcome_events["subject_id"],
        _f_date=outcome_events[outcome_date_variable],
    )

    with_event = joined.left_join(
        future_outcomes,
        (joined["subject_id"] == future_outcomes["_f_pid"])
        & (future_outcomes["_f_date"] >= joined["cohort_start_date"]),
    ).mutate(
        _days_to_event=(
            lambda t: (t["_f_date"] - t["cohort_start_date"]).cast("int64")
        )
    )

    # Take the FIRST (minimum) days_to_event per row
    group_cols2 = [c for c in joined.columns]
    with_event = (
        with_event.group_by(group_cols2)
        .agg(_days_to_event=with_event["_days_to_event"].min())
    )

    # --- Step 4: Apply censoring hierarchy ---
    # Start with days_to_exit from observation period / cohort exit
    censor_time = with_event["_days_to_exit"]

    # Apply censor_on_date if provided
    if censor_on_date is not None:
        # censor_on_date is a column name in x with a date
        days_to_censor_date = (
            with_event[censor_on_date] - with_event["cohort_start_date"]
        ).cast("int64")
        censor_time = ibis.least(censor_time, days_to_censor_date)

    # Apply follow_up_days cap
    if not math.isinf(follow_up_days):
        cap = ibis.literal(int(follow_up_days))
        censor_time = ibis.least(censor_time, cap)

    # --- Step 5+6: Compute status and time ---
    # Event occurred if days_to_event is not null AND days_to_event <= censor_time
    status_expr = ibis.cases(
        (
            with_event["_days_to_event"].notnull()
            & (with_event["_days_to_event"] <= censor_time),
            1,
        ),
        else_=0,
    )

    # Time: days_to_event if event, censor_time if censored
    time_expr = ibis.cases(
        (
            with_event["_days_to_event"].notnull()
            & (with_event["_days_to_event"] <= censor_time),
            with_event["_days_to_event"],
        ),
        else_=censor_time,
    )

    # --- Step 7: Null out for washout exclusions ---
    final_status = ibis.cases(
        (with_event["_has_washout_event"].cast("boolean"), ibis.null().cast("int64")),
        else_=status_expr,
    )
    final_time = ibis.cases(
        (with_event["_has_washout_event"].cast("boolean"), ibis.null().cast("int64")),
        else_=time_expr,
    )

    result = with_event.mutate(
        **{status_column: final_status, time_column: final_time}
    )

    # Select only original columns + new columns
    select_cols = [c for c in orig_cols if c != "_row_id"] + [time_column, status_column]
    result = result.select(*select_cols)

    return x._with_data(result)
