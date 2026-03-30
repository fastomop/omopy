"""Survival estimation — Kaplan-Meier and competing risk analysis.

Implements ``estimate_single_event_survival()`` and
``estimate_competing_risk_survival()`` — the main analytical functions
that compute survival curves, summary statistics, risk tables, and
attrition from cohort data.

This is the Python equivalent of R's ``estimateSingleEventSurvival()``
and ``estimateCompetingRiskSurvival()`` from the CohortSurvival package.

Uses lifelines for Kaplan-Meier estimation and a custom Aalen-Johansen
implementation for competing risk cumulative incidence.
"""

from __future__ import annotations

import math
import warnings
from typing import Any

import numpy as np
import polars as pl
from lifelines import KaplanMeierFitter

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)
from omopy.survival._add_survival import add_cohort_survival

__all__ = [
    "estimate_competing_risk_survival",
    "estimate_single_event_survival",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PACKAGE_NAME = "omopy.survival"
_PACKAGE_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def estimate_single_event_survival(
    cdm: CdmReference,
    target_cohort_table: str,
    outcome_cohort_table: str,
    *,
    target_cohort_id: int | list[int] | None = None,
    outcome_cohort_id: int | list[int] | None = None,
    outcome_date_variable: str = "cohort_start_date",
    outcome_washout: int | float = float("inf"),
    censor_on_cohort_exit: bool = False,
    censor_on_date: str | None = None,
    follow_up_days: int | float = float("inf"),
    strata: list[str] | None = None,
    event_gap: int = 30,
    estimate_gap: int = 1,
    restricted_mean_follow_up: int | None = None,
    minimum_survival_days: int = 1,
) -> SummarisedResult:
    """Estimate Kaplan-Meier survival for a single event.

    Parameters
    ----------
    cdm
        CDM reference.
    target_cohort_table
        Name of the target (exposure/index) cohort table.
    outcome_cohort_table
        Name of the outcome cohort table.
    target_cohort_id
        Which target cohort IDs to analyse. ``None`` = all.
    outcome_cohort_id
        Which outcome cohort IDs to analyse. ``None`` = all.
    outcome_date_variable
        Date column in outcome cohort for event timing.
    outcome_washout
        Days before index to exclude prior events. ``inf`` = entire history.
    censor_on_cohort_exit
        Censor at target cohort end date rather than observation end.
    censor_on_date
        Column name with a censoring date.
    follow_up_days
        Maximum follow-up in days. ``inf`` = no cap.
    strata
        Column names for stratification (must be in target cohort).
    event_gap
        Interval width in days for the risk table.
    estimate_gap
        Step size in days for the survival curve estimates.
    restricted_mean_follow_up
        Time horizon for restricted mean survival. ``None`` = max observed.
    minimum_survival_days
        Exclude persons with follow-up < this many days.

    Returns
    -------
    SummarisedResult
        Result with ``survival_estimates``, ``survival_events``,
        ``survival_summary``, and ``survival_attrition`` result types.
    """
    return _estimate_survival(
        cdm=cdm,
        target_cohort_table=target_cohort_table,
        outcome_cohort_table=outcome_cohort_table,
        competing_outcome_cohort_table=None,
        target_cohort_id=target_cohort_id,
        outcome_cohort_id=outcome_cohort_id,
        outcome_date_variable=outcome_date_variable,
        outcome_washout=outcome_washout,
        competing_outcome_cohort_id=None,
        competing_outcome_date_variable="cohort_start_date",
        competing_outcome_washout=float("inf"),
        censor_on_cohort_exit=censor_on_cohort_exit,
        censor_on_date=censor_on_date,
        follow_up_days=follow_up_days,
        strata=strata,
        event_gap=event_gap,
        estimate_gap=estimate_gap,
        restricted_mean_follow_up=restricted_mean_follow_up,
        minimum_survival_days=minimum_survival_days,
        analysis_type="single_event",
    )


def estimate_competing_risk_survival(
    cdm: CdmReference,
    target_cohort_table: str,
    outcome_cohort_table: str,
    competing_outcome_cohort_table: str,
    *,
    target_cohort_id: int | list[int] | None = None,
    outcome_cohort_id: int | list[int] | None = None,
    outcome_date_variable: str = "cohort_start_date",
    outcome_washout: int | float = float("inf"),
    competing_outcome_cohort_id: int | list[int] | None = None,
    competing_outcome_date_variable: str = "cohort_start_date",
    competing_outcome_washout: int | float = float("inf"),
    censor_on_cohort_exit: bool = False,
    censor_on_date: str | None = None,
    follow_up_days: int | float = float("inf"),
    strata: list[str] | None = None,
    event_gap: int = 30,
    estimate_gap: int = 1,
    restricted_mean_follow_up: int | None = None,
    minimum_survival_days: int = 1,
) -> SummarisedResult:
    """Estimate cumulative incidence with competing risks.

    Uses the Aalen-Johansen estimator to compute cumulative incidence
    functions (CIF) in the presence of a competing event.

    Parameters
    ----------
    cdm
        CDM reference.
    target_cohort_table
        Name of the target cohort table.
    outcome_cohort_table
        Name of the primary outcome cohort table.
    competing_outcome_cohort_table
        Name of the competing outcome cohort table.
    target_cohort_id, outcome_cohort_id
        Cohort ID filters.
    outcome_date_variable
        Date column for primary outcome timing.
    outcome_washout
        Washout period for primary outcome.
    competing_outcome_cohort_id
        Which competing outcome cohort IDs to use.
    competing_outcome_date_variable
        Date column for competing outcome timing.
    competing_outcome_washout
        Washout period for competing outcome.
    censor_on_cohort_exit
        Censor at target cohort end.
    censor_on_date
        Column with censoring date.
    follow_up_days
        Maximum follow-up in days.
    strata
        Stratification columns.
    event_gap, estimate_gap
        Risk table interval and estimate step.
    restricted_mean_follow_up
        Time horizon for restricted mean.
    minimum_survival_days
        Minimum follow-up filter.

    Returns
    -------
    SummarisedResult
        Result with competing risk cumulative incidence estimates.
    """
    return _estimate_survival(
        cdm=cdm,
        target_cohort_table=target_cohort_table,
        outcome_cohort_table=outcome_cohort_table,
        competing_outcome_cohort_table=competing_outcome_cohort_table,
        target_cohort_id=target_cohort_id,
        outcome_cohort_id=outcome_cohort_id,
        outcome_date_variable=outcome_date_variable,
        outcome_washout=outcome_washout,
        competing_outcome_cohort_id=competing_outcome_cohort_id,
        competing_outcome_date_variable=competing_outcome_date_variable,
        competing_outcome_washout=competing_outcome_washout,
        censor_on_cohort_exit=censor_on_cohort_exit,
        censor_on_date=censor_on_date,
        follow_up_days=follow_up_days,
        strata=strata,
        event_gap=event_gap,
        estimate_gap=estimate_gap,
        restricted_mean_follow_up=restricted_mean_follow_up,
        minimum_survival_days=minimum_survival_days,
        analysis_type="competing_risk",
    )


# ---------------------------------------------------------------------------
# Internal estimation engine
# ---------------------------------------------------------------------------


def _estimate_survival(
    *,
    cdm: CdmReference,
    target_cohort_table: str,
    outcome_cohort_table: str,
    competing_outcome_cohort_table: str | None,
    target_cohort_id: int | list[int] | None,
    outcome_cohort_id: int | list[int] | None,
    outcome_date_variable: str,
    outcome_washout: int | float,
    competing_outcome_cohort_id: int | list[int] | None,
    competing_outcome_date_variable: str,
    competing_outcome_washout: int | float,
    censor_on_cohort_exit: bool,
    censor_on_date: str | None,
    follow_up_days: int | float,
    strata: list[str] | None,
    event_gap: int,
    estimate_gap: int,
    restricted_mean_follow_up: int | None,
    minimum_survival_days: int,
    analysis_type: str,
) -> SummarisedResult:
    """Core estimation engine shared by single-event and competing-risk."""
    # Resolve cohort tables and IDs
    target_ct = _resolve_cohort(cdm, target_cohort_table)
    outcome_ct = _resolve_cohort(cdm, outcome_cohort_table)
    target_ids = _get_cohort_ids(target_ct, target_cohort_id)
    outcome_ids = _get_cohort_ids(outcome_ct, outcome_cohort_id)

    competing_ct = None
    competing_ids: list[int] = []
    if competing_outcome_cohort_table is not None:
        competing_ct = _resolve_cohort(cdm, competing_outcome_cohort_table)
        competing_ids = _get_cohort_ids(competing_ct, competing_outcome_cohort_id)
        if not competing_ids:
            competing_ids = [competing_ct.settings["cohort_definition_id"][0]]

    all_data: list[pl.DataFrame] = []
    all_settings: list[dict[str, Any]] = []
    result_id = 0

    for t_id in target_ids:
        for o_id in outcome_ids:
            result_id += 1
            target_name = _cohort_name(target_ct, t_id)
            outcome_name = _cohort_name(outcome_ct, o_id)

            # Step 1: Add outcome survival (time + status)
            filtered_target = _filter_cohort_table(target_ct, t_id)
            enriched = add_cohort_survival(
                filtered_target,
                cdm,
                outcome_cohort_table=outcome_ct,
                outcome_cohort_id=o_id,
                outcome_date_variable=outcome_date_variable,
                outcome_washout=outcome_washout,
                censor_on_cohort_exit=censor_on_cohort_exit,
                censor_on_date=censor_on_date,
                follow_up_days=follow_up_days,
                time_column="outcome_time",
                status_column="outcome_status",
            )

            # Collect to Polars for local computation
            local_df = enriched.collect()

            # Track attrition
            attrition_rows: list[dict[str, Any]] = []
            n_initial = len(local_df)
            attrition_rows.append(
                _attrition_row(
                    result_id,
                    target_name,
                    outcome_name,
                    "Starting population",
                    n_initial,
                    n_initial,
                    0,
                )
            )

            # Step 2: Remove NAs (washout exclusions)
            pre_washout = len(local_df)
            local_df = local_df.filter(
                pl.col("outcome_time").is_not_null() & pl.col("outcome_status").is_not_null()
            )
            n_after_washout = len(local_df)
            excluded_washout = pre_washout - n_after_washout
            if excluded_washout > 0:
                attrition_rows.append(
                    _attrition_row(
                        result_id,
                        target_name,
                        outcome_name,
                        "Excluded: prior outcome in washout",
                        n_after_washout,
                        excluded_washout,
                        len(attrition_rows),
                    )
                )

            # Step 3: Filter minimum survival days
            pre_min = len(local_df)
            local_df = local_df.filter(pl.col("outcome_time") >= minimum_survival_days)
            n_after_min = len(local_df)
            excluded_min = pre_min - n_after_min
            if excluded_min > 0:
                attrition_rows.append(
                    _attrition_row(
                        result_id,
                        target_name,
                        outcome_name,
                        f"Excluded: follow-up < {minimum_survival_days} days",
                        n_after_min,
                        excluded_min,
                        len(attrition_rows),
                    )
                )

            # Handle competing risk
            if analysis_type == "competing_risk" and competing_ct is not None:
                c_id = competing_ids[0] if competing_ids else 1
                competing_name = _cohort_name(competing_ct, c_id)

                # Add competing outcome time/status
                filtered_target2 = _filter_cohort_table(target_ct, t_id)
                competing_enriched = add_cohort_survival(
                    filtered_target2,
                    cdm,
                    outcome_cohort_table=competing_ct,
                    outcome_cohort_id=c_id,
                    outcome_date_variable=competing_outcome_date_variable,
                    outcome_washout=competing_outcome_washout,
                    censor_on_cohort_exit=censor_on_cohort_exit,
                    censor_on_date=censor_on_date,
                    follow_up_days=follow_up_days,
                    time_column="competing_time",
                    status_column="competing_status",
                )
                competing_local = competing_enriched.collect()

                # Join competing info onto the main df
                # Use subject_id + cohort_start_date as key
                join_cols = ["subject_id", "cohort_start_date"]
                competing_cols = competing_local.select(
                    *join_cols, "competing_time", "competing_status"
                )
                local_df = local_df.join(competing_cols, on=join_cols, how="left")

                # Create combined status: 0=censored, 1=primary event, 2=competing event
                local_df = _add_competing_risk_vars(local_df)
            else:
                competing_name = ""

            # Build strata groups
            strata_groups = _get_strata_groups(local_df, strata or [])

            for s_name, s_level, s_mask in strata_groups:
                sub_df = local_df.filter(s_mask) if s_mask is not None else local_df

                if len(sub_df) == 0:
                    continue

                if analysis_type == "single_event":
                    estimates, events, summary = _single_event_survival(
                        sub_df,
                        event_gap=event_gap,
                        estimate_gap=estimate_gap,
                        restricted_mean_follow_up=restricted_mean_follow_up,
                    )
                else:
                    estimates, events, summary = _competing_risk_survival(
                        sub_df,
                        event_gap=event_gap,
                        estimate_gap=estimate_gap,
                        restricted_mean_follow_up=restricted_mean_follow_up,
                    )

                # Format into SummarisedResult rows
                est_rows = _format_estimates(
                    estimates,
                    result_id,
                    cdm.cdm_name,
                    target_name,
                    outcome_name,
                    s_name,
                    s_level,
                )
                evt_rows = _format_events(
                    events,
                    result_id,
                    cdm.cdm_name,
                    target_name,
                    outcome_name,
                    s_name,
                    s_level,
                    event_gap,
                )
                sum_rows = _format_summary(
                    summary,
                    result_id,
                    cdm.cdm_name,
                    target_name,
                    outcome_name,
                    s_name,
                    s_level,
                )
                all_data.extend([est_rows, evt_rows, sum_rows])

            # Add attrition data
            attr_df = _format_attrition(attrition_rows, cdm.cdm_name)
            all_data.append(attr_df)

            # Build settings
            settings_dict: dict[str, Any] = {
                "result_id": result_id,
                "result_type": f"survival_{analysis_type}",
                "package_name": _PACKAGE_NAME,
                "package_version": _PACKAGE_VERSION,
                "analysis_type": analysis_type,
                "target_cohort_name": target_name,
                "outcome_cohort_name": outcome_name,
                "outcome_date_variable": outcome_date_variable,
                "outcome_washout": str(outcome_washout),
                "censor_on_cohort_exit": str(censor_on_cohort_exit),
                "censor_on_date": str(censor_on_date) if censor_on_date else "",
                "follow_up_days": str(follow_up_days),
                "event_gap": str(event_gap),
                "estimate_gap": str(estimate_gap),
                "restricted_mean_follow_up": str(restricted_mean_follow_up or ""),
                "minimum_survival_days": str(minimum_survival_days),
            }
            if analysis_type == "competing_risk":
                settings_dict["competing_outcome_cohort_name"] = competing_name
            all_settings.append(settings_dict)

    if all_data:
        combined = pl.concat([d for d in all_data if len(d) > 0])
    else:
        combined = _empty_data()

    settings_df = (
        pl.DataFrame(all_settings).cast({"result_id": pl.Int64})
        if all_settings
        else _empty_settings()
    )

    return SummarisedResult(combined, settings=settings_df)


# ---------------------------------------------------------------------------
# Single-event (Kaplan-Meier) survival
# ---------------------------------------------------------------------------


def _single_event_survival(
    df: pl.DataFrame,
    *,
    event_gap: int,
    estimate_gap: int,
    restricted_mean_follow_up: int | None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Compute KM survival estimates, risk table, and summary stats.

    Returns
    -------
    (estimates, events, summary)
        estimates: DataFrame with columns [time, estimate, lower, upper]
        events: DataFrame with columns [time, n_risk, n_events, n_censor]
        summary: dict of summary statistics
    """
    times = df["outcome_time"].to_numpy().astype(float)
    events = df["outcome_status"].to_numpy().astype(float)

    kmf = KaplanMeierFitter()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kmf.fit(durations=times, event_observed=events)

    max_time = int(times.max()) if len(times) > 0 else 0
    if not math.isinf(follow_up := (restricted_mean_follow_up or max_time)):
        max_time_for_estimates = int(follow_up)
    else:
        max_time_for_estimates = max_time

    # Time points for estimates
    timepoints = list(range(0, max_time_for_estimates + 1, estimate_gap))
    if timepoints and timepoints[-1] != max_time_for_estimates:
        timepoints.append(max_time_for_estimates)

    # Extract survival estimates at timepoints
    if timepoints:
        surv_vals = kmf.survival_function_at_times(timepoints).values
        ci_df = kmf.confidence_interval_survival_function_
        # Interpolate CI at timepoints
        ci_lower = np.interp(timepoints, ci_df.index.values, ci_df.iloc[:, 0].values)
        ci_upper = np.interp(timepoints, ci_df.index.values, ci_df.iloc[:, 1].values)
    else:
        surv_vals = np.array([])
        ci_lower = np.array([])
        ci_upper = np.array([])

    estimates_df = pl.DataFrame(
        {
            "time": timepoints,
            "estimate": surv_vals.tolist(),
            "estimate_95CI_lower": ci_lower.tolist(),
            "estimate_95CI_upper": ci_upper.tolist(),
        }
    )

    # Risk table (events grouped by event_gap intervals)
    events_df = _compute_risk_table(df, event_gap, max_time)

    # Summary statistics
    summary = _compute_km_summary(kmf, df, restricted_mean_follow_up)

    return estimates_df, events_df, summary


# ---------------------------------------------------------------------------
# Competing risk (Aalen-Johansen) survival
# ---------------------------------------------------------------------------


def _competing_risk_survival(
    df: pl.DataFrame,
    *,
    event_gap: int,
    estimate_gap: int,
    restricted_mean_follow_up: int | None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Compute cumulative incidence function with competing risks.

    Uses the Aalen-Johansen estimator computed from first principles:
    CIF_k(t) = sum_{j: t_j <= t} h_k(t_j) * S(t_{j-1})

    where h_k(t_j) = d_k(t_j) / n(t_j) is the cause-specific hazard
    and S(t) is the overall Kaplan-Meier survival.

    Returns (estimates, events, summary) with the same structure as
    single-event but estimates represent cumulative incidence (not survival).
    """
    # combined_status: 0=censored, 1=primary event, 2=competing event
    times_arr = df["combined_time"].to_numpy().astype(float)
    status_arr = df["combined_status"].to_numpy().astype(int)

    # Get sorted unique event times
    event_times = np.sort(np.unique(times_arr[status_arr > 0]))

    n_total = len(times_arr)
    cif1 = np.zeros(len(event_times))  # CIF for primary event
    cif1_var = np.zeros(len(event_times))  # variance estimate
    overall_surv = 1.0

    for i, t in enumerate(event_times):
        # Number at risk at time t
        n_risk = np.sum(times_arr >= t)
        if n_risk == 0:
            continue

        # Cause-specific events at time t
        d1 = np.sum((times_arr == t) & (status_arr == 1))  # primary
        d2 = np.sum((times_arr == t) & (status_arr == 2))  # competing
        d_total = d1 + d2

        # Cause-specific hazard for primary event
        h1 = d1 / n_risk

        # CIF increment
        cif1[i] = (cif1[i - 1] if i > 0 else 0) + h1 * overall_surv

        # Update overall survival (for next step)
        overall_surv *= 1 - d_total / n_risk

    max_time = int(times_arr.max()) if len(times_arr) > 0 else 0
    max_time_est = restricted_mean_follow_up or max_time

    # Interpolate to estimate_gap timepoints
    timepoints = list(range(0, max_time_est + 1, estimate_gap))
    if timepoints and timepoints[-1] != max_time_est:
        timepoints.append(max_time_est)

    if len(event_times) > 0 and len(timepoints) > 0:
        cif_vals = np.interp(
            timepoints,
            event_times,
            cif1,
            left=0.0,
            right=cif1[-1] if len(cif1) > 0 else 0.0,
        )
    else:
        cif_vals = np.zeros(len(timepoints))

    # Approximate CI using Greenwood-like variance (simplified)
    # For a proper implementation, we'd use the full AJ variance formula
    # Here we use a simple approximation: SE ~ sqrt(CIF * (1-CIF) / n_risk)
    n_at_risk_at_times = (
        np.array([np.sum(times_arr >= t) for t in timepoints]) if timepoints else np.array([])
    )

    ci_lower = np.zeros(len(timepoints))
    ci_upper = np.zeros(len(timepoints))
    for j in range(len(timepoints)):
        n_r = max(n_at_risk_at_times[j], 1) if len(n_at_risk_at_times) > 0 else 1
        se = np.sqrt(max(cif_vals[j] * (1 - cif_vals[j]) / n_r, 0))
        ci_lower[j] = max(0.0, cif_vals[j] - 1.96 * se)
        ci_upper[j] = min(1.0, cif_vals[j] + 1.96 * se)

    estimates_df = pl.DataFrame(
        {
            "time": timepoints,
            "estimate": cif_vals.tolist(),
            "estimate_95CI_lower": ci_lower.tolist(),
            "estimate_95CI_upper": ci_upper.tolist(),
        }
    )

    # Risk table
    events_df = _compute_risk_table_competing(df, event_gap, max_time)

    # Summary
    summary = _compute_cif_summary(
        df,
        cif1,
        event_times,
        restricted_mean_follow_up,
    )

    return estimates_df, events_df, summary


# ---------------------------------------------------------------------------
# Risk table computation
# ---------------------------------------------------------------------------


def _compute_risk_table(
    df: pl.DataFrame,
    event_gap: int,
    max_time: int,
) -> pl.DataFrame:
    """Compute risk table with n_risk, n_events, n_censor per interval."""
    times = df["outcome_time"].to_numpy()
    events = df["outcome_status"].to_numpy()
    n_total = len(times)

    intervals = list(range(0, max_time + event_gap, event_gap))
    rows: list[dict[str, Any]] = []

    for i, t_start in enumerate(intervals):
        t_end = t_start + event_gap

        # Number at risk at start of interval
        n_risk = int(np.sum(times >= t_start))

        # Events in [t_start, t_end)
        mask = (times >= t_start) & (times < t_end)
        n_events = int(np.sum(mask & (events == 1)))
        n_censor = int(np.sum(mask & (events == 0)))

        rows.append(
            {
                "time": t_start,
                "n_risk": n_risk,
                "n_events": n_events,
                "n_censor": n_censor,
            }
        )

    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            {
                "time": [],
                "n_risk": [],
                "n_events": [],
                "n_censor": [],
            }
        )
    )


def _compute_risk_table_competing(
    df: pl.DataFrame,
    event_gap: int,
    max_time: int,
) -> pl.DataFrame:
    """Compute risk table for competing risk analysis."""
    times = df["combined_time"].to_numpy()
    status = df["combined_status"].to_numpy()

    intervals = list(range(0, max_time + event_gap, event_gap))
    rows: list[dict[str, Any]] = []

    for t_start in intervals:
        t_end = t_start + event_gap
        n_risk = int(np.sum(times >= t_start))
        mask = (times >= t_start) & (times < t_end)
        n_events = int(np.sum(mask & (status == 1)))
        n_censor = int(np.sum(mask & (status == 0)))

        rows.append(
            {
                "time": t_start,
                "n_risk": n_risk,
                "n_events": n_events,
                "n_censor": n_censor,
            }
        )

    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            {
                "time": [],
                "n_risk": [],
                "n_events": [],
                "n_censor": [],
            }
        )
    )


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------


def _compute_km_summary(
    kmf: KaplanMeierFitter,
    df: pl.DataFrame,
    restricted_mean_follow_up: int | None,
) -> dict[str, Any]:
    """Extract summary statistics from a fitted KM model."""
    n_records = len(df)
    n_events = int(df["outcome_status"].sum())

    # Median survival
    median_surv = kmf.median_survival_time_
    if np.isinf(median_surv) or np.isnan(median_surv):
        median_surv = None

    # Median CI from lifelines
    try:
        ci = kmf.confidence_interval_median_survival_time_
        median_lower = float(ci.iloc[0, 0])
        median_upper = float(ci.iloc[0, 1])
        if np.isinf(median_lower) or np.isnan(median_lower):
            median_lower = None
        if np.isinf(median_upper) or np.isnan(median_upper):
            median_upper = None
    except Exception:
        median_lower = None
        median_upper = None

    # Restricted mean survival time (RMST)
    times = df["outcome_time"].to_numpy().astype(float)
    max_time = float(times.max()) if len(times) > 0 else 0.0
    rmst_limit = float(restricted_mean_follow_up) if restricted_mean_follow_up else max_time

    # Compute RMST as area under KM curve up to rmst_limit
    sf = kmf.survival_function_
    sf_times = sf.index.values.astype(float)
    sf_vals = sf.iloc[:, 0].values.astype(float)

    # Prepend time=0 if not present
    if len(sf_times) == 0 or sf_times[0] > 0:
        sf_times = np.concatenate([[0.0], sf_times])
        sf_vals = np.concatenate([[1.0], sf_vals])

    # Truncate at rmst_limit
    mask = sf_times <= rmst_limit
    trunc_times = sf_times[mask]
    trunc_vals = sf_vals[mask]

    # Add endpoint at rmst_limit
    if len(trunc_times) > 0 and trunc_times[-1] < rmst_limit:
        endpoint_val = float(np.interp(rmst_limit, sf_times, sf_vals))
        trunc_times = np.append(trunc_times, rmst_limit)
        trunc_vals = np.append(trunc_vals, endpoint_val)

    # RMST = integral of S(t) from 0 to rmst_limit (trapezoidal)
    if len(trunc_times) >= 2:
        rmst = float(np.trapezoid(trunc_vals, trunc_times))
    else:
        rmst = 0.0

    # RMST SE (Greenwood formula-based approximation)
    # Simple approximation: SE ~ RMST / sqrt(n_records)
    rmst_se = rmst / max(math.sqrt(n_records), 1.0)

    # Quantiles
    quantiles = {}
    for q_name, q_val in [
        ("q0", 0.0),
        ("q05", 0.05),
        ("q25", 0.25),
        ("q75", 0.75),
        ("q95", 0.95),
        ("q100", 1.0),
    ]:
        if q_val == 0.0:
            quantiles[q_name] = float(times.min()) if len(times) > 0 else None
        elif q_val == 1.0:
            quantiles[q_name] = float(times.max()) if len(times) > 0 else None
        else:
            try:
                qv = kmf.percentile(1 - q_val)
                if np.isinf(qv) or np.isnan(qv):
                    quantiles[q_name] = None
                else:
                    quantiles[q_name] = float(qv)
            except Exception:
                quantiles[q_name] = None

    return {
        "number_records": n_records,
        "n_events": n_events,
        "median_survival": median_surv,
        "median_survival_95CI_lower": median_lower,
        "median_survival_95CI_upper": median_upper,
        "restricted_mean_survival": rmst,
        "restricted_mean_survival_se": rmst_se,
        "restricted_mean_survival_95CI_lower": rmst - 1.96 * rmst_se,
        "restricted_mean_survival_95CI_upper": rmst + 1.96 * rmst_se,
        **quantiles,
    }


def _compute_cif_summary(
    df: pl.DataFrame,
    cif1: np.ndarray,
    event_times: np.ndarray,
    restricted_mean_follow_up: int | None,
) -> dict[str, Any]:
    """Compute summary statistics for competing risk CIF."""
    n_records = len(df)
    n_events = int((df["combined_status"] == 1).sum())

    # Median time to event (time when CIF crosses 0.5)
    median_surv = None
    for i, val in enumerate(cif1):
        if val >= 0.5:
            median_surv = float(event_times[i])
            break

    times_arr = df["combined_time"].to_numpy().astype(float)
    max_time = float(times_arr.max()) if len(times_arr) > 0 else 0.0

    # RMST (area under 1-CIF curve)
    rmst_limit = float(restricted_mean_follow_up) if restricted_mean_follow_up else max_time
    if len(event_times) > 0:
        et = np.concatenate([[0.0], event_times.astype(float)])
        cv = np.concatenate([[0.0], cif1.astype(float)])
        mask = et <= rmst_limit
        trunc_t = et[mask]
        trunc_c = cv[mask]
        if len(trunc_t) > 0 and trunc_t[-1] < rmst_limit:
            endpoint = float(np.interp(rmst_limit, et, cv))
            trunc_t = np.append(trunc_t, rmst_limit)
            trunc_c = np.append(trunc_c, endpoint)
        if len(trunc_t) >= 2:
            rmst = float(np.trapezoid(1 - trunc_c, trunc_t))
        else:
            rmst = 0.0
    else:
        rmst = rmst_limit

    rmst_se = rmst / max(math.sqrt(n_records), 1.0)

    return {
        "number_records": n_records,
        "n_events": n_events,
        "median_survival": median_surv,
        "median_survival_95CI_lower": None,
        "median_survival_95CI_upper": None,
        "restricted_mean_survival": rmst,
        "restricted_mean_survival_se": rmst_se,
        "restricted_mean_survival_95CI_lower": rmst - 1.96 * rmst_se,
        "restricted_mean_survival_95CI_upper": rmst + 1.96 * rmst_se,
    }


# ---------------------------------------------------------------------------
# Competing risk variable construction
# ---------------------------------------------------------------------------


def _add_competing_risk_vars(df: pl.DataFrame) -> pl.DataFrame:
    """Create combined time/status for competing risk analysis.

    Combined status:
    - 0 = censored (neither primary nor competing event)
    - 1 = primary event (occurred first)
    - 2 = competing event (occurred first)

    When both events occur at the same time, primary event takes precedence.
    """
    return df.with_columns(
        [
            # Combined time: minimum of outcome_time and competing_time
            pl.when(
                pl.col("competing_time").is_not_null()
                & (pl.col("competing_time") < pl.col("outcome_time"))
            )
            .then(pl.col("competing_time"))
            .otherwise(pl.col("outcome_time"))
            .alias("combined_time"),
            # Combined status
            pl.when(
                (pl.col("outcome_status") == 1)
                & (
                    pl.col("competing_status").is_null()
                    | (pl.col("competing_status") == 0)
                    | (pl.col("outcome_time") <= pl.col("competing_time"))
                )
            )
            .then(pl.lit(1))
            .when(
                (pl.col("competing_status") == 1)
                & (
                    pl.col("outcome_status").is_null()
                    | (pl.col("outcome_status") == 0)
                    | (pl.col("competing_time") < pl.col("outcome_time"))
                )
            )
            .then(pl.lit(2))
            .otherwise(pl.lit(0))
            .alias("combined_status"),
        ]
    )


# ---------------------------------------------------------------------------
# Format results into SummarisedResult rows
# ---------------------------------------------------------------------------


def _format_estimates(
    estimates: pl.DataFrame,
    result_id: int,
    cdm_name: str,
    target_name: str,
    outcome_name: str,
    strata_name: str,
    strata_level: str,
) -> pl.DataFrame:
    """Format survival/CIF estimates into SummarisedResult long format."""
    rows: list[dict[str, str]] = []
    for row in estimates.iter_rows(named=True):
        t = row["time"]
        for est_name in ["estimate", "estimate_95CI_lower", "estimate_95CI_upper"]:
            val = row[est_name]
            rows.append(
                {
                    "result_id": str(result_id),
                    "cdm_name": cdm_name,
                    "group_name": "target_cohort",
                    "group_level": target_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "outcome",
                    "variable_level": outcome_name,
                    "estimate_name": est_name,
                    "estimate_type": "numeric",
                    "estimate_value": _fmt(val),
                    "additional_name": "time",
                    "additional_level": str(t),
                }
            )

    return _rows_to_df(rows)


def _format_events(
    events: pl.DataFrame,
    result_id: int,
    cdm_name: str,
    target_name: str,
    outcome_name: str,
    strata_name: str,
    strata_level: str,
    event_gap: int,
) -> pl.DataFrame:
    """Format risk table into SummarisedResult long format."""
    rows: list[dict[str, str]] = []
    for row in events.iter_rows(named=True):
        t = row["time"]
        for est_name, est_val in [
            ("n_risk_count", row["n_risk"]),
            ("n_events_count", row["n_events"]),
            ("n_censor_count", row["n_censor"]),
        ]:
            rows.append(
                {
                    "result_id": str(result_id),
                    "cdm_name": cdm_name,
                    "group_name": "target_cohort",
                    "group_level": target_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "outcome",
                    "variable_level": outcome_name,
                    "estimate_name": est_name,
                    "estimate_type": "integer",
                    "estimate_value": str(int(est_val)),
                    "additional_name": NAME_LEVEL_SEP.join(["time", "eventgap"]),
                    "additional_level": NAME_LEVEL_SEP.join([str(t), str(event_gap)]),
                }
            )

    return _rows_to_df(rows)


def _format_summary(
    summary: dict[str, Any],
    result_id: int,
    cdm_name: str,
    target_name: str,
    outcome_name: str,
    strata_name: str,
    strata_level: str,
) -> pl.DataFrame:
    """Format summary statistics into SummarisedResult long format."""
    rows: list[dict[str, str]] = []

    for est_name, est_val in summary.items():
        est_type = "integer" if est_name in ("number_records", "n_events") else "numeric"
        rows.append(
            {
                "result_id": str(result_id),
                "cdm_name": cdm_name,
                "group_name": "target_cohort",
                "group_level": target_name,
                "strata_name": strata_name,
                "strata_level": strata_level,
                "variable_name": "outcome",
                "variable_level": outcome_name,
                "estimate_name": est_name,
                "estimate_type": est_type,
                "estimate_value": _fmt(est_val),
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }
        )

    return _rows_to_df(rows)


def _format_attrition(
    attrition_rows: list[dict[str, Any]],
    cdm_name: str,
) -> pl.DataFrame:
    """Format attrition tracking into SummarisedResult long format."""
    rows: list[dict[str, str]] = []
    for attr in attrition_rows:
        for est_name in ["number_records", "excluded_records"]:
            rows.append(
                {
                    "result_id": str(attr["result_id"]),
                    "cdm_name": cdm_name,
                    "group_name": "target_cohort",
                    "group_level": attr["target_name"],
                    "strata_name": "reason",
                    "strata_level": attr["reason"],
                    "variable_name": "outcome",
                    "variable_level": attr["outcome_name"],
                    "estimate_name": est_name,
                    "estimate_type": "integer",
                    "estimate_value": str(attr[est_name]),
                    "additional_name": "reason_id",
                    "additional_level": str(attr["reason_id"]),
                }
            )
    return _rows_to_df(rows)


def _attrition_row(
    result_id: int,
    target_name: str,
    outcome_name: str,
    reason: str,
    number_records: int,
    excluded_records: int,
    reason_id: int,
) -> dict[str, Any]:
    """Build an attrition tracking dict."""
    return {
        "result_id": result_id,
        "target_name": target_name,
        "outcome_name": outcome_name,
        "reason": reason,
        "number_records": number_records,
        "excluded_records": excluded_records,
        "reason_id": reason_id,
    }


# ---------------------------------------------------------------------------
# Strata helpers
# ---------------------------------------------------------------------------


def _get_strata_groups(
    df: pl.DataFrame,
    strata_cols: list[str],
) -> list[tuple[str, str, pl.Expr | None]]:
    """Build strata group definitions.

    Always includes overall. Each unique combination of strata columns
    produces an additional group.
    """
    groups: list[tuple[str, str, pl.Expr | None]] = []

    # Overall
    groups.append((OVERALL, OVERALL, None))

    if not strata_cols:
        return groups

    available = [c for c in strata_cols if c in df.columns]
    if not available:
        return groups

    strata_name = NAME_LEVEL_SEP.join(available)

    # Get unique combinations
    combos = df.select(available).unique().sort(available)
    for combo_row in combos.iter_rows(named=True):
        level_parts = [str(combo_row[c]) for c in available]
        strata_level = NAME_LEVEL_SEP.join(level_parts)

        # Build filter expression
        expr = pl.lit(True)
        for c in available:
            expr = expr & (pl.col(c) == combo_row[c])

        groups.append((strata_name, strata_level, expr))

    return groups


# ---------------------------------------------------------------------------
# Cohort resolution helpers
# ---------------------------------------------------------------------------


def _resolve_cohort(cdm: CdmReference, table_name: str) -> CohortTable:
    """Resolve a cohort table name to a CohortTable."""
    ct = cdm[table_name]
    if not isinstance(ct, CohortTable):
        msg = f"Table '{table_name}' is not a CohortTable"
        raise TypeError(msg)
    return ct


def _get_cohort_ids(ct: CohortTable, ids: int | list[int] | None) -> list[int]:
    """Get cohort IDs to iterate over."""
    if ids is not None:
        return [ids] if isinstance(ids, int) else list(ids)
    return ct.settings["cohort_definition_id"].to_list()


def _cohort_name(ct: CohortTable, cohort_id: int) -> str:
    """Get the name for a cohort definition ID."""
    settings = ct.settings
    match = settings.filter(pl.col("cohort_definition_id") == cohort_id)
    if len(match) > 0:
        return str(match["cohort_name"][0])
    return f"cohort_{cohort_id}"


def _filter_cohort_table(ct: CohortTable, cohort_id: int) -> CohortTable:
    """Filter a CohortTable to a single cohort definition ID."""
    from omopy.profiles._demographics import _get_ibis_table
    import ibis as _ibis

    tbl = _get_ibis_table(ct)
    filtered = tbl.filter(tbl["cohort_definition_id"] == _ibis.literal(cohort_id))
    return ct._with_data(filtered)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt(val: Any) -> str:
    """Format a value for estimate_value column."""
    if val is None:
        return ""
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return ""
        return f"{val:.10g}"
    return str(val)


def _rows_to_df(rows: list[dict[str, str]]) -> pl.DataFrame:
    """Convert list of row dicts to a polars DataFrame with SR columns."""
    if not rows:
        return _empty_data()
    df = pl.DataFrame(rows)
    # Ensure result_id is Int64
    return df.with_columns(pl.col("result_id").cast(pl.Int64))


def _empty_data() -> pl.DataFrame:
    """Create an empty DataFrame with SummarisedResult columns."""
    return pl.DataFrame(
        {col: pl.Series([], dtype=pl.Utf8) for col in SUMMARISED_RESULT_COLUMNS}
    ).with_columns(pl.col("result_id").cast(pl.Int64))


def _empty_settings() -> pl.DataFrame:
    """Create an empty settings DataFrame."""
    return pl.DataFrame(
        {
            "result_id": pl.Series([], dtype=pl.Int64),
            "result_type": pl.Series([], dtype=pl.Utf8),
            "package_name": pl.Series([], dtype=pl.Utf8),
            "package_version": pl.Series([], dtype=pl.Utf8),
        }
    )
