"""Incidence and prevalence estimation.

Implements ``estimate_incidence()``, ``estimate_point_prevalence()``,
and ``estimate_period_prevalence()`` — the core analytical functions
that compute rates and proportions from denominator/outcome cohorts.
"""

from __future__ import annotations

import datetime
import math
from typing import Any, Literal

import polars as pl
from scipy.stats import chi2, norm

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "estimate_incidence",
    "estimate_period_prevalence",
    "estimate_point_prevalence",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PACKAGE_NAME = "omopy.incidence"
_PACKAGE_VERSION = "0.1.0"

_INTERVAL_TYPES = ("weeks", "months", "quarters", "years", "overall")

# Z-score for 95% CI
_Z_975 = float(norm.ppf(0.975))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def estimate_incidence(
    cdm: CdmReference,
    denominator_table: str,
    outcome_table: str,
    *,
    censor_table: str | None = None,
    denominator_cohort_id: int | list[int] | None = None,
    outcome_cohort_id: int | list[int] | None = None,
    censor_cohort_id: int | list[int] | None = None,
    interval: Literal["weeks", "months", "quarters", "years", "overall"] = "years",
    complete_database_intervals: bool = True,
    outcome_washout: int | float = float("inf"),
    repeated_events: bool = False,
    strata: list[str] | None = None,
    include_overall_strata: bool = True,
) -> SummarisedResult:
    """Estimate incidence rates from denominator and outcome cohorts.

    Computes incidence as outcome events per 100,000 person-years with
    exact Poisson confidence intervals.

    Parameters
    ----------
    cdm
        CDM reference.
    denominator_table
        Name of the denominator cohort table.
    outcome_table
        Name of the outcome cohort table.
    censor_table
        Optional censoring cohort table name.
    denominator_cohort_id
        Which denominator cohort IDs to use. ``None`` = all.
    outcome_cohort_id
        Which outcome cohort IDs to use. ``None`` = all.
    censor_cohort_id
        Which censor cohort IDs to use.
    interval
        Time interval for rate calculation.
    complete_database_intervals
        Only include intervals fully captured by database observation.
    outcome_washout
        Days between events. ``float('inf')`` means first event only.
    repeated_events
        Allow multiple events per person.
    strata
        Column names in the denominator table for stratification.
    include_overall_strata
        Include an overall (unstratified) analysis.

    Returns
    -------
    SummarisedResult
        Summarised result with incidence estimates.
    """
    denom_ct, outcome_ct = _resolve_cohorts(
        cdm,
        denominator_table,
        outcome_table,
        denominator_cohort_id,
        outcome_cohort_id,
    )
    censor_df = _resolve_censor(cdm, censor_table, censor_cohort_id)

    denom_ids = _get_cohort_ids(denom_ct, denominator_cohort_id)
    outcome_ids = _get_cohort_ids(outcome_ct, outcome_cohort_id)

    all_rows: list[pl.DataFrame] = []
    all_settings: list[dict[str, Any]] = []
    result_id = 0

    for d_id in denom_ids:
        for o_id in outcome_ids:
            result_id += 1
            denom_df = _filter_cohort(denom_ct, d_id)
            outcome_df = _filter_cohort(outcome_ct, o_id)

            # Get strata columns if available
            strata_cols = strata or []
            strata_groups = _get_strata_groups(
                denom_df, strata_cols, include_overall_strata
            )

            for strata_name, strata_level, strata_mask in strata_groups:
                sub_denom = (
                    denom_df.filter(strata_mask)
                    if strata_mask is not None
                    else denom_df
                )

                rows = _compute_incidence(
                    denom_df=sub_denom,
                    outcome_df=outcome_df,
                    censor_df=censor_df,
                    interval=interval,
                    complete_database_intervals=complete_database_intervals,
                    outcome_washout=outcome_washout,
                    repeated_events=repeated_events,
                    result_id=result_id,
                    cdm_name=cdm.cdm_name,
                    denom_cohort_name=_cohort_name(denom_ct, d_id),
                    outcome_cohort_name=_cohort_name(outcome_ct, o_id),
                    strata_name=strata_name,
                    strata_level=strata_level,
                )
                all_rows.append(rows)

            all_settings.append(
                {
                    "result_id": result_id,
                    "result_type": "incidence",
                    "package_name": _PACKAGE_NAME,
                    "package_version": _PACKAGE_VERSION,
                    "denominator_cohort_id": d_id,
                    "outcome_cohort_id": o_id,
                    "interval": interval,
                    "outcome_washout": str(outcome_washout),
                    "repeated_events": str(repeated_events),
                    "complete_database_intervals": str(complete_database_intervals),
                }
            )

    data = pl.concat(all_rows) if all_rows else _empty_summarised_result()
    settings = (
        pl.DataFrame(all_settings).cast({"result_id": pl.Int64})
        if all_settings
        else None
    )
    return SummarisedResult(data, settings=settings)


def estimate_point_prevalence(
    cdm: CdmReference,
    denominator_table: str,
    outcome_table: str,
    *,
    denominator_cohort_id: int | list[int] | None = None,
    outcome_cohort_id: int | list[int] | None = None,
    interval: Literal["weeks", "months", "quarters", "years", "overall"] = "years",
    time_point: Literal["start", "middle", "end"] = "start",
    strata: list[str] | None = None,
    include_overall_strata: bool = True,
) -> SummarisedResult:
    """Estimate point prevalence at a specific time within each interval.

    Counts the proportion of persons with the outcome on a given date
    within each calendar interval.

    Parameters
    ----------
    cdm
        CDM reference.
    denominator_table
        Name of the denominator cohort table.
    outcome_table
        Name of the outcome cohort table.
    denominator_cohort_id, outcome_cohort_id
        Cohort ID filters.
    interval
        Calendar interval.
    time_point
        Where in the interval to measure: ``"start"``, ``"middle"``, or ``"end"``.
    strata
        Stratification columns.
    include_overall_strata
        Include unstratified analysis.

    Returns
    -------
    SummarisedResult
        Summarised result with point prevalence estimates.
    """
    denom_ct, outcome_ct = _resolve_cohorts(
        cdm,
        denominator_table,
        outcome_table,
        denominator_cohort_id,
        outcome_cohort_id,
    )

    denom_ids = _get_cohort_ids(denom_ct, denominator_cohort_id)
    outcome_ids = _get_cohort_ids(outcome_ct, outcome_cohort_id)

    all_rows: list[pl.DataFrame] = []
    all_settings: list[dict[str, Any]] = []
    result_id = 0

    for d_id in denom_ids:
        for o_id in outcome_ids:
            result_id += 1
            denom_df = _filter_cohort(denom_ct, d_id)
            outcome_df = _filter_cohort(outcome_ct, o_id)

            strata_cols = strata or []
            strata_groups = _get_strata_groups(
                denom_df, strata_cols, include_overall_strata
            )

            for strata_name, strata_level, strata_mask in strata_groups:
                sub_denom = (
                    denom_df.filter(strata_mask)
                    if strata_mask is not None
                    else denom_df
                )

                rows = _compute_point_prevalence(
                    denom_df=sub_denom,
                    outcome_df=outcome_df,
                    interval=interval,
                    time_point=time_point,
                    result_id=result_id,
                    cdm_name=cdm.cdm_name,
                    denom_cohort_name=_cohort_name(denom_ct, d_id),
                    outcome_cohort_name=_cohort_name(outcome_ct, o_id),
                    strata_name=strata_name,
                    strata_level=strata_level,
                )
                all_rows.append(rows)

            all_settings.append(
                {
                    "result_id": result_id,
                    "result_type": "point_prevalence",
                    "package_name": _PACKAGE_NAME,
                    "package_version": _PACKAGE_VERSION,
                    "denominator_cohort_id": d_id,
                    "outcome_cohort_id": o_id,
                    "interval": interval,
                    "time_point": time_point,
                }
            )

    data = pl.concat(all_rows) if all_rows else _empty_summarised_result()
    settings = (
        pl.DataFrame(all_settings).cast({"result_id": pl.Int64})
        if all_settings
        else None
    )
    return SummarisedResult(data, settings=settings)


def estimate_period_prevalence(
    cdm: CdmReference,
    denominator_table: str,
    outcome_table: str,
    *,
    denominator_cohort_id: int | list[int] | None = None,
    outcome_cohort_id: int | list[int] | None = None,
    interval: Literal["weeks", "months", "quarters", "years", "overall"] = "years",
    complete_database_intervals: bool = True,
    full_contribution: bool = False,
    strata: list[str] | None = None,
    include_overall_strata: bool = True,
) -> SummarisedResult:
    """Estimate period prevalence over each calendar interval.

    Counts the proportion of persons with any overlap with the outcome
    during each interval, among those contributing time.

    Parameters
    ----------
    cdm
        CDM reference.
    denominator_table
        Name of the denominator cohort table.
    outcome_table
        Name of the outcome cohort table.
    denominator_cohort_id, outcome_cohort_id
        Cohort ID filters.
    interval
        Calendar interval.
    complete_database_intervals
        Only include intervals fully captured by observation.
    full_contribution
        Require the person to be observed for the full interval.
    strata
        Stratification columns.
    include_overall_strata
        Include unstratified analysis.

    Returns
    -------
    SummarisedResult
        Summarised result with period prevalence estimates.
    """
    denom_ct, outcome_ct = _resolve_cohorts(
        cdm,
        denominator_table,
        outcome_table,
        denominator_cohort_id,
        outcome_cohort_id,
    )

    denom_ids = _get_cohort_ids(denom_ct, denominator_cohort_id)
    outcome_ids = _get_cohort_ids(outcome_ct, outcome_cohort_id)

    all_rows: list[pl.DataFrame] = []
    all_settings: list[dict[str, Any]] = []
    result_id = 0

    for d_id in denom_ids:
        for o_id in outcome_ids:
            result_id += 1
            denom_df = _filter_cohort(denom_ct, d_id)
            outcome_df = _filter_cohort(outcome_ct, o_id)

            strata_cols = strata or []
            strata_groups = _get_strata_groups(
                denom_df, strata_cols, include_overall_strata
            )

            for strata_name, strata_level, strata_mask in strata_groups:
                sub_denom = (
                    denom_df.filter(strata_mask)
                    if strata_mask is not None
                    else denom_df
                )

                rows = _compute_period_prevalence(
                    denom_df=sub_denom,
                    outcome_df=outcome_df,
                    interval=interval,
                    complete_database_intervals=complete_database_intervals,
                    full_contribution=full_contribution,
                    result_id=result_id,
                    cdm_name=cdm.cdm_name,
                    denom_cohort_name=_cohort_name(denom_ct, d_id),
                    outcome_cohort_name=_cohort_name(outcome_ct, o_id),
                    strata_name=strata_name,
                    strata_level=strata_level,
                )
                all_rows.append(rows)

            all_settings.append(
                {
                    "result_id": result_id,
                    "result_type": "period_prevalence",
                    "package_name": _PACKAGE_NAME,
                    "package_version": _PACKAGE_VERSION,
                    "denominator_cohort_id": d_id,
                    "outcome_cohort_id": o_id,
                    "interval": interval,
                    "complete_database_intervals": str(complete_database_intervals),
                    "full_contribution": str(full_contribution),
                }
            )

    data = pl.concat(all_rows) if all_rows else _empty_summarised_result()
    settings = (
        pl.DataFrame(all_settings).cast({"result_id": pl.Int64})
        if all_settings
        else None
    )
    return SummarisedResult(data, settings=settings)


# ---------------------------------------------------------------------------
# Incidence computation engine
# ---------------------------------------------------------------------------


def _compute_incidence(
    *,
    denom_df: pl.DataFrame,
    outcome_df: pl.DataFrame,
    censor_df: pl.DataFrame | None,
    interval: str,
    complete_database_intervals: bool,
    outcome_washout: int | float,
    repeated_events: bool,
    result_id: int,
    cdm_name: str,
    denom_cohort_name: str,
    outcome_cohort_name: str,
    strata_name: str,
    strata_level: str,
) -> pl.DataFrame:
    """Compute incidence for one denominator × outcome combination."""
    if denom_df.is_empty():
        return _empty_summarised_result()

    # Apply censoring to denominator
    if censor_df is not None and not censor_df.is_empty():
        denom_df = _apply_censoring(denom_df, censor_df)

    # Generate calendar intervals
    intervals = _generate_intervals(denom_df, interval)

    if intervals.is_empty():
        return _empty_summarised_result()

    # Filter to complete database intervals if requested
    if complete_database_intervals and interval != "overall":
        intervals = _filter_complete_intervals(intervals, denom_df)

    if intervals.is_empty():
        return _empty_summarised_result()

    # Prepare outcome events
    outcome_events = (
        outcome_df.rename({"subject_id": "person_id"})
        .select("person_id", "cohort_start_date")
        .rename({"cohort_start_date": "outcome_date"})
    )

    # Apply washout logic
    if not repeated_events or outcome_washout != float("inf"):
        outcome_events = _apply_washout(
            outcome_events, outcome_washout, repeated_events
        )

    # For each interval, compute person-time and events
    rows: list[dict[str, Any]] = []
    denom_persons = denom_df.rename({"subject_id": "person_id"})

    for row in intervals.iter_rows(named=True):
        int_start = row["interval_start"]
        int_end = row["interval_end"]
        int_label = row["interval_label"]

        # Person-time: overlap of denominator periods with interval
        pt_df = (
            denom_persons.filter(
                (pl.col("cohort_start_date") <= int_end)
                & (pl.col("cohort_end_date") >= int_start)
            )
            .with_columns(
                pl.col("cohort_start_date")
                .clip(lower_bound=int_start)
                .alias("_pt_start"),
                pl.col("cohort_end_date").clip(upper_bound=int_end).alias("_pt_end"),
            )
            .with_columns(
                ((pl.col("_pt_end") - pl.col("_pt_start")).dt.total_days() + 1).alias(
                    "_days"
                )
            )
        )

        n_persons = pt_df["person_id"].n_unique() if not pt_df.is_empty() else 0
        person_days = int(pt_df["_days"].sum()) if not pt_df.is_empty() else 0
        person_years = person_days / 365.25

        # Events in this interval
        at_risk_persons = (
            set(pt_df["person_id"].to_list()) if not pt_df.is_empty() else set()
        )
        events_in_interval = outcome_events.filter(
            (pl.col("outcome_date") >= int_start)
            & (pl.col("outcome_date") <= int_end)
            & pl.col("person_id").is_in(list(at_risk_persons))
        )
        n_events = len(events_in_interval)
        (events_in_interval["person_id"].n_unique() if n_events > 0 else 0)

        # Incidence rate per 100,000 person-years
        if person_years > 0:
            ir_val = (n_events / person_years) * 100_000
            ir_lower, ir_upper = _poisson_ci(n_events, person_years)
        else:
            ir_val = 0.0
            ir_lower = 0.0
            ir_upper = 0.0

        # Build result rows for this interval
        estimates = [
            ("n_persons", "integer", str(n_persons)),
            ("person_days", "integer", str(person_days)),
            ("person_years", "numeric", f"{person_years:.4f}"),
            ("n_events", "integer", str(n_events)),
            ("incidence_100000_pys", "numeric", f"{ir_val:.4f}"),
            ("incidence_100000_pys_95ci_lower", "numeric", f"{ir_lower:.4f}"),
            ("incidence_100000_pys_95ci_upper", "numeric", f"{ir_upper:.4f}"),
        ]

        for est_name, est_type, est_val in estimates:
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "denominator_cohort_name"
                    + NAME_LEVEL_SEP
                    + "outcome_cohort_name",
                    "group_level": denom_cohort_name
                    + NAME_LEVEL_SEP
                    + outcome_cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "incidence",
                    "variable_level": int_label,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_val,
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )

    if not rows:
        return _empty_summarised_result()

    return pl.DataFrame(rows).cast({"result_id": pl.Int64})


# ---------------------------------------------------------------------------
# Point prevalence computation
# ---------------------------------------------------------------------------


def _compute_point_prevalence(
    *,
    denom_df: pl.DataFrame,
    outcome_df: pl.DataFrame,
    interval: str,
    time_point: str,
    result_id: int,
    cdm_name: str,
    denom_cohort_name: str,
    outcome_cohort_name: str,
    strata_name: str,
    strata_level: str,
) -> pl.DataFrame:
    """Compute point prevalence for one denominator × outcome combination."""
    if denom_df.is_empty():
        return _empty_summarised_result()

    intervals = _generate_intervals(denom_df, interval)
    if intervals.is_empty():
        return _empty_summarised_result()

    denom_persons = denom_df.rename({"subject_id": "person_id"})
    outcome_persons = outcome_df.rename({"subject_id": "person_id"})

    rows: list[dict[str, Any]] = []

    for row in intervals.iter_rows(named=True):
        int_start = row["interval_start"]
        int_end = row["interval_end"]
        int_label = row["interval_label"]

        # Determine the point date
        point_date = _get_time_point(int_start, int_end, time_point)

        # Denominator: persons observed on the point date
        denom_at_point = denom_persons.filter(
            (pl.col("cohort_start_date") <= point_date)
            & (pl.col("cohort_end_date") >= point_date)
        )
        n_denom = (
            denom_at_point["person_id"].n_unique()
            if not denom_at_point.is_empty()
            else 0
        )

        if n_denom == 0:
            continue

        # Numerator: persons with outcome on the point date
        denom_person_ids = set(denom_at_point["person_id"].to_list())
        outcome_at_point = outcome_persons.filter(
            (pl.col("cohort_start_date") <= point_date)
            & (pl.col("cohort_end_date") >= point_date)
            & pl.col("person_id").is_in(list(denom_person_ids))
        )
        n_outcome = (
            outcome_at_point["person_id"].n_unique()
            if not outcome_at_point.is_empty()
            else 0
        )

        # Prevalence
        prevalence = n_outcome / n_denom if n_denom > 0 else 0.0
        prev_lower, prev_upper = _wilson_ci(n_outcome, n_denom)

        estimates = [
            ("n_persons", "integer", str(n_denom)),
            ("n_cases", "integer", str(n_outcome)),
            ("prevalence", "numeric", f"{prevalence:.6f}"),
            ("prevalence_95ci_lower", "numeric", f"{prev_lower:.6f}"),
            ("prevalence_95ci_upper", "numeric", f"{prev_upper:.6f}"),
        ]

        for est_name, est_type, est_val in estimates:
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "denominator_cohort_name"
                    + NAME_LEVEL_SEP
                    + "outcome_cohort_name",
                    "group_level": denom_cohort_name
                    + NAME_LEVEL_SEP
                    + outcome_cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "point_prevalence",
                    "variable_level": int_label,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_val,
                    "additional_name": "time_point",
                    "additional_level": time_point,
                }
            )

    if not rows:
        return _empty_summarised_result()

    return pl.DataFrame(rows).cast({"result_id": pl.Int64})


# ---------------------------------------------------------------------------
# Period prevalence computation
# ---------------------------------------------------------------------------


def _compute_period_prevalence(
    *,
    denom_df: pl.DataFrame,
    outcome_df: pl.DataFrame,
    interval: str,
    complete_database_intervals: bool,
    full_contribution: bool,
    result_id: int,
    cdm_name: str,
    denom_cohort_name: str,
    outcome_cohort_name: str,
    strata_name: str,
    strata_level: str,
) -> pl.DataFrame:
    """Compute period prevalence for one denominator × outcome combination."""
    if denom_df.is_empty():
        return _empty_summarised_result()

    intervals = _generate_intervals(denom_df, interval)
    if intervals.is_empty():
        return _empty_summarised_result()

    if complete_database_intervals and interval != "overall":
        intervals = _filter_complete_intervals(intervals, denom_df)
    if intervals.is_empty():
        return _empty_summarised_result()

    denom_persons = denom_df.rename({"subject_id": "person_id"})
    outcome_persons = outcome_df.rename({"subject_id": "person_id"})

    rows: list[dict[str, Any]] = []

    for row in intervals.iter_rows(named=True):
        int_start = row["interval_start"]
        int_end = row["interval_end"]
        int_label = row["interval_label"]

        # Denominator: persons contributing to this interval
        denom_in_interval = denom_persons.filter(
            (pl.col("cohort_start_date") <= int_end)
            & (pl.col("cohort_end_date") >= int_start)
        )

        if full_contribution:
            # Require observation for the full interval
            denom_in_interval = denom_in_interval.filter(
                (pl.col("cohort_start_date") <= int_start)
                & (pl.col("cohort_end_date") >= int_end)
            )

        n_denom = (
            denom_in_interval["person_id"].n_unique()
            if not denom_in_interval.is_empty()
            else 0
        )

        if n_denom == 0:
            continue

        # Numerator: persons with any outcome overlap during the interval
        denom_person_ids = set(denom_in_interval["person_id"].to_list())
        outcome_in_interval = outcome_persons.filter(
            (pl.col("cohort_start_date") <= int_end)
            & (pl.col("cohort_end_date") >= int_start)
            & pl.col("person_id").is_in(list(denom_person_ids))
        )
        n_outcome = (
            outcome_in_interval["person_id"].n_unique()
            if not outcome_in_interval.is_empty()
            else 0
        )

        prevalence = n_outcome / n_denom if n_denom > 0 else 0.0
        prev_lower, prev_upper = _wilson_ci(n_outcome, n_denom)

        estimates = [
            ("n_persons", "integer", str(n_denom)),
            ("n_cases", "integer", str(n_outcome)),
            ("prevalence", "numeric", f"{prevalence:.6f}"),
            ("prevalence_95ci_lower", "numeric", f"{prev_lower:.6f}"),
            ("prevalence_95ci_upper", "numeric", f"{prev_upper:.6f}"),
        ]

        for est_name, est_type, est_val in estimates:
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "denominator_cohort_name"
                    + NAME_LEVEL_SEP
                    + "outcome_cohort_name",
                    "group_level": denom_cohort_name
                    + NAME_LEVEL_SEP
                    + outcome_cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "period_prevalence",
                    "variable_level": int_label,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_val,
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )

    if not rows:
        return _empty_summarised_result()

    return pl.DataFrame(rows).cast({"result_id": pl.Int64})


# ---------------------------------------------------------------------------
# Calendar interval generation
# ---------------------------------------------------------------------------


def _generate_intervals(denom_df: pl.DataFrame, interval: str) -> pl.DataFrame:
    """Generate calendar intervals covering the denominator cohort span."""
    min_date = denom_df.select(pl.col("cohort_start_date").min()).item()
    max_date = denom_df.select(pl.col("cohort_end_date").max()).item()

    if min_date is None or max_date is None:
        return pl.DataFrame(
            schema={
                "interval_start": pl.Date,
                "interval_end": pl.Date,
                "interval_label": pl.Utf8,
            }
        )

    if isinstance(min_date, datetime.datetime):
        min_date = min_date.date()
    if isinstance(max_date, datetime.datetime):
        max_date = max_date.date()

    if interval == "overall":
        return pl.DataFrame(
            {
                "interval_start": [min_date],
                "interval_end": [max_date],
                "interval_label": ["overall"],
            }
        ).cast({"interval_start": pl.Date, "interval_end": pl.Date})

    starts: list[datetime.date] = []
    ends: list[datetime.date] = []
    labels: list[str] = []

    current = _interval_start(min_date, interval)

    while current <= max_date:
        end = _interval_end(current, interval)
        starts.append(current)
        ends.append(end)
        labels.append(_interval_label(current, interval))
        current = _next_interval_start(current, interval)

    if not starts:
        return pl.DataFrame(
            schema={
                "interval_start": pl.Date,
                "interval_end": pl.Date,
                "interval_label": pl.Utf8,
            }
        )

    return pl.DataFrame(
        {
            "interval_start": starts,
            "interval_end": ends,
            "interval_label": labels,
        }
    ).cast({"interval_start": pl.Date, "interval_end": pl.Date})


def _interval_start(d: datetime.date, interval: str) -> datetime.date:
    """Get the start of the interval containing date *d*."""
    if interval == "years":
        return datetime.date(d.year, 1, 1)
    if interval == "quarters":
        q = (d.month - 1) // 3
        return datetime.date(d.year, q * 3 + 1, 1)
    if interval == "months":
        return datetime.date(d.year, d.month, 1)
    if interval == "weeks":
        # ISO week starts on Monday
        return d - datetime.timedelta(days=d.weekday())
    return d


def _interval_end(start: datetime.date, interval: str) -> datetime.date:
    """Get the last day of the interval starting at *start*."""
    if interval == "years":
        return datetime.date(start.year, 12, 31)
    if interval == "quarters":
        month = start.month + 2
        if month == 12:
            return datetime.date(start.year, 12, 31)
        return datetime.date(start.year, month + 1, 1) - datetime.timedelta(days=1)
    if interval == "months":
        if start.month == 12:
            return datetime.date(start.year, 12, 31)
        return datetime.date(start.year, start.month + 1, 1) - datetime.timedelta(
            days=1
        )
    if interval == "weeks":
        return start + datetime.timedelta(days=6)
    return start


def _next_interval_start(start: datetime.date, interval: str) -> datetime.date:
    """Get the start of the next interval after the given start."""
    if interval == "years":
        return datetime.date(start.year + 1, 1, 1)
    if interval == "quarters":
        month = start.month + 3
        if month > 12:
            return datetime.date(start.year + 1, month - 12, 1)
        return datetime.date(start.year, month, 1)
    if interval == "months":
        if start.month == 12:
            return datetime.date(start.year + 1, 1, 1)
        return datetime.date(start.year, start.month + 1, 1)
    if interval == "weeks":
        return start + datetime.timedelta(days=7)
    return start + datetime.timedelta(days=1)


def _interval_label(start: datetime.date, interval: str) -> str:
    """Generate a human-readable label for an interval."""
    if interval == "years":
        return str(start.year)
    if interval == "quarters":
        q = (start.month - 1) // 3 + 1
        return f"{start.year} Q{q}"
    if interval == "months":
        return f"{start.year}-{start.month:02d}"
    if interval == "weeks":
        iso = start.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return str(start)


# ---------------------------------------------------------------------------
# Complete database intervals
# ---------------------------------------------------------------------------


def _filter_complete_intervals(
    intervals: pl.DataFrame, denom_df: pl.DataFrame
) -> pl.DataFrame:
    """Keep only intervals fully captured by the database observation."""
    db_min = denom_df.select(pl.col("cohort_start_date").min()).item()
    db_max = denom_df.select(pl.col("cohort_end_date").max()).item()

    if isinstance(db_min, datetime.datetime):
        db_min = db_min.date()
    if isinstance(db_max, datetime.datetime):
        db_max = db_max.date()

    return intervals.filter(
        (pl.col("interval_start") >= db_min) & (pl.col("interval_end") <= db_max)
    )


# ---------------------------------------------------------------------------
# Washout / censoring / event processing
# ---------------------------------------------------------------------------


def _apply_washout(
    events: pl.DataFrame,
    washout: int | float,
    repeated_events: bool,
) -> pl.DataFrame:
    """Apply outcome washout logic to event data.

    If ``washout == float('inf')`` and ``repeated_events == False``,
    only the first event per person is kept.
    """
    if events.is_empty():
        return events

    # Sort events chronologically per person
    events = events.sort(["person_id", "outcome_date"])

    if not repeated_events and washout == float("inf"):
        # First event only
        return events.group_by("person_id").first()

    if not repeated_events:
        # First event only (washout doesn't matter if no repeated events)
        return events.group_by("person_id").first()

    if washout == float("inf"):
        # Repeated events but infinite washout = only first event
        return events.group_by("person_id").first()

    # Repeated events with finite washout
    # Keep events where the gap from the previous event is >= washout days
    result_rows: list[dict[str, Any]] = []
    for person_id, group in events.group_by("person_id"):
        group = group.sort("outcome_date")
        dates = group["outcome_date"].to_list()
        last_event_date = None
        for d in dates:
            if last_event_date is None:
                result_rows.append({"person_id": person_id[0], "outcome_date": d})
                last_event_date = d
            else:
                gap = (d - last_event_date).days
                if gap >= washout:
                    result_rows.append({"person_id": person_id[0], "outcome_date": d})
                    last_event_date = d

    if not result_rows:
        return events.head(0)

    return pl.DataFrame(result_rows).cast(
        {
            "person_id": events["person_id"].dtype,
            "outcome_date": pl.Date,
        }
    )


def _apply_censoring(denom_df: pl.DataFrame, censor_df: pl.DataFrame) -> pl.DataFrame:
    """Truncate denominator follow-up at censor dates."""
    censor = censor_df.rename({"subject_id": "person_id"}).select(
        "person_id",
        pl.col("cohort_start_date").alias("censor_date"),
    )

    # For each denominator row, find the earliest censor date
    merged = denom_df.join(
        censor.rename({"person_id": "subject_id"}),
        on="subject_id",
        how="left",
    )

    # Only censor if censor_date falls within the denominator period
    merged = (
        merged.with_columns(
            pl.when(
                pl.col("censor_date").is_not_null()
                & (pl.col("censor_date") >= pl.col("cohort_start_date"))
                & (pl.col("censor_date") <= pl.col("cohort_end_date"))
            )
            .then(pl.col("censor_date") - pl.duration(days=1))
            .otherwise(pl.col("cohort_end_date"))
            .alias("cohort_end_date")
        )
        .drop("censor_date")
        .filter(pl.col("cohort_start_date") <= pl.col("cohort_end_date"))
    )

    return merged


# ---------------------------------------------------------------------------
# Time point helpers
# ---------------------------------------------------------------------------


def _get_time_point(
    int_start: datetime.date, int_end: datetime.date, time_point: str
) -> datetime.date:
    """Get the specific date within an interval for point prevalence."""
    if time_point == "start":
        return int_start
    if time_point == "end":
        return int_end
    # middle
    delta = (int_end - int_start).days
    return int_start + datetime.timedelta(days=delta // 2)


# ---------------------------------------------------------------------------
# Confidence intervals
# ---------------------------------------------------------------------------


def _poisson_ci(
    events: int, person_years: float, alpha: float = 0.05
) -> tuple[float, float]:
    """Exact Poisson confidence interval for incidence rate per 100,000 PY.

    Uses the chi-squared method:
        lower = chi2.ppf(alpha/2, 2*events) / (2 * person_years) * 100000
        upper = chi2.ppf(1-alpha/2, 2*(events+1)) / (2 * person_years) * 100000
    """
    if person_years <= 0:
        return 0.0, 0.0

    if events == 0:
        lower = 0.0
        upper = float(chi2.ppf(1 - alpha / 2, 2)) / (2 * person_years) * 100_000
        return lower, upper

    lower = float(chi2.ppf(alpha / 2, 2 * events)) / (2 * person_years) * 100_000
    upper = (
        float(chi2.ppf(1 - alpha / 2, 2 * (events + 1))) / (2 * person_years) * 100_000
    )
    return lower, upper


def _wilson_ci(x: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    """Wilson score confidence interval for a proportion.

    Parameters
    ----------
    x : int
        Number of successes (cases).
    n : int
        Number of trials (population).

    Returns
    -------
    (lower, upper)
    """
    if n == 0:
        return 0.0, 0.0

    p = x / n
    z = _Z_975
    z2 = z * z

    denom = n + z2
    centre = (x + z2 / 2) / denom
    margin = (z * math.sqrt(n)) / denom * math.sqrt(p * (1 - p) + z2 / (4 * n))

    lower = max(0.0, centre - margin)
    upper = min(1.0, centre + margin)
    return lower, upper


# ---------------------------------------------------------------------------
# Strata helpers
# ---------------------------------------------------------------------------


def _get_strata_groups(
    denom_df: pl.DataFrame,
    strata_cols: list[str],
    include_overall: bool,
) -> list[tuple[str, str, pl.Expr | None]]:
    """Build strata group definitions.

    Returns list of (strata_name, strata_level, filter_expr) tuples.
    """
    groups: list[tuple[str, str, pl.Expr | None]] = []

    if include_overall:
        groups.append((OVERALL, OVERALL, None))

    if strata_cols:
        available = [c for c in strata_cols if c in denom_df.columns]
        if available:
            strata_name = NAME_LEVEL_SEP.join(available)
            distinct = denom_df.select(available).unique()
            for row in distinct.iter_rows(named=True):
                level_parts = [str(row[c]) for c in available]
                strata_level = NAME_LEVEL_SEP.join(level_parts)
                mask = pl.lit(True)
                for c in available:
                    mask = mask & (pl.col(c) == row[c])
                groups.append((strata_name, strata_level, mask))

    return groups


# ---------------------------------------------------------------------------
# Cohort resolution helpers
# ---------------------------------------------------------------------------


def _resolve_cohorts(
    cdm: CdmReference,
    denom_name: str,
    outcome_name: str,
    denom_id: int | list[int] | None,
    outcome_id: int | list[int] | None,
) -> tuple[CohortTable, CohortTable]:
    """Resolve denominator and outcome cohort tables from the CDM."""
    denom_ct = cdm[denom_name]
    outcome_ct = cdm[outcome_name]
    if not isinstance(denom_ct, CohortTable):
        msg = f"'{denom_name}' is not a CohortTable"
        raise TypeError(msg)
    if not isinstance(outcome_ct, CohortTable):
        msg = f"'{outcome_name}' is not a CohortTable"
        raise TypeError(msg)
    return denom_ct, outcome_ct


def _get_cohort_ids(ct: CohortTable, cohort_id: int | list[int] | None) -> list[int]:
    """Get the cohort IDs to process."""
    if cohort_id is None:
        return ct.cohort_ids
    if isinstance(cohort_id, int):
        return [cohort_id]
    return list(cohort_id)


def _filter_cohort(ct: CohortTable, cohort_id: int) -> pl.DataFrame:
    """Collect a single cohort from a CohortTable."""
    df = ct.collect()
    return df.filter(pl.col("cohort_definition_id") == cohort_id)


def _cohort_name(ct: CohortTable, cohort_id: int) -> str:
    """Get the cohort name for a given ID."""
    settings = ct.settings
    row = settings.filter(pl.col("cohort_definition_id") == cohort_id)
    if row.is_empty():
        return f"cohort_{cohort_id}"
    return row["cohort_name"][0]


def _resolve_censor(
    cdm: CdmReference,
    censor_table: str | None,
    censor_cohort_id: int | list[int] | None,
) -> pl.DataFrame | None:
    """Resolve optional censoring cohort."""
    if censor_table is None:
        return None
    ct = cdm[censor_table]
    if not isinstance(ct, CohortTable):
        return None
    df = ct.collect()
    if censor_cohort_id is not None:
        if isinstance(censor_cohort_id, int):
            censor_cohort_id = [censor_cohort_id]
        df = df.filter(pl.col("cohort_definition_id").is_in(censor_cohort_id))
    return df


# ---------------------------------------------------------------------------
# Empty result helper
# ---------------------------------------------------------------------------


def _empty_summarised_result() -> pl.DataFrame:
    """Return an empty DataFrame with SummarisedResult columns."""
    from omopy.generics.summarised_result import SUMMARISED_RESULT_COLUMNS

    return pl.DataFrame(
        schema={
            c: pl.Utf8 if c != "result_id" else pl.Int64
            for c in SUMMARISED_RESULT_COLUMNS
        }
    )
