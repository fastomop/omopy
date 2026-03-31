"""Denominator cohort generation for incidence and prevalence analyses.

Implements ``generate_denominator_cohort_set()`` and
``generate_target_denominator_cohort_set()`` — the entry-point functions
that create population-level or target-scoped denominator cohorts from
observation period data.
"""

from __future__ import annotations

import datetime
import itertools
from typing import Any, Literal

import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cohort_table import CohortTable
from omopy.profiles._demographics import _get_ibis_table

__all__ = [
    "generate_denominator_cohort_set",
    "generate_target_denominator_cohort_set",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PACKAGE_NAME = "omopy.incidence"
_PACKAGE_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_denominator_cohort_set(
    cdm: CdmReference,
    name: str = "denominator",
    *,
    cohort_date_range: tuple[datetime.date | None, datetime.date | None] = (None, None),
    age_group: list[tuple[int, int]] | None = None,
    sex: list[Literal["Both", "Male", "Female"]]
    | Literal["Both", "Male", "Female"] = "Both",
    days_prior_observation: int | list[int] = 0,
    requirement_interactions: bool = True,
) -> CdmReference:
    """Generate denominator cohorts from the general population.

    Creates one or more denominator cohorts based on observation periods,
    optionally stratified by age group, sex, and required prior observation.
    When ``requirement_interactions`` is ``True``, every combination of
    the supplied criteria generates a separate cohort.

    Parameters
    ----------
    cdm
        CDM reference containing ``person`` and ``observation_period`` tables.
    name
        Name for the output cohort table in the CDM.
    cohort_date_range
        ``(start_date, end_date)`` study window. ``None`` values use the
        earliest/latest observation dates in the database.
    age_group
        List of ``(min_age, max_age)`` tuples. Default ``[(0, 150)]``.
    sex
        ``"Both"``, ``"Male"``, ``"Female"``, or a list of these.
    days_prior_observation
        Required days of prior observation. Integer or list of integers.
    requirement_interactions
        If ``True``, create cohorts for all combinations of criteria.
        If ``False``, criteria are applied independently.

    Returns
    -------
    CdmReference
        The CDM with the new denominator cohort table attached.
    """
    # Normalise inputs
    if age_group is None:
        age_group = [(0, 150)]
    if isinstance(sex, str):
        sex = [sex]
    if isinstance(days_prior_observation, int):
        days_prior_observation = [days_prior_observation]

    # Build specification combos
    specs = _build_specs(
        age_group, sex, days_prior_observation, requirement_interactions
    )

    # Get person + observation_period
    person_tbl = _get_ibis_table(cdm["person"])
    obs_tbl = _get_ibis_table(cdm["observation_period"])

    # Resolve study window
    study_start, study_end = _resolve_study_window(obs_tbl, cohort_date_range)

    # Build denominator cohort
    all_cohort_rows: list[pl.DataFrame] = []
    all_attrition: list[pl.DataFrame] = []

    for spec in specs:
        cohort_df, attrition_df = _build_general_denominator(
            person_tbl=person_tbl,
            obs_tbl=obs_tbl,
            study_start=study_start,
            study_end=study_end,
            cohort_id=spec["cohort_definition_id"],
            age_min=spec["age_min"],
            age_max=spec["age_max"],
            sex_filter=spec["sex"],
            days_prior=spec["days_prior_observation"],
        )
        all_cohort_rows.append(cohort_df)
        all_attrition.append(attrition_df)

    # Concatenate results
    cohort_data = pl.concat(all_cohort_rows) if all_cohort_rows else _empty_cohort_df()
    attrition_data = (
        pl.concat(all_attrition) if all_attrition else _empty_attrition_df()
    )

    # Build settings
    settings_df = _build_settings(specs)

    cohort_table = CohortTable(
        data=cohort_data,
        tbl_name=name,
        tbl_source="local",
        settings=settings_df,
        attrition=attrition_data,
    )
    cdm[name] = cohort_table
    return cdm


def generate_target_denominator_cohort_set(
    cdm: CdmReference,
    name: str = "denominator",
    *,
    target_cohort_table: str,
    target_cohort_id: int | list[int] | None = None,
    cohort_date_range: tuple[datetime.date | None, datetime.date | None] = (None, None),
    time_at_risk: tuple[int, float] | list[tuple[int, float]] | None = None,
    age_group: list[tuple[int, int]] | None = None,
    sex: list[Literal["Both", "Male", "Female"]]
    | Literal["Both", "Male", "Female"] = "Both",
    days_prior_observation: int | list[int] = 0,
    requirements_at_entry: bool = True,
    requirement_interactions: bool = True,
) -> CdmReference:
    """Generate denominator cohorts scoped to a target cohort.

    Like :func:`generate_denominator_cohort_set` but restricts time
    contribution to when a person is in a target cohort, with optional
    time-at-risk windows relative to target cohort entry.

    Parameters
    ----------
    cdm
        CDM reference.
    name
        Name for the output cohort table.
    target_cohort_table
        Name of an existing cohort table in the CDM to use as target.
    target_cohort_id
        Which cohort IDs from the target table to use. ``None`` = all.
    cohort_date_range
        Study window.
    time_at_risk
        ``(start_offset, end_offset)`` in days relative to target cohort
        entry. ``float('inf')`` for the end means use observation end.
        Can be a list for multiple windows.
    age_group, sex, days_prior_observation
        Stratification criteria.
    requirements_at_entry
        If ``True``, age/prior observation criteria must be met at
        target cohort start. If ``False``, contribution starts once
        criteria are met during follow-up.
    requirement_interactions
        Cross-product of all criteria?

    Returns
    -------
    CdmReference
        The CDM with the new denominator cohort table attached.
    """
    # Normalise inputs
    if age_group is None:
        age_group = [(0, 150)]
    if isinstance(sex, str):
        sex = [sex]
    if isinstance(days_prior_observation, int):
        days_prior_observation = [days_prior_observation]
    if time_at_risk is None:
        time_at_risk = [(0, float("inf"))]
    elif isinstance(time_at_risk, tuple):
        time_at_risk = [time_at_risk]

    # Get target cohort
    target_ct = cdm[target_cohort_table]
    if not isinstance(target_ct, CohortTable):
        msg = f"'{target_cohort_table}' is not a CohortTable in the CDM"
        raise TypeError(msg)

    target_data = target_ct.collect()

    # Filter to requested target cohort IDs
    if target_cohort_id is not None:
        if isinstance(target_cohort_id, int):
            target_cohort_id = [target_cohort_id]
        target_data = target_data.filter(
            pl.col("cohort_definition_id").is_in(target_cohort_id)
        )

    # Build specification combos (including time_at_risk)
    specs = _build_target_specs(
        age_group, sex, days_prior_observation, time_at_risk, requirement_interactions
    )

    person_tbl = _get_ibis_table(cdm["person"])
    obs_tbl = _get_ibis_table(cdm["observation_period"])
    study_start, study_end = _resolve_study_window(obs_tbl, cohort_date_range)

    all_cohort_rows: list[pl.DataFrame] = []
    all_attrition: list[pl.DataFrame] = []

    for spec in specs:
        cohort_df, attrition_df = _build_target_denominator(
            person_tbl=person_tbl,
            obs_tbl=obs_tbl,
            target_data=target_data,
            study_start=study_start,
            study_end=study_end,
            cohort_id=spec["cohort_definition_id"],
            age_min=spec["age_min"],
            age_max=spec["age_max"],
            sex_filter=spec["sex"],
            days_prior=spec["days_prior_observation"],
            tar_start=spec["time_at_risk_start"],
            tar_end=spec["time_at_risk_end"],
            requirements_at_entry=requirements_at_entry,
        )
        all_cohort_rows.append(cohort_df)
        all_attrition.append(attrition_df)

    cohort_data = pl.concat(all_cohort_rows) if all_cohort_rows else _empty_cohort_df()
    attrition_data = (
        pl.concat(all_attrition) if all_attrition else _empty_attrition_df()
    )

    settings_df = _build_target_settings(specs, target_cohort_table)

    cohort_table = CohortTable(
        data=cohort_data,
        tbl_name=name,
        tbl_source="local",
        settings=settings_df,
        attrition=attrition_data,
    )
    cdm[name] = cohort_table
    return cdm


# ---------------------------------------------------------------------------
# Specification builders
# ---------------------------------------------------------------------------


def _build_specs(
    age_groups: list[tuple[int, int]],
    sexes: list[str],
    days_prior_list: list[int],
    interactions: bool,
) -> list[dict[str, Any]]:
    """Build a list of denominator specifications.

    Each spec defines one cohort with its age range, sex, and prior obs.
    """
    if interactions:
        combos = list(itertools.product(age_groups, sexes, days_prior_list))
    else:
        # Non-interaction: one cohort per unique criterion value
        combos = []
        seen: set[tuple[Any, ...]] = set()
        for ag in age_groups:
            key = (ag, "Both", 0)
            if key not in seen:
                combos.append(key)
                seen.add(key)
        for s in sexes:
            key = ((0, 150), s, 0)
            if key not in seen:
                combos.append(key)
                seen.add(key)
        for dp in days_prior_list:
            key = ((0, 150), "Both", dp)
            if key not in seen:
                combos.append(key)
                seen.add(key)

    specs = []
    for i, (ag, s, dp) in enumerate(combos, start=1):
        specs.append(
            {
                "cohort_definition_id": i,
                "age_min": ag[0],
                "age_max": ag[1],
                "sex": s,
                "days_prior_observation": dp,
            }
        )
    return specs


def _build_target_specs(
    age_groups: list[tuple[int, int]],
    sexes: list[str],
    days_prior_list: list[int],
    time_at_risk_list: list[tuple[int, float]],
    interactions: bool,
) -> list[dict[str, Any]]:
    """Build specifications for target-based denominators."""
    if interactions:
        combos = list(
            itertools.product(age_groups, sexes, days_prior_list, time_at_risk_list)
        )
    else:
        combos = []
        seen: set[tuple[Any, ...]] = set()
        base_ag = (0, 150)
        base_sex = "Both"
        base_dp = 0
        base_tar = time_at_risk_list[0]
        for ag in age_groups:
            key = (ag, base_sex, base_dp, base_tar)
            if key not in seen:
                combos.append(key)
                seen.add(key)
        for s in sexes:
            key = (base_ag, s, base_dp, base_tar)
            if key not in seen:
                combos.append(key)
                seen.add(key)
        for dp in days_prior_list:
            key = (base_ag, base_sex, dp, base_tar)
            if key not in seen:
                combos.append(key)
                seen.add(key)
        for tar in time_at_risk_list:
            key = (base_ag, base_sex, base_dp, tar)
            if key not in seen:
                combos.append(key)
                seen.add(key)

    specs = []
    for i, (ag, s, dp, tar) in enumerate(combos, start=1):
        specs.append(
            {
                "cohort_definition_id": i,
                "age_min": ag[0],
                "age_max": ag[1],
                "sex": s,
                "days_prior_observation": dp,
                "time_at_risk_start": tar[0],
                "time_at_risk_end": tar[1],
            }
        )
    return specs


# ---------------------------------------------------------------------------
# Settings builders
# ---------------------------------------------------------------------------


def _build_settings(specs: list[dict[str, Any]]) -> pl.DataFrame:
    """Build the settings DataFrame for general denominators."""
    rows = []
    for spec in specs:
        age_label = f"{spec['age_min']} to {spec['age_max']}"
        cohort_name = (
            f"{age_label}; {spec['sex']}; {spec['days_prior_observation']} prior obs"
        )
        rows.append(
            {
                "cohort_definition_id": spec["cohort_definition_id"],
                "cohort_name": cohort_name,
                "age_group": age_label,
                "sex": spec["sex"],
                "days_prior_observation": spec["days_prior_observation"],
            }
        )
    return pl.DataFrame(rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "days_prior_observation": pl.Int64,
        }
    )


def _build_target_settings(
    specs: list[dict[str, Any]], target_table: str
) -> pl.DataFrame:
    """Build the settings DataFrame for target-based denominators."""
    rows = []
    for spec in specs:
        age_label = f"{spec['age_min']} to {spec['age_max']}"
        tar_end = (
            "Inf"
            if spec["time_at_risk_end"] == float("inf")
            else str(int(spec["time_at_risk_end"]))
        )
        tar_label = f"{spec['time_at_risk_start']} to {tar_end}"
        cohort_name = (
            f"{age_label}; {spec['sex']}; {spec['days_prior_observation']} prior obs; "
            f"TAR {tar_label}"
        )
        rows.append(
            {
                "cohort_definition_id": spec["cohort_definition_id"],
                "cohort_name": cohort_name,
                "age_group": age_label,
                "sex": spec["sex"],
                "days_prior_observation": spec["days_prior_observation"],
                "time_at_risk": tar_label,
                "target_cohort_table": target_table,
            }
        )
    return pl.DataFrame(rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "days_prior_observation": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------
# Core denominator building (general population)
# ---------------------------------------------------------------------------


def _build_general_denominator(
    *,
    person_tbl: ir.Table,
    obs_tbl: ir.Table,
    study_start: datetime.date,
    study_end: datetime.date,
    cohort_id: int,
    age_min: int,
    age_max: int,
    sex_filter: str,
    days_prior: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build a single general-population denominator cohort.

    Returns (cohort_df, attrition_df).
    """
    # Step 1: Get all persons with their observation periods
    persons = person_tbl.select(
        "person_id",
        "year_of_birth",
        "month_of_birth",
        "day_of_birth",
        "gender_concept_id",
    )
    obs = obs_tbl.select(
        "person_id",
        "observation_period_start_date",
        "observation_period_end_date",
    )

    joined = obs.join(persons, "person_id")

    # Materialize to Polars for date manipulation
    df = _ibis_to_polars(joined)

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id,
            [
                ("Qualifying population", 0, 0, 0, 0),
            ],
        )

    # Attrition tracking
    initial_records = len(df)
    initial_subjects = df["person_id"].n_unique()
    attrition_steps: list[tuple[str, int, int, int, int]] = []

    # Step 2: Clip observation periods to study window
    df = df.with_columns(
        pl.col("observation_period_start_date")
        .clip(lower_bound=study_start)
        .alias("cohort_start_date"),
        pl.col("observation_period_end_date")
        .clip(upper_bound=study_end)
        .alias("cohort_end_date"),
    ).filter(pl.col("cohort_start_date") <= pl.col("cohort_end_date"))

    after_clip_records = len(df)
    after_clip_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    attrition_steps.append(
        (
            "Qualifying population",
            after_clip_records,
            after_clip_subjects,
            initial_records - after_clip_records,
            initial_subjects - after_clip_subjects,
        )
    )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Step 3: Apply sex filter
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if sex_filter != "Both":
        sex_concept = 8507 if sex_filter == "Male" else 8532
        df = df.filter(pl.col("gender_concept_id") == sex_concept)

    sex_records = len(df)
    sex_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if sex_filter != "Both":
        attrition_steps.append(
            (
                f"Sex requirement: {sex_filter}",
                sex_records,
                sex_subjects,
                prev_records - sex_records,
                prev_subjects - sex_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Step 4: Apply prior observation requirement
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if days_prior > 0:
        df = df.with_columns(
            (pl.col("cohort_start_date") - pl.col("observation_period_start_date"))
            .dt.total_days()
            .alias("_prior_obs_days")
        )
        # Shift entry forward if needed
        df = (
            df.with_columns(
                pl.when(pl.col("_prior_obs_days") < days_prior)
                .then(
                    pl.col("observation_period_start_date")
                    + pl.duration(days=days_prior)
                )
                .otherwise(pl.col("cohort_start_date"))
                .alias("cohort_start_date")
            )
            .filter(pl.col("cohort_start_date") <= pl.col("cohort_end_date"))
            .drop("_prior_obs_days")
        )

    prior_records = len(df)
    prior_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if days_prior > 0:
        attrition_steps.append(
            (
                f"Prior observation >= {days_prior} days",
                prior_records,
                prior_subjects,
                prev_records - prior_records,
                prev_subjects - prior_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Step 5: Apply age restriction
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if age_min > 0 or age_max < 150:
        df = _apply_age_restriction(df, age_min, age_max)

    age_records = len(df)
    age_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if age_min > 0 or age_max < 150:
        attrition_steps.append(
            (
                f"Age requirement: {age_min} to {age_max}",
                age_records,
                age_subjects,
                prev_records - age_records,
                prev_subjects - age_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Step 6: Final cohort construction
    result = df.select(
        pl.lit(cohort_id).cast(pl.Int64).alias("cohort_definition_id"),
        pl.col("person_id").cast(pl.Int64).alias("subject_id"),
        pl.col("cohort_start_date").cast(pl.Date),
        pl.col("cohort_end_date").cast(pl.Date),
    )

    return result, _make_attrition(cohort_id, attrition_steps)


# ---------------------------------------------------------------------------
# Core denominator building (target cohort)
# ---------------------------------------------------------------------------


def _build_target_denominator(
    *,
    person_tbl: ir.Table,
    obs_tbl: ir.Table,
    target_data: pl.DataFrame,
    study_start: datetime.date,
    study_end: datetime.date,
    cohort_id: int,
    age_min: int,
    age_max: int,
    sex_filter: str,
    days_prior: int,
    tar_start: int,
    tar_end: float,
    requirements_at_entry: bool,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build a single target-scoped denominator cohort."""
    # Get person data
    persons_df = _ibis_to_polars(
        person_tbl.select(
            "person_id",
            "year_of_birth",
            "month_of_birth",
            "day_of_birth",
            "gender_concept_id",
        )
    )
    obs_df = _ibis_to_polars(
        obs_tbl.select(
            "person_id",
            "observation_period_start_date",
            "observation_period_end_date",
        )
    )

    if target_data.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id,
            [
                ("Qualifying population", 0, 0, 0, 0),
            ],
        )

    # Step 1: Build time-at-risk windows from target cohort entries
    tar = target_data.rename({"subject_id": "person_id"}).select(
        "person_id",
        "cohort_start_date",
        "cohort_end_date",
    )

    # Apply time-at-risk offsets
    if tar_end == float("inf"):
        # Use observation period end as the end
        tar = tar.join(obs_df, on="person_id", how="left")
        tar = tar.with_columns(
            (pl.col("cohort_start_date") + pl.duration(days=tar_start)).alias(
                "denom_start"
            ),
            pl.col("observation_period_end_date").alias("denom_end"),
        ).filter(
            (pl.col("denom_start") >= pl.col("observation_period_start_date"))
            & (pl.col("denom_start") <= pl.col("observation_period_end_date"))
        )
    else:
        tar_end_int = int(tar_end)
        tar = tar.with_columns(
            (pl.col("cohort_start_date") + pl.duration(days=tar_start)).alias(
                "denom_start"
            ),
            (pl.col("cohort_start_date") + pl.duration(days=tar_end_int)).alias(
                "denom_end"
            ),
        )
        # Clip to observation period
        tar = tar.join(obs_df, on="person_id", how="left")
        tar = tar.filter(
            (pl.col("denom_start") >= pl.col("observation_period_start_date"))
            & (pl.col("denom_start") <= pl.col("observation_period_end_date"))
        ).with_columns(
            pl.col("denom_end").clip(upper_bound=pl.col("observation_period_end_date")),
        )

    if tar.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id,
            [
                ("Qualifying population", 0, 0, 0, 0),
            ],
        )

    # Clip to study window
    tar = tar.with_columns(
        pl.col("denom_start").clip(lower_bound=study_start),
        pl.col("denom_end").clip(upper_bound=study_end),
    ).filter(pl.col("denom_start") <= pl.col("denom_end"))

    # Join person data
    df = tar.join(persons_df, on="person_id", how="left")

    initial_records = len(df)
    initial_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    attrition_steps: list[tuple[str, int, int, int, int]] = [
        ("Qualifying population", initial_records, initial_subjects, 0, 0),
    ]

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Apply sex filter
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if sex_filter != "Both":
        sex_concept = 8507 if sex_filter == "Male" else 8532
        df = df.filter(pl.col("gender_concept_id") == sex_concept)

    sex_records = len(df)
    sex_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if sex_filter != "Both":
        attrition_steps.append(
            (
                f"Sex requirement: {sex_filter}",
                sex_records,
                sex_subjects,
                prev_records - sex_records,
                prev_subjects - sex_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Apply prior observation
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if days_prior > 0:
        if requirements_at_entry:
            # Check prior obs at the target cohort start (denom_start)
            df = (
                df.with_columns(
                    (pl.col("denom_start") - pl.col("observation_period_start_date"))
                    .dt.total_days()
                    .alias("_prior_obs_days")
                )
                .filter(pl.col("_prior_obs_days") >= days_prior)
                .drop("_prior_obs_days")
            )
        else:
            # Shift entry forward if needed
            df = df.with_columns(
                (pl.col("denom_start") - pl.col("observation_period_start_date"))
                .dt.total_days()
                .alias("_prior_obs_days")
            )
            df = (
                df.with_columns(
                    pl.when(pl.col("_prior_obs_days") < days_prior)
                    .then(
                        pl.col("observation_period_start_date")
                        + pl.duration(days=days_prior)
                    )
                    .otherwise(pl.col("denom_start"))
                    .alias("denom_start")
                )
                .filter(pl.col("denom_start") <= pl.col("denom_end"))
                .drop("_prior_obs_days")
            )

    prior_records = len(df)
    prior_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if days_prior > 0:
        attrition_steps.append(
            (
                f"Prior observation >= {days_prior} days",
                prior_records,
                prior_subjects,
                prev_records - prior_records,
                prev_subjects - prior_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Apply age restriction
    prev_records = len(df)
    prev_subjects = df["person_id"].n_unique()
    if age_min > 0 or age_max < 150:
        df = _apply_age_restriction_target(df, age_min, age_max, requirements_at_entry)

    age_records = len(df)
    age_subjects = df["person_id"].n_unique() if not df.is_empty() else 0
    if age_min > 0 or age_max < 150:
        attrition_steps.append(
            (
                f"Age requirement: {age_min} to {age_max}",
                age_records,
                age_subjects,
                prev_records - age_records,
                prev_subjects - age_subjects,
            )
        )

    if df.is_empty():
        return _empty_cohort_df_with_id(cohort_id), _make_attrition(
            cohort_id, attrition_steps
        )

    # Final construction
    result = df.select(
        pl.lit(cohort_id).cast(pl.Int64).alias("cohort_definition_id"),
        pl.col("person_id").cast(pl.Int64).alias("subject_id"),
        pl.col("denom_start").cast(pl.Date).alias("cohort_start_date"),
        pl.col("denom_end").cast(pl.Date).alias("cohort_end_date"),
    )

    return result, _make_attrition(cohort_id, attrition_steps)


# ---------------------------------------------------------------------------
# Age restriction helpers
# ---------------------------------------------------------------------------


def _apply_age_restriction(
    df: pl.DataFrame, age_min: int, age_max: int
) -> pl.DataFrame:
    """Restrict denominator cohort entry/exit by age.

    A person enters the day they reach ``age_min`` (or cohort_start_date,
    whichever is later) and exits the day before they exceed ``age_max``
    (or cohort_end_date, whichever is earlier).
    """
    df = _add_birth_date(df)

    # Date the person reaches age_min
    df = df.with_columns(
        _date_at_age(pl.col("_birth_date"), age_min).alias("_age_entry"),
        _date_at_age(pl.col("_birth_date"), age_max + 1).alias("_age_exit_raw"),
    )

    # age_exit is one day before exceeding age_max+1
    df = df.with_columns(
        (pl.col("_age_exit_raw") - pl.duration(days=1)).alias("_age_exit"),
    )

    # Clip cohort dates
    df = (
        df.with_columns(
            pl.max_horizontal("cohort_start_date", "_age_entry").alias(
                "cohort_start_date"
            ),
            pl.min_horizontal("cohort_end_date", "_age_exit").alias("cohort_end_date"),
        )
        .filter(pl.col("cohort_start_date") <= pl.col("cohort_end_date"))
        .drop("_birth_date", "_age_entry", "_age_exit_raw", "_age_exit")
    )

    return df


def _apply_age_restriction_target(
    df: pl.DataFrame, age_min: int, age_max: int, requirements_at_entry: bool
) -> pl.DataFrame:
    """Age restriction for target-based denominators."""
    df = _add_birth_date(df)

    if requirements_at_entry:
        # Age must be within range at denom_start
        df = (
            df.with_columns(
                _compute_age_at_date(
                    pl.col("_birth_date"), pl.col("denom_start")
                ).alias("_age")
            )
            .filter((pl.col("_age") >= age_min) & (pl.col("_age") <= age_max))
            .drop("_age", "_birth_date")
        )
    else:
        # Clip like general population
        df = (
            df.with_columns(
                _date_at_age(pl.col("_birth_date"), age_min).alias("_age_entry"),
                _date_at_age(pl.col("_birth_date"), age_max + 1).alias("_age_exit_raw"),
            )
            .with_columns(
                (pl.col("_age_exit_raw") - pl.duration(days=1)).alias("_age_exit"),
            )
            .with_columns(
                pl.max_horizontal("denom_start", "_age_entry").alias("denom_start"),
                pl.min_horizontal("denom_end", "_age_exit").alias("denom_end"),
            )
            .filter(pl.col("denom_start") <= pl.col("denom_end"))
            .drop("_birth_date", "_age_entry", "_age_exit_raw", "_age_exit")
        )

    return df


def _add_birth_date(df: pl.DataFrame) -> pl.DataFrame:
    """Add a ``_birth_date`` column from year/month/day of birth."""
    return df.with_columns(
        pl.date(
            pl.col("year_of_birth"),
            pl.col("month_of_birth").fill_null(1),
            pl.col("day_of_birth").fill_null(1),
        ).alias("_birth_date")
    )


def _date_at_age(birth_date_expr: pl.Expr, age: int) -> pl.Expr:
    """Return the date when the person reaches the given age (birthday)."""
    return pl.date(
        birth_date_expr.dt.year() + age,
        birth_date_expr.dt.month(),
        birth_date_expr.dt.day(),
    )


def _compute_age_at_date(birth_date_expr: pl.Expr, at_date_expr: pl.Expr) -> pl.Expr:
    """Compute integer age (in years) at a given date."""
    return (
        at_date_expr.dt.year()
        - birth_date_expr.dt.year()
        - (
            (at_date_expr.dt.month() < birth_date_expr.dt.month())
            | (
                (at_date_expr.dt.month() == birth_date_expr.dt.month())
                & (at_date_expr.dt.day() < birth_date_expr.dt.day())
            )
        ).cast(pl.Int32)
    )


# ---------------------------------------------------------------------------
# Study window resolution
# ---------------------------------------------------------------------------


def _resolve_study_window(
    obs_tbl: ir.Table,
    cohort_date_range: tuple[datetime.date | None, datetime.date | None],
) -> tuple[datetime.date, datetime.date]:
    """Resolve the study window from explicit dates or observation data."""
    study_start, study_end = cohort_date_range

    if study_start is not None and study_end is not None:
        return study_start, study_end

    # Need to query the database for bounds
    aggs = []
    if study_start is None:
        aggs.append(obs_tbl["observation_period_start_date"].min().name("min_date"))
    if study_end is None:
        aggs.append(obs_tbl["observation_period_end_date"].max().name("max_date"))

    result = obs_tbl.aggregate(aggs).execute()

    if study_start is None:
        val = result["min_date"].iloc[0]
        study_start = _to_date(val)
    if study_end is None:
        val = result["max_date"].iloc[0]
        study_end = _to_date(val)

    return study_start, study_end


def _to_date(val: Any) -> datetime.date:
    """Convert a pandas/numpy date to a Python date."""
    if isinstance(val, datetime.date):
        return val
    if hasattr(val, "date"):
        return val.date()
    import pandas as pd

    if isinstance(val, pd.Timestamp):
        return val.date()
    return datetime.date.fromisoformat(str(val))


# ---------------------------------------------------------------------------
# Attrition helpers
# ---------------------------------------------------------------------------


def _make_attrition(
    cohort_id: int,
    steps: list[tuple[str, int, int, int, int]],
) -> pl.DataFrame:
    """Build an attrition DataFrame from step tuples.

    Each step: (reason, n_records, n_subjects, excluded_records, excluded_subjects)
    """
    rows = []
    for i, (reason, nr, ns, er, es) in enumerate(steps, start=1):
        rows.append(
            {
                "cohort_definition_id": cohort_id,
                "number_records": nr,
                "number_subjects": ns,
                "reason_id": i,
                "reason": reason,
                "excluded_records": er,
                "excluded_subjects": es,
            }
        )
    if not rows:
        return _empty_attrition_df()
    return pl.DataFrame(rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "number_records": pl.Int64,
            "number_subjects": pl.Int64,
            "reason_id": pl.Int64,
            "excluded_records": pl.Int64,
            "excluded_subjects": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------
# Empty DataFrames
# ---------------------------------------------------------------------------


def _empty_cohort_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        }
    )


def _empty_cohort_df_with_id(cohort_id: int) -> pl.DataFrame:
    return _empty_cohort_df()


def _empty_attrition_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "cohort_definition_id": pl.Int64,
            "number_records": pl.Int64,
            "number_subjects": pl.Int64,
            "reason_id": pl.Int64,
            "reason": pl.Utf8,
            "excluded_records": pl.Int64,
            "excluded_subjects": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------
# Ibis -> Polars helper
# ---------------------------------------------------------------------------


def _ibis_to_polars(tbl: ir.Table) -> pl.DataFrame:
    """Execute an Ibis table expression and return as Polars DataFrame."""
    try:
        return pl.from_arrow(tbl.to_pyarrow())
    except Exception:
        import pandas as pd

        result = tbl.execute()
        if isinstance(result, pd.DataFrame):
            return pl.from_pandas(result)
        return result
