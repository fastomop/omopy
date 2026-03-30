"""Mock CDM generation and episode validation for pregnancy testing.

Provides :func:`mock_pregnancy_cdm` which creates a minimal CDM with
realistic pregnancy-related records, and :func:`validate_episodes` which
checks episode period constraints.
"""

from __future__ import annotations

import datetime
import random
from typing import Any

import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable

from omopy.pregnancy._concepts import (
    HIP_CONCEPTS,
    PPS_CONCEPTS,
    ESD_CONCEPTS,
)

__all__ = ["mock_pregnancy_cdm", "validate_episodes"]


def mock_pregnancy_cdm(
    *,
    seed: int = 42,
    n_persons: int = 20,
) -> CdmReference:
    """Create a mock CDM with pregnancy-related records for testing.

    Generates synthetic data including:

    - ``person`` table (females aged 15–45)
    - ``observation_period`` table
    - ``condition_occurrence`` with pregnancy outcome codes
    - ``procedure_occurrence`` with delivery procedure codes
    - ``measurement`` with gestational age measurements
    - ``observation`` with prenatal observation codes

    Parameters
    ----------
    seed
        Random seed for reproducibility.
    n_persons
        Number of persons to generate.

    Returns
    -------
    CdmReference
        CDM with clinical tables containing pregnancy-related records.
    """
    rng = random.Random(seed)

    # Collect concept IDs by domain/table for realistic assignment
    condition_concepts: list[int] = []
    procedure_concepts: list[int] = []
    measurement_concepts: list[int] = []
    observation_concepts: list[int] = []

    # HIP concepts go to condition or procedure
    for cid, info in HIP_CONCEPTS.items():
        if info["category"] in ("LB", "SB", "SA", "ECT"):
            condition_concepts.append(cid)
        else:
            procedure_concepts.append(cid)

    # PPS concepts go to observation
    for cid in PPS_CONCEPTS:
        observation_concepts.append(cid)

    # ESD concepts go to measurement or condition
    for cid, info in ESD_CONCEPTS.items():
        if info["domain"] == "measurement":
            measurement_concepts.append(cid)
        else:
            condition_concepts.append(cid)

    # Ensure we have at least something in each list
    if not procedure_concepts:
        procedure_concepts = condition_concepts[:2]
    if not observation_concepts:
        observation_concepts = condition_concepts[:2]
    if not measurement_concepts:
        measurement_concepts = [4260747]  # gestational age

    # Generate persons (all female for pregnancy)
    person_rows: list[dict[str, Any]] = []
    obs_period_rows: list[dict[str, Any]] = []

    study_start = datetime.date(2015, 1, 1)
    study_end = datetime.date(2023, 12, 31)
    study_days = (study_end - study_start).days

    for i in range(1, n_persons + 1):
        yob = rng.randint(1980, 2005)
        mob = rng.randint(1, 12)
        dob = rng.randint(1, 28)

        person_rows.append(
            {
                "person_id": i,
                "year_of_birth": yob,
                "month_of_birth": mob,
                "day_of_birth": dob,
                "gender_concept_id": 8532,  # Female
                "race_concept_id": 0,
                "ethnicity_concept_id": 0,
            }
        )

        obs_start_offset = rng.randint(0, study_days // 3)
        obs_end_offset = rng.randint(obs_start_offset + 365, study_days)
        obs_start = study_start + datetime.timedelta(days=obs_start_offset)
        obs_end = study_start + datetime.timedelta(days=min(obs_end_offset, study_days))

        obs_period_rows.append(
            {
                "person_id": i,
                "observation_period_id": i,
                "observation_period_start_date": obs_start,
                "observation_period_end_date": obs_end,
                "period_type_concept_id": 44814724,
            }
        )

    # Generate pregnancy events
    cond_rows: list[dict[str, Any]] = []
    proc_rows: list[dict[str, Any]] = []
    meas_rows: list[dict[str, Any]] = []
    obs_rows: list[dict[str, Any]] = []

    cond_id = 1
    proc_id = 1
    meas_id = 1
    obs_id = 1

    for i in range(1, n_persons + 1):
        obs_start = obs_period_rows[i - 1]["observation_period_start_date"]
        obs_end = obs_period_rows[i - 1]["observation_period_end_date"]
        obs_days = (obs_end - obs_start).days
        if obs_days < 280:
            continue

        # Decide how many pregnancies (0-2)
        n_pregnancies = rng.choices([0, 1, 2], weights=[0.2, 0.5, 0.3])[0]

        current_date = obs_start + datetime.timedelta(days=rng.randint(30, 90))

        for _preg in range(n_pregnancies):
            if (obs_end - current_date).days < 200:
                break

            # Choose outcome type
            outcome_type = rng.choices(
                ["LB", "SB", "SA", "AB", "ECT"],
                weights=[0.6, 0.05, 0.15, 0.1, 0.1],
            )[0]

            # Determine gestation duration based on outcome
            if outcome_type in ("LB", "DELIV"):
                gest_weeks = rng.randint(34, 42)
            elif outcome_type == "SB":
                gest_weeks = rng.randint(20, 40)
            elif outcome_type in ("SA",):
                gest_weeks = rng.randint(4, 20)
            elif outcome_type == "AB":
                gest_weeks = rng.randint(4, 24)
            elif outcome_type == "ECT":
                gest_weeks = rng.randint(4, 12)
            else:
                gest_weeks = rng.randint(20, 40)

            conception_date = current_date
            outcome_date = conception_date + datetime.timedelta(days=gest_weeks * 7)

            if outcome_date > obs_end:
                break

            # Add outcome condition
            outcome_concepts_for_cat = [
                cid for cid, info in HIP_CONCEPTS.items() if info["category"] == outcome_type
            ]
            if outcome_concepts_for_cat:
                chosen_concept = rng.choice(outcome_concepts_for_cat)
                cond_rows.append(
                    {
                        "condition_occurrence_id": cond_id,
                        "person_id": i,
                        "condition_concept_id": chosen_concept,
                        "condition_start_date": outcome_date,
                        "condition_end_date": outcome_date,
                        "condition_type_concept_id": 32817,
                    }
                )
                cond_id += 1

            # Add delivery procedure for LB
            if outcome_type == "LB" and procedure_concepts:
                deliv_concepts = [
                    cid for cid, info in HIP_CONCEPTS.items() if info["category"] == "DELIV"
                ]
                if deliv_concepts:
                    proc_rows.append(
                        {
                            "procedure_occurrence_id": proc_id,
                            "person_id": i,
                            "procedure_concept_id": rng.choice(deliv_concepts),
                            "procedure_date": outcome_date,
                            "procedure_type_concept_id": 32817,
                        }
                    )
                    proc_id += 1

            # Add prenatal observations (PPS concepts)
            n_prenatal = rng.randint(2, 5)
            for visit_idx in range(n_prenatal):
                visit_month = rng.randint(1, max(1, gest_weeks // 4))
                visit_date = conception_date + datetime.timedelta(
                    days=visit_month * 30 + rng.randint(-5, 5)
                )
                if visit_date < obs_start or visit_date > obs_end:
                    continue

                # Pick PPS concept appropriate for the month
                suitable = [
                    cid
                    for cid, info in PPS_CONCEPTS.items()
                    if info["min_month"] <= visit_month <= info["max_month"]
                ]
                if not suitable:
                    suitable = list(PPS_CONCEPTS.keys())

                obs_rows.append(
                    {
                        "observation_id": obs_id,
                        "person_id": i,
                        "observation_concept_id": rng.choice(suitable),
                        "observation_date": visit_date,
                        "observation_type_concept_id": 32817,
                    }
                )
                obs_id += 1

            # Add gestational age measurement (ESD)
            if rng.random() < 0.7:
                ga_date = outcome_date - datetime.timedelta(days=rng.randint(0, 14))
                if ga_date >= obs_start:
                    ga_concepts = [
                        cid for cid, info in ESD_CONCEPTS.items() if info["category"] == "GW"
                    ]
                    if ga_concepts:
                        meas_rows.append(
                            {
                                "measurement_id": meas_id,
                                "person_id": i,
                                "measurement_concept_id": rng.choice(ga_concepts),
                                "measurement_date": ga_date,
                                "measurement_type_concept_id": 32817,
                                "value_as_number": float(gest_weeks),
                                "unit_concept_id": 0,
                            }
                        )
                        meas_id += 1

            # Add trimester conditions (ESD GR3m)
            if rng.random() < 0.5:
                trimester_concepts = [
                    cid for cid, info in ESD_CONCEPTS.items() if info["category"] == "GR3m"
                ]
                if trimester_concepts:
                    tri_month = rng.randint(1, min(9, gest_weeks // 4))
                    tri_date = conception_date + datetime.timedelta(days=tri_month * 30)
                    if obs_start <= tri_date <= obs_end:
                        cond_rows.append(
                            {
                                "condition_occurrence_id": cond_id,
                                "person_id": i,
                                "condition_concept_id": rng.choice(trimester_concepts),
                                "condition_start_date": tri_date,
                                "condition_end_date": tri_date,
                                "condition_type_concept_id": 32817,
                            }
                        )
                        cond_id += 1

            # Move current_date forward past this pregnancy
            current_date = outcome_date + datetime.timedelta(days=rng.randint(180, 400))

    # Build DataFrames
    person_df = pl.DataFrame(person_rows).cast(
        {
            "person_id": pl.Int64,
            "year_of_birth": pl.Int32,
            "month_of_birth": pl.Int32,
            "day_of_birth": pl.Int32,
            "gender_concept_id": pl.Int32,
            "race_concept_id": pl.Int32,
            "ethnicity_concept_id": pl.Int32,
        }
    )
    obs_period_df = pl.DataFrame(obs_period_rows).cast(
        {
            "person_id": pl.Int64,
            "observation_period_id": pl.Int64,
            "observation_period_start_date": pl.Date,
            "observation_period_end_date": pl.Date,
            "period_type_concept_id": pl.Int64,
        }
    )

    cond_schema = {
        "condition_occurrence_id": pl.Int64,
        "person_id": pl.Int64,
        "condition_concept_id": pl.Int64,
        "condition_start_date": pl.Date,
        "condition_end_date": pl.Date,
        "condition_type_concept_id": pl.Int64,
    }
    proc_schema = {
        "procedure_occurrence_id": pl.Int64,
        "person_id": pl.Int64,
        "procedure_concept_id": pl.Int64,
        "procedure_date": pl.Date,
        "procedure_type_concept_id": pl.Int64,
    }
    meas_schema = {
        "measurement_id": pl.Int64,
        "person_id": pl.Int64,
        "measurement_concept_id": pl.Int64,
        "measurement_date": pl.Date,
        "measurement_type_concept_id": pl.Int64,
        "value_as_number": pl.Float64,
        "unit_concept_id": pl.Int64,
    }
    obs_schema = {
        "observation_id": pl.Int64,
        "person_id": pl.Int64,
        "observation_concept_id": pl.Int64,
        "observation_date": pl.Date,
        "observation_type_concept_id": pl.Int64,
    }

    cond_df = (
        pl.DataFrame(cond_rows, schema=cond_schema)
        if cond_rows
        else pl.DataFrame(schema=cond_schema)
    )
    proc_df = (
        pl.DataFrame(proc_rows, schema=proc_schema)
        if proc_rows
        else pl.DataFrame(schema=proc_schema)
    )
    meas_df = (
        pl.DataFrame(meas_rows, schema=meas_schema)
        if meas_rows
        else pl.DataFrame(schema=meas_schema)
    )
    obs_df = (
        pl.DataFrame(obs_rows, schema=obs_schema) if obs_rows else pl.DataFrame(schema=obs_schema)
    )

    # Build CDM
    cdm = CdmReference(
        tables={
            "person": CdmTable(person_df, tbl_name="person"),
            "observation_period": CdmTable(obs_period_df, tbl_name="observation_period"),
            "condition_occurrence": CdmTable(cond_df, tbl_name="condition_occurrence"),
            "procedure_occurrence": CdmTable(proc_df, tbl_name="procedure_occurrence"),
            "measurement": CdmTable(meas_df, tbl_name="measurement"),
            "observation": CdmTable(obs_df, tbl_name="observation"),
        },
        cdm_name="mock_pregnancy",
    )

    return cdm


def validate_episodes(
    episodes: pl.DataFrame,
    *,
    max_days: int = 365,
) -> pl.DataFrame:
    """Validate pregnancy episode periods.

    Checks that episodes satisfy basic temporal constraints:

    - ``episode_start_date`` <= ``episode_end_date``
    - Duration does not exceed *max_days*
    - No overlapping episodes for the same person

    Parameters
    ----------
    episodes
        DataFrame with at least: person_id, episode_start_date,
        episode_end_date.
    max_days
        Maximum allowed episode duration in days.

    Returns
    -------
    pl.DataFrame
        Validation report with columns: check, n_violations, details.
    """
    checks: list[dict[str, Any]] = []

    if episodes.height == 0:
        return pl.DataFrame(
            {
                "check": ["no_episodes"],
                "n_violations": [0],
                "details": ["No episodes to validate"],
            }
        )

    # Check 1: start <= end
    if "episode_start_date" in episodes.columns and "episode_end_date" in episodes.columns:
        bad_dates = episodes.filter(pl.col("episode_start_date") > pl.col("episode_end_date"))
        checks.append(
            {
                "check": "start_before_end",
                "n_violations": bad_dates.height,
                "details": f"{bad_dates.height} episodes with start > end",
            }
        )

        # Check 2: duration <= max_days
        durations = (episodes["episode_end_date"] - episodes["episode_start_date"]).dt.total_days()
        too_long = (durations > max_days).sum()
        checks.append(
            {
                "check": "max_duration",
                "n_violations": int(too_long),
                "details": f"{too_long} episodes exceeding {max_days} days",
            }
        )

    # Check 3: no overlaps within same person
    if "person_id" in episodes.columns:
        n_overlaps = 0
        for pid in episodes["person_id"].unique().to_list():
            person_eps = episodes.filter(pl.col("person_id") == pid).sort("episode_start_date")
            if person_eps.height < 2:
                continue
            starts = person_eps["episode_start_date"].to_list()
            ends = person_eps["episode_end_date"].to_list()
            for j in range(1, len(starts)):
                if starts[j] < ends[j - 1]:
                    n_overlaps += 1

        checks.append(
            {
                "check": "no_overlaps",
                "n_violations": n_overlaps,
                "details": f"{n_overlaps} overlapping episode pairs",
            }
        )

    if not checks:
        checks.append(
            {
                "check": "unknown",
                "n_violations": 0,
                "details": "No checks could be performed",
            }
        )

    return pl.DataFrame(checks).cast(
        {
            "check": pl.Utf8,
            "n_violations": pl.Int64,
            "details": pl.Utf8,
        }
    )
