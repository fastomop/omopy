"""Mock CDM data for survival testing.

Implements ``mock_survival()`` — creates a mock CDM with cohort tables
suitable for testing survival analysis functions.

This is the Python equivalent of R's ``mockMGUS2cdm()`` from the
CohortSurvival package, adapted to use synthetic data rather than
the mgus2 dataset.
"""

from __future__ import annotations

import datetime
import random
from typing import Any

import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable

__all__ = ["mock_survival"]


def mock_survival(
    n_persons: int = 200,
    *,
    seed: int = 42,
    target_name: str = "target",
    outcome_name: str = "outcome",
    competing_name: str = "competing",
    event_rate: float = 0.3,
    competing_rate: float = 0.15,
    max_follow_up: int = 3650,
    include_strata: bool = True,
) -> CdmReference:
    """Create a mock CDM with cohort tables for survival analysis testing.

    Generates synthetic person, observation_period, target cohort,
    outcome cohort, and competing risk cohort tables.

    Parameters
    ----------
    n_persons
        Number of persons to simulate.
    seed
        Random seed for reproducibility.
    target_name
        Name of the target cohort.
    outcome_name
        Name of the outcome cohort.
    competing_name
        Name of the competing risk cohort.
    event_rate
        Proportion of persons who experience the primary event.
    competing_rate
        Proportion of persons who experience the competing event.
    max_follow_up
        Maximum follow-up time in days.
    include_strata
        If ``True``, add ``sex`` and ``age_group`` columns to the
        target cohort for stratified analysis.

    Returns
    -------
    CdmReference
        CDM with ``person``, ``observation_period``, target cohort,
        outcome cohort, and competing cohort tables.
    """
    rng = random.Random(seed)

    base_date = datetime.date(2010, 1, 1)

    # Generate persons
    persons: list[dict[str, Any]] = []
    target_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    competing_rows: list[dict[str, Any]] = []
    obs_rows: list[dict[str, Any]] = []

    for pid in range(1, n_persons + 1):
        # Random enrollment date
        enroll_offset = rng.randint(0, 1825)  # 0-5 years from base
        enroll_date = base_date + datetime.timedelta(days=enroll_offset)

        # Follow-up time
        fu_days = rng.randint(30, max_follow_up)
        end_date = enroll_date + datetime.timedelta(days=fu_days)

        # Observation period (extends before and after cohort)
        obs_start = enroll_date - datetime.timedelta(days=rng.randint(0, 365))
        obs_end = end_date + datetime.timedelta(days=rng.randint(0, 180))

        obs_rows.append({
            "observation_period_id": pid,
            "person_id": pid,
            "observation_period_start_date": obs_start,
            "observation_period_end_date": obs_end,
            "period_type_concept_id": 44814724,
        })

        # Person
        sex = rng.choice(["Male", "Female"])
        age = rng.randint(20, 90)
        year_of_birth = enroll_date.year - age
        gender_concept_id = 8507 if sex == "Male" else 8532

        persons.append({
            "person_id": pid,
            "gender_concept_id": gender_concept_id,
            "year_of_birth": year_of_birth,
            "month_of_birth": rng.randint(1, 12),
            "day_of_birth": rng.randint(1, 28),
            "race_concept_id": 0,
            "ethnicity_concept_id": 0,
        })

        # Target cohort entry
        target_entry: dict[str, Any] = {
            "cohort_definition_id": 1,
            "subject_id": pid,
            "cohort_start_date": enroll_date,
            "cohort_end_date": end_date,
        }
        if include_strata:
            target_entry["sex"] = sex
            target_entry["age_group"] = (
                "young" if age < 50
                else "middle" if age < 70
                else "old"
            )
        target_rows.append(target_entry)

        # Primary outcome (event_rate chance)
        roll = rng.random()
        if roll < event_rate:
            event_days = rng.randint(1, fu_days)
            event_date = enroll_date + datetime.timedelta(days=event_days)
            outcome_rows.append({
                "cohort_definition_id": 1,
                "subject_id": pid,
                "cohort_start_date": event_date,
                "cohort_end_date": event_date,
            })
        elif roll < event_rate + competing_rate:
            # Competing event
            event_days = rng.randint(1, fu_days)
            event_date = enroll_date + datetime.timedelta(days=event_days)
            competing_rows.append({
                "cohort_definition_id": 1,
                "subject_id": pid,
                "cohort_start_date": event_date,
                "cohort_end_date": event_date,
            })

    # Build DataFrames with proper types
    target_df = pl.DataFrame(target_rows).cast({
        "cohort_definition_id": pl.Int64,
        "subject_id": pl.Int64,
        "cohort_start_date": pl.Date,
        "cohort_end_date": pl.Date,
    })
    outcome_df = pl.DataFrame(
        outcome_rows,
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        },
    ) if outcome_rows else pl.DataFrame({
        "cohort_definition_id": pl.Series([], dtype=pl.Int64),
        "subject_id": pl.Series([], dtype=pl.Int64),
        "cohort_start_date": pl.Series([], dtype=pl.Date),
        "cohort_end_date": pl.Series([], dtype=pl.Date),
    })
    competing_df = pl.DataFrame(
        competing_rows,
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        },
    ) if competing_rows else pl.DataFrame({
        "cohort_definition_id": pl.Series([], dtype=pl.Int64),
        "subject_id": pl.Series([], dtype=pl.Int64),
        "cohort_start_date": pl.Series([], dtype=pl.Date),
        "cohort_end_date": pl.Series([], dtype=pl.Date),
    })
    person_df = pl.DataFrame(persons).cast({
        "person_id": pl.Int64,
        "year_of_birth": pl.Int32,
        "month_of_birth": pl.Int32,
        "day_of_birth": pl.Int32,
        "gender_concept_id": pl.Int32,
        "race_concept_id": pl.Int32,
        "ethnicity_concept_id": pl.Int32,
    })
    obs_df = pl.DataFrame(obs_rows).cast({
        "person_id": pl.Int64,
        "observation_period_id": pl.Int64,
        "observation_period_start_date": pl.Date,
        "observation_period_end_date": pl.Date,
        "period_type_concept_id": pl.Int64,
    })

    # Settings
    settings_target = pl.DataFrame({
        "cohort_definition_id": [1],
        "cohort_name": [target_name],
    }).cast({"cohort_definition_id": pl.Int64})

    settings_outcome = pl.DataFrame({
        "cohort_definition_id": [1],
        "cohort_name": [outcome_name],
    }).cast({"cohort_definition_id": pl.Int64})

    settings_competing = pl.DataFrame({
        "cohort_definition_id": [1],
        "cohort_name": [competing_name],
    }).cast({"cohort_definition_id": pl.Int64})

    # Build CdmReference
    cdm = CdmReference(
        tables={
            "person": CdmTable(person_df, tbl_name="person"),
            "observation_period": CdmTable(obs_df, tbl_name="observation_period"),
        },
        cdm_name="mock_survival",
    )

    cdm[target_name] = CohortTable(
        target_df, tbl_name=target_name, settings=settings_target,
    )
    cdm[outcome_name] = CohortTable(
        outcome_df, tbl_name=outcome_name, settings=settings_outcome,
    )
    cdm[competing_name] = CohortTable(
        competing_df, tbl_name=competing_name, settings=settings_competing,
    )

    return cdm
