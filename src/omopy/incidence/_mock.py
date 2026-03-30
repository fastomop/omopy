"""Mock data generators for incidence/prevalence testing.

Provides ``mock_incidence_prevalence()`` to create a minimal CDM with
denominator and outcome cohorts for testing, and
``benchmark_incidence_prevalence()`` for timing analyses.
"""

from __future__ import annotations

import datetime
import random
import time
from typing import Any

import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable

__all__ = [
    "benchmark_incidence_prevalence",
    "mock_incidence_prevalence",
]


def mock_incidence_prevalence(
    *,
    sample_size: int = 100,
    outcome_prevalence: float = 0.2,
    seed: int | None = None,
    study_start: datetime.date | None = None,
    study_end: datetime.date | None = None,
) -> CdmReference:
    """Create a mock CDM reference for incidence/prevalence testing.

    Generates synthetic person, observation_period, and cohort tables
    suitable for testing the estimation functions.

    Parameters
    ----------
    sample_size
        Number of persons.
    outcome_prevalence
        Probability that each person has an outcome event.
    seed
        Random seed for reproducibility.
    study_start
        Start of study period. Default: ``2010-01-01``.
    study_end
        End of study period. Default: ``2020-12-31``.

    Returns
    -------
    CdmReference
        CDM with ``person``, ``observation_period``, ``target`` cohort,
        and ``outcome`` cohort tables.
    """
    if seed is not None:
        random.seed(seed)

    if study_start is None:
        study_start = datetime.date(2010, 1, 1)
    if study_end is None:
        study_end = datetime.date(2020, 12, 31)

    study_days = (study_end - study_start).days

    # Generate persons
    person_rows = []
    obs_rows = []
    for i in range(1, sample_size + 1):
        year_of_birth = random.randint(1940, 2000)
        month_of_birth = random.randint(1, 12)
        day_of_birth = random.randint(1, 28)
        gender = random.choice([8507, 8532])  # Male or Female

        # Observation period
        obs_start_offset = random.randint(0, study_days // 2)
        obs_end_offset = random.randint(obs_start_offset + 180, study_days)
        obs_start = study_start + datetime.timedelta(days=obs_start_offset)
        obs_end = study_start + datetime.timedelta(days=min(obs_end_offset, study_days))

        person_rows.append(
            {
                "person_id": i,
                "year_of_birth": year_of_birth,
                "month_of_birth": month_of_birth,
                "day_of_birth": day_of_birth,
                "gender_concept_id": gender,
                "race_concept_id": 0,
                "ethnicity_concept_id": 0,
            }
        )
        obs_rows.append(
            {
                "person_id": i,
                "observation_period_id": i,
                "observation_period_start_date": obs_start,
                "observation_period_end_date": obs_end,
                "period_type_concept_id": 44814724,
            }
        )

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
    obs_df = pl.DataFrame(obs_rows).cast(
        {
            "person_id": pl.Int64,
            "observation_period_id": pl.Int64,
            "observation_period_start_date": pl.Date,
            "observation_period_end_date": pl.Date,
            "period_type_concept_id": pl.Int64,
        }
    )

    # Generate target cohort (everyone is in the target)
    target_rows = []
    for i, obs in enumerate(obs_rows, start=1):
        target_rows.append(
            {
                "cohort_definition_id": 1,
                "subject_id": obs["person_id"],
                "cohort_start_date": obs["observation_period_start_date"],
                "cohort_end_date": obs["observation_period_end_date"],
            }
        )

    target_df = pl.DataFrame(target_rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        }
    )

    # Generate outcome cohort
    outcome_rows = []
    for i, obs in enumerate(obs_rows, start=1):
        if random.random() < outcome_prevalence:
            obs_start = obs["observation_period_start_date"]
            obs_end = obs["observation_period_end_date"]
            obs_duration = (obs_end - obs_start).days
            if obs_duration > 1:
                event_offset = random.randint(1, obs_duration - 1)
                event_date = obs_start + datetime.timedelta(days=event_offset)
                # Outcome lasts 30 days or until obs end
                event_end = min(event_date + datetime.timedelta(days=30), obs_end)
                outcome_rows.append(
                    {
                        "cohort_definition_id": 1,
                        "subject_id": obs["person_id"],
                        "cohort_start_date": event_date,
                        "cohort_end_date": event_end,
                    }
                )

    outcome_df = pl.DataFrame(
        outcome_rows,
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        },
    )

    # Build CDM
    target_settings = pl.DataFrame(
        {
            "cohort_definition_id": [1],
            "cohort_name": ["target"],
        }
    ).cast({"cohort_definition_id": pl.Int64})

    outcome_settings = pl.DataFrame(
        {
            "cohort_definition_id": [1],
            "cohort_name": ["outcome"],
        }
    ).cast({"cohort_definition_id": pl.Int64})

    cdm = CdmReference(
        tables={
            "person": CdmTable(person_df, tbl_name="person"),
            "observation_period": CdmTable(obs_df, tbl_name="observation_period"),
        },
        cdm_name="mock",
    )

    cdm["target"] = CohortTable(target_df, tbl_name="target", settings=target_settings)
    cdm["outcome"] = CohortTable(outcome_df, tbl_name="outcome", settings=outcome_settings)

    return cdm


def benchmark_incidence_prevalence(
    cdm: CdmReference,
    *,
    analysis_type: str = "all",
) -> dict[str, float]:
    """Run timing benchmarks on incidence and prevalence estimation.

    Parameters
    ----------
    cdm
        CDM reference with denominator and outcome cohorts.
    analysis_type
        ``"all"``, ``"incidence"``, or ``"prevalence"``.

    Returns
    -------
    dict[str, float]
        Timing results in seconds.
    """
    from omopy.incidence._denominator import generate_denominator_cohort_set
    from omopy.incidence._estimate import (
        estimate_incidence,
        estimate_period_prevalence,
        estimate_point_prevalence,
    )

    results: dict[str, float] = {}

    # Denominator generation
    t0 = time.perf_counter()
    cdm = generate_denominator_cohort_set(cdm, name="benchmark_denom")
    results["denominator_generation"] = time.perf_counter() - t0

    # Check if outcome table exists
    has_outcome = False
    for name in cdm.table_names:
        tbl = cdm[name]
        if isinstance(tbl, CohortTable) and name != "benchmark_denom":
            outcome_name = name
            has_outcome = True
            break

    if not has_outcome:
        return results

    if analysis_type in ("all", "incidence"):
        t0 = time.perf_counter()
        estimate_incidence(cdm, "benchmark_denom", outcome_name)
        results["incidence_estimation"] = time.perf_counter() - t0

    if analysis_type in ("all", "prevalence"):
        t0 = time.perf_counter()
        estimate_point_prevalence(cdm, "benchmark_denom", outcome_name)
        results["point_prevalence_estimation"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        estimate_period_prevalence(cdm, "benchmark_denom", outcome_name)
        results["period_prevalence_estimation"] = time.perf_counter() - t0

    return results
