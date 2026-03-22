"""Mock data generators for testing the vis module."""

from __future__ import annotations

import polars as pl

from omopy.generics.summarised_result import SummarisedResult

__all__ = ["mock_summarised_result"]


def mock_summarised_result(
    *,
    n_cohorts: int = 2,
    n_strata: int = 3,
) -> SummarisedResult:
    """Generate a mock :class:`SummarisedResult` for testing.

    Produces results with:
    - *n_cohorts* cohort groups (``cohort_1``, ``cohort_2``, ...)
    - *n_strata* strata combinations drawn from ``overall``,
      ``age_group &&& sex`` pairs, and single ``sex`` strata.
    - Variables: ``number subjects`` (count), ``age`` (mean, sd),
      ``Medications Amoxiciline`` (count, percentage).

    Args:
        n_cohorts: Number of cohort groups.
        n_strata: Number of strata combinations (max 9).

    Returns:
        A :class:`SummarisedResult` with realistic test data.
    """
    import random

    random.seed(42)

    strata_pool = [
        ("overall", "overall"),
        ("age_group &&& sex", "<40 &&& Male"),
        ("age_group &&& sex", "<40 &&& Female"),
        ("age_group &&& sex", ">=40 &&& Male"),
        ("age_group &&& sex", ">=40 &&& Female"),
        ("sex", "Male"),
        ("sex", "Female"),
        ("age_group", "<40"),
        ("age_group", ">=40"),
    ]
    strata = strata_pool[: min(n_strata, len(strata_pool))]

    rows: list[dict[str, str | int]] = []
    result_id = 1

    for cohort_i in range(1, n_cohorts + 1):
        cohort_name = f"cohort_{cohort_i}"
        for strata_name, strata_level in strata:
            # number subjects — count
            count = random.randint(50, 500)
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": "mock_cdm",
                    "group_name": "cohort_name",
                    "group_level": cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "number subjects",
                    "variable_level": "number subjects",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(count),
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )
            # age — mean
            mean_age = round(random.uniform(30, 70), 2)
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": "mock_cdm",
                    "group_name": "cohort_name",
                    "group_level": cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "age",
                    "variable_level": "age",
                    "estimate_name": "mean",
                    "estimate_type": "numeric",
                    "estimate_value": str(mean_age),
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )
            # age — sd
            sd_age = round(random.uniform(5, 20), 2)
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": "mock_cdm",
                    "group_name": "cohort_name",
                    "group_level": cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "age",
                    "variable_level": "age",
                    "estimate_name": "sd",
                    "estimate_type": "numeric",
                    "estimate_value": str(sd_age),
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )
            # Medications Amoxiciline — count
            med_count = random.randint(10, count)
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": "mock_cdm",
                    "group_name": "cohort_name",
                    "group_level": cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "Medications",
                    "variable_level": "Amoxiciline",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(med_count),
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )
            # Medications Amoxiciline — percentage
            pct = round(med_count / count * 100, 2) if count > 0 else 0.0
            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": "mock_cdm",
                    "group_name": "cohort_name",
                    "group_level": cohort_name,
                    "strata_name": strata_name,
                    "strata_level": strata_level,
                    "variable_name": "Medications",
                    "variable_level": "Amoxiciline",
                    "estimate_name": "percentage",
                    "estimate_type": "percentage",
                    "estimate_value": str(pct),
                    "additional_name": "overall",
                    "additional_level": "overall",
                }
            )

    data = pl.DataFrame(rows)
    settings = pl.DataFrame(
        {
            "result_id": [result_id],
            "result_type": ["mock_result"],
            "package_name": ["omopy"],
            "package_version": ["0.1.0"],
            "min_cell_count": ["5"],
        }
    )
    return SummarisedResult(data, settings=settings)
