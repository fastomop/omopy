"""Mock data generation for cohort characteristics.

Provides :func:`mock_cohort_characteristics` which generates realistic
but synthetic SummarisedResult data for testing and documentation.
"""

from __future__ import annotations

import random
from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = ["mock_cohort_characteristics"]

_PACKAGE_NAME = "omopy.characteristics"
_PACKAGE_VERSION = "0.1.0"


def mock_cohort_characteristics(
    *,
    n_cohorts: int = 2,
    n_strata: int = 0,
    seed: int = 42,
) -> SummarisedResult:
    """Generate a mock SummarisedResult for cohort characteristics.

    Creates synthetic data representative of a
    ``summarise_characteristics()`` output, useful for testing
    table/plot functions without requiring a database.

    Parameters
    ----------
    n_cohorts
        Number of cohorts to simulate.
    n_strata
        Number of additional strata to include (0 = overall only).
    seed
        Random seed for reproducibility.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_characteristics"``.
    """
    rng = random.Random(seed)

    cohort_names = [f"cohort_{i + 1}" for i in range(n_cohorts)]
    strata_specs: list[tuple[str, str]] = [(OVERALL, OVERALL)]

    if n_strata > 0:
        strata_specs.append(("sex", "Male"))
        strata_specs.append(("sex", "Female"))
        if n_strata > 1:
            age_groups = ["<40", "40-65", ">65"]
            for ag in age_groups:
                strata_specs.append(("age_group", ag))

    rows: list[dict[str, Any]] = []
    result_id = 1

    for cname in cohort_names:
        for sname, slevel in strata_specs:
            n_subjects = rng.randint(50, 500)
            n_records = n_subjects + rng.randint(0, 200)

            base = {
                "result_id": result_id,
                "cdm_name": "mock_cdm",
                "group_name": "cohort_name",
                "group_level": cname,
                "strata_name": sname,
                "strata_level": slevel,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }

            # Counts
            rows.append(
                {
                    **base,
                    "variable_name": "Number records",
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(n_records),
                }
            )
            rows.append(
                {
                    **base,
                    "variable_name": "Number subjects",
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(n_subjects),
                }
            )

            # Age (numeric)
            age_mean = rng.uniform(35.0, 75.0)
            age_sd = rng.uniform(8.0, 20.0)
            age_min = max(0, age_mean - 3 * age_sd)
            age_max = age_mean + 3 * age_sd
            for est_name, est_type, est_value in [
                ("mean", "numeric", f"{age_mean:.2f}"),
                ("sd", "numeric", f"{age_sd:.2f}"),
                ("min", "numeric", f"{age_min:.2f}"),
                ("q25", "numeric", f"{age_mean - age_sd:.2f}"),
                ("median", "numeric", f"{age_mean:.2f}"),
                ("q75", "numeric", f"{age_mean + age_sd:.2f}"),
                ("max", "numeric", f"{age_max:.2f}"),
            ]:
                rows.append(
                    {
                        **base,
                        "variable_name": "Age",
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                    }
                )

            # Sex (categorical)
            n_male = rng.randint(int(n_subjects * 0.3), int(n_subjects * 0.7))
            n_female = n_subjects - n_male
            for level, count in [("Male", n_male), ("Female", n_female)]:
                pct = count / n_subjects * 100 if n_subjects > 0 else 0
                rows.append(
                    {
                        **base,
                        "variable_name": "Sex",
                        "variable_level": level,
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(count),
                    }
                )
                rows.append(
                    {
                        **base,
                        "variable_name": "Sex",
                        "variable_level": level,
                        "estimate_name": "percentage",
                        "estimate_type": "percentage",
                        "estimate_value": f"{pct:.2f}",
                    }
                )

            # Prior observation (numeric)
            po_mean = rng.uniform(500, 3000)
            po_sd = rng.uniform(200, 800)
            for est_name, est_type, est_value in [
                ("mean", "numeric", f"{po_mean:.2f}"),
                ("sd", "numeric", f"{po_sd:.2f}"),
                ("min", "numeric", f"{max(0, po_mean - 2 * po_sd):.2f}"),
                ("q25", "numeric", f"{po_mean - po_sd:.2f}"),
                ("median", "numeric", f"{po_mean:.2f}"),
                ("q75", "numeric", f"{po_mean + po_sd:.2f}"),
                ("max", "numeric", f"{po_mean + 2 * po_sd:.2f}"),
            ]:
                rows.append(
                    {
                        **base,
                        "variable_name": "Prior observation",
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                    }
                )

    data = pl.DataFrame(rows)
    settings = pl.DataFrame(
        {
            "result_id": [result_id],
            "result_type": ["summarise_characteristics"],
            "package_name": [_PACKAGE_NAME],
            "package_version": [_PACKAGE_VERSION],
        }
    )

    return SummarisedResult(data, settings=settings)
