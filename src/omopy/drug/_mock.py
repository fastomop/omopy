"""Mock data generation for drug utilisation.

Provides :func:`mock_drug_utilisation` which generates realistic
but synthetic SummarisedResult data for testing table and plot
functions without requiring a database.

Also provides a placeholder :func:`benchmark_drug_utilisation`.
"""

from __future__ import annotations

import random
from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "mock_drug_utilisation",
    "benchmark_drug_utilisation",
]

_PACKAGE_NAME = "omopy.drug"
_PACKAGE_VERSION = "0.1.0"


# ===================================================================
# mock_drug_utilisation
# ===================================================================


def mock_drug_utilisation(
    *,
    n_cohorts: int = 2,
    n_concept_sets: int = 1,
    n_strata: int = 0,
    seed: int = 42,
) -> SummarisedResult:
    """Generate a mock SummarisedResult for drug utilisation.

    Creates synthetic data representative of a
    ``summarise_drug_utilisation()`` output, useful for testing
    table/plot functions without requiring a database.

    Parameters
    ----------
    n_cohorts
        Number of cohorts to simulate.
    n_concept_sets
        Number of concept sets per cohort.
    n_strata
        Number of additional strata (0 = overall only).
    seed
        Random seed for reproducibility.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_drug_utilisation"``.
    """
    rng = random.Random(seed)

    cohort_names = [f"cohort_{i + 1}" for i in range(n_cohorts)]
    concept_set_names = [f"drug_{i + 1}" for i in range(n_concept_sets)]

    strata_specs: list[tuple[str, str]] = [(OVERALL, OVERALL)]
    if n_strata > 0:
        strata_specs.append(("sex", "Male"))
        strata_specs.append(("sex", "Female"))
        if n_strata > 1:
            for ag in ("<40", "40-65", ">65"):
                strata_specs.append(("age_group", ag))

    rows: list[dict[str, Any]] = []
    result_id = 1

    # Drug utilisation metrics to mock
    metrics = [
        ("number exposures", 1.0, 10.0),
        ("number eras", 1.0, 5.0),
        ("days exposed", 10.0, 365.0),
        ("days prescribed", 10.0, 365.0),
        ("time to exposure", 0.0, 30.0),
        ("initial exposure duration", 7.0, 90.0),
        ("initial quantity", 10.0, 100.0),
        ("cumulative quantity", 50.0, 500.0),
        ("initial daily dose", 5.0, 50.0),
        ("cumulative dose", 100.0, 5000.0),
    ]

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
            }

            # Count rows
            rows.append({
                **base,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
                "variable_name": "Number records",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(n_records),
            })
            rows.append({
                **base,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
                "variable_name": "Number subjects",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(n_subjects),
            })

            for cs_name in concept_set_names:
                for metric_name, low, high in metrics:
                    mean_val = rng.uniform(low, high)
                    sd_val = rng.uniform(0.1 * mean_val, 0.5 * mean_val)
                    min_val = max(0, mean_val - 2 * sd_val)
                    max_val = mean_val + 2 * sd_val
                    q25_val = mean_val - 0.7 * sd_val
                    q75_val = mean_val + 0.7 * sd_val

                    metric_base = {
                        **base,
                        "additional_name": "concept_set",
                        "additional_level": cs_name,
                        "variable_name": metric_name,
                        "variable_level": "",
                    }

                    for est_name, est_type, est_val in [
                        ("mean", "numeric", f"{mean_val:.2f}"),
                        ("sd", "numeric", f"{sd_val:.2f}"),
                        ("min", "numeric", f"{min_val:.2f}"),
                        ("q25", "numeric", f"{q25_val:.2f}"),
                        ("median", "numeric", f"{mean_val:.2f}"),
                        ("q75", "numeric", f"{q75_val:.2f}"),
                        ("max", "numeric", f"{max_val:.2f}"),
                        ("count_missing", "integer", str(rng.randint(0, 5))),
                        (
                            "percentage_missing",
                            "percentage",
                            f"{rng.uniform(0, 5):.2f}",
                        ),
                    ]:
                        rows.append({
                            **metric_base,
                            "estimate_name": est_name,
                            "estimate_type": est_type,
                            "estimate_value": est_val,
                        })

    data = pl.DataFrame(rows)
    settings = pl.DataFrame({
        "result_id": [result_id],
        "result_type": ["summarise_drug_utilisation"],
        "package_name": [_PACKAGE_NAME],
        "package_version": [_PACKAGE_VERSION],
    })

    return SummarisedResult(data, settings=settings)


# ===================================================================
# benchmark_drug_utilisation
# ===================================================================


def benchmark_drug_utilisation(
    *,
    verbose: bool = True,
) -> dict[str, Any]:
    """Placeholder for drug utilisation benchmarking.

    In the R package this runs a set of standard queries to benchmark
    database performance. This is a placeholder for future
    implementation.

    Parameters
    ----------
    verbose
        Print progress messages.

    Returns
    -------
    dict[str, Any]
        Benchmark results (currently empty).
    """
    if verbose:
        import warnings
        warnings.warn(
            "benchmark_drug_utilisation is a placeholder — not yet implemented",
            stacklevel=2,
        )
    return {}
