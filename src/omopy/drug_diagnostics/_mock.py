"""Mock data and benchmarking for drug diagnostics.

Provides :func:`mock_drug_exposure` for generating synthetic
DiagnosticsResult data for testing, and :func:`benchmark_drug_diagnostics`
for timing execution across different configurations.
"""

from __future__ import annotations

import random
import time
from typing import Any

import polars as pl

from omopy.drug_diagnostics._checks import (
    AVAILABLE_CHECKS,
    DiagnosticsResult,
    _MISSING_COLUMNS,
    _QUANTILE_NAMES,
)

__all__ = [
    "benchmark_drug_diagnostics",
    "mock_drug_exposure",
]


def mock_drug_exposure(
    *,
    n_ingredients: int = 2,
    n_records_per_ingredient: int = 100,
    seed: int = 42,
    include_checks: list[str] | None = None,
) -> DiagnosticsResult:
    """Generate a mock DiagnosticsResult for testing.

    Creates synthetic data representative of :func:`execute_checks` output,
    useful for testing table/plot/summarise functions without requiring a
    database.

    Parameters
    ----------
    n_ingredients
        Number of ingredient concepts to simulate.
    n_records_per_ingredient
        Number of drug exposure records per ingredient.
    seed
        Random seed for reproducibility.
    include_checks
        Which checks to include. Defaults to all available checks.

    Returns
    -------
    DiagnosticsResult
        Mock results with realistic distributions.
    """
    rng = random.Random(seed)
    checks = include_checks or list(AVAILABLE_CHECKS)

    ingredient_names = [
        name for name, _ in zip(
            ["Acetaminophen", "Ibuprofen", "Amoxicillin", "Metformin",
             "Lisinopril", "Atorvastatin", "Omeprazole", "Amlodipine"],
            range(n_ingredients),
        )
    ]
    ingredient_ids = [1125315 + i * 1000 for i in range(n_ingredients)]
    ingredient_concepts = dict(zip(ingredient_ids, ingredient_names))

    results: dict[str, pl.DataFrame] = {}

    for check in checks:
        parts: list[pl.DataFrame] = []

        for ing_id, ing_name in zip(ingredient_ids, ingredient_names):
            n = n_records_per_ingredient
            n_persons = max(1, n // rng.randint(2, 5))

            if check == "missing":
                rows = []
                for col in _MISSING_COLUMNS:
                    n_missing = rng.randint(0, n)
                    rows.append({
                        "ingredient_concept_id": ing_id,
                        "ingredient": ing_name,
                        "variable": col,
                        "n_records": n,
                        "n_sample": n,
                        "n_missing": n_missing,
                        "n_not_missing": n - n_missing,
                        "proportion_missing": n_missing / n,
                    })
                parts.append(pl.DataFrame(rows))

            elif check == "exposure_duration":
                mean_dur = rng.uniform(10.0, 90.0)
                sd_dur = rng.uniform(3.0, 30.0)
                n_neg = rng.randint(0, 3)
                row: dict[str, Any] = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                    "n_negative_duration": n_neg,
                    "proportion_negative_duration": n_neg / n,
                }
                for qn, qv in zip(
                    _QUANTILE_NAMES,
                    [max(1, mean_dur - 2*sd_dur), max(1, mean_dur - 1.5*sd_dur),
                     max(1, mean_dur - 0.7*sd_dur), mean_dur,
                     mean_dur + 0.7*sd_dur, mean_dur + 1.5*sd_dur, mean_dur + 2*sd_dur],
                ):
                    row[f"duration_{qn}"] = round(qv, 1)
                row["duration_mean"] = round(mean_dur, 2)
                row["duration_sd"] = round(sd_dur, 2)
                row["duration_min"] = max(1, round(mean_dur - 3*sd_dur))
                row["duration_max"] = round(mean_dur + 3*sd_dur)
                row["duration_count"] = n
                row["duration_count_missing"] = 0
                parts.append(pl.DataFrame([row]))

            elif check == "type":
                types = [(32817, "EHR"), (32818, "EHR administration"), (32869, "Case Report Form")]
                rows = []
                remaining = n
                for tid, tname in types[:rng.randint(1, len(types))]:
                    if remaining <= 0:
                        break
                    cnt = rng.randint(1, remaining)
                    remaining -= cnt
                    rows.append({
                        "ingredient_concept_id": ing_id,
                        "ingredient": ing_name,
                        "drug_type_concept_id": tid,
                        "drug_type": tname,
                        "n_records": n,
                        "n_sample": n,
                        "count": cnt,
                        "proportion": cnt / n,
                    })
                parts.append(pl.DataFrame(rows))

            elif check == "route":
                routes = [(4128794, "Oral"), (4302612, "Intravenous"), (4186831, "Topical")]
                rows = []
                remaining = n
                for rid, rname in routes[:rng.randint(1, len(routes))]:
                    if remaining <= 0:
                        break
                    cnt = rng.randint(1, max(1, remaining))
                    remaining -= cnt
                    rows.append({
                        "ingredient_concept_id": ing_id,
                        "ingredient": ing_name,
                        "route_concept_id": rid,
                        "route": rname,
                        "n_records": n,
                        "n_sample": n,
                        "count": cnt,
                        "proportion": cnt / n,
                    })
                parts.append(pl.DataFrame(rows))

            elif check == "source_concept":
                rows = []
                for i in range(rng.randint(2, 6)):
                    cnt = rng.randint(5, max(5, n // 3))
                    rows.append({
                        "ingredient_concept_id": ing_id,
                        "ingredient": ing_name,
                        "drug_concept_id": ing_id + i + 1,
                        "drug_source_concept_id": ing_id + i + 100,
                        "drug_source_value": f"{ing_name.lower()}_{i}mg",
                        "n_records": n,
                        "n_sample": n,
                        "count": cnt,
                        "proportion": cnt / n,
                    })
                parts.append(pl.DataFrame(rows))

            elif check == "days_supply":
                mean_ds = rng.uniform(14.0, 90.0)
                sd_ds = rng.uniform(5.0, 20.0)
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                }
                for qn, qv in zip(
                    _QUANTILE_NAMES,
                    [max(1, mean_ds - 2*sd_ds), max(1, mean_ds - 1.5*sd_ds),
                     max(1, mean_ds - 0.7*sd_ds), mean_ds,
                     mean_ds + 0.7*sd_ds, mean_ds + 1.5*sd_ds, mean_ds + 2*sd_ds],
                ):
                    row[f"days_supply_{qn}"] = round(qv, 1)
                row["days_supply_mean"] = round(mean_ds, 2)
                row["days_supply_sd"] = round(sd_ds, 2)
                row["days_supply_min"] = max(1, round(mean_ds - 3*sd_ds))
                row["days_supply_max"] = round(mean_ds + 3*sd_ds)
                row["days_supply_count"] = n - rng.randint(0, 10)
                row["days_supply_count_missing"] = n - row["days_supply_count"]
                n_match = rng.randint(0, n)
                row["n_days_supply_match_date_diff"] = n_match
                row["n_days_supply_differ_date_diff"] = rng.randint(0, n - n_match)
                row["n_days_supply_or_dates_missing"] = n - n_match - row["n_days_supply_differ_date_diff"]
                parts.append(pl.DataFrame([row]))

            elif check == "verbatim_end_date":
                n_missing = rng.randint(0, n // 3)
                n_equal = rng.randint(0, n - n_missing)
                n_differ = n - n_missing - n_equal
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                    "n_verbatim_end_date_missing": n_missing,
                    "n_verbatim_end_date_equal": n_equal,
                    "n_verbatim_end_date_differ": n_differ,
                    "proportion_verbatim_end_date_missing": n_missing / n,
                    "proportion_verbatim_end_date_equal": n_equal / n,
                    "proportion_verbatim_end_date_differ": n_differ / n,
                }
                parts.append(pl.DataFrame([row]))

            elif check == "dose":
                n_with = rng.randint(0, n)
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                    "n_with_dose": n_with,
                    "n_without_dose": n - n_with,
                    "proportion_with_dose": n_with / n,
                }
                parts.append(pl.DataFrame([row]))

            elif check == "sig":
                sigs = ["<missing>", "Take 1 tablet daily", "Take 2 tablets twice daily"]
                rows = []
                remaining = n
                for sig in sigs[:rng.randint(1, len(sigs))]:
                    if remaining <= 0:
                        break
                    cnt = rng.randint(1, max(1, remaining))
                    remaining -= cnt
                    rows.append({
                        "ingredient_concept_id": ing_id,
                        "ingredient": ing_name,
                        "sig": sig,
                        "n_records": n,
                        "n_sample": n,
                        "count": cnt,
                        "proportion": cnt / n,
                    })
                parts.append(pl.DataFrame(rows))

            elif check == "quantity":
                mean_qty = rng.uniform(10.0, 100.0)
                sd_qty = rng.uniform(5.0, 30.0)
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                }
                for qn, qv in zip(
                    _QUANTILE_NAMES,
                    [max(0, mean_qty - 2*sd_qty), max(0, mean_qty - 1.5*sd_qty),
                     max(0, mean_qty - 0.7*sd_qty), mean_qty,
                     mean_qty + 0.7*sd_qty, mean_qty + 1.5*sd_qty, mean_qty + 2*sd_qty],
                ):
                    row[f"quantity_{qn}"] = round(qv, 1)
                row["quantity_mean"] = round(mean_qty, 2)
                row["quantity_sd"] = round(sd_qty, 2)
                row["quantity_min"] = max(0, round(mean_qty - 3*sd_qty))
                row["quantity_max"] = round(mean_qty + 3*sd_qty)
                row["quantity_count"] = n - rng.randint(0, n)
                row["quantity_count_missing"] = n - row["quantity_count"]
                parts.append(pl.DataFrame([row]))

            elif check == "days_between":
                mean_gap = rng.uniform(5.0, 60.0)
                sd_gap = rng.uniform(3.0, 20.0)
                n_multi = rng.randint(1, n_persons)
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                    "n_persons": n_persons,
                    "n_persons_multiple_records": n_multi,
                }
                for qn, qv in zip(
                    _QUANTILE_NAMES,
                    [max(0, mean_gap - 2*sd_gap), max(0, mean_gap - 1.5*sd_gap),
                     max(0, mean_gap - 0.7*sd_gap), mean_gap,
                     mean_gap + 0.7*sd_gap, mean_gap + 1.5*sd_gap, mean_gap + 2*sd_gap],
                ):
                    row[f"days_between_{qn}"] = round(qv, 1)
                row["days_between_mean"] = round(mean_gap, 2)
                row["days_between_sd"] = round(sd_gap, 2)
                row["days_between_min"] = max(0, round(mean_gap - 3*sd_gap))
                row["days_between_max"] = round(mean_gap + 3*sd_gap)
                row["days_between_count"] = rng.randint(1, n)
                row["days_between_count_missing"] = 0
                parts.append(pl.DataFrame([row]))

            elif check == "diagnostics_summary":
                row = {
                    "ingredient_concept_id": ing_id,
                    "ingredient": ing_name,
                    "n_records": n,
                    "n_sample": n,
                    "n_persons": n_persons,
                    "mean_proportion_missing": round(rng.uniform(0.0, 0.3), 4),
                    "median_duration_days": round(rng.uniform(10.0, 90.0), 1),
                    "n_negative_duration": rng.randint(0, 3),
                    "median_days_supply": round(rng.uniform(14.0, 90.0), 1),
                    "median_quantity": round(rng.uniform(10.0, 100.0), 1),
                    "proportion_with_dose": round(rng.uniform(0.0, 1.0), 4),
                    "proportion_verbatim_end_date_missing": round(rng.uniform(0.0, 0.2), 4),
                }
                parts.append(pl.DataFrame([row]))

        if parts:
            results[check] = pl.concat(parts, how="diagonal_relaxed")

    return DiagnosticsResult(
        results=results,
        checks_performed=tuple(checks),
        ingredient_concepts=ingredient_concepts,
        cdm_name="mock_cdm",
        sample_size=n_records_per_ingredient,
        min_cell_count=5,
        execution_time_seconds=0.0,
    )


def benchmark_drug_diagnostics(
    cdm: Any,
    ingredient_concept_ids: list[int],
    *,
    checks: list[str] | None = None,
    sample_size: int | None = 10_000,
    n_runs: int = 3,
) -> pl.DataFrame:
    """Benchmark execute_checks performance.

    Runs :func:`execute_checks` multiple times and reports timing statistics.

    Parameters
    ----------
    cdm
        A ``CdmReference`` connected to an OMOP CDM database.
    ingredient_concept_ids
        Ingredient concept IDs to diagnose.
    checks
        Which checks to run. Defaults to all.
    sample_size
        Maximum records to sample per ingredient.
    n_runs
        Number of repetitions for timing.

    Returns
    -------
    polars.DataFrame
        DataFrame with columns: ``run``, ``ingredient_concept_id``,
        ``n_records``, ``execution_time_seconds``.
    """
    from omopy.drug_diagnostics._checks import execute_checks

    rows: list[dict[str, Any]] = []

    for run_idx in range(n_runs):
        t_start = time.monotonic()
        result = execute_checks(
            cdm,
            ingredient_concept_ids,
            checks=checks,
            sample_size=sample_size,
            min_cell_count=0,
        )
        t_end = time.monotonic()

        for ing_id, ing_name in result.ingredient_concepts.items():
            # Count records for this ingredient from the summary
            n_rec = 0
            if "diagnostics_summary" in result.results:
                summary = result.results["diagnostics_summary"]
                ing_rows = summary.filter(pl.col("ingredient_concept_id") == ing_id)
                if ing_rows.height > 0 and "n_records" in ing_rows.columns:
                    n_rec = int(ing_rows["n_records"][0])

            rows.append({
                "run": run_idx + 1,
                "ingredient_concept_id": ing_id,
                "ingredient": ing_name,
                "n_records": n_rec,
                "execution_time_seconds": round(t_end - t_start, 3),
            })

    return pl.DataFrame(rows)
