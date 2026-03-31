"""Mock data generation for treatment pathways.

Provides :func:`mock_treatment_pathways` which generates realistic
but synthetic SummarisedResult data for testing table and plot
functions without requiring a database or running the compute pipeline.
"""

from __future__ import annotations

import random
from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "mock_treatment_pathways",
]

_PACKAGE_NAME = "omopy.treatment"
_PACKAGE_VERSION = "0.1.0"


# ===================================================================
# mock_treatment_pathways
# ===================================================================


def mock_treatment_pathways(
    *,
    n_targets: int = 1,
    n_drugs: int = 4,
    n_pathways: int = 15,
    include_duration: bool = True,
    seed: int = 42,
) -> SummarisedResult:
    """Generate a mock SummarisedResult for treatment pathways.

    Creates synthetic data representative of
    ``summarise_treatment_pathways()`` and optionally
    ``summarise_event_duration()`` output, useful for testing
    table/plot functions without requiring a database.

    Parameters
    ----------
    n_targets
        Number of target cohorts to simulate.
    n_drugs
        Number of distinct drug treatments to include.
    n_pathways
        Number of distinct pathways to generate per target.
    include_duration
        If ``True``, also include ``summarise_event_duration`` rows.
    seed
        Random seed for reproducibility.

    Returns
    -------
    SummarisedResult
        With ``result_type`` values ``"summarise_treatment_pathways"``
        and optionally ``"summarise_event_duration"``.
    """
    rng = random.Random(seed)

    target_names = [f"target_{i + 1}" for i in range(n_targets)]
    drug_names = [
        name
        for name, _ in zip(
            [
                "Aspirin",
                "Metformin",
                "Lisinopril",
                "Atorvastatin",
                "Omeprazole",
                "Amlodipine",
                "Metoprolol",
                "Simvastatin",
            ],
            range(n_drugs),
            strict=False,
        )
    ]

    rows: list[dict[str, Any]] = []
    result_ids: list[int] = []
    result_types: list[str] = []

    # --- Pathway rows (result_id=1) --------------------------------
    rid_pathway = 1
    result_ids.append(rid_pathway)
    result_types.append("summarise_treatment_pathways")

    for target in target_names:
        total_subjects = rng.randint(200, 1000)

        # Generate realistic pathways
        generated_paths: list[str] = []
        for _ in range(n_pathways):
            n_steps = rng.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
            steps: list[str] = []
            for _s in range(n_steps):
                # Occasionally create combinations
                if rng.random() < 0.15 and len(drug_names) >= 2:
                    combo = sorted(rng.sample(drug_names, 2))
                    steps.append("+".join(combo))
                else:
                    steps.append(rng.choice(drug_names))
            generated_paths.append("-".join(steps))

        # Deduplicate
        generated_paths = list(dict.fromkeys(generated_paths))

        # Assign frequencies (Zipf-like distribution)
        remaining = total_subjects
        path_freqs: list[tuple[str, int]] = []
        for i, path in enumerate(generated_paths):
            if i == len(generated_paths) - 1:
                freq = max(5, remaining)
            else:
                freq = max(5, int(remaining * rng.uniform(0.1, 0.4)))
                remaining -= freq
                if remaining < 5:
                    remaining = 5
            path_freqs.append((path, freq))

        for path, freq in path_freqs:
            pct = freq / total_subjects * 100
            base = {
                "result_id": rid_pathway,
                "cdm_name": "mock_cdm",
                "group_name": "target_cohort_name",
                "group_level": target,
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "treatment_pathway",
                "variable_level": path,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }
            rows.append(
                {
                    **base,
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(freq),
                }
            )
            rows.append(
                {
                    **base,
                    "estimate_name": "percentage",
                    "estimate_type": "percentage",
                    "estimate_value": f"{pct:.2f}",
                }
            )

        # Count rows
        rows.append(
            {
                "result_id": rid_pathway,
                "cdm_name": "mock_cdm",
                "group_name": "target_cohort_name",
                "group_level": target,
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Number records",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(total_subjects + rng.randint(0, 200)),
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }
        )
        rows.append(
            {
                "result_id": rid_pathway,
                "cdm_name": "mock_cdm",
                "group_name": "target_cohort_name",
                "group_level": target,
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Number subjects",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(total_subjects),
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }
        )

    # --- Duration rows (result_id=2) --------------------------------
    if include_duration:
        rid_duration = 2
        result_ids.append(rid_duration)
        result_types.append("summarise_event_duration")

        estimates = ("min", "q25", "median", "q75", "max", "mean", "sd", "count")

        for target in target_names:
            # Overall and per-line
            for line in ["overall", "1", "2", "3"]:
                # Mono and combination events
                for event_name in ["mono-event", "combination-event", *drug_names]:
                    mean_dur = rng.uniform(15.0, 180.0)
                    sd_dur = rng.uniform(5.0, 0.5 * mean_dur)
                    min_dur = max(1, mean_dur - 2 * sd_dur)
                    max_dur = mean_dur + 2 * sd_dur
                    q25_dur = mean_dur - 0.7 * sd_dur
                    q75_dur = mean_dur + 0.7 * sd_dur
                    n_events = rng.randint(10, 200)

                    stats = {
                        "min": str(int(min_dur)),
                        "q25": str(int(q25_dur)),
                        "median": str(int(mean_dur)),
                        "q75": str(int(q75_dur)),
                        "max": str(int(max_dur)),
                        "mean": f"{mean_dur:.2f}",
                        "sd": f"{sd_dur:.2f}",
                        "count": str(n_events),
                    }

                    for est in estimates:
                        rows.append(
                            {
                                "result_id": rid_duration,
                                "cdm_name": "mock_cdm",
                                "group_name": "target_cohort_name",
                                "group_level": target,
                                "strata_name": OVERALL,
                                "strata_level": OVERALL,
                                "variable_name": event_name,
                                "variable_level": "",
                                "estimate_name": est,
                                "estimate_type": (
                                    "integer" if est == "count" else "numeric"
                                ),
                                "estimate_value": stats[est],
                                "additional_name": "line",
                                "additional_level": line,
                            }
                        )

    data = pl.DataFrame(rows)
    settings = pl.DataFrame(
        {
            "result_id": result_ids,
            "result_type": result_types,
            "package_name": [_PACKAGE_NAME] * len(result_ids),
            "package_version": [_PACKAGE_VERSION] * len(result_ids),
        }
    )

    return SummarisedResult(data, settings=settings)
