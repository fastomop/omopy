"""Summarise pregnancy results into SummarisedResult format.

Converts :class:`PregnancyResult` episodes into the standard OHDSI
:class:`SummarisedResult` long format with counts, percentages, and
gestational age statistics broken down by outcome category and optional
strata.
"""

from __future__ import annotations

import math
from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

from omopy.pregnancy._identify import PregnancyResult

__all__ = ["summarise_pregnancies"]

_PACKAGE_NAME = "omopy.pregnancy"
_PACKAGE_VERSION = "0.1.0"


def summarise_pregnancies(
    result: PregnancyResult,
    *,
    strata: list[str] | None = None,
) -> SummarisedResult:
    """Summarise pregnancy episodes into SummarisedResult format.

    Parameters
    ----------
    result
        A :class:`PregnancyResult` from :func:`identify_pregnancies`.
    strata
        Optional list of columns to stratify by (e.g., ``["category"]``).

    Returns
    -------
    SummarisedResult
        Standard OHDSI result format with pregnancy episode statistics.
    """
    episodes: pl.DataFrame = result.episodes

    rows: list[dict[str, Any]] = []
    result_id = 1

    strata_specs: list[tuple[str, str]] = [(OVERALL, OVERALL)]

    if strata and episodes.height > 0:
        for s_col in strata:
            if s_col in episodes.columns:
                for val in sorted(episodes[s_col].drop_nulls().unique().to_list()):
                    strata_specs.append((s_col, str(val)))

    for sname, slevel in strata_specs:
        if sname == OVERALL:
            df = episodes
        else:
            df = episodes.filter(pl.col(sname).cast(pl.Utf8) == slevel)

        base = {
            "result_id": result_id,
            "cdm_name": result.cdm_name,
            "group_name": OVERALL,
            "group_level": OVERALL,
            "strata_name": sname,
            "strata_level": slevel,
            "additional_name": OVERALL,
            "additional_level": OVERALL,
        }

        # Total episode count
        n_episodes = df.height
        rows.append(
            {
                **base,
                "variable_name": "Number episodes",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(n_episodes),
            }
        )

        # Distinct persons
        if n_episodes > 0:
            n_persons = df["person_id"].n_unique()
        else:
            n_persons = 0
        rows.append(
            {
                **base,
                "variable_name": "Number persons",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(n_persons),
            }
        )

        # Episodes by category
        if n_episodes > 0 and "category" in df.columns:
            cat_counts = df.group_by("category").agg(pl.len().alias("n")).sort("category")
            for cat_row in cat_counts.iter_rows(named=True):
                cat_name = cat_row["category"]
                count = cat_row["n"]
                pct = count / n_episodes * 100 if n_episodes > 0 else 0

                rows.append(
                    {
                        **base,
                        "variable_name": "Outcome category",
                        "variable_level": str(cat_name) if cat_name is not None else "Unknown",
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(count),
                    }
                )
                rows.append(
                    {
                        **base,
                        "variable_name": "Outcome category",
                        "variable_level": str(cat_name) if cat_name is not None else "Unknown",
                        "estimate_name": "percentage",
                        "estimate_type": "percentage",
                        "estimate_value": f"{pct:.2f}",
                    }
                )

        # Episode duration statistics
        if n_episodes > 0:
            durations = (
                (df["episode_end_date"] - df["episode_start_date"])
                .dt.total_days()
                .cast(pl.Float64)
            )
            dur_valid = durations.drop_nulls()
            if dur_valid.len() > 0:
                for est_name, est_value in [
                    ("mean", f"{dur_valid.mean():.2f}"),
                    ("sd", f"{dur_valid.std():.2f}" if dur_valid.len() > 1 else "0.00"),
                    ("min", f"{dur_valid.min():.2f}"),
                    ("median", f"{dur_valid.median():.2f}"),
                    ("max", f"{dur_valid.max():.2f}"),
                ]:
                    rows.append(
                        {
                            **base,
                            "variable_name": "Episode duration (days)",
                            "variable_level": "",
                            "estimate_name": est_name,
                            "estimate_type": "numeric",
                            "estimate_value": est_value,
                        }
                    )

        # Gestational age statistics (if available)
        if n_episodes > 0 and "gestational_age_weeks" in df.columns:
            ga = df["gestational_age_weeks"].drop_nulls().cast(pl.Float64)
            if ga.len() > 0:
                for est_name, est_value in [
                    ("mean", f"{ga.mean():.2f}"),
                    ("sd", f"{ga.std():.2f}" if ga.len() > 1 else "0.00"),
                    ("min", f"{ga.min():.2f}"),
                    ("median", f"{ga.median():.2f}"),
                    ("max", f"{ga.max():.2f}"),
                ]:
                    rows.append(
                        {
                            **base,
                            "variable_name": "Gestational age (weeks)",
                            "variable_level": "",
                            "estimate_name": est_name,
                            "estimate_type": "numeric",
                            "estimate_value": est_value,
                        }
                    )

        # Source distribution
        if n_episodes > 0 and "source" in df.columns:
            src_counts = df.group_by("source").agg(pl.len().alias("n")).sort("source")
            for src_row in src_counts.iter_rows(named=True):
                src_name = src_row["source"]
                count = src_row["n"]
                rows.append(
                    {
                        **base,
                        "variable_name": "Episode source",
                        "variable_level": str(src_name) if src_name else "Unknown",
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(count),
                    }
                )

        # Precision distribution
        if n_episodes > 0 and "precision" in df.columns:
            prec_counts = df.group_by("precision").agg(pl.len().alias("n")).sort("precision")
            for prec_row in prec_counts.iter_rows(named=True):
                prec_name = prec_row["precision"]
                count = prec_row["n"]
                rows.append(
                    {
                        **base,
                        "variable_name": "Start date precision",
                        "variable_level": str(prec_name) if prec_name else "Unknown",
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(count),
                    }
                )

    if not rows:
        # Return minimal empty result
        rows.append(
            {
                "result_id": result_id,
                "cdm_name": result.cdm_name,
                "group_name": OVERALL,
                "group_level": OVERALL,
                "strata_name": OVERALL,
                "strata_level": OVERALL,
                "variable_name": "Number episodes",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": "0",
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }
        )

    data = pl.DataFrame(rows)
    settings = pl.DataFrame(
        {
            "result_id": [result_id],
            "result_type": ["summarise_pregnancies"],
            "package_name": [_PACKAGE_NAME],
            "package_version": [_PACKAGE_VERSION],
        }
    )

    return SummarisedResult(data, settings=settings)
