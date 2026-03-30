"""HIP algorithm — outcome-anchored pregnancy identification.

Implements the two-pass HIP (HIPPS Identification from Pregnancy outcomes)
algorithm from the HIPPS method (Smith et al. 2024):

**Pass 1 — Outcome-first:**
Walk backwards through each person's HIP records.  When an outcome concept
is found, check Matcho spacing from the previously assigned outcome.  If
spacing is sufficient, create a new pregnancy episode anchored on that
outcome date.

**Pass 2 — Gestation-only:**
Records not assigned in Pass 1 are grouped into 10-month windows and
assigned to "PREG" (ongoing/unspecified) episodes.
"""

from __future__ import annotations

import logging

import polars as pl

from omopy.pregnancy._concepts import (
    MATCHO_OUTCOME_LIMITS,
    MATCHO_TERM_DURATIONS,
)

__all__ = ["_run_hip"]

log = logging.getLogger(__name__)

# Categories that represent definitive outcomes
_OUTCOME_CATS: frozenset[str] = frozenset({"LB", "SB", "AB", "SA", "DELIV", "ECT"})

# Default minimum spacing when category pair not in MATCHO table
_DEFAULT_MIN_SPACING: int = 42


def _run_hip(
    hip_records: pl.DataFrame,
    *,
    just_gestation: bool = True,
) -> pl.DataFrame:
    """Run the HIP outcome-anchored algorithm.

    Parameters
    ----------
    hip_records
        DataFrame with columns: person_id, concept_id, record_date,
        category, gest_value.
    just_gestation
        If True, run Pass 2 to assign gestation-only episodes.

    Returns
    -------
    pl.DataFrame
        Pregnancy episodes with columns: person_id, episode_id, category,
        episode_start_date, episode_end_date, outcome_date, outcome_concept_id.
    """
    result_schema = {
        "person_id": pl.Int64,
        "episode_id": pl.Int64,
        "category": pl.Utf8,
        "episode_start_date": pl.Date,
        "episode_end_date": pl.Date,
        "outcome_date": pl.Date,
        "outcome_concept_id": pl.Int64,
    }

    if hip_records.height == 0:
        log.info("No HIP records to process.")
        return pl.DataFrame(schema=result_schema)

    episodes: list[dict] = []
    assigned_indices: set[int] = set()
    episode_counter = 0

    # Process each person separately
    persons = hip_records["person_id"].unique().sort().to_list()

    for pid in persons:
        person_recs = (
            hip_records.filter(pl.col("person_id") == pid)
            .sort("record_date")
        )
        n = person_recs.height
        if n == 0:
            continue

        dates = person_recs["record_date"].to_list()
        cats = person_recs["category"].to_list()
        concept_ids = person_recs["concept_id"].to_list()
        # Get global row indices for tracking assignment
        global_mask = hip_records["person_id"] == pid
        global_sorted = (
            hip_records.with_row_index("_global_idx")
            .filter(global_mask)
            .sort("record_date")
        )
        global_indices = global_sorted["_global_idx"].to_list()

        # ---- Pass 1: Outcome-first (walk backwards) ----
        last_outcome_cat: str | None = None
        last_outcome_date = None

        for i in range(n - 1, -1, -1):
            cat = cats[i]
            if cat is None or cat not in _OUTCOME_CATS:
                continue

            dt = dates[i]

            # Check Matcho spacing
            if last_outcome_date is not None and last_outcome_cat is not None:
                days_gap = (last_outcome_date - dt).days
                min_days = MATCHO_OUTCOME_LIMITS.get(
                    (cat, last_outcome_cat), _DEFAULT_MIN_SPACING
                )
                if days_gap < min_days:
                    # Too close to previous outcome — skip this record
                    assigned_indices.add(global_indices[i])
                    continue

            # Create a new episode
            episode_counter += 1
            term_min, term_max = MATCHO_TERM_DURATIONS.get(cat, (28, 308))
            from datetime import timedelta

            ep_start = dt - timedelta(days=term_max)
            ep_end = dt

            episodes.append({
                "person_id": pid,
                "episode_id": episode_counter,
                "category": cat,
                "episode_start_date": ep_start,
                "episode_end_date": ep_end,
                "outcome_date": dt,
                "outcome_concept_id": concept_ids[i],
            })

            assigned_indices.add(global_indices[i])
            last_outcome_cat = cat
            last_outcome_date = dt

        # Also mark non-outcome records that fall within an episode's window
        # as assigned
        for i in range(n):
            cat = cats[i]
            if global_indices[i] in assigned_indices:
                continue
            dt = dates[i]
            for ep in episodes:
                if (
                    ep["person_id"] == pid
                    and ep["episode_start_date"] <= dt <= ep["episode_end_date"]
                ):
                    assigned_indices.add(global_indices[i])
                    break

    # ---- Pass 2: Gestation-only ----
    if just_gestation:
        unassigned_mask = ~pl.Series(
            "_idx",
            range(hip_records.height),
        ).is_in(list(assigned_indices))

        unassigned = hip_records.with_row_index("_idx").filter(unassigned_mask).drop("_idx")

        if unassigned.height > 0:
            for pid in unassigned["person_id"].unique().sort().to_list():
                precs = (
                    unassigned.filter(pl.col("person_id") == pid)
                    .sort("record_date")
                )
                if precs.height == 0:
                    continue

                from datetime import timedelta

                u_dates = precs["record_date"].to_list()
                u_concepts = precs["concept_id"].to_list()

                # Group into 10-month (305-day) windows
                current_start = u_dates[0]
                group_dates: list = [u_dates[0]]
                group_concepts: list = [u_concepts[0]]

                for j in range(1, len(u_dates)):
                    gap = (u_dates[j] - current_start).days
                    if gap > 305:
                        # Emit episode for current group
                        episode_counter += 1
                        episodes.append({
                            "person_id": pid,
                            "episode_id": episode_counter,
                            "category": "PREG",
                            "episode_start_date": group_dates[0],
                            "episode_end_date": group_dates[-1],
                            "outcome_date": None,
                            "outcome_concept_id": group_concepts[0],
                        })
                        current_start = u_dates[j]
                        group_dates = [u_dates[j]]
                        group_concepts = [u_concepts[j]]
                    else:
                        group_dates.append(u_dates[j])
                        group_concepts.append(u_concepts[j])

                # Emit final group
                if group_dates:
                    episode_counter += 1
                    episodes.append({
                        "person_id": pid,
                        "episode_id": episode_counter,
                        "category": "PREG",
                        "episode_start_date": group_dates[0],
                        "episode_end_date": group_dates[-1],
                        "outcome_date": None,
                        "outcome_concept_id": group_concepts[0],
                    })

    if not episodes:
        return pl.DataFrame(schema=result_schema)

    result = pl.DataFrame(episodes).cast({
        "person_id": pl.Int64,
        "episode_id": pl.Int64,
        "category": pl.Utf8,
        "episode_start_date": pl.Date,
        "episode_end_date": pl.Date,
        "outcome_date": pl.Date,
        "outcome_concept_id": pl.Int64,
    })

    log.info("HIP produced %d episodes.", result.height)
    return result
