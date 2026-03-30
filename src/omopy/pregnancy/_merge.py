"""Merge HIP and PPS episodes into unified pregnancy episodes.

Performs a full outer join of HIP and PPS episodes by person and temporal
overlap, then resolves many-to-many matches with up to 10 iterative
deduplication rounds minimising |hip_end − pps_end|.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import polars as pl

__all__ = ["_merge_hipps"]

log = logging.getLogger(__name__)

# Maximum deduplication iterations
_MAX_DEDUP_ROUNDS: int = 10


def _merge_hipps(
    hip_episodes: pl.DataFrame,
    pps_episodes: pl.DataFrame,
) -> pl.DataFrame:
    """Merge HIP and PPS episodes into unified pregnancy episodes.

    Parameters
    ----------
    hip_episodes
        HIP episodes with columns: person_id, episode_id, category,
        episode_start_date, episode_end_date, outcome_date, outcome_concept_id.
    pps_episodes
        PPS episodes with columns: person_id, episode_id,
        episode_start_date, episode_end_date, n_pps_records, category.

    Returns
    -------
    pl.DataFrame
        Merged episodes with columns: person_id, merged_episode_id,
        hip_episode_id, pps_episode_id, category, episode_start_date,
        episode_end_date, outcome_date, outcome_concept_id, n_pps_records,
        source.
    """
    result_schema = {
        "person_id": pl.Int64,
        "merged_episode_id": pl.Int64,
        "hip_episode_id": pl.Int64,
        "pps_episode_id": pl.Int64,
        "category": pl.Utf8,
        "episode_start_date": pl.Date,
        "episode_end_date": pl.Date,
        "outcome_date": pl.Date,
        "outcome_concept_id": pl.Int64,
        "n_pps_records": pl.Int64,
        "source": pl.Utf8,
    }

    has_hip = hip_episodes.height > 0
    has_pps = pps_episodes.height > 0

    if not has_hip and not has_pps:
        log.info("No episodes to merge.")
        return pl.DataFrame(schema=result_schema)

    merged_rows: list[dict] = []
    episode_counter = 0

    # Build HIP lookup by person
    hip_by_person: dict[int, list[dict]] = {}
    if has_hip:
        for row in hip_episodes.iter_rows(named=True):
            pid = row["person_id"]
            hip_by_person.setdefault(pid, []).append(row)

    # Build PPS lookup by person
    pps_by_person: dict[int, list[dict]] = {}
    if has_pps:
        for row in pps_episodes.iter_rows(named=True):
            pid = row["person_id"]
            pps_by_person.setdefault(pid, []).append(row)

    all_persons = set(hip_by_person.keys()) | set(pps_by_person.keys())

    for pid in sorted(all_persons):
        hip_eps = hip_by_person.get(pid, [])
        pps_eps = pps_by_person.get(pid, [])

        # Find overlapping pairs
        matched_hip: set[int] = set()
        matched_pps: set[int] = set()

        # Build overlap matrix
        overlaps: list[tuple[int, int, int]] = []  # (hip_idx, pps_idx, |end_diff|)
        for hi, h in enumerate(hip_eps):
            for pi, p in enumerate(pps_eps):
                # Check temporal overlap
                h_start = h["episode_start_date"]
                h_end = h["episode_end_date"]
                p_start = p["episode_start_date"]
                p_end = p["episode_end_date"]

                if h_start <= p_end and p_start <= h_end:
                    end_diff = abs((h_end - p_end).days)
                    overlaps.append((hi, pi, end_diff))

        # Sort by |end_diff| ascending (best match first)
        overlaps.sort(key=lambda x: x[2])

        # Iterative deduplication: assign best matches
        for _round in range(_MAX_DEDUP_ROUNDS):
            made_assignment = False
            for hi, pi, diff in overlaps:
                if hi not in matched_hip and pi not in matched_pps:
                    matched_hip.add(hi)
                    matched_pps.add(pi)
                    h = hip_eps[hi]
                    p = pps_eps[pi]

                    episode_counter += 1
                    merged_rows.append({
                        "person_id": pid,
                        "merged_episode_id": episode_counter,
                        "hip_episode_id": h["episode_id"],
                        "pps_episode_id": p["episode_id"],
                        "category": h["category"],
                        "episode_start_date": min(
                            h["episode_start_date"], p["episode_start_date"]
                        ),
                        "episode_end_date": max(
                            h["episode_end_date"], p["episode_end_date"]
                        ),
                        "outcome_date": h.get("outcome_date"),
                        "outcome_concept_id": h.get("outcome_concept_id"),
                        "n_pps_records": p.get("n_pps_records", 0),
                        "source": "HIP+PPS",
                    })
                    made_assignment = True
            if not made_assignment:
                break

        # Unmatched HIP episodes
        for hi, h in enumerate(hip_eps):
            if hi not in matched_hip:
                episode_counter += 1
                merged_rows.append({
                    "person_id": pid,
                    "merged_episode_id": episode_counter,
                    "hip_episode_id": h["episode_id"],
                    "pps_episode_id": None,
                    "category": h["category"],
                    "episode_start_date": h["episode_start_date"],
                    "episode_end_date": h["episode_end_date"],
                    "outcome_date": h.get("outcome_date"),
                    "outcome_concept_id": h.get("outcome_concept_id"),
                    "n_pps_records": 0,
                    "source": "HIP",
                })

        # Unmatched PPS episodes
        for pi, p in enumerate(pps_eps):
            if pi not in matched_pps:
                episode_counter += 1
                merged_rows.append({
                    "person_id": pid,
                    "merged_episode_id": episode_counter,
                    "hip_episode_id": None,
                    "pps_episode_id": p["episode_id"],
                    "category": p.get("category", "PREG"),
                    "episode_start_date": p["episode_start_date"],
                    "episode_end_date": p["episode_end_date"],
                    "outcome_date": None,
                    "outcome_concept_id": None,
                    "n_pps_records": p.get("n_pps_records", 0),
                    "source": "PPS",
                })

    if not merged_rows:
        return pl.DataFrame(schema=result_schema)

    result = pl.DataFrame(merged_rows).cast({
        "person_id": pl.Int64,
        "merged_episode_id": pl.Int64,
        "hip_episode_id": pl.Int64,
        "pps_episode_id": pl.Int64,
        "category": pl.Utf8,
        "episode_start_date": pl.Date,
        "episode_end_date": pl.Date,
        "outcome_date": pl.Date,
        "outcome_concept_id": pl.Int64,
        "n_pps_records": pl.Int64,
        "source": pl.Utf8,
    })

    log.info("Merged into %d episodes.", result.height)
    return result
