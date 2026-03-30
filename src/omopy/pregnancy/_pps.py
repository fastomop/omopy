"""PPS algorithm — gestational-timing pregnancy identification.

Identifies pregnancy episodes from gestational-timing concepts (prenatal
visits, screenings, ultrasounds, lab tests) that carry information about
*when* in a pregnancy they typically occur.

For each person, records are walked forwards.  A new episode is started
when a gap exceeds 300 days (10 months) or when the record's expected
gestational timing disagrees with the current episode.  Episodes longer
than 365 days are discarded.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import polars as pl

__all__ = ["_run_pps"]

log = logging.getLogger(__name__)

# Maximum gap (days) before forcing a new episode
_MAX_GAP_DAYS: int = 300

# Tolerance in months for timing agreement
_TIMING_TOLERANCE_MONTHS: int = 2

# Maximum episode length (days) — episodes longer than this are removed
_MAX_EPISODE_DAYS: int = 365

# Minimum gap (days) to consider starting a new episode on disagreement
_MIN_DISAGREEMENT_GAP_DAYS: int = 30


def _run_pps(
    pps_records: pl.DataFrame,
) -> pl.DataFrame:
    """Run the PPS gestational-timing algorithm.

    Parameters
    ----------
    pps_records
        DataFrame with columns: person_id, concept_id, record_date,
        min_month, max_month.

    Returns
    -------
    pl.DataFrame
        Pregnancy episodes with columns: person_id, episode_id,
        episode_start_date, episode_end_date, n_pps_records, category.
    """
    result_schema = {
        "person_id": pl.Int64,
        "episode_id": pl.Int64,
        "episode_start_date": pl.Date,
        "episode_end_date": pl.Date,
        "n_pps_records": pl.Int64,
        "category": pl.Utf8,
    }

    if pps_records.height == 0:
        log.info("No PPS records to process.")
        return pl.DataFrame(schema=result_schema)

    episodes: list[dict] = []
    episode_counter = 0

    persons = pps_records["person_id"].unique().sort().to_list()

    for pid in persons:
        precs = pps_records.filter(pl.col("person_id") == pid).sort("record_date")
        if precs.height == 0:
            continue

        dates = precs["record_date"].to_list()
        min_months = precs["min_month"].to_list()
        max_months = precs["max_month"].to_list()

        # Start first episode
        ep_start = dates[0]
        ep_end = dates[0]
        ep_count = 1

        for j in range(1, len(dates)):
            dt = dates[j]
            gap_days = (dt - ep_end).days

            # Force new episode if gap > 10 months
            if gap_days > _MAX_GAP_DAYS:
                # Emit current episode
                episode_counter += 1
                episodes.append(
                    {
                        "person_id": pid,
                        "episode_id": episode_counter,
                        "episode_start_date": ep_start,
                        "episode_end_date": ep_end,
                        "n_pps_records": ep_count,
                        "category": "PREG",
                    }
                )
                ep_start = dt
                ep_end = dt
                ep_count = 1
                continue

            # Check timing agreement
            elapsed_days = (dt - ep_start).days
            elapsed_months = elapsed_days / 30.0

            expected_min = min_months[j]
            expected_max = max_months[j]

            if expected_min is not None and expected_max is not None:
                # Does the elapsed time agree with the expected gestational month?
                agrees = elapsed_months >= (
                    expected_min - _TIMING_TOLERANCE_MONTHS
                ) and elapsed_months <= (expected_max + _TIMING_TOLERANCE_MONTHS)

                if not agrees and gap_days > _MIN_DISAGREEMENT_GAP_DAYS:
                    # Timing disagrees and there's a meaningful gap — new episode
                    episode_counter += 1
                    episodes.append(
                        {
                            "person_id": pid,
                            "episode_id": episode_counter,
                            "episode_start_date": ep_start,
                            "episode_end_date": ep_end,
                            "n_pps_records": ep_count,
                            "category": "PREG",
                        }
                    )
                    ep_start = dt
                    ep_end = dt
                    ep_count = 1
                    continue

            # Record agrees or tolerance holds — extend current episode
            ep_end = dt
            ep_count += 1

        # Emit final episode for this person
        episode_counter += 1
        episodes.append(
            {
                "person_id": pid,
                "episode_id": episode_counter,
                "episode_start_date": ep_start,
                "episode_end_date": ep_end,
                "n_pps_records": ep_count,
                "category": "PREG",
            }
        )

    if not episodes:
        return pl.DataFrame(schema=result_schema)

    result = pl.DataFrame(episodes).cast(
        {
            "person_id": pl.Int64,
            "episode_id": pl.Int64,
            "episode_start_date": pl.Date,
            "episode_end_date": pl.Date,
            "n_pps_records": pl.Int64,
            "category": pl.Utf8,
        }
    )

    # Remove episodes longer than 365 days
    result = result.with_columns(
        ((pl.col("episode_end_date") - pl.col("episode_start_date")).dt.total_days()).alias(
            "_duration"
        )
    )
    n_before = result.height
    result = result.filter(pl.col("_duration") <= _MAX_EPISODE_DAYS).drop("_duration")
    n_removed = n_before - result.height
    if n_removed > 0:
        log.info("PPS removed %d episodes exceeding %d days.", n_removed, _MAX_EPISODE_DAYS)

    log.info("PPS produced %d episodes.", result.height)
    return result
