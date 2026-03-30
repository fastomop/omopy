"""ESD algorithm — Episode Start Date refinement.

Refines the inferred pregnancy start date using gestational-week (GW)
measurements and gestational-range (GR3m — trimester) evidence from the
CDM.  Also harmonises the final outcome category when HIP and PPS
disagree, and computes precision / concordance metrics.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import polars as pl

from omopy.pregnancy._concepts import (
    ESD_CONCEPTS,
    GR3M_MONTH_RANGES,
    MATCHO_TERM_DURATIONS,
)

__all__ = ["_run_esd"]

log = logging.getLogger(__name__)


def _run_esd(
    merged_episodes: pl.DataFrame,
    esd_records: pl.DataFrame,
) -> pl.DataFrame:
    """Run the ESD (Episode Start Date) refinement algorithm.

    Parameters
    ----------
    merged_episodes
        Merged episodes from ``_merge_hipps()`` with columns including
        person_id, merged_episode_id, category, episode_start_date,
        episode_end_date, outcome_date, source.
    esd_records
        ESD concept records with columns: person_id, concept_id,
        record_date, value_as_number, esd_category, esd_domain.

    Returns
    -------
    pl.DataFrame
        Refined episodes with additional columns: esd_start_date,
        gestational_age_weeks, precision, final_start_date.
    """
    if merged_episodes.height == 0:
        log.info("No episodes for ESD refinement.")
        return merged_episodes.with_columns(
            pl.lit(None, dtype=pl.Date).alias("esd_start_date"),
            pl.lit(None, dtype=pl.Float64).alias("gestational_age_weeks"),
            pl.lit("low", dtype=pl.Utf8).alias("precision"),
            pl.lit(None, dtype=pl.Date).alias("final_start_date"),
        )

    # Process each episode
    esd_starts: list = []
    gest_ages: list = []
    precisions: list = []

    for row in merged_episodes.iter_rows(named=True):
        pid = row["person_id"]
        ep_start = row["episode_start_date"]
        ep_end = row["episode_end_date"]

        esd_start = None
        gest_age = None
        precision = "low"

        if esd_records.height > 0:
            # Find ESD records for this person within episode window
            # Use a generous window: ep_start - 30 days to ep_end + 30 days
            window_start = ep_start - timedelta(days=30)
            window_end = ep_end + timedelta(days=30)

            person_esd = esd_records.filter(
                (pl.col("person_id") == pid)
                & (pl.col("record_date") >= window_start)
                & (pl.col("record_date") <= window_end)
            )

            if person_esd.height > 0:
                # Try GW (gestational week) evidence first
                gw_records = person_esd.filter(pl.col("esd_category") == "GW")

                if gw_records.height > 0:
                    # Use records that have a numeric value
                    gw_with_val = gw_records.filter(pl.col("value_as_number").is_not_null())

                    if gw_with_val.height > 0:
                        # Pick the record closest to outcome/end date
                        gw_with_val = gw_with_val.sort("record_date", descending=True)
                        best = gw_with_val.row(0, named=True)
                        weeks = best["value_as_number"]
                        rec_date = best["record_date"]

                        if weeks is not None and 1 <= weeks <= 45:
                            esd_start = rec_date - timedelta(days=int(weeks * 7))
                            gest_age = float(weeks)
                            precision = "high"

                # Try GR3m (gestational range / trimester) evidence
                if esd_start is None:
                    gr_records = person_esd.filter(pl.col("esd_category") == "GR3m")

                    if gr_records.height > 0:
                        # Collect trimester evidence
                        month_ranges: list[tuple[int, int]] = []
                        for gr_row in gr_records.iter_rows(named=True):
                            cid = gr_row["concept_id"]
                            if cid in GR3M_MONTH_RANGES:
                                month_ranges.append(GR3M_MONTH_RANGES[cid])

                        if month_ranges:
                            # Intersect ranges to find best estimate
                            all_mins = [r[0] for r in month_ranges]
                            all_maxs = [r[1] for r in month_ranges]
                            range_min = max(all_mins)
                            range_max = min(all_maxs)

                            if range_min <= range_max:
                                midpoint_months = (range_min + range_max) / 2.0
                            else:
                                midpoint_months = (min(all_mins) + max(all_maxs)) / 2.0

                            # Use the earliest GR record date
                            earliest_gr = gr_records.sort("record_date").row(0, named=True)
                            rec_date = earliest_gr["record_date"]
                            days_offset = int(midpoint_months * 30)
                            esd_start = rec_date - timedelta(days=days_offset)
                            gest_age = midpoint_months * 4.33  # approx weeks
                            precision = "medium"

        esd_starts.append(esd_start)
        gest_ages.append(gest_age)
        precisions.append(precision)

    # Compute final_start_date: prefer ESD, then fall back to original
    final_starts = []
    for i, row in enumerate(merged_episodes.iter_rows(named=True)):
        if esd_starts[i] is not None:
            final_starts.append(esd_starts[i])
        else:
            # Fall back to category-based estimate
            cat = row["category"]
            outcome_dt = row.get("outcome_date")
            if outcome_dt is not None and cat in MATCHO_TERM_DURATIONS:
                _min_d, max_d = MATCHO_TERM_DURATIONS[cat]
                final_starts.append(outcome_dt - timedelta(days=max_d))
            else:
                final_starts.append(row["episode_start_date"])

    # Build result
    result = merged_episodes.with_columns(
        pl.Series("esd_start_date", esd_starts, dtype=pl.Date),
        pl.Series("gestational_age_weeks", gest_ages, dtype=pl.Float64),
        pl.Series("precision", precisions, dtype=pl.Utf8),
        pl.Series("final_start_date", final_starts, dtype=pl.Date),
    )

    log.info("ESD refinement complete for %d episodes.", result.height)
    return result
