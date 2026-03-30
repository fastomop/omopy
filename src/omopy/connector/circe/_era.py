"""Era collapse — merge overlapping/adjacent cohort periods.

Implements the gaps-and-islands algorithm used by CIRCE to collapse
overlapping or nearby cohort periods into contiguous eras.

Algorithm (matching the R/SQL implementation):
1. Pad each end_date by ``era_pad`` days.
2. Detect new eras: a row starts a new era if its start_date is after the
   running maximum of padded end_dates for previous rows (per person).
3. Assign a group index via cumulative sum of the ``is_start`` flag.
4. MIN(start_date) / MAX(end_date) per group — then un-pad the final
   end_date by subtracting ``era_pad`` days.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

__all__ = ["collapse_eras"]


def collapse_eras(
    cohort_rows: ir.Table,
    era_pad: int = 0,
) -> ir.Table:
    """Collapse overlapping cohort periods into non-overlapping eras.

    Parameters
    ----------
    cohort_rows
        Table with columns: ``person_id``, ``start_date``, ``end_date``.
    era_pad
        Number of days to use as a gap tolerance.  Periods separated by
        ≤ ``era_pad`` days will be merged.

    Returns
    -------
    ir.Table
        Collapsed table with columns: ``person_id``, ``start_date``,
        ``end_date``.
    """
    if era_pad < 0:
        msg = f"era_pad must be non-negative, got {era_pad}"
        raise ValueError(msg)

    pad = ibis.literal(era_pad).cast("int64")

    # Step 1: Pad end_date
    padded = cohort_rows.mutate(
        padded_end=cohort_rows.end_date + pad * ibis.interval(days=1),
    )

    # Step 2: Detect new era starts.
    # A row starts a new era when its start_date is greater than the running
    # max of padded_end for all preceding rows (per person).
    # Use a window: MAX(padded_end) OVER (PARTITION BY person_id
    #   ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    prev_max_end = padded.padded_end.max().over(
        ibis.window(
            group_by="person_id",
            order_by="start_date",
            preceding=(None, 1),  # UNBOUNDED PRECEDING to 1 PRECEDING
            following=None,  # no following rows
        )
    )

    marked = padded.mutate(
        is_start=ibis.cases(
            (prev_max_end >= padded.start_date, 0),
            else_=1,
        ),
    )

    # Step 3: Cumulative sum of is_start → group index
    # ORDER BY start_date, is_start DESC  (the DESC ensures that if two rows
    # share the same start_date, the one flagged as is_start=1 comes first,
    # so the cumsum is correct.)
    group_idx = marked.is_start.sum().over(
        ibis.window(
            group_by="person_id",
            order_by=[ibis.asc("start_date"), ibis.desc("is_start")],
            preceding=(None, 0),  # UNBOUNDED PRECEDING to CURRENT ROW
            following=None,
        )
    )

    grouped = marked.mutate(group_idx=group_idx)

    # Step 4: Aggregate per group, then un-pad the end date
    result = grouped.group_by(["person_id", "group_idx"]).agg(
        start_date=grouped.start_date.min(),
        end_date=grouped.padded_end.max(),
    )

    # Un-pad: subtract era_pad from the final end date
    result = result.mutate(
        end_date=result.end_date - pad * ibis.interval(days=1),
    ).drop("group_idx")

    return result
