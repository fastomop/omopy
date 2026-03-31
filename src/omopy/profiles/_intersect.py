"""Core intersection engine — the universal workhorse for PatientProfiles.

The ``_add_intersect()`` function handles all intersection operations:
flag, count, date, days, and field extraction. It is called by the
cohort, table, concept, and death intersection functions.

The algorithm:
1. Add a synthetic ``_row_id`` to the input table for reliable join-back
2. Prepare the overlap table (target table with canonical columns)
3. LEFT JOIN input with overlap on person_id
4. Filter by time window (interval overlap on day offsets)
5. Compute the requested value (flag/count/date/days/field) grouped by ``_row_id``
6. LEFT JOIN result back to original on ``_row_id``, fill missing values
7. Drop ``_row_id``

This is the Python equivalent of R's internal ``.addIntersect()``
function from the PatientProfiles package.
"""

from __future__ import annotations

import math
from typing import Literal

import ibis
import ibis.expr.types as ir

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.profiles._columns import person_id_column
from omopy.profiles._demographics import _get_ibis_table
from omopy.profiles._windows import (
    Window,
    format_name_style,
    window_name,
)

__all__: list[str] = []

# Value types that the intersection engine can compute.
IntersectValue = Literal["flag", "count", "date", "days"] | str

# Internal row-id column name (must not collide with real columns).
_ROW_ID = "_ix_row_id"


def _add_intersect(
    x: CdmTable,
    cdm: CdmReference,
    *,
    target_table: ir.Table,
    target_person_col: str,
    target_start_date: str,
    target_end_date: str | None,
    value: IntersectValue | list[IntersectValue],
    filter_variable: str | None = None,
    filter_id: list[int] | None = None,
    id_name: list[str] | None = None,
    windows: list[Window],
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    in_observation: bool = True,
    order: Literal["first", "last"] = "first",
    name_style: str = "{value}_{id_name}_{window_name}",
) -> CdmTable:
    """Core intersection engine.

    Parameters
    ----------
    x
        Input CDM table to add columns to.
    cdm
        CDM reference for observation_period lookups.
    target_table
        The Ibis table to intersect with (already resolved).
    target_person_col
        Person identifier column in the target table.
    target_start_date
        Start date column in the target table.
    target_end_date
        End date column in the target table. If None, uses start date only.
    value
        What to compute. One or more of: ``"flag"``, ``"count"``,
        ``"date"``, ``"days"``, or a column name from the target table.
    filter_variable
        Column in target to filter on (e.g. ``"cohort_definition_id"``).
    filter_id
        Values to filter to.
    id_name
        Human-readable names for each filter_id.
    windows
        Time windows (already validated).
    index_date
        Column in x containing the reference date.
    censor_date
        Optional column in x for censoring.
    in_observation
        Whether to restrict to in-observation events.
    order
        For date/days/field: ``"first"`` or ``"last"`` event.
    name_style
        Column naming template.

    Returns
    -------
    CdmTable
        Input table with new columns added.
    """
    tbl = _get_ibis_table(x)
    pid = person_id_column(tbl.columns)
    list(tbl.columns)

    # ── Add synthetic row ID for reliable join-back ─────────────────
    tbl_with_id = tbl.mutate(**{_ROW_ID: ibis.row_number()})

    # Normalize value to list
    values = [value] if isinstance(value, str) else list(value)

    # Normalize id_name
    if filter_variable is None or filter_id is None:
        filter_id_list: list[int] = []
        id_name_list: list[str] = ["all"]
    else:
        filter_id_list = list(filter_id)
        if id_name is None:
            id_name_list = [str(i) for i in filter_id_list]
        else:
            id_name_list = list(id_name)

    # ── Phase 1: Prepare the overlap table ──────────────────────────
    overlap = _prepare_overlap_table(
        target_table=target_table,
        target_person_col=target_person_col,
        target_start_date=target_start_date,
        target_end_date=target_end_date,
        filter_variable=filter_variable,
        filter_id=filter_id_list,
        id_name=id_name_list,
        extra_fields=[v for v in values if v not in ("flag", "count", "date", "days")],
    )

    # ── Phase 2: Compute each (value, id_name, window) ──────────────
    result_tbl = tbl_with_id
    for idn in id_name_list:
        # Filter overlap to this id_name
        if len(id_name_list) == 1:
            ov = overlap
        else:
            ov = overlap.filter(overlap["_id_name"] == ibis.literal(idn))

        for w in windows:
            wn = window_name(w)

            # LEFT JOIN input (with row_id) to overlap on person_id
            joined = tbl_with_id.left_join(
                ov,
                tbl_with_id[pid] == ov["_ov_pid"],
            )

            # Compute day offsets
            start_diff = (joined["_ov_start"] - joined[index_date]).cast("int64")
            end_diff = (joined["_ov_end"] - joined[index_date]).cast("int64")

            # Apply window filter on day offsets
            joined = _apply_window_day_filter(joined, start_diff, end_diff, w)

            # Apply censor date filter
            if censor_date is not None:
                joined = joined.filter(
                    joined["_ov_start"].isnull()
                    | (joined["_ov_start"] <= joined[censor_date])
                )

            # Apply in-observation filter
            if in_observation:
                joined = _apply_in_observation_filter(joined, cdm, pid, index_date)

            # Compute each value type for this id_name + window
            for v in values:
                col_name = format_name_style(
                    name_style,
                    value=v,
                    id_name=idn,
                    window_name=wn,
                    table_name=idn,
                    cohort_name=idn,
                    concept_name=idn,
                    field=v if v not in ("flag", "count", "date", "days") else "",
                )

                computed = _compute_value(
                    joined,
                    pid,
                    index_date,
                    v,
                    col_name,
                    order,
                )
                # computed has columns: _ROW_ID, col_name
                result_tbl = _left_join_column(result_tbl, computed, col_name, v)

    # ── Drop the synthetic row ID ───────────────────────────────────
    result_tbl = result_tbl.drop(_ROW_ID)
    return x._with_data(result_tbl)


def _prepare_overlap_table(
    *,
    target_table: ir.Table,
    target_person_col: str,
    target_start_date: str,
    target_end_date: str | None,
    filter_variable: str | None,
    filter_id: list[int],
    id_name: list[str],
    extra_fields: list[str],
) -> ir.Table:
    """Prepare the overlap table with canonical column names."""
    select_dict: dict[str, ir.Column] = {
        "_ov_pid": target_table[target_person_col],
        "_ov_start": target_table[target_start_date],
    }

    if target_end_date is not None and target_end_date != target_start_date:
        select_dict["_ov_end"] = target_table[target_end_date].fill_null(
            target_table[target_start_date]
        )
    else:
        select_dict["_ov_end"] = target_table[target_start_date]

    # Extra fields for field-type intersections
    for field in extra_fields:
        if field in target_table.columns:
            select_dict[f"_ov_{field}"] = target_table[field]

    # Apply filter
    if filter_variable is not None and filter_id:
        cases = [
            (
                target_table[filter_variable].cast("int64") == ibis.literal(fid),
                idn,
            )
            for fid, idn in zip(filter_id, id_name, strict=False)
        ]
        select_dict["_id_name"] = ibis.cases(*cases, else_=ibis.null().cast("string"))

        result = target_table.select(**select_dict).filter(
            lambda t: t["_id_name"].notnull()
        )
    else:
        select_dict["_id_name"] = ibis.literal(id_name[0] if id_name else "all")
        result = target_table.select(**select_dict)

    return result


def _apply_window_day_filter(
    joined: ir.Table,
    start_diff: ir.Column,
    end_diff: ir.Column,
    window: Window,
) -> ir.Table:
    """Filter joined table to rows where event interval overlaps window.

    Interval overlap: event [start_diff, end_diff] overlaps window [lo, hi].
    Condition: end_diff >= lo AND start_diff <= hi
    (Also allows NULL from left join to pass through.)
    """
    lo, hi = window

    filters = []
    if not math.isinf(lo):
        lo_lit = ibis.literal(int(lo))
        filters.append(joined["_ov_pid"].isnull() | (end_diff >= lo_lit))
    if not math.isinf(hi):
        hi_lit = ibis.literal(int(hi))
        filters.append(joined["_ov_pid"].isnull() | (start_diff <= hi_lit))

    if filters:
        combined = filters[0]
        for f in filters[1:]:
            combined = combined & f
        return joined.filter(combined)
    return joined


def _apply_in_observation_filter(
    joined: ir.Table,
    cdm: CdmReference,
    pid: str,
    index_date: str,
) -> ir.Table:
    """Filter to events within the person's observation period.

    Requires: obs_start <= event_end AND event_start <= obs_end
    AND obs_start <= index_date AND index_date <= obs_end
    """
    obs = _get_ibis_table(cdm["observation_period"]).select(
        _obs_pid2=ibis._.person_id,
        _obs_start2=ibis._.observation_period_start_date,
        _obs_end2=ibis._.observation_period_end_date,
    )

    # Join with observation period
    joined2 = joined.left_join(obs, joined[pid] == obs["_obs_pid2"])

    # Filter: index date within obs period AND event within obs period
    joined2 = joined2.filter(
        joined2["_obs_start2"].isnull()
        | (
            (joined2["_obs_start2"] <= joined2[index_date])
            & (joined2[index_date] <= joined2["_obs_end2"])
            & (
                joined2["_ov_pid"].isnull()
                | (
                    (joined2["_obs_start2"] <= joined2["_ov_end"])
                    & (joined2["_ov_start"] <= joined2["_obs_end2"])
                )
            )
        )
    )

    # Drop obs columns
    keep_cols = [c for c in joined2.columns if not c.startswith("_obs_")]
    return joined2.select(*keep_cols)


def _compute_value(
    joined: ir.Table,
    pid: str,
    index_date: str,
    value_type: str,
    col_name: str,
    order: str,
) -> ir.Table:
    """Compute a single value column from the joined table.

    Returns a table with just ``_ROW_ID`` and ``col_name``.
    All grouping is done by ``_ROW_ID`` to guarantee one result per input row.
    """
    if value_type == "flag":
        # Flag: 1 if any match, 0 otherwise
        flag_expr = ibis.cases(
            (joined["_ov_pid"].notnull(), ibis.literal(1)),
            else_=ibis.literal(0),
        )
        result = joined.group_by(_ROW_ID).agg(**{col_name: flag_expr.max()})
        return result

    elif value_type == "count":
        # Count non-null _ov_pid entries per row
        has_match = ibis.cases(
            (joined["_ov_pid"].notnull(), ibis.literal(1)),
            else_=ibis.literal(0),
        )
        result = joined.group_by(_ROW_ID).agg(**{col_name: has_match.sum()})
        return result

    elif value_type == "date":
        # Date of the first/last matching event.
        # Strategy: compute day offset, take min/max, store as int.
        # We convert back to date in the join-back step using the original index_date.
        diff_expr = (joined["_ov_start"] - joined[index_date]).cast("int64")
        # Only count non-null overlaps
        diff_when_matched = ibis.cases(
            (joined["_ov_pid"].notnull(), diff_expr),
            else_=ibis.null().cast("int64"),
        )
        if order == "first":
            agg_expr = diff_when_matched.min()
        else:
            agg_expr = diff_when_matched.max()

        result = joined.group_by(_ROW_ID).agg(
            _date_diff=agg_expr,
        )
        # Convert day offset back to actual date.
        # We need the index_date from the original row — join back to get it,
        # or carry it through. Simpler: carry _ROW_ID + index_date through
        # the aggregation by including it in a secondary step.
        #
        # Actually, since all rows in a group share the same _ROW_ID and the
        # same index_date value, we can just take any(index_date):
        result2 = joined.group_by(_ROW_ID).agg(
            _date_diff=agg_expr,
            _idx_date=joined[index_date].max(),  # all same value per group
        )
        result2 = result2.mutate(
            **{
                col_name: ibis.cases(
                    (
                        result2["_date_diff"].notnull(),
                        (
                            result2["_idx_date"]
                            + result2["_date_diff"] * ibis.interval(days=1)
                        ).cast("date"),
                    ),
                    else_=ibis.null().cast("date"),
                )
            }
        )
        return result2.select(_ROW_ID, col_name)

    elif value_type == "days":
        # Days from index date to first/last matching event
        diff_expr = (joined["_ov_start"] - joined[index_date]).cast("int64")
        diff_when_matched = ibis.cases(
            (joined["_ov_pid"].notnull(), diff_expr),
            else_=ibis.null().cast("int64"),
        )
        if order == "first":
            agg_expr = diff_when_matched.min()
        else:
            agg_expr = diff_when_matched.max()

        result = joined.group_by(_ROW_ID).agg(**{col_name: agg_expr})
        return result

    else:
        # Field: extract a specific column value from the first/last match
        field_col = f"_ov_{value_type}"
        if field_col not in joined.columns:
            msg = (
                f"Field '{value_type}' not found in target table. "
                f"Available overlay columns: "
                f"{[c for c in joined.columns if c.startswith('_ov_')]}"
            )
            raise ValueError(msg)

        # Filter to matched rows only, rank by day offset
        matched = joined.filter(joined["_ov_pid"].notnull())
        diff_expr = (matched["_ov_start"] - matched[index_date]).cast("int64")

        if order == "first":
            row_num = ibis.row_number().over(
                group_by=[_ROW_ID],
                order_by=diff_expr,
            )
        else:
            row_num = ibis.row_number().over(
                group_by=[_ROW_ID],
                order_by=ibis.desc(diff_expr),
            )

        matched = matched.mutate(_rn=row_num)
        matched = matched.filter(matched["_rn"] == 0)  # 0-indexed
        return matched.select(_ROW_ID, **{col_name: matched[field_col]})


def _left_join_column(
    tbl: ir.Table,
    computed: ir.Table,
    col_name: str,
    value_type: str,
) -> ir.Table:
    """Left join a computed column back to the main table via ``_ROW_ID``.

    Fills missing with appropriate defaults:
    - flag/count: 0
    - date/days/field: NULL
    """
    # Rename the row-id in computed to avoid Ibis join ambiguity
    computed_r = computed.select(
        **{f"{_ROW_ID}_r": computed[_ROW_ID], col_name: computed[col_name]}
    )

    result = tbl.left_join(
        computed_r,
        tbl[_ROW_ID] == computed_r[f"{_ROW_ID}_r"],
    )

    # Fill missing and select only tbl columns + the new column
    if value_type in ("flag", "count"):
        fill_expr = result[col_name].fill_null(ibis.literal(0))
    else:
        fill_expr = result[col_name]

    # Select all original tbl columns plus the new one
    select_cols = {c: result[c] for c in tbl.columns}
    select_cols[col_name] = fill_expr
    return result.select(**select_cols)
