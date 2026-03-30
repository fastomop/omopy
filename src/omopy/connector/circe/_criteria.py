"""Correlated criteria, temporal windows, and inclusion rule evaluation.

Implements the SQL logic for:
- Temporal window constraints (when correlated events can occur)
- Correlated criteria evaluation (count matching events)
- Criteria group evaluation (ALL/ANY/AT_LEAST/AT_MOST)
- Demographic criteria evaluation (age/gender at index)
- Inclusion rule application with bitmask logic
"""

from __future__ import annotations

from typing import Any

import ibis
import ibis.expr.types as ir

from omopy.connector.circe._types import (
    CorrelatedCriteria,
    CriteriaGroup,
    CriteriaLimit,
    DemographicCriteria,
    InclusionRule,
    NumericRange,
    Occurrence,
    TemporalWindow,
    WindowEndpoint,
)
from omopy.connector.circe._domain_queries import build_domain_query

__all__ = [
    "apply_observation_window",
    "apply_limit",
    "evaluate_criteria_group",
    "apply_inclusion_rules",
]


# ---------------------------------------------------------------------------
# Observation window
# ---------------------------------------------------------------------------


def apply_observation_window(
    events: ir.Table,
    obs_period: ir.Table,
    prior_days: int = 0,
    post_days: int = 0,
) -> ir.Table:
    """Filter events to those within observation periods.

    Parameters
    ----------
    events
        Events table with columns: person_id, event_id, start_date, end_date,
        visit_occurrence_id, sort_date.
    obs_period
        Observation period table with: person_id,
        observation_period_start_date, observation_period_end_date.
    prior_days
        Required days of observation before the event.
    post_days
        Required days of observation after the event.

    Returns
    -------
    ir.Table
        Events enriched with op_start_date and op_end_date columns
        (the observation period boundaries for each event).
    """
    # Join events to observation_period on person_id
    # where event falls within the observation period + required window
    joined = (
        events.join(
            obs_period,
            events.person_id == obs_period.person_id,
        )
        .filter(
            (
                events.start_date
                >= (
                    obs_period.observation_period_start_date
                    + ibis.literal(prior_days).cast("int64") * ibis.interval(days=1)
                )
            )
            & (
                events.start_date
                <= (
                    obs_period.observation_period_end_date
                    - ibis.literal(post_days).cast("int64") * ibis.interval(days=1)
                )
            )
        )
        .select(
            # Keep event columns
            person_id=events.person_id,
            event_id=events.event_id,
            start_date=events.start_date,
            end_date=events.end_date,
            visit_occurrence_id=events.visit_occurrence_id,
            sort_date=events.sort_date,
            # Add observation period boundaries
            op_start_date=obs_period.observation_period_start_date,
            op_end_date=obs_period.observation_period_end_date,
        )
    )

    return joined


# ---------------------------------------------------------------------------
# Limit helpers
# ---------------------------------------------------------------------------


def apply_limit(
    events: ir.Table,
    limit: CriteriaLimit,
) -> ir.Table:
    """Apply a criteria limit (First/Last/All) to events.

    Parameters
    ----------
    events
        Events table with person_id and sort_date columns.
    limit
        The limit specification.

    Returns
    -------
    ir.Table
        Filtered events.
    """
    if limit.type == "All":
        return events

    ascending = limit.type == "First"
    order_col = ibis.asc("sort_date") if ascending else ibis.desc("sort_date")

    return (
        events.mutate(
            _rn=ibis.row_number().over(ibis.window(group_by="person_id", order_by=order_col))
        )
        .filter(ibis._._rn == 0)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Temporal window evaluation
# ---------------------------------------------------------------------------


def _window_bound_expr(
    endpoint: WindowEndpoint,
    index_date: ir.Column,
    op_start: ir.Column,
    op_end: ir.Column,
) -> ir.Column:
    """Compute the date boundary for one end of a temporal window.

    If days is None (unbounded), returns the observation period boundary.
    """
    if endpoint.days is None:
        # Unbounded: use observation period start or end
        if endpoint.coeff == -1:
            return op_start
        else:
            return op_end

    offset = ibis.literal(endpoint.days * endpoint.coeff).cast("int64")
    return index_date + offset * ibis.interval(days=1)


def _apply_temporal_window(
    correlated_events: ir.Table,
    index_events: ir.Table,
    window: TemporalWindow,
    date_col: str = "start_date",
) -> ir.Table:
    """Apply temporal window constraints to correlated events.

    Filters correlated events based on their temporal relationship to
    the index event.

    Parameters
    ----------
    correlated_events
        Table of candidate correlated events with: person_id, event_id,
        start_date, end_date.
    index_events
        Index events with: person_id, event_id, start_date, end_date,
        op_start_date, op_end_date.
    window
        The temporal window specification.
    date_col
        Which date column of the correlated event to compare.
        "start_date" or "end_date".

    Returns
    -------
    ir.Table
        Correlated events filtered by the temporal window, with
        index_event_id added.
    """
    # Determine which index date to reference
    if window.use_index_end:
        ref_date = index_events.end_date
    else:
        ref_date = index_events.start_date

    # Determine which correlated event date to compare
    if window.use_event_end:
        comp_date_col = "end_date"
    else:
        comp_date_col = date_col

    # Compute window boundaries
    win_start = _window_bound_expr(
        window.start,
        ref_date,
        index_events.op_start_date,
        index_events.op_end_date,
    )
    win_end = _window_bound_expr(
        window.end,
        ref_date,
        index_events.op_start_date,
        index_events.op_end_date,
    )

    # Join on person_id and apply window filter
    joined = correlated_events.join(
        index_events,
        correlated_events.person_id == index_events.person_id,
    ).filter(
        (correlated_events[comp_date_col] >= win_start)
        & (correlated_events[comp_date_col] <= win_end)
    )

    return joined


# ---------------------------------------------------------------------------
# Evaluate correlated criteria
# ---------------------------------------------------------------------------


def _evaluate_correlated_criteria(
    cc: CorrelatedCriteria,
    index_events: ir.Table,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
) -> ir.Table:
    """Evaluate a single correlated criterion.

    Returns a table with index event IDs (person_id, event_id) that
    meet the criterion's occurrence requirement.

    Parameters
    ----------
    cc
        The correlated criterion.
    index_events
        Index events with standardised columns + op_start_date, op_end_date.
    con, catalog, cdm_schema, codeset_tables
        Database context for building domain queries.

    Returns
    -------
    ir.Table
        Table with person_id and event_id of matching index events.
    """
    # Build the correlated domain query
    correlated = build_domain_query(
        cc.criteria,
        con,
        catalog,
        cdm_schema,
        codeset_tables,
    )

    # Apply temporal window(s)
    if cc.start_window:
        joined = _apply_temporal_window(correlated, index_events, cc.start_window, "start_date")
    else:
        # No temporal window — just join on person_id
        joined = correlated.join(
            index_events,
            correlated.person_id == index_events.person_id,
        )

    # Apply visit restriction
    if cc.restrict_visit:
        joined = joined.filter(correlated.visit_occurrence_id == index_events.visit_occurrence_id)

    # Count occurrences per index event
    occ = cc.occurrence or Occurrence(type=2, count=1)

    count_col = "correlated_event_id"
    if occ.is_distinct:
        agg_expr = joined.correlated_event_id.nunique()
    else:
        # Count using the correlated event's event_id
        # We need to reference it correctly after the join
        agg_expr = joined.count()

    # Group by index event and count
    counts = joined.group_by([index_events.person_id, index_events.event_id]).agg(cnt=agg_expr)

    # Apply occurrence filter
    if occ.type == 0:  # Exactly
        counts = counts.filter(counts.cnt == occ.count)
    elif occ.type == 1:  # AtMost
        counts = counts.filter(counts.cnt <= occ.count)
    elif occ.type == 2:  # AtLeast
        counts = counts.filter(counts.cnt >= occ.count)

    return counts.select("person_id", "event_id")


# ---------------------------------------------------------------------------
# Evaluate demographic criteria
# ---------------------------------------------------------------------------


def _evaluate_demographic_criteria(
    dc: DemographicCriteria,
    index_events: ir.Table,
    person_tbl: ir.Table,
) -> ir.Table:
    """Evaluate demographic criteria against index events.

    Returns index event IDs (person_id, event_id) that match.
    """
    # Join events to person table
    joined = index_events.join(
        person_tbl.select(
            "person_id",
            "gender_concept_id",
            "year_of_birth",
            *([c] for c in ["race_concept_id", "ethnicity_concept_id"] if c in person_tbl.columns),
        ),
        "person_id",
    )

    filters: list[ir.BooleanValue] = []

    if dc.gender is not None:
        filters.append(joined.gender_concept_id.isin(list(dc.gender.concept_ids)))

    if dc.race is not None and "race_concept_id" in joined.columns:
        filters.append(joined.race_concept_id.isin(list(dc.race.concept_ids)))

    if dc.ethnicity is not None and "ethnicity_concept_id" in joined.columns:
        filters.append(joined.ethnicity_concept_id.isin(list(dc.ethnicity.concept_ids)))

    if dc.age is not None:
        age_expr = (index_events.start_date.year() - joined.year_of_birth).cast("int64")
        from omopy.connector.circe._domain_queries import _numeric_filter

        filters.append(_numeric_filter(age_expr, dc.age))

    if filters:
        combined = filters[0]
        for f in filters[1:]:
            combined = combined & f
        joined = joined.filter(combined)

    return joined.select(
        person_id=index_events.person_id,
        event_id=index_events.event_id,
    )


# ---------------------------------------------------------------------------
# Evaluate criteria group (recursive)
# ---------------------------------------------------------------------------


def evaluate_criteria_group(
    group: CriteriaGroup,
    index_events: ir.Table,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
    person_tbl: ir.Table | None = None,
) -> ir.Table:
    """Evaluate a criteria group against index events.

    Returns the subset of index events that match the group's logic.

    Parameters
    ----------
    group
        The criteria group (ALL, ANY, AT_LEAST, AT_MOST).
    index_events
        Index events to filter.
    con, catalog, cdm_schema, codeset_tables
        Database context.
    person_tbl
        Person table for demographic criteria.

    Returns
    -------
    ir.Table
        Index events matching the group criteria.
    """
    if person_tbl is None:
        person_tbl = con.table("person", database=(catalog, cdm_schema))

    # Collect results from each sub-criterion
    results: list[ir.Table] = []

    # Correlated criteria
    for cc in group.criteria_list:
        matching = _evaluate_correlated_criteria(
            cc,
            index_events,
            con,
            catalog,
            cdm_schema,
            codeset_tables,
        )
        results.append(matching)

    # Demographic criteria
    for dc in group.demographic_criteria_list:
        matching = _evaluate_demographic_criteria(dc, index_events, person_tbl)
        results.append(matching)

    # Nested groups (recursive)
    for sub_group in group.groups:
        matching = evaluate_criteria_group(
            sub_group,
            index_events,
            con,
            catalog,
            cdm_schema,
            codeset_tables,
            person_tbl,
        )
        results.append(matching)

    if not results:
        # No criteria — all events match
        return index_events.select("person_id", "event_id")

    # Combine results based on group type
    index_key = index_events.select("person_id", "event_id")

    if group.type == "ALL":
        # All criteria must be met: INTERSECT
        combined = results[0]
        for r in results[1:]:
            combined = combined.intersect(r)
        return combined

    elif group.type == "ANY":
        # At least one criterion met: UNION
        combined = results[0]
        for r in results[1:]:
            combined = combined.union(r)
        return combined.distinct()

    elif group.type in ("AT_LEAST", "AT_MOST"):
        # Count how many criteria each event passes
        # Tag each result with a criterion index
        tagged: list[ir.Table] = []
        for i, r in enumerate(results):
            tagged.append(r.mutate(criterion_idx=ibis.literal(i)))
        all_matches = tagged[0]
        for t in tagged[1:]:
            all_matches = all_matches.union(t)

        counts = all_matches.group_by(["person_id", "event_id"]).agg(
            criteria_met=all_matches.criterion_idx.nunique()
        )

        if group.type == "AT_LEAST":
            counts = counts.filter(counts.criteria_met >= group.count)
        else:  # AT_MOST
            counts = counts.filter(counts.criteria_met <= group.count)

        return counts.select("person_id", "event_id")

    else:
        msg = f"Unknown criteria group type: {group.type}"
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Inclusion rules (bitmask approach)
# ---------------------------------------------------------------------------


def apply_inclusion_rules(
    events: ir.Table,
    rules: tuple[InclusionRule, ...],
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
    person_tbl: ir.Table | None = None,
) -> tuple[ir.Table, list[ir.Table]]:
    """Apply inclusion rules sequentially using bitmask logic.

    Parameters
    ----------
    events
        Qualified events (with op_start_date, op_end_date).
    rules
        Tuple of inclusion rules to apply.
    con, catalog, cdm_schema, codeset_tables
        Database context.
    person_tbl
        Person table for demographic criteria.

    Returns
    -------
    tuple[ir.Table, list[ir.Table]]
        - Events that pass all inclusion rules
        - List of intermediate results per rule (for attrition)
    """
    if not rules:
        return events, []

    if person_tbl is None:
        person_tbl = con.table("person", database=(catalog, cdm_schema))

    current = events
    rule_results: list[ir.Table] = []

    for rule in rules:
        # Evaluate the rule's criteria group
        matching = evaluate_criteria_group(
            rule.expression,
            current,
            con,
            catalog,
            cdm_schema,
            codeset_tables,
            person_tbl,
        )

        # Semi-join: keep only events that passed the rule
        current = current.filter(current.event_id.isin(matching.event_id))
        rule_results.append(current)

    return current, rule_results
