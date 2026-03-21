"""End strategy computation and censoring.

Determines when cohort membership ends for each qualifying event.
Three options:
1. **DateOffset**: event start/end date + N days (capped at observation period end)
2. **CustomEra**: end of drug/condition era containing the event
3. **Default**: observation period end date

Censoring criteria (if present) provide additional end dates. The final
end date for each event is the *earliest* across the chosen strategy and
all censoring sources.
"""

from __future__ import annotations

from typing import Any

import ibis
import ibis.expr.types as ir

from omopy.connector.circe._types import (
    CohortExpression,
    CustomEraStrategy,
    DateOffsetStrategy,
    DomainCriteria,
    EndStrategy,
)
from omopy.connector.circe._domain_queries import build_domain_query

__all__ = ["compute_cohort_end_dates"]


def compute_cohort_end_dates(
    included_events: ir.Table,
    expression: CohortExpression,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
) -> ir.Table:
    """Compute the cohort end date for every included event.

    The algorithm mirrors CIRCE:
    1. Collect candidate end dates from the end strategy (or default).
    2. Collect candidate end dates from censoring criteria.
    3. UNION ALL candidates and pick the *earliest* per (person_id, event_id)
       that is >= event start_date.

    Parameters
    ----------
    included_events
        Events that passed all inclusion rules.  Must have columns:
        ``person_id``, ``event_id``, ``start_date``, ``end_date``,
        ``op_start_date``, ``op_end_date``.
    expression
        Parsed cohort expression.
    con, catalog, cdm_schema, codeset_tables
        Database context.

    Returns
    -------
    ir.Table
        Table with columns ``person_id``, ``event_id``, ``start_date``,
        ``end_date`` where ``end_date`` is the computed cohort end date.
    """
    end_parts: list[ir.Table] = []

    strategy = expression.end_strategy
    has_custom_end = False

    if strategy is not None:
        if strategy.date_offset is not None:
            has_custom_end = True
            end_parts.append(
                _date_offset_end(included_events, strategy.date_offset)
            )
        elif strategy.custom_era is not None:
            has_custom_end = True
            end_parts.append(
                _custom_era_end(
                    included_events,
                    strategy.custom_era,
                    con,
                    catalog,
                    cdm_schema,
                    codeset_tables,
                )
            )

    if not has_custom_end:
        # Default: observation period end
        end_parts.append(
            included_events.select(
                event_id=included_events.event_id,
                person_id=included_events.person_id,
                end_date=included_events.op_end_date,
            )
        )

    # Censoring criteria
    for censor_crit in expression.censoring_criteria:
        end_parts.append(
            _censoring_end(
                included_events,
                censor_crit,
                con,
                catalog,
                cdm_schema,
                codeset_tables,
            )
        )

    # UNION ALL candidate end dates
    if len(end_parts) == 1:
        all_ends = end_parts[0]
    else:
        all_ends = end_parts[0]
        for p in end_parts[1:]:
            all_ends = all_ends.union(p)

    # Join back to included events, keep only ends >= start_date,
    # then pick earliest per event.
    #
    # Ibis 12 requires that join predicates reference the *immediate*
    # relations being joined. We alias both sides to avoid ambiguity.
    left = included_events.select("person_id", "event_id", "start_date")
    # Rename all_ends columns to avoid collision with left
    right = all_ends.rename(
        right_person_id="person_id",
        right_event_id="event_id",
    )

    joined = left.join(
        right,
        (left.person_id == right.right_person_id)
        & (left.event_id == right.right_event_id),
    ).filter(
        lambda t: t.end_date >= t.start_date,
    ).select("person_id", "event_id", "start_date", "end_date")

    # Pick earliest end date per (person_id, event_id)
    result = (
        joined.group_by(["person_id", "event_id", "start_date"])
        .agg(end_date=joined.end_date.min())
    )

    return result


# ---------------------------------------------------------------------------
# DateOffset end strategy
# ---------------------------------------------------------------------------


def _date_offset_end(
    events: ir.Table,
    date_offset: DateOffsetStrategy,
) -> ir.Table:
    """Compute end date via DateOffset strategy.

    ``end_date = event.{start_date|end_date} + offset``
    capped at ``op_end_date``.
    """
    if date_offset.date_field == "StartDate":
        base_date = events.start_date
    else:
        base_date = events.end_date

    offset_days = ibis.literal(date_offset.offset).cast("int64")
    computed_end = base_date + offset_days * ibis.interval(days=1)

    # Cap at observation period end
    capped = ibis.cases(
        (computed_end > events.op_end_date, events.op_end_date),
        else_=computed_end,
    )

    return events.select(
        event_id=events.event_id,
        person_id=events.person_id,
        end_date=capped,
    )


# ---------------------------------------------------------------------------
# CustomEra end strategy
# ---------------------------------------------------------------------------


def _custom_era_end(
    events: ir.Table,
    custom_era: CustomEraStrategy,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
) -> ir.Table:
    """Compute end date using a drug/condition era containing the event.

    Joins included events to the drug_era (or condition_era) table where
    the event start falls within the era, the era's drug matches the
    codeset, and picks the earliest era end date.
    """
    # Determine which era table to use based on whether we have a drug codeset
    # The CustomEraStrategy stores a drug_codeset_id which references a concept set.
    # In the R impl it can also be a condition codeset; here we check for the
    # presence of drug_era first, condition_era as fallback.
    codeset_id = custom_era.drug_codeset_id
    codeset_tbl = codeset_tables.get(codeset_id)

    # Try drug_era first
    try:
        drug_era = con.table("drug_era", database=(catalog, cdm_schema))
        use_drug = True
    except Exception:
        use_drug = False

    if use_drug:
        era_tbl = drug_era
        era_concept_col = "drug_concept_id"
        era_start_col = "drug_era_start_date"
        era_end_col = "drug_era_end_date"
    else:
        # Fall back to condition_era
        era_tbl = con.table("condition_era", database=(catalog, cdm_schema))
        era_concept_col = "condition_concept_id"
        era_start_col = "condition_era_start_date"
        era_end_col = "condition_era_end_date"

    # Join: event start falls within era, and era concept is in the codeset.
    # Rename era_tbl columns to avoid collision with events.
    era_renamed = era_tbl.rename(
        era_person_id="person_id",
    )

    joined = events.join(
        era_renamed,
        (events.person_id == era_renamed.era_person_id)
        & (events.start_date >= era_renamed[era_start_col])
        & (events.start_date <= era_renamed[era_end_col]),
    )

    # Filter to matching codeset concepts
    if codeset_tbl is not None:
        joined = joined.filter(
            joined[era_concept_col].isin(codeset_tbl.concept_id)
        )

    # Cap era end at op_end_date
    era_end = joined[era_end_col]
    capped_end = ibis.cases(
        (era_end > joined.op_end_date, joined.op_end_date),
        else_=era_end,
    )

    # Apply offset
    offset_days = ibis.literal(custom_era.offset).cast("int64")
    final_end = capped_end + offset_days * ibis.interval(days=1)

    selected = joined.select(
        event_id=joined.event_id,
        person_id=joined.person_id,
        end_date=final_end,
    )

    # Pick earliest end date per event (an event may overlap multiple eras)
    return (
        selected.group_by(["event_id", "person_id"])
        .agg(end_date=selected.end_date.min())
    )


# ---------------------------------------------------------------------------
# Censoring criteria
# ---------------------------------------------------------------------------


def _censoring_end(
    included_events: ir.Table,
    censor_criteria: DomainCriteria,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
) -> ir.Table:
    """Compute censoring end date from a censoring criterion.

    For each included event, finds the earliest occurrence of the censoring
    event that falls on or after the event start_date and within the
    observation period. The censoring event start_date becomes the end_date.
    """
    # Build the censoring events query
    censor_events = build_domain_query(
        censor_criteria,
        con,
        catalog,
        cdm_schema,
        codeset_tables,
    )

    # Join: censoring event start_date >= included event start_date
    # and <= included event op_end_date.
    # Rename censor_events columns to avoid collision.
    left = included_events.select(
        "person_id", "event_id", "start_date", "op_end_date",
    )
    right = censor_events.rename(
        censor_person_id="person_id",
        censor_start_date="start_date",
    )

    joined = left.join(
        right,
        left.person_id == right.censor_person_id,
    ).filter(
        lambda t: (t.censor_start_date >= t.start_date)
        & (t.censor_start_date <= t.op_end_date),
    ).select(
        person_id=lambda t: t.person_id,
        event_id=lambda t: t.event_id,
        end_date=lambda t: t.censor_start_date,
    )

    # Pick earliest censoring date per event
    return (
        joined.group_by(["event_id", "person_id"])
        .agg(end_date=joined.end_date.min())
    )
