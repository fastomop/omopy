"""CIRCE cohort generation engine.

Orchestrates the full CIRCE pipeline:

1. Parse JSON → ``CohortExpression``
2. Resolve concept sets against the database
3. Build primary events from domain criteria
4. Apply observation window filter
5. Apply primary criteria limit (First/Last/All)
6. Apply additional criteria (if any)
7. Apply qualified limit
8. Apply inclusion rules (sequential filtering)
9. Apply expression limit
10. Compute cohort end dates (strategy + censoring)
11. Collapse overlapping periods (era-ification)
12. Apply censor window (hard date clamp)
13. Build CohortTable with settings, attrition, and codelist
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cohort_table import CohortTable
from omopy.connector.db_source import DbSource

from omopy.connector.circe._types import CohortExpression, CriteriaLimit
from omopy.connector.circe._parser import (
    parse_cohort_expression,
    parse_cohort_json,
    read_cohort_set,
)
from omopy.connector.circe._concept_resolver import resolve_concept_sets
from omopy.connector.circe._domain_queries import build_domain_query
from omopy.connector.circe._criteria import (
    apply_inclusion_rules,
    apply_limit,
    apply_observation_window,
    evaluate_criteria_group,
)
from omopy.connector.circe._end_strategy import compute_cohort_end_dates
from omopy.connector.circe._era import collapse_eras

__all__ = ["generate_cohort_set"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_cohort_set(
    cdm: CdmReference,
    cohort_set: dict[str, Any] | list[dict[str, Any]] | str | Path,
    name: str = "cohort",
) -> CdmReference:
    """Generate cohorts from CIRCE JSON definitions.

    Parameters
    ----------
    cdm
        A database-backed CdmReference (from ``cdm_from_con``).
    cohort_set
        One of:
        - A single CIRCE JSON dict (with ``id``, ``name``, ``json``)
        - A list of such dicts
        - A path to a directory of ``*.json`` files
        - A path to a single ``*.json`` file
        - A JSON string
    name
        Name for the cohort table in the CDM.

    Returns
    -------
    CdmReference
        The CDM with a new :class:`CohortTable` added under ``cdm[name]``.
    """
    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "generate_cohort_set requires a database-backed CDM (DbSource)"
        raise TypeError(msg)

    con = source.connection
    catalog = source.catalog
    cdm_schema = source.cdm_schema

    # Normalise cohort_set into a list of {cohort_definition_id, cohort_name, expression}
    definitions = _normalise_cohort_set(cohort_set)

    if not definitions:
        msg = "No cohort definitions provided"
        raise ValueError(msg)

    # Build each cohort
    all_rows: list[pl.DataFrame] = []
    all_settings: list[dict[str, Any]] = []
    all_attrition: list[pl.DataFrame] = []

    for defn in definitions:
        cohort_id = defn["cohort_definition_id"]
        cohort_name = defn["cohort_name"]
        expression = defn["expression"]

        rows, attrition = _generate_single_cohort(
            expression=expression,
            cohort_definition_id=cohort_id,
            con=con,
            catalog=catalog,
            cdm_schema=cdm_schema,
        )

        all_rows.append(rows)
        all_settings.append(
            {"cohort_definition_id": cohort_id, "cohort_name": cohort_name}
        )
        all_attrition.append(attrition)

    # Combine results
    if all_rows:
        cohort_df = pl.concat(all_rows, how="vertical_relaxed")
    else:
        cohort_df = pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "subject_id": pl.Int64,
                "cohort_start_date": pl.Date,
                "cohort_end_date": pl.Date,
            }
        )

    settings_df = pl.DataFrame(all_settings).cast(
        {"cohort_definition_id": pl.Int64}
    )

    attrition_df = pl.concat(all_attrition, how="vertical_relaxed")

    # Codelist — for CIRCE, we store concept set info
    codelist_df = _build_codelist(definitions)

    cohort_table = CohortTable(
        data=cohort_df,
        tbl_name=name,
        tbl_source=source.source_type,
        settings=settings_df,
        attrition=attrition_df,
        cohort_codelist=codelist_df,
    )

    cdm[name] = cohort_table
    return cdm


# ---------------------------------------------------------------------------
# Single cohort generation
# ---------------------------------------------------------------------------


def _generate_single_cohort(
    expression: CohortExpression,
    cohort_definition_id: int,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the full CIRCE pipeline for one cohort definition.

    Returns (cohort_rows, attrition) as Polars DataFrames.
    """
    attrition_steps: list[dict[str, Any]] = []

    def _record_attrition(
        reason: str,
        events: ir.Table | None,
        prev_count: int,
    ) -> int:
        """Record an attrition step and return the current count."""
        if events is not None:
            try:
                current = events.count().execute()
            except Exception:
                current = 0
        else:
            current = 0
        excluded = prev_count - current
        attrition_steps.append(
            {
                "cohort_definition_id": cohort_definition_id,
                "number_records": current,
                "number_subjects": current,  # approx; more precise would need distinct
                "reason_id": len(attrition_steps) + 1,
                "reason": reason,
                "excluded_records": excluded,
                "excluded_subjects": excluded,
            }
        )
        return current

    # -----------------------------------------------------------------------
    # Step 1: Resolve concept sets
    # -----------------------------------------------------------------------
    codeset_tables = resolve_concept_sets(
        expression.concept_sets, con, catalog, cdm_schema
    )

    # -----------------------------------------------------------------------
    # Step 2: Build primary events (UNION of all domain criteria)
    # -----------------------------------------------------------------------
    primary = expression.primary_criteria
    event_parts: list[ir.Table] = []

    for dc in primary.criteria_list:
        part = build_domain_query(
            dc, con, catalog, cdm_schema, codeset_tables
        )
        event_parts.append(part)

    if not event_parts:
        # No primary criteria — empty cohort
        return _empty_result(cohort_definition_id, attrition_steps)

    # Union all domain criteria events
    events = event_parts[0]
    for p in event_parts[1:]:
        events = events.union(p)

    # Assign sequential event_id via row_number (needed after union)
    events = events.mutate(
        event_id=ibis.row_number().over(
            ibis.window(order_by="sort_date")
        )
    )

    count = _record_attrition("Initial events", events, 0)
    if count == 0:
        return _empty_result(cohort_definition_id, attrition_steps)

    # -----------------------------------------------------------------------
    # Step 3: Apply observation window
    # -----------------------------------------------------------------------
    obs_period = con.table("observation_period", database=(catalog, cdm_schema))

    events = apply_observation_window(
        events,
        obs_period,
        prior_days=primary.observation_window.prior_days,
        post_days=primary.observation_window.post_days,
    )

    count = _record_attrition("Observation window filter", events, count)
    if count == 0:
        return _empty_result(cohort_definition_id, attrition_steps)

    # -----------------------------------------------------------------------
    # Step 4: Apply primary criteria limit
    # -----------------------------------------------------------------------
    events = apply_limit(events, primary.primary_limit)

    count = _record_attrition("Primary criteria limit", events, count)

    # -----------------------------------------------------------------------
    # Step 5: Apply additional criteria (before qualified limit)
    # -----------------------------------------------------------------------
    if expression.additional_criteria is not None:
        person_tbl = con.table("person", database=(catalog, cdm_schema))
        matching = evaluate_criteria_group(
            expression.additional_criteria,
            events,
            con,
            catalog,
            cdm_schema,
            codeset_tables,
            person_tbl,
        )
        events = events.filter(events.event_id.isin(matching.event_id))
        count = _record_attrition("Additional criteria", events, count)
        if count == 0:
            return _empty_result(cohort_definition_id, attrition_steps)

    # -----------------------------------------------------------------------
    # Step 6: Apply qualified limit
    # -----------------------------------------------------------------------
    events = apply_limit(events, expression.qualified_limit)

    count = _record_attrition("Qualified limit", events, count)

    # -----------------------------------------------------------------------
    # Step 7: Apply inclusion rules
    # -----------------------------------------------------------------------
    if expression.inclusion_rules:
        events, rule_intermediates = apply_inclusion_rules(
            events,
            expression.inclusion_rules,
            con,
            catalog,
            cdm_schema,
            codeset_tables,
        )

        # Record attrition for each rule
        for i, rule in enumerate(expression.inclusion_rules):
            count = _record_attrition(
                f"Inclusion rule: {rule.name}",
                rule_intermediates[i] if i < len(rule_intermediates) else events,
                count,
            )

        if count == 0:
            return _empty_result(cohort_definition_id, attrition_steps)

    # -----------------------------------------------------------------------
    # Step 8: Apply expression limit
    # -----------------------------------------------------------------------
    events = apply_limit(events, expression.expression_limit)

    count = _record_attrition("Expression limit", events, count)

    # These are the "included events"
    included_events = events

    # -----------------------------------------------------------------------
    # Step 9: Compute end dates (strategy + censoring)
    # -----------------------------------------------------------------------
    cohort_rows = compute_cohort_end_dates(
        included_events,
        expression,
        con,
        catalog,
        cdm_schema,
        codeset_tables,
    )

    # -----------------------------------------------------------------------
    # Step 10: Collapse overlapping periods (era-ification)
    # -----------------------------------------------------------------------
    era_pad = expression.collapse_settings.era_pad

    collapsed = collapse_eras(
        cohort_rows.select("person_id", "start_date", "end_date"),
        era_pad=era_pad,
    )

    # -----------------------------------------------------------------------
    # Step 11: Apply censor window (hard date clamp)
    # -----------------------------------------------------------------------
    cw = expression.censor_window
    if cw.start_date is not None or cw.end_date is not None:
        collapsed = _apply_censor_window(collapsed, cw.start_date, cw.end_date)

    # -----------------------------------------------------------------------
    # Step 12: Materialise to Polars DataFrame
    # -----------------------------------------------------------------------
    final = collapsed.select(
        person_id=collapsed.person_id,
        start_date=collapsed.start_date,
        end_date=collapsed.end_date,
    )

    try:
        result_df = final.to_polars()
    except AttributeError:
        # Ibis backend may not have to_polars; use execute
        result_df = pl.from_pandas(final.execute())

    # Shape into standard cohort columns
    if result_df.is_empty():
        cohort_df = pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "subject_id": pl.Int64,
                "cohort_start_date": pl.Date,
                "cohort_end_date": pl.Date,
            }
        )
    else:
        cohort_df = result_df.select(
            cohort_definition_id=pl.lit(cohort_definition_id).cast(pl.Int64),
            subject_id=pl.col("person_id").cast(pl.Int64),
            cohort_start_date=pl.col("start_date").cast(pl.Date),
            cohort_end_date=pl.col("end_date").cast(pl.Date),
        ).filter(
            # Remove rows where end < start (can happen after censor window)
            pl.col("cohort_end_date") >= pl.col("cohort_start_date")
        )

    _record_attrition("Final cohort", None, count)
    # Fix the last step — use actual row count
    if attrition_steps:
        attrition_steps[-1]["number_records"] = len(cohort_df)
        attrition_steps[-1]["number_subjects"] = cohort_df["subject_id"].n_unique() if len(cohort_df) > 0 else 0
        attrition_steps[-1]["excluded_records"] = count - len(cohort_df)
        attrition_steps[-1]["excluded_subjects"] = count - len(cohort_df)

    attrition_df = pl.DataFrame(attrition_steps).cast(
        {
            "cohort_definition_id": pl.Int64,
            "number_records": pl.Int64,
            "number_subjects": pl.Int64,
            "reason_id": pl.Int64,
            "excluded_records": pl.Int64,
            "excluded_subjects": pl.Int64,
        }
    )

    return cohort_df, attrition_df


# ---------------------------------------------------------------------------
# Censor window
# ---------------------------------------------------------------------------


def _apply_censor_window(
    cohort: ir.Table,
    start_date: str | None,
    end_date: str | None,
) -> ir.Table:
    """Clamp cohort start/end dates to the censor window boundaries.

    - If censor window has a start_date, cohort start_date is clamped upward.
    - If censor window has an end_date, cohort end_date is clamped downward.
    - Rows where start > end after clamping are removed.
    """
    result = cohort

    if start_date is not None:
        cw_start = ibis.literal(start_date).cast("date")
        result = result.mutate(
            start_date=ibis.cases(
                (result.start_date > cw_start, result.start_date),
                else_=cw_start,
            ),
        )

    if end_date is not None:
        cw_end = ibis.literal(end_date).cast("date")
        result = result.mutate(
            end_date=ibis.cases(
                (result.end_date < cw_end, result.end_date),
                else_=cw_end,
            ),
        )

    # Remove rows where end < start
    result = result.filter(result.end_date >= result.start_date)

    return result


# ---------------------------------------------------------------------------
# Input normalisation
# ---------------------------------------------------------------------------


def _normalise_cohort_set(
    cohort_set: dict[str, Any] | list[dict[str, Any]] | str | Path,
) -> list[dict[str, Any]]:
    """Normalise various input forms into a list of cohort definitions.

    Each entry has keys: ``cohort_definition_id``, ``cohort_name``,
    ``expression`` (CohortExpression).
    """
    # String: could be a path or JSON
    if isinstance(cohort_set, str):
        p = Path(cohort_set)
        if p.is_dir():
            return read_cohort_set(p)
        elif p.is_file():
            return read_cohort_set(p.parent)  # treat as directory
        else:
            # Assume it's a JSON string
            expr = parse_cohort_json(cohort_set)
            return [
                {
                    "cohort_definition_id": 1,
                    "cohort_name": "cohort_1",
                    "expression": expr,
                }
            ]

    if isinstance(cohort_set, Path):
        if cohort_set.is_dir():
            return read_cohort_set(cohort_set)
        elif cohort_set.is_file():
            text = cohort_set.read_text()
            expr = parse_cohort_json(text)
            return [
                {
                    "cohort_definition_id": 1,
                    "cohort_name": cohort_set.stem,
                    "expression": expr,
                }
            ]
        else:
            msg = f"Path does not exist: {cohort_set}"
            raise FileNotFoundError(msg)

    # Single dict
    if isinstance(cohort_set, dict):
        return [_normalise_single_defn(cohort_set, idx=1)]

    # List of dicts
    if isinstance(cohort_set, list):
        return [
            _normalise_single_defn(d, idx=i + 1) for i, d in enumerate(cohort_set)
        ]

    msg = f"Unsupported cohort_set type: {type(cohort_set)}"
    raise TypeError(msg)


def _normalise_single_defn(
    d: dict[str, Any],
    idx: int,
) -> dict[str, Any]:
    """Normalise a single cohort definition dict."""
    # Accept multiple key formats
    cid = d.get("cohort_definition_id") or d.get("id") or idx
    cname = d.get("cohort_name") or d.get("name") or f"cohort_{cid}"

    # Expression can be a CohortExpression, a dict, or a JSON string
    expr = d.get("expression") or d.get("json")
    if expr is None:
        msg = f"Cohort definition {cid} missing 'expression' or 'json' key"
        raise ValueError(msg)

    if isinstance(expr, str):
        expr = parse_cohort_json(expr)
    elif isinstance(expr, dict):
        expr = parse_cohort_expression(expr)
    elif not isinstance(expr, CohortExpression):
        msg = f"Unsupported expression type: {type(expr)}"
        raise TypeError(msg)

    return {
        "cohort_definition_id": cid,
        "cohort_name": cname,
        "expression": expr,
    }


# ---------------------------------------------------------------------------
# Codelist builder
# ---------------------------------------------------------------------------


def _build_codelist(
    definitions: list[dict[str, Any]],
) -> pl.DataFrame:
    """Build a codelist DataFrame from parsed definitions."""
    rows: list[dict[str, Any]] = []

    for defn in definitions:
        cid = defn["cohort_definition_id"]
        expr: CohortExpression = defn["expression"]
        for cs in expr.concept_sets:
            for item in cs.items:
                rows.append(
                    {
                        "cohort_definition_id": cid,
                        "codelist_name": cs.name,
                        "concept_id": item.concept.concept_id,
                        "codelist_type": "concept_set",
                    }
                )

    if not rows:
        return pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "codelist_name": pl.Utf8,
                "concept_id": pl.Int64,
                "codelist_type": pl.Utf8,
            }
        )

    return pl.DataFrame(rows).cast(
        {"cohort_definition_id": pl.Int64, "concept_id": pl.Int64}
    )


# ---------------------------------------------------------------------------
# Empty result helper
# ---------------------------------------------------------------------------


def _empty_result(
    cohort_definition_id: int,
    attrition_steps: list[dict[str, Any]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return empty cohort + attrition for a cohort with no qualifying events."""
    cohort_df = pl.DataFrame(
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        }
    )

    if attrition_steps:
        attrition_df = pl.DataFrame(attrition_steps).cast(
            {
                "cohort_definition_id": pl.Int64,
                "number_records": pl.Int64,
                "number_subjects": pl.Int64,
                "reason_id": pl.Int64,
                "excluded_records": pl.Int64,
                "excluded_subjects": pl.Int64,
            }
        )
    else:
        attrition_df = pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "number_records": pl.Int64,
                "number_subjects": pl.Int64,
                "reason_id": pl.Int64,
                "reason": pl.Utf8,
                "excluded_records": pl.Int64,
                "excluded_subjects": pl.Int64,
            }
        )

    return cohort_df, attrition_df
