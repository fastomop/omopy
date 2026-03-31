"""Concept-based cohort generation.

Generates cohorts from concept sets by looking up clinical events in the
appropriate domain tables (condition_occurrence, drug_exposure, etc.),
then applying observation period constraints and collapsing overlapping
periods.

This is the Python equivalent of R's ``generateConceptCohortSet()``.
"""

from __future__ import annotations

import contextlib
from typing import Any, Literal

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.connector.db_source import DbSource
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist, ConceptSetExpression
from omopy.generics.cohort_table import CohortTable

__all__ = ["generate_concept_cohort_set"]


# ---------------------------------------------------------------------------
# Domain → table mapping
# ---------------------------------------------------------------------------

DOMAIN_TABLE_MAP: dict[str, dict[str, str]] = {
    "condition": {
        "table": "condition_occurrence",
        "concept_id_col": "condition_concept_id",
        "start_date_col": "condition_start_date",
        "end_date_col": "condition_end_date",
    },
    "drug": {
        "table": "drug_exposure",
        "concept_id_col": "drug_concept_id",
        "start_date_col": "drug_exposure_start_date",
        "end_date_col": "drug_exposure_end_date",
    },
    "procedure": {
        "table": "procedure_occurrence",
        "concept_id_col": "procedure_concept_id",
        "start_date_col": "procedure_date",
        "end_date_col": "procedure_date",
    },
    "observation": {
        "table": "observation",
        "concept_id_col": "observation_concept_id",
        "start_date_col": "observation_date",
        "end_date_col": "observation_date",
    },
    "measurement": {
        "table": "measurement",
        "concept_id_col": "measurement_concept_id",
        "start_date_col": "measurement_date",
        "end_date_col": "measurement_date",
    },
    "visit": {
        "table": "visit_occurrence",
        "concept_id_col": "visit_concept_id",
        "start_date_col": "visit_start_date",
        "end_date_col": "visit_end_date",
    },
    "device": {
        "table": "device_exposure",
        "concept_id_col": "device_concept_id",
        "start_date_col": "device_exposure_start_date",
        "end_date_col": "device_exposure_end_date",
    },
}


def generate_concept_cohort_set(
    cdm: CdmReference,
    concept_set: Codelist | ConceptSetExpression | dict[str, list[int]],
    name: str,
    *,
    limit: Literal["first", "all"] = "first",
    required_observation: tuple[int, int] = (0, 0),
    end: Literal["observation_period_end_date", "event_end_date"]
    | int = "observation_period_end_date",
) -> CdmReference:
    """Generate a cohort from concept sets.

    Each concept set becomes one cohort. The function:

    1. Resolves concept IDs (optionally including descendants)
    2. Looks up clinical events in the appropriate domain tables
    3. Constrains to observation periods
    4. Applies required observation time before/after index
    5. Applies limit (first occurrence or all)
    6. Collapses overlapping periods
    7. Creates a CohortTable with settings, attrition, and codelist

    Parameters
    ----------
    cdm
        A CdmReference with database-backed tables.
    concept_set
        Concept sets to generate cohorts from. Can be:
        - ``Codelist``: named mapping of concept ID lists
        - ``ConceptSetExpression``: with descendant/exclude flags
        - ``dict[str, list[int]]``: simple named list of concept IDs
    name
        Name for the cohort table in the CDM.
    limit
        ``"first"`` (default) keeps only the first event per person per cohort.
        ``"all"`` keeps all events.
    required_observation
        ``(prior_days, future_days)`` — minimum observation time before and
        after the index date for inclusion.
    end
        How to set ``cohort_end_date``:
        - ``"observation_period_end_date"`` (default): use observation period end
        - ``"event_end_date"``: use the clinical event's end date
        - ``int``: fixed number of days after the event start date

    Returns
    -------
    CdmReference
        The CDM with a new CohortTable added under ``cdm[name]``.
    """
    if limit not in ("first", "all"):
        msg = f"limit must be 'first' or 'all', got {limit!r}"
        raise ValueError(msg)

    # Normalize concept_set to a standard form
    concept_defs = _normalize_concept_set(concept_set)

    # We need Ibis access — get the source
    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "generate_concept_cohort_set requires a database-backed CDM (DbSource)"
        raise TypeError(msg)
    con = source.connection

    # Build concept lookup table
    # (cohort_definition_id, concept_id, include_descendants, is_excluded)
    concept_rows = _build_concept_rows(concept_defs)

    # Upload as temp table to the database
    concept_arrow = _concept_rows_to_arrow(concept_rows)
    temp_name = f"__omopy_concept_tmp_{name}"
    _register_arrow_temp(con, temp_name, concept_arrow, source.cdm_schema)

    try:
        # Resolve descendants
        concepts_tbl = _resolve_concepts(con, temp_name, cdm, concept_defs, source)

        # Get domains present in the data
        domains = _get_domains(concepts_tbl, con)

        if not domains:
            # No matching concepts — return empty cohort
            return _empty_cohort(
                cdm, name, concept_defs, limit, required_observation, end
            )

        # Look up events from each domain table
        events = _gather_domain_events(cdm, concepts_tbl, domains, con, source)

        if events is None:
            return _empty_cohort(
                cdm, name, concept_defs, limit, required_observation, end
            )

        # Apply observation period constraints
        cohort_tbl = _apply_observation_constraints(
            events, cdm, required_observation, end, limit, con, source
        )

        # Materialise and build CohortTable
        return _build_cohort_result(
            cdm,
            name,
            cohort_tbl,
            concept_defs,
            concept_rows,
            limit,
            required_observation,
            end,
            con,
            source,
        )
    finally:
        _drop_temp(con, temp_name, source.cdm_schema)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_concept_set(
    concept_set: Codelist | ConceptSetExpression | dict[str, list[int]],
) -> list[dict[str, Any]]:
    """Normalize various concept set inputs to a uniform list.

    Returns a list of dicts with keys:
    - cohort_definition_id (int, 1-based)
    - cohort_name (str)
    - concepts: list of (concept_id, include_descendants, is_excluded)
    """
    result = []

    if isinstance(concept_set, ConceptSetExpression):
        for idx, (cname, entries) in enumerate(concept_set.items(), start=1):
            concepts = [
                (e.concept_id, e.include_descendants, e.is_excluded) for e in entries
            ]
            result.append(
                {
                    "cohort_definition_id": idx,
                    "cohort_name": cname,
                    "concepts": concepts,
                }
            )
    elif isinstance(concept_set, Codelist):
        for idx, (cname, ids) in enumerate(concept_set.items(), start=1):
            concepts = [(cid, False, False) for cid in ids]
            result.append(
                {
                    "cohort_definition_id": idx,
                    "cohort_name": cname,
                    "concepts": concepts,
                }
            )
    elif isinstance(concept_set, dict):
        for idx, (cname, ids) in enumerate(concept_set.items(), start=1):
            concepts = [(int(cid), False, False) for cid in ids]
            result.append(
                {
                    "cohort_definition_id": idx,
                    "cohort_name": cname,
                    "concepts": concepts,
                }
            )
    else:
        msg = (
            "concept_set must be Codelist, ConceptSetExpression,"
            f" or dict, got {type(concept_set).__name__}"
        )
        raise TypeError(msg)

    return result


def _build_concept_rows(
    concept_defs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flatten concept definitions into per-row dicts for upload."""
    rows = []
    for cdef in concept_defs:
        for concept_id, include_desc, is_excluded in cdef["concepts"]:
            rows.append(
                {
                    "cohort_definition_id": cdef["cohort_definition_id"],
                    "cohort_name": cdef["cohort_name"],
                    "concept_id": concept_id,
                    "include_descendants": include_desc,
                    "is_excluded": is_excluded,
                }
            )
    return rows


def _concept_rows_to_arrow(rows: list[dict[str, Any]]) -> Any:
    """Convert concept rows to a PyArrow table for upload."""
    import pyarrow as pa

    if not rows:
        schema = pa.schema(
            [
                ("cohort_definition_id", pa.int64()),
                ("cohort_name", pa.string()),
                ("concept_id", pa.int64()),
                ("include_descendants", pa.bool_()),
                ("is_excluded", pa.bool_()),
            ]
        )
        return pa.table(
            {name: [] for name in schema.names},
            schema=schema,
        )

    return pa.table(
        {
            "cohort_definition_id": pa.array(
                [r["cohort_definition_id"] for r in rows], type=pa.int64()
            ),
            "cohort_name": [r["cohort_name"] for r in rows],
            "concept_id": pa.array([r["concept_id"] for r in rows], type=pa.int64()),
            "include_descendants": [r["include_descendants"] for r in rows],
            "is_excluded": [r["is_excluded"] for r in rows],
        }
    )


def _register_arrow_temp(
    con: Any, name: str, arrow_table: Any, cdm_schema: str
) -> None:
    """Register an Arrow table as a temporary table in the database."""
    native = con.con  # DuckDB native connection
    native.register(name, arrow_table)


def _drop_temp(con: Any, name: str, cdm_schema: str) -> None:
    """Clean up temporary table."""
    with contextlib.suppress(Exception):
        con.con.unregister(name)


def _resolve_concepts(
    con: Any,
    temp_name: str,
    cdm: CdmReference,
    concept_defs: list[dict[str, Any]],
    source: Any,
) -> ir.Table:
    """Resolve concept IDs, expanding descendants if needed.

    Returns an Ibis table with (cohort_definition_id, concept_id, domain_id).
    """
    catalog = source._catalog
    schema = source.cdm_schema

    # Base concepts from the temp table
    base = con.table(temp_name)

    has_descendants = any(
        inc_desc for cdef in concept_defs for _, inc_desc, _ in cdef["concepts"]
    )

    if has_descendants:
        # Expand descendants
        ancestor = con.table("concept_ancestor", database=(catalog, schema))

        desc_concepts = (
            base.filter(base.include_descendants == True)  # noqa: E712
            .join(ancestor, base.concept_id == ancestor.ancestor_concept_id)
            .select(
                cohort_definition_id=base.cohort_definition_id,
                concept_id=ancestor.descendant_concept_id.cast("int64"),
                is_excluded=base.is_excluded,
            )
        )

        base_selected = base.select(
            cohort_definition_id=base.cohort_definition_id,
            concept_id=base.concept_id.cast("int64"),
            is_excluded=base.is_excluded,
        )

        all_concepts = base_selected.union(desc_concepts)
    else:
        all_concepts = base.select("cohort_definition_id", "concept_id", "is_excluded")

    # Filter out excluded concepts and join with concept table for domain_id
    concept_tbl = con.table("concept", database=(catalog, schema))
    concept_lookup = concept_tbl.select(
        concept_id=concept_tbl.concept_id.cast("int64"),
        domain_id=concept_tbl.domain_id,
    )

    resolved = (
        all_concepts.filter(all_concepts.is_excluded == False)  # noqa: E712
        .join(
            concept_lookup,
            "concept_id",
        )
        .select("cohort_definition_id", "concept_id", "domain_id")
        .distinct()
    )

    return resolved


def _get_domains(concepts_tbl: ir.Table, con: Any) -> list[str]:
    """Get the list of domains present in the resolved concepts."""
    domain_result = concepts_tbl.select("domain_id").distinct().to_pyarrow()
    domains = [
        str(d).lower()
        for d in domain_result.column("domain_id").to_pylist()
        if d is not None
    ]
    return [d for d in domains if d in DOMAIN_TABLE_MAP]


def _gather_domain_events(
    cdm: CdmReference,
    concepts_tbl: ir.Table,
    domains: list[str],
    con: Any,
    source: Any,
) -> ir.Table | None:
    """Look up clinical events across domain tables and union them."""
    catalog = source._catalog
    schema = source.cdm_schema
    parts: list[ir.Table] = []

    for domain in domains:
        info = DOMAIN_TABLE_MAP[domain]
        table_name = info["table"]

        if table_name not in cdm:
            continue

        domain_tbl = con.table(table_name, database=(catalog, schema))

        # Filter concepts for this domain
        domain_concepts = concepts_tbl.filter(concepts_tbl.domain_id.lower() == domain)

        # Join events with matching concepts (cast to int64 for type consistency)
        concept_id_expr = domain_tbl[info["concept_id_col"]].cast("int64")
        joined = domain_tbl.join(
            domain_concepts,
            concept_id_expr == domain_concepts.concept_id,
        )

        # Select cohort columns
        events = joined.select(
            cohort_definition_id=joined.cohort_definition_id,
            subject_id=joined.person_id,
            cohort_start_date=joined[info["start_date_col"]],
            cohort_end_date=ibis.coalesce(
                joined[info["end_date_col"]],
                joined[info["start_date_col"]],
            ),
        )

        # Filter out records where end < start
        events = events.filter(events.cohort_start_date <= events.cohort_end_date)

        parts.append(events)

    if not parts:
        return None

    result = parts[0]
    for p in parts[1:]:
        result = result.union(p)

    return result


def _apply_observation_constraints(
    events: ir.Table,
    cdm: CdmReference,
    required_observation: tuple[int, int],
    end: str | int,
    limit: str,
    con: Any,
    source: Any,
) -> ir.Table:
    """Apply observation period constraints, end date strategy, limit, and collapse."""
    catalog = source._catalog
    schema = source.cdm_schema

    obs_period = con.table("observation_period", database=(catalog, schema))
    obs = obs_period.select(
        subject_id=obs_period.person_id,
        observation_period_start_date=obs_period.observation_period_start_date,
        observation_period_end_date=obs_period.observation_period_end_date,
    )

    # Join with observation period
    cohort = events.join(obs, "subject_id")

    # Event start must be within observation period
    cohort = cohort.filter(
        (cohort.observation_period_start_date <= cohort.cohort_start_date)
        & (cohort.cohort_start_date <= cohort.observation_period_end_date)
    )

    # Required prior observation
    prior_days, future_days = required_observation
    if prior_days > 0:
        cohort = cohort.filter(
            (cohort.cohort_start_date - cohort.observation_period_start_date).cast(
                "int64"
            )
            >= prior_days
        )

    if future_days > 0:
        cohort = cohort.filter(
            (cohort.observation_period_end_date - cohort.cohort_start_date).cast(
                "int64"
            )
            >= future_days
        )

    # Apply end date strategy
    if end == "observation_period_end_date":
        cohort = cohort.mutate(
            cohort_end_date=cohort.observation_period_end_date,
        )
    elif isinstance(end, int):
        cohort = cohort.mutate(
            cohort_end_date=(cohort.cohort_start_date + ibis.interval(days=end)),
        )
    # else "event_end_date" — keep as-is

    # Cap end date at observation period end
    cohort = cohort.mutate(
        cohort_end_date=ibis.least(
            cohort.cohort_end_date,
            cohort.observation_period_end_date,
        ),
    )

    cohort = cohort.select(
        "cohort_definition_id",
        "subject_id",
        "cohort_start_date",
        "cohort_end_date",
    )

    # Apply limit
    if limit == "first":
        cohort = (
            cohort.group_by("cohort_definition_id", "subject_id")
            .order_by("cohort_start_date")
            .mutate(rn=ibis.row_number())
            .filter(ibis._.rn == 0)
            .drop("rn")
        )

    # Collapse overlapping periods
    cohort = _collapse_cohort(cohort)

    return cohort


def _collapse_cohort(cohort: ir.Table) -> ir.Table:
    """Collapse overlapping cohort periods using a gaps-and-islands approach.

    Merges overlapping or adjacent time intervals for the same
    (cohort_definition_id, subject_id) group. Uses a cumulative-max
    gap detection technique:

    1. Order events by start date within each group.
    2. For each row, compute the max end date of all *preceding* rows.
    3. A new island starts when start_date > max_prev_end (a gap exists).
    4. Cumulative sum of island-start flags gives island IDs.
    5. Aggregate each island to (min start, max end).

    Note: for ``limit="first"`` (one row per person/cohort), this is
    effectively a no-op since there are no overlapping intervals.
    """
    grp_window = ibis.window(
        group_by=["cohort_definition_id", "subject_id"],
        order_by="cohort_start_date",
    )

    # Step 1: running max of end date for all preceding rows (excluding current)
    # We use lag(1) of the cumulative max to get max-end-of-all-prior-rows.
    cum_max_end = cohort.cohort_end_date.max().over(
        ibis.window(
            group_by=["cohort_definition_id", "subject_id"],
            order_by="cohort_start_date",
            following=0,
        )
    )
    cohort = cohort.mutate(_cum_max_end=cum_max_end)

    # lag(1) of cumulative max gives the max end date of all rows before current
    prev_max_end = cohort._cum_max_end.lag(1).over(grp_window)
    cohort = cohort.mutate(_prev_max_end=prev_max_end)

    # Step 2: mark new islands
    cohort = cohort.mutate(
        _is_new=ibis.cases(
            (cohort._prev_max_end.isnull(), 1),
            (cohort.cohort_start_date > cohort._prev_max_end, 1),
            else_=0,
        )
    )

    # Step 3: cumulative sum of is_new gives island_id
    island_id = cohort._is_new.sum().over(grp_window)
    cohort = cohort.mutate(_island_id=island_id)

    # Step 4: aggregate per island
    collapsed = (
        cohort.group_by("cohort_definition_id", "subject_id", "_island_id")
        .agg(
            cohort_start_date=cohort.cohort_start_date.min(),
            cohort_end_date=cohort.cohort_end_date.max(),
        )
        .drop("_island_id")
    )

    return collapsed


def _build_cohort_result(
    cdm: CdmReference,
    name: str,
    cohort_tbl: ir.Table,
    concept_defs: list[dict[str, Any]],
    concept_rows: list[dict[str, Any]],
    limit: str,
    required_observation: tuple[int, int],
    end: str | int,
    con: Any,
    source: Any,
) -> CdmReference:
    """Materialise the cohort and build CohortTable with metadata."""
    # Materialise to Polars
    cohort_arrow = cohort_tbl.to_pyarrow()
    cohort_df = pl.from_arrow(cohort_arrow)

    # Ensure proper dtypes
    cohort_df = cohort_df.cast(
        {
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        }
    )

    # Build settings
    settings_rows = []
    for cdef in concept_defs:
        settings_rows.append(
            {
                "cohort_definition_id": cdef["cohort_definition_id"],
                "cohort_name": cdef["cohort_name"],
                "limit": limit,
                "prior_observation": required_observation[0],
                "future_observation": required_observation[1],
                "end": str(end),
            }
        )
    settings_df = pl.DataFrame(settings_rows).cast(
        {
            "cohort_definition_id": pl.Int64,
        }
    )

    # Build attrition
    counts = cohort_df.group_by("cohort_definition_id").agg(
        pl.len().alias("number_records"),
        pl.col("subject_id").n_unique().alias("number_subjects"),
    )
    attrition_rows = []
    for cdef in concept_defs:
        cid = cdef["cohort_definition_id"]
        match = counts.filter(pl.col("cohort_definition_id") == cid)
        if len(match) > 0:
            nr = match["number_records"][0]
            ns = match["number_subjects"][0]
        else:
            nr, ns = 0, 0
        attrition_rows.append(
            {
                "cohort_definition_id": cid,
                "number_records": nr,
                "number_subjects": ns,
                "reason_id": 1,
                "reason": "Initial qualifying events",
                "excluded_records": 0,
                "excluded_subjects": 0,
            }
        )
    attrition_df = pl.DataFrame(attrition_rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "number_records": pl.Int64,
            "number_subjects": pl.Int64,
            "reason_id": pl.Int64,
            "excluded_records": pl.Int64,
            "excluded_subjects": pl.Int64,
        }
    )

    # Build cohort codelist
    codelist_rows = []
    for row in concept_rows:
        codelist_rows.append(
            {
                "cohort_definition_id": row["cohort_definition_id"],
                "codelist_name": row["cohort_name"],
                "concept_id": row["concept_id"],
                "codelist_type": "index event",
            }
        )
    codelist_df = (
        pl.DataFrame(codelist_rows).cast(
            {
                "cohort_definition_id": pl.Int64,
                "concept_id": pl.Int64,
            }
        )
        if codelist_rows
        else pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "codelist_name": pl.Utf8,
                "concept_id": pl.Int64,
                "codelist_type": pl.Utf8,
            }
        )
    )

    # Also write the cohort table to the database write schema (best-effort;
    # silently skipped if the connection is read-only or no write schema)
    with contextlib.suppress(Exception):
        _write_cohort_to_db(cohort_df, name, con, source)

    # Create CohortTable
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


def _write_cohort_to_db(
    cohort_df: pl.DataFrame,
    name: str,
    con: Any,
    source: Any,
) -> None:
    """Write the cohort DataFrame to the database write schema."""
    arrow_table = cohort_df.to_arrow()
    write_schema = source.write_schema
    fqn = f'"{write_schema}"."{name}"'

    with contextlib.suppress(Exception):
        con.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{write_schema}"')

    temp_reg = f"__omopy_cohort_{name}"
    native = con.con
    native.register(temp_reg, arrow_table)
    try:
        con.raw_sql(f"DROP TABLE IF EXISTS {fqn}")
        con.raw_sql(f'CREATE TABLE {fqn} AS SELECT * FROM "{temp_reg}"')
    finally:
        with contextlib.suppress(Exception):
            native.unregister(temp_reg)


def _empty_cohort(
    cdm: CdmReference,
    name: str,
    concept_defs: list[dict[str, Any]],
    limit: str,
    required_observation: tuple[int, int],
    end: str | int,
) -> CdmReference:
    """Return a CDM with an empty CohortTable when no concepts match."""
    empty_df = pl.DataFrame(
        schema={
            "cohort_definition_id": pl.Int64,
            "subject_id": pl.Int64,
            "cohort_start_date": pl.Date,
            "cohort_end_date": pl.Date,
        }
    )
    settings_rows = []
    for cdef in concept_defs:
        settings_rows.append(
            {
                "cohort_definition_id": cdef["cohort_definition_id"],
                "cohort_name": cdef["cohort_name"],
                "limit": limit,
                "prior_observation": required_observation[0],
                "future_observation": required_observation[1],
                "end": str(end),
            }
        )
    settings_df = pl.DataFrame(settings_rows).cast(
        {
            "cohort_definition_id": pl.Int64,
        }
    )

    attrition_rows = []
    for cdef in concept_defs:
        attrition_rows.append(
            {
                "cohort_definition_id": cdef["cohort_definition_id"],
                "number_records": 0,
                "number_subjects": 0,
                "reason_id": 1,
                "reason": "Initial qualifying events",
                "excluded_records": 0,
                "excluded_subjects": 0,
            }
        )
    attrition_df = pl.DataFrame(attrition_rows).cast(
        {
            "cohort_definition_id": pl.Int64,
            "number_records": pl.Int64,
            "number_subjects": pl.Int64,
            "reason_id": pl.Int64,
            "excluded_records": pl.Int64,
            "excluded_subjects": pl.Int64,
        }
    )

    cohort_table = CohortTable(
        data=empty_df,
        tbl_name=name,
        tbl_source="local",
        settings=settings_df,
        attrition=attrition_df,
    )
    cdm[name] = cohort_table
    return cdm
