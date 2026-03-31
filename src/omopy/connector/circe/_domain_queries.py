"""Domain-specific Ibis query builders for CIRCE criteria.

Each domain (ConditionOccurrence, DrugExposure, etc.) gets its events
from the corresponding CDM table, applying codeset filters, date filters,
person filters, etc.

All builders return Ibis Table expressions with standardised columns:
``person_id``, ``event_id``, ``start_date``, ``end_date``,
``visit_occurrence_id``, ``sort_date``.
"""

from __future__ import annotations

import ibis
import ibis.expr.types as ir

from omopy.connector.circe._types import DomainCriteria, NumericRange

__all__ = ["build_domain_query"]


# ---------------------------------------------------------------------------
# Domain → table/column mapping
# ---------------------------------------------------------------------------

# For each domain type: (table_name, pk_col, concept_id_col,
#                        start_date_col, end_date_col, visit_col_or_None)
_DOMAIN_MAP: dict[str, dict[str, str | None]] = {
    "ConditionOccurrence": {
        "table": "condition_occurrence",
        "pk": "condition_occurrence_id",
        "concept_id": "condition_concept_id",
        "source_concept_id": "condition_source_concept_id",
        "start_date": "condition_start_date",
        "end_date": "condition_end_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "condition_type_concept_id",
        "provider_col": "provider_id",
    },
    "DrugExposure": {
        "table": "drug_exposure",
        "pk": "drug_exposure_id",
        "concept_id": "drug_concept_id",
        "source_concept_id": "drug_source_concept_id",
        "start_date": "drug_exposure_start_date",
        "end_date": "drug_exposure_end_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "drug_type_concept_id",
        "provider_col": "provider_id",
    },
    "ProcedureOccurrence": {
        "table": "procedure_occurrence",
        "pk": "procedure_occurrence_id",
        "concept_id": "procedure_concept_id",
        "source_concept_id": "procedure_source_concept_id",
        "start_date": "procedure_date",
        "end_date": "procedure_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "procedure_type_concept_id",
        "provider_col": "provider_id",
    },
    "VisitOccurrence": {
        "table": "visit_occurrence",
        "pk": "visit_occurrence_id",
        "concept_id": "visit_concept_id",
        "source_concept_id": "visit_source_concept_id",
        "start_date": "visit_start_date",
        "end_date": "visit_end_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "visit_type_concept_id",
        "provider_col": "provider_id",
    },
    "Observation": {
        "table": "observation",
        "pk": "observation_id",
        "concept_id": "observation_concept_id",
        "source_concept_id": "observation_source_concept_id",
        "start_date": "observation_date",
        "end_date": "observation_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "observation_type_concept_id",
        "provider_col": "provider_id",
    },
    "Measurement": {
        "table": "measurement",
        "pk": "measurement_id",
        "concept_id": "measurement_concept_id",
        "source_concept_id": "measurement_source_concept_id",
        "start_date": "measurement_date",
        "end_date": "measurement_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "measurement_type_concept_id",
        "provider_col": "provider_id",
    },
    "DeviceExposure": {
        "table": "device_exposure",
        "pk": "device_exposure_id",
        "concept_id": "device_concept_id",
        "source_concept_id": "device_source_concept_id",
        "start_date": "device_exposure_start_date",
        "end_date": "device_exposure_end_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "device_type_concept_id",
        "provider_col": "provider_id",
    },
    "Death": {
        "table": "death",
        "pk": "person_id",  # death doesn't have its own PK in CDM 5.4
        "concept_id": "cause_concept_id",
        "source_concept_id": "cause_source_concept_id",
        "start_date": "death_date",
        "end_date": "death_date",
        "visit_col": None,
        "type_concept_id": "death_type_concept_id",
        "provider_col": None,
    },
    "VisitDetail": {
        "table": "visit_detail",
        "pk": "visit_detail_id",
        "concept_id": "visit_detail_concept_id",
        "source_concept_id": "visit_detail_source_concept_id",
        "start_date": "visit_detail_start_date",
        "end_date": "visit_detail_end_date",
        "visit_col": "visit_occurrence_id",
        "type_concept_id": "visit_detail_type_concept_id",
        "provider_col": "provider_id",
    },
    "ObservationPeriod": {
        "table": "observation_period",
        "pk": "observation_period_id",
        "concept_id": None,  # No concept ID for observation_period
        "source_concept_id": None,
        "start_date": "observation_period_start_date",
        "end_date": "observation_period_end_date",
        "visit_col": None,
        "type_concept_id": "period_type_concept_id",
        "provider_col": None,
    },
    "ConditionEra": {
        "table": "condition_era",
        "pk": "condition_era_id",
        "concept_id": "condition_concept_id",
        "source_concept_id": None,
        "start_date": "condition_era_start_date",
        "end_date": "condition_era_end_date",
        "visit_col": None,
        "type_concept_id": None,
        "provider_col": None,
    },
    "DrugEra": {
        "table": "drug_era",
        "pk": "drug_era_id",
        "concept_id": "drug_concept_id",
        "source_concept_id": None,
        "start_date": "drug_era_start_date",
        "end_date": "drug_era_end_date",
        "visit_col": None,
        "type_concept_id": None,
        "provider_col": None,
    },
}


def build_domain_query(
    criteria: DomainCriteria,
    con: ibis.BaseBackend,
    catalog: str,
    cdm_schema: str,
    codeset_tables: dict[int, ir.Table],
    person_tbl: ir.Table | None = None,
) -> ir.Table:
    """Build an Ibis query for a single domain criteria.

    Parameters
    ----------
    criteria
        The parsed domain criteria.
    con
        Ibis backend connection.
    catalog
        Database catalog name.
    cdm_schema
        Schema containing CDM tables.
    codeset_tables
        Resolved concept set tables (from ``resolve_concept_sets``).
    person_tbl
        Person table (for age/gender filtering). If None, loaded from DB.

    Returns
    -------
    ir.Table
        Ibis expression with standardised columns:
        ``person_id``, ``event_id``, ``start_date``, ``end_date``,
        ``visit_occurrence_id``, ``sort_date``.
    """
    domain = criteria.domain_type
    if domain not in _DOMAIN_MAP:
        msg = f"Unsupported domain type: {domain}"
        raise ValueError(msg)

    dm = _DOMAIN_MAP[domain]
    table_name = dm["table"]

    # Get the domain table
    try:
        tbl = con.table(table_name, database=(catalog, cdm_schema))
    except Exception:
        msg = f"Table '{table_name}' not found in schema '{cdm_schema}'"
        raise ValueError(msg) from None

    # Column references
    pk_col = dm["pk"]
    concept_id_col = dm["concept_id"]
    start_date_col = dm["start_date"]
    end_date_col = dm["end_date"]
    visit_col = dm["visit_col"]

    # --- Apply codeset filter ---
    if criteria.codeset_id is not None and concept_id_col is not None:
        codeset = codeset_tables.get(criteria.codeset_id)
        if codeset is not None:
            # Semi-join: filter rows where concept_id is in the codeset
            tbl = tbl.filter(tbl[concept_id_col].isin(codeset.concept_id))

    # --- Handle DateAdjustment (swap start/end columns) ---
    if criteria.date_adjustment:
        adj = criteria.date_adjustment
        logical_start = end_date_col if adj.start_with == "END_DATE" else start_date_col
        logical_end = start_date_col if adj.end_with == "START_DATE" else end_date_col
    else:
        logical_start = start_date_col
        logical_end = end_date_col

    # --- Build standardised output ---
    # COALESCE end_date with start_date + 1 day (if they differ)
    if logical_start != logical_end:
        end_expr = ibis.coalesce(
            tbl[logical_end],
            tbl[logical_start] + ibis.interval(days=1),
        )
    else:
        end_expr = tbl[logical_end]

    # Visit occurrence ID (NULL if domain doesn't have visits)
    if visit_col and visit_col in tbl.columns:
        visit_expr = tbl[visit_col]
    else:
        visit_expr = ibis.literal(None).cast("int64").name("visit_occurrence_id")

    # Build the base query with standardised columns
    query = tbl.select(
        person_id=tbl.person_id,
        event_id=tbl[pk_col].cast("int64"),
        start_date=tbl[logical_start],
        end_date=end_expr,
        visit_occurrence_id=visit_expr,
        sort_date=tbl[logical_start],
    )

    # --- Apply filters ---

    # Person-level filters (age, gender) require joining to person table
    if criteria.age is not None or criteria.gender is not None:
        if person_tbl is None:
            person_tbl = con.table("person", database=(catalog, cdm_schema))

        if criteria.gender is not None:
            gender_ids = list(criteria.gender.concept_ids)
            query = query.join(
                person_tbl.select("person_id", "gender_concept_id", "year_of_birth"),
                "person_id",
            ).filter(person_tbl.gender_concept_id.isin(gender_ids))
            if criteria.age is not None:
                # Age at event start
                age_expr = (query.start_date.year() - query.year_of_birth).cast("int64")
                query = query.filter(_numeric_filter(age_expr, criteria.age))
            # Drop joined columns, keep standard set
            query = query.select(
                "person_id",
                "event_id",
                "start_date",
                "end_date",
                "visit_occurrence_id",
                "sort_date",
            )
        elif criteria.age is not None:
            query = query.join(
                person_tbl.select("person_id", "year_of_birth"),
                "person_id",
            )
            age_expr = (query.start_date.year() - query.year_of_birth).cast("int64")
            query = query.filter(_numeric_filter(age_expr, criteria.age)).select(
                "person_id",
                "event_id",
                "start_date",
                "end_date",
                "visit_occurrence_id",
                "sort_date",
            )

    # --- First occurrence only ---
    if criteria.first:
        query = (
            query.mutate(
                _ordinal=ibis.row_number().over(
                    ibis.window(
                        group_by="person_id",
                        order_by="sort_date",
                    )
                )
            )
            .filter(
                ibis._._ == 0  # row_number is 0-indexed in Ibis
            )
            .drop("_ordinal")
        )

    return query


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------


def _numeric_filter(expr: ir.Column, range_: NumericRange) -> ir.BooleanValue:
    """Apply a numeric range filter to an expression."""
    op = range_.op
    val = range_.value
    if op == "gt":
        return expr > val
    elif op == "gte":
        return expr >= val
    elif op == "lt":
        return expr < val
    elif op == "lte":
        return expr <= val
    elif op == "eq":
        return expr == val
    elif op == "neq":
        return expr != val
    elif op == "bt":
        return (expr >= val) & (expr <= range_.extent)
    elif op == "!bt":
        return (expr < val) | (expr > range_.extent)
    else:
        msg = f"Unknown numeric operator: {op}"
        raise ValueError(msg)
