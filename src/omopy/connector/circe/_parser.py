"""JSON parser for CIRCE cohort definitions.

Handles both PascalCase (Atlas-generated) and camelCase field names.
Converts raw JSON dicts into the typed dataclass hierarchy from ``_types``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from omopy.connector.circe._types import (
    CohortExpression,
    CollapseSettings,
    CensorWindow,
    Concept,
    ConceptFilter,
    ConceptItem,
    ConceptSet,
    CorrelatedCriteria,
    CriteriaGroup,
    CriteriaLimit,
    CustomEraStrategy,
    DateAdjustment,
    DateOffsetStrategy,
    DemographicCriteria,
    DomainCriteria,
    EndStrategy,
    InclusionRule,
    NumericRange,
    Occurrence,
    ObservationWindow,
    PrimaryCriteria,
    TemporalWindow,
    TextFilter,
    WindowEndpoint,
)

__all__ = ["parse_cohort_expression", "parse_cohort_json"]


# ---------------------------------------------------------------------------
# Field name normalisation helpers
# ---------------------------------------------------------------------------

# All supported domain type keys (as they appear in JSON criteria objects)
_DOMAIN_KEYS: set[str] = {
    "ConditionOccurrence",
    "DrugExposure",
    "ProcedureOccurrence",
    "VisitOccurrence",
    "Observation",
    "Measurement",
    "DeviceExposure",
    "Specimen",
    "Death",
    "VisitDetail",
    "ObservationPeriod",
    "PayerPlanPeriod",
    "LocationRegion",
    "ConditionEra",
    "DrugEra",
    "DoseEra",
}


# camelCase → PascalCase normalisation map for all known keys.
# We only normalise keys we actually look up; domain type keys (ConditionOccurrence etc.)
# are already PascalCase in the JSON spec and are never camelCase.
_CAMEL_TO_PASCAL: dict[str, str] = {
    # Top-level expression keys
    "conceptSets": "ConceptSets",
    "primaryCriteria": "PrimaryCriteria",
    "additionalCriteria": "AdditionalCriteria",
    "qualifiedLimit": "QualifiedLimit",
    "inclusionRules": "InclusionRules",
    "expressionLimit": "ExpressionLimit",
    "endStrategy": "EndStrategy",
    "censoringCriteria": "CensoringCriteria",
    "collapseSettings": "CollapseSettings",
    "censorWindow": "CensorWindow",
    # PrimaryCriteria inner keys
    "criteriaList": "CriteriaList",
    "observationWindow": "ObservationWindow",
    "primaryCriteriaLimit": "PrimaryCriteriaLimit",
    "priorDays": "PriorDays",
    "postDays": "PostDays",
    # Limit/strategy keys
    "type": "Type",
    "collapseType": "CollapseType",
    "eraPad": "EraPad",
    # EndStrategy
    "dateOffset": "DateOffset",
    "dateField": "DateField",
    "offset": "Offset",
    "customEra": "CustomEra",
    "drugCodesetId": "DrugCodesetId",
    "gapDays": "GapDays",
    "daysSupplyOverride": "DaysSupplyOverride",
    # CensorWindow
    "startDate": "StartDate",
    "endDate": "EndDate",
    # CriteriaGroup
    "demographicCriteriaList": "DemographicCriteriaList",
    "groups": "Groups",
    # ConceptSet
    "codesetId": "CodesetId",
}


def _normalise_keys(obj: Any, depth: int = 0) -> Any:
    """Recursively normalise camelCase JSON keys to PascalCase.

    Leaves values (strings, numbers, booleans) untouched. Only dict keys
    that appear in ``_CAMEL_TO_PASCAL`` are renamed; unknown keys pass through.
    """
    if depth > 50:
        return obj
    if isinstance(obj, dict):
        return {_CAMEL_TO_PASCAL.get(k, k): _normalise_keys(v, depth + 1) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalise_keys(item, depth + 1) for item in obj]
    return obj


def _get(d: dict, *keys: str, default: Any = None) -> Any:
    """Get value from dict trying multiple key variants."""
    for k in keys:
        if k in d:
            return d[k]
    return default


# ---------------------------------------------------------------------------
# Concept parsing
# ---------------------------------------------------------------------------


def _parse_concept(d: dict) -> Concept:
    return Concept(
        concept_id=d.get("CONCEPT_ID", 0),
        concept_name=d.get("CONCEPT_NAME", ""),
        concept_code=d.get("CONCEPT_CODE", ""),
        domain_id=d.get("DOMAIN_ID", ""),
        vocabulary_id=d.get("VOCABULARY_ID", ""),
        standard_concept=d.get("STANDARD_CONCEPT", ""),
        invalid_reason=d.get("INVALID_REASON", ""),
        concept_class_id=d.get("CONCEPT_CLASS_ID", ""),
    )


def _parse_concept_item(d: dict) -> ConceptItem:
    concept = _parse_concept(d.get("concept", {}))
    return ConceptItem(
        concept=concept,
        include_descendants=d.get("includeDescendants", False),
        include_mapped=d.get("includeMapped", False),
        is_excluded=d.get("isExcluded", False),
    )


def _parse_concept_set(d: dict) -> ConceptSet:
    items_raw = d.get("expression", {}).get("items", [])
    items = tuple(_parse_concept_item(i) for i in items_raw)
    return ConceptSet(
        id=d.get("id", 0),
        name=d.get("name", ""),
        items=items,
    )


# ---------------------------------------------------------------------------
# Filter parsing
# ---------------------------------------------------------------------------


def _parse_numeric_range(d: dict | None) -> NumericRange | None:
    if d is None:
        return None
    return NumericRange(
        value=d.get("Value", 0),
        op=d.get("Op", "gte"),
        extent=d.get("Extent"),
    )


def _parse_text_filter(d: dict | None) -> TextFilter | None:
    if d is None:
        return None
    return TextFilter(
        text=d.get("Text", ""),
        op=d.get("Op", "contains"),
    )


def _parse_concept_filter(lst: list | None) -> ConceptFilter | None:
    if not lst:
        return None
    ids = tuple(c.get("CONCEPT_ID", 0) if isinstance(c, dict) else int(c) for c in lst)
    return ConceptFilter(concept_ids=ids)


# ---------------------------------------------------------------------------
# Date adjustment
# ---------------------------------------------------------------------------


def _parse_date_adjustment(d: dict | None) -> DateAdjustment | None:
    if d is None:
        return None
    return DateAdjustment(
        start_with=d.get("StartWith", "START_DATE"),
        end_with=d.get("EndWith", "END_DATE"),
    )


# ---------------------------------------------------------------------------
# Domain criteria parsing
# ---------------------------------------------------------------------------

# Mapping from domain type key to its type-concept filter JSON key
_TYPE_FILTER_KEYS: dict[str, str] = {
    "ConditionOccurrence": "ConditionType",
    "DrugExposure": "DrugType",
    "ProcedureOccurrence": "ProcedureType",
    "VisitOccurrence": "VisitType",
    "Observation": "ObservationType",
    "Measurement": "MeasurementType",
    "DeviceExposure": "DeviceType",
    "Specimen": "SpecimenType",
    "Death": "DeathType",
    "VisitDetail": "VisitDetailType",
    "ConditionEra": "ConditionType",
    "DrugEra": "DrugType",
}


def _parse_domain_criteria(domain_type: str, d: dict) -> DomainCriteria:
    """Parse the inner dict of a domain criteria object."""
    type_key = _TYPE_FILTER_KEYS.get(domain_type, "")
    return DomainCriteria(
        domain_type=domain_type,  # type: ignore[arg-type]
        codeset_id=d.get("CodesetId"),
        first=d.get("First", False),
        date_adjustment=_parse_date_adjustment(d.get("DateAdjustment")),
        occurrence_start_date=_parse_numeric_range(d.get("OccurrenceStartDate")),
        occurrence_end_date=_parse_numeric_range(d.get("OccurrenceEndDate")),
        age=_parse_numeric_range(d.get("Age")),
        gender=_parse_concept_filter(d.get("Gender")),
        type_filter=_parse_concept_filter(d.get(type_key)) if type_key else None,
        visit_type=_parse_concept_filter(d.get("VisitType")),
        provider_specialty=_parse_concept_filter(d.get("ProviderSpecialty")),
        value_as_number=_parse_numeric_range(d.get("ValueAsNumber")),
        value_as_concept=_parse_concept_filter(d.get("ValueAsConcept")),
        unit=_parse_concept_filter(d.get("Unit")),
        range_low=_parse_numeric_range(d.get("RangeLow")),
        range_high=_parse_numeric_range(d.get("RangeHigh")),
        quantity=_parse_numeric_range(d.get("Quantity")),
        stop_reason=_parse_text_filter(d.get("StopReason")),
        days_supply=_parse_numeric_range(d.get("DaysSupply")),
        route_concept=_parse_concept_filter(d.get("RouteConcept")),
        effective_drug_dose=_parse_numeric_range(d.get("EffectiveDrugDose")),
        lot_number=_parse_text_filter(d.get("LotNumber")),
        refills=_parse_numeric_range(d.get("Refills")),
        correlated_criteria=_parse_criteria_group(d.get("CorrelatedCriteria")),
    )


def _extract_domain_criteria(d: dict) -> DomainCriteria:
    """Extract domain criteria from a criteria list item (which has one domain key)."""
    for key in _DOMAIN_KEYS:
        if key in d:
            return _parse_domain_criteria(key, d[key])
    msg = f"No recognised domain key in criteria: {list(d.keys())}"
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Temporal window and occurrence
# ---------------------------------------------------------------------------


def _parse_window_endpoint(d: dict | None) -> WindowEndpoint:
    if d is None:
        return WindowEndpoint()
    return WindowEndpoint(
        days=d.get("Days"),
        coeff=d.get("Coeff", 1),
    )


def _parse_temporal_window(d: dict | None) -> TemporalWindow | None:
    if d is None:
        return None
    return TemporalWindow(
        start=_parse_window_endpoint(d.get("Start")),
        end=_parse_window_endpoint(d.get("End")),
        use_index_end=d.get("UseIndexEnd", False),
        use_event_end=d.get("UseEventEnd", False),
    )


def _parse_occurrence(d: dict | None) -> Occurrence | None:
    if d is None:
        return None
    return Occurrence(
        type=d.get("Type", 2),
        count=d.get("Count", 1),
        is_distinct=d.get("IsDistinct", False),
    )


# ---------------------------------------------------------------------------
# Correlated criteria
# ---------------------------------------------------------------------------


def _parse_correlated_criteria(d: dict) -> CorrelatedCriteria:
    criteria_dict = d.get("Criteria", {})
    return CorrelatedCriteria(
        criteria=_extract_domain_criteria(criteria_dict),
        start_window=_parse_temporal_window(d.get("StartWindow")),
        end_window=_parse_temporal_window(d.get("EndWindow")),
        occurrence=_parse_occurrence(d.get("Occurrence")),
        restrict_visit=d.get("RestrictVisit", False),
        ignore_observation_period=d.get("IgnoreObservationPeriod", False),
    )


# ---------------------------------------------------------------------------
# Demographic criteria
# ---------------------------------------------------------------------------


def _parse_demographic_criteria(d: dict) -> DemographicCriteria:
    return DemographicCriteria(
        age=_parse_numeric_range(d.get("Age")),
        gender=_parse_concept_filter(d.get("Gender")),
        race=_parse_concept_filter(d.get("Race")),
        ethnicity=_parse_concept_filter(d.get("Ethnicity")),
        occurrence_start_date=_parse_numeric_range(d.get("OccurrenceStartDate")),
        occurrence_end_date=_parse_numeric_range(d.get("OccurrenceEndDate")),
    )


# ---------------------------------------------------------------------------
# Criteria group (recursive)
# ---------------------------------------------------------------------------


def _parse_criteria_group(d: dict | None) -> CriteriaGroup | None:
    if d is None:
        return None
    return CriteriaGroup(
        type=d.get("Type", "ALL"),
        count=d.get("Count", 0),
        criteria_list=tuple(_parse_correlated_criteria(c) for c in d.get("CriteriaList", [])),
        demographic_criteria_list=tuple(
            _parse_demographic_criteria(c) for c in d.get("DemographicCriteriaList", [])
        ),
        groups=tuple(
            _parse_criteria_group(g)  # type: ignore[misc]
            for g in d.get("Groups", [])
            if g is not None
        ),
    )


# ---------------------------------------------------------------------------
# Primary criteria
# ---------------------------------------------------------------------------


def _parse_primary_criteria(d: dict) -> PrimaryCriteria:
    criteria_list = tuple(_extract_domain_criteria(c) for c in d.get("CriteriaList", []))
    obs_window = d.get("ObservationWindow", {})
    # Handle both PrimaryCriteriaLimit and PrimaryLimit
    limit_dict = _get(d, "PrimaryCriteriaLimit", "PrimaryLimit", default={})
    return PrimaryCriteria(
        criteria_list=criteria_list,
        observation_window=ObservationWindow(
            prior_days=obs_window.get("PriorDays", 0),
            post_days=obs_window.get("PostDays", 0),
        ),
        primary_limit=CriteriaLimit(type=limit_dict.get("Type", "All")),
    )


# ---------------------------------------------------------------------------
# End strategy
# ---------------------------------------------------------------------------


def _parse_end_strategy(d: dict | None) -> EndStrategy | None:
    if d is None or not d:
        return None

    date_offset_dict = d.get("DateOffset")
    custom_era_dict = _get(d, "CustomEra", "DrugEra")

    if date_offset_dict:
        return EndStrategy(
            date_offset=DateOffsetStrategy(
                date_field=date_offset_dict.get("DateField", "StartDate"),
                offset=date_offset_dict.get("Offset", 0),
            )
        )
    elif custom_era_dict:
        return EndStrategy(
            custom_era=CustomEraStrategy(
                drug_codeset_id=custom_era_dict.get("DrugCodesetId", 0),
                gap_days=custom_era_dict.get("GapDays", 0),
                offset=custom_era_dict.get("Offset", 0),
                days_supply_override=custom_era_dict.get("DaysSupplyOverride"),
            )
        )
    return None


# ---------------------------------------------------------------------------
# Collapse settings + censor window
# ---------------------------------------------------------------------------


def _parse_collapse_settings(d: dict | None) -> CollapseSettings:
    if d is None:
        return CollapseSettings()
    return CollapseSettings(
        collapse_type=d.get("CollapseType", "ERA"),
        era_pad=d.get("EraPad", 0),
    )


def _parse_censor_window(d: dict | None) -> CensorWindow:
    if d is None or not d:
        return CensorWindow()
    return CensorWindow(
        start_date=d.get("StartDate"),
        end_date=d.get("EndDate"),
    )


# ---------------------------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------------------------


def parse_cohort_expression(d: dict) -> CohortExpression:
    """Parse a raw JSON dict into a CohortExpression.

    Handles both PascalCase and camelCase field names.

    Parameters
    ----------
    d
        A dict from ``json.loads()`` of a CIRCE cohort definition.

    Returns
    -------
    CohortExpression
        The fully parsed, typed cohort expression.
    """
    # Normalise camelCase keys to PascalCase throughout
    d = _normalise_keys(d)

    concept_sets = tuple(_parse_concept_set(cs) for cs in d.get("ConceptSets", []))

    primary = _parse_primary_criteria(d.get("PrimaryCriteria", {}))

    additional = _parse_criteria_group(d.get("AdditionalCriteria"))

    qualified_limit = CriteriaLimit(type=d.get("QualifiedLimit", {}).get("Type", "All"))

    inclusion_rules = tuple(
        InclusionRule(
            name=r.get("name", ""),
            expression=_parse_criteria_group(r.get("expression", {})) or CriteriaGroup(),
        )
        for r in d.get("InclusionRules", [])
    )

    expression_limit = CriteriaLimit(type=d.get("ExpressionLimit", {}).get("Type", "All"))

    end_strategy = _parse_end_strategy(d.get("EndStrategy"))

    censoring = tuple(_extract_domain_criteria(c) for c in d.get("CensoringCriteria", []))

    collapse = _parse_collapse_settings(d.get("CollapseSettings"))
    censor_window = _parse_censor_window(d.get("CensorWindow"))

    return CohortExpression(
        concept_sets=concept_sets,
        primary_criteria=primary,
        additional_criteria=additional,
        qualified_limit=qualified_limit,
        inclusion_rules=inclusion_rules,
        expression_limit=expression_limit,
        end_strategy=end_strategy,
        censoring_criteria=censoring,
        collapse_settings=collapse,
        censor_window=censor_window,
    )


def parse_cohort_json(json_str: str) -> CohortExpression:
    """Parse a CIRCE cohort JSON string into a CohortExpression.

    Parameters
    ----------
    json_str
        A JSON string containing a CIRCE cohort definition.

    Returns
    -------
    CohortExpression
        The fully parsed cohort expression.
    """
    return parse_cohort_expression(json.loads(json_str))


def read_cohort_set(
    directory: str | Path,
) -> list[dict[str, Any]]:
    """Read a directory of cohort JSON files.

    Returns a list of dicts with keys:
    - ``cohort_definition_id``: Sequential integer starting at 1
    - ``cohort_name``: Filename stem
    - ``expression``: Parsed CohortExpression
    - ``json_path``: Path to the JSON file

    Parameters
    ----------
    directory
        Path to directory containing ``*.json`` cohort files.

    Returns
    -------
    list[dict]
        One entry per JSON file found.
    """
    path = Path(directory)
    if not path.is_dir():
        msg = f"Not a directory: {path}"
        raise FileNotFoundError(msg)

    results = []
    json_files = sorted(path.glob("*.json"))

    for idx, json_file in enumerate(json_files, start=1):
        raw = json.loads(json_file.read_text(encoding="utf-8"))
        expression = parse_cohort_expression(raw)
        results.append(
            {
                "cohort_definition_id": idx,
                "cohort_name": json_file.stem,
                "expression": expression,
                "json_path": json_file,
            }
        )

    return results
