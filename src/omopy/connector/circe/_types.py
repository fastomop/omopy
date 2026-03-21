"""Dataclasses representing a parsed CIRCE cohort expression.

These are pure data containers — no SQL generation logic here.
All field names are Pythonic (snake_case) regardless of the original JSON.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = [
    "CohortExpression",
    "ConceptSet",
    "ConceptItem",
    "Concept",
    "PrimaryCriteria",
    "CriteriaLimit",
    "ObservationWindow",
    "DomainCriteria",
    "DateAdjustment",
    "NumericRange",
    "TextFilter",
    "ConceptFilter",
    "TemporalWindow",
    "WindowEndpoint",
    "Occurrence",
    "CorrelatedCriteria",
    "CriteriaGroup",
    "DemographicCriteria",
    "InclusionRule",
    "EndStrategy",
    "DateOffsetStrategy",
    "CustomEraStrategy",
    "CollapseSettings",
    "CensorWindow",
]


# ---------------------------------------------------------------------------
# Concept set types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Concept:
    """A single OMOP concept."""

    concept_id: int
    concept_name: str = ""
    concept_code: str = ""
    domain_id: str = ""
    vocabulary_id: str = ""
    standard_concept: str = ""
    invalid_reason: str = ""
    concept_class_id: str = ""


@dataclass(frozen=True, slots=True)
class ConceptItem:
    """A concept with inclusion/exclusion flags."""

    concept: Concept
    include_descendants: bool = False
    include_mapped: bool = False
    is_excluded: bool = False


@dataclass(frozen=True, slots=True)
class ConceptSet:
    """A named concept set (reusable across criteria)."""

    id: int
    name: str
    items: tuple[ConceptItem, ...] = ()


# ---------------------------------------------------------------------------
# Criteria filters
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NumericRange:
    """Numeric comparison filter (e.g., Age, Quantity, ValueAsNumber)."""

    value: float
    op: str  # "gt", "gte", "lt", "lte", "eq", "neq", "bt", "!bt"
    extent: float | None = None  # For "bt" (between) and "!bt" (not between)


@dataclass(frozen=True, slots=True)
class TextFilter:
    """Text comparison filter (e.g., StopReason)."""

    text: str
    op: str  # "contains", "startsWith", "endsWith", "eq"


@dataclass(frozen=True, slots=True)
class ConceptFilter:
    """Filter by a list of concept IDs."""

    concept_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class DateAdjustment:
    """Override which DB columns map to logical start/end dates."""

    start_with: str = "START_DATE"  # "START_DATE" or "END_DATE"
    end_with: str = "END_DATE"  # "START_DATE" or "END_DATE"


# ---------------------------------------------------------------------------
# Domain criteria (event-level filters)
# ---------------------------------------------------------------------------

# All supported domain types
DomainType = Literal[
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
]


@dataclass(frozen=True, slots=True)
class DomainCriteria:
    """Criteria for a specific clinical domain.

    This represents the contents of e.g. ``{"ConditionOccurrence": {...}}``.
    """

    domain_type: DomainType
    codeset_id: int | None = None
    first: bool = False
    date_adjustment: DateAdjustment | None = None

    # Date filters
    occurrence_start_date: NumericRange | None = None
    occurrence_end_date: NumericRange | None = None

    # Person filters
    age: NumericRange | None = None
    gender: ConceptFilter | None = None

    # Type filters (domain-specific: condition_type, drug_type, etc.)
    type_filter: ConceptFilter | None = None

    # Visit / provider filters
    visit_type: ConceptFilter | None = None
    provider_specialty: ConceptFilter | None = None

    # Value filters (Measurement, Observation)
    value_as_number: NumericRange | None = None
    value_as_concept: ConceptFilter | None = None
    unit: ConceptFilter | None = None
    range_low: NumericRange | None = None
    range_high: NumericRange | None = None

    # Other filters
    quantity: NumericRange | None = None
    stop_reason: TextFilter | None = None

    # Days supply (DrugExposure)
    days_supply: NumericRange | None = None

    # Route (DrugExposure)
    route_concept: ConceptFilter | None = None

    # Effective dose (DrugExposure)
    effective_drug_dose: NumericRange | None = None

    # Lot number (DrugExposure)
    lot_number: TextFilter | None = None

    # Refills (DrugExposure)
    refills: NumericRange | None = None

    # Correlated criteria (nested, applied within the domain query)
    correlated_criteria: CriteriaGroup | None = None


# ---------------------------------------------------------------------------
# Temporal windows and occurrence
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WindowEndpoint:
    """One end of a temporal window (relative to index event)."""

    days: int | None = None  # None = unbounded
    coeff: int = 1  # -1 = before index, 1 = after index


@dataclass(frozen=True, slots=True)
class TemporalWindow:
    """A temporal window defining when correlated events can occur."""

    start: WindowEndpoint = dataclasses.field(default_factory=WindowEndpoint)
    end: WindowEndpoint = dataclasses.field(default_factory=WindowEndpoint)
    use_index_end: bool = False  # Reference index event's end_date
    use_event_end: bool = False  # Reference correlated event's end_date


@dataclass(frozen=True, slots=True)
class Occurrence:
    """Occurrence count requirement for correlated criteria."""

    type: int  # 0=Exactly, 1=AtMost, 2=AtLeast
    count: int = 1
    is_distinct: bool = False


# ---------------------------------------------------------------------------
# Correlated criteria
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CorrelatedCriteria:
    """A single correlated criterion within an inclusion rule."""

    criteria: DomainCriteria
    start_window: TemporalWindow | None = None
    end_window: TemporalWindow | None = None
    occurrence: Occurrence | None = None
    restrict_visit: bool = False
    ignore_observation_period: bool = False


# ---------------------------------------------------------------------------
# Demographic criteria
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DemographicCriteria:
    """Demographic filter (age, gender, race, ethnicity)."""

    age: NumericRange | None = None
    gender: ConceptFilter | None = None
    race: ConceptFilter | None = None
    ethnicity: ConceptFilter | None = None
    occurrence_start_date: NumericRange | None = None
    occurrence_end_date: NumericRange | None = None


# ---------------------------------------------------------------------------
# Criteria group (recursive)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CriteriaGroup:
    """A group of criteria with a combining type (ALL, ANY, AT_LEAST, AT_MOST)."""

    type: str = "ALL"  # "ALL", "ANY", "AT_LEAST", "AT_MOST"
    count: int = 0  # Threshold for AT_LEAST/AT_MOST
    criteria_list: tuple[CorrelatedCriteria, ...] = ()
    demographic_criteria_list: tuple[DemographicCriteria, ...] = ()
    groups: tuple[CriteriaGroup, ...] = ()


# ---------------------------------------------------------------------------
# Inclusion rule
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InclusionRule:
    """A named inclusion rule with a criteria group expression."""

    name: str
    expression: CriteriaGroup


# ---------------------------------------------------------------------------
# Criteria limit
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CriteriaLimit:
    """Limit on qualifying events: First, Last, or All."""

    type: str = "All"  # "All", "First", "Last"


# ---------------------------------------------------------------------------
# Observation window
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ObservationWindow:
    """Required observation before/after index event."""

    prior_days: int = 0
    post_days: int = 0


# ---------------------------------------------------------------------------
# Primary criteria
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PrimaryCriteria:
    """Primary criteria defining initial qualifying events."""

    criteria_list: tuple[DomainCriteria, ...] = ()
    observation_window: ObservationWindow = dataclasses.field(
        default_factory=ObservationWindow
    )
    primary_limit: CriteriaLimit = dataclasses.field(
        default_factory=CriteriaLimit
    )


# ---------------------------------------------------------------------------
# End strategy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DateOffsetStrategy:
    """End date = event date field + offset days."""

    date_field: str = "StartDate"  # "StartDate" or "EndDate"
    offset: int = 0


@dataclass(frozen=True, slots=True)
class CustomEraStrategy:
    """End date based on drug exposure eras."""

    drug_codeset_id: int = 0
    gap_days: int = 0
    offset: int = 0
    days_supply_override: int | None = None


@dataclass(frozen=True, slots=True)
class EndStrategy:
    """Cohort exit strategy. Only one of date_offset or custom_era is set."""

    date_offset: DateOffsetStrategy | None = None
    custom_era: CustomEraStrategy | None = None


# ---------------------------------------------------------------------------
# Collapse settings + censor window
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CollapseSettings:
    """Settings for collapsing overlapping cohort periods."""

    collapse_type: str = "ERA"
    era_pad: int = 0


@dataclass(frozen=True, slots=True)
class CensorWindow:
    """Hard date boundaries for the cohort."""

    start_date: str | None = None  # ISO date string "YYYY-MM-DD"
    end_date: str | None = None  # ISO date string "YYYY-MM-DD"


# ---------------------------------------------------------------------------
# Top-level cohort expression
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CohortExpression:
    """A complete CIRCE cohort definition."""

    concept_sets: tuple[ConceptSet, ...] = ()
    primary_criteria: PrimaryCriteria = dataclasses.field(
        default_factory=PrimaryCriteria
    )
    additional_criteria: CriteriaGroup | None = None
    qualified_limit: CriteriaLimit = dataclasses.field(
        default_factory=CriteriaLimit
    )
    inclusion_rules: tuple[InclusionRule, ...] = ()
    expression_limit: CriteriaLimit = dataclasses.field(
        default_factory=CriteriaLimit
    )
    end_strategy: EndStrategy | None = None
    censoring_criteria: tuple[DomainCriteria, ...] = ()
    collapse_settings: CollapseSettings = dataclasses.field(
        default_factory=CollapseSettings
    )
    censor_window: CensorWindow = dataclasses.field(
        default_factory=CensorWindow
    )
