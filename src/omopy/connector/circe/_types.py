"""Pydantic models representing a parsed CIRCE cohort expression.

These are pure data containers — no SQL generation logic here.
All field names are Pythonic (snake_case) regardless of the original JSON.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "CensorWindow",
    "CohortExpression",
    "CollapseSettings",
    "Concept",
    "ConceptFilter",
    "ConceptItem",
    "ConceptSet",
    "CorrelatedCriteria",
    "CriteriaGroup",
    "CriteriaLimit",
    "CustomEraStrategy",
    "DateAdjustment",
    "DateOffsetStrategy",
    "DemographicCriteria",
    "DomainCriteria",
    "EndStrategy",
    "InclusionRule",
    "NumericRange",
    "ObservationWindow",
    "Occurrence",
    "PrimaryCriteria",
    "TemporalWindow",
    "TextFilter",
    "WindowEndpoint",
]


# ---------------------------------------------------------------------------
# Concept set types
# ---------------------------------------------------------------------------


class Concept(BaseModel):
    """A single OMOP concept."""

    model_config = ConfigDict(frozen=True)

    concept_id: int
    concept_name: str = ""
    concept_code: str = ""
    domain_id: str = ""
    vocabulary_id: str = ""
    standard_concept: str = ""
    invalid_reason: str = ""
    concept_class_id: str = ""


class ConceptItem(BaseModel):
    """A concept with inclusion/exclusion flags."""

    model_config = ConfigDict(frozen=True)

    concept: Concept
    include_descendants: bool = False
    include_mapped: bool = False
    is_excluded: bool = False


class ConceptSet(BaseModel):
    """A named concept set (reusable across criteria)."""

    model_config = ConfigDict(frozen=True)

    id: int
    name: str
    items: tuple[ConceptItem, ...] = ()


# ---------------------------------------------------------------------------
# Criteria filters
# ---------------------------------------------------------------------------


class NumericRange(BaseModel):
    """Numeric comparison filter (e.g., Age, Quantity, ValueAsNumber)."""

    model_config = ConfigDict(frozen=True)

    value: float
    op: str  # "gt", "gte", "lt", "lte", "eq", "neq", "bt", "!bt"
    extent: float | None = None  # For "bt" (between) and "!bt" (not between)


class TextFilter(BaseModel):
    """Text comparison filter (e.g., StopReason)."""

    model_config = ConfigDict(frozen=True)

    text: str
    op: str  # "contains", "startsWith", "endsWith", "eq"


class ConceptFilter(BaseModel):
    """Filter by a list of concept IDs."""

    model_config = ConfigDict(frozen=True)

    concept_ids: tuple[int, ...] = ()


class DateAdjustment(BaseModel):
    """Override which DB columns map to logical start/end dates."""

    model_config = ConfigDict(frozen=True)

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


class DomainCriteria(BaseModel):
    """Criteria for a specific clinical domain.

    This represents the contents of e.g. ``{"ConditionOccurrence": {...}}``.
    """

    model_config = ConfigDict(frozen=True)

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


class WindowEndpoint(BaseModel):
    """One end of a temporal window (relative to index event)."""

    model_config = ConfigDict(frozen=True)

    days: int | None = None  # None = unbounded
    coeff: int = 1  # -1 = before index, 1 = after index


class TemporalWindow(BaseModel):
    """A temporal window defining when correlated events can occur."""

    model_config = ConfigDict(frozen=True)

    start: WindowEndpoint = Field(default_factory=WindowEndpoint)
    end: WindowEndpoint = Field(default_factory=WindowEndpoint)
    use_index_end: bool = False  # Reference index event's end_date
    use_event_end: bool = False  # Reference correlated event's end_date


class Occurrence(BaseModel):
    """Occurrence count requirement for correlated criteria."""

    model_config = ConfigDict(frozen=True)

    type: int  # 0=Exactly, 1=AtMost, 2=AtLeast
    count: int = 1
    is_distinct: bool = False


# ---------------------------------------------------------------------------
# Correlated criteria
# ---------------------------------------------------------------------------


class CorrelatedCriteria(BaseModel):
    """A single correlated criterion within an inclusion rule."""

    model_config = ConfigDict(frozen=True)

    criteria: DomainCriteria
    start_window: TemporalWindow | None = None
    end_window: TemporalWindow | None = None
    occurrence: Occurrence | None = None
    restrict_visit: bool = False
    ignore_observation_period: bool = False


# ---------------------------------------------------------------------------
# Demographic criteria
# ---------------------------------------------------------------------------


class DemographicCriteria(BaseModel):
    """Demographic filter (age, gender, race, ethnicity)."""

    model_config = ConfigDict(frozen=True)

    age: NumericRange | None = None
    gender: ConceptFilter | None = None
    race: ConceptFilter | None = None
    ethnicity: ConceptFilter | None = None
    occurrence_start_date: NumericRange | None = None
    occurrence_end_date: NumericRange | None = None


# ---------------------------------------------------------------------------
# Criteria group (recursive)
# ---------------------------------------------------------------------------


class CriteriaGroup(BaseModel):
    """A group of criteria with a combining type (ALL, ANY, AT_LEAST, AT_MOST)."""

    model_config = ConfigDict(frozen=True)

    type: str = "ALL"  # "ALL", "ANY", "AT_LEAST", "AT_MOST"
    count: int = 0  # Threshold for AT_LEAST/AT_MOST
    criteria_list: tuple[CorrelatedCriteria, ...] = ()
    demographic_criteria_list: tuple[DemographicCriteria, ...] = ()
    groups: tuple[CriteriaGroup, ...] = ()


# ---------------------------------------------------------------------------
# Inclusion rule
# ---------------------------------------------------------------------------


class InclusionRule(BaseModel):
    """A named inclusion rule with a criteria group expression."""

    model_config = ConfigDict(frozen=True)

    name: str
    expression: CriteriaGroup


# ---------------------------------------------------------------------------
# Criteria limit
# ---------------------------------------------------------------------------


class CriteriaLimit(BaseModel):
    """Limit on qualifying events: First, Last, or All."""

    model_config = ConfigDict(frozen=True)

    type: str = "All"  # "All", "First", "Last"


# ---------------------------------------------------------------------------
# Observation window
# ---------------------------------------------------------------------------


class ObservationWindow(BaseModel):
    """Required observation before/after index event."""

    model_config = ConfigDict(frozen=True)

    prior_days: int = 0
    post_days: int = 0


# ---------------------------------------------------------------------------
# Primary criteria
# ---------------------------------------------------------------------------


class PrimaryCriteria(BaseModel):
    """Primary criteria defining initial qualifying events."""

    model_config = ConfigDict(frozen=True)

    criteria_list: tuple[DomainCriteria, ...] = ()
    observation_window: ObservationWindow = Field(default_factory=ObservationWindow)
    primary_limit: CriteriaLimit = Field(default_factory=CriteriaLimit)


# ---------------------------------------------------------------------------
# End strategy
# ---------------------------------------------------------------------------


class DateOffsetStrategy(BaseModel):
    """End date = event date field + offset days."""

    model_config = ConfigDict(frozen=True)

    date_field: str = "StartDate"  # "StartDate" or "EndDate"
    offset: int = 0


class CustomEraStrategy(BaseModel):
    """End date based on drug exposure eras."""

    model_config = ConfigDict(frozen=True)

    drug_codeset_id: int = 0
    gap_days: int = 0
    offset: int = 0
    days_supply_override: int | None = None


class EndStrategy(BaseModel):
    """Cohort exit strategy. Only one of date_offset or custom_era is set."""

    model_config = ConfigDict(frozen=True)

    date_offset: DateOffsetStrategy | None = None
    custom_era: CustomEraStrategy | None = None


# ---------------------------------------------------------------------------
# Collapse settings + censor window
# ---------------------------------------------------------------------------


class CollapseSettings(BaseModel):
    """Settings for collapsing overlapping cohort periods."""

    model_config = ConfigDict(frozen=True)

    collapse_type: str = "ERA"
    era_pad: int = 0


class CensorWindow(BaseModel):
    """Hard date boundaries for the cohort."""

    model_config = ConfigDict(frozen=True)

    start_date: str | None = None  # ISO date string "YYYY-MM-DD"
    end_date: str | None = None  # ISO date string "YYYY-MM-DD"


# ---------------------------------------------------------------------------
# Top-level cohort expression
# ---------------------------------------------------------------------------


class CohortExpression(BaseModel):
    """A complete CIRCE cohort definition."""

    model_config = ConfigDict(frozen=True)

    concept_sets: tuple[ConceptSet, ...] = ()
    primary_criteria: PrimaryCriteria = Field(default_factory=PrimaryCriteria)
    additional_criteria: CriteriaGroup | None = None
    qualified_limit: CriteriaLimit = Field(default_factory=CriteriaLimit)
    inclusion_rules: tuple[InclusionRule, ...] = ()
    expression_limit: CriteriaLimit = Field(default_factory=CriteriaLimit)
    end_strategy: EndStrategy | None = None
    censoring_criteria: tuple[DomainCriteria, ...] = ()
    collapse_settings: CollapseSettings = Field(default_factory=CollapseSettings)
    censor_window: CensorWindow = Field(default_factory=CensorWindow)


# Rebuild forward references for recursive models
DomainCriteria.model_rebuild()
CriteriaGroup.model_rebuild()
CohortExpression.model_rebuild()
