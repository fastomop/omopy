"""``omopy.connector.circe`` — CIRCE cohort definition engine.

Clean-room Python implementation of the CIRCE cohort generation algorithm.
Generates cohorts from ATLAS/CIRCE JSON definitions against an OMOP CDM
database using Ibis for lazy query construction.

Primary entry point::

    from omopy.connector.circe import generate_cohort_set

    cdm = generate_cohort_set(cdm, "/path/to/cohort_jsons/", name="my_cohort")
"""

from omopy.connector.circe._engine import generate_cohort_set
from omopy.connector.circe._parser import (
    parse_cohort_expression,
    parse_cohort_json,
    read_cohort_set,
)
from omopy.connector.circe._types import (
    CensorWindow,
    CohortExpression,
    CollapseSettings,
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
    ObservationWindow,
    Occurrence,
    PrimaryCriteria,
    TemporalWindow,
    TextFilter,
    WindowEndpoint,
)

__all__ = [
    # Engine
    "generate_cohort_set",
    # Parser
    "parse_cohort_expression",
    "parse_cohort_json",
    "read_cohort_set",
    # Types
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
