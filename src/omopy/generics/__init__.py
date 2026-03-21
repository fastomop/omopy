"""``omopy.generics`` — Core type system for OMOPy.

This subpackage provides the foundational types that all other OMOPy modules
depend on, mirroring R's ``omopgenerics`` package.

Core classes:
    - :class:`CdmReference` — Top-level CDM container
    - :class:`CdmSource` — Protocol for CDM backends
    - :class:`CdmTable` — Metadata-preserving table wrapper
    - :class:`CohortTable` — Cohort table with settings/attrition/codelist
    - :class:`Codelist` — Named mapping of concept ID lists
    - :class:`ConceptSetExpression` — Concept sets with include/exclude flags
    - :class:`SummarisedResult` — Standard OHDSI result format
    - :class:`CdmSchema` — CDM version schema registry

Enums:
    - :class:`CdmVersion` — CDM version (5.3 / 5.4)
    - :class:`TableType` — Table type classification
    - :class:`CdmDataType` — CDM data types
    - :class:`TableGroup` — Logical table groups
    - :class:`TableSchema` — Database schema types
"""

from omopy.generics._io import (
    export_codelist,
    export_concept_set_expression,
    export_summarised_result,
    import_codelist,
    import_concept_set_expression,
    import_summarised_result,
)
from omopy.generics._schema import (
    CdmSchema,
    FieldSpec,
    ResultFieldSpec,
    TableSpec,
)
from omopy.generics._types import (
    GROUP_COUNT_VARIABLES,
    NAME_LEVEL_SEP,
    OVERALL,
    SUPPORTED_CDM_VERSIONS,
    CdmDataType,
    CdmVersion,
    CdmVersionLiteral,
    TableGroup,
    TableSchema,
    TableType,
)
from omopy.generics._validation import (
    assert_character,
    assert_choice,
    assert_class,
    assert_date,
    assert_list,
    assert_logical,
    assert_numeric,
    assert_table_columns,
    assert_true,
)
from omopy.generics.cdm_reference import CdmReference, CdmSource
from omopy.generics.cdm_table import CdmTable
from omopy.generics.codelist import Codelist, ConceptEntry, ConceptSetExpression
from omopy.generics.cohort_table import COHORT_REQUIRED_COLUMNS, CohortTable
from omopy.generics.summarised_result import (
    SETTINGS_REQUIRED_COLUMNS,
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)

__all__ = [
    # Core classes
    "CdmReference",
    "CdmSource",
    "CdmTable",
    "CohortTable",
    "Codelist",
    "ConceptEntry",
    "ConceptSetExpression",
    "SummarisedResult",
    # Schema
    "CdmSchema",
    "FieldSpec",
    "TableSpec",
    "ResultFieldSpec",
    # Enums
    "CdmVersion",
    "CdmDataType",
    "TableType",
    "TableGroup",
    "TableSchema",
    # Constants
    "SUPPORTED_CDM_VERSIONS",
    "NAME_LEVEL_SEP",
    "OVERALL",
    "COHORT_REQUIRED_COLUMNS",
    "SUMMARISED_RESULT_COLUMNS",
    "SETTINGS_REQUIRED_COLUMNS",
    # Validation
    "assert_character",
    "assert_choice",
    "assert_class",
    "assert_date",
    "assert_list",
    "assert_logical",
    "assert_numeric",
    "assert_true",
    "assert_table_columns",
    # I/O
    "export_codelist",
    "import_codelist",
    "export_concept_set_expression",
    "import_concept_set_expression",
    "export_summarised_result",
    "import_summarised_result",
]
