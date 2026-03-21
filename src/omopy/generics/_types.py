"""Core enums, type aliases, and constants for OMOPy generics."""

from __future__ import annotations

import enum
from typing import Literal

__all__ = [
    "CdmVersion",
    "TableType",
    "CdmDataType",
    "TableGroup",
    "SUPPORTED_CDM_VERSIONS",
    "NAME_LEVEL_SEP",
    "OVERALL",
    "GROUP_COUNT_VARIABLES",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_CDM_VERSIONS: tuple[str, ...] = ("5.3", "5.4")

#: Separator used to combine multiple name-level pairs (e.g. in strata/group
#: columns of SummarisedResult).
NAME_LEVEL_SEP: str = " &&& "

#: Sentinel value for the "overall" stratum / group.
OVERALL: str = "overall"

#: Variable names that trigger group-level suppression in summarised results.
GROUP_COUNT_VARIABLES: tuple[str, ...] = ("number subjects", "number records")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class CdmVersion(str, enum.Enum):
    """Supported OMOP CDM versions."""

    V5_3 = "5.3"
    V5_4 = "5.4"

    def __str__(self) -> str:
        return self.value


class TableType(str, enum.Enum):
    """Classification of CDM table types."""

    CDM_TABLE = "cdm_table"
    COHORT = "cohort"
    ACHILLES = "achilles"

    def __str__(self) -> str:
        return self.value


class CdmDataType(str, enum.Enum):
    """Data types used in OMOP CDM field specifications."""

    INTEGER = "integer"
    FLOAT = "float"
    VARCHAR = "varchar"
    DATE = "date"
    DATETIME = "datetime"
    LOGICAL = "logical"

    @classmethod
    def from_spec(cls, raw: str) -> CdmDataType:
        """Parse a CDM datatype string like ``'varchar(50)'`` or ``'integer'``."""
        raw_lower = raw.strip().lower()
        if raw_lower.startswith("varchar"):
            return cls.VARCHAR
        try:
            return cls(raw_lower)
        except ValueError:
            msg = f"Unknown CDM datatype: {raw!r}"
            raise ValueError(msg) from None


class TableGroup(str, enum.Enum):
    """Logical groupings of CDM tables for batch selection."""

    VOCAB = "vocab"
    ALL = "all"
    CLINICAL = "clinical"
    DERIVED = "derived"
    DEFAULT = "default"

    def __str__(self) -> str:
        return self.value


class TableSchema(str, enum.Enum):
    """Database schema a CDM table lives in."""

    CDM = "cdm"
    VOCAB = "vocab"
    RESULTS = "results"

    def __str__(self) -> str:
        return self.value


# ---------------------------------------------------------------------------
# Literal types for strict typing
# ---------------------------------------------------------------------------

CdmVersionLiteral = Literal["5.3", "5.4"]
TableTypeLiteral = Literal["cdm_table", "cohort", "achilles"]
