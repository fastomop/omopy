"""CDM schema specifications loaded from CSV data files.

This module provides:
- ``FieldSpec``: Frozen Pydantic model for a single CDM field definition.
- ``TableSpec``: Frozen Pydantic model for a CDM table-level definition.
- ``ResultFieldSpec``: Frozen Pydantic model for a summarised-result field definition.
- ``CdmSchema``: Main registry that lazily loads and caches specs for v5.3 / v5.4.
"""

from __future__ import annotations

import csv
import functools
import re
import warnings
from collections.abc import Sequence
from importlib import resources

from pydantic import BaseModel, ConfigDict, model_validator

from omopy.generics._types import (
    CdmDataType,
    CdmVersion,
    TableGroup,
    TableSchema,
    TableType,
)

__all__ = [
    "FieldSpec",
    "TableSpec",
    "ResultFieldSpec",
    "CdmSchema",
    "FIELD_TABLE_COLUMNS",
]


# ---------------------------------------------------------------------------
# Frozen Pydantic models for spec rows
# ---------------------------------------------------------------------------


class FieldSpec(BaseModel):
    """A single field in a CDM table (from ``fieldsTables``)."""

    model_config = ConfigDict(frozen=True)

    cdm_table_name: str
    cdm_field_name: str
    is_required: bool
    cdm_datatype: str  # raw string, e.g. "varchar(50)", "integer"
    type: TableType

    @property
    def datatype_enum(self) -> CdmDataType:
        return CdmDataType.from_spec(self.cdm_datatype)

    @property
    def varchar_length(self) -> int | None:
        """Extract max length from ``varchar(N)`` or ``varchar(max)``."""
        m = re.match(r"varchar\((\w+)\)", self.cdm_datatype, re.IGNORECASE)
        if m:
            val = m.group(1)
            return None if val.lower() == "max" else int(val)
        return None


# Suppress Pydantic warning about 'schema' field shadowing the deprecated
# BaseModel.schema() classmethod — we never call it as a classmethod.
warnings.filterwarnings(
    "ignore",
    message='Field name "schema" in "TableSpec"',
    category=UserWarning,
)


class TableSpec(BaseModel):
    """Table-level metadata from the CDM spec CSVs."""

    model_config = ConfigDict(frozen=True, protected_namespaces=())

    cdm_table_name: str
    schema: TableSchema
    is_required: bool
    concept_prefix: str | None = None
    group_vocab: bool = False
    group_all: bool = False
    group_clinical: bool = False
    group_derived: bool = False
    group_default: bool = False

    @model_validator(mode="after")
    def _normalise_na(self) -> TableSpec:
        # Normalise "NA" strings to None for concept_prefix
        if isinstance(self.concept_prefix, str) and self.concept_prefix.upper() == "NA":
            object.__setattr__(self, "concept_prefix", None)
        return self

    def in_group(self, group: TableGroup) -> bool:
        mapping = {
            TableGroup.VOCAB: self.group_vocab,
            TableGroup.ALL: self.group_all,
            TableGroup.CLINICAL: self.group_clinical,
            TableGroup.DERIVED: self.group_derived,
            TableGroup.DEFAULT: self.group_default,
        }
        return mapping.get(group, False)


class ResultFieldSpec(BaseModel):
    """Field specification for a summarised/compared result."""

    model_config = ConfigDict(frozen=True)

    result: str
    result_field_name: str
    is_required: bool
    datatype: str
    na_allowed: bool
    pair: str | None = None


class _FieldTableColumn(BaseModel):
    """Mapping of clinical tables to their semantic column roles."""

    model_config = ConfigDict(frozen=True)

    table_name: str
    start_date: str | None = None
    end_date: str | None = None
    standard_concept: str | None = None
    source_concept: str | None = None
    type_concept: str | None = None
    unique_id: str | None = None
    domain_id: str | None = None
    person_id: str | None = None


# ---------------------------------------------------------------------------
# Hardcoded fieldTablesColumns (from omopgenerics sysdata.rda)
# ---------------------------------------------------------------------------

FIELD_TABLE_COLUMNS: tuple[_FieldTableColumn, ...] = (
    _FieldTableColumn(
        table_name="person",
        start_date=None,
        end_date=None,
        standard_concept=None,
        source_concept=None,
        type_concept=None,
        unique_id="person_id",
        domain_id=None,
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="observation_period",
        start_date="observation_period_start_date",
        end_date="observation_period_end_date",
        standard_concept=None,
        source_concept=None,
        type_concept="period_type_concept_id",
        unique_id="observation_period_id",
        domain_id=None,
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="visit_occurrence",
        start_date="visit_start_date",
        end_date="visit_end_date",
        standard_concept="visit_concept_id",
        source_concept="visit_source_concept_id",
        type_concept="visit_type_concept_id",
        unique_id="visit_occurrence_id",
        domain_id="Visit",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="visit_detail",
        start_date="visit_detail_start_date",
        end_date="visit_detail_end_date",
        standard_concept="visit_detail_concept_id",
        source_concept="visit_detail_source_concept_id",
        type_concept="visit_detail_type_concept_id",
        unique_id="visit_detail_id",
        domain_id="Visit Detail",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="condition_occurrence",
        start_date="condition_start_date",
        end_date="condition_end_date",
        standard_concept="condition_concept_id",
        source_concept="condition_source_concept_id",
        type_concept="condition_type_concept_id",
        unique_id="condition_occurrence_id",
        domain_id="Condition",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="drug_exposure",
        start_date="drug_exposure_start_date",
        end_date="drug_exposure_end_date",
        standard_concept="drug_concept_id",
        source_concept="drug_source_concept_id",
        type_concept="drug_type_concept_id",
        unique_id="drug_exposure_id",
        domain_id="Drug",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="procedure_occurrence",
        start_date="procedure_date",
        end_date=None,
        standard_concept="procedure_concept_id",
        source_concept="procedure_source_concept_id",
        type_concept="procedure_type_concept_id",
        unique_id="procedure_occurrence_id",
        domain_id="Procedure",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="device_exposure",
        start_date="device_exposure_start_date",
        end_date="device_exposure_end_date",
        standard_concept="device_concept_id",
        source_concept="device_source_concept_id",
        type_concept="device_type_concept_id",
        unique_id="device_exposure_id",
        domain_id="Device",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="measurement",
        start_date="measurement_date",
        end_date=None,
        standard_concept="measurement_concept_id",
        source_concept="measurement_source_concept_id",
        type_concept="measurement_type_concept_id",
        unique_id="measurement_id",
        domain_id="Measurement",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="observation",
        start_date="observation_date",
        end_date=None,
        standard_concept="observation_concept_id",
        source_concept="observation_source_concept_id",
        type_concept="observation_type_concept_id",
        unique_id="observation_id",
        domain_id="Observation",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="death",
        start_date="death_date",
        end_date=None,
        standard_concept=None,
        source_concept=None,
        type_concept="death_type_concept_id",
        unique_id=None,
        domain_id=None,
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="specimen",
        start_date="specimen_date",
        end_date=None,
        standard_concept="specimen_concept_id",
        source_concept=None,
        type_concept="specimen_type_concept_id",
        unique_id="specimen_id",
        domain_id="Specimen",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="condition_era",
        start_date="condition_era_start_date",
        end_date="condition_era_end_date",
        standard_concept="condition_concept_id",
        source_concept=None,
        type_concept=None,
        unique_id="condition_era_id",
        domain_id="Condition",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="drug_era",
        start_date="drug_era_start_date",
        end_date="drug_era_end_date",
        standard_concept="drug_concept_id",
        source_concept=None,
        type_concept=None,
        unique_id="drug_era_id",
        domain_id="Drug",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="dose_era",
        start_date="dose_era_start_date",
        end_date="dose_era_end_date",
        standard_concept="drug_concept_id",
        source_concept=None,
        type_concept=None,
        unique_id="dose_era_id",
        domain_id="Drug",
        person_id="person_id",
    ),
    _FieldTableColumn(
        table_name="payer_plan_period",
        start_date="payer_plan_period_start_date",
        end_date="payer_plan_period_end_date",
        standard_concept=None,
        source_concept=None,
        type_concept=None,
        unique_id="payer_plan_period_id",
        domain_id=None,
        person_id="person_id",
    ),
)


# ---------------------------------------------------------------------------
# Hardcoded fieldsResults (from omopgenerics sysdata.rda)
# ---------------------------------------------------------------------------

_FIELDS_RESULTS: tuple[ResultFieldSpec, ...] = (
    # summarised_result (13 fields)
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="result_id",
        is_required=True,
        datatype="integer",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="cdm_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="group_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair="name1",
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="group_level",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair="level1",
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="strata_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair="name2",
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="strata_level",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair="level2",
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="variable_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="variable_level",
        is_required=True,
        datatype="character",
        na_allowed=True,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="estimate_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="estimate_type",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="estimate_value",
        is_required=True,
        datatype="character",
        na_allowed=True,
        pair=None,
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="additional_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair="name3",
    ),
    ResultFieldSpec(
        result="summarised_result",
        result_field_name="additional_level",
        is_required=True,
        datatype="character",
        na_allowed=True,
        pair="level3",
    ),
    # settings (4 required fields)
    ResultFieldSpec(
        result="settings",
        result_field_name="result_id",
        is_required=True,
        datatype="integer",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="settings",
        result_field_name="result_type",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="settings",
        result_field_name="package_name",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
    ResultFieldSpec(
        result="settings",
        result_field_name="package_version",
        is_required=True,
        datatype="character",
        na_allowed=False,
        pair=None,
    ),
)


# ---------------------------------------------------------------------------
# CSV loading helpers
# ---------------------------------------------------------------------------


def _bool_from_csv(val: str) -> bool:
    return val.strip().upper() == "TRUE"


def _read_fields_tables_csv(version: CdmVersion) -> tuple[FieldSpec, ...]:
    """Load the fieldsTables CSV (from extracted sysdata.rda) for a CDM version."""
    filename = f"fieldsTables_v{version.value.replace('.', '')}.csv"
    data_dir = resources.files("omopy.generics") / "_data"
    text = (data_dir / filename).read_text(encoding="utf-8")
    reader = csv.DictReader(text.splitlines())
    specs: list[FieldSpec] = []
    for row in reader:
        specs.append(
            FieldSpec(
                cdm_table_name=row["cdm_table_name"].strip().strip('"'),
                cdm_field_name=row["cdm_field_name"].strip().strip('"'),
                is_required=_bool_from_csv(row["is_required"]),
                cdm_datatype=row["cdm_datatype"].strip().strip('"'),
                type=TableType(row["type"].strip().strip('"')),
            )
        )
    return tuple(specs)


def _read_table_level_csv(version: CdmVersion) -> tuple[TableSpec, ...]:
    """Load the OMOP CDM Table Level CSV from inst/csv/."""
    filename = f"OMOP_CDMv{version.value}_Table_Level.csv"
    data_dir = resources.files("omopy.generics") / "_data"
    text = (data_dir / filename).read_text(encoding="utf-8")
    reader = csv.DictReader(text.splitlines())
    specs: list[TableSpec] = []
    for row in reader:
        specs.append(
            TableSpec(
                cdm_table_name=row["cdmTableName"],
                schema=TableSchema(row["schema"]),
                is_required=_bool_from_csv(row["isRequired"]),
                concept_prefix=row.get("conceptPrefix", "NA"),
                group_vocab=_bool_from_csv(row.get("group_vocab", "FALSE")),
                group_all=_bool_from_csv(row.get("group_all", "FALSE")),
                group_clinical=_bool_from_csv(row.get("group_clinical", "FALSE")),
                group_derived=_bool_from_csv(row.get("group_derived", "FALSE")),
                group_default=_bool_from_csv(row.get("group_default", "FALSE")),
            )
        )
    return tuple(specs)


# ---------------------------------------------------------------------------
# Main schema registry
# ---------------------------------------------------------------------------


class CdmSchema:
    """Registry for OMOP CDM schema specifications.

    All data is lazily loaded and cached at the class level on first access.

    Usage::

        schema = CdmSchema(CdmVersion.V5_4)
        person_fields = schema.fields_for_table("person")
        required_tables = schema.required_table_names()
    """

    def __init__(self, version: CdmVersion = CdmVersion.V5_4) -> None:
        self._version = version

    @property
    def version(self) -> CdmVersion:
        return self._version

    # -- Cached loaders (class-level, keyed by version) ---------------------

    @staticmethod
    @functools.cache
    def _get_field_specs(version: CdmVersion) -> tuple[FieldSpec, ...]:
        return _read_fields_tables_csv(version)

    @staticmethod
    @functools.cache
    def _get_table_specs(version: CdmVersion) -> tuple[TableSpec, ...]:
        return _read_table_level_csv(version)

    # -- Public accessors ---------------------------------------------------

    @property
    def field_specs(self) -> tuple[FieldSpec, ...]:
        """All field specs for this CDM version."""
        return self._get_field_specs(self._version)

    @property
    def table_specs(self) -> tuple[TableSpec, ...]:
        """All table-level specs for this CDM version."""
        return self._get_table_specs(self._version)

    @property
    def result_field_specs(self) -> tuple[ResultFieldSpec, ...]:
        """Specs for summarised_result / settings fields."""
        return _FIELDS_RESULTS

    @property
    def field_table_columns(self) -> tuple[_FieldTableColumn, ...]:
        """Semantic column mappings for clinical tables."""
        return FIELD_TABLE_COLUMNS

    def fields_for_table(self, table_name: str) -> tuple[FieldSpec, ...]:
        """Return field specs for a specific table."""
        return tuple(f for f in self.field_specs if f.cdm_table_name == table_name)

    def required_fields_for_table(self, table_name: str) -> tuple[FieldSpec, ...]:
        """Return only required field specs for a table."""
        return tuple(f for f in self.fields_for_table(table_name) if f.is_required)

    def table_names(self, *, table_type: TableType | None = None) -> tuple[str, ...]:
        """Return all table names, optionally filtered by type."""
        seen: set[str] = set()
        result: list[str] = []
        for f in self.field_specs:
            if table_type is not None and f.type != table_type:
                continue
            if f.cdm_table_name not in seen:
                seen.add(f.cdm_table_name)
                result.append(f.cdm_table_name)
        return tuple(result)

    def required_table_names(self) -> tuple[str, ...]:
        """Return names of tables marked as required at table level."""
        return tuple(t.cdm_table_name for t in self.table_specs if t.is_required)

    def table_names_in_group(self, group: TableGroup) -> tuple[str, ...]:
        """Return table names belonging to a logical group."""
        return tuple(t.cdm_table_name for t in self.table_specs if t.in_group(group))

    def table_spec_for(self, table_name: str) -> TableSpec | None:
        """Return the TableSpec for a specific table, or None."""
        for t in self.table_specs:
            if t.cdm_table_name == table_name:
                return t
        return None

    def field_column_info(self, table_name: str) -> _FieldTableColumn | None:
        """Get semantic column mapping for a clinical table."""
        for fc in FIELD_TABLE_COLUMNS:
            if fc.table_name == table_name:
                return fc
        return None

    def validate_columns(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        check_required: bool = True,
    ) -> list[str]:
        """Validate columns against the spec. Returns list of error messages.

        Checks:
        1. If *check_required*, all required columns must be present.
        2. (Warning-level) Extra columns not in spec are noted.
        """
        errors: list[str] = []
        specs = {f.cdm_field_name: f for f in self.fields_for_table(table_name)}

        if not specs:
            return errors  # unknown table, nothing to validate

        col_set = set(columns)
        if check_required:
            for name, spec in specs.items():
                if spec.is_required and name not in col_set:
                    errors.append(f"Required column '{name}' missing from table '{table_name}'")

        return errors
