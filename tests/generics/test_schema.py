"""Tests for omopy.generics._schema — FieldSpec, TableSpec, ResultFieldSpec, CdmSchema."""

import dataclasses

import pytest

from omopy.generics._schema import (
    FIELD_TABLE_COLUMNS,
    CdmSchema,
    FieldSpec,
    ResultFieldSpec,
    TableSpec,
    _FieldTableColumn,
)
from omopy.generics._types import (
    CdmDataType,
    CdmVersion,
    TableGroup,
    TableSchema,
    TableType,
)


# ---------------------------------------------------------------------------
# FieldSpec
# ---------------------------------------------------------------------------


class TestFieldSpec:
    def test_creation(self):
        fs = FieldSpec(
            cdm_table_name="person",
            cdm_field_name="person_id",
            is_required=True,
            cdm_datatype="integer",
            type=TableType.CDM_TABLE,
        )
        assert fs.cdm_table_name == "person"
        assert fs.cdm_field_name == "person_id"
        assert fs.is_required is True
        assert fs.cdm_datatype == "integer"
        assert fs.type is TableType.CDM_TABLE

    def test_frozen(self):
        fs = FieldSpec("person", "person_id", True, "integer", TableType.CDM_TABLE)
        with pytest.raises(dataclasses.FrozenInstanceError):
            fs.cdm_table_name = "other"  # type: ignore[misc]

    def test_datatype_enum_integer(self):
        fs = FieldSpec("t", "f", True, "integer", TableType.CDM_TABLE)
        assert fs.datatype_enum is CdmDataType.INTEGER

    def test_datatype_enum_varchar(self):
        fs = FieldSpec("t", "f", True, "varchar(50)", TableType.CDM_TABLE)
        assert fs.datatype_enum is CdmDataType.VARCHAR

    def test_varchar_length(self):
        fs = FieldSpec("t", "f", True, "varchar(255)", TableType.CDM_TABLE)
        assert fs.varchar_length == 255

    def test_varchar_max(self):
        fs = FieldSpec("t", "f", True, "varchar(max)", TableType.CDM_TABLE)
        assert fs.varchar_length is None

    def test_varchar_length_non_varchar(self):
        fs = FieldSpec("t", "f", True, "integer", TableType.CDM_TABLE)
        assert fs.varchar_length is None

    def test_equality(self):
        a = FieldSpec("t", "f", True, "integer", TableType.CDM_TABLE)
        b = FieldSpec("t", "f", True, "integer", TableType.CDM_TABLE)
        assert a == b

    def test_hashable(self):
        fs = FieldSpec("t", "f", True, "integer", TableType.CDM_TABLE)
        assert hash(fs) is not None
        # Can be used in sets
        s = {fs}
        assert fs in s


# ---------------------------------------------------------------------------
# TableSpec
# ---------------------------------------------------------------------------


class TestTableSpec:
    def test_creation(self):
        ts = TableSpec(
            cdm_table_name="person",
            schema=TableSchema.CDM,
            is_required=True,
        )
        assert ts.cdm_table_name == "person"
        assert ts.schema is TableSchema.CDM
        assert ts.is_required is True
        assert ts.concept_prefix is None

    def test_frozen(self):
        ts = TableSpec("person", TableSchema.CDM, True)
        with pytest.raises(dataclasses.FrozenInstanceError):
            ts.cdm_table_name = "other"  # type: ignore[misc]

    def test_na_to_none(self):
        ts = TableSpec("t", TableSchema.CDM, True, concept_prefix="NA")
        assert ts.concept_prefix is None

    def test_na_case_insensitive(self):
        ts = TableSpec("t", TableSchema.CDM, True, concept_prefix="na")
        assert ts.concept_prefix is None

    def test_non_na_preserved(self):
        ts = TableSpec("t", TableSchema.CDM, True, concept_prefix="condition")
        assert ts.concept_prefix == "condition"

    def test_in_group(self):
        ts = TableSpec("t", TableSchema.CDM, True, group_clinical=True, group_all=True)
        assert ts.in_group(TableGroup.CLINICAL) is True
        assert ts.in_group(TableGroup.ALL) is True
        assert ts.in_group(TableGroup.VOCAB) is False
        assert ts.in_group(TableGroup.DERIVED) is False

    def test_defaults(self):
        ts = TableSpec("t", TableSchema.CDM, True)
        assert ts.group_vocab is False
        assert ts.group_all is False
        assert ts.group_clinical is False
        assert ts.group_derived is False
        assert ts.group_default is False


# ---------------------------------------------------------------------------
# ResultFieldSpec
# ---------------------------------------------------------------------------


class TestResultFieldSpec:
    def test_creation(self):
        rfs = ResultFieldSpec(
            result="summarised_result",
            result_field_name="result_id",
            is_required=True,
            datatype="integer",
            na_allowed=False,
        )
        assert rfs.result == "summarised_result"
        assert rfs.result_field_name == "result_id"
        assert rfs.pair is None

    def test_with_pair(self):
        rfs = ResultFieldSpec("summarised_result", "group_name", True, "character", False, "name1")
        assert rfs.pair == "name1"

    def test_frozen(self):
        rfs = ResultFieldSpec("r", "f", True, "integer", False)
        with pytest.raises(dataclasses.FrozenInstanceError):
            rfs.result = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _FieldTableColumn
# ---------------------------------------------------------------------------


class TestFieldTableColumn:
    def test_field_table_columns_count(self):
        assert len(FIELD_TABLE_COLUMNS) == 16

    def test_person_entry(self):
        person = FIELD_TABLE_COLUMNS[0]
        assert person.table_name == "person"
        assert person.unique_id == "person_id"
        assert person.person_id == "person_id"
        assert person.start_date is None
        assert person.standard_concept is None

    def test_condition_occurrence_entry(self):
        cond = next(fc for fc in FIELD_TABLE_COLUMNS if fc.table_name == "condition_occurrence")
        assert cond.start_date == "condition_start_date"
        assert cond.end_date == "condition_end_date"
        assert cond.standard_concept == "condition_concept_id"
        assert cond.domain_id == "Condition"

    def test_all_have_person_id(self):
        for fc in FIELD_TABLE_COLUMNS:
            assert fc.person_id == "person_id", f"{fc.table_name} missing person_id"


# ---------------------------------------------------------------------------
# CdmSchema
# ---------------------------------------------------------------------------


class TestCdmSchema:
    def test_default_version(self):
        schema = CdmSchema()
        assert schema.version is CdmVersion.V5_4

    def test_v54_field_count(self):
        schema = CdmSchema(CdmVersion.V5_4)
        assert len(schema.field_specs) == 484

    def test_v53_field_count(self):
        schema = CdmSchema(CdmVersion.V5_3)
        assert len(schema.field_specs) == 448

    def test_v54_table_count(self):
        schema = CdmSchema(CdmVersion.V5_4)
        assert len(schema.table_specs) == 39

    def test_v53_table_count(self):
        schema = CdmSchema(CdmVersion.V5_3)
        assert len(schema.table_specs) == 37

    def test_fields_for_table_person(self):
        schema = CdmSchema(CdmVersion.V5_4)
        person_fields = schema.fields_for_table("person")
        assert len(person_fields) > 0
        assert all(f.cdm_table_name == "person" for f in person_fields)
        names = {f.cdm_field_name for f in person_fields}
        assert "person_id" in names
        assert "gender_concept_id" in names

    def test_required_fields_for_table(self):
        schema = CdmSchema(CdmVersion.V5_4)
        required = schema.required_fields_for_table("person")
        assert all(f.is_required for f in required)
        assert len(required) <= len(schema.fields_for_table("person"))

    def test_table_names_all(self):
        schema = CdmSchema(CdmVersion.V5_4)
        names = schema.table_names()
        assert "person" in names
        assert "observation_period" in names
        assert "cohort" in names

    def test_table_names_by_type(self):
        schema = CdmSchema(CdmVersion.V5_4)
        cdm_tables = schema.table_names(table_type=TableType.CDM_TABLE)
        assert "person" in cdm_tables
        # Cohort tables should not be in cdm_table type
        cohort_tables = schema.table_names(table_type=TableType.COHORT)
        assert "cohort" in cohort_tables

    def test_required_table_names(self):
        schema = CdmSchema(CdmVersion.V5_4)
        required = schema.required_table_names()
        assert "person" in required
        assert "observation_period" in required

    def test_table_names_in_group(self):
        schema = CdmSchema(CdmVersion.V5_4)
        clinical = schema.table_names_in_group(TableGroup.CLINICAL)
        assert len(clinical) > 0
        # Clinical tables should include things like condition_occurrence
        assert "condition_occurrence" in clinical

    def test_table_spec_for(self):
        schema = CdmSchema(CdmVersion.V5_4)
        person_spec = schema.table_spec_for("person")
        assert person_spec is not None
        assert person_spec.cdm_table_name == "person"
        assert person_spec.is_required is True

    def test_table_spec_for_nonexistent(self):
        schema = CdmSchema(CdmVersion.V5_4)
        assert schema.table_spec_for("nonexistent_table") is None

    def test_field_column_info(self):
        schema = CdmSchema(CdmVersion.V5_4)
        info = schema.field_column_info("condition_occurrence")
        assert info is not None
        assert info.start_date == "condition_start_date"

    def test_field_column_info_nonexistent(self):
        schema = CdmSchema(CdmVersion.V5_4)
        assert schema.field_column_info("nonexistent_table") is None

    def test_result_field_specs(self):
        schema = CdmSchema()
        rfs = schema.result_field_specs
        assert len(rfs) > 0
        # Should have both summarised_result and settings entries
        results = {r.result for r in rfs}
        assert "summarised_result" in results
        assert "settings" in results

    def test_validate_columns_ok(self):
        schema = CdmSchema(CdmVersion.V5_4)
        person_cols = [f.cdm_field_name for f in schema.fields_for_table("person")]
        errors = schema.validate_columns("person", person_cols)
        assert errors == []

    def test_validate_columns_missing_required(self):
        schema = CdmSchema(CdmVersion.V5_4)
        errors = schema.validate_columns("person", ["person_id"])
        # Should report missing required columns
        assert len(errors) > 0
        assert any("missing" in e.lower() for e in errors)

    def test_validate_columns_unknown_table(self):
        schema = CdmSchema(CdmVersion.V5_4)
        errors = schema.validate_columns("nonexistent", ["col1"])
        assert errors == []

    def test_v54_has_episode(self):
        """v5.4 should have episode/episode_event tables that v5.3 doesn't."""
        schema54 = CdmSchema(CdmVersion.V5_4)
        schema53 = CdmSchema(CdmVersion.V5_3)
        names54 = schema54.table_names()
        names53 = schema53.table_names()
        assert "episode" in names54
        assert "episode" not in names53

    def test_caching(self):
        """Schema data is cached — two CdmSchema instances share field data."""
        a = CdmSchema(CdmVersion.V5_4)
        b = CdmSchema(CdmVersion.V5_4)
        assert a.field_specs is b.field_specs  # same tuple object from cache
