"""Tests for omopy.generics._types — enums, constants, and type aliases."""

import pytest

from omopy.generics._types import (
    GROUP_COUNT_VARIABLES,
    NAME_LEVEL_SEP,
    OVERALL,
    SUPPORTED_CDM_VERSIONS,
    CdmDataType,
    CdmVersion,
    TableGroup,
    TableSchema,
    TableType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_supported_cdm_versions(self):
        assert SUPPORTED_CDM_VERSIONS == ("5.3", "5.4")

    def test_name_level_sep(self):
        assert NAME_LEVEL_SEP == " &&& "
        # Must have spaces around the ampersands
        assert NAME_LEVEL_SEP.strip() == "&&&"

    def test_overall(self):
        assert OVERALL == "overall"

    def test_group_count_variables(self):
        assert "number subjects" in GROUP_COUNT_VARIABLES
        assert "number records" in GROUP_COUNT_VARIABLES
        assert len(GROUP_COUNT_VARIABLES) == 2


# ---------------------------------------------------------------------------
# CdmVersion
# ---------------------------------------------------------------------------


class TestCdmVersion:
    def test_values(self):
        assert CdmVersion.V5_3.value == "5.3"
        assert CdmVersion.V5_4.value == "5.4"

    def test_str(self):
        assert str(CdmVersion.V5_3) == "5.3"
        assert str(CdmVersion.V5_4) == "5.4"

    def test_is_str_enum(self):
        assert isinstance(CdmVersion.V5_3, str)

    def test_from_string(self):
        assert CdmVersion("5.3") is CdmVersion.V5_3
        assert CdmVersion("5.4") is CdmVersion.V5_4

    def test_invalid_version(self):
        with pytest.raises(ValueError):
            CdmVersion("5.5")

    def test_members(self):
        assert len(CdmVersion) == 2


# ---------------------------------------------------------------------------
# TableType
# ---------------------------------------------------------------------------


class TestTableType:
    def test_values(self):
        assert TableType.CDM_TABLE.value == "cdm_table"
        assert TableType.COHORT.value == "cohort"
        assert TableType.ACHILLES.value == "achilles"

    def test_str(self):
        assert str(TableType.CDM_TABLE) == "cdm_table"

    def test_from_string(self):
        assert TableType("cdm_table") is TableType.CDM_TABLE

    def test_members(self):
        assert len(TableType) == 3


# ---------------------------------------------------------------------------
# CdmDataType
# ---------------------------------------------------------------------------


class TestCdmDataType:
    def test_all_values(self):
        expected = {"integer", "float", "varchar", "date", "datetime", "logical"}
        actual = {dt.value for dt in CdmDataType}
        assert actual == expected

    def test_from_spec_simple(self):
        assert CdmDataType.from_spec("integer") is CdmDataType.INTEGER
        assert CdmDataType.from_spec("float") is CdmDataType.FLOAT
        assert CdmDataType.from_spec("date") is CdmDataType.DATE
        assert CdmDataType.from_spec("datetime") is CdmDataType.DATETIME

    def test_from_spec_varchar(self):
        assert CdmDataType.from_spec("varchar(50)") is CdmDataType.VARCHAR
        assert CdmDataType.from_spec("varchar(max)") is CdmDataType.VARCHAR
        assert CdmDataType.from_spec("VARCHAR(255)") is CdmDataType.VARCHAR

    def test_from_spec_whitespace(self):
        assert CdmDataType.from_spec("  integer  ") is CdmDataType.INTEGER

    def test_from_spec_unknown(self):
        with pytest.raises(ValueError, match="Unknown CDM datatype"):
            CdmDataType.from_spec("blob")


# ---------------------------------------------------------------------------
# TableGroup
# ---------------------------------------------------------------------------


class TestTableGroup:
    def test_values(self):
        expected = {"vocab", "all", "clinical", "derived", "default"}
        actual = {g.value for g in TableGroup}
        assert actual == expected

    def test_str(self):
        assert str(TableGroup.CLINICAL) == "clinical"


# ---------------------------------------------------------------------------
# TableSchema
# ---------------------------------------------------------------------------


class TestTableSchema:
    def test_values(self):
        expected = {"cdm", "vocab", "results"}
        actual = {s.value for s in TableSchema}
        assert actual == expected

    def test_str(self):
        assert str(TableSchema.CDM) == "cdm"
