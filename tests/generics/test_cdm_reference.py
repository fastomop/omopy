"""Tests for omopy.generics.cdm_reference — CdmReference and CdmSource."""

import polars as pl
import pytest

from omopy.generics._types import CdmVersion
from omopy.generics.cdm_reference import CdmReference, CdmSource
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _person_table() -> CdmTable:
    return CdmTable(
        pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "gender_concept_id": [8507, 8532, 8507],
                "year_of_birth": [1990, 1985, 2000],
            }
        ),
        tbl_name="person",
    )


def _obs_period_table() -> CdmTable:
    return CdmTable(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2, 3],
                "person_id": [1, 2, 3],
                "observation_period_start_date": ["2020-01-01"] * 3,
                "observation_period_end_date": ["2024-12-31"] * 3,
                "period_type_concept_id": [44814724] * 3,
            }
        ),
        tbl_name="observation_period",
    )


def _cohort_table() -> CohortTable:
    return CohortTable(
        pl.DataFrame(
            {
                "cohort_definition_id": [1, 1, 2],
                "subject_id": [1, 2, 1],
                "cohort_start_date": ["2020-01-01"] * 3,
                "cohort_end_date": ["2020-12-31"] * 3,
            }
        ),
        tbl_name="my_cohort",
    )


# ---------------------------------------------------------------------------
# CdmReference basics
# ---------------------------------------------------------------------------


class TestCdmReferenceBasics:
    def test_creation_empty(self):
        cdm = CdmReference()
        assert len(cdm) == 0
        assert cdm.cdm_version is CdmVersion.V5_4

    def test_creation_with_tables(self):
        cdm = CdmReference(
            tables={
                "person": _person_table(),
                "observation_period": _obs_period_table(),
            },
            cdm_version=CdmVersion.V5_4,
            cdm_name="test_cdm",
        )
        assert len(cdm) == 2
        assert cdm.cdm_name == "test_cdm"

    def test_creation_sets_cdm_backref(self):
        person = _person_table()
        cdm = CdmReference(tables={"person": person})
        assert person.cdm is cdm

    def test_version(self):
        cdm = CdmReference(cdm_version=CdmVersion.V5_3)
        assert cdm.cdm_version is CdmVersion.V5_3


# ---------------------------------------------------------------------------
# Dict-like access
# ---------------------------------------------------------------------------


class TestCdmReferenceAccess:
    def test_getitem(self):
        cdm = CdmReference(tables={"person": _person_table()})
        assert cdm["person"].tbl_name == "person"

    def test_getitem_missing(self):
        cdm = CdmReference()
        with pytest.raises(KeyError, match="not found"):
            cdm["person"]

    def test_setitem(self):
        cdm = CdmReference()
        person = _person_table()
        cdm["person"] = person
        assert "person" in cdm
        assert person.cdm is cdm

    def test_delitem(self):
        cdm = CdmReference(tables={"person": _person_table()})
        del cdm["person"]
        assert "person" not in cdm
        assert len(cdm) == 0

    def test_delitem_missing(self):
        cdm = CdmReference()
        with pytest.raises(KeyError, match="not found"):
            del cdm["person"]

    def test_contains(self):
        cdm = CdmReference(tables={"person": _person_table()})
        assert "person" in cdm
        assert "missing" not in cdm

    def test_iter(self):
        cdm = CdmReference(
            tables={
                "person": _person_table(),
                "observation_period": _obs_period_table(),
            }
        )
        names = list(cdm)
        assert set(names) == {"person", "observation_period"}

    def test_len(self):
        cdm = CdmReference(tables={"person": _person_table()})
        assert len(cdm) == 1

    def test_get(self):
        cdm = CdmReference(tables={"person": _person_table()})
        assert cdm.get("person") is not None
        assert cdm.get("missing") is None
        assert cdm.get("missing", "default") == "default"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestCdmReferenceProperties:
    def test_table_names(self):
        cdm = CdmReference(
            tables={
                "person": _person_table(),
                "observation_period": _obs_period_table(),
            }
        )
        assert set(cdm.table_names) == {"person", "observation_period"}

    def test_cdm_name_setter(self):
        cdm = CdmReference(cdm_name="old")
        cdm.cdm_name = "new"
        assert cdm.cdm_name == "new"

    def test_cdm_source_none(self):
        cdm = CdmReference()
        assert cdm.cdm_source is None

    def test_cohort_tables(self):
        cdm = CdmReference(
            tables={
                "person": _person_table(),
                "my_cohort": _cohort_table(),
            }
        )
        cohorts = cdm.cohort_tables
        assert "my_cohort" in cohorts
        assert "person" not in cohorts
        assert isinstance(cohorts["my_cohort"], CohortTable)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


class TestCdmReferenceSnapshot:
    def test_snapshot(self):
        cdm = CdmReference(
            tables={"person": _person_table()},
            cdm_name="test",
        )
        snap = cdm.snapshot()
        assert snap["cdm_name"] == "test"
        assert snap["cdm_version"] == "5.4"
        assert snap["source_type"] == "local"
        assert "person" in snap["tables"]
        assert snap["tables"]["person"]["nrows"] == 3


# ---------------------------------------------------------------------------
# Table selection
# ---------------------------------------------------------------------------


class TestSelectTables:
    def test_select_existing(self):
        cdm = CdmReference(
            tables={
                "person": _person_table(),
                "observation_period": _obs_period_table(),
            }
        )
        subset = cdm.select_tables(["person"])
        assert len(subset) == 1
        assert "person" in subset

    def test_select_nonexistent_ignored(self):
        cdm = CdmReference(tables={"person": _person_table()})
        subset = cdm.select_tables(["person", "nonexistent"])
        assert len(subset) == 1


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestCdmReferenceRepr:
    def test_repr_named(self):
        cdm = CdmReference(
            tables={"person": _person_table()},
            cdm_name="my_cdm",
        )
        r = repr(cdm)
        assert "my_cdm" in r
        assert "local" in r
        assert "tables=1" in r

    def test_repr_unnamed(self):
        cdm = CdmReference()
        r = repr(cdm)
        assert "(unnamed)" in r


# ---------------------------------------------------------------------------
# CdmSource protocol
# ---------------------------------------------------------------------------


class TestCdmSourceProtocol:
    def test_protocol_check(self):
        """CdmSource is a Protocol — can't instantiate directly but can check."""

        class FakeSource:
            @property
            def source_type(self) -> str:
                return "fake"

            def list_tables(self) -> list[str]:
                return ["person"]

            def read_table(self, table_name: str) -> CdmTable:
                return _person_table()

            def write_table(
                self, table: CdmTable, table_name: str | None = None
            ) -> None:
                pass

            def drop_table(self, table_name: str) -> None:
                pass

        source = FakeSource()
        assert isinstance(source, CdmSource)
