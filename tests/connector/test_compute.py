"""Tests for omopy.connector.compute.

Covers compute_permanent, append_permanent, compute_query.
"""

from __future__ import annotations

import ibis
import pytest

from omopy.connector._connection import _get_catalog
from omopy.connector.compute import (
    _table_exists,
    _unique_table_name,
    append_permanent,
    compute_permanent,
    compute_query,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def writable_con(tmp_path):
    """Writable DuckDB connection with sample data."""
    db_path = tmp_path / "test_compute.duckdb"
    con = ibis.duckdb.connect(str(db_path))

    # Create a schema and sample table
    con.raw_sql('CREATE SCHEMA IF NOT EXISTS "work"')
    con.raw_sql("""
        CREATE TABLE work.sample_data (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)
    con.raw_sql("INSERT INTO work.sample_data VALUES (1, 'a', 1.0)")
    con.raw_sql("INSERT INTO work.sample_data VALUES (2, 'b', 2.0)")
    con.raw_sql("INSERT INTO work.sample_data VALUES (3, 'c', 3.0)")

    yield con
    con.disconnect()


@pytest.fixture()
def sample_expr(writable_con):
    """An Ibis expression referencing the sample data."""
    catalog = _get_catalog(writable_con)
    return writable_con.table("sample_data", database=(catalog, "work"))


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestHelpers:
    """Test utility functions."""

    def test_unique_table_name(self):
        name1 = _unique_table_name()
        name2 = _unique_table_name()
        assert name1 != name2
        assert name1.startswith("omopy_tmp_")

    def test_unique_table_name_custom_prefix(self):
        name = _unique_table_name("my_prefix")
        assert name.startswith("my_prefix_")

    def test_table_exists_true(self, writable_con):
        assert _table_exists(writable_con, "sample_data", "work") is True

    def test_table_exists_false(self, writable_con):
        assert _table_exists(writable_con, "nonexistent", "work") is False


# ---------------------------------------------------------------------------
# compute_permanent tests
# ---------------------------------------------------------------------------


class TestComputePermanent:
    """Tests for compute_permanent."""

    def test_basic_materialise(self, writable_con, sample_expr):
        """Can materialise an expression to a permanent table."""
        result = compute_permanent(
            sample_expr.filter(sample_expr.value > 1.5),
            name="filtered",
            con=writable_con,
            schema="work",
        )
        # Returns a lazy Ibis table
        df = result.to_pyarrow()
        assert df.num_rows == 2  # value > 1.5: rows 2 and 3

    def test_overwrite_true(self, writable_con, sample_expr):
        """overwrite=True replaces existing table."""
        compute_permanent(
            sample_expr,
            name="overwrite_test",
            con=writable_con,
            schema="work",
        )
        # Now overwrite with filtered data
        result = compute_permanent(
            sample_expr.filter(sample_expr.id == 1),
            name="overwrite_test",
            con=writable_con,
            schema="work",
            overwrite=True,
        )
        df = result.to_pyarrow()
        assert df.num_rows == 1

    def test_overwrite_false_raises(self, writable_con, sample_expr):
        """overwrite=False raises when table exists."""
        compute_permanent(
            sample_expr,
            name="no_overwrite",
            con=writable_con,
            schema="work",
        )
        with pytest.raises(ValueError, match="already exists"):
            compute_permanent(
                sample_expr,
                name="no_overwrite",
                con=writable_con,
                schema="work",
                overwrite=False,
            )

    def test_auto_infer_connection(self, writable_con, sample_expr):
        """Connection is inferred from the expression when not passed."""
        result = compute_permanent(
            sample_expr,
            name="inferred_con",
            schema="work",
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_cdm_table_input(self, writable_con, sample_expr):
        """Can accept a CdmTable wrapping an Ibis expression."""
        from omopy.generics.cdm_table import CdmTable

        cdm_tbl = CdmTable(data=sample_expr, tbl_name="sample")
        result = compute_permanent(
            cdm_tbl,
            name="from_cdm_table",
            con=writable_con,
            schema="work",
        )
        assert result.to_pyarrow().num_rows == 3

    def test_invalid_input_type(self):
        """Raises TypeError for non-Ibis, non-CdmTable input."""
        with pytest.raises(TypeError, match="Expected Ibis Table or CdmTable"):
            compute_permanent("not_a_table", name="test")

    def test_creates_schema_if_needed(self, writable_con, sample_expr):
        """Automatically creates the schema if it doesn't exist."""
        result = compute_permanent(
            sample_expr,
            name="new_schema_table",
            con=writable_con,
            schema="new_schema",
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3


# ---------------------------------------------------------------------------
# append_permanent tests
# ---------------------------------------------------------------------------


class TestAppendPermanent:
    """Tests for append_permanent."""

    def test_append_to_existing(self, writable_con, sample_expr):
        """Appends rows to an existing table."""
        compute_permanent(
            sample_expr.filter(sample_expr.id <= 2),
            name="append_target",
            con=writable_con,
            schema="work",
        )
        # Now append the third row
        append_permanent(
            sample_expr.filter(sample_expr.id == 3),
            name="append_target",
            con=writable_con,
            schema="work",
        )
        catalog = _get_catalog(writable_con)
        result = writable_con.table("append_target", database=(catalog, "work"))
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_append_creates_if_not_exists(self, writable_con, sample_expr):
        """Creates the table if it doesn't exist."""
        result = append_permanent(
            sample_expr,
            name="append_new",
            con=writable_con,
            schema="work",
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_append_twice(self, writable_con, sample_expr):
        """Can append multiple times."""
        append_permanent(
            sample_expr.filter(sample_expr.id == 1),
            name="append_multi",
            con=writable_con,
            schema="work",
        )
        append_permanent(
            sample_expr.filter(sample_expr.id == 2),
            name="append_multi",
            con=writable_con,
            schema="work",
        )
        append_permanent(
            sample_expr.filter(sample_expr.id == 3),
            name="append_multi",
            con=writable_con,
            schema="work",
        )
        catalog = _get_catalog(writable_con)
        result = writable_con.table("append_multi", database=(catalog, "work"))
        df = result.to_pyarrow()
        assert df.num_rows == 3


# ---------------------------------------------------------------------------
# compute_query tests
# ---------------------------------------------------------------------------


class TestComputeQuery:
    """Tests for compute_query."""

    def test_temporary_table(self, writable_con, sample_expr):
        """Creates a temporary table."""
        result = compute_query(
            sample_expr,
            name="temp_test",
            con=writable_con,
            temporary=True,
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_temporary_with_auto_name(self, writable_con, sample_expr):
        """Auto-generates a unique name for temp tables."""
        result = compute_query(
            sample_expr,
            con=writable_con,
            temporary=True,
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_permanent_via_compute_query(self, writable_con, sample_expr):
        """compute_query with temporary=False creates permanent table."""
        result = compute_query(
            sample_expr,
            name="perm_via_query",
            con=writable_con,
            temporary=False,
            schema="work",
        )
        df = result.to_pyarrow()
        assert df.num_rows == 3

    def test_overwrite_temp(self, writable_con, sample_expr):
        """Can overwrite a temporary table."""
        compute_query(
            sample_expr,
            name="temp_overwrite",
            con=writable_con,
            temporary=True,
        )
        result = compute_query(
            sample_expr.filter(sample_expr.id == 1),
            name="temp_overwrite",
            con=writable_con,
            temporary=True,
            overwrite=True,
        )
        df = result.to_pyarrow()
        assert df.num_rows == 1

    def test_complex_expression(self, writable_con, sample_expr):
        """Can materialise a complex expression (filter + mutate)."""
        expr = sample_expr.filter(sample_expr.value >= 2.0).select(
            sample_expr.id,
            doubled=sample_expr.value * 2,
        )
        result = compute_query(
            expr,
            name="complex",
            con=writable_con,
            temporary=True,
        )
        df = result.to_pyarrow()
        assert df.num_rows == 2
        assert "doubled" in [f.name for f in df.schema]


# ---------------------------------------------------------------------------
# Integration with Synthea (read-only — limited to read operations)
# ---------------------------------------------------------------------------


class TestComputeWithSynthea:
    """Integration tests using the Synthea test database.

    Synthea is read-only, so we can only test that expressions compile
    and that compute works against a writable copy.
    """

    def test_compute_from_synthea_data(self, synthea_con, tmp_path):
        """Can compute a query sourced from Synthea data into a new DB."""
        # Get data from synthea
        person = synthea_con.table("person", database=("synthea", "base"))
        person_df = person.select("person_id", "gender_concept_id").to_pyarrow()

        # Create a writable DB and upload
        write_con = ibis.duckdb.connect(str(tmp_path / "write.duckdb"))
        write_con.con.register("person_data", person_df)
        person_tbl = write_con.table("person_data")

        # Compute a permanent table
        result = compute_permanent(
            person_tbl.filter(person_tbl.gender_concept_id == 8507),
            name="male_persons",
            con=write_con,
            schema="main",
        )
        df = result.to_pyarrow()
        assert df.num_rows > 0
        write_con.disconnect()
