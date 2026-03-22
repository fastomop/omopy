"""DbSource — database-backed CDM source implementing the CdmSource protocol.

DbSource wraps an Ibis backend connection and provides lazy access to
OMOP CDM tables. Tables are returned as CdmTable instances wrapping
Ibis Table expressions (lazy — no data fetched until ``.collect()``).
"""

from __future__ import annotations

from typing import Any

import ibis
import polars as pl

from omopy.generics._schema import CdmSchema
from omopy.generics._types import CdmVersion
from omopy.generics.cdm_table import CdmTable
from omopy.connector._connection import IbisConnection, _get_catalog

__all__ = ["DbSource"]


class DbSource:
    """Database-backed CDM source.

    Implements the :class:`~omopy.generics.cdm_reference.CdmSource` protocol.
    Provides lazy table access via Ibis — no data is fetched until a table
    is materialised with ``CdmTable.collect()``.

    Parameters
    ----------
    con
        An Ibis backend connection (e.g. from ``ibis.duckdb.connect()``).
    cdm_schema
        The database schema containing CDM tables (e.g. ``"base"``).
    write_schema
        The schema for writing results (cohorts, etc.). Defaults to ``cdm_schema``.
    cdm_version
        The OMOP CDM version. If ``None``, auto-detected from ``cdm_source`` table.
    cdm_name
        Human-readable name for this CDM. If ``None``, read from ``cdm_source``.

    Examples
    --------
    >>> import ibis
    >>> con = ibis.duckdb.connect("synthea.duckdb", read_only=True)
    >>> source = DbSource(con, cdm_schema="base")
    >>> tables = source.list_tables()
    >>> "person" in tables
    True
    """

    __slots__ = (
        "_con",
        "_cdm_schema",
        "_write_schema",
        "_catalog",
        "_cdm_version",
        "_cdm_name",
        "_available_tables",
    )

    def __init__(
        self,
        con: IbisConnection,
        cdm_schema: str,
        *,
        write_schema: str | None = None,
        cdm_version: CdmVersion | None = None,
        cdm_name: str | None = None,
    ) -> None:
        self._con = con
        self._cdm_schema = cdm_schema
        self._write_schema = write_schema or cdm_schema
        self._catalog = _get_catalog(con)

        # Discover available tables
        self._available_tables = sorted(
            con.list_tables(database=(self._catalog, cdm_schema))
        )

        # Auto-detect CDM version if not provided
        if cdm_version is not None:
            self._cdm_version = cdm_version
        else:
            self._cdm_version = self._detect_version()

        # Auto-detect CDM name if not provided
        if cdm_name is not None:
            self._cdm_name = cdm_name
        else:
            self._cdm_name = self._detect_name()

    # -- CdmSource protocol implementation ----------------------------------

    @property
    def source_type(self) -> str:
        """Backend identifier derived from the Ibis connection."""
        return self._con.name

    def list_tables(self) -> list[str]:
        """Return names of all available tables in the CDM schema."""
        return list(self._available_tables)

    def read_table(self, table_name: str) -> CdmTable:
        """Return a lazy CdmTable wrapping an Ibis table expression.

        No data is fetched — the returned CdmTable holds a lazy Ibis
        reference that only executes on ``.collect()``.
        """
        if table_name not in self._available_tables:
            msg = (
                f"Table '{table_name}' not found in schema '{self._cdm_schema}'. "
                f"Available: {self._available_tables}"
            )
            raise KeyError(msg)
        ibis_table = self._con.table(
            table_name, database=(self._catalog, self._cdm_schema)
        )
        return CdmTable(
            data=ibis_table,
            tbl_name=table_name,
            tbl_source=self.source_type,
        )

    def write_table(self, table: CdmTable, table_name: str | None = None) -> None:
        """Write/materialise a table into the write schema.

        Supports writing Polars DataFrames, Ibis expressions, and
        PyArrow tables into the database.
        """
        name = table_name or table.tbl_name
        data = table.data

        if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            # Convert Polars to Arrow, then insert
            if isinstance(data, pl.LazyFrame):
                data = data.collect()
            arrow_table = data.to_arrow()
            self._con.raw_sql(
                f'CREATE SCHEMA IF NOT EXISTS "{self._write_schema}"'
            )
            # Use DuckDB's ability to insert from Arrow
            self._insert_arrow(name, arrow_table)
        elif hasattr(data, "to_pyarrow"):
            # Ibis table expression — materialise to Arrow and insert
            arrow_table = data.to_pyarrow()
            self._con.raw_sql(
                f'CREATE SCHEMA IF NOT EXISTS "{self._write_schema}"'
            )
            self._insert_arrow(name, arrow_table)
        else:
            msg = f"Cannot write data of type {type(data).__name__}"
            raise TypeError(msg)

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the write schema."""
        fqn = f'"{self._write_schema}"."{table_name}"'
        self._con.raw_sql(f"DROP TABLE IF EXISTS {fqn}")
        # Refresh cache
        if table_name in self._available_tables:
            self._available_tables.remove(table_name)

    # -- Extra properties ---------------------------------------------------

    @property
    def connection(self) -> IbisConnection:
        """The underlying Ibis backend connection."""
        return self._con

    @property
    def cdm_schema(self) -> str:
        """The database schema containing CDM tables."""
        return self._cdm_schema

    @property
    def write_schema(self) -> str:
        """The database schema for writing results."""
        return self._write_schema

    @property
    def catalog(self) -> str:
        """The database catalog (e.g. DuckDB database name)."""
        return self._catalog

    @property
    def cdm_version(self) -> CdmVersion:
        """The detected or specified CDM version."""
        return self._cdm_version

    @property
    def cdm_name(self) -> str:
        """The CDM source name."""
        return self._cdm_name

    # -- Internals ----------------------------------------------------------

    def _detect_version(self) -> CdmVersion:
        """Auto-detect CDM version from the cdm_source table."""
        if "cdm_source" not in self._available_tables:
            # Default if no cdm_source table
            return CdmVersion.V5_4

        try:
            tbl = self._con.table(
                "cdm_source", database=(self._catalog, self._cdm_schema)
            )
            result = tbl.select("cdm_version").limit(1).to_pyarrow()
            version_str = str(result.column("cdm_version")[0])
            return CdmVersion(version_str)
        except Exception:
            return CdmVersion.V5_4

    def _detect_name(self) -> str:
        """Auto-detect CDM name from the cdm_source table."""
        if "cdm_source" not in self._available_tables:
            return ""

        try:
            tbl = self._con.table(
                "cdm_source", database=(self._catalog, self._cdm_schema)
            )
            result = tbl.select("cdm_source_name").limit(1).to_pyarrow()
            return str(result.column("cdm_source_name")[0])
        except Exception:
            return ""

    def _insert_arrow(self, table_name: str, arrow_table: Any) -> None:
        """Insert an Arrow table into the write schema."""
        fqn = f'"{self._write_schema}"."{table_name}"'
        # Drop existing table first
        self._con.raw_sql(f"DROP TABLE IF EXISTS {fqn}")
        # Register Arrow table temporarily and CREATE AS SELECT
        temp_name = f"__omopy_temp_{table_name}"
        # Use the native DuckDB connection to register the Arrow table
        native_con = self._con.con
        native_con.register(temp_name, arrow_table)
        try:
            self._con.raw_sql(f'CREATE TABLE {fqn} AS SELECT * FROM "{temp_name}"')
        finally:
            native_con.unregister(temp_name)

    def __repr__(self) -> str:
        n = len(self._available_tables)
        return (
            f"DbSource(backend='{self.source_type}', "
            f"schema='{self._cdm_schema}', "
            f"version={self._cdm_version}, "
            f"tables={n})"
        )
