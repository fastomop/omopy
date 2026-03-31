"""Factory function to create a CdmReference from a database connection.

``cdm_from_con()`` is the primary entry point for users. It takes an Ibis
backend connection (or DuckDB file path), auto-detects the CDM schema and
version, and returns a fully-populated CdmReference with lazy table access.
"""

from __future__ import annotations

from pathlib import Path

from omopy.connector._connection import (
    IbisConnection,
    connect_duckdb,
    detect_cdm_schema,
)
from omopy.connector.db_source import DbSource
from omopy.generics._schema import CdmSchema
from omopy.generics._types import CdmVersion
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable

__all__ = ["cdm_from_con"]


def cdm_from_con(
    con: IbisConnection | str | Path,
    *,
    cdm_schema: str | None = None,
    write_schema: str | None = None,
    cdm_version: CdmVersion | None = None,
    cdm_name: str | None = None,
    cdm_tables: list[str] | None = None,
) -> CdmReference:
    """Create a CdmReference from a database connection.

    This is the main entry point for connecting to an OMOP CDM database.
    It auto-detects the schema, CDM version, and available tables, then
    returns a CdmReference with lazy (Ibis-backed) table access.

    Parameters
    ----------
    con
        An Ibis backend connection, or a path to a DuckDB file.
        If a string/Path is given, it is opened as a DuckDB database.
    cdm_schema
        The database schema containing CDM tables. Auto-detected if None.
    write_schema
        The schema for writing results (cohorts, etc.). Defaults to ``cdm_schema``.
    cdm_version
        The OMOP CDM version. Auto-detected from ``cdm_source`` table if None.
    cdm_name
        Human-readable name. Auto-detected from ``cdm_source`` table if None.
    cdm_tables
        Specific table names to load. If None, loads all standard CDM tables
        found in the schema.

    Returns
    -------
    CdmReference
        A CDM reference with lazy database-backed tables.

    Examples
    --------
    >>> cdm = cdm_from_con("synthea.duckdb", cdm_schema="base")
    >>> cdm.cdm_name
    'dbt-synthea'
    >>> cdm["person"].count()
    27
    >>> # Tables are lazy — no data fetched until .collect()
    >>> person_df = cdm["person"].collect()
    """
    # If path given, open as DuckDB
    if isinstance(con, (str, Path)):
        con = connect_duckdb(con, read_only=True)

    # Auto-detect CDM schema
    schema = detect_cdm_schema(con, cdm_schema=cdm_schema)

    # Create DbSource
    source = DbSource(
        con,
        cdm_schema=schema,
        write_schema=write_schema,
        cdm_version=cdm_version,
        cdm_name=cdm_name,
    )

    # Determine which tables to load
    available = set(source.list_tables())
    cdm_schema_obj = CdmSchema(source.cdm_version)
    standard_tables = set(cdm_schema_obj.table_names())

    if cdm_tables is not None:
        # User specified exact tables
        to_load = [t for t in cdm_tables if t in available]
    else:
        # Load all standard CDM tables that exist in the database
        to_load = sorted(available & standard_tables)

    # Build lazy CdmTable instances
    tables: dict[str, CdmTable] = {}
    for name in to_load:
        tables[name] = source.read_table(name)

    # Assemble CdmReference
    cdm = CdmReference(
        tables=tables,
        cdm_version=source.cdm_version,
        cdm_name=source.cdm_name,
        cdm_source=source,
    )

    return cdm
