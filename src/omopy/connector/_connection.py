"""Low-level connection helpers for database backends.

Provides a thin wrapper around Ibis connection creation, with support
for DuckDB (primary), PostgreSQL, and other OMOP-common backends.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import ibis

__all__ = [
    "IbisConnection",
    "connect_duckdb",
    "detect_cdm_schema",
]

# Type alias for any Ibis backend connection
IbisConnection = ibis.BaseBackend


def connect_duckdb(
    database: str | Path,
    *,
    read_only: bool = False,
    **kwargs: Any,
) -> IbisConnection:
    """Connect to a DuckDB database file via Ibis.

    Parameters
    ----------
    database
        Path to the ``.duckdb`` file.
    read_only
        Open in read-only mode (default False).
    **kwargs
        Extra keyword arguments forwarded to ``ibis.duckdb.connect()``.

    Returns
    -------
    IbisConnection
        An Ibis DuckDB backend connection.
    """
    path = Path(database)
    if not path.exists():
        msg = f"DuckDB file not found: {path}"
        raise FileNotFoundError(msg)
    return ibis.duckdb.connect(str(path), read_only=read_only, **kwargs)


def detect_cdm_schema(
    con: IbisConnection,
    *,
    cdm_schema: str | None = None,
) -> str:
    """Detect the database schema containing CDM tables.

    Looks for a schema that contains the ``person`` table, which is
    mandatory in every valid OMOP CDM instance.

    Parameters
    ----------
    con
        An Ibis backend connection.
    cdm_schema
        If provided, validates this schema has CDM tables and returns it.

    Returns
    -------
    str
        The schema name (e.g. ``"base"``, ``"cdm"``, ``"main"``).

    Raises
    ------
    ValueError
        If no schema with CDM tables can be found.
    """
    if cdm_schema is not None:
        # Validate user-provided schema
        tables = _list_tables_in_schema(con, cdm_schema)
        if "person" not in tables:
            msg = (
                f"Schema '{cdm_schema}' does not contain a 'person' table. "
                f"Available tables: {sorted(tables)[:10]}"
            )
            raise ValueError(msg)
        return cdm_schema

    # Auto-detect: try common schema names
    for candidate in _candidate_schemas(con):
        tables = _list_tables_in_schema(con, candidate)
        if "person" in tables:
            return candidate

    msg = "Could not find a schema containing CDM tables (looked for 'person' table)"
    raise ValueError(msg)


def _list_tables_in_schema(con: IbisConnection, schema: str) -> set[str]:
    """List table names in the given schema."""
    try:
        # Ibis 12+: database param for DuckDB is (catalog, schema)
        # We need to detect the catalog name first
        catalog = _get_catalog(con)
        tables = con.list_tables(database=(catalog, schema))
        return set(tables)
    except Exception:
        # Fallback: try raw SQL
        try:
            result = con.raw_sql(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            )
            return {row[0] for row in result.fetchall()}
        except Exception:
            return set()


def _get_catalog(con: IbisConnection) -> str:
    """Get the catalog (database) name for the connection."""
    try:
        result = con.raw_sql("SELECT current_database()").fetchone()
        return result[0]
    except Exception:
        return "main"


def _candidate_schemas(con: IbisConnection) -> list[str]:
    """Return candidate schema names to search for CDM tables."""
    # Start with common CDM schema names
    common = ["cdm", "base", "public", "dbo", "main"]

    # Also query actual schemas in the database
    try:
        result = con.raw_sql(
            "SELECT DISTINCT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', 'pg_catalog')"
        )
        actual = [row[0] for row in result.fetchall()]
    except Exception:
        actual = []

    # Return common first, then any others not already in the list
    seen = set(common)
    for s in actual:
        if s not in seen:
            common.append(s)
            seen.add(s)
    return common
