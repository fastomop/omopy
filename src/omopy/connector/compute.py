"""Materialise Ibis queries to persistent database tables.

Provides ``compute_permanent`` and ``append_permanent`` for creating or
appending to permanent tables in the database, plus ``compute_query`` as
a general-purpose materialisation helper.

This is the Python equivalent of R's ``compute.R``.
"""

from __future__ import annotations

import uuid
from typing import Any

import ibis
import ibis.expr.types as ir

from omopy.connector._connection import IbisConnection, _get_catalog
from omopy.generics.cdm_table import CdmTable

__all__ = ["compute_permanent", "append_permanent", "compute_query"]


# ---------------------------------------------------------------------------
# Helper: generate a unique table name
# ---------------------------------------------------------------------------


def _unique_table_name(prefix: str = "omopy_tmp") -> str:
    """Generate a unique table name with the given prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Helper: fully-qualified table name
# ---------------------------------------------------------------------------


def _fully_qualified_name(
    con: IbisConnection,
    table_name: str,
    schema: str | None = None,
    catalog: str | None = None,
) -> str:
    """Build a fully qualified, quoted table name."""
    parts: list[str] = []
    if catalog:
        parts.append(f'"{catalog}"')
    if schema:
        parts.append(f'"{schema}"')
    parts.append(f'"{table_name}"')
    return ".".join(parts)


def _table_exists(
    con: IbisConnection,
    table_name: str,
    schema: str | None = None,
) -> bool:
    """Check if a table exists in the given schema."""
    try:
        catalog = _get_catalog(con)
        if schema:
            tables = con.list_tables(database=(catalog, schema))
        else:
            tables = con.list_tables()
        return table_name in tables
    except Exception:
        return False


# ---------------------------------------------------------------------------
# compute_permanent
# ---------------------------------------------------------------------------


def compute_permanent(
    expr: ir.Table | CdmTable,
    *,
    name: str,
    con: IbisConnection | None = None,
    schema: str | None = None,
    overwrite: bool = True,
) -> ir.Table:
    """Materialise an Ibis expression to a permanent database table.

    Executes the query and stores the result as a persistent table.
    Returns a lazy Ibis reference to the newly created table.

    Parameters
    ----------
    expr
        An Ibis Table expression or CdmTable to materialise.
    name
        Name for the new table.
    con
        Ibis backend connection. If ``None``, inferred from *expr*.
    schema
        Database schema for the new table. If ``None``, uses default.
    overwrite
        If ``True`` (default), drop an existing table with the same name
        before creating. If ``False``, raise an error if the table exists.

    Returns
    -------
    ir.Table
        A lazy Ibis reference to the newly created permanent table.

    Raises
    ------
    ValueError
        If the table exists and ``overwrite`` is ``False``.
    TypeError
        If *expr* is not an Ibis expression or CdmTable.

    Examples
    --------
    >>> import ibis
    >>> con = ibis.duckdb.connect("my.duckdb")
    >>> concept = con.table("concept", database=("mydb", "base"))
    >>> drug_count = concept.filter(concept.domain_id == "Drug").count()
    >>> result = compute_permanent(
    ...     concept.filter(concept.domain_id == "Drug"),
    ...     name="drug_concepts",
    ...     schema="results",
    ... )
    """
    # Unwrap CdmTable
    if isinstance(expr, CdmTable):
        ibis_expr = expr.data
        if not isinstance(ibis_expr, ir.Table):
            msg = (
                "compute_permanent requires an Ibis-backed CdmTable, "
                f"got {type(ibis_expr).__name__}"
            )
            raise TypeError(msg)
    elif isinstance(expr, ir.Table):
        ibis_expr = expr
    else:
        msg = f"Expected Ibis Table or CdmTable, got {type(expr).__name__}"
        raise TypeError(msg)

    # Resolve connection
    if con is None:
        # Try to get the backend from the expression
        try:
            con = ibis_expr._find_backend()
        except Exception:
            msg = "Cannot determine connection from expression; pass con= explicitly"
            raise ValueError(msg) from None

    catalog = _get_catalog(con)

    # Check if table exists
    if _table_exists(con, name, schema):
        if not overwrite:
            fqn = _fully_qualified_name(con, name, schema, catalog)
            msg = f"Table {fqn} already exists. Set overwrite=True to replace it."
            raise ValueError(msg)
        # Drop existing
        fqn = _fully_qualified_name(con, name, schema)
        con.raw_sql(f"DROP TABLE IF EXISTS {fqn}")

    # Build CREATE TABLE ... AS SELECT ...
    # Render the Ibis expression to SQL
    sql_query = ibis.to_sql(ibis_expr)

    if schema:
        # Ensure schema exists
        con.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        fqn = _fully_qualified_name(con, name, schema)
    else:
        fqn = f'"{name}"'

    create_sql = f"CREATE TABLE {fqn} AS {sql_query}"
    con.raw_sql(create_sql)

    # Return a lazy reference to the new table
    if schema:
        return con.table(name, database=(catalog, schema))
    else:
        return con.table(name)


# ---------------------------------------------------------------------------
# append_permanent
# ---------------------------------------------------------------------------


def append_permanent(
    expr: ir.Table | CdmTable,
    *,
    name: str,
    con: IbisConnection | None = None,
    schema: str | None = None,
) -> ir.Table:
    """Append the result of an Ibis expression to an existing table.

    If the target table does not exist, it is created (equivalent to
    ``compute_permanent`` with ``overwrite=False``).

    Parameters
    ----------
    expr
        An Ibis Table expression or CdmTable to append.
    name
        Name of the target table.
    con
        Ibis backend connection. If ``None``, inferred from *expr*.
    schema
        Database schema for the target table.

    Returns
    -------
    ir.Table
        A lazy Ibis reference to the (now-appended) table.

    Examples
    --------
    >>> # First batch
    >>> compute_permanent(batch1, name="results", schema="work")
    >>> # Subsequent batches
    >>> append_permanent(batch2, name="results", schema="work")
    """
    # Unwrap CdmTable
    if isinstance(expr, CdmTable):
        ibis_expr = expr.data
        if not isinstance(ibis_expr, ir.Table):
            msg = (
                "append_permanent requires an Ibis-backed CdmTable, "
                f"got {type(ibis_expr).__name__}"
            )
            raise TypeError(msg)
    elif isinstance(expr, ir.Table):
        ibis_expr = expr
    else:
        msg = f"Expected Ibis Table or CdmTable, got {type(expr).__name__}"
        raise TypeError(msg)

    # Resolve connection
    if con is None:
        try:
            con = ibis_expr._find_backend()
        except Exception:
            msg = "Cannot determine connection from expression; pass con= explicitly"
            raise ValueError(msg) from None

    catalog = _get_catalog(con)

    # If table doesn't exist, create it
    if not _table_exists(con, name, schema):
        return compute_permanent(ibis_expr, name=name, con=con, schema=schema, overwrite=False)

    # Table exists — INSERT INTO ... SELECT ...
    sql_query = ibis.to_sql(ibis_expr)

    if schema:
        fqn = _fully_qualified_name(con, name, schema)
    else:
        fqn = f'"{name}"'

    insert_sql = f"INSERT INTO {fqn} {sql_query}"
    con.raw_sql(insert_sql)

    # Return lazy reference
    if schema:
        return con.table(name, database=(catalog, schema))
    else:
        return con.table(name)


# ---------------------------------------------------------------------------
# compute_query — general-purpose materialisation
# ---------------------------------------------------------------------------


def compute_query(
    expr: ir.Table | CdmTable,
    *,
    name: str | None = None,
    con: IbisConnection | None = None,
    temporary: bool = True,
    schema: str | None = None,
    overwrite: bool = True,
) -> ir.Table:
    """Execute an Ibis query and store the result in the database.

    This is a general-purpose materialisation function. If ``temporary``
    is ``True`` (default), creates a temporary table. Otherwise creates
    a permanent table in the specified schema.

    Parameters
    ----------
    expr
        An Ibis Table expression or CdmTable.
    name
        Table name. If ``None``, a unique name is generated.
    con
        Ibis backend connection. If ``None``, inferred from *expr*.
    temporary
        If ``True`` (default), create a temporary table.
    schema
        Schema for permanent tables (ignored when ``temporary=True``).
    overwrite
        Whether to overwrite existing tables.

    Returns
    -------
    ir.Table
        A lazy Ibis reference to the newly created table.
    """
    if name is None:
        name = _unique_table_name()

    # Unwrap CdmTable
    if isinstance(expr, CdmTable):
        ibis_expr = expr.data
        if not isinstance(ibis_expr, ir.Table):
            msg = f"compute_query requires an Ibis-backed CdmTable, got {type(ibis_expr).__name__}"
            raise TypeError(msg)
    elif isinstance(expr, ir.Table):
        ibis_expr = expr
    else:
        msg = f"Expected Ibis Table or CdmTable, got {type(expr).__name__}"
        raise TypeError(msg)

    # Resolve connection
    if con is None:
        try:
            con = ibis_expr._find_backend()
        except Exception:
            msg = "Cannot determine connection from expression; pass con= explicitly"
            raise ValueError(msg) from None

    if temporary:
        # Create temporary table using CREATE TEMP TABLE ... AS SELECT
        sql_query = ibis.to_sql(ibis_expr)

        # Handle overwrite for temp tables
        if overwrite:
            con.raw_sql(f'DROP TABLE IF EXISTS "{name}"')

        create_sql = f'CREATE TEMPORARY TABLE "{name}" AS {sql_query}'
        con.raw_sql(create_sql)

        # Return lazy reference to the temp table
        return con.table(name)
    else:
        # Permanent table
        return compute_permanent(
            ibis_expr,
            name=name,
            con=con,
            schema=schema,
            overwrite=overwrite,
        )
