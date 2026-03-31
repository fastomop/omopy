"""Copy a CDM between database connections.

Provides ``copy_cdm_to`` for transferring CDM tables from one connection
(or in-memory CDM) to another database, preserving cohort metadata.

This is the Python equivalent of R's ``copyCdmTo.R``.
"""

from __future__ import annotations

import logging

import polars as pl

from omopy.connector._connection import IbisConnection, _get_catalog
from omopy.connector.db_source import DbSource
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable

__all__ = ["copy_cdm_to"]

logger = logging.getLogger(__name__)


def copy_cdm_to(
    cdm: CdmReference,
    con: IbisConnection,
    *,
    schema: str,
    overwrite: bool = False,
) -> CdmReference:
    """Copy a CDM from one source to a new database connection.

    Collects each table from the source CDM (materialising lazy tables),
    uploads it to the target connection, and builds a new CdmReference
    pointing to the target database. Cohort tables have their settings,
    attrition, and codelist metadata preserved.

    Parameters
    ----------
    cdm
        The source CDM reference to copy.
    con
        Target Ibis backend connection to copy into.
    schema
        Schema in the target database where tables will be created.
        The schema is created if it does not exist.
    overwrite
        If ``True``, overwrite existing tables. If ``False`` (default),
        raise an error if a table already exists.

    Returns
    -------
    CdmReference
        A new CDM reference pointing to the tables in the target database.

    Raises
    ------
    ValueError
        If a table already exists in the target schema and ``overwrite``
        is ``False``.

    Examples
    --------
    >>> from omopy.connector import cdm_from_con, copy_cdm_to
    >>> import ibis
    >>> # Load source CDM
    >>> cdm = cdm_from_con("source.duckdb", cdm_schema="base")
    >>> # Copy to a new database
    >>> target_con = ibis.duckdb.connect("target.duckdb")
    >>> new_cdm = copy_cdm_to(cdm, target_con, schema="cdm")
    """
    catalog = _get_catalog(con)

    # Ensure target schema exists
    try:
        con.raw_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    except Exception:
        logger.warning("Could not create schema '%s' — it may already exist", schema)

    # Create a DbSource for the target
    target_source = DbSource(
        con,
        cdm_schema=schema,
        write_schema=schema,
        cdm_version=cdm.cdm_version,
        cdm_name=cdm.cdm_name,
    )

    # Determine table copy order: person and observation_period first
    all_tables = list(cdm.table_names)
    priority = ["person", "observation_period"]
    ordered = [t for t in priority if t in all_tables]
    ordered += [t for t in all_tables if t not in priority]

    total = len(ordered)
    new_tables: dict[str, CdmTable] = {}

    for idx, table_name in enumerate(ordered, 1):
        tbl = cdm[table_name]

        # Materialise the table to Polars
        df = tbl.collect()
        nrows = len(df)
        logger.info(
            "Uploading table %s (%d rows) [%d/%d]",
            table_name,
            nrows,
            idx,
            total,
        )

        # Upload to target
        _upload_dataframe(
            con=con,
            df=df,
            table_name=table_name,
            schema=schema,
            catalog=catalog,
            overwrite=overwrite,
        )

        # Create lazy reference back
        ibis_table = con.table(table_name, database=(catalog, schema))
        new_tbl = CdmTable(
            data=ibis_table,
            tbl_name=table_name,
            tbl_source=con.name,
        )

        # Handle cohort tables: preserve metadata
        if isinstance(tbl, CohortTable):
            new_tbl = CohortTable(
                data=ibis_table,
                tbl_name=table_name,
                tbl_source=con.name,
                settings=tbl.settings,
                attrition=tbl.attrition,
                cohort_codelist=tbl.cohort_codelist,
            )

        new_tables[table_name] = new_tbl

    # Build new CdmReference
    new_cdm = CdmReference(
        tables=new_tables,
        cdm_version=cdm.cdm_version,
        cdm_name=cdm.cdm_name,
        cdm_source=target_source,
    )

    return new_cdm


def _upload_dataframe(
    con: IbisConnection,
    df: pl.DataFrame,
    table_name: str,
    schema: str,
    catalog: str,
    overwrite: bool,
) -> None:
    """Upload a Polars DataFrame to a database table.

    Uses PyArrow as the zero-copy interchange format.
    """
    fqn = f'"{schema}"."{table_name}"'

    # Check if table already exists
    try:
        existing = con.list_tables(database=(catalog, schema))
        if table_name in existing:
            if not overwrite:
                msg = (
                    f"Table {fqn} already exists in target."
                    " Set overwrite=True to replace."
                )
                raise ValueError(msg)
            con.raw_sql(f"DROP TABLE IF EXISTS {fqn}")
    except ValueError:
        raise
    except Exception:
        pass  # Schema might not exist yet, that's fine

    # Convert Polars to Arrow
    arrow_table = df.to_arrow()

    # Register Arrow table as temp view and CREATE AS SELECT
    temp_name = f"__omopy_copy_{table_name}"
    native_con = con.con
    native_con.register(temp_name, arrow_table)
    try:
        con.raw_sql(f'CREATE TABLE {fqn} AS SELECT * FROM "{temp_name}"')
    finally:
        native_con.unregister(temp_name)
