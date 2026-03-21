"""CDM subsetting and sampling utilities.

Provides functions to create subsets of a CDM by cohort membership
or random sampling, equivalent to R's ``cdmSubsetCohort()`` and
``cdmSample()``.
"""

from __future__ import annotations

from typing import Any

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.connector.db_source import DbSource

__all__ = ["cdm_subset", "cdm_subset_cohort", "cdm_sample"]


def cdm_subset(
    cdm: CdmReference,
    person_ids: list[int],
) -> CdmReference:
    """Subset a CDM to a specific set of person IDs.

    Returns a new CdmReference where all clinical tables are filtered
    to only include the specified persons.  Tables remain lazy
    (Ibis-backed) — the filtering is applied as SQL predicates, not
    materialised.

    Parameters
    ----------
    cdm
        A CdmReference with database-backed or local tables.
    person_ids
        Explicit list of person_id values to include.

    Returns
    -------
    CdmReference
        A new CDM with all person-linked tables filtered.

    Raises
    ------
    ValueError
        If *person_ids* is empty.
    """
    if not person_ids:
        msg = "person_ids must not be empty"
        raise ValueError(msg)

    source = cdm.cdm_source
    if isinstance(source, DbSource):
        return _subset_cdm_by_person_list(cdm, list(person_ids))
    else:
        pid_df = pl.DataFrame({"person_id": person_ids})
        return _subset_cdm_by_persons_polars(cdm, pid_df)


def cdm_subset_cohort(
    cdm: CdmReference,
    cohort_table: str = "cohort",
    cohort_id: list[int] | None = None,
) -> CdmReference:
    """Subset a CDM to individuals in one or more cohorts.

    Returns a new CdmReference where all clinical tables are filtered
    to only include persons who appear in the specified cohort(s).
    Tables remain lazy (Ibis-backed) — the filtering is applied as
    SQL predicates, not materialised.

    Parameters
    ----------
    cdm
        A CdmReference with database-backed tables.
    cohort_table
        Name of a cohort table in the CDM.
    cohort_id
        Specific cohort definition IDs to include. If None, all cohorts
        in the table are used.

    Returns
    -------
    CdmReference
        A new CDM with all person-linked tables filtered to the cohort subjects.
    """
    if cohort_table not in cdm:
        msg = f"Cohort table '{cohort_table}' not found in CDM"
        raise KeyError(msg)

    cohort = cdm[cohort_table]

    # Materialise cohort to Polars to extract subject_ids (works for both
    # Polars and Ibis-backed cohort data)
    cohort_df = cohort.collect()

    if cohort_id is not None:
        cohort_df = cohort_df.filter(
            pl.col("cohort_definition_id").is_in(cohort_id)
        )

    # Get distinct person IDs as a Polars Series
    subject_ids = cohort_df.select("subject_id").unique().rename({"subject_id": "person_id"})

    # Upload as Ibis temp table if we have a DB-backed CDM, otherwise stay Polars
    source = cdm.cdm_source
    if isinstance(source, DbSource):
        # Get person IDs as a Python list for use in .isin() filters
        pid_list = subject_ids["person_id"].to_list()
        return _subset_cdm_by_person_list(cdm, pid_list)
    else:
        return _subset_cdm_by_persons_polars(cdm, subject_ids)


def cdm_sample(
    cdm: CdmReference,
    n: int,
    *,
    seed: int | None = None,
) -> CdmReference:
    """Subset a CDM to a random sample of persons.

    Only persons present in both the ``person`` table and
    ``observation_period`` table are eligible for sampling.

    Parameters
    ----------
    cdm
        A CdmReference with database-backed tables.
    n
        Number of persons to include.
    seed
        Random seed for reproducibility.

    Returns
    -------
    CdmReference
        A new CDM with all person-linked tables filtered to the sampled persons.
    """
    if "person" not in cdm:
        msg = "CDM must have a 'person' table for sampling"
        raise KeyError(msg)

    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "cdm_sample requires a database-backed CDM (DbSource)"
        raise TypeError(msg)

    person_data = cdm["person"].data

    # Get persons in both person and observation_period
    if "observation_period" in cdm:
        obs_data = cdm["observation_period"].data
        eligible = person_data.select("person_id").join(
            obs_data.select("person_id").distinct(),
            "person_id",
        )
    else:
        eligible = person_data.select("person_id")

    eligible = eligible.distinct()

    # Random sample — use order_by(random()) for database-side sampling
    if seed is not None:
        try:
            source.connection.raw_sql(f"SELECT setseed({seed / 2**31})")
        except Exception:
            pass

    sampled = (
        eligible
        .order_by(ibis.random())
        .limit(n)
    )

    return _subset_cdm_by_persons_ibis(cdm, sampled)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _polars_to_ibis_temp(
    df: pl.DataFrame,
    source: DbSource,
    temp_name: str,
) -> ir.Table:
    """Upload a Polars DataFrame as a temp table and return an Ibis reference."""
    arrow = df.to_arrow()
    native = source.connection.con
    native.register(temp_name, arrow)
    return source.connection.table(temp_name)


def _drop_ibis_temp(source: DbSource, temp_name: str) -> None:
    """Clean up a temporary table."""
    try:
        source.connection.con.unregister(temp_name)
    except Exception:
        pass


def _subset_cdm_by_person_list(
    cdm: CdmReference,
    person_ids: list[int],
) -> CdmReference:
    """Create a new CDM where all tables are filtered by a list of person IDs.

    Uses ``.isin()`` filters so everything stays lazy (no temp tables needed).
    Tables with ``person_id`` are filtered directly.
    Tables with ``subject_id`` (cohorts) are filtered on subject_id.
    Tables without either are passed through unchanged.
    """
    new_tables: dict[str, CdmTable] = {}

    for name in cdm:
        tbl = cdm[name]
        data = tbl.data
        cols = tbl.columns

        if "person_id" in cols:
            if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
                filtered = data.filter(pl.col("person_id").is_in(person_ids))
            else:
                # Ibis table
                filtered = data.filter(data.person_id.isin(person_ids))
            new_tables[name] = tbl._with_data(filtered)
        elif "subject_id" in cols:
            if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
                filtered = data.filter(pl.col("subject_id").is_in(person_ids))
            else:
                filtered = data.filter(data.subject_id.isin(person_ids))
            new_tables[name] = tbl._with_data(filtered)
        else:
            new_tables[name] = tbl

    return CdmReference(
        tables=new_tables,
        cdm_version=cdm.cdm_version,
        cdm_name=cdm.cdm_name,
        cdm_source=cdm.cdm_source,
    )


def _subset_cdm_by_persons_ibis(
    cdm: CdmReference,
    person_ids: ir.Table,
) -> CdmReference:
    """Create a new CDM where all tables are filtered by person_id via Ibis join.

    Tables with a ``person_id`` column are filtered by inner join.
    Tables with a ``subject_id`` column (cohorts) are joined on subject_id.
    Tables without either column are passed through unchanged.
    """
    new_tables: dict[str, CdmTable] = {}

    for name in cdm:
        tbl = cdm[name]
        data = tbl.data
        cols = tbl.columns

        if "person_id" in cols:
            if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
                # Polars data — materialise person_ids and join in Polars
                pid_df = pl.from_arrow(person_ids.to_pyarrow())
                if isinstance(data, pl.LazyFrame):
                    filtered = data.join(pid_df.lazy(), on="person_id", how="inner").collect()
                else:
                    filtered = data.join(pid_df, on="person_id", how="inner")
            else:
                # Ibis data — join in Ibis
                filtered = data.join(person_ids, "person_id")
            new_tables[name] = tbl._with_data(filtered)
        elif "subject_id" in cols:
            # Cohort tables use subject_id
            if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
                pid_df = pl.from_arrow(person_ids.to_pyarrow()).rename({"person_id": "subject_id"})
                if isinstance(data, pl.LazyFrame):
                    filtered = data.join(pid_df.lazy(), on="subject_id", how="inner").collect()
                else:
                    filtered = data.join(pid_df, on="subject_id", how="inner")
            else:
                person_as_subject = person_ids.rename(subject_id="person_id")
                filtered = data.join(person_as_subject, "subject_id")
            new_tables[name] = tbl._with_data(filtered)
        else:
            # Non-person tables (vocabulary, etc.) — pass through
            new_tables[name] = tbl

    return CdmReference(
        tables=new_tables,
        cdm_version=cdm.cdm_version,
        cdm_name=cdm.cdm_name,
        cdm_source=cdm.cdm_source,
    )


def _subset_cdm_by_persons_polars(
    cdm: CdmReference,
    person_ids: pl.DataFrame,
) -> CdmReference:
    """Create a new CDM where all tables are filtered by person_id via Polars join."""
    new_tables: dict[str, CdmTable] = {}

    for name in cdm:
        tbl = cdm[name]
        cols = tbl.columns

        if "person_id" in cols:
            df = tbl.collect()
            filtered = df.join(person_ids, on="person_id", how="inner")
            new_tables[name] = tbl._with_data(filtered)
        elif "subject_id" in cols:
            df = tbl.collect()
            pid_as_subj = person_ids.rename({"person_id": "subject_id"})
            filtered = df.join(pid_as_subj, on="subject_id", how="inner")
            new_tables[name] = tbl._with_data(filtered)
        else:
            new_tables[name] = tbl

    return CdmReference(
        tables=new_tables,
        cdm_version=cdm.cdm_version,
        cdm_name=cdm.cdm_name,
        cdm_source=cdm.cdm_source,
    )
