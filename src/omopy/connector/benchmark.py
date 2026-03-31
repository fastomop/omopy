"""CDM benchmark — timed diagnostic queries.

Provides ``benchmark()`` to run a set of standard queries against a CDM
and measure execution time.  Equivalent to R's
``benchmarkCDMConnector()``.
"""

from __future__ import annotations

import time

import polars as pl

from omopy.connector.db_source import DbSource
from omopy.generics.cdm_reference import CdmReference

__all__ = ["benchmark"]


def benchmark(cdm: CdmReference) -> pl.DataFrame:
    """Run benchmark queries against a CDM and return timings.

    Executes a standard set of queries (distinct count, group-by, joins,
    collect) against vocab and clinical tables, timing each.

    Parameters
    ----------
    cdm
        A CdmReference (database-backed recommended for meaningful timings).

    Returns
    -------
    pl.DataFrame
        A DataFrame with columns: ``task``, ``time_taken_secs``,
        ``time_taken_mins``, ``dbms``, ``person_n``.
    """
    timings: list[dict] = []

    # Determine dbms
    source = cdm.cdm_source
    dbms = source.source_type if isinstance(source, DbSource) else "local"

    # Person count (needed for output, also a warm-up)
    person_n = _execute_count(cdm, "person")

    # ---- Task 1: distinct count of concept_relationship ----
    if "concept_relationship" in cdm:
        task = "distinct count of concept relationship table"
        start = time.monotonic()
        _distinct_count(cdm, "concept_relationship")
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # ---- Task 2: count by relationship_id in concept_relationship ----
    if "concept_relationship" in cdm:
        task = "count of different relationship IDs in concept relationship table"
        start = time.monotonic()
        _group_count(cdm, "concept_relationship", "relationship_id")
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # ---- Task 3: join concept + concept_class, compute to temp ----
    if "concept" in cdm and "concept_class" in cdm:
        task = "join of concept and concept class computed to a temp table"
        start = time.monotonic()
        _join_and_compute(cdm, "concept", "concept_class", "concept_class_concept_id")
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # ---- Task 4: collect concept table ----
    if "concept" in cdm:
        task = "concept table collected into memory"
        start = time.monotonic()
        cdm["concept"].collect()
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # ---- Task 5: join person + observation_period, collect ----
    if "person" in cdm and "observation_period" in cdm:
        task = "join of person and observation period collected into memory"
        start = time.monotonic()
        _join_and_collect(cdm, "person", "observation_period", "person_id")
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # ---- Task 6: obs period date summary by gender ----
    if "person" in cdm and "observation_period" in cdm:
        task = "summary of observation period start and end dates by gender concept id"
        start = time.monotonic()
        _obs_period_summary_by_gender(cdm)
        elapsed = time.monotonic() - start
        timings.append({"task": task, "time_taken_secs": round(elapsed, 4)})

    # Build result
    result = pl.DataFrame(timings)
    result = result.with_columns(
        (pl.col("time_taken_secs") / 60.0).round(4).alias("time_taken_mins"),
        pl.lit(dbms).alias("dbms"),
        pl.lit(person_n).alias("person_n"),
    )

    return result


# ---------------------------------------------------------------------------
# Internal query helpers
# ---------------------------------------------------------------------------


def _execute_count(cdm: CdmReference, table_name: str) -> int:
    """Quick row count."""
    tbl = cdm[table_name]
    data = tbl.data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        return len(data)

    result = data.count().execute()
    if hasattr(result, "item"):
        return int(result.item())
    return int(result)


def _distinct_count(cdm: CdmReference, table_name: str) -> int:
    """Distinct row count (forces computation)."""
    data = cdm[table_name].data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        return data.unique().height

    result = data.distinct().count().execute()
    if hasattr(result, "item"):
        return int(result.item())
    return int(result)


def _group_count(cdm: CdmReference, table_name: str, col: str) -> object:
    """Group-by count, collected."""
    data = cdm[table_name].data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        return data.group_by(col).len()

    return data.group_by(col).count().execute()


def _join_and_compute(
    cdm: CdmReference,
    left: str,
    right: str,
    right_key: str,
) -> object:
    """Left join two tables, execute the query."""
    l_data = cdm[left].data
    r_data = cdm[right].data

    if isinstance(l_data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(l_data, pl.LazyFrame):
            l_data = l_data.collect()
        if isinstance(r_data, pl.LazyFrame):
            r_data = r_data.collect()
        return l_data.join(
            r_data,
            left_on="concept_id",
            right_on=right_key,
            how="left",
            suffix="_y",
        )

    joined = l_data.left_join(r_data, l_data["concept_id"] == r_data[right_key])
    return joined.execute()


def _join_and_collect(
    cdm: CdmReference,
    left: str,
    right: str,
    key: str,
) -> object:
    """Inner join two tables and collect."""
    l_data = cdm[left].data
    r_data = cdm[right].data

    if isinstance(l_data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(l_data, pl.LazyFrame):
            l_data = l_data.collect()
        if isinstance(r_data, pl.LazyFrame):
            r_data = r_data.collect()
        return l_data.join(r_data, on=key, how="inner")

    return l_data.inner_join(r_data, key).execute()


def _obs_period_summary_by_gender(cdm: CdmReference) -> object:
    """Join person + obs_period, group by gender, aggregate dates."""
    p_data = cdm["person"].data
    o_data = cdm["observation_period"].data

    if isinstance(p_data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(p_data, pl.LazyFrame):
            p_data = p_data.collect()
        if isinstance(o_data, pl.LazyFrame):
            o_data = o_data.collect()
        joined = p_data.join(o_data, on="person_id", how="inner")
        return joined.group_by("gender_concept_id").agg(
            pl.col("observation_period_start_date").min().alias("min_start"),
            pl.col("observation_period_end_date").max().alias("max_end"),
        )

    joined = p_data.inner_join(o_data, "person_id")
    result = joined.group_by("gender_concept_id").agg(
        min_start=joined["observation_period_start_date"].min(),
        max_end=joined["observation_period_end_date"].max(),
    )
    return result.execute()
