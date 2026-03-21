"""``omopy.connector`` — Database CDM access for OMOPy.

This subpackage provides the database connection layer, allowing users
to connect to OMOP CDM databases and get lazy, Ibis-backed table access.

Primary entry point::

    from omopy.connector import cdm_from_con

    cdm = cdm_from_con("synthea.duckdb", cdm_schema="base")
    person = cdm["person"]        # lazy — no data fetched
    df = person.collect()          # materialises to Polars DataFrame
"""

from omopy.connector._connection import (
    IbisConnection,
    connect_duckdb,
    detect_cdm_schema,
)
from omopy.connector.benchmark import benchmark
from omopy.connector.cdm_flatten import cdm_flatten
from omopy.connector.cdm_from_con import cdm_from_con
from omopy.connector.cdm_subset import cdm_sample, cdm_subset, cdm_subset_cohort
from omopy.connector.circe import generate_cohort_set
from omopy.connector.cohort_generation import generate_concept_cohort_set
from omopy.connector.compute import append_permanent, compute_permanent, compute_query
from omopy.connector.copy_cdm import copy_cdm_to
from omopy.connector.data_hash import compute_data_hash
from omopy.connector.date_helpers import (
    dateadd,
    dateadd_polars,
    datediff,
    datediff_polars,
    datepart,
)
from omopy.connector.db_source import DbSource
from omopy.connector.snapshot import CdmSnapshot, snapshot
from omopy.connector.summarise_quantile import summarise_quantile
from omopy.connector.tbl_group import tbl_group

__all__ = [
    "CdmSnapshot",
    "DbSource",
    "IbisConnection",
    "append_permanent",
    "benchmark",
    "cdm_flatten",
    "cdm_from_con",
    "cdm_sample",
    "cdm_subset",
    "cdm_subset_cohort",
    "compute_data_hash",
    "compute_permanent",
    "compute_query",
    "connect_duckdb",
    "copy_cdm_to",
    "dateadd",
    "dateadd_polars",
    "datediff",
    "datediff_polars",
    "datepart",
    "detect_cdm_schema",
    "generate_cohort_set",
    "generate_concept_cohort_set",
    "snapshot",
    "summarise_quantile",
    "tbl_group",
]
