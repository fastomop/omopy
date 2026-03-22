# omopy.connector

Database CDM access layer for OMOPy — connect to OMOP CDM databases and get
lazy, Ibis-backed table access.

This module is the Python equivalent of the R `CDMConnector` package.

## Connection & Factory

### cdm_from_con

::: omopy.connector.cdm_from_con

### connect_duckdb

::: omopy.connector.connect_duckdb

### detect_cdm_schema

::: omopy.connector.detect_cdm_schema

## Core Classes

### DbSource

::: omopy.connector.DbSource
    options:
      show_bases: true

### CdmSnapshot

::: omopy.connector.CdmSnapshot
    options:
      show_bases: true

### IbisConnection

::: omopy.connector.IbisConnection

## Cohort Generation

### generate_concept_cohort_set

::: omopy.connector.generate_concept_cohort_set

### generate_cohort_set

::: omopy.connector.generate_cohort_set

## CDM Operations

### cdm_subset

::: omopy.connector.cdm_subset

### cdm_subset_cohort

::: omopy.connector.cdm_subset_cohort

### cdm_sample

::: omopy.connector.cdm_sample

### cdm_flatten

::: omopy.connector.cdm_flatten

### copy_cdm_to

::: omopy.connector.copy_cdm_to

### tbl_group

::: omopy.connector.tbl_group

### snapshot

::: omopy.connector.snapshot

## Compute & Persistence

### compute_permanent

::: omopy.connector.compute_permanent

### compute_query

::: omopy.connector.compute_query

### append_permanent

::: omopy.connector.append_permanent

## Date Helpers

### dateadd

::: omopy.connector.dateadd

### datediff

::: omopy.connector.datediff

### datepart

::: omopy.connector.datepart

### dateadd_polars

::: omopy.connector.dateadd_polars

### datediff_polars

::: omopy.connector.datediff_polars

## Analytics

### summarise_quantile

::: omopy.connector.summarise_quantile

### compute_data_hash

::: omopy.connector.compute_data_hash

### benchmark

::: omopy.connector.benchmark
