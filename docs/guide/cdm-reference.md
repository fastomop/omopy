# Working with CDM References

A `CdmReference` is the central object in OMOPy. It represents a connection to
an OMOP CDM database and provides dict-like access to all CDM tables.

## Creating a CDM Reference

```python
from omopy.connector import cdm_from_con

# From a DuckDB file path
cdm = cdm_from_con("data/omop.duckdb", cdm_schema="cdm")

# From an Ibis connection with explicit options
import ibis
con = ibis.postgres.connect(host="localhost", database="omop")
cdm = cdm_from_con(
    con,
    cdm_schema="cdm",
    write_schema="results",
    cdm_version="5.4",
    cdm_name="my_cdm",
)
```

### Parameters

- **`cdm_schema`** — The database schema containing CDM tables (required)
- **`write_schema`** — Schema for writing cohort tables (defaults to `cdm_schema`)
- **`cdm_version`** — `"5.3"` or `"5.4"` (auto-detected if omitted)
- **`cdm_name`** — A human-readable name for this CDM instance
- **`cdm_tables`** — Restrict to specific tables (auto-discovered if omitted)

## Accessing Tables

```python
# Dict-style access
person = cdm["person"]
conditions = cdm["condition_occurrence"]

# List available tables
print(cdm.table_names)

# Check if a table exists
if "drug_exposure" in cdm:
    drugs = cdm["drug_exposure"]
```

Each table is a `CdmTable` — a thin wrapper around an Ibis expression that
carries metadata (table name, source, CDM back-reference).

## CdmTable Operations

`CdmTable` supports Ibis-style operations while preserving metadata:

```python
# Filter (use .data to access the underlying Ibis expression)
young = person.filter(person.data.year_of_birth > 1990)

# Select columns
subset = person.select("person_id", "gender_concept_id")

# Join tables
joined = conditions.join(person, "person_id")

# Count rows (executes a query)
n = person.count()

# Collect to Polars DataFrame
df = person.head(100).collect()
```

## CDM Metadata

```python
print(cdm.cdm_version)    # "5.4"
print(cdm.cdm_name)       # "my_cdm"
print(cdm.cdm_source)     # CdmSource | None
```

## Subsetting

```python
from omopy.connector import cdm_subset, cdm_sample, cdm_subset_cohort

# By person IDs
small = cdm_subset(cdm, person_ids=[1, 2, 3, 4, 5])

# Random sample
sample = cdm_sample(cdm, n=100)

# By cohort membership (pass the cohort table name as a string)
subset = cdm_subset_cohort(cdm, "my_cohort", cohort_id=[1])
```

All subsetting operations return a **new** `CdmReference` with filters applied
lazily. No data is copied until you collect.

## Snapshots

```python
from omopy.connector import snapshot

snap = snapshot(cdm)
print(snap.person_count)
print(snap.cdm_source_name)
print(snap.vocabulary_version)
print(snap.earliest_observation_period_start_date)
print(snap.latest_observation_period_end_date)
```
