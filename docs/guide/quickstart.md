# Quickstart

This guide walks through the core OMOPy workflow: connecting to a database,
inspecting CDM tables, generating cohorts, and enriching with patient data.

## Connect to a Database

```python
from omopy.connector import cdm_from_con

# DuckDB file — the simplest case
cdm = cdm_from_con("data/synthea.duckdb", cdm_schema="base")

# Or from an existing Ibis connection
import ibis
con = ibis.duckdb.connect("data/synthea.duckdb", read_only=True)
cdm = cdm_from_con(con, cdm_schema="base")
```

`cdm_from_con` auto-detects the CDM version, discovers available tables,
and returns a `CdmReference` — a dict-like container of lazy `CdmTable` objects.

## Explore the CDM

```python
# List available tables
print(cdm.table_names)
# ['person', 'observation_period', 'condition_occurrence', ...]

# Access a table (returns a CdmTable wrapping an Ibis expression)
person = cdm["person"]
print(person.count())  # number of rows

# Collect to a Polars DataFrame
df = person.head(5).collect()
print(df)
```

## Take a Snapshot

```python
from omopy.connector import snapshot

snap = snapshot(cdm)
print(snap.person_count)
print(snap.vocabulary_version)
print(snap.earliest_observation_period_start_date)
print(snap.latest_observation_period_end_date)
```

## Generate a Cohort

### From Concept Sets

```python
from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set

# Define conditions by OMOP concept IDs
codelist = Codelist({
    "hypertension": [320128],
    "diabetes": [201826],
})

# Generate cohorts — returns an updated CDM with a new CohortTable
cdm = generate_concept_cohort_set(
    cdm,
    codelist,
    name="conditions",
)

# Access the cohort
cohort = cdm["conditions"]
print(cohort.count())
print(cohort.collect())
```

### From CIRCE JSON (ATLAS Definitions)

```python
from omopy.connector import generate_cohort_set

cdm = generate_cohort_set(
    cdm,
    "path/to/cohort_definitions/",
    name="atlas_cohorts",
)
```

## Enrich with Patient Profiles

```python
from omopy.profiles import add_demographics, add_age

# Add age, sex, and observation period info
enriched = add_demographics(
    cdm["conditions"],
    cdm,
    age=True,
    sex=True,
    prior_observation=True,
)

df = enriched.collect()
print(df.columns)
# [..., 'age', 'sex', 'prior_observation']
```

## Build a Codelist from Vocabulary

```python
from omopy.codelist import get_candidate_codes, get_descendants

# Search for concepts by keyword
candidates = get_candidate_codes(
    cdm,
    keywords=["sinusitis"],
    domains=["Condition"],
    standard_concept="S",
)
print(candidates)

# Expand to include descendant concepts
expanded = get_descendants(cdm, candidates)
print(expanded)
```

## Subset the CDM

```python
from omopy.connector import cdm_subset, cdm_sample

# Subset to specific persons
small_cdm = cdm_subset(cdm, person_ids=[1, 2, 3])

# Or take a random sample
sample_cdm = cdm_sample(cdm, n=10)
```

## Export Results

```python
from omopy.generics import export_summarised_result

# If you have a SummarisedResult
export_summarised_result(result, "output/results.csv", min_cell_count=5)
```
