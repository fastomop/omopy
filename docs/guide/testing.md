# Test Data Generation

The `omopy.testing` module provides utilities for creating small,
hand-crafted test patient populations for OMOP CDM studies. It is the
Python equivalent of the R `TestGenerator` package.

## Overview

The module supports two complementary workflows:

1. **File-based** — Define patients in Excel/CSV, validate, export to JSON,
   then load into a CdmReference
2. **Programmatic** — Generate synthetic mock CDMs directly in code for
   quick unit tests

Both workflows produce a standard `CdmReference` that can be used with
all other OMOPy modules.

## Workflow 1: File-Based Test Patients

### Step 1: Generate a Template

Create blank Excel templates with the correct CDM column headers:

```python
from omopy.testing import generate_test_tables

# Generate template for specific tables
path = generate_test_tables(
    ["person", "observation_period", "condition_occurrence", "drug_exposure"],
    cdm_version="5.4",
    output_path="tests/fixtures/",
    filename="my_study_patients.xlsx",
)
print(f"Template created at: {path}")
```

The template contains one sheet per requested table, with column headers
matching the CDM specification.

### Step 2: Fill In Patient Data

Open the generated Excel file and enter your test patient data. Each row
represents one record. For example, the `person` sheet might contain:

| person_id | gender_concept_id | year_of_birth | race_concept_id |
|-----------|-------------------|---------------|-----------------|
| 1 | 8507 | 1980 | 8527 |
| 2 | 8532 | 1975 | 8527 |
| 3 | 8507 | 1990 | 8516 |

### Step 3: Read and Validate

```python
from omopy.testing import read_patients

# Read Excel, validate, and optionally export to JSON
data = read_patients(
    "tests/fixtures/my_study_patients.xlsx",
    cdm_version="5.4",
    test_name="my_study",
    output_path="tests/fixtures/patients.json",  # Optional JSON export
)
# data is a dict[str, pl.DataFrame] — one DataFrame per CDM table
```

### Step 4: Load into CdmReference

```python
from omopy.testing import patients_cdm

cdm = patients_cdm(
    "tests/fixtures/patients.json",
    cdm_version="5.4",
    cdm_name="my_test_cdm",
)

# Use with any OMOPy module
from omopy.profiles import add_demographics
result = add_demographics(cdm["person"], cdm)
```

## Workflow 2: Programmatic Mock CDM

For quick unit tests that don't need hand-crafted data:

```python
from omopy.testing import mock_test_cdm

cdm = mock_test_cdm(
    seed=42,
    n_persons=20,
    cdm_version="5.4",
    include_conditions=True,
    include_drugs=True,
    include_measurements=True,
)

# Inspect generated data
print(cdm["person"].collect())
print(cdm["observation_period"].collect())
print(cdm["condition_occurrence"].collect())
```

### Mock CDM Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `seed` | `42` | Random seed for reproducibility |
| `n_persons` | `5` | Number of persons to generate |
| `cdm_version` | `"5.4"` | CDM version (`"5.3"` or `"5.4"`) |
| `include_conditions` | `True` | Generate condition_occurrence records |
| `include_drugs` | `True` | Generate drug_exposure records |
| `include_measurements` | `False` | Generate measurement records |

## Data Validation

Validate a dict of DataFrames against the CDM specification without
reading from files:

```python
from omopy.testing import validate_patient_data
import polars as pl

data = {
    "person": pl.DataFrame({
        "person_id": [1, 2],
        "gender_concept_id": [8507, 8532],
        "year_of_birth": [1980, 1975],
        "race_concept_id": [8527, 8527],
        "ethnicity_concept_id": [0, 0],
    }),
    "observation_period": pl.DataFrame({
        "observation_period_id": [1, 2],
        "person_id": [1, 2],
        "observation_period_start_date": ["2020-01-01", "2020-01-01"],
        "observation_period_end_date": ["2023-12-31", "2023-12-31"],
        "period_type_concept_id": [44814724, 44814724],
    }),
}

issues = validate_patient_data(data, cdm_version="5.4")
# Returns validation issues (empty if all valid)
```

## Cohort Timeline Visualization

Visualize cohort membership timelines for individual patients:

```python
from omopy.testing import graph_cohort
import polars as pl

# Define cohort data
target_cohort = pl.DataFrame({
    "cohort_definition_id": [1, 1],
    "subject_id": [1, 2],
    "cohort_start_date": ["2020-01-01", "2020-06-01"],
    "cohort_end_date": ["2020-12-31", "2021-05-31"],
})

outcome_cohort = pl.DataFrame({
    "cohort_definition_id": [2, 2],
    "subject_id": [1, 2],
    "cohort_start_date": ["2020-03-15", "2020-09-10"],
    "cohort_end_date": ["2020-03-15", "2020-09-10"],
})

# Plot timeline for subject 1
fig = graph_cohort(
    subject_id=1,
    cohorts={
        "Target": target_cohort,
        "Outcome": outcome_cohort,
    },
)
fig.show()
```

The timeline plot shows horizontal bars for each cohort the patient
belongs to, with start and end dates on the x-axis.

## Integration with Other Modules

The CdmReference objects produced by `patients_cdm()` and `mock_test_cdm()`
are Polars-backed (no database). They work with any OMOPy module that
accepts a CdmReference:

```python
from omopy.testing import mock_test_cdm
from omopy.characteristics import summarise_characteristics

cdm = mock_test_cdm(seed=42, n_persons=50)

# Create a cohort from the mock data
# (the mock CDM includes observation_period records that can serve as
# a simple cohort definition)

from omopy.generics import CohortTable
import polars as pl

obs = cdm["observation_period"].collect()
cohort_df = obs.select(
    pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"),
    pl.col("person_id").alias("subject_id"),
    pl.col("observation_period_start_date").alias("cohort_start_date"),
    pl.col("observation_period_end_date").alias("cohort_end_date"),
)
settings = pl.DataFrame({
    "cohort_definition_id": [1],
    "cohort_name": ["all_patients"],
})
cohort = CohortTable(cohort_df, settings=settings)
cohort.cdm = cdm
```

## Comparison with R

| R (TestGenerator) | Python (omopy.testing) |
|---|---|
| `readPatients()` | `read_patients()` |
| `validatePatientData()` | `validate_patient_data()` |
| `patientsCDM()` | `patients_cdm()` |
| `mockTestCDM()` | `mock_test_cdm()` |
| `generateTestTables()` | `generate_test_tables()` |
| `graphCohort()` | `graph_cohort()` |
| tibbles + R list | Polars DataFrames + CdmReference |
| readxl / writexl | openpyxl |
