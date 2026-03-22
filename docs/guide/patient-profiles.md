# Patient Profiles

The `omopy.profiles` module enriches cohort tables with patient-level clinical
data. It is the Python equivalent of the R `PatientProfiles` package.

All functions take a `CdmTable` (typically a cohort) and the parent `CdmReference`,
and return a new `CdmTable` with additional columns. Operations are lazy — no
queries execute until you `.collect()`.

## Demographics

```python
from omopy.profiles import (
    add_demographics,
    add_age,
    add_sex,
    add_prior_observation,
    add_future_observation,
    add_date_of_birth,
    add_in_observation,
)

# Add multiple demographics at once
enriched = add_demographics(
    cdm["my_cohort"],
    cdm,
    age=True,
    sex=True,
    prior_observation=True,
    future_observation=True,
)

# Or add individually
with_age = add_age(cdm["my_cohort"], cdm)
with_sex = add_sex(cdm["my_cohort"], cdm)
```

### Age Groups

```python
from omopy.profiles import add_age, add_categories

# Add age first
with_age = add_age(cdm["my_cohort"], cdm)

# Then categorise
with_groups = add_categories(
    with_age,
    column_name="age",
    categories={"0-17": [0, 17], "18-64": [18, 64], "65+": [65, 150]},
)
```

## Cohort Intersections

Check if patients in one cohort have records in another cohort:

```python
from omopy.profiles import (
    add_cohort_intersect_flag,
    add_cohort_intersect_count,
    add_cohort_intersect_date,
    add_cohort_intersect_days,
)

# Binary flag: does the patient have a diabetes record?
with_flag = add_cohort_intersect_flag(
    cdm["hypertension"],
    cdm,
    target_cohort_table="diabetes_cohort",
    target_cohort_id=1,
    window=(0, float("inf")),  # any time after index
)

# Count of intersections
with_count = add_cohort_intersect_count(
    cdm["hypertension"],
    cdm,
    target_cohort_table="diabetes_cohort",
    target_cohort_id=1,
    window=(-365, -1),  # in the year before index
)
```

## Table Intersections

Check for records in CDM domain tables:

```python
from omopy.profiles import (
    add_table_intersect_flag,
    add_table_intersect_count,
    add_table_intersect_date,
    add_table_intersect_days,
)

# Any drug exposure in the 6 months before?
with_flag = add_table_intersect_flag(
    cdm["my_cohort"],
    cdm,
    table_name="drug_exposure",
    window=(-180, -1),
)

# Date of first condition after index
with_date = add_table_intersect_date(
    cdm["my_cohort"],
    cdm,
    table_name="condition_occurrence",
    window=(0, 365),
    order="first",
)
```

## Concept Intersections

Check for specific concept IDs in domain tables:

```python
from omopy.profiles import (
    add_concept_intersect_flag,
    add_concept_intersect_count,
)

# Has the patient had a specific lab measurement?
with_flag = add_concept_intersect_flag(
    cdm["my_cohort"],
    cdm,
    concept_ids=[3004249],  # Systolic blood pressure
    table_name="measurement",
    window=(-30, 0),
)
```

## Death

```python
from omopy.profiles import add_death_flag, add_death_date, add_death_days

with_death = add_death_flag(cdm["my_cohort"], cdm)
with_date = add_death_date(cdm["my_cohort"], cdm)
with_days = add_death_days(cdm["my_cohort"], cdm)
```

## Windows

Windows define the time period relative to the index date
(`cohort_start_date`) to search for intersecting records.

```python
# Single window: any time after index
window = (0, float("inf"))

# Multiple windows
windows = [
    (-365, -1),          # year before
    (0, 0),              # on index date
    (1, 365),            # year after
    (-float("inf"), -1), # any time before
]
```

Window bounds are in **days**. Negative = before index, positive = after index.
Use `float("inf")` and `float("-inf")` for unbounded.
