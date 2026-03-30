# Cohort Characteristics

The `omopy.characteristics` module provides analytical functions for
characterizing cohorts defined in OMOP CDM databases. It is the Python
equivalent of the R `CohortCharacteristics` package.

## Overview

The module has three layers:

1. **Summarise** — compute statistics and return `SummarisedResult` objects
2. **Table** — format results as display-ready tables (via `omopy.vis`)
3. **Plot** — visualize results as plotly figures

## Summarise Characteristics

The primary function for cohort characterization computes demographics,
counts, and clinical variable distributions:

```python
from omopy.connector import cdm_from_con
from omopy.connector import generate_concept_cohort_set
from omopy.generics import Codelist
from omopy.characteristics import summarise_characteristics

# Connect and generate a cohort
cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")
codelist = Codelist({"hypertension": [320128]})
cdm = generate_concept_cohort_set(cdm, codelist, name="my_cohort")

# Summarise characteristics with demographics
result = summarise_characteristics(
    cdm["my_cohort"],
    demographics=True,
    counts=True,
)

# Result is a SummarisedResult with 13 standard columns
print(result.data)
```

The result includes:

- **Number records** and **Number subjects** per cohort
- **Age** — min, q25, median, q75, max, mean, sd
- **Sex** — count and percentage per category
- **Prior observation** and **Future observation** — distribution
- **Days in cohort** — distribution

### Stratification

Add strata columns to break down results by subgroups:

```python
from omopy.profiles import add_sex

# Pre-add the column you want to stratify by
cohort_with_sex = add_sex(cdm["my_cohort"], cdm)

result = summarise_characteristics(
    cohort_with_sex,
    strata=["sex"],
    demographics=True,
)

# Result includes both overall and per-sex strata
strata = result.data["strata_name"].unique().to_list()
# ['overall', 'sex']
```

### Filtering by Cohort ID

All summarise functions accept a `cohort_id` parameter to restrict analysis
to specific cohort definitions:

```python
result = summarise_characteristics(
    cdm["my_cohort"],
    cohort_id=[1],  # Only cohort definition ID 1
    demographics=True,
)
```

### Adding Intersections

Enrich with clinical intersections before summarising:

```python
result = summarise_characteristics(
    cdm["my_cohort"],
    demographics=True,
    table_intersect_flag=[
        {"table_name": "drug_exposure", "window": (-365, -1)},
    ],
)
```

## Cohort Counts

Quick subject and record counts per cohort:

```python
from omopy.characteristics import summarise_cohort_count

counts = summarise_cohort_count(cdm["my_cohort"])
```

## Cohort Attrition

Summarise the step-by-step attrition (filtering) that built a cohort:

```python
from omopy.characteristics import summarise_cohort_attrition

attrition = summarise_cohort_attrition(cdm["my_cohort"])
```

The result has `strata_name="reason"` and `additional_name="reason_id"`,
with variables for `number_records`, `number_subjects`,
`excluded_records`, and `excluded_subjects`.

## Cohort Timing

Compute the distribution of days between entries across different cohorts
for subjects appearing in multiple cohorts:

```python
from omopy.characteristics import summarise_cohort_timing

timing = summarise_cohort_timing(cdm["my_cohort"])
```

The `group_name` uses the compound format
`"cohort_name_reference &&& cohort_name_comparator"`.

## Cohort Overlap

Count subjects appearing in one, both, or neither of two cohorts:

```python
from omopy.characteristics import summarise_cohort_overlap

overlap = summarise_cohort_overlap(cdm["my_cohort"])
```

Returns counts and percentages for "Only in reference cohort",
"Only in comparator cohort", and "In both cohorts".

## Large-Scale Characteristics

Compute concept-level prevalence across time windows:

```python
from omopy.characteristics import summarise_large_scale_characteristics

lsc = summarise_large_scale_characteristics(
    cdm["my_cohort"],
    event_in_window=["condition_occurrence", "drug_exposure"],
    window=[(-365, -1), (0, 0), (1, 365)],
    minimum_frequency=0.01,
)
```

## Cohort Codelist

Summarise the codelists used to define each cohort:

```python
from omopy.characteristics import summarise_cohort_codelist

codelist = summarise_cohort_codelist(cdm["my_cohort"])
```

## Tables

All table functions wrap `omopy.vis.vis_omop_table()` with domain-specific
formatting defaults. They accept a `SummarisedResult` and return a
formatted table (Polars DataFrame by default, or `great_tables.GT`):

```python
from omopy.characteristics import (
    summarise_characteristics,
    table_characteristics,
)

result = summarise_characteristics(cdm["my_cohort"], demographics=True)

# Polars DataFrame with formatted columns
df = table_characteristics(result, output="polars")

# great_tables GT object for rich display
gt = table_characteristics(result, output="gt")
```

Available table functions:

| Function | Input result_type |
|----------|------------------|
| `table_characteristics` | `summarise_characteristics` |
| `table_cohort_count` | `summarise_cohort_count` |
| `table_cohort_attrition` | `summarise_cohort_attrition` |
| `table_cohort_timing` | `summarise_cohort_timing` |
| `table_cohort_overlap` | `summarise_cohort_overlap` |
| `table_large_scale_characteristics` | `summarise_large_scale_characteristics` |
| `table_top_large_scale_characteristics` | `summarise_large_scale_characteristics` |

## Plots

All plot functions return `plotly.graph_objects.Figure` objects:

```python
from omopy.characteristics import (
    summarise_cohort_count,
    plot_cohort_count,
)

counts = summarise_cohort_count(cdm["my_cohort"])
fig = plot_cohort_count(counts)
fig.show()
```

Available plot functions:

| Function | Chart Type |
|----------|-----------|
| `plot_characteristics` | Bar, scatter, or box plot |
| `plot_cohort_count` | Bar chart |
| `plot_cohort_attrition` | Flowchart (Plotly shapes) |
| `plot_cohort_timing` | Box or density plot |
| `plot_cohort_overlap` | Stacked bar chart |
| `plot_large_scale_characteristics` | Scatter plot |
| `plot_compared_large_scale_characteristics` | Scatter with diagonal reference |

## Mock Data

Generate mock `SummarisedResult` objects for testing:

```python
from omopy.characteristics import mock_cohort_characteristics

mock = mock_cohort_characteristics(
    n_cohorts=2,
    seed=42,
)
```

## Working with Results

All summarise functions return `SummarisedResult` objects from
`omopy.generics`. These support standard operations:

```python
# Tidy format (unpack group/strata into named columns)
tidy_df = result.tidy()

# Pivot estimates into wide format
wide_df = result.pivot_estimates()

# Filter by settings
filtered = result.filter_settings(result_type="summarise_characteristics")

# Apply minimum cell count suppression
suppressed = result.suppress(min_cell_count=5)

# Split by group
groups = result.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.
