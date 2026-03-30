# Treatment Patterns

The `omopy.treatment` module provides treatment pathway analysis for
OMOP CDM cohorts. It is the Python equivalent of the R `TreatmentPatterns`
package.

## Overview

The module has four layers:

1. **Compute** — extract sequential treatment pathways from cohort data
2. **Summarise** — aggregate pathways into frequencies and duration statistics
3. **Table** — format results as publication-ready tables (via `omopy.vis`)
4. **Plot** — visualize pathways as Sankey diagrams, sunburst charts, and
   event duration box plots

## Concepts

### Cohort Types

Treatment pathway analysis uses three types of cohorts, defined via
`CohortSpec`:

| Type | Purpose |
|------|---------|
| **target** | Defines the observation window per patient (e.g., "hypertension diagnosis") |
| **event** | Treatments to track within the window (e.g., "lisinopril", "amlodipine") |
| **exit** | Optional end-of-pathway markers, appended after processing |

Each patient's treatment history is extracted from **event** cohort records
that fall within the **target** cohort's observation window.

### Algorithm Steps

The `compute_pathways()` function runs a 6-step pipeline:

1. **Ingest** — Pull cohort data, join with person table for demographics
   (age, sex), filter by minimum era duration
2. **Treatment history** — Match event cohorts to target observation windows,
   clip events to the window boundaries
3. **Split event cohorts** (optional) — Split specified cohorts into
   acute/therapy sub-cohorts based on a duration cutoff
4. **Era collapse** — Iteratively merge consecutive same-drug eras separated
   by ≤ `era_collapse_size` days
5. **Combination window** — Detect overlapping events ≥ `combination_window`
   days and create combination treatments (e.g., "DrugA+DrugB")
6. **Filter** — Apply treatment filtering: `"first"` (first occurrence per
   drug), `"changes"` (remove consecutive duplicates), or `"all"`

## Step 1: Set Up Cohorts

You need a `CohortTable` containing both target and event cohort records,
plus a `CdmReference` for demographics:

```python
from omopy.connector import cdm_from_con, generate_concept_cohort_set
from omopy.drug import generate_drug_utilisation_cohort_set
from omopy.generics import Codelist, CohortTable
import polars as pl

cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")

# Generate target cohort (condition-based)
cdm = generate_concept_cohort_set(
    cdm,
    Codelist({"hypertension": [320128]}),
    name="target",
)

# Generate event cohorts (drug-based)
cdm = generate_drug_utilisation_cohort_set(
    cdm,
    name="drugs",
    concept_set={
        "lisinopril": [1308216],
        "amlodipine": [1332418],
    },
    gap_era=30,
)

# Combine into a single CohortTable with consistent IDs
target_df = cdm["target"].collect().select(
    pl.lit(100).cast(pl.Int64).alias("cohort_definition_id"),
    "subject_id",
    "cohort_start_date",
    "cohort_end_date",
)
drug_df = cdm["drugs"].collect().select(
    "cohort_definition_id",
    "subject_id",
    "cohort_start_date",
    "cohort_end_date",
)

combined = pl.concat([target_df, drug_df], how="diagonal_relaxed")
settings = pl.DataFrame({
    "cohort_definition_id": [100, 1, 2],
    "cohort_name": ["hypertension", "lisinopril", "amlodipine"],
})
cohort = CohortTable(combined, settings=settings)
cohort.cdm = cdm
```

## Step 2: Define Cohort Specs

Map each cohort definition to its role in the pathway analysis:

```python
from omopy.treatment import CohortSpec

cohort_specs = [
    CohortSpec(cohort_id=100, cohort_name="hypertension", type="target"),
    CohortSpec(cohort_id=1, cohort_name="lisinopril", type="event"),
    CohortSpec(cohort_id=2, cohort_name="amlodipine", type="event"),
]
```

## Step 3: Compute Pathways

```python
from omopy.treatment import compute_pathways

result = compute_pathways(
    cohort,
    cdm,
    cohort_specs,
    era_collapse_size=30,      # Merge same-drug gaps ≤ 30 days
    combination_window=30,     # Overlaps ≥ 30 days → combination
    filter_treatments="first", # Keep first occurrence per drug
    max_path_length=5,         # Truncate paths to 5 steps
)
```

The result is a `PathwayResult` containing:

- **`treatment_history`** — a Polars DataFrame with one row per treatment
  event per person, including `person_id`, `event_cohort_id`,
  `event_cohort_name`, `event_seq`, `duration_era`, `age`, `sex`
- **`attrition`** — a Polars DataFrame tracking record/subject counts
  through each pipeline step
- **`cohorts`** — the original `CohortSpec` definitions
- **`cdm_name`** — the CDM source name
- **`arguments`** — the parameters used for the computation

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `era_collapse_size` | `30` | Maximum gap (days) between same-drug eras to merge |
| `combination_window` | `30` | Minimum overlap (days) to create a combination treatment |
| `filter_treatments` | `"first"` | `"first"`, `"changes"`, or `"all"` |
| `max_path_length` | `5` | Maximum number of treatment steps per patient |
| `min_era_duration` | `0` | Minimum era duration (days) to include |
| `split_event_cohorts` | `None` | Dict mapping cohort IDs to duration cutoffs for acute/therapy split |

### Treatment Paths

Paths are encoded as dash-separated strings. Combinations use `+`:

```
"lisinopril"                        # Single drug
"lisinopril-amlodipine"             # Sequential: lisinopril then amlodipine
"amlodipine+lisinopril"             # Combination (alphabetically sorted)
"lisinopril-amlodipine+lisinopril"  # lisinopril, then combination
```

## Step 4: Summarise

### Treatment Pathways

Aggregate pathways into frequencies by path, age group, sex, and index year:

```python
from omopy.treatment import summarise_treatment_pathways

summary = summarise_treatment_pathways(
    result,
    min_cell_count=5,  # Suppress small counts
)
```

The result is a `SummarisedResult` with `result_type="treatment_pathways"`.
Each row represents a unique combination of path, demographics, and target
cohort.

### Event Duration

Compute duration statistics for each treatment event:

```python
from omopy.treatment import summarise_event_duration

duration = summarise_event_duration(result)
```

The result contains summary statistics (min, q1, median, q3, max, mean, sd)
per event cohort, both overall and by position in the pathway.

## Step 5: Tables

Format results as publication-ready tables:

```python
from omopy.treatment import table_treatment_pathways, table_event_duration

# Treatment pathway frequency table
tbl = table_treatment_pathways(summary, type="polars")

# Event duration statistics table
dur_tbl = table_event_duration(duration, type="polars")
```

Use `type="gt"` for a `great_tables.GT` object for rich HTML display.

## Step 6: Plots

### Sankey Diagram

Visualise treatment flows between steps:

```python
from omopy.treatment import plot_sankey

fig = plot_sankey(summary)
fig.show()
```

The Sankey diagram shows treatment transitions across sequential steps,
with link widths proportional to patient counts.

### Sunburst Chart

Visualise treatment path hierarchies:

```python
from omopy.treatment import plot_sunburst

fig = plot_sunburst(summary)
fig.show()
```

The sunburst chart shows the hierarchical structure of treatment paths,
with inner rings representing earlier treatment steps.

### Event Duration Box Plot

Visualise treatment duration distributions:

```python
from omopy.treatment import plot_event_duration

fig = plot_event_duration(duration)
fig.show()
```

## Mock Data

Generate synthetic treatment pathway data for testing:

```python
from omopy.treatment import mock_treatment_pathways

mock = mock_treatment_pathways(
    seed=42,
    include_duration=True,  # Include event duration data
)

# Use mock data with table/plot functions
tbl = table_treatment_pathways(mock, type="polars")
fig = plot_sankey(mock)
```

## Working with Results

All summarise functions return `SummarisedResult` objects from
`omopy.generics`. These support standard operations:

```python
# Tidy format (unpack group/strata into named columns)
tidy_df = summary.tidy()

# Filter by settings
filtered = summary.filter_settings(result_type="treatment_pathways")

# Apply minimum cell count suppression
suppressed = summary.suppress(min_cell_count=5)

# Split by group
groups = summary.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.
