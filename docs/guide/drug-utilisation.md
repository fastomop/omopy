# Drug Utilisation

The `omopy.drug` module provides comprehensive drug utilisation analysis
for OMOP CDM databases. It is the Python equivalent of the R
`DrugUtilisation` package.

## Overview

The module has five layers:

1. **Cohort generation** — create drug cohorts from concept sets, ingredients, or ATC codes
2. **Requirements** — filter cohorts by temporal criteria (first entry, washout, date range)
3. **Add metrics** — enrich cohorts with utilisation columns (exposures, eras, doses, days)
4. **Summarise** — aggregate metrics into `SummarisedResult` objects
5. **Present** — render results as formatted tables or interactive plots

## Step 1: Generate a Drug Cohort

The primary function creates a drug cohort by resolving concept sets
(including descendants) against the `drug_exposure` table, constraining
to observation periods, and collapsing overlapping records into eras:

```python
from omopy.connector import cdm_from_con
from omopy.drug import generate_drug_utilisation_cohort_set

cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")

cdm = generate_drug_utilisation_cohort_set(
    cdm,
    name="my_drugs",
    concept_set={"aspirin": [1112807], "ibuprofen": [1177480]},
    gap_era=30,  # Merge records within 30 days of each other
)

# The cohort is now available as a CohortTable
print(cdm["my_drugs"].collect())
print(cdm["my_drugs"].settings)
```

### By Ingredient Name

Look up ingredients by name in the vocabulary and generate cohorts:

```python
from omopy.drug import generate_ingredient_cohort_set

cdm = generate_ingredient_cohort_set(
    cdm,
    name="by_ingredient",
    ingredient=["acetaminophen", "ibuprofen"],
    gap_era=7,
)
```

### By ATC Code

Resolve ATC classification codes to generate cohorts:

```python
from omopy.drug import generate_atc_cohort_set

cdm = generate_atc_cohort_set(
    cdm,
    name="by_atc",
    atc_name=["alimentary tract and metabolism"],
    level="ATC 1st",
    gap_era=0,
)
```

### Era Collapsing

Collapse an existing cohort's records into eras (merge nearby records):

```python
from omopy.drug import erafy_cohort, cohort_gap_era

# Collapse records within 14 days
cdm["my_drugs"] = erafy_cohort(cdm["my_drugs"], gap_era=14)

# Query the gap_era setting for each cohort definition
gap_eras = cohort_gap_era(cdm["my_drugs"])
# {1: 14, 2: 14}
```

## Step 2: Filter the Cohort

Apply requirement filters that record attrition in the cohort metadata:

```python
from omopy.drug import (
    require_is_first_drug_entry,
    require_prior_drug_washout,
    require_observation_before_drug,
    require_drug_in_date_range,
)

# Keep only the first drug entry per subject per cohort
cdm["my_drugs"] = require_is_first_drug_entry(cdm["my_drugs"])

# Require 180 days gap since prior drug entry
cdm["my_drugs"] = require_prior_drug_washout(
    cdm["my_drugs"], days=180,
)

# Require 365 days of prior observation before drug start
cdm["my_drugs"] = require_observation_before_drug(
    cdm["my_drugs"], cdm, days=365,
)

# Restrict to a specific date range
cdm["my_drugs"] = require_drug_in_date_range(
    cdm["my_drugs"],
    date_range=("2015-01-01", "2020-12-31"),
)

# View attrition
print(cdm["my_drugs"].attrition)
```

## Step 3: Add Utilisation Metrics

### All-in-One

The `add_drug_utilisation()` function computes all metrics at once:

```python
from omopy.drug import add_drug_utilisation

enriched = add_drug_utilisation(
    cdm["my_drugs"],
    gap_era=30,
    number_exposures=True,
    number_eras=True,
    days_exposed=True,
    days_prescribed=True,
    time_to_exposure=True,
    initial_exposure_duration=True,
    initial_quantity=True,
    cumulative_quantity=True,
    initial_daily_dose=False,  # Requires drug_strength data
    cumulative_dose=False,     # Requires drug_strength data
)

print(enriched.collect())
```

### Individual Metric Functions

Each metric can also be added individually:

```python
from omopy.drug import (
    add_number_exposures,
    add_number_eras,
    add_days_exposed,
    add_days_prescribed,
    add_time_to_exposure,
    add_initial_exposure_duration,
    add_initial_quantity,
    add_cumulative_quantity,
)

# Number of raw drug_exposure records per person
cohort = add_number_exposures(cdm["my_drugs"])

# Number of eras (collapsed records with gap)
cohort = add_number_eras(cdm["my_drugs"], gap_era=30)

# Total days in eras (no double counting)
cohort = add_days_exposed(cdm["my_drugs"], gap_era=30)

# Total days across raw prescriptions (may overlap)
cohort = add_days_prescribed(cdm["my_drugs"])
```

### Dose Metrics

Daily dose calculation requires a populated `drug_strength` table:

```python
from omopy.drug import add_daily_dose, add_initial_daily_dose

# Add daily_dose and unit columns to drug_exposure data
dose_data = add_daily_dose(cdm["drug_exposure"], cdm)

# Add initial daily dose to a drug cohort
cohort = add_initial_daily_dose(cdm["my_drugs"])
```

### Drug Restart Analysis

Classify what happens after a drug cohort ends — restart, switch, or
untreated:

```python
from omopy.drug import add_drug_restart

cohort = add_drug_restart(
    cdm["my_drugs"],
    follow_up_days=[180, 365],
)

# Adds columns like "drug_restart_180" with values:
# "restart", "switch", "restart and switch", "untreated"
```

### Indication and Treatment

Add flags indicating which indication or treatment cohorts overlap with
the drug cohort:

```python
from omopy.drug import add_indication, add_treatment

# Add indication flags (which conditions preceded the drug)
cohort = add_indication(
    cdm["my_drugs"],
    indication_cohort_name="indications",
    window=(-365, 0),
    mutually_exclusive=True,
)

# Add treatment flags (which other drugs followed)
cohort = add_treatment(
    cdm["my_drugs"],
    treatment_cohort_name="treatments",
    window=(0, 365),
)
```

## Step 4: Summarise

### Drug Utilisation Summary

Aggregate utilisation metrics across the cohort:

```python
from omopy.drug import summarise_drug_utilisation

result = summarise_drug_utilisation(
    cdm["my_drugs"],
    gap_era=30,
    number_exposures=True,
    days_exposed=True,
    initial_daily_dose=False,
    cumulative_dose=False,
)

# Result is a SummarisedResult with distribution statistics
# (mean, sd, median, q25, q75, count_missing, percentage_missing)
print(result.data)
```

### Indication and Treatment Summary

```python
from omopy.drug import summarise_indication, summarise_treatment

ind_result = summarise_indication(
    cdm["my_drugs"],
    indication_cohort_name="indications",
    window=[(-365, 0)],
)

tx_result = summarise_treatment(
    cdm["my_drugs"],
    treatment_cohort_name="treatments",
    window=[(0, 365)],
)
```

### Drug Restart Summary

```python
from omopy.drug import summarise_drug_restart

restart_result = summarise_drug_restart(
    cdm["my_drugs"],
    follow_up_days=[180, 365],
)
```

### Proportion of Patients Covered (PPC)

Compute the day-by-day proportion of patients with active drug exposure:

```python
from omopy.drug import summarise_proportion_of_patients_covered

ppc_result = summarise_proportion_of_patients_covered(
    cdm["my_drugs"],
    follow_up_days=365,
)
```

### Dose Coverage

Summarise dose calculation coverage and distribution:

```python
from omopy.drug import summarise_dose_coverage

dose_result = summarise_dose_coverage(
    cdm["my_drugs"],
    ingredient_concept_id=1112807,
)
```

## Step 5: Tables and Plots

### Tables

All table functions wrap `omopy.vis.vis_omop_table()` with domain-specific
formatting defaults:

```python
from omopy.drug import (
    table_drug_utilisation,
    table_indication,
    table_treatment,
    table_drug_restart,
    table_dose_coverage,
    table_proportion_of_patients_covered,
)

# Polars DataFrame with formatted columns
df = table_drug_utilisation(result, type="polars")

# great_tables GT object for rich display
gt = table_drug_utilisation(result, type="gt")
```

| Function | Input result_type |
|----------|------------------|
| `table_drug_utilisation` | `summarise_drug_utilisation` |
| `table_indication` | `summarise_indication` |
| `table_treatment` | `summarise_treatment` |
| `table_drug_restart` | `summarise_drug_restart` |
| `table_dose_coverage` | `summarise_dose_coverage` |
| `table_proportion_of_patients_covered` | `summarise_proportion_of_patients_covered` |

### Plots

All plot functions return `plotly.graph_objects.Figure` objects:

```python
from omopy.drug import (
    plot_drug_utilisation,
    plot_indication,
    plot_treatment,
    plot_drug_restart,
    plot_proportion_of_patients_covered,
)

# Box plot of utilisation metrics
fig = plot_drug_utilisation(result, plot_type="boxplot")
fig.show()

# Bar chart of utilisation metrics
fig = plot_drug_utilisation(result, plot_type="barplot")
fig.show()

# PPC line plot
fig = plot_proportion_of_patients_covered(ppc_result)
fig.show()
```

| Function | Chart Type |
|----------|-----------|
| `plot_drug_utilisation` | Box plot or bar chart |
| `plot_indication` | Stacked bar chart |
| `plot_treatment` | Stacked bar chart |
| `plot_drug_restart` | Stacked bar chart |
| `plot_proportion_of_patients_covered` | Line plot with CI ribbon |

## Drug Strength Patterns

Inspect the drug strength pattern table to understand dose calculation
coverage in your database:

```python
from omopy.drug import pattern_table

patterns = pattern_table(cdm)
print(patterns)
```

## Mock Data

Generate synthetic drug utilisation results for testing:

```python
from omopy.drug import mock_drug_utilisation

mock = mock_drug_utilisation(
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

# Filter by settings
filtered = result.filter_settings(result_type="summarise_drug_utilisation")

# Apply minimum cell count suppression
suppressed = result.suppress(min_cell_count=5)

# Split by group
groups = result.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.
