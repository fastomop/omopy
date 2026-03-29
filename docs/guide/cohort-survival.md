# Cohort Survival

The `omopy.survival` module provides survival analysis functions for
OMOP CDM cohorts. It is the Python equivalent of the R `CohortSurvival`
package.

## Overview

The module has four layers:

1. **Add survival data** — enrich a cohort with time-to-event and status columns
2. **Estimate** — compute Kaplan-Meier or competing risk survival curves
3. **Table** — format results as publication-ready tables (via `omopy.vis`)
4. **Plot** — visualize survival curves as plotly figures

## Step 1: Set Up Cohorts

Survival analysis requires a **target cohort** (the exposed/index population)
and an **outcome cohort** (the event of interest). These are standard OMOP
cohort tables in your CDM:

```python
from omopy.connector import cdm_from_con, generate_concept_cohort_set
from omopy.generics import Codelist

cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")

# Define target and outcome cohorts
cdm = generate_concept_cohort_set(
    cdm,
    Codelist({"diabetes": [201826]}),
    name="target",
)
cdm = generate_concept_cohort_set(
    cdm,
    Codelist({"stroke": [439847]}),
    name="outcome",
)
```

## Step 2: Add Survival Columns

The `add_cohort_survival()` function enriches each row of a cohort with
`time` (days to event or censoring) and `status` (1 = event, 0 = censored):

```python
from omopy.survival import add_cohort_survival

cohort = add_cohort_survival(
    cdm["target"],
    cdm,
    outcome_cohort_table="outcome",
    outcome_cohort_id=1,
    outcome_washout=180,       # Exclude persons with prior event in 180 days
    censor_on_cohort_exit=True,
    follow_up_days=365,        # Cap follow-up at 1 year
)

print(cohort.collect())
# subject_id | cohort_start_date | cohort_end_date | time | status
# 1          | 2020-01-15        | 2021-01-14      | 87   | 1
# 2          | 2020-03-01        | 2021-02-28      | 365  | 0
```

### Censoring Hierarchy

Survival time is computed from the index date (target cohort start) to
the earliest of:

1. **Event date** — first outcome occurrence after the index
2. **Cohort exit** — target cohort end date (if `censor_on_cohort_exit=True`)
3. **Censor date** — custom column value (if `censor_on_date` is specified)
4. **Follow-up cap** — maximum days of follow-up (if `follow_up_days` is finite)
5. **Observation end** — end of the observation period

### Washout

The `outcome_washout` parameter excludes persons who had the outcome
event within a specified window before their index date. Set to `inf`
(the default) to require the entire prior history to be event-free.

## Step 3: Estimate Survival

### Single Event (Kaplan-Meier)

The primary function estimates Kaplan-Meier survival from target/outcome
cohort pairs:

```python
from omopy.survival import estimate_single_event_survival

result = estimate_single_event_survival(
    cdm,
    target_cohort_table="target",
    outcome_cohort_table="outcome",
    outcome_washout=180,
    censor_on_cohort_exit=False,
    follow_up_days=365,
    strata=["sex"],      # Stratify by pre-added columns
    event_gap=30,        # Risk table interval width
    estimate_gap=1,      # Survival curve resolution (days)
)
```

The result is a `SummarisedResult` containing four types of data:

| Result type | Content |
|------------|---------|
| `survival_estimates` | Time-point survival probabilities with CIs |
| `survival_events` | Risk table (n_risk, n_event, n_censor per interval) |
| `survival_summary` | Median survival, RMST, quantiles |
| `survival_attrition` | Step-by-step subject counts through the pipeline |

### Competing Risks (Aalen-Johansen)

When a competing event can prevent the outcome of interest, use the
cumulative incidence function:

```python
from omopy.survival import estimate_competing_risk_survival

# Add a competing risk cohort (e.g., death)
cdm = generate_concept_cohort_set(
    cdm,
    Codelist({"death": [4306655]}),
    name="competing",
)

result = estimate_competing_risk_survival(
    cdm,
    target_cohort_table="target",
    outcome_cohort_table="outcome",
    competing_outcome_cohort_table="competing",
    follow_up_days=365,
)
```

The competing risk result reports **cumulative incidence** (probability of
the event occurring) rather than survival probability.

### Multiple Cohort Combinations

Both estimation functions accept lists of cohort IDs to analyse all
combinations:

```python
result = estimate_single_event_survival(
    cdm,
    target_cohort_table="target",
    outcome_cohort_table="outcome",
    target_cohort_id=[1, 2],    # Analyse both target definitions
    outcome_cohort_id=[1, 2],   # Against both outcome definitions
)
```

## Step 4: Convert Results

The `as_survival_result()` function converts the long-format
`SummarisedResult` into structured wide-format DataFrames:

```python
from omopy.survival import as_survival_result

wide = as_survival_result(result)

# Dict with keys: "estimates", "events", "summary", "attrition"
print(wide["estimates"].columns)
# ['result_id', 'cdm_name', 'target_cohort', 'outcome',
#  'strata_name', 'strata_level', 'time', 'estimate', 'estimate_95CI_lower', ...]

print(wide["summary"])
# median_survival, restricted_mean_survival, quantiles
```

## Step 5: Tables

All table functions format the `SummarisedResult` into publication-ready
output:

```python
from omopy.survival import (
    table_survival,
    table_survival_events,
    table_survival_attrition,
)

# Survival summary table (median, RMST, quantiles)
tbl = table_survival(result, output="polars")

# Risk table (n at risk, events, censored per interval)
events_tbl = table_survival_events(result, output="polars")

# Attrition table (step-by-step counts)
att_tbl = table_survival_attrition(result, output="polars")
```

You can also request `output="gt"` for a `great_tables.GT` object for
rich HTML display.

### Table Options

Query the default options for customization:

```python
from omopy.survival import options_table_survival

defaults = options_table_survival()
# {'header': 'estimate', 'group_column': ['target_cohort'], ...}
```

## Step 6: Plots

### Survival Curves

The `plot_survival()` function creates Kaplan-Meier or cumulative incidence
curves with optional confidence interval ribbons:

```python
from omopy.survival import plot_survival

fig = plot_survival(result)
fig.show()

# With CI ribbons and faceting
fig = plot_survival(
    result,
    confidence_interval=True,
    facet="target_cohort",
    colour="outcome",
    time_scale="days",
)
fig.show()
```

### Risk Tables

The plot can include an integrated risk table below the curve:

```python
fig = plot_survival(
    result,
    risk_table=True,
    risk_table_times=[0, 30, 90, 180, 365],
)
fig.show()
```

### Grouping Columns

Discover which columns are available for faceting or colouring:

```python
from omopy.survival import available_survival_grouping

# All available grouping columns
cols = available_survival_grouping(result)
# ['target_cohort', 'outcome', 'analysis_type', ...]

# Only columns with more than one value
varying = available_survival_grouping(result, varying=True)
# ['outcome']
```

## Mock Data

Generate synthetic CDM data for testing survival workflows:

```python
from omopy.survival import mock_survival

mock_cdm = mock_survival(
    n_persons=200,
    seed=42,
    event_rate=0.3,
    competing_rate=0.15,
    max_follow_up=3650,
    include_strata=True,
)

# The mock CDM has target, outcome, and competing cohort tables
print(mock_cdm["target"].collect())
```

## Working with Results

All estimation functions return `SummarisedResult` objects from
`omopy.generics`. These support standard operations:

```python
# Tidy format (unpack group/strata into named columns)
tidy_df = result.tidy()

# Filter by settings
filtered = result.filter_settings(result_type="survival_estimates")

# Apply minimum cell count suppression
suppressed = result.suppress(min_cell_count=5)

# Split by group
groups = result.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.
