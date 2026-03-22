# Incidence & Prevalence

The `omopy.incidence` module estimates incidence rates and prevalence
proportions from OMOP CDM data. It is the Python equivalent of the R
`IncidencePrevalence` package.

## Overview

The analysis pipeline has three stages:

1. **Denominator generation** — build denominator cohorts from observation
   periods, optionally stratified by age, sex, and prior observation
2. **Estimation** — compute incidence rates or prevalence proportions over
   calendar intervals with confidence intervals
3. **Presentation** — format results as tables or plots

## Step 1: Generate a Denominator

The denominator defines the population at risk. It is built from the
`observation_period` table, clipped to a study window and filtered by
demographic criteria.

```python
from omopy.connector import cdm_from_con
from omopy.incidence import generate_denominator_cohort_set

cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")

cdm = generate_denominator_cohort_set(
    cdm,
    name="denominator",
    study_period_start="2010-01-01",
    study_period_end="2020-12-31",
)

# The denominator is now a CohortTable in the CDM
print(cdm["denominator"].collect())
```

### Age and Sex Stratification

Generate multiple denominator cohorts stratified by age groups and sex:

```python
cdm = generate_denominator_cohort_set(
    cdm,
    name="denominator",
    age_group=[(0, 17), (18, 64), (65, 150)],
    sex=["Male", "Female", "Both"],
    days_prior_observation=365,
    study_period_start="2015-01-01",
    study_period_end="2020-12-31",
)

# Settings show one row per stratum combination
print(cdm["denominator"].settings)
```

Each combination of `age_group` and `sex` produces a separate cohort
definition in the returned `CohortTable`, with attrition tracking showing
how many persons were excluded at each filtering step.

### Target-Based Denominator

If you already have a target cohort (e.g., persons with a specific
condition), restrict the denominator to those persons:

```python
from omopy.incidence import generate_target_denominator_cohort_set

cdm = generate_target_denominator_cohort_set(
    cdm,
    target_cohort_table="my_target_cohort",
    name="target_denom",
    study_period_start="2015-01-01",
    study_period_end="2020-12-31",
)
```

## Step 2: Estimate Incidence or Prevalence

### Incidence

Incidence measures the rate of new outcome events per person-time at risk:

```python
from omopy.incidence import estimate_incidence

result = estimate_incidence(
    cdm,
    denominator_table="denominator",
    outcome_table="outcome_cohort",
    interval="years",
    outcome_washout=float("inf"),  # Only first event per person
    repeating_events=False,
)

# Result is a SummarisedResult
print(result.data.columns)
```

Key parameters:

- `interval` — `"weeks"`, `"months"`, `"quarters"`, `"years"`, or `"overall"`
- `outcome_washout` — days to exclude after a prior event (`inf` = first event only)
- `repeating_events` — if `True`, allow the same person to contribute multiple events
- `complete_database_intervals` — if `True`, only include intervals where the
  entire denominator is observable

Confidence intervals use the **Poisson exact method** (chi-squared quantiles
via `scipy.stats.chi2`). Results are expressed per 100,000 person-years.

### Point Prevalence

Point prevalence measures the proportion with an active outcome at a specific
time point within each interval:

```python
from omopy.incidence import estimate_point_prevalence

result = estimate_point_prevalence(
    cdm,
    denominator_table="denominator",
    outcome_table="outcome_cohort",
    interval="years",
    time_point="start",  # or "middle", "end"
)
```

### Period Prevalence

Period prevalence measures the proportion with any active outcome during
each interval:

```python
from omopy.incidence import estimate_period_prevalence

result = estimate_period_prevalence(
    cdm,
    denominator_table="denominator",
    outcome_table="outcome_cohort",
    interval="quarters",
)
```

Prevalence confidence intervals use the **Wilson score method**
(`scipy.stats.norm`).

## Step 3: View Results

### Convert to Tidy DataFrames

Pivot the long-form `SummarisedResult` into a wide, analysis-ready DataFrame:

```python
from omopy.incidence import as_incidence_result, as_prevalence_result

# Wide DataFrame with named columns
inc_df = as_incidence_result(result)
print(inc_df.columns)
# ['denominator_cohort_name', 'outcome_cohort_name', 'incidence_start_date',
#  'incidence_end_date', 'n_events', 'n_persons', 'person_years',
#  'incidence_100000_pys', 'incidence_100000_pys_95ci_lower',
#  'incidence_100000_pys_95ci_upper', ...]

prev_df = as_prevalence_result(prev_result)
```

### Tables

Format results as display-ready tables:

```python
from omopy.incidence import table_incidence, table_prevalence

# Polars DataFrame with formatted columns
df = table_incidence(result, output="polars")

# great_tables GT object for rich display
gt = table_incidence(result, output="gt")
```

Attrition tables show how many persons were excluded at each step:

```python
from omopy.incidence import table_incidence_attrition

attrition_table = table_incidence_attrition(result)
```

### Plots

Visualize incidence and prevalence trends:

```python
from omopy.incidence import plot_incidence, plot_prevalence

# Incidence over time — scatter plot with CI ribbons
fig = plot_incidence(result)
fig.show()

# Prevalence over time
fig = plot_prevalence(prev_result)
fig.show()
```

Population pyramid plots show the denominator size per interval:

```python
from omopy.incidence import plot_incidence_population

fig = plot_incidence_population(result)
fig.show()
```

### Grouping and Faceting

Discover which columns are available for grouping in plots:

```python
from omopy.incidence import available_incidence_grouping

grouping_cols = available_incidence_grouping(result)
# e.g., ['denominator_cohort_name', 'outcome_cohort_name', ...]
```

## Mock Data

Generate synthetic incidence/prevalence data for testing and prototyping:

```python
from omopy.incidence import mock_incidence_prevalence

mock_cdm = mock_incidence_prevalence(
    n_persons=1000,
    seed=42,
)

# mock_cdm has person, observation_period, target, and outcome tables
```

## Benchmarking

Time the full analysis pipeline:

```python
from omopy.incidence import benchmark_incidence_prevalence

timing = benchmark_incidence_prevalence(cdm)
print(f"Total time: {timing['total']:.2f}s")
```

## Working with Results

All estimation functions return `SummarisedResult` objects from
`omopy.generics`. These support standard operations:

```python
# Tidy format (unpack group/strata into named columns)
tidy_df = result.tidy()

# Filter by settings
filtered = result.filter_settings(result_type="estimate_incidence")

# Apply minimum cell count suppression
suppressed = result.suppress(min_cell_count=5)

# Split by group
groups = result.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.
