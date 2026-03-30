# Drug Exposure Diagnostics

The `omopy.drug_diagnostics` module provides comprehensive diagnostic
checks on drug exposure records in an OMOP CDM database. It is the
Python equivalent of the R `DrugExposureDiagnostics` package.

## Overview

The module has four layers:

1. **Execute** — run configurable diagnostic checks on drug_exposure records
2. **Summarise** — convert check results into the standard SummarisedResult format
3. **Table** — format results as publication-ready tables (via `omopy.vis`)
4. **Plot** — visualise checks as bar charts and box plots

## Available Checks

The `execute_checks()` function supports 12 configurable checks:

| Check | Description |
|-------|-------------|
| `"missing"` | Missing value counts for 15 drug_exposure columns |
| `"exposure_duration"` | Quantile distribution of exposure duration (end - start + 1 days) |
| `"type"` | Frequency of drug_type_concept_id values |
| `"route"` | Frequency of route_concept_id values |
| `"source_concept"` | Source concept mapping analysis |
| `"days_supply"` | Quantile distribution + comparison with date diff |
| `"verbatim_end_date"` | Comparison of verbatim_end_date vs drug_exposure_end_date |
| `"dose"` | Daily dose coverage (requires drug_strength data) |
| `"sig"` | Frequency of sig (verbatim instruction) values |
| `"quantity"` | Quantile distribution of quantity field |
| `"days_between"` | Time between consecutive records per patient |
| `"diagnostics_summary"` | Aggregated summary across all other checks |

## Step 1: Connect to CDM

```python
import ibis
from omopy.connector import cdm_from_con

con = ibis.duckdb.connect("my_database.duckdb", read_only=True)
cdm = cdm_from_con(con, cdm_schema="cdm")
```

## Step 2: Run Diagnostics

Specify one or more ingredient concept IDs and which checks to run:

```python
from omopy.drug_diagnostics import execute_checks

# Run all checks for two ingredients
result = execute_checks(
    cdm,
    ingredient_concept_ids=[1125315, 1503297],
    sample_size=10_000,      # Sample per ingredient (None = all records)
    min_cell_count=5,        # Privacy protection threshold
)

# Run specific checks only
result = execute_checks(
    cdm,
    ingredient_concept_ids=[1125315],
    checks=["missing", "exposure_duration", "type"],
)
```

## Step 3: Explore Results

The result is a `DiagnosticsResult` object containing a dict of Polars
DataFrames — one per check:

```python
# Dict-like access
result["missing"]           # -> pl.DataFrame
result["exposure_duration"]  # -> pl.DataFrame
result["type"]              # -> pl.DataFrame

# Metadata
result.checks_performed     # -> ('missing', 'exposure_duration', ...)
result.ingredient_concepts  # -> {1125315: 'Acetaminophen', ...}
result.execution_time_seconds  # -> 2.345

# Iterate
for check_name, df in result.items():
    print(f"{check_name}: {df.height} rows")
```

### Understanding Missing Values

```python
missing = result["missing"]
# Columns: ingredient_concept_id, ingredient, variable,
#           n_records, n_sample, n_missing, n_not_missing,
#           proportion_missing
print(missing.filter(pl.col("proportion_missing") > 0.5))
```

### Understanding Duration Distribution

```python
duration = result["exposure_duration"]
# Columns include: duration_q05 through duration_q95,
#                   duration_mean, duration_sd, duration_min, duration_max,
#                   n_negative_duration, proportion_negative_duration
```

## Step 4: Summarise to SummarisedResult

Convert to the standard 13-column format for interop with table/plot
functions and other OMOPy modules:

```python
from omopy.drug_diagnostics import summarise_drug_diagnostics

summary = summarise_drug_diagnostics(result)
# -> SummarisedResult with result_type per check
```

## Step 5: Visualise

### Tables

```python
from omopy.drug_diagnostics import table_drug_diagnostics

# All checks as one table
table = table_drug_diagnostics(summary, type="polars")

# Single check
table = table_drug_diagnostics(summary, check="missing", type="gt")
```

### Plots

```python
from omopy.drug_diagnostics import plot_drug_diagnostics

# Missing values bar chart
fig = plot_drug_diagnostics(summary, check="missing")
fig.show()

# Exposure duration box plot
fig = plot_drug_diagnostics(summary, check="exposure_duration")

# Drug type frequencies
fig = plot_drug_diagnostics(summary, check="type")

# Custom title
fig = plot_drug_diagnostics(
    summary,
    check="route",
    title="Route Frequency for Acetaminophen",
)
```

## Privacy Protection

The `min_cell_count` parameter replaces counts below the threshold with
`None` and adds a `result_obscured` column. Set to `0` to disable:

```python
# Strict privacy
result = execute_checks(cdm, [1125315], min_cell_count=10)

# No suppression (for internal analysis)
result = execute_checks(cdm, [1125315], min_cell_count=0)
```

## Sampling

For large datasets, `sample_size` limits the number of records analysed
per ingredient (default: 10,000). Set to `None` for all records:

```python
# Quick analysis
result = execute_checks(cdm, [1125315], sample_size=1000)

# Full analysis
result = execute_checks(cdm, [1125315], sample_size=None)
```

## Testing with Mock Data

```python
from omopy.drug_diagnostics import mock_drug_exposure

# Generate mock DiagnosticsResult for testing
mock_result = mock_drug_exposure(
    n_ingredients=3,
    n_records_per_ingredient=200,
    seed=42,
)
mock_result["missing"]  # Synthetic missing data
```

## Benchmarking

```python
from omopy.drug_diagnostics import benchmark_drug_diagnostics

bench = benchmark_drug_diagnostics(
    cdm,
    ingredient_concept_ids=[1125315, 1503297],
    n_runs=3,
)
print(bench)
# -> DataFrame with run, ingredient, execution_time_seconds
```

## Comparison with R

| R (DrugExposureDiagnostics) | Python (omopy.drug_diagnostics) |
|---|---|
| `executeChecks()` | `execute_checks()` |
| `mockDrugExposure()` | `mock_drug_exposure()` |
| `writeResultToDisk()` | Use `df.write_csv()` / `df.write_parquet()` |
| `viewResults()` (Shiny) | Not ported — use plotly interactivity |
| Named list of tibbles | `DiagnosticsResult` (Pydantic model, dict of Polars DataFrames) |
| `minCellCount` | `min_cell_count` |
| `sampleSize` | `sample_size` |
