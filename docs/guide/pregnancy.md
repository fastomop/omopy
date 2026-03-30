# Pregnancy Episode Identification

The `omopy.pregnancy` module identifies pregnancy episodes from OMOP CDM
data using the HIPPS algorithm (Smith et al. 2024). It is the Python
equivalent of the R `PregnancyIdentifier` package.

## Overview

The module has four layers:

1. **Identify** — run the HIPPS algorithm to extract pregnancy episodes
2. **Summarise** — convert episodes into the standard SummarisedResult format
3. **Table** — format results as publication-ready tables (via `omopy.vis`)
4. **Plot** — visualise outcome distributions, gestational age, and timelines

## The HIPPS Algorithm

HIPPS (Hierarchical Identification of Pregnancy Periods and States)
combines two complementary approaches:

| Stage | Name | Description |
|-------|------|-------------|
| **HIP** | Outcome-anchored | Identifies episodes by locating pregnancy outcome codes (live birth, stillbirth, abortion, etc.) and working backwards to estimate the start date |
| **PPS** | Gestational-timing | Identifies episodes by locating gestational age markers and estimating start from the timing |
| **Merge** | HIPPS merge | Combines HIP and PPS episodes, resolving conflicts |
| **ESD** | Episode Start Date | Refines episode start dates using supporting evidence (e.g., LMP records, prenatal visits) |

### Outcome Categories

The algorithm classifies pregnancy outcomes into 8 categories:

| Code | Category |
|------|----------|
| `LB` | Live birth |
| `SB` | Stillbirth |
| `AB` | Induced abortion |
| `SA` | Spontaneous abortion |
| `DELIV` | Delivery (unspecified) |
| `ECT` | Ectopic pregnancy |
| `PREG` | Pregnancy (ongoing / unspecified outcome) |

These are available as the `OUTCOME_CATEGORIES` constant.

## Step 1: Connect to CDM

```python
import ibis
from omopy.connector import cdm_from_con

con = ibis.duckdb.connect("my_database.duckdb", read_only=True)
cdm = cdm_from_con(con, cdm_schema="cdm")
```

## Step 2: Identify Pregnancies

```python
from omopy.pregnancy import identify_pregnancies
import datetime

result = identify_pregnancies(
    cdm,
    start_date=datetime.date(2015, 1, 1),  # Study window start
    end_date=datetime.date(2023, 12, 31),  # Study window end
    age_bounds=(10, 55),                   # Age range (years)
    just_gestation=False,                  # Include non-gestation evidence
    min_cell_count=5,                      # Privacy threshold
)
```

The result is a `PregnancyResult` object containing the full pipeline
output:

```python
# Episode DataFrames (Polars)
result.episodes            # Final merged episodes
result.hip_episodes        # HIP-only episodes (outcome-anchored)
result.pps_episodes        # PPS-only episodes (gestational-timing)
result.merged_episodes     # Pre-ESD merged episodes

# Metadata
result.metadata            # Dict with pipeline parameters and counts
```

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `start_date` | `None` | Study window start (`datetime.date` or `None` for all) |
| `end_date` | `None` | Study window end (`datetime.date` or `None` for all) |
| `age_bounds` | `(10, 55)` | Min/max age at pregnancy start |
| `just_gestation` | `True` | If `True`, only use gestation-related evidence |
| `min_cell_count` | `5` | Privacy suppression threshold |

### Episode DataFrame Columns

The `result.episodes` DataFrame contains:

| Column | Type | Description |
|--------|------|-------------|
| `person_id` | Int64 | Patient identifier |
| `episode_start_date` | Date | Estimated pregnancy start |
| `episode_end_date` | Date | Outcome/end date |
| `outcome_category` | Utf8 | One of the 8 outcome codes |
| `gestational_days` | Int64 | Estimated gestational length in days |
| `source` | Utf8 | `"hip"`, `"pps"`, or `"merged"` |
| `confidence` | Utf8 | Confidence level of the estimate |

## Step 3: Summarise

Convert episodes to the standard 13-column SummarisedResult format:

```python
from omopy.pregnancy import summarise_pregnancies

summary = summarise_pregnancies(
    result,
    strata=["outcome_category"],  # Optional grouping
)
```

The summary includes counts, proportions, and gestational age statistics
per outcome category (and per stratum if specified).

## Step 4: Visualise

### Tables

```python
from omopy.pregnancy import table_pregnancies

# Polars DataFrame
tbl = table_pregnancies(summary, type="polars")

# great-tables GT object
tbl = table_pregnancies(summary, type="gt")
```

### Plots

```python
from omopy.pregnancy import plot_pregnancies

# Outcome distribution bar chart
fig = plot_pregnancies(summary, type="outcome_distribution")
fig.show()

# Gestational age box plot
fig = plot_pregnancies(summary, type="gestational_age")

# Timeline plot
fig = plot_pregnancies(summary, type="timeline")
```

## Validation

Validate episode periods for consistency:

```python
from omopy.pregnancy import validate_episodes
import polars as pl

episodes = result.episodes
issues = validate_episodes(episodes, max_days=320)
# Returns a DataFrame of episodes with potential issues
# (e.g., gestational period > max_days, overlapping episodes)
```

## Testing with Mock Data

Generate a synthetic CDM with pregnancy-related records:

```python
from omopy.pregnancy import mock_pregnancy_cdm

mock_cdm = mock_pregnancy_cdm(
    seed=42,
    n_persons=50,
)

# Use mock CDM with the full pipeline
result = identify_pregnancies(mock_cdm)
summary = summarise_pregnancies(result)
```

## Working with Results

All summarise functions return `SummarisedResult` objects from
`omopy.generics`. These support standard operations:

```python
# Tidy format (unpack group/strata into named columns)
tidy_df = summary.tidy()

# Filter by settings
filtered = summary.filter_settings(result_type="pregnancy_summary")

# Apply minimum cell count suppression
suppressed = summary.suppress(min_cell_count=5)

# Split by group
groups = summary.split_group()
```

See the [SummarisedResult reference](../reference/generics.md) for full details.

## Comparison with R

| R (PregnancyIdentifier) | Python (omopy.pregnancy) |
|---|---|
| `identifyPregnancies()` | `identify_pregnancies()` |
| R list with data.frames | `PregnancyResult` (Pydantic model, Polars DataFrames) |
| `summarisePregnancies()` | `summarise_pregnancies()` |
| `tablePregnancies()` | `table_pregnancies()` |
| `plotPregnancies()` | `plot_pregnancies()` |
| `mockPregnancyCdm()` | `mock_pregnancy_cdm()` |
| `validateEpisodes()` | `validate_episodes()` |
| `OUTCOME_CATEGORIES` | `OUTCOME_CATEGORIES` |
