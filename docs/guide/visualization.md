# Visualization

The `omopy.vis` module formats, tabulates, and plots summarised results.
It is the Python equivalent of the R `visOmopResults` package.

Table rendering uses [great_tables](https://posit-dev.github.io/great-tables/);
plot rendering uses [plotly](https://plotly.com/python/).

## Format Pipeline

The core workflow is a composable pipeline of format functions:

```python
from omopy.vis import (
    format_estimate_value,
    format_estimate_name,
    format_header,
    vis_omop_table,
)

# Step 1: Format numeric precision (round, add thousands separators)
result = format_estimate_value(result)

# Step 2: Combine estimates into display strings
result = format_estimate_name(
    result,
    estimate_name={"N (%)": "<count> (<percentage>%)"},
)

# Step 3: Pivot columns into multi-level headers
df = format_header(result, header=["cohort_name"])
```

Or use `vis_omop_table` to run the entire pipeline at once:

```python
table = vis_omop_table(
    result,
    estimate_name={"N (%)": "<count> (<percentage>%)"},
    header=["cohort_name"],
    title="Demographics by Cohort",
)
```

## Formatting Estimate Values

```python
from omopy.vis import format_estimate_value

# Default: integers get 0 decimals, numerics get 2, percentages get 1
formatted = format_estimate_value(result)

# Custom decimal places and separators
formatted = format_estimate_value(
    result,
    decimals={"numeric": 3, "percentage": 2},
    decimal_mark=",",   # European-style: 1.234,56
    big_mark=".",
)
```

## Formatting Estimate Names

Combine multiple estimates into composite display strings using templates:

```python
from omopy.vis import format_estimate_name

formatted = format_estimate_name(
    result,
    estimate_name={
        "N (%)": "<count> (<percentage>%)",
        "Mean (SD)": "<mean> (<sd>)",
    },
    keep_not_formatted=True,   # keep rows that don't match any template
    use_format_order=True,     # order output by template key order
)
```

Templates reference estimate names with `<name>` placeholders. For example,
`"<count> (<percentage>%)"` replaces `<count>` and `<percentage>` with
values from rows sharing the same group/strata/variable combination.

## Suppressed Values

Replace suppressed counts (from minimum cell count rules) with `<N`:

```python
from omopy.vis import format_min_cell_count

result = format_min_cell_count(result)
```

## Tables

### High-Level: vis_omop_table

The main entry point for rendering a `SummarisedResult` as a table:

```python
from omopy.vis import vis_omop_table

table = vis_omop_table(
    result,
    estimate_name={"N (%)": "<count> (<percentage>%)"},
    header=["cohort_name"],
    group_column=["variable_name"],
    hide=["cdm_name"],
    title="Demographics Summary",
    subtitle="By cohort",
)
```

This returns a `great_tables.GT` object (if available) or a `polars.DataFrame`.
Pass `type="polars"` to always get a DataFrame.

### Lower-Level: vis_table and format_table

For non-`SummarisedResult` DataFrames:

```python
from omopy.vis import vis_table, format_table

# vis_table: SummarisedResult -> formatted table (skips estimate pipeline)
table = vis_table(result, header=["strata"], title="Results")

# format_table: any Polars DataFrame -> great_tables or polars output
import polars as pl
df = pl.DataFrame({"name": ["A", "B"], "value": [1.23, 4.56]})
table = format_table(df, title="My Table")
```

## Plots

All plot functions accept a `SummarisedResult` (auto-tidied) or a plain
`polars.DataFrame`, and return a `plotly.graph_objects.Figure`.

### Scatter Plot

```python
from omopy.vis import scatter_plot

fig = scatter_plot(
    result,
    x="age",
    y="count",
    colour="cohort_name",
    line=True,       # connect points with lines
    point=True,      # show data points
    title="Count by Age",
)
fig.show()
```

Add a confidence ribbon:

```python
fig = scatter_plot(
    result,
    x="age",
    y="mean",
    ribbon=True,
    y_min="lower",
    y_max="upper",
)
```

### Bar Plot

```python
from omopy.vis import bar_plot

fig = bar_plot(
    result,
    x="variable_name",
    y="count",
    colour="cohort_name",
    facet="strata",
    position="dodge",  # or "stack"
    title="Counts by Variable",
)
```

### Box Plot

```python
from omopy.vis import box_plot

fig = box_plot(
    result,
    x="cohort_name",
    lower="q25",
    middle="median",
    upper="q75",
    colour="sex",
    facet=["strata_1", "strata_2"],  # 2-element list for row/col grid
    title="Age Distribution",
)
```

## Styling

### Table Style

```python
from omopy.vis import TableStyle, default_table_style, vis_omop_table

style = TableStyle(
    font_size=14,
    title_align="center",
    header_background="#4472C4",
    header_color="white",
    group_background="#D9E2F3",
)

table = vis_omop_table(result, style=style, title="Styled Table")
```

### Plot Style

```python
from omopy.vis import PlotStyle, default_plot_style, scatter_plot

style = PlotStyle(
    color_palette=["#E41A1C", "#377EB8", "#4DAF4A"],
    font_size=14,
    font_family="Arial",
    background_color="#F8F8F8",
    show_legend=True,
)

fig = scatter_plot(result, x="age", y="count", style=style)
```

## Text Customisation

The `customise_text` function transforms column names or labels:

```python
from omopy.vis import customise_text

# Apply a function
labels = customise_text(
    ["cohort_name", "variable_name"],
    fun=str.title,
)
# ["Cohort_Name", "Variable_Name"]

# Map specific values
labels = customise_text(
    ["cohort_name", "cdm_name"],
    custom={"cohort_name": "Cohort", "cdm_name": "Database"},
)

# Keep certain values unchanged
labels = customise_text(
    ["cohort_name", "N (%)"],
    fun=str.upper,
    keep=["N (%)"],
)
# ["COHORT_NAME", "N (%)"]
```

## Mock Data for Testing

Generate a synthetic `SummarisedResult` for prototyping:

```python
from omopy.vis import mock_summarised_result

result = mock_summarised_result(n_cohorts=3, n_strata=4)
print(result.data.shape)  # (many rows, 13 columns)
```
