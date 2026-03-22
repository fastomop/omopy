# omopy.characteristics

Cohort characterization analytics — summarise, tabulate, and plot cohort
characteristics, counts, attrition, timing, overlap, large-scale
characteristics, and codelist usage.

This module is the Python equivalent of the R `CohortCharacteristics` package.
Table rendering delegates to [`omopy.vis`](vis.md); plot rendering uses
[plotly](https://plotly.com/python/).

## Summarise Functions

The core analytical functions. Each queries cohort data (optionally enriching
with demographics and intersections via `omopy.profiles`) and produces a
`SummarisedResult`.

::: omopy.characteristics.summarise_characteristics

::: omopy.characteristics.summarise_cohort_count

::: omopy.characteristics.summarise_cohort_attrition

::: omopy.characteristics.summarise_cohort_timing

::: omopy.characteristics.summarise_cohort_overlap

::: omopy.characteristics.summarise_large_scale_characteristics

::: omopy.characteristics.summarise_cohort_codelist

## Table Functions

Thin wrappers around `vis_omop_table()` / `vis_table()` with
domain-specific defaults for estimate formatting, headers, and grouping.

::: omopy.characteristics.table_characteristics

::: omopy.characteristics.table_cohort_count

::: omopy.characteristics.table_cohort_attrition

::: omopy.characteristics.table_cohort_timing

::: omopy.characteristics.table_cohort_overlap

::: omopy.characteristics.table_large_scale_characteristics

::: omopy.characteristics.table_top_large_scale_characteristics

::: omopy.characteristics.available_table_columns

## Plot Functions

Wrappers around `bar_plot()`, `scatter_plot()`, `box_plot()`, and custom
Plotly visualizations.

::: omopy.characteristics.plot_characteristics

::: omopy.characteristics.plot_cohort_count

::: omopy.characteristics.plot_cohort_attrition

::: omopy.characteristics.plot_cohort_overlap

::: omopy.characteristics.plot_cohort_timing

::: omopy.characteristics.plot_large_scale_characteristics

::: omopy.characteristics.plot_compared_large_scale_characteristics

## Mock Data

::: omopy.characteristics.mock_cohort_characteristics
