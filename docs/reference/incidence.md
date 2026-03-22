# omopy.incidence

Incidence and prevalence estimation — generate denominator cohorts, compute
incidence rates and prevalence proportions, and present results as tables
and plots.

This module is the Python equivalent of the R `IncidencePrevalence` package.
Table rendering delegates to [`omopy.vis`](vis.md); plot rendering uses
[plotly](https://plotly.com/python/); confidence intervals use
[scipy](https://scipy.org/).

## Denominator Generation

Build denominator cohorts from observation periods, optionally stratified
by age, sex, and prior observation requirements.

::: omopy.incidence.generate_denominator_cohort_set

::: omopy.incidence.generate_target_denominator_cohort_set

## Core Estimation

Compute incidence rates and prevalence proportions over calendar intervals
with confidence intervals, washout logic, and strata support.

::: omopy.incidence.estimate_incidence

::: omopy.incidence.estimate_point_prevalence

::: omopy.incidence.estimate_period_prevalence

## Result Conversion

Pivot long-form `SummarisedResult` objects into wide tidy DataFrames with
named columns for each estimate.

::: omopy.incidence.as_incidence_result

::: omopy.incidence.as_prevalence_result

## Table Functions

Thin wrappers around `vis_omop_table()` with epidemiological formatting
defaults.

::: omopy.incidence.table_incidence

::: omopy.incidence.table_prevalence

::: omopy.incidence.table_incidence_attrition

::: omopy.incidence.table_prevalence_attrition

::: omopy.incidence.options_table_incidence

::: omopy.incidence.options_table_prevalence

## Plot Functions

Wrappers around `scatter_plot()` and `bar_plot()` with epidemiological
defaults.

::: omopy.incidence.plot_incidence

::: omopy.incidence.plot_prevalence

::: omopy.incidence.plot_incidence_population

::: omopy.incidence.plot_prevalence_population

## Grouping Helpers

Discover available grouping columns for faceted plots.

::: omopy.incidence.available_incidence_grouping

::: omopy.incidence.available_prevalence_grouping

## Mock Data & Benchmarking

::: omopy.incidence.mock_incidence_prevalence

::: omopy.incidence.benchmark_incidence_prevalence
