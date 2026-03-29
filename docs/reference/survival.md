# omopy.survival

Cohort survival analysis — Kaplan-Meier estimation, competing risk
cumulative incidence, risk tables, and survival plots.

This module is the Python equivalent of the R `CohortSurvival` package.
Kaplan-Meier estimation uses [lifelines](https://lifelines.readthedocs.io/);
competing risk analysis uses a custom Aalen-Johansen implementation.
Table rendering delegates to [`omopy.vis`](vis.md); plot rendering uses
[plotly](https://plotly.com/python/).

## Core Estimation

Compute survival curves, summary statistics, risk tables, and attrition
from target and outcome cohorts.

::: omopy.survival.estimate_single_event_survival

::: omopy.survival.estimate_competing_risk_survival

## Add Survival Columns

Enrich a cohort table with survival time and event status columns.

::: omopy.survival.add_cohort_survival

## Result Conversion

Convert a long-format `SummarisedResult` into structured wide-format
DataFrames for estimates, events, summary, and attrition.

::: omopy.survival.as_survival_result

## Table Functions

Format survival results as publication-ready tables using
`omopy.vis.vis_omop_table()`.

::: omopy.survival.table_survival

::: omopy.survival.table_survival_events

::: omopy.survival.table_survival_attrition

::: omopy.survival.options_table_survival

## Plot Functions

Kaplan-Meier and cumulative incidence curves with confidence interval
ribbons, risk tables, and faceting support.

::: omopy.survival.plot_survival

::: omopy.survival.available_survival_grouping

## Mock Data

::: omopy.survival.mock_survival
