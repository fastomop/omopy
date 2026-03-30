# omopy.treatment

Treatment pathway analysis — compute sequential treatment pathways from
OMOP CDM cohort data, summarise frequencies and durations, and visualise
results as Sankey diagrams, sunburst charts, and box plots.

This module is the Python equivalent of the R `TreatmentPatterns` package.
Plot rendering uses [plotly](https://plotly.com/python/); table rendering
delegates to [`omopy.vis`](vis.md).

## Core Types

Pydantic models for defining cohort roles and storing pathway results.

::: omopy.treatment.CohortSpec

::: omopy.treatment.PathwayResult

## Pathway Computation

Compute sequential treatment pathways from cohort data.

::: omopy.treatment.compute_pathways

## Summarise Functions

Aggregate pathway results into frequencies and duration statistics.

::: omopy.treatment.summarise_treatment_pathways

::: omopy.treatment.summarise_event_duration

## Table Functions

Format summarised results as publication-ready tables using
`omopy.vis.vis_omop_table()`.

::: omopy.treatment.table_treatment_pathways

::: omopy.treatment.table_event_duration

## Plot Functions

Sankey diagrams, sunburst charts, and event duration box plots.

::: omopy.treatment.plot_sankey

::: omopy.treatment.plot_sunburst

::: omopy.treatment.plot_event_duration

## Mock Data

::: omopy.treatment.mock_treatment_pathways
