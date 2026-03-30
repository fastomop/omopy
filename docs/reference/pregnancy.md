# omopy.pregnancy

Pregnancy episode identification using the HIPPS algorithm — identify
pregnancy episodes from OMOP CDM data, summarise findings, and visualise
results.

This module is the Python equivalent of the R `PregnancyIdentifier`
package. Table rendering delegates to [`omopy.vis`](vis.md); plot rendering
uses [plotly](https://plotly.com/python/).

## Constants

::: omopy.pregnancy.OUTCOME_CATEGORIES

## Core Types

Pydantic model for storing pregnancy identification results.

::: omopy.pregnancy.PregnancyResult

## Pregnancy Identification

Run the HIPPS algorithm to identify pregnancy episodes.

::: omopy.pregnancy.identify_pregnancies

## Summarise Functions

Aggregate pregnancy episodes into a standardised `SummarisedResult`.

::: omopy.pregnancy.summarise_pregnancies

## Table Functions

Format summarised results as publication-ready tables using
`omopy.vis.vis_omop_table()`.

::: omopy.pregnancy.table_pregnancies

## Plot Functions

Visualise pregnancy outcomes, gestational age, and timelines.

::: omopy.pregnancy.plot_pregnancies

## Validation & Mock Data

::: omopy.pregnancy.validate_episodes

::: omopy.pregnancy.mock_pregnancy_cdm
