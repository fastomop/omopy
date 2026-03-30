# omopy.drug_diagnostics

Drug exposure diagnostics — run configurable quality checks on
`drug_exposure` records for specified ingredient concepts, summarise
findings, and visualise results.

This module is the Python equivalent of the R `DrugExposureDiagnostics`
package. Table rendering delegates to [`omopy.vis`](vis.md); plot rendering
uses [plotly](https://plotly.com/python/).

## Constants

::: omopy.drug_diagnostics.AVAILABLE_CHECKS

## Core Types

Pydantic model for storing diagnostic check results.

::: omopy.drug_diagnostics.DiagnosticsResult

## Diagnostic Computation

Run one or more diagnostic checks on drug exposure records.

::: omopy.drug_diagnostics.execute_checks

## Summarise Functions

Aggregate diagnostic results into a standardised `SummarisedResult`.

::: omopy.drug_diagnostics.summarise_drug_diagnostics

## Table Functions

Format summarised results as publication-ready tables using
`omopy.vis.vis_omop_table()`.

::: omopy.drug_diagnostics.table_drug_diagnostics

## Plot Functions

Visualise diagnostic results as bar charts and box plots.

::: omopy.drug_diagnostics.plot_drug_diagnostics

## Mock Data & Benchmarking

::: omopy.drug_diagnostics.mock_drug_exposure

::: omopy.drug_diagnostics.benchmark_drug_diagnostics
