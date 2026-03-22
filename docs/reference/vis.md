# omopy.vis

Visualization and formatting for OMOP CDM summarised results — format
estimates, render tables, and create plots from `SummarisedResult` objects.

This module is the Python equivalent of the R `visOmopResults` package.
Table rendering uses [great_tables](https://posit-dev.github.io/great-tables/);
plot rendering uses [plotly](https://plotly.com/python/).

## Format Pipeline

Composable functions for transforming summarised results into display-ready data.
The typical pipeline is:
`format_estimate_value → format_estimate_name → format_header → format_table`.

::: omopy.vis.format_estimate_value

::: omopy.vis.format_estimate_name

::: omopy.vis.format_header

::: omopy.vis.format_min_cell_count

## Tidy Helpers

::: omopy.vis.tidy_result

::: omopy.vis.tidy_columns

## High-Level Tables

::: omopy.vis.vis_omop_table

::: omopy.vis.vis_table

::: omopy.vis.format_table

## Plots

::: omopy.vis.scatter_plot

::: omopy.vis.bar_plot

::: omopy.vis.box_plot

## Style Configuration

::: omopy.vis.TableStyle

::: omopy.vis.PlotStyle

::: omopy.vis.customise_text

::: omopy.vis.default_table_style

::: omopy.vis.default_plot_style

## Mock Data

::: omopy.vis.mock_summarised_result
