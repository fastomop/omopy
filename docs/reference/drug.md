# omopy.drug

Drug utilisation analysis — generate drug cohorts, compute utilisation
metrics (exposures, eras, doses, days), assess indications and treatment
patterns, and present results as tables and plots.

This module is the Python equivalent of the R `DrugUtilisation` package.
Table rendering delegates to [`omopy.vis`](vis.md); plot rendering uses
[plotly](https://plotly.com/python/); confidence intervals use
[scipy](https://scipy.org/).

## Cohort Generation

Generate drug cohorts from concept sets, ingredient names, or ATC codes.
Records are collapsed into eras using a configurable gap.

::: omopy.drug.generate_drug_utilisation_cohort_set

::: omopy.drug.generate_ingredient_cohort_set

::: omopy.drug.generate_atc_cohort_set

::: omopy.drug.erafy_cohort

::: omopy.drug.cohort_gap_era

## Daily Dose

Compute standardised daily doses from the `drug_strength` table using
pattern-matched formulas and unit conversions.

::: omopy.drug.add_daily_dose

::: omopy.drug.pattern_table

## Requirement / Filter Functions

Filter drug cohorts by temporal or logical criteria. Each function records
attrition in the cohort metadata.

::: omopy.drug.require_is_first_drug_entry

::: omopy.drug.require_prior_drug_washout

::: omopy.drug.require_observation_before_drug

::: omopy.drug.require_drug_in_date_range

## Add Drug Use Metrics

Enrich a drug cohort with computed utilisation columns. All functions
return a new `CohortTable` with additional columns.

::: omopy.drug.add_drug_utilisation

::: omopy.drug.add_number_exposures

::: omopy.drug.add_number_eras

::: omopy.drug.add_days_exposed

::: omopy.drug.add_days_prescribed

::: omopy.drug.add_time_to_exposure

::: omopy.drug.add_initial_exposure_duration

::: omopy.drug.add_initial_quantity

::: omopy.drug.add_cumulative_quantity

::: omopy.drug.add_initial_daily_dose

::: omopy.drug.add_cumulative_dose

::: omopy.drug.add_drug_restart

## Add Intersect

Enrich a cohort with indication or treatment flags from other cohorts.

::: omopy.drug.add_indication

::: omopy.drug.add_treatment

## Summarise Functions

Aggregate drug utilisation data into `SummarisedResult` objects.

::: omopy.drug.summarise_drug_utilisation

::: omopy.drug.summarise_indication

::: omopy.drug.summarise_treatment

::: omopy.drug.summarise_drug_restart

::: omopy.drug.summarise_dose_coverage

::: omopy.drug.summarise_proportion_of_patients_covered

## Table Functions

Thin wrappers around `vis_omop_table()` with domain-specific defaults
for estimate formatting, headers, and grouping.

::: omopy.drug.table_drug_utilisation

::: omopy.drug.table_indication

::: omopy.drug.table_treatment

::: omopy.drug.table_drug_restart

::: omopy.drug.table_dose_coverage

::: omopy.drug.table_proportion_of_patients_covered

## Plot Functions

Wrappers around `bar_plot()`, `scatter_plot()`, and `box_plot()` with
drug-utilisation-specific defaults.

::: omopy.drug.plot_drug_utilisation

::: omopy.drug.plot_indication

::: omopy.drug.plot_treatment

::: omopy.drug.plot_drug_restart

::: omopy.drug.plot_proportion_of_patients_covered

## Mock Data & Benchmarking

::: omopy.drug.mock_drug_utilisation

::: omopy.drug.benchmark_drug_utilisation
