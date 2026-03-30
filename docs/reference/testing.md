# omopy.testing

Test data generation for OMOP CDM studies — read patient data from
Excel/CSV, validate against CDM specifications, construct CdmReference
objects, and generate mock test databases.

This module is the Python equivalent of the R `TestGenerator` package.
Excel I/O uses [openpyxl](https://openpyxl.readthedocs.io/); cohort
timeline plots use [plotly](https://plotly.com/python/).

## Read & Validate

Read patient data from files and validate against CDM specifications.

::: omopy.testing.read_patients

::: omopy.testing.validate_patient_data

## CDM Construction

Build CdmReference objects from JSON test definitions or synthetic data.

::: omopy.testing.patients_cdm

::: omopy.testing.mock_test_cdm

## Template Generation

Generate blank Excel templates with CDM-compliant column headers.

::: omopy.testing.generate_test_tables

## Visualization

Plot cohort membership timelines for individual patients.

::: omopy.testing.graph_cohort
