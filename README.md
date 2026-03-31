# OMOPy

[![CI](https://github.com/darwin-eu/omopy/actions/workflows/ci.yml/badge.svg)](https://github.com/darwin-eu/omopy/actions/workflows/ci.yml)
[![Docs](https://github.com/darwin-eu/omopy/actions/workflows/docs.yml/badge.svg)](https://darwin-eu.github.io/omopy)
[![PyPI](https://img.shields.io/pypi/v/omopy)](https://pypi.org/project/omopy/)
[![Python](https://img.shields.io/pypi/pyversions/omopy)](https://pypi.org/project/omopy/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Pythonic, type-safe interface for OMOP CDM databases.**

OMOPy is a single Python package that reimplements the
[DARWIN EU](https://www.darwin-eu.org/) R package ecosystem for working with
[OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) databases.
It provides lazy database access via [Ibis](https://ibis-project.org/),
type-safe data structures via [Pydantic](https://docs.pydantic.dev/) and
[Polars](https://pola.rs/), and a clean Pythonic API with full type hints.

## Background

The [DARWIN EU Coordination Centre](https://www.darwin-eu.org/) develops and
maintains an ecosystem of R packages for observational health research using
the OMOP CDM. These include
[CDMConnector](https://github.com/darwin-eu/CDMConnector),
[PatientProfiles](https://github.com/darwin-eu-dev/PatientProfiles),
[CohortCharacteristics](https://github.com/darwin-eu-dev/CohortCharacteristics),
[IncidencePrevalence](https://github.com/darwin-eu-dev/IncidencePrevalence),
[DrugUtilisation](https://github.com/darwin-eu-dev/DrugUtilisation),
[CohortSurvival](https://github.com/darwin-eu-dev/CohortSurvival),
[TreatmentPatterns](https://github.com/darwin-eu-dev/TreatmentPatterns),
and others.

OMOPy consolidates these ~17 R packages into a single Python library, bringing
the DARWIN EU analytical toolkit to the Python data science ecosystem. The
package preserves the conceptual model and analytical capabilities of the R
packages while providing a Pythonic API that follows Python conventions and
leverages modern Python tooling.

## Features

- **Single package** — one `pip install omopy` replaces 17 R packages
- **Lazy by default** — Ibis constructs SQL queries; nothing executes until
  you call `.collect()`
- **Type-safe** — Pydantic models with frozen immutability; full type
  annotations throughout
- **Pythonic** — snake_case, context managers, keyword arguments, no R idioms
- **Database-agnostic** — DuckDB, PostgreSQL, SQL Server, Snowflake, BigQuery,
  and more via Ibis backends

## Modules

| Module | R Equivalent | Description |
|--------|-------------|-------------|
| `omopy.generics` | omopgenerics | Core type system: CDM references, tables, codelists, summarised results |
| `omopy.connector` | CDMConnector | Database connections, CDM loading, cohort generation, CIRCE engine |
| `omopy.profiles` | PatientProfiles | Patient-level enrichment: demographics, intersections, death |
| `omopy.codelist` | CodelistGenerator | Vocabulary search, hierarchy traversal, codelist operations |
| `omopy.vis` | visOmopResults | Format, tabulate, and plot summarised results |
| `omopy.characteristics` | CohortCharacteristics | Cohort characterization: summarise, tabulate, plot |
| `omopy.incidence` | IncidencePrevalence | Incidence rates and prevalence proportions |
| `omopy.drug` | DrugUtilisation | Drug cohort generation, utilisation metrics, dose analysis |
| `omopy.survival` | CohortSurvival | Kaplan-Meier survival, competing risks, survival plots |
| `omopy.treatment` | TreatmentPatterns | Treatment pathway analysis, Sankey and sunburst plots |
| `omopy.drug_diagnostics` | DrugExposureDiagnostics | Drug exposure quality checks and diagnostics |
| `omopy.pregnancy` | PregnancyIdentifier | Pregnancy episode identification (HIPPS algorithm) |
| `omopy.testing` | TestGenerator | Test data generation for OMOP CDM studies |

## Installation

```bash
pip install omopy
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add omopy
```

### Requirements

- **Python >= 3.14**
- A database with OMOP CDM v5.3 or v5.4 tables

### Optional database backends

```bash
pip install omopy[postgres]    # PostgreSQL via psycopg
pip install omopy[mssql]       # SQL Server via pyodbc
pip install omopy[snowflake]   # Snowflake
pip install omopy[bigquery]    # Google BigQuery
pip install omopy[all]         # All backends
```

## Quick Start

```python
from omopy.connector import cdm_from_con, generate_concept_cohort_set
from omopy.generics import Codelist

# Connect to a DuckDB OMOP CDM database
cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="main")

# Define a concept-based cohort
codelist = Codelist({"hypertension": [320128]})
cdm = generate_concept_cohort_set(cdm, codelist, name="hypertension")

# Enrich with demographics
from omopy.profiles import add_demographics
enriched = add_demographics(cdm["hypertension"], cdm)

# Collect to a Polars DataFrame
df = enriched.collect()
print(df)
```

```python
# Characterise the cohort
from omopy.characteristics import summarise_characteristics, table_characteristics

result = summarise_characteristics(cdm["hypertension"])
table_characteristics(result, type="gt")
```

```python
# Estimate incidence
from omopy.incidence import (
    generate_denominator_cohort_set,
    estimate_incidence,
    plot_incidence,
)

cdm = generate_denominator_cohort_set(cdm, name="denominator")
inc = estimate_incidence(cdm, "denominator", "hypertension", interval="years")
plot_incidence(inc)
```

## Notebooks

The `notebooks/` directory contains 12 fully executable Jupyter notebooks
demonstrating every major capability:

| Notebook | Topic |
|----------|-------|
| `01_getting_started` | CDM connection, tables, snapshot, subsetting |
| `02_codelist_generation` | Vocabulary search, hierarchy, codelist operations |
| `03_cohort_generation` | Concept-based and CIRCE/JSON cohort generation |
| `04_cohort_characteristics` | Summarise, tabulate, and plot cohort characteristics |
| `05_patient_profiles` | Demographics, intersects, categories, enrichment |
| `06_incidence_prevalence` | Denominator generation, incidence and prevalence |
| `07_drug_utilisation` | Drug cohorts, utilisation metrics, indication |
| `08_cohort_survival` | Single-event and competing-risk survival analysis |
| `09_treatment_patterns` | Treatment pathways, Sankey and sunburst plots |
| `10_drug_exposure_diagnostics` | Drug exposure quality checks |
| `11_pregnancy_analysis` | HIPPS pregnancy identification algorithm |
| `12_visualisation_styling` | Table and plot formatting, styles |

## Documentation

Full API documentation is available at
**[darwin-eu.github.io/omopy](https://darwin-eu.github.io/omopy)**.

## Development

```bash
# Clone and install
git clone https://github.com/darwin-eu/omopy.git
cd omopy
uv sync --all-extras --dev

# Run tests (1619 tests)
uv run pytest

# Run linting
uv run ruff check src/ tests/

# Build documentation
uv run python docs/_build.py build --strict

# Install pre-commit hooks
uv run pre-commit install
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide,
including the code style guide, type hint requirements, and pull request
process.

## About DARWIN EU

[DARWIN EU](https://www.darwin-eu.org/) (Data Analysis and Real-World
Interrogation Network) is a federated network of data, expertise, and
services for generating reliable evidence on the real-world safety and
effectiveness of medicines. It is coordinated by the
[DARWIN EU Coordination Centre](https://www.darwin-eu.org/) and supports
regulatory decision-making by the
[European Medicines Agency (EMA)](https://www.ema.europa.eu/).

OMOPy builds on the analytical methods and tooling developed by the DARWIN EU
community and the broader [OHDSI](https://www.ohdsi.org/) (Observational
Health Data Sciences and Informatics) network. The
[OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) provides
the standardised data structure that underpins all analyses.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.
