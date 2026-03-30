---
hide:
  - navigation
---

# OMOPy

**Pythonic, type-safe interface for OMOP CDM databases.**

OMOPy is a single Python package that reimplements the DARWIN-EU R package
ecosystem for working with OMOP Common Data Model databases. It provides
lazy database access via [Ibis](https://ibis-project.org/), type-safe data
structures via [Pydantic](https://docs.pydantic.dev/) and
[Polars](https://pola.rs/), and a clean Pythonic API with full type hints.

## Modules

| Module | R Equivalent | Description |
|--------|-------------|-------------|
| [`omopy.generics`](reference/generics.md) | omopgenerics | Core type system: CDM references, tables, codelists, summarised results |
| [`omopy.connector`](reference/connector.md) | CDMConnector | Database connections, CDM loading, cohort generation, CIRCE engine |
| [`omopy.profiles`](reference/profiles.md) | PatientProfiles | Patient-level enrichment: demographics, intersections, death |
| [`omopy.codelist`](reference/codelist.md) | CodelistGenerator | Vocabulary search, hierarchy traversal, codelist operations |
| [`omopy.vis`](reference/vis.md) | visOmopResults | Format, tabulate, and plot summarised results |
| [`omopy.characteristics`](reference/characteristics.md) | CohortCharacteristics | Cohort characterization: summarise, tabulate, plot |
| [`omopy.incidence`](reference/incidence.md) | IncidencePrevalence | Incidence rates and prevalence proportions |
| [`omopy.drug`](reference/drug.md) | DrugUtilisation | Drug cohort generation, utilisation metrics, dose analysis |
| [`omopy.survival`](reference/survival.md) | CohortSurvival | Kaplan-Meier survival, competing risks, survival plots |

## Quick Example

```python
from omopy.connector import cdm_from_con, generate_concept_cohort_set
from omopy.generics import Codelist

# Connect to a DuckDB OMOP CDM database
cdm = cdm_from_con("path/to/omop.duckdb", cdm_schema="cdm")

# Define a concept-based cohort
codelist = Codelist({"hypertension": [320128]})
cdm = generate_concept_cohort_set(cdm, codelist, name="hypertension_cohort")

# Enrich with demographics
from omopy.profiles import add_demographics
result = add_demographics(cdm["hypertension_cohort"], cdm)

# Collect to a Polars DataFrame
df = result.collect()
print(df)
```

## Design Principles

- **Single package** — one `pip install omopy` replaces 17 R packages
- **Lazy by default** — Ibis constructs SQL queries; nothing executes until you call `.collect()`
- **Type-safe** — Pydantic models with frozen immutability; full type annotations throughout
- **Pythonic** — snake_case, context managers, keyword arguments, no R idioms
- **Database-agnostic** — DuckDB, PostgreSQL, SQL Server, Snowflake, BigQuery, and more via Ibis backends

## Requirements

- Python >= 3.14
- A database with OMOP CDM v5.3 or v5.4 tables

## Status

| Phase | Module | Status |
|-------|--------|--------|
| Phase 0 | `omopy.generics` | Complete (236 tests) |
| Phase 1+2 | `omopy.connector` | Complete (292 tests) |
| Phase 3A | `omopy.profiles` | Complete (122 tests) |
| Phase 3B | `omopy.codelist` | Complete (122 tests) |
| Phase 3C | `omopy.vis` | Complete (115 tests) |
| Phase 4A | `omopy.characteristics` | Complete (73 tests) |
| Phase 4B | `omopy.incidence` | Complete (86 tests) |
| Phase 5A | `omopy.drug` | Complete (101 tests) |
| Phase 5B | `omopy.survival` | Complete (80 tests) |

**Total: 1227 tests, all passing.**
