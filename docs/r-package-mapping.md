# R Package Mapping

OMOPy reimplements the [OHDSI](https://github.com/OHDSI) / DARWIN-EU R
package ecosystem as a **single Python monorepo package**. The table below
shows how each R package maps to an OMOPy module.

For the full development history, design decisions, and technical details
behind each module, see the [Audit Trail](audit-trail.md).

## Package ↔ Module mapping

| OHDSI R Package | OMOPy Module | Phase | Description |
|---|---|---|---|
| [omopgenerics](https://github.com/OHDSI/omopgenerics) | `omopy.generics` | 0 | Core type system — CDM schema, codelists, cohort tables, summarised results |
| [CDMConnector](https://github.com/OHDSI/CDMConnector) | `omopy.connector` | 1–2 | Database connection, CDM reference, cohort generation, CIRCE engine, subsetting, snapshots |
| [PatientProfiles](https://github.com/OHDSI/PatientProfiles) | `omopy.profiles` | 3A | Patient-level enrichment — demographics, intersections (flag/count/date/days), death |
| [CodelistGenerator](https://github.com/OHDSI/CodelistGenerator) | `omopy.codelist` | 3B | Vocabulary-based code list generation, hierarchy traversal, diagnostics |
| [visOmopResults](https://github.com/OHDSI/visOmopResults) | `omopy.vis` | 3C | Formatting, tabulation, and plotting of `SummarisedResult` objects |
| [CohortCharacteristics](https://github.com/OHDSI/CohortCharacteristics) | `omopy.characteristics` | 4A | Cohort characterisation — summarise, table, and plot functions for demographics, timing, overlap |
| [IncidencePrevalence](https://github.com/OHDSI/IncidencePrevalence) | `omopy.incidence` | 4B | Denominator generation, incidence rate and prevalence estimation with confidence intervals |
| [DrugUtilisation](https://github.com/OHDSI/DrugUtilisation) | `omopy.drug` | 5A | Drug cohort generation, daily dose, utilisation metrics, indication, treatment, dose coverage |
| [CohortSurvival](https://github.com/OHDSI/CohortSurvival) | `omopy.survival` | 5B | Kaplan-Meier and Aalen-Johansen competing-risk survival analysis |
| [TreatmentPatterns](https://github.com/OHDSI/TreatmentPatterns) | `omopy.treatment` | 6A | Sequential treatment pathway computation, Sankey/sunburst visualisation |
| [DrugExposureDiagnostics](https://github.com/OHDSI/DrugExposureDiagnostics) | `omopy.drug_diagnostics` | 6B | 12 diagnostic checks on drug exposure records (missingness, duration, dose, etc.) |
| [PregnancyIdentifier](https://github.com/OHDSI/PregnancyIdentifier) | `omopy.pregnancy` | 7A | HIPPS algorithm for pregnancy episode identification |
| [TestGenerator](https://github.com/OHDSI/TestGenerator) | `omopy.testing` | 8A | Synthetic OMOP CDM test data generation |

## Key technology differences

The table below summarises the main technology substitutions made in
the Python rewrite:

| Concern | R ecosystem | OMOPy (Python) |
|---|---|---|
| **Lazy SQL** | dbplyr | [Ibis](https://ibis-project.org/) |
| **DataFrames** | tibble / data.frame | [Polars](https://pola.rs/) |
| **Data models** | S4 classes / R6 | [Pydantic](https://docs.pydantic.dev/) `BaseModel` |
| **Plotting** | ggplot2 + plotly | [Plotly](https://plotly.com/python/) |
| **Tables** | gt | [great_tables](https://posit-dev.github.io/great-tables/) |
| **Survival** | survival (R) | [lifelines](https://lifelines.readthedocs.io/) + custom Aalen-Johansen |
| **Statistics** | stats (R) | [SciPy](https://scipy.org/) |
| **Package manager** | renv | [uv](https://docs.astral.sh/uv/) |

## Design philosophy

1. **Single package.** All 13 R packages are consolidated into one
   installable Python package (`pip install omopy`) with sub-modules.
2. **Clean-room implementation.** Code was written against specifications
   and documentation only — no R source code was consulted.
3. **Lazy by default.** Database queries are built as Ibis expressions
   and only executed when `.collect()` is called.
4. **Standardised output.** All analytics produce `SummarisedResult`
   objects (the Python equivalent of `summarised_result` in omopgenerics),
   enabling consistent downstream formatting, tabulation, and plotting.