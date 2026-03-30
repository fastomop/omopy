# Rewrite Roadmap

This document maps all 22 repositories in the
[darwin-eu](https://github.com/orgs/darwin-eu/repositories) GitHub organization,
classifies each one, and lays out a phased plan for incorporating them into the
**OMOPy** monorepo.

## Repository Inventory

### Complete Classification of All 22 Repositories

| # | Repository | Type | R Exports | Status | OMOPy Module |
|---|-----------|------|-----------|--------|-------------|
| 1 | **omopgenerics** | Core R package | ~39 | **Done** | `omopy.generics` |
| 2 | **CDMConnector** | Core R package | ~23 | **Done** | `omopy.connector` |
| 3 | **PatientProfiles** | Core R package | ~30 | **Done** | `omopy.profiles` |
| 4 | **CodelistGenerator** | Core R package | ~14 | **Done** | `omopy.codelist` |
| 5 | **visOmopResults** | Core R package | ~19 | **Done** | `omopy.vis` |
| 6 | **CohortCharacteristics** | Analytics R package | ~36 | **Done** | `omopy.characteristics` |
| 7 | **IncidencePrevalence** | Analytics R package | ~29 | **Done** | `omopy.incidence` |
| 8 | **DrugUtilisation** | Analytics R package | ~57 | **Done** | `omopy.drug` |
| 9 | **CohortSurvival** | Analytics R package | ~21 | **Done** | `omopy.survival` |
| 10 | **TreatmentPatterns** | Analytics R package | ~10 | Planned | `omopy.treatment` |
| 11 | **DrugExposureDiagnostics** | Analytics R package | ~7 | Planned | `omopy.drug_diagnostics` |
| 12 | **PregnancyIdentifier** | Clinical R package | TBD | Planned | `omopy.pregnancy` |
| 13 | **TestGenerator** | Testing R package | TBD | Planned | `omopy.testing` |
| 14 | **DashboardExport** | Tooling (data export) | ~3 | Low priority | `omopy.export` (maybe) |
| 15 | **CdmOnboarding** | Tooling (QA/onboarding) | TBD | Partial candidate | `omopy.onboarding` (partial) |
| 16 | **DarwinBenchmark** | Tooling (benchmarking) | TBD | Later | `omopy.benchmark` (extend existing) |
| 17 | **EunomiaDatasets** | Data repository (CSV) | N/A | Consume as-is | Test fixtures |
| 18 | **DarwinShinyModules** | R Shiny UI library | N/A | **Out of scope** | — |
| 19 | **ReportGenerator** | R Shiny + Word reports | N/A | **Out of scope** | — |
| 20 | **execution-engine** | Platform (Java/TS/Docker) | N/A | **Out of scope** | — |
| 21 | **TestReleaseGitAction** | CI/CD tooling | N/A | **Out of scope** | — |
| 22 | **.github** | Org profile | N/A | **Out of scope** | — |

### Classification Summary

- **Already implemented (9):** omopgenerics, CDMConnector, PatientProfiles, CodelistGenerator, visOmopResults, CohortCharacteristics, IncidencePrevalence, DrugUtilisation, CohortSurvival
- **Candidates for rewrite (4):** TreatmentPatterns, DrugExposureDiagnostics, PregnancyIdentifier, TestGenerator
- **Low priority / partial (3):** DashboardExport, CdmOnboarding, DarwinBenchmark
- **Out of scope (6):** DarwinShinyModules, ReportGenerator, execution-engine, TestReleaseGitAction, .github, EunomiaDatasets (data only, consumed directly)

---

## R Dependency Graph

Understanding the dependency order is critical. Here is the dependency graph for the DARWIN-EU R packages:

```
Layer 0 (Foundation):
  omopgenerics                    → omopy.generics ✅

Layer 1 (Data Access):
  CDMConnector                    → omopy.connector ✅
    └── depends on: omopgenerics

Layer 2 (Patient-Level Computation):
  PatientProfiles                 → omopy.profiles ✅
    └── depends on: omopgenerics, CDMConnector

  CodelistGenerator               → omopy.codelist ✅
    └── depends on: omopgenerics, CDMConnector

Layer 3 (Visualization):
  visOmopResults                  → omopy.vis ✅
    └── depends on: omopgenerics

Layer 4 (Domain Analytics):
  CohortCharacteristics           → omopy.characteristics ✅
    └── depends on: omopgenerics, CDMConnector, PatientProfiles
    └── suggests: visOmopResults, CodelistGenerator

  IncidencePrevalence             → omopy.incidence ✅
    └── depends on: omopgenerics, CDMConnector, PatientProfiles
    └── suggests: visOmopResults

  DrugUtilisation                 → omopy.drug ✅
    └── depends on: omopgenerics, PatientProfiles, CodelistGenerator
    └── suggests: CDMConnector, visOmopResults, CohortSurvival

  CohortSurvival                  → omopy.survival ✅
    └── depends on: omopgenerics, CDMConnector, PatientProfiles
    └── depends on: survival (R package → lifelines in Python)
    └── suggests: visOmopResults, CodelistGenerator

  TreatmentPatterns               → omopy.treatment
    └── depends on: CDMConnector
    └── suggests: visOmopResults

Layer 5 (Specialized / Downstream):
  DrugExposureDiagnostics         → omopy.drug_diagnostics
    └── depends on: CDMConnector, omopgenerics, DrugUtilisation

  PregnancyIdentifier             → omopy.pregnancy
    └── depends on: CDMConnector, CohortCharacteristics,
        IncidencePrevalence, PatientProfiles, omopgenerics

Layer 6 (Testing / Tooling):
  TestGenerator                   → omopy.testing
    └── depends on: CDMConnector, omopgenerics
```

---

## Phased Rewrite Plan

### Phase 0-3: COMPLETE ✅

Already implemented with 887 tests:

| Phase | Module | R Equivalent | Tests |
|-------|--------|-------------|-------|
| 0 | `omopy.generics` | omopgenerics | 236 |
| 1+2 | `omopy.connector` | CDMConnector | 292 |
| 3A | `omopy.profiles` | PatientProfiles | 122 |
| 3B | `omopy.codelist` | CodelistGenerator | 122 |
| 3C | `omopy.vis` | visOmopResults | 115 |

---

### Phase 4A: `omopy.characteristics` — COMPLETE ✅

**R package:** CohortCharacteristics (23 exports: 7 summarise, 8 table, 7 plot, 1 mock)

**Implemented:**

- 7 summarise functions: `summarise_characteristics`, `summarise_cohort_count`, `summarise_cohort_attrition`, `summarise_cohort_timing`, `summarise_cohort_overlap`, `summarise_large_scale_characteristics`, `summarise_cohort_codelist`
- 8 table functions: wrappers around `vis_omop_table()` with domain-specific defaults
- 7 plot functions: wrappers around `bar_plot()`, `scatter_plot()`, `box_plot()`, plus custom Plotly attrition flowchart
- 1 mock function: `mock_cohort_characteristics()`
- Internal aggregation engine with variable classification, estimate computation, strata resolution
- Duplicate column detection in `summarise_characteristics()` (avoids Ibis errors when strata columns already exist)

**Tests: 73** (61 unit + 12 integration against Synthea database)

**Source: ~2,450 lines** across 4 files (`_summarise.py`, `_table.py`, `_plot.py`, `_mock.py`)

---

### Phase 4B: `omopy.incidence` — COMPLETE ✅

**R package:** IncidencePrevalence (21 exports: 2 denominator, 3 estimation, 2 result, 6 table, 4 plot, 2 grouping, 2 utility)

**Implemented:**

- 2 denominator functions: `generate_denominator_cohort_set`, `generate_target_denominator_cohort_set`
- 3 estimation functions: `estimate_incidence`, `estimate_point_prevalence`, `estimate_period_prevalence`
- 2 result conversion functions: `as_incidence_result`, `as_prevalence_result`
- 6 table functions: wrappers around `vis_omop_table()` with epidemiological defaults
- 4 plot functions: wrappers around `scatter_plot()` and `bar_plot()`
- 2 grouping helpers: `available_incidence_grouping`, `available_prevalence_grouping`
- 2 utility functions: `mock_incidence_prevalence`, `benchmark_incidence_prevalence`
- Full calendar interval engine (weeks/months/quarters/years/overall)
- Poisson exact CI for incidence, Wilson score CI for prevalence (via scipy)
- Outcome washout logic, censoring, complete database intervals
- Attrition tracking through denominator generation

**Tests: 86** (79 unit + 7 integration against Synthea database)

**Source: ~2,200 lines** across 6 files (`_denominator.py`, `_estimate.py`, `_result.py`, `_table.py`, `_plot.py`, `_mock.py`)

---

### Phase 5: Drug Analytics & Survival

These packages depend on Layer 4 or are parallel to it.

#### Phase 5A: `omopy.drug` (DrugUtilisation) — COMPLETE ✅

**R package:** 57 exports (the largest package in the ecosystem)

**Implemented:**

- **Cohort generation (5):** `generate_drug_utilisation_cohort_set`, `generate_ingredient_cohort_set`, `generate_atc_cohort_set`, `erafy_cohort`, `cohort_gap_era`
- **Daily dose (2):** `add_daily_dose`, `pattern_table`
- **Requirement/filter (4):** `require_is_first_drug_entry`, `require_prior_drug_washout`, `require_observation_before_drug`, `require_drug_in_date_range`
- **Add drug use metrics (12):** `add_drug_utilisation` (all-in-one), plus 11 individual metric functions for exposures, eras, days, quantities, doses, restart
- **Add intersect (2):** `add_indication`, `add_treatment`
- **Summarise (6):** Drug utilisation, indication, treatment, drug restart, dose coverage, proportion of patients covered
- **Table (6):** Wrappers around `vis_omop_table()` with domain-specific defaults
- **Plot (5):** Box plots, bar charts, stacked bars, line plots with CI ribbons
- **Utilities (2):** `mock_drug_utilisation`, `benchmark_drug_utilisation`
- **Drug strength pattern engine:** 41 patterns, 4 formulas, unit standardization
- **Era collapsing:** Gaps-and-islands algorithm with configurable gap_era

**Tests: 101** (67 unit + 34 integration against Synthea database)

**Source: ~5,900 lines** across 12 files

#### Phase 5B: `omopy.survival` (CohortSurvival) — COMPLETE ✅

**R package:** 21 exports (13 unique, 8 re-exports from omopgenerics)

**Implemented:**

- **Core estimation (2):** `estimate_single_event_survival` (Kaplan-Meier), `estimate_competing_risk_survival` (Aalen-Johansen CIF)
- **Add columns (1):** `add_cohort_survival` — enrich cohort with time/status columns
- **Result conversion (1):** `as_survival_result` — wide-format DataFrames
- **Table (4):** `table_survival`, `table_survival_events`, `table_survival_attrition`, `options_table_survival`
- **Plot (2):** `plot_survival` (KM/CIF curves with CI ribbons), `available_survival_grouping`
- **Mock (1):** `mock_survival` — synthetic CDM with target/outcome/competing cohorts

**Key Python library:** `lifelines` for Kaplan-Meier; custom Aalen-Johansen for competing risks.

**Tests: 80** (unit + integration against Synthea database)

**Source: ~2,548 lines** across 7 files (`_add_survival.py`, `_estimate.py`, `_result.py`, `_table.py`, `_plot.py`, `_mock.py`, `__init__.py`)

---

### Phase 6: Treatment Patterns & Drug Diagnostics

#### Phase 6A: `omopy.treatment` (TreatmentPatterns)

**R package:** 10 exports

**Scope:**

- **Core (2):** `compute_pathways()` and `execute_treatment_patterns()` — compute sequential treatment pathways from cohort data.
- **Export (2):** Export aggregate and patient-level results.
- **Visualization (4):** Sankey diagrams, sunburst plots, event duration plots.
- **Utilities (2):** Results constructor, data model specs.

**Dependencies:** CDMConnector (done). Does not depend on PatientProfiles,
CodelistGenerator, or other analytics packages directly.

**Estimated effort:** Medium. The pathway computation algorithm (determining
treatment sequences, handling overlaps and gaps) is the core complexity.
Sankey/sunburst visualizations can use plotly's Sankey trace.

**Key Python library:** `plotly` (Sankey diagram support built-in).

**Estimated size:** ~1,500-2,000 lines of source, ~80-120 tests.

#### Phase 6B: `omopy.drug_diagnostics` (DrugExposureDiagnostics)

**R package:** 7 exports

**Scope:**

- **Core (1):** `execute_checks()` — runs ~12 diagnostic checks on drug_exposure records for specified ingredients.
- **Utilities (3):** Mock data, write results to disk, benchmarking.
- **Interactive (2):** Shiny-based result viewer (will NOT be ported — use notebook/HTML output instead).

**Dependencies:** CDMConnector, omopgenerics (done). DrugUtilisation
(Phase 5A, for dose calculations). This is why it's in Phase 6.

**Estimated effort:** Small-medium. The diagnostic checks are mostly SQL
aggregations. The main dependency on DrugUtilisation is for dose-related checks.

**Estimated size:** ~800-1,200 lines of source, ~60-80 tests.

---

### Phase 7: Specialized Clinical Algorithms

#### Phase 7A: `omopy.pregnancy` (PregnancyIdentifier)

**R package:** Exports TBD (newly released)

**Scope:** Identify pregnancy episodes from OMOP CDM data using the HIPPS
algorithm. Map scattered pregnancy-related codes to structured episodes
with inferred start/end dates and outcome categories.

**Dependencies:** CDMConnector, PatientProfiles, omopgenerics (all done).
CohortCharacteristics (Phase 4A), IncidencePrevalence (Phase 4B).

**Estimated effort:** Medium-large. The HIPPS algorithm has complex logic
for episode identification, conflict resolution, and date inference.

**Estimated size:** ~2,000-3,000 lines of source, ~120-180 tests.

---

### Phase 8: Testing Infrastructure

#### Phase 8A: `omopy.testing` (TestGenerator)

**R package:** Exports TBD

**Scope:** Create deterministic test fixtures for OMOP CDM studies. Read
micro-populations from Excel/CSV, generate JSON test definitions, and
populate mock CDM databases. This is the testing infrastructure that
enables Python-based study development.

**Dependencies:** CDMConnector, omopgenerics (done).

**Estimated effort:** Small-medium. The core logic is data transformation
(Excel/CSV → CDM tables). Can leverage existing `data/synthea.duckdb`
patterns.

**Estimated size:** ~800-1,200 lines of source, ~50-80 tests.

---

### Not Planned for OMOPy Rewrite

These repositories are out of scope for the monorepo:

| Repository | Reason |
|-----------|--------|
| **DarwinShinyModules** | R Shiny UI library — technology-specific. Python equivalent would be Streamlit/Dash, a separate project. |
| **ReportGenerator** | R Shiny + Word document generation — technology-specific. |
| **execution-engine** | Java/TypeScript deployment platform — not an analytics library. |
| **TestReleaseGitAction** | CI/CD GitHub Action — org infrastructure. |
| **.github** | GitHub org profile. |
| **EunomiaDatasets** | Data-only repository. CSV files consumed directly by tests. |

### Low Priority / Partial

| Repository | Notes |
|-----------|-------|
| **DashboardExport** | Thin SQL wrapper for Achilles results. Could be a small utility function in `omopy.connector` or `omopy.export`. Very low complexity. |
| **CdmOnboarding** | SQL data extraction parts are portable. Word report generation and R-environment checks are not. Could be partially reimplemented as `omopy.onboarding` with HTML/Markdown output. |
| **DarwinBenchmark** | Depends on all analytics packages existing first. When Phases 4-7 are done, extend `omopy.connector.benchmark` to cover the full suite. |

---

## Estimated Total Effort

| Phase | Module | Est. Lines | Est. Tests | Status |
|-------|--------|-----------|-----------|--------|
| 0 | `omopy.generics` | 2,511 | 236 | **Done** |
| 1+2 | `omopy.connector` | 7,072 | 292 | **Done** |
| 3A | `omopy.profiles` | 3,737 | 122 | **Done** |
| 3B | `omopy.codelist` | 1,424 | 122 | **Done** |
| 3C | `omopy.vis` | 1,767 | 115 | **Done** |
| 4A | `omopy.characteristics` | 3,007 | 73 | **Done** |
| 4B | `omopy.incidence` | 3,315 | 86 | **Done** |
| 5A | `omopy.drug` | 6,297 | 101 | **Done** |
| 5B | `omopy.survival` | 2,548 | 80 | **Done** |
| 6A | `omopy.treatment` | 1,500-2,000 | 80-120 | Planned |
| 6B | `omopy.drug_diagnostics` | 800-1,200 | 60-80 | Planned |
| 7A | `omopy.pregnancy` | 2,000-3,000 | 120-180 | Planned |
| 8A | `omopy.testing` | 800-1,200 | 50-80 | Planned |
| | **Total (done)** | **~31,724** | **1,227** | |
| | **Total (planned)** | **~5,100-7,400** | **~310-460** | |
| | **Grand total** | **~36,824-39,124** | **~1,537-1,687** | |

---

## Recommended Execution Order

```
Now ─────────────────────────────────────────────────────────────────►

Phase 4A: characteristics ✅──┐
                               ├──► Phase 5A: drug ✅──► Phase 6B: drug_diagnostics
Phase 4B: incidence ✅────────┤
                               ├──► Phase 7A: pregnancy
Phase 5B: survival ✅─────────┘

Phase 6A: treatment (independent, can start anytime after Phase 2)

Phase 8A: testing (independent, can start anytime after Phase 2)
```

Phases 4A and 4B can run in **parallel** — they share the same dependencies
(generics, connector, profiles) but do not depend on each other.

Phase 5B (survival) can also start in parallel with Phase 4, since it only
depends on Layers 0-2 plus `lifelines`.

Phase 6A (treatment) is also independent — it only needs CDMConnector.

The critical path is:
**Phase 4A/4B → Phase 5A (drug) → Phase 6B (drug_diagnostics) → Phase 7A (pregnancy)**

---

## New Python Dependencies Per Phase

| Phase | New Dependencies |
|-------|-----------------|
| 4A | None (uses existing stack) |
| 4B | `scipy` (confidence intervals) |
| 5A | None (uses existing stack) |
| 5B | `lifelines` (Kaplan-Meier, cumulative incidence) |
| 6A | None (`plotly` Sankey already available) |
| 6B | None (uses existing stack + Phase 5A) |
| 7A | None (uses existing stack) |
| 8A | `openpyxl` (Excel reading) |
