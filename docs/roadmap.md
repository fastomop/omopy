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
| 6 | **CohortCharacteristics** | Analytics R package | ~36 | Planned | `omopy.characteristics` |
| 7 | **IncidencePrevalence** | Analytics R package | ~29 | Planned | `omopy.incidence` |
| 8 | **DrugUtilisation** | Analytics R package | ~57 | Planned | `omopy.drug` |
| 9 | **CohortSurvival** | Analytics R package | ~21 | Planned | `omopy.survival` |
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

- **Already implemented (5):** omopgenerics, CDMConnector, PatientProfiles, CodelistGenerator, visOmopResults
- **Candidates for rewrite (8):** CohortCharacteristics, IncidencePrevalence, DrugUtilisation, CohortSurvival, TreatmentPatterns, DrugExposureDiagnostics, PregnancyIdentifier, TestGenerator
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
  CohortCharacteristics           → omopy.characteristics
    └── depends on: omopgenerics, CDMConnector, PatientProfiles
    └── suggests: visOmopResults, CodelistGenerator

  IncidencePrevalence             → omopy.incidence
    └── depends on: omopgenerics, CDMConnector, PatientProfiles
    └── suggests: visOmopResults

  DrugUtilisation                 → omopy.drug
    └── depends on: omopgenerics, PatientProfiles, CodelistGenerator
    └── suggests: CDMConnector, visOmopResults, CohortSurvival

  CohortSurvival                  → omopy.survival
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

Already implemented with 890 tests:

| Phase | Module | R Equivalent | Tests |
|-------|--------|-------------|-------|
| 0 | `omopy.generics` | omopgenerics | 236 |
| 1+2 | `omopy.connector` | CDMConnector | 310 |
| 3A | `omopy.profiles` | PatientProfiles | 107 |
| 3B | `omopy.codelist` | CodelistGenerator | 122 |
| 3C | `omopy.vis` | visOmopResults | 115 |

---

### Phase 4: Cohort Characterization & Incidence/Prevalence

These two packages form the primary analytical layer. They depend only on
packages we have already implemented (Layers 0-3). They can be worked on
in parallel since they do not depend on each other.

#### Phase 4A: `omopy.characteristics` (CohortCharacteristics)

**R package:** 36 exports (7 summarise, 8 table, 7 plot, 14 utilities/re-exports)

**Scope:**

- **Summarise functions (7):** These are the core analytical functions. Each queries the CDM
  and produces a `SummarisedResult`:
    - `summarise_characteristics()` — demographics, counts, clinical variables
    - `summarise_cohort_count()` — record and subject counts per cohort
    - `summarise_cohort_attrition()` — step-by-step filtering flow
    - `summarise_cohort_timing()` — time intervals between cohort entries
    - `summarise_cohort_overlap()` — subject overlap across cohorts
    - `summarise_large_scale_characteristics()` — prevalence of conditions/drugs/etc. in time windows
    - `summarise_cohort_codelist()` — codelist usage within cohorts

- **Table/plot wrappers (15):** These delegate to `omopy.vis` for rendering.
  Each takes a `SummarisedResult` and produces a formatted table or plotly figure.
  These are thin wrappers that configure `vis_omop_table()` / plotly with
  domain-specific defaults.

- **Utilities (14):** Most are re-exports from `omopy.generics` that we already
  have. Mock data generator and benchmark utility are new.

**Estimated effort:** Medium-large. The summarise functions contain the real logic,
using `omopy.profiles` for demographics and intersections. Table/plot wrappers are
straightforward delegations to `omopy.vis`.

**Dependencies satisfied:** All (generics, connector, profiles, codelist, vis).

**Estimated size:** ~2,000-3,000 lines of source, ~150-200 tests.

#### Phase 4B: `omopy.incidence` (IncidencePrevalence)

**R package:** 29 exports (5 core estimation, 8 table/plot, 4 result handling, 12 utilities)

**Scope:**

- **Core estimation (5):**
    - `generate_denominator_cohort_set()` — creates denominator cohorts stratified by age/sex/observation
    - `generate_target_denominator_cohort_set()` — variant with target cohort
    - `estimate_incidence()` — computes incidence rates with CIs
    - `estimate_point_prevalence()` — prevalence at specific time points
    - `estimate_period_prevalence()` — prevalence over intervals

- **Result handling (4):** Validation/coercion/querying of result objects.

- **Table/plot (8):** Wrappers around `omopy.vis` with epidemiological defaults.

- **Utilities (12):** Mock data, benchmarks, re-exports.

**Estimated effort:** Large. The denominator generation and estimation logic are
computationally non-trivial — they involve complex date arithmetic, stratification,
washout periods, and confidence interval computation. The epidemiological
statistics (incidence rates, prevalence proportions) need careful implementation.

**Dependencies satisfied:** All (generics, connector, profiles).

**Key Python library:** May use `scipy.stats` for confidence intervals.

**Estimated size:** ~3,000-4,000 lines of source, ~200-300 tests.

---

### Phase 5: Drug Analytics & Survival

These packages depend on Layer 4 or are parallel to it.

#### Phase 5A: `omopy.drug` (DrugUtilisation)

**R package:** 57 exports (the largest package in the ecosystem)

**Scope:**

- **Cohort generation (4):** Generate drug cohorts by ingredient, ATC code, or general criteria. Era-fy cohorts (merge nearby records).
- **Cohort requirements (4):** Filter cohorts by first entry, observation, washout, date range.
- **Add columns (15):** Compute and add drug utilisation metrics: daily dose, cumulative dose/quantity, days exposed/prescribed, number of eras/exposures, time-to-exposure, indication, drug restart, treatment patterns.
- **Summarise (6):** Aggregate drug utilisation, indication, PPC adherence, restart/switch, treatment, dose coverage.
- **Table/plot (11):** Visualisation wrappers.
- **Utilities (17):** Mock data, benchmarks, dose pattern tables, re-exports.

**Dependencies:** omopgenerics, PatientProfiles, CodelistGenerator (all done).
CDMConnector (done, soft dep). visOmopResults (done, for table/plot). CohortSurvival (soft dep for some analyses).

**Estimated effort:** Very large. This is the most complex package. Drug dose
calculation requires pattern-matching tables and unit conversions. The 15
"add column" functions each have non-trivial SQL/computation logic.

**Key challenge:** Dose pattern tables for mapping drug_exposure records to
standardised daily doses. These are based on OMOP vocabulary drug_strength
lookups and unit conversions.

**Estimated size:** ~4,000-6,000 lines of source, ~250-350 tests.

#### Phase 5B: `omopy.survival` (CohortSurvival)

**R package:** 21 exports

**Scope:**

- **Core estimation (3):**
    - `estimate_single_event_survival()` — Kaplan-Meier survival from target/outcome cohorts
    - `estimate_competing_risk_survival()` — cumulative incidence with competing risks
    - `add_cohort_survival()` — add survival time/event columns to a cohort table

- **Visualization (5):** Survival curves, risk tables, attrition tables.
- **Utilities (13):** Mock data, validation, re-exports.

**Dependencies:** omopgenerics, CDMConnector, PatientProfiles (all done).
R's `survival` package → Python's `lifelines` library.

**Estimated effort:** Medium. The Kaplan-Meier logic is well-defined and
`lifelines` provides the statistical foundation. The main work is in
building the cohort-to-survival-data pipeline and the visualization
wrappers.

**Key Python library:** `lifelines` for Kaplan-Meier, cumulative incidence.

**Estimated size:** ~1,500-2,500 lines of source, ~100-150 tests.

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
| 0 | `omopy.generics` | 3,400 | 236 | **Done** |
| 1+2 | `omopy.connector` | 6,200 | 310 | **Done** |
| 3A | `omopy.profiles` | 3,700 | 107 | **Done** |
| 3B | `omopy.codelist` | 1,400 | 122 | **Done** |
| 3C | `omopy.vis` | 1,200 | 115 | **Done** |
| 4A | `omopy.characteristics` | 2,000-3,000 | 150-200 | Planned |
| 4B | `omopy.incidence` | 3,000-4,000 | 200-300 | Planned |
| 5A | `omopy.drug` | 4,000-6,000 | 250-350 | Planned |
| 5B | `omopy.survival` | 1,500-2,500 | 100-150 | Planned |
| 6A | `omopy.treatment` | 1,500-2,000 | 80-120 | Planned |
| 6B | `omopy.drug_diagnostics` | 800-1,200 | 60-80 | Planned |
| 7A | `omopy.pregnancy` | 2,000-3,000 | 120-180 | Planned |
| 8A | `omopy.testing` | 800-1,200 | 50-80 | Planned |
| | **Total (done)** | **~16,000** | **890** | |
| | **Total (planned)** | **~15,600-22,900** | **~1,010-1,460** | |
| | **Grand total** | **~31,600-38,900** | **~1,900-2,350** | |

---

## Recommended Execution Order

```
Now ─────────────────────────────────────────────────────────────────►

Phase 4A: characteristics  ──┐
                              ├──► Phase 5A: drug ──► Phase 6B: drug_diagnostics
Phase 4B: incidence ─────────┤
                              ├──► Phase 7A: pregnancy
Phase 5B: survival ──────────┘

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
