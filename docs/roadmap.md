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
| 10 | **TreatmentPatterns** | Analytics R package | ~10 | **Done** | `omopy.treatment` |
| 11 | **DrugExposureDiagnostics** | Analytics R package | ~7 | **Done** | `omopy.drug_diagnostics` |
| 12 | **PregnancyIdentifier** | Clinical R package | ~14 | **Done** | `omopy.pregnancy` |
| 13 | **TestGenerator** | Testing R package | ~7 | **Done** | `omopy.testing` |
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

- **Already implemented (13):** omopgenerics, CDMConnector, PatientProfiles, CodelistGenerator, visOmopResults, CohortCharacteristics, IncidencePrevalence, DrugUtilisation, CohortSurvival, TreatmentPatterns, DrugExposureDiagnostics, PregnancyIdentifier, TestGenerator
- **Candidates for rewrite (0):** All planned packages implemented
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

  TreatmentPatterns               → omopy.treatment ✅
    └── depends on: CDMConnector
    └── suggests: visOmopResults

Layer 5 (Specialized / Downstream):
  DrugExposureDiagnostics         → omopy.drug_diagnostics ✅
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

#### Phase 6A: `omopy.treatment` (TreatmentPatterns) — COMPLETE ✅

**R package:** 10 exports

**Implemented:**

- **Core types (2):** `CohortSpec` (Pydantic model for cohort role definition),
  `PathwayResult` (Pydantic model for pipeline output)
- **Computation (1):** `compute_pathways()` — 6-step pipeline: ingest, treatment
  history, split events, era collapse, combination window, filter treatments
- **Summarise (2):** `summarise_treatment_pathways()` (path frequencies),
  `summarise_event_duration()` (duration statistics)
- **Table (2):** `table_treatment_pathways()`, `table_event_duration()` — wrappers
  around `vis_omop_table()`
- **Plot (3):** `plot_sankey()` (Sankey diagram), `plot_sunburst()` (sunburst chart),
  `plot_event_duration()` (box plot)
- **Mock (1):** `mock_treatment_pathways()` — synthetic SummarisedResult for testing

**Key algorithms:**

- **Era collapse** — Iterative merge of same-drug eras separated by ≤ N days
- **Combination window** — FRFS/LRFS overlap detection creating "A+B" combinations
- **Treatment filtering** — "first" (first per drug), "changes" (remove consecutive
  duplicates), or "all"

**Tests: 127** (109 unit + 18 integration against Synthea database)

**Source: ~2,596 lines** across 6 files (`_pathway.py`, `_summarise.py`, `_table.py`,
`_plot.py`, `_mock.py`, `__init__.py`)

---

#### Phase 6B: `omopy.drug_diagnostics` (DrugExposureDiagnostics) — COMPLETE ✅

**R package:** 7 exports

**Implemented:**

- **Core (3):** `AVAILABLE_CHECKS` constant (12 check names), `DiagnosticsResult`
  Pydantic model, `execute_checks()` — runs configurable diagnostic checks on
  drug_exposure records for specified ingredient concept IDs
- **Checks (12):** missing values, exposure duration, type, route, source concept,
  days supply, verbatim end date, dose coverage, sig, quantity, days between
  consecutive records, diagnostics summary
- **Summarise (1):** `summarise_drug_diagnostics()` — convert to SummarisedResult
- **Table (1):** `table_drug_diagnostics()` — wrapper around `vis_omop_table()`
- **Plot (1):** `plot_drug_diagnostics()` — bar charts and box plots per check type
- **Mock/benchmark (2):** `mock_drug_exposure()`, `benchmark_drug_diagnostics()`

**Key features:**

- Configurable sampling (random N records per ingredient, or all)
- Min cell count obscuring across all checks
- Descendant concept resolution via `concept_ancestor` table
- Dose check delegates to `omopy.drug.add_daily_dose()` pattern engine

**Tests: 80** (55 unit + 25 integration against Synthea database)

**Source: ~1,830 lines** across 5 files (`_checks.py`, `_summarise.py`,
`_table.py`, `_plot.py`, `_mock.py`)

---

### Phase 7: Specialized Clinical Algorithms

#### Phase 7A: `omopy.pregnancy` (PregnancyIdentifier) — COMPLETE ✅

**R package:** PregnancyIdentifier (v3.2.2, 14 exports)

**Implemented:**

- **Core pipeline (1):** `identify_pregnancies()` — Main entry point running full
  HIPPS algorithm (init → HIP → PPS → merge → ESD)
- **Result container (1):** `PregnancyResult` — Pydantic model holding episodes,
  hip_episodes, pps_episodes, merged_episodes, metadata
- **Summarise (1):** `summarise_pregnancies()` — Convert to SummarisedResult
- **Table (1):** `table_pregnancies()` — Wrapper around `vis_omop_table()`
- **Plot (1):** `plot_pregnancies()` — Outcome distribution, gestational age,
  timeline plots
- **Utilities (2):** `mock_pregnancy_cdm()`, `validate_episodes()`
- **Constants (1):** `OUTCOME_CATEGORIES` — 8 outcome category definitions

**Key algorithms:**

- **HIP** (outcome-anchored) — Two-pass algorithm locating pregnancy outcome codes
  and working backwards to estimate start dates
- **PPS** (gestational-timing) — Locates gestational age markers and estimates
  start from timing information
- **HIPPS merge** — Combines HIP and PPS episodes with conflict resolution
- **ESD** (Episode Start Date) — Refines start dates using LMP records and
  prenatal visit evidence

**Tests: 122** (106 unit + 16 integration against Synthea database)

**Source: ~2,318 lines** across 11 files

---

### Phase 8: Testing Infrastructure

#### Phase 8A: `omopy.testing` (TestGenerator) — COMPLETE ✅

**R package:** TestGenerator (v0.4.0, 7 exports)

**Implemented:**

- **Read/validate (2):** `read_patients()` (Excel/CSV → dict of DataFrames),
  `validate_patient_data()` (validate against CDM spec)
- **CDM construction (2):** `patients_cdm()` (JSON → Polars CdmReference),
  `mock_test_cdm()` (synthetic mock CDM)
- **Template generation (1):** `generate_test_tables()` (blank Excel templates)
- **Visualization (1):** `graph_cohort()` (Plotly cohort timeline)

**Tests: 63** (all unit, no database needed)

**Source: ~815 lines** across 5 files

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
| 6A | `omopy.treatment` | 2,596 | 127 | **Done** |
| 6B | `omopy.drug_diagnostics` | 1,830 | 80 | **Done** |
| 7A | `omopy.pregnancy` | 2,318 | 122 | **Done** |
| 8A | `omopy.testing` | 815 | 63 | **Done** |
| | **Total** | **~38,237** | **1,619** | |

---

## Recommended Execution Order

```
Now ─────────────────────────────────────────────────────────────────►

Phase 4A: characteristics ✅──┐
                               ├──► Phase 5A: drug ✅──► Phase 6B: drug_diagnostics ✅
Phase 4B: incidence ✅────────┤
                               ├──► Phase 7A: pregnancy ✅
Phase 5B: survival ✅─────────┘

Phase 6A: treatment ✅ (independent, completed)

Phase 8A: testing ✅ (independent, completed)
```

All 13 phases are now **COMPLETE**.

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
