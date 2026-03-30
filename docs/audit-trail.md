# Audit Trail

This document records the full history of work done on the OMOPy project:
what was built, how it was built, key decisions made, problems encountered,
and their resolutions.

## Project Overview

**Goal:** Reimplement the DARWIN-EU R package ecosystem as a single Python
monorepo package called **OMOPy** (`omopy` on PyPI).

**Location:** `/home/vc/dev/darwin/omopy/` (sibling to the R CDMConnector
package at `/home/vc/dev/darwin/CDMConnector/`)

**Started:** 2026-03-21

---

## Commit History

All commits are on the `main` branch, in chronological order:

| Commit | Date | Message |
|--------|------|---------|
| `af37f77` | 2026-03-21 23:12 | Initial commit |
| `720b59a` | 2026-03-22 01:24 | build: add pydantic>=2.12 dependency and fix dev dependency group |
| `96e3dff` | 2026-03-22 01:24 | fix: add CPython 3.14.0b4 compatibility shim for Pydantic typing._eval_type |
| `fedc47e` | 2026-03-22 01:24 | refactor: convert 31 frozen dataclasses to Pydantic BaseModel across 5 files |
| `c990073` | 2026-03-22 01:25 | test: fix 17 tests broken by dataclass-to-Pydantic migration |
| `fa66cd0` | 2026-03-22 01:25 | feat: add omopy.profiles module (Phase 3A — PatientProfiles equivalent) |
| `40fb35d` | 2026-03-22 01:26 | feat: add omopy.codelist module (Phase 3B — CodelistGenerator equivalent) |
| `cc05d34` | 2026-03-22 01:37 | docs: add mkdocs-material documentation site with API reference |
| `0aaec1a` | 2026-03-22 01:49 | feat: add omopy.vis module (Phase 3C — visOmopResults equivalent) |
| `78eaf52` | 2026-03-22 01:53 | docs: complete documentation with vis module reference and user guide |
| `9bdc436` | 2026-03-22 02:05 | docs: add rewrite roadmap and audit trail |
| `249744e` | 2026-03-22 | feat: add omopy.characteristics module (Phase 4A — CohortCharacteristics equivalent) |
| `079fbbc` | 2026-03-22 | feat: add omopy.incidence module (Phase 4B — IncidencePrevalence equivalent) |
| `65ff1b7` | 2026-03-22 | feat: add omopy.drug module (Phase 5A — DrugUtilisation equivalent) |
| `31e63f1` | 2026-03-22 | feat: add omopy.survival module (Phase 5B — CohortSurvival equivalent) |
| `fca5c83` | 2026-03-22 | docs: fix documentation errors found during comprehensive audit |
| TBD | 2026-03-30 | feat: add omopy.treatment module (Phase 6A — TreatmentPatterns equivalent) |
| TBD | 2026-03-30 | feat: add omopy.drug_diagnostics module (Phase 6B — DrugExposureDiagnostics equivalent) |

---

## Phase 0: Generics (omopgenerics equivalent)

### What was built

The `omopy.generics` module (10 source files, 2,511 lines) provides the
foundational type system for the entire OMOPy package. It defines the core
data structures that all other modules depend on.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `_types.py` | 114 | Enums: `CdmVersion`, `CdmDataType`, `TableType`, `TableGroup`, `TableSchema` |
| `_schema.py` | 503 | `FieldSpec`, `TableSpec`, `ResultFieldSpec`, `CdmSchema` — CDM schema definitions loaded from CSV data files |
| `_validation.py` | 218 | `assert_*` validation helper functions |
| `_data/` | — | CDM schema CSV files for v5.3 and v5.4 |
| `_io.py` | 313 | Import/export for codelists, concept set expressions, summarised results |
| `cdm_table.py` | 242 | `CdmTable` — lazy table backed by Ibis expressions |
| `cdm_reference.py` | 210 | `CdmReference` — collection of CdmTables, `CdmSource` protocol |
| `codelist.py` | 163 | `Codelist` (dict-like), `ConceptEntry` (Pydantic), `ConceptSetExpression` |
| `cohort_table.py` | 204 | `CohortTable` — specialized CdmTable with attrition tracking |
| `summarised_result.py` | 428 | `SummarisedResult` — standardized 13-column result format |

### Public API (38 exports)

- 10 classes: `CdmTable`, `CdmReference`, `CdmSchema`, `FieldSpec`, `TableSpec`, `ResultFieldSpec`, `Codelist`, `ConceptEntry`, `ConceptSetExpression`, `CohortTable`, `SummarisedResult`
- 5 enums: `CdmVersion`, `CdmDataType`, `TableType`, `TableGroup`, `TableSchema`
- 1 type alias: `CdmSource` (Protocol)
- 8 constants: `OVERALL`, `NAME_LEVEL_SEP`, etc.
- 15 functions: validation, IO, schema queries

### Tests: 236 passing (10 test files)

### Key design decisions

1. **Pydantic BaseModel** over stdlib `dataclasses`. Frozen models with
   `ConfigDict(frozen=True)` provide immutability + validation. Required
   a CPython 3.14 compat shim (see Discoveries below).

2. **Polars as primary DataFrame**. `SummarisedResult.data` and
   `CdmTable.collect()` return Polars DataFrames. Pandas compatibility
   via `.to_pandas()`.

3. **Ibis for lazy SQL**. `CdmTable` wraps an Ibis table expression.
   Nothing executes until `.collect()` is called.

4. **CSV-driven schema definitions**. CDM v5.3 and v5.4 table/field specs
   are loaded from CSV files at import time, matching the R package's approach.

---

## Phase 1: Connector Core (CDMConnector equivalent — core)

### What was built

The core database connection layer: connect to a database, discover CDM
tables, and build a `CdmReference`.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `_connection.py` | 151 | Database connection factory (DuckDB, PostgreSQL, etc.) via SQLAlchemy + Ibis |
| `db_source.py` | 250 | `DbSource` — encapsulates connection + schema + catalog metadata |
| `cdm_from_con.py` | 115 | `cdm_from_con()` — main entry point: connect → discover tables → build CdmReference |

### Key design decisions

1. **DbSource as the connection wrapper**. Holds the Ibis connection,
   schema names, catalog, CDM version, and source type. Implements the
   `CdmSource` protocol from generics.

2. **Ibis `database=(catalog, schema)` tuple syntax** for DuckDB. Discovered
   that Ibis 12.0.0 uses this tuple form for `list_tables()` and `table()`.

3. **Automatic CDM version detection** from the `cdm_source` table if not
   specified.

---

## Phase 2: Connector Extended (CDMConnector equivalent — full)

### What was built

All remaining CDMConnector functionality beyond basic connection:

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `cohort_generation.py` | 784 | `generate_concept_cohort_set()`, `generate_cohort_set()` — cohort construction from codelists and CIRCE JSON |
| `cdm_subset.py` | 331 | `cdm_subset_cohort()`, `cdm_subset_person()` — subset a CDM to specific persons/cohorts |
| `date_helpers.py` | 365 | `date_count()`, `date_histogram()`, `date_range_filter()` — date-based utilities |
| `compute.py` | 361 | `compute_query()`, `append_permanent_table()`, `drop_table()` — materialize lazy queries |
| `copy_cdm.py` | 194 | `copy_cdm_to()` — copy CDM tables between schemas/databases |
| `tbl_group.py` | 61 | `tbl_group()` — group tables by domain |
| `snapshot.py` | 292 | `snapshot()` — capture CDM metadata snapshot |
| `cdm_flatten.py` | 268 | `cdm_flatten()` — flatten CDM to person-level wide table |
| `summarise_quantile.py` | 241 | `summarise_quantile()` — compute quantiles across CDM tables |
| `data_hash.py` | 172 | `data_hash()` — deterministic hash of CDM data for reproducibility |
| `benchmark.py` | 221 | `benchmark()` — time CDM operations |
| `circe/` | 2,694 | CIRCE cohort engine (clean-room Python rewrite) |

### CIRCE Engine (`circe/` subpackage)

| File | Lines | Purpose |
|------|-------|---------|
| `_types.py` | 446 | Pydantic models for CIRCE JSON schema (30+ classes) |
| `_parser.py` | 580 | Parse CIRCE JSON into typed Python objects |
| `_concept_resolver.py` | 146 | Resolve concept sets against vocabulary tables |
| `_domain_queries.py` | 348 | Generate per-domain SQL via Ibis for criteria |
| `_criteria.py` | 535 | Evaluate inclusion/exclusion/correlated criteria |
| `_end_strategy.py` | 328 | Cohort end date strategies (date offset, end-of-observation) |
| `_era.py` | 104 | Collapse overlapping intervals into eras |
| `_engine.py` | 632 | `CirceEngine.execute()` — orchestrate the full pipeline |

**Critical decision:** The CIRCE engine was written as a **clean-room
implementation** against the CIRCE JSON specification only. No R source code
was consulted. This avoids any copyright concerns and ensures the Python
implementation is idiomatic.

### Tests: 292 passing (17 test files)

---

## Phase 3A: Profiles (PatientProfiles equivalent)

### What was built

Patient-level enrichment functions. All take a `CdmTable` (typically a
cohort) and a `CdmReference`, returning a new `CdmTable` with additional
columns.

### Source files (11 files, 3,737 lines)

| File | Lines | Purpose |
|------|-------|---------|
| `_demographics.py` | 951 | `add_demographics()`, `add_age()`, `add_sex()`, `add_prior_observation()`, `add_future_observation()`, `add_date_of_birth()`, `add_in_observation()` |
| `_cohort_intersect.py` | 382 | `add_cohort_intersect_flag()`, `add_cohort_intersect_count()`, `add_cohort_intersect_date()`, `add_cohort_intersect_days()` |
| `_concept_intersect.py` | 507 | `add_concept_intersect_flag()`, `add_concept_intersect_count()`, `add_concept_intersect_date()`, `add_concept_intersect_days()` |
| `_table_intersect.py` | 361 | `add_table_intersect_flag()`, `add_table_intersect_count()`, `add_table_intersect_date()`, `add_table_intersect_days()` |
| `_intersect.py` | 482 | Shared intersection logic (flag/count/date/days computation) |
| `_columns.py` | 229 | `add_cdm_name()`, `add_cohort_name()` |
| `_categories.py` | 126 | `add_categories()` — bin continuous variables |
| `_death.py` | 197 | `add_death_flag()`, `add_death_date()`, `add_death_days()` |
| `_windows.py` | 153 | Time window parsing and validation |
| `_utilities.py` | 221 | `summarise_table_counts()` |

### Tests: 122 passing (10 test files)

---

## Phase 3B: Codelist (CodelistGenerator equivalent)

### What was built

Vocabulary-based code list generation and analysis.

### Source files (8 files, 1,424 lines)

| File | Lines | Purpose |
|------|-------|---------|
| `_search.py` | 226 | `get_candidate_codes()`, `get_mappings()` |
| `_hierarchy.py` | 140 | `get_descendants()`, `get_ancestors()` |
| `_drug.py` | 193 | `get_drug_ingredient_codes()`, `get_atc_codes()` |
| `_operations.py` | 115 | `union_codelists()`, `intersect_codelists()`, `compare_codelists()` |
| `_subset.py` | 205 | `subset_to_codes_in_use()`, `subset_by_domain()`, `subset_by_vocabulary()` |
| `_stratify.py` | 125 | `stratify_by_domain()`, `stratify_by_vocabulary()`, `stratify_by_concept_class()` |
| `_diagnostics.py` | 335 | `summarise_code_use()`, `summarise_orphan_codes()` |

### Tests: 122 passing (8 test files)

---

## Phase 3C: Vis (visOmopResults equivalent)

### What was built

Formatting, tabulation, and plotting for `SummarisedResult` objects.

### Source files (6 files, ~1,767 lines)

| File | Lines | Purpose |
|------|-------|---------|
| `_format.py` | 463 | `format_estimate_value()`, `format_estimate_name()`, `format_header()`, `format_min_cell_count()`, `tidy_result()`, `tidy_columns()` |
| `_mock.py` | 162 | `mock_summarised_result()` |
| `_style.py` | 164 | `TableStyle`, `PlotStyle`, `customise_text()`, `default_table_style()`, `default_plot_style()` |
| `_table.py` | 454 | `vis_omop_table()`, `vis_table()`, `format_table()` |
| `_plot.py` | 443 | `scatter_plot()`, `bar_plot()`, `box_plot()` |

### Tests: 115 passing (6 test files)

### Key design decisions

1. **great_tables** for table rendering (Python port of R's gt).
2. **plotly** for all plots (replaces ggplot2 + plotly in R).
3. **TableStyle/PlotStyle frozen dataclasses** replace R's YAML brand system.
4. **Header encoding** uses `\n`-delimited keys in column names for
   multi-level headers.

---

## Pydantic Migration

### What happened

The initial codebase used stdlib `@dataclass(frozen=True, slots=True)` for
all data model classes. This was migrated to Pydantic `BaseModel` with
`ConfigDict(frozen=True)`.

### Scope

31 classes across 5 files were converted:

- `src/omopy/generics/_schema.py` — `FieldSpec`, `TableSpec`, `ResultFieldSpec`, `CdmSchema`
- `src/omopy/generics/codelist.py` — `ConceptEntry`
- `src/omopy/generics/cohort_table.py` — `CohortTable` (partial)
- `src/omopy/generics/summarised_result.py` — `SummarisedResult` (partial)
- `src/omopy/connector/circe/_types.py` — 26 CIRCE JSON model classes

### Problems encountered

1. **CPython 3.14.0b4 + Pydantic >=2.12 incompatibility.** Pydantic calls
   `typing._eval_type(prefer_fwd_module=True)`, but CPython 3.14.0b4 uses
   the parameter name `parent_fwdref`. This causes a `TypeError`.

   **Fix:** Monkey-patch in `src/omopy/__init__.py` that wraps
   `typing._eval_type` to translate `prefer_fwd_module` → `parent_fwdref`.
   Guarded to only activate on 3.14.0 beta builds.

2. **`TableSpec.schema` shadows `BaseModel.schema()`.** Pydantic's
   `BaseModel` has a classmethod `.schema()`. Our `TableSpec` has a field
   called `schema`. Suppressed with `warnings.filterwarnings("ignore", ...)`.

3. **17 tests broke** due to constructor API changes (Pydantic is stricter
   about keyword arguments and field validation). Fixed in commit `c990073`.

### Testing note

`tests/conftest.py` imports `omopy` at the top level to ensure the CPython
shim runs before any Pydantic model is defined during test collection.

The mkdocs build wrapper `docs/_build.py` applies the same shim before
importing mkdocs CLI, since `mkdocstrings-python` uses Pydantic internally.

---

## Documentation Infrastructure

### Setup

- **mkdocs-material** theme with dark/light mode toggle
- **mkdocstrings-python** for API reference auto-generation from docstrings
- **Google-style docstrings** throughout the codebase
- Custom `docs/_build.py` wrapper for CPython 3.14 compatibility

### Pages created

| Page | Type | Description |
|------|------|-------------|
| `docs/index.md` | Overview | Project overview, module table, quick example, status |
| `docs/guide/installation.md` | Guide | Installation instructions |
| `docs/guide/quickstart.md` | Guide | End-to-end walkthrough |
| `docs/guide/architecture.md` | Guide | Design decisions, tech stack |
| `docs/guide/cdm-reference.md` | Guide | Working with CdmReference |
| `docs/guide/cohort-generation.md` | Guide | Cohort generation guide |
| `docs/guide/patient-profiles.md` | Guide | Patient profiles module guide |
| `docs/guide/codelist-generation.md` | Guide | Codelist module guide |
| `docs/guide/visualization.md` | Guide | Visualization module guide |
| `docs/reference/generics.md` | API ref | mkdocstrings autodoc for 38 exports |
| `docs/reference/connector.md` | API ref | mkdocstrings autodoc for 26 exports |
| `docs/reference/profiles.md` | API ref | mkdocstrings autodoc for 40 exports |
| `docs/reference/codelist.md` | API ref | mkdocstrings autodoc for 17 exports |
| `docs/reference/vis.md` | API ref | mkdocstrings autodoc for 18 exports |
| `docs/reference/characteristics.md` | API ref | mkdocstrings autodoc for 23 exports |
| `docs/reference/incidence.md` | API ref | mkdocstrings autodoc for 21 exports |
| `docs/reference/drug.md` | API ref | mkdocstrings autodoc for 44 exports |
| `docs/reference/survival.md` | API ref | mkdocstrings autodoc for 11 exports |
| `docs/reference/treatment.md` | API ref | mkdocstrings autodoc for 11 exports |
| `docs/guide/cohort-characteristics.md` | Guide | Cohort characteristics module guide |
| `docs/guide/incidence-prevalence.md` | Guide | Incidence & prevalence module guide |
| `docs/guide/drug-utilisation.md` | Guide | Drug utilisation module guide |
| `docs/guide/cohort-survival.md` | Guide | Cohort survival module guide |
| `docs/guide/treatment-patterns.md` | Guide | Treatment patterns module guide |
| `docs/roadmap.md` | Project | Rewrite roadmap and repository inventory |
| `docs/audit-trail.md` | Project | This audit trail |

---

## Technical Discoveries & Gotchas

### Polars-specific

1. `frame_equal()` deprecated → use `equals()`.
2. Null joins: use `nulls_equal=True` (was `join_nulls=True`, deprecated v1.24).
3. `list.get()` evaluates on all rows in `when().then()` — pre-filter + scatter back.
4. `pl.lit(2)` creates `Int32`, columns may be `Int64` — must cast.
5. `LazyFrame.columns` triggers `PerformanceWarning` — use `collect_schema().names()`.
6. `dt.month()` returns `i8` (range -128..127) — overflows on multiply! Cast to `Int64` first.
7. `fill_null()` with string on integer column silently does nothing — cast to `Utf8` first.

### Ibis / DuckDB

1. `ibis.duckdb.connect().con` is the native DuckDB connection.
2. Arrow tables registered as temp views via `con.con.register(name, arrow_table)`.
3. `ibis.case()` does NOT exist in Ibis 12.0.0 — use `ibis.cases((cond, val), ...)` tuple syntax.
4. `ibis.row_number()` is 0-indexed.
5. `rename()` uses `rename(new_name="old_name")` keyword syntax.
6. Date subtraction: `(date1 - date2).cast("int64")` → integer days.
7. `.execute()` returns **Pandas** DataFrames, not Polars.
8. `ibis.memtable(arrow_table)` converts Polars → Ibis in-memory tables (via `.to_arrow()`).
9. `fillna()` deprecated in v9.1 → use `fill_null()`.
10. `database=(catalog, schema)` tuple for DuckDB table access.

### Test Database (data/synthea.duckdb)

- CDM v5.4, schema `base`, catalog `synthea`
- 27 persons, 36 OMOP tables
- Key counts: person=27, visit=599, condition=59, drug=663, measurement=562, observation=856
- Concept tables: concept=31976, concept_ancestor=115241, concept_relationship=24860
- CDM source name: "dbt-synthea"
- Concept 192671 (GI hemorrhage) does NOT exist in this DB
- Available condition concepts: 40481087 (Viral sinusitis, 4 occ), 320128 (Essential hypertension, 6 occ)
- 14 male persons out of 27 (gender_concept_id=8507)
- concept_id column type: int32 in DuckDB
- 320128 has NO descendants besides itself
- 28 standard Drug Ingredient concepts, 732 ATC concepts

### Dev Environment

- `pyproject.toml` originally had dev dependencies only in `[project.optional-dependencies] dev`, which `uv sync` doesn't auto-install. Added duplicate `[dependency-groups] dev` section.

---

## Phase 4A: Characteristics (CohortCharacteristics equivalent)

### What was built

The `omopy.characteristics` module (5 source files, ~3,007 lines) provides
cohort characterization analytics — the Python equivalent of the R
`CohortCharacteristics` package.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `_summarise.py` | ~1,580 | 7 summarise functions + internal aggregation engine |
| `_table.py` | ~370 | 8 table functions (wrappers around `vis_omop_table()`) |
| `_plot.py` | ~350 | 7 plot functions (bar, scatter, box, custom attrition flowchart) |
| `_mock.py` | ~160 | `mock_cohort_characteristics()` |

### Public API (23 exports)

- **7 summarise functions:** `summarise_characteristics`, `summarise_cohort_count`,
  `summarise_cohort_attrition`, `summarise_cohort_timing`, `summarise_cohort_overlap`,
  `summarise_large_scale_characteristics`, `summarise_cohort_codelist`
- **8 table functions:** `table_characteristics`, `table_cohort_count`, `table_cohort_attrition`,
  `table_cohort_timing`, `table_cohort_overlap`, `table_top_large_scale_characteristics`,
  `table_large_scale_characteristics`, `available_table_columns`
- **7 plot functions:** `plot_characteristics`, `plot_cohort_count`, `plot_cohort_attrition`,
  `plot_cohort_timing`, `plot_cohort_overlap`, `plot_large_scale_characteristics`,
  `plot_compared_large_scale_characteristics`
- **1 mock:** `mock_cohort_characteristics`

### Internal aggregation engine

The core of the module is an internal aggregation engine in `_summarise.py`:

- `_classify_variable()` — classify columns as numeric/categorical/date/binary
- `_compute_estimates()` — compute requested statistics (count, mean, sd, quantiles, etc.)
- `_compute_categorical_estimates()` — count + percentage per category level
- `_summarise_variables()` — aggregate variables into SummarisedResult rows
- `_add_count_rows()` — add subject/record count rows
- `_resolve_strata()` — generate (strata_name, strata_level, filtered_df) tuples
- `_filter_settings_by_cohort_id()` — filter settings metadata when cohort_id is specified
- `_make_settings()` — create settings DataFrame for SummarisedResult
- `_empty_result()` — create empty result with correct schema
- `_window_name()` — format time windows as human-readable strings

### Tests: 73 passing (61 unit + 12 integration)

- Unit tests use mock `CohortTable` objects (no database)
- Integration tests generate real cohorts from the Synthea database via CIRCE engine,
  then run all summarise functions and validate results

### Key design decisions

1. **Delegate to `omopy.profiles`** for demographics and intersections. The
   `summarise_characteristics()` function calls `add_demographics()`,
   `add_table_intersect_*()`, etc. before aggregation.

2. **Detect existing columns** before adding demographics. If a strata column
   (e.g., `sex`) was pre-added before calling `summarise_characteristics()`,
   the function skips re-adding it to avoid Ibis duplicate column errors.

3. **`filter_cohort_id` bug fix.** The `filter_cohort_id()` utility from
   `omopy.profiles` filters the Ibis data but does NOT filter the `.settings`
   metadata. All 7 summarise functions now use `_filter_settings_by_cohort_id()`
   to ensure settings are consistent with the filtered data.

4. **Table/plot wrappers are thin.** They delegate to `omopy.vis` functions
   with domain-specific defaults for estimate formatting, headers, and grouping.
   The custom attrition flowchart uses Plotly shapes and annotations directly.

### Problems encountered

1. **Duplicate column error.** When users add a column (e.g., `sex`) for
   stratification and then call `summarise_characteristics(demographics=True)`,
   Ibis raised `IbisInputError: Duplicate column name 'sex'`. Fixed by
   detecting existing columns and skipping re-addition.

2. **`filter_cohort_id` settings inconsistency.** Initially only applied the
   fix in `summarise_characteristics()`. The remaining 5 functions
   (`summarise_cohort_attrition`, `summarise_cohort_timing`,
   `summarise_cohort_overlap`, `summarise_large_scale_characteristics`,
   `summarise_cohort_codelist`) still used unfiltered settings. Fixed all 5.

---

## Phase 4B: Incidence (IncidencePrevalence equivalent)

### What was built

The `omopy.incidence` module (7 source files, ~3,315 lines) provides
incidence and prevalence estimation — the Python equivalent of the R
`IncidencePrevalence` package.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `_denominator.py` | ~580 | Denominator cohort generation with age/sex/observation filtering and attrition |
| `_estimate.py` | ~670 | Incidence and prevalence estimation with interval engine and CIs |
| `_result.py` | ~160 | Result conversion from SummarisedResult to tidy DataFrames |
| `_table.py` | ~280 | 4 table functions + 2 options functions (wrappers around `vis_omop_table()`) |
| `_plot.py` | ~300 | 4 plot functions + 2 grouping helpers (wrappers around vis plots) |
| `_mock.py` | ~210 | Mock CDM generation and benchmarking |

### Public API (21 exports)

- **2 denominator functions:** `generate_denominator_cohort_set`,
  `generate_target_denominator_cohort_set`
- **3 estimation functions:** `estimate_incidence`, `estimate_point_prevalence`,
  `estimate_period_prevalence`
- **2 result conversion:** `as_incidence_result`, `as_prevalence_result`
- **6 table functions:** `table_incidence`, `table_prevalence`,
  `table_incidence_attrition`, `table_prevalence_attrition`,
  `options_table_incidence`, `options_table_prevalence`
- **4 plot functions:** `plot_incidence`, `plot_prevalence`,
  `plot_incidence_population`, `plot_prevalence_population`
- **2 grouping helpers:** `available_incidence_grouping`, `available_prevalence_grouping`
- **2 utilities:** `mock_incidence_prevalence`, `benchmark_incidence_prevalence`

### Tests: 86 passing (79 unit + 7 integration)

### Key design decisions

1. **scipy for CIs.** `scipy.stats.chi2` for Poisson exact CIs (incidence),
   `scipy.stats.norm` for Wilson score CIs (prevalence).

2. **Calendar interval engine shared.** All three estimation functions share
   the same interval generation and person-time calculation code.

---

## Phase 5A: Drug (DrugUtilisation equivalent)

### What was built

The `omopy.drug` module (12 source files, ~6,297 lines) provides comprehensive
drug utilisation analysis — the Python equivalent of the R `DrugUtilisation`
package, which is the largest package in the DARWIN-EU ecosystem with 57 exports.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `_data/patterns.py` | ~130 | 41 drug strength patterns, unit constants, formula constants, unit conversion dicts |
| `_cohort_generation.py` | ~715 | `generate_drug_utilisation_cohort_set`, `generate_ingredient_cohort_set`, `generate_atc_cohort_set`, `erafy_cohort`, `cohort_gap_era` |
| `_daily_dose.py` | ~470 | `add_daily_dose`, `pattern_table` — drug strength pattern matching and dose calculation |
| `_require.py` | ~375 | 4 requirement/filter functions with attrition tracking |
| `_add_drug_use.py` | ~1,695 | `add_drug_utilisation` (all-in-one) + 11 individual metric functions + `add_drug_restart` |
| `_add_intersect.py` | ~510 | `add_indication`, `add_treatment` — cohort intersection analysis |
| `_summarise.py` | ~1,240 | 6 summarise functions + internal helpers (strata resolution, numeric estimates, Wilson CI) |
| `_table.py` | ~370 | 6 table wrapper functions |
| `_plot.py` | ~310 | 5 plot wrapper functions |
| `_mock.py` | ~200 | `mock_drug_utilisation`, `benchmark_drug_utilisation` |

### Public API (44 exports)

- **Cohort generation (5):** `generate_drug_utilisation_cohort_set`, `generate_ingredient_cohort_set`, `generate_atc_cohort_set`, `erafy_cohort`, `cohort_gap_era`
- **Daily dose (2):** `add_daily_dose`, `pattern_table`
- **Requirement/filter (4):** `require_is_first_drug_entry`, `require_prior_drug_washout`, `require_observation_before_drug`, `require_drug_in_date_range`
- **Add drug use metrics (12):** `add_drug_utilisation`, `add_number_exposures`, `add_number_eras`, `add_days_exposed`, `add_days_prescribed`, `add_time_to_exposure`, `add_initial_exposure_duration`, `add_initial_quantity`, `add_cumulative_quantity`, `add_initial_daily_dose`, `add_cumulative_dose`, `add_drug_restart`
- **Add intersect (2):** `add_indication`, `add_treatment`
- **Summarise (6):** `summarise_drug_utilisation`, `summarise_indication`, `summarise_treatment`, `summarise_drug_restart`, `summarise_dose_coverage`, `summarise_proportion_of_patients_covered`
- **Table (6):** `table_drug_utilisation`, `table_indication`, `table_treatment`, `table_drug_restart`, `table_dose_coverage`, `table_proportion_of_patients_covered`
- **Plot (5):** `plot_drug_utilisation`, `plot_indication`, `plot_treatment`, `plot_drug_restart`, `plot_proportion_of_patients_covered`
- **Mock/benchmark (2):** `mock_drug_utilisation`, `benchmark_drug_utilisation`

### Internal algorithms

**Drug cohort generation** (`_cohort_generation.py`):

- Resolves concept sets including descendants via `concept_ancestor` table
- Filters `drug_exposure` records by resolved concepts
- Constrains to observation periods (start must be within observation period)
- Applies limit strategy (first event only, or all events)
- Collapses overlapping/nearby records into eras using gaps-and-islands algorithm
- Stores `gap_era` setting in cohort metadata for downstream use

**Drug strength pattern matching** (`_daily_dose.py`, `_data/patterns.py`):

- 41 patterns defined by presence/absence of amount_numeric, numerator_numeric,
  denominator_numeric and their unit concept IDs
- 4 formula types: fixed_amount, concentration, time_based_with_denom, time_based_no_denom
- Unit standardization: micrograms→mg (÷1000), mega-IU→IU (÷1M), liters→mL (×1000)
- Output units: milligram, milliliter, international unit, milliequivalent

**Drug utilisation metrics** (`_add_drug_use.py`):

All 12 add functions delegate to a shared internal engine that:
1. Joins cohort persons with `drug_exposure` records matching concept set (with descendants)
2. Clips to configurable [indexDate, censorDate] window
3. Computes metrics per concept set entry (exposures, eras, days, quantities, doses)
4. Left-joins metrics back to cohort, fills nulls appropriately

**Drug restart classification** (`_add_drug_use.py`):

1. Computes `censor_days` (future observation from cohort_end_date)
2. Computes `restart_days` (days to next entry of same drug)
3. Computes `switch_days` (days to earliest entry of a different drug)
4. Classifies per follow-up window: "restart", "switch", "restart and switch", "untreated"

**Proportion of patients covered** (`_summarise.py`):

- Day-by-day calculation of the proportion with active drug exposure
- Wilson score confidence intervals via `scipy.stats.norm`

### Tests: 101 passing (67 unit + 34 integration)

- Unit tests cover imports, patterns, constants, mock data, table/plot wrappers,
  summarise helpers, format utilities, era collapsing, gap_era retrieval, requirement
  functions
- Integration tests generate real drug cohorts from the Synthea database,
  apply requirements, compute utilisation metrics, run all summarise functions,
  and validate PPC and pattern table output

### Key design decisions

1. **Deferred temp table cleanup.** Ibis expressions are lazy — registering a
   temp table for pattern data, building an expression referencing it, then
   unregistering in a `finally` block causes errors when the expression is
   later materialized. Solution: track temp table names and defer cleanup
   until after `.to_pyarrow()` materialization completes.

2. **Dose metrics disabled by default in Synthea tests.** The Synthea test
   database has an empty `drug_strength` table and NULL quantities, so
   `initial_daily_dose` and `cumulative_dose` are disabled in integration
   tests. Pattern matching and dose calculation are tested via unit tests
   with mock data.

3. **cohort_gap_era() returns dict, not list.** The mapping is
   `{cohort_definition_id: gap_era}`, making lookups O(1).

4. **Individual metric functions have minimal parameters.** Only
   `add_number_eras` and `add_days_exposed` accept `gap_era` (they need
   era collapsing); `add_number_exposures` and `add_days_prescribed` do not.

### Problems encountered

1. **Ibis temp table lifecycle bug.** Fixed by deferring `con.con.unregister()`
   calls until after lazy expressions are materialized.

2. **List ATC name handling.** `get_atc_codes()` accepts only `str | None`.
   `generate_atc_cohort_set()` iterates over the list and merges codelists.

3. **pattern_table(None) raised AttributeError instead of TypeError.** Added
   explicit type guard at the top of the function.

---

## Phase 5B: Survival (CohortSurvival equivalent)

### What was built

The `omopy.survival` module (7 source files, ~2,548 lines) provides cohort
survival analysis — the Python equivalent of the R `CohortSurvival` package.
It implements Kaplan-Meier estimation via lifelines and a custom
Aalen-Johansen estimator for competing risk cumulative incidence.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `__init__.py` | 78 | 11 exports |
| `_add_survival.py` | 272 | `add_cohort_survival()` — enrich cohort with time/status columns |
| `_estimate.py` | 1,186 | `estimate_single_event_survival()`, `estimate_competing_risk_survival()`, internal KM/AJ engine, risk table, summary stats |
| `_result.py` | 104 | `as_survival_result()` — wide-format conversion |
| `_table.py` | 320 | `table_survival()`, `table_survival_events()`, `table_survival_attrition()`, `options_table_survival()` |
| `_plot.py` | 344 | `plot_survival()`, `available_survival_grouping()` |
| `_mock.py` | 244 | `mock_survival()` — synthetic CDM with target/outcome/competing cohorts |

### Public API (11 exports)

- **2 estimation functions:** `estimate_single_event_survival` (Kaplan-Meier),
  `estimate_competing_risk_survival` (Aalen-Johansen CIF)
- **1 add function:** `add_cohort_survival` — enrich cohort with time/status columns
- **1 result conversion:** `as_survival_result` — wide-format DataFrames
- **4 table functions:** `table_survival`, `table_survival_events`,
  `table_survival_attrition`, `options_table_survival`
- **2 plot functions:** `plot_survival` (KM/CIF curves with CI ribbons),
  `available_survival_grouping`
- **1 mock:** `mock_survival`

### Internal algorithms

**Add cohort survival** (`_add_survival.py`):

1. Add `future_observation` (days to end of observation period) via profiles
2. Check for outcome events in washout period (flag via cohort intersect)
3. Get `days_to_event` (days from index to first outcome after index)
4. Apply censoring hierarchy: cohort exit → censor date → follow-up cap
5. Compute `status`: 1 if event occurred before censoring, else 0
6. Compute `time`: days_to_event if event, days_to_exit if censored
7. Set time/status to NA for anyone with an event in the washout period

**Kaplan-Meier estimation** (`_estimate.py`):

- Uses `lifelines.KaplanMeierFitter` for survival curve estimation
- Extracts survival function, CIs, median, quantiles, RMST
- Computes risk table (n_risk, n_event, n_censor) per interval
- Generates attrition tracking through the pipeline

**Aalen-Johansen competing risk** (`_estimate.py`):

- Custom implementation from first principles (lifelines does not include it)
- CIF_k(t) = sum h_k(t_j) * S(t_{j-1}) where h_k is cause-specific hazard
- Produces cumulative incidence curves with CIs via Greenwood variance

### Tests: 80 passing (unit + integration against Synthea database)

### Key design decisions

1. **lifelines for KM, custom AJ for competing risks.** lifelines 0.30.3
   provides robust KM estimation but has no built-in Aalen-Johansen.
   The custom implementation computes CIF from cause-specific hazards
   and the overall Kaplan-Meier survival, following the standard
   textbook formula.

2. **Four result types in one SummarisedResult.** The estimation functions
   pack `survival_estimates`, `survival_events`, `survival_summary`, and
   `survival_attrition` into a single SummarisedResult using different
   `result_type` values in settings. The `as_survival_result()` function
   unpacks them into separate wide-format DataFrames.

3. **Censoring hierarchy matches R.** The censoring logic follows the same
   hierarchy as the R CohortSurvival package: event → cohort exit → censor
   date → follow-up cap → observation end.

4. **Mock data uses synthetic generation.** Rather than porting the R
   package's `mockMGUS2cdm()` (which uses the mgus2 dataset), the Python
   `mock_survival()` generates fully synthetic data with configurable
   event rates and competing risk rates.

### Problems encountered

1. **NumPy 2.0 API change.** `np.trapz` was removed in NumPy 2.0 — replaced
   with `np.trapezoid`. Two occurrences in `_estimate.py` (RMST computation)
   were fixed.

2. **Ibis IntegrityError with dead code.** An initial broken attempt at a
   washout join in `_add_survival.py` used expressions from a different
   relation in a join predicate. Even though it was dead code (followed by
   the working implementation), it executed first and threw
   `ibis.common.exceptions.IntegrityError`. Fixed by removing the dead code.

---

## Phase 6A: Treatment (TreatmentPatterns equivalent)

### What was built

The `omopy.treatment` module (6 source files, ~2,634 lines) provides
treatment pathway analysis — the Python equivalent of the R
`TreatmentPatterns` package. It computes sequential treatment pathways
from OMOP CDM cohort data, summarises frequencies and durations, and
visualises results as Sankey diagrams, sunburst charts, and box plots.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `__init__.py` | 81 | 11 exports |
| `_pathway.py` | 1,141 | `compute_pathways()`, `CohortSpec`, `PathwayResult`, 6-step pipeline engine |
| `_summarise.py` | 527 | `summarise_treatment_pathways()`, `summarise_event_duration()` |
| `_table.py` | 167 | `table_treatment_pathways()`, `table_event_duration()` |
| `_plot.py` | 481 | `plot_sankey()`, `plot_sunburst()`, `plot_event_duration()` |
| `_mock.py` | 237 | `mock_treatment_pathways()` |

### Public API (11 exports)

- **2 core types:** `CohortSpec` (Pydantic model for cohort role definition),
  `PathwayResult` (Pydantic model for pipeline output)
- **1 computation:** `compute_pathways` — 6-step pipeline
- **2 summarise:** `summarise_treatment_pathways`, `summarise_event_duration`
- **2 table:** `table_treatment_pathways`, `table_event_duration`
- **3 plot:** `plot_sankey`, `plot_sunburst`, `plot_event_duration`
- **1 mock:** `mock_treatment_pathways`

### Internal algorithms

**`compute_pathways()` 6-step pipeline** (`_pathway.py`):

1. **Ingest** — Collect CohortTable to Polars, join with `person` table for
   demographics (age, sex), filter by `min_era_duration`
2. **Treatment history** — Match event cohorts to target observation windows,
   clip events to window boundaries, assign `n_target`
3. **Split event cohorts** (optional) — Split specified cohorts into
   acute/therapy sub-cohorts based on duration cutoff
4. **Era collapse** — Iteratively merge consecutive same-drug eras separated
   by ≤ `era_collapse_size` days (loops until convergence)
5. **Combination window** — Iteratively detect overlaps ≥ `combination_window`
   days, create "A+B" combination IDs using FRFS/LRFS logic
6. **Filter treatments** — "first" (first occurrence per drug), "changes"
   (remove consecutive duplicates), or "all"
7. **Finalize** — Assign `event_seq`, truncate to `max_path_length`, resolve
   cohort names, append exit cohorts

**Era collapse** groups by `(person_id, event_cohort_id, n_target)`, checks
the gap between consecutive same-drug eras, and merges if gap ≤ threshold.
Iterates until no more merges occur.

**Combination window** handles three overlap cases:
- **Switch** (overlap < combinationWindow): truncate or keep
- **FRFS** (First Received First Stopped): creates 3 segments
  (pre-overlap, overlap, post-overlap)
- **LRFS** (Last Received First Stopped): current era entirely within previous

Combination IDs use `+`-joined sorted cohort IDs (e.g., `"1+3"`), resolved
to sorted names (`"DrugA+DrugC"`).

**Summarise functions** (`_summarise.py`):

- `summarise_treatment_pathways()` — Build dash-separated path strings per
  person, compute frequencies by path × age group × sex × index year,
  apply minimum cell count suppression
- `summarise_event_duration()` — Compute min/q1/median/q3/max/mean/sd
  per event cohort, both overall and by position in the pathway

**Plot functions** (`_plot.py`):

- `plot_sankey()` — Parse dash-separated paths into step transitions,
  build Plotly Sankey trace with source/target/value arrays
- `plot_sunburst()` — Parse paths into hierarchical parent-child labels,
  build Plotly sunburst trace
- `plot_event_duration()` — Build Plotly box trace per event cohort

### Tests: 127 passing (109 unit + 18 integration)

- Unit tests use synthetic Polars DataFrames (no database)
- Integration tests build CohortTable from Synthea drug and condition
  cohorts, run full pipeline, validate PathwayResult structure, test
  summarise → table/plot end-to-end

### Key design decisions

1. **Polars-only internal processing.** Unlike other modules that use Ibis
   for lazy computation, the treatment module collects cohort data upfront
   and processes entirely in Polars. The pathway algorithm requires iterative
   row-level operations (era collapse, combination detection) that don't
   map well to SQL.

2. **CohortSpec as Pydantic model.** Instead of passing dicts or positional
   arguments, cohort roles are defined via typed `CohortSpec` objects that
   validate at construction time.

3. **PathwayResult bundles everything.** The result object carries the
   treatment history, attrition, cohort specs, CDM name, and arguments.
   This makes the summarise/table/plot functions self-contained — they
   don't need the CDM or CohortTable again.

4. **Path encoding as dash-separated strings.** Steps separated by `-`,
   combinations by `+` (alphabetically sorted). Example:
   `"lisinopril-amlodipine+lisinopril-amlodipine"`.

5. **Mock function returns SummarisedResult directly.** Unlike
   `compute_pathways()` which returns `PathwayResult`, the mock function
   returns a `SummarisedResult` for direct use with table/plot functions,
   matching the pattern established by other modules.

### Problems encountered

1. **`CohortTable._tbl` does not exist.** The initial implementation
   accessed `cohort._tbl` and `cdm["person"]._tbl`, but CohortTable uses
   `.data` (property) and `.collect()` (method). Fixed by using
   `cohort.collect()` and `cdm["person"].collect()` to get Polars
   DataFrames directly.

2. **`_filter_treatments` on empty DF.** `map_elements` on an empty column
   with no `event_cohort_id` raises `ColumnNotFoundError`. Fixed by
   returning early when `df.height == 0`.

3. **`_finalize_pathways` on empty DF.** `with_columns(pl.lit(...))` on an
   empty DataFrame creates a phantom row. Fixed by building a schema-only
   DataFrame instead.

4. **`_filter_result_type` type mismatch.** `result_id` can be Utf8 in data
   but Int64 in settings (or vice versa). Fixed by casting `matching_ids`
   to match each column's dtype independently.

5. **`str.concat` deprecation.** Polars deprecated `str.concat` in favor of
   `str.join`. Fixed in `_summarise.py`.

6. **Plotly title assertion.** `fig.layout.title` returns a `Title` object,
   not a string. Must use `fig.layout.title.text` for assertion in tests.

---

## Phase 6B: Drug Diagnostics (DrugExposureDiagnostics equivalent)

### What was built

The `omopy.drug_diagnostics` module (6 source files, ~1,830 lines) provides
comprehensive diagnostic checks on drug exposure records — the Python
equivalent of the R `DrugExposureDiagnostics` package.

### Source files

| File | Lines | Purpose |
|------|-------|---------|
| `__init__.py` | 73 | 8 exports, module docstring with usage example |
| `_checks.py` | ~780 | `execute_checks()`, `DiagnosticsResult`, 12 check implementations, `_resolve_descendants()`, `_fetch_drug_records()`, `_quantile_stats()`, `_obscure_df()` |
| `_summarise.py` | ~370 | `summarise_drug_diagnostics()`, converters for each check type |
| `_table.py` | ~110 | `table_drug_diagnostics()` thin wrapper |
| `_plot.py` | ~260 | `plot_drug_diagnostics()`, `_plot_categorical()`, `_plot_quantile()`, `_plot_missing()` |
| `_mock.py` | ~310 | `mock_drug_exposure()`, `benchmark_drug_diagnostics()` |

### Public API (8 exports)

- **1 constant:** `AVAILABLE_CHECKS` (list of 12 check names)
- **1 model:** `DiagnosticsResult` (Pydantic, dict-like access to check DataFrames)
- **1 computation:** `execute_checks` — runs selected checks on drug_exposure records
- **1 summarise:** `summarise_drug_diagnostics` — convert to SummarisedResult
- **1 table:** `table_drug_diagnostics` — wrapper around `vis_omop_table()`
- **1 plot:** `plot_drug_diagnostics` — bar/box plots per check type
- **2 mock/benchmark:** `mock_drug_exposure`, `benchmark_drug_diagnostics`

### 12 diagnostic checks

1. **missing** — Count missing values for 15 drug_exposure columns
2. **exposure_duration** — Quantile distribution of exposure duration in days
3. **type** — Frequency of drug_type_concept_id values
4. **route** — Frequency of route_concept_id values
5. **source_concept** — Source concept mapping analysis (source → standard)
6. **days_supply** — Quantile distribution + comparison with date-derived duration
7. **verbatim_end_date** — Comparison of verbatim_end_date vs drug_exposure_end_date
8. **dose** — Daily dose coverage via drug_strength pattern matching
9. **sig** — Frequency of sig values
10. **quantity** — Quantile distribution of quantity field
11. **days_between** — Time between consecutive drug records per patient
12. **diagnostics_summary** — Aggregated summary across all other checks

### Tests: 80 passing (55 unit + 25 integration)

- Unit tests cover AVAILABLE_CHECKS, DiagnosticsResult, _quantile_stats,
  _obscure_df, all 12 check functions, mock_drug_exposure,
  summarise_drug_diagnostics, table_drug_diagnostics, plot_drug_diagnostics
- Integration tests run execute_checks against Synthea database, validate
  sampling, min_cell_count, full pipeline end-to-end

### Key design decisions

1. **DiagnosticsResult as Pydantic model with dict-like access.** `result["missing"]`
   returns the Polars DataFrame for the missing check. Iterable over check names.
   Metadata (ingredient IDs, CDM name, checks run, sample size) stored as fields.

2. **Configurable sampling.** `sample_size` parameter limits records per ingredient
   for performance. Set to `None` to use all records.

3. **Min cell count obscuring.** Applied consistently across all checks — counts
   below threshold are replaced with `f"<{min_cell_count}"` and associated
   statistics are nullified. Uses the `_obscure_df()` helper pattern.

4. **Dose check delegates to drug module.** Rather than reimplementing drug
   strength pattern matching, the dose check calls `omopy.drug.add_daily_dose()`
   and `omopy.drug.pattern_table()` internally.

5. **Summary check is meta.** The `diagnostics_summary` check aggregates results
   from all other enabled checks into a single overview DataFrame.

### Problems encountered

1. **Synthea drug_strength table is empty.** The dose check always returns 0%
   coverage in integration tests. Validated that the check logic works correctly
   with mock data in unit tests.

2. **Synthea quantity/sig always NULL.** Quantity quantile check returns all-null
   quantiles. Sig check shows only `<missing>`. Both are expected for synthetic
   data — unit tests validate correct behavior with non-null mock data.

3. **`rng.randint(1, remaining)` fails when `remaining == 0`.** In mock data
   generation, the loop that distributes records across ingredients could
   exhaust the remaining count. Fixed with `if remaining <= 0: break` guard.

---

## Codebase Statistics

### Source code

| Module | Files | Lines |
|--------|-------|-------|
| `omopy.generics` | 10 | 2,511 |
| `omopy.connector` | 24 (incl. circe/) | 7,072 |
| `omopy.profiles` | 11 | 3,737 |
| `omopy.codelist` | 8 | 1,424 |
| `omopy.vis` | 6 | 1,767 |
| `omopy.characteristics` | 5 | 3,007 |
| `omopy.incidence` | 7 | 3,315 |
| `omopy.drug` | 12 | 6,297 |
| `omopy.survival` | 7 | 2,548 |
| `omopy.treatment` | 6 | 2,634 |
| `omopy.drug_diagnostics` | 6 | 1,830 |
| `omopy.__init__` | 1 | 46 |
| **Total** | **103** | **~36,188** |

### Tests

| Module | Files | Lines | Tests |
|--------|-------|-------|-------|
| `tests/generics/` | 10 | 2,007 | 236 |
| `tests/connector/` | 17 | 3,509 | 292 |
| `tests/profiles/` | 10 | 1,313 | 122 |
| `tests/codelist/` | 8 | 1,190 | 122 |
| `tests/vis/` | 6 | 878 | 115 |
| `tests/characteristics/` | 2 | 1,211 | 73 |
| `tests/incidence/` | 2 | 1,146 | 86 |
| `tests/drug/` | 2 | 1,469 | 101 |
| `tests/survival/` | 2 | 851 | 80 |
| `tests/treatment/` | 2 | 1,560 | 127 |
| `tests/drug_diagnostics/` | 3 | 1,250 | 80 |
| `tests/conftest.py` | 1 | 41 | — |
| **Total** | **65** | **~16,425** | **1,434** |

### Public API: 257 exports total

- `omopy.generics`: 38 (10 classes, 5 enums, 1 type alias, 8 constants, 14 functions)
- `omopy.connector`: 26 (2 classes, 1 type alias, 23 functions)
- `omopy.profiles`: 40 (1 type alias, 39 functions)
- `omopy.codelist`: 17 (17 functions)
- `omopy.vis`: 18 (2 classes, 14 functions, 2 factory functions)
- `omopy.characteristics`: 23 (23 functions)
- `omopy.incidence`: 21 (21 functions)
- `omopy.drug`: 44 (44 functions)
- `omopy.survival`: 11 (11 functions)
- `omopy.treatment`: 11 (2 classes, 9 functions)
- `omopy.drug_diagnostics`: 8 (1 constant, 1 class, 6 functions)

### Dependencies

Core runtime dependencies:

| Package | Version | Purpose |
|---------|---------|---------|
| `ibis-framework` | >= 12.0.0 | Lazy SQL query construction |
| `duckdb` | >= 1.5.0 | Default database backend |
| `sqlalchemy` | >= 2.0.48 | Database connection management |
| `polars` | >= 1.0 | Primary local DataFrame |
| `pandas` | >= 2.3.3 | DataFrame compatibility |
| `pydantic` | >= 2.12 | Data model validation |
| `pyarrow` | >= 23.0.1 | Arrow interchange format |
| `great-tables` | >= 0.21.0 | Table rendering |
| `plotly` | >= 6.6.0 | Plot rendering |
| `scipy` | >= 1.15.0 | Statistical confidence intervals |
| `lifelines` | >= 0.29 | Kaplan-Meier survival analysis |

---

## Build & Test Commands

```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=omopy

# Build documentation
uv run python docs/_build.py build --strict

# Serve documentation locally
uv run python docs/_build.py serve

# Type check (when configured)
uv run mypy src/omopy
```

---

## What Comes Next

See [Roadmap](roadmap.md) for the detailed plan covering the remaining
2 DARWIN-EU packages to be implemented as OMOPy modules.
Next up: Phase 7A (`omopy.pregnancy` — PregnancyIdentifier).
