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
| `5cc4faf` | 2026-03-22 | feat: add omopy.incidence module (Phase 4B — IncidencePrevalence equivalent) |

---

## Phase 0: Generics (omopgenerics equivalent)

### What was built

The `omopy.generics` module (10 source files, 3,400 lines) provides the
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

### Public API (39 exports)

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

### Tests: 310 passing (12 test files)

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

### Tests: 107 passing (9 test files)

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

### Tests: 122 passing (7 test files)

---

## Phase 3C: Vis (visOmopResults equivalent)

### What was built

Formatting, tabulation, and plotting for `SummarisedResult` objects.

### Source files (6 files, ~1,200 lines)

| File | Lines | Purpose |
|------|-------|---------|
| `_format.py` | 463 | `format_estimate_value()`, `format_estimate_name()`, `format_header()`, `format_min_cell_count()`, `tidy_result()`, `tidy_columns()` |
| `_mock.py` | 162 | `mock_summarised_result()` |
| `_style.py` | 164 | `TableStyle`, `PlotStyle`, `customise_text()`, `default_table_style()`, `default_plot_style()` |
| `_table.py` | 454 | `vis_omop_table()`, `vis_table()`, `format_table()` |
| `_plot.py` | 443 | `scatter_plot()`, `bar_plot()`, `box_plot()` |

### Tests: 115 passing (5 test files)

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
| `docs/reference/generics.md` | API ref | mkdocstrings autodoc for 39 exports |
| `docs/reference/connector.md` | API ref | mkdocstrings autodoc for 23 exports |
| `docs/reference/profiles.md` | API ref | mkdocstrings autodoc for 30 exports |
| `docs/reference/codelist.md` | API ref | mkdocstrings autodoc for 14 exports |
| `docs/reference/vis.md` | API ref | mkdocstrings autodoc for 19 exports |

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

The `omopy.characteristics` module (4 source files, ~2,450 lines) provides
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
  `table_cohort_timing`, `table_cohort_overlap`, `table_large_scale_characteristics`,
  `table_cohort_codelist`, `available_table_columns`
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

The `omopy.incidence` module (6 source files, ~2,200 lines) provides
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

### Internal algorithms

**Denominator generation** (`_denominator.py`):

- Starts from `observation_period` table, clips to study window
- Applies sex filter, prior observation requirement
- Age restriction: clips cohort entry to when person enters age range,
  exits when person leaves age range (date arithmetic on year_of_birth)
- Generates one cohort definition per age/sex stratum combination
- Full attrition tracking: records persons excluded at each step

**Calendar interval engine** (`_estimate.py`):

- Generates intervals for weeks/months/quarters/years/overall
- Complete database intervals: only includes intervals where the earliest
  denominator start <= interval start AND latest denominator end >= interval end
- Intersects person-time with intervals to compute days at risk

**Incidence estimation:**

- Person-years = days at risk / 365.25
- Poisson exact CI: `lower = chi2.ppf(alpha/2, 2*events) / (2*person_years)`
- Rate per 100,000 person-years

**Prevalence estimation:**

- Point prevalence: proportion with active outcome at a specific time point
- Period prevalence: proportion with any active outcome during interval
- Wilson score CI: `centre = (x + z^2/2) / (n + z^2)`, interval uses
  `z * sqrt(p(1-p)/n + z^2/4n^2) / (1 + z^2/n)`

**Outcome washout:** If `outcome_washout == inf` and `repeating_events == False`,
only the first event per person counts. With finite washout, a person can
re-enter the at-risk pool after the washout period elapses.

### Tests: 86 passing (79 unit + 7 integration)

- Unit tests use mock `CdmReference` objects with in-memory tables
- Integration tests generate denominator cohorts and estimate incidence/prevalence
  from the Synthea database

### Key design decisions

1. **scipy for CIs.** `scipy.stats.chi2` for Poisson exact CIs (incidence),
   `scipy.stats.norm` for Wilson score CIs (prevalence). Added `scipy>=1.15.0`
   to `pyproject.toml` dependencies.

2. **Calendar interval engine shared.** All three estimation functions share
   the same interval generation and person-time calculation code.

3. **Delegate to `omopy.vis` for presentation.** Table and plot functions are
   thin wrappers with epidemiological defaults.

4. **Attrition as first-class output.** The denominator CohortTable carries
   full attrition metadata showing persons excluded at each filtering step.

### Problems encountered

1. **Wilson CI floating point precision.** `_wilson_ci(0, 100)` returns a
   lower bound of ~3.5e-18, not exactly 0.0. Tests use
   `assert lower < 1e-10` instead of `assert lower == 0.0`.

2. **Polars `dt.month()` returns i8.** When computing age-based date
   offsets, month arithmetic overflows int8. Fixed by casting to Int64
   before multiplication.

---

## Codebase Statistics

### Source code

| Module | Files | Lines |
|--------|-------|-------|
| `omopy.generics` | 10 | 3,400 |
| `omopy.connector` | 20 (incl. circe/) | 6,200 |
| `omopy.profiles` | 11 | 3,700 |
| `omopy.codelist` | 8 | 1,400 |
| `omopy.vis` | 6 | 1,200 |
| `omopy.characteristics` | 4 | 2,450 |
| `omopy.incidence` | 6 | 2,200 |
| `omopy.__init__` | 1 | 46 |
| **Total** | **66** | **~20,600** |

### Tests

| Module | Files | Lines | Tests |
|--------|-------|-------|-------|
| `tests/generics/` | 10 | 2,500 | 236 |
| `tests/connector/` | 12 | 3,900 | 310 |
| `tests/profiles/` | 9 | 1,300 | 107 |
| `tests/codelist/` | 7 | 1,200 | 122 |
| `tests/vis/` | 5 | 900 | 115 |
| `tests/characteristics/` | 1 | ~1,200 | 73 |
| `tests/incidence/` | 1 | ~1,400 | 86 |
| `tests/conftest.py` | 1 | 41 | — |
| **Total** | **46** | **~12,400** | **1046** |

### Public API: 169 exports total

- `omopy.generics`: 39 (10 classes, 5 enums, 1 type alias, 8 constants, 15 functions)
- `omopy.connector`: 23 (2 classes, 1 type alias, 20 functions)
- `omopy.profiles`: 30 (1 type alias, 29 functions)
- `omopy.codelist`: 14 (14 functions)
- `omopy.vis`: 19 (2 classes, 15 functions, 2 factory functions)
- `omopy.characteristics`: 23 (23 functions)
- `omopy.incidence`: 21 (21 functions)

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
6 DARWIN-EU packages to be implemented as OMOPy modules.
Next up: Phase 5A (`omopy.drug` — DrugUtilisation).
