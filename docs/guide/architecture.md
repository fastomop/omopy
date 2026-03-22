# Architecture

OMOPy is a single Python package (`omopy`) that consolidates 17 R packages from
the DARWIN-EU ecosystem into a layered monorepo architecture.

## Package Structure

```
omopy/
├── omopy.generics     ← Core type system (Phase 0)
├── omopy.connector    ← Database CDM access (Phase 1+2)
│   └── circe/         ← CIRCE cohort engine
├── omopy.profiles     ← Patient-level enrichment (Phase 3A)
├── omopy.codelist     ← Vocabulary codelist tools (Phase 3B)
└── omopy.vis          ← Visualisation (Phase 3C, planned)
```

## Layer Dependencies

```
omopy.vis ──────────┐
omopy.codelist ─────┤
omopy.profiles ─────┼──▶ omopy.connector ──▶ omopy.generics
                    │
```

Each higher-level module depends only on modules below it. `omopy.generics` has
no internal dependencies and can be used standalone.

## Technology Stack

| Concern | Technology | Role |
|---------|-----------|------|
| Lazy query construction | [Ibis](https://ibis-project.org/) | Database-agnostic SQL generation |
| Database connections | [SQLAlchemy](https://www.sqlalchemy.org/) | Connection URIs and pooling |
| SQL transpilation | [sqlglot](https://github.com/tobymao/sqlglot) | Dialect-aware SQL manipulation |
| Local DataFrames | [Polars](https://pola.rs/) | Primary in-memory DataFrame |
| Compatibility | [Pandas](https://pandas.pydata.org/) | Interop with Ibis `.execute()` |
| Data models | [Pydantic v2](https://docs.pydantic.dev/) | Frozen, validated data classes |
| Arrow interchange | [PyArrow](https://arrow.apache.org/docs/python/) | Zero-copy data transfer |

## Design Decisions

### Lazy by Default

All CDM table access returns Ibis expressions. SQL is only executed when you
explicitly call `.collect()` (Polars) or `.execute()` (Pandas). This means:

- Queries are composed without hitting the database
- The database optimizer sees the full query plan
- Memory usage stays constant regardless of table size
- You can work with tables containing billions of rows

### Frozen Pydantic Models

Configuration and schema objects use `model_config = ConfigDict(frozen=True)`:

- Thread-safe by construction
- Hashable (usable as dict keys and in sets)
- Validated on creation (Pydantic catches type errors early)
- Serialisable to JSON via `.model_dump_json()`

### CdmReference as a Dict-Like Container

A `CdmReference` acts like a `dict[str, CdmTable]` with metadata:

```python
cdm["person"]                  # access table by name
cdm.table_names                # list all table names
cdm.cdm_version                # "5.3" or "5.4"
cdm.cdm_name                   # data source name
```

Cohort generation returns a *new* `CdmReference` with the cohort table added.
CDM objects are not mutated in place.

### CIRCE Engine

The CIRCE cohort generation engine is a **clean-room Python implementation**
built against the CIRCE JSON specification. It was NOT ported from R source code.

The engine:

1. Parses ATLAS JSON into typed Pydantic models (`CohortExpression`)
2. Resolves concept sets against vocabulary tables
3. Builds Ibis query plans for primary criteria, inclusion rules, end strategies
4. Executes the final cohort as a materialised database table

### Column Naming Conventions

All column names use `snake_case`. The original OMOP CDM column names are
preserved as-is (they already use snake_case). Generated columns from
`omopy.profiles` follow the pattern:

```
{metric}_{table_or_concept}_{window}
```

For example: `flag_condition_occurrence_0_to_inf`, `age`, `sex`.

## OMOP CDM Support

| Version | Tables | Fields | Status |
|---------|--------|--------|--------|
| v5.3 | 37 | 448 | Fully supported |
| v5.4 | 39 | 484 | Fully supported |

Schema specifications are loaded from bundled CSV files and cached per version.
