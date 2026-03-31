# Contributing to OMOPy

Thank you for your interest in contributing to OMOPy. This document provides
guidelines and best practices for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Code Style Guide](#code-style-guide)
  - [Python Version](#python-version)
  - [Type Hints](#type-hints)
  - [Naming Conventions](#naming-conventions)
  - [Docstrings](#docstrings)
  - [Imports](#imports)
  - [Error Handling](#error-handling)
  - [Data Structures](#data-structures)
- [Pre-commit Hooks](#pre-commit-hooks)
- [Testing](#testing)
- [Commit Messages](#commit-messages)
- [Pull Request Process](#pull-request-process)
- [Architecture Overview](#architecture-overview)

## Getting Started

1. Fork the repository on GitHub.
2. Clone your fork locally.
3. Create a feature branch from `main`.
4. Make your changes following the guidelines below.
5. Push to your fork and submit a pull request.

## Development Setup

OMOPy uses [uv](https://docs.astral.sh/uv/) as its package manager and
[hatchling](https://hatch.pypa.io/) as its build backend.

```bash
# Clone the repository
git clone https://github.com/darwin-eu/omopy.git
cd omopy

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the project with all development dependencies
uv sync --all-extras --dev

# Install pre-commit hooks
uv run pre-commit install

# Verify everything works
uv run pytest
```

## Code Style Guide

OMOPy follows strict coding standards to ensure consistency, readability, and
maintainability across the codebase.

### Python Version

OMOPy requires **Python >= 3.14**. Use modern Python features freely:

- PEP 604 union types: `str | None` instead of `Optional[str]`
- PEP 585 generics: `list[str]` instead of `List[str]`
- PEP 695 type aliases (where appropriate)
- `match` statements (PEP 634) where they improve clarity
- f-strings everywhere (no `.format()` or `%` formatting)

### Type Hints

**Every public function, method, and class attribute must have type
annotations.** This is non-negotiable.

```python
# Good
def estimate_incidence(
    cdm: CdmReference,
    denominator_table: str,
    outcome_table: str,
    *,
    interval: Literal["months", "years"] = "years",
    repeated_events: bool = False,
    outcome_washout: int | None = None,
) -> SummarisedResult:
    ...

# Bad — missing annotations
def estimate_incidence(cdm, denominator_table, outcome_table, interval="years"):
    ...
```

#### Type hint rules

- Use `|` for union types, not `Union`.
- Use `X | None` for optional parameters, not `Optional[X]`.
- Use `tuple[int, ...]` for variable-length tuples.
- Use `Literal["a", "b"]` for string enumerations.
- Return types are always required, including `-> None` for void functions.
- Use `Self` (from `typing`) for methods that return the instance.
- Use `Any` sparingly — prefer specific types or generics.
- Private helper functions should also be annotated; the only exception is
  trivial lambdas or closures.

#### Pydantic models

All domain models use [Pydantic](https://docs.pydantic.dev/) with frozen
immutability:

```python
from pydantic import BaseModel

class CohortSpec(BaseModel):
    model_config = {"frozen": True}

    cohort_id: int
    cohort_name: str
    type: Literal["target", "event", "exit"]
```

### Naming Conventions

Follow [PEP 8](https://peps.python.org/pep-0008/) strictly:

| Element             | Convention          | Example                        |
|---------------------|---------------------|--------------------------------|
| Modules             | `snake_case`        | `cdm_reference.py`            |
| Packages            | `snake_case`        | `omopy.connector`             |
| Classes             | `PascalCase`        | `CdmReference`, `CohortTable` |
| Functions/methods   | `snake_case`        | `add_demographics()`          |
| Constants           | `UPPER_SNAKE_CASE`  | `CDM_TABLES`, `MAX_RETRIES`   |
| Private members     | `_leading_underscore` | `_validate_schema()`         |
| Type aliases        | `PascalCase`        | `ConceptId = int`              |

Additional naming rules:

- Boolean parameters/variables should read as predicates:
  `is_valid`, `has_data`, `include_descendants`.
- Avoid abbreviations except well-known ones:
  `cdm`, `sql`, `id`, `df`, `col`.
- Use keyword-only arguments (after `*`) for all parameters beyond the first
  two positional arguments. This prevents positional argument mistakes and
  makes call sites self-documenting.

### Docstrings

All public functions, classes, and modules must have docstrings following
[Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings):

```python
def generate_concept_cohort_set(
    cdm: CdmReference,
    concept_set: Codelist,
    name: str,
    *,
    limit: Literal["first", "all"] = "first",
    required_observation: tuple[int, int] = (0, 0),
    end: str = "observation_period_end_date",
) -> CdmReference:
    """Generate cohorts based on concept sets.

    Creates one cohort per entry in the concept set. Each cohort contains
    persons with at least one record of the specified concepts, subject to
    the observation requirements.

    Args:
        cdm: A CDM reference with an active database connection.
        concept_set: A Codelist mapping cohort names to concept IDs.
        name: Name for the resulting cohort table.
        limit: Whether to keep only the ``"first"`` event per person
            or ``"all"`` events.
        required_observation: Tuple of (days_before, days_after) of
            required observation around the index date.
        end: Column name or strategy for determining cohort end dates.

    Returns:
        The CDM reference with the new cohort table attached.

    Raises:
        ValueError: If the concept set is empty.
        KeyError: If the CDM does not contain required tables.

    Examples:
        >>> codelist = Codelist({"diabetes": [201826]})
        >>> cdm = generate_concept_cohort_set(cdm, codelist, "diabetes_cohort")
    """
```

#### Docstring rules

- First line: imperative summary ("Generate ...", "Compute ...", "Return ...").
- Blank line after the summary.
- `Args:` section for all parameters (name, type is inferred from annotation).
- `Returns:` section describing what is returned.
- `Raises:` section if the function can raise exceptions.
- `Examples:` section for non-trivial public functions.

### Imports

Follow these import ordering rules (enforced by `ruff`):

1. Standard library
2. Third-party packages
3. Local (`omopy`) imports

```python
# Good
import datetime
from collections.abc import Sequence

import ibis
import polars as pl
from pydantic import BaseModel

from omopy.generics import CdmReference, CohortTable, SummarisedResult
```

Additional rules:

- Never use wildcard imports (`from module import *`).
- Prefer absolute imports over relative imports.
- Use `from __future__ import annotations` only if back-porting is needed
  (currently not — we target 3.14+).
- Group `TYPE_CHECKING` imports in an `if TYPE_CHECKING:` block to avoid
  circular imports at runtime.

### Error Handling

- Raise specific exceptions: `ValueError`, `TypeError`, `KeyError`.
- Include descriptive error messages with context.
- Use custom exception classes for domain-specific errors when a module
  has more than two or three error conditions.
- Never catch bare `Exception` unless re-raising.

```python
# Good
if not isinstance(concept_set, Codelist):
    msg = f"Expected Codelist, got {type(concept_set).__name__}"
    raise TypeError(msg)

# Bad
if not isinstance(concept_set, Codelist):
    raise Exception("wrong type")
```

### Data Structures

OMOPy uses a consistent set of data abstractions:

| Layer           | Library   | Use case                                  |
|-----------------|-----------|-------------------------------------------|
| Lazy queries    | Ibis      | Database interaction, SQL generation       |
| Collected data  | Polars    | In-memory DataFrames after `.collect()`    |
| Domain models   | Pydantic  | Validated, frozen, immutable configurations|
| Table rendering | great_tables | GT table output for display             |
| Plotting        | Plotly    | Interactive visualisations                 |

Never mix Pandas into the core pipeline. Pandas is used only at the boundary
when interfacing with `great_tables` (which requires Pandas DataFrames).

## Pre-commit Hooks

OMOPy uses [pre-commit](https://pre-commit.com/) to enforce code quality
before each commit. The hooks are configured in `.pre-commit-config.yaml`.

### Installed hooks

| Hook                     | Purpose                                    |
|--------------------------|--------------------------------------------|
| `ruff-check`             | Lint Python code (with `--fix`)            |
| `ruff-format`            | Format Python code                         |
| `mypy`                   | Static type checking                       |
| `trailing-whitespace`    | Remove trailing whitespace                 |
| `end-of-file-fixer`      | Ensure files end with a newline            |
| `check-yaml`             | Validate YAML syntax                       |
| `check-toml`             | Validate TOML syntax                       |
| `check-added-large-files`| Block files > 500 KB                       |
| `check-merge-conflict`   | Detect merge conflict markers              |
| `debug-statements`       | Detect leftover `breakpoint()` / `pdb`     |

### Running hooks manually

```bash
# Run all hooks on all files
uv run pre-commit run --all-files

# Run a specific hook
uv run pre-commit run ruff-check --all-files

# Update hook versions
uv run pre-commit autoupdate
```

### Ruff configuration

Ruff is configured in `pyproject.toml`:

- **Line length:** 99 characters
- **Target:** Python 3.14
- **Enabled rule sets:** E, F, W (pyflakes/pycodestyle), I (isort),
  N (naming), UP (pyupgrade), B (bugbear), A (builtins shadowing),
  SIM (simplification), TCH (type-checking imports), RUF (ruff-specific)

## Testing

### Running tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=omopy --cov-report=term-missing

# Run a specific test file
uv run pytest tests/connector/test_cohort_generation.py

# Run tests matching a pattern
uv run pytest -k "test_incidence"

# Skip slow/integration tests
uv run pytest -m "not slow"
```

### Writing tests

- Place tests in `tests/` mirroring the `src/omopy/` package structure.
- Use `pytest` fixtures (not `unittest.TestCase`).
- Use [syrupy](https://github.com/toptal/syrupy) for snapshot testing where
  appropriate.
- Use [hypothesis](https://hypothesis.readthedocs.io/) for property-based
  testing of pure functions.
- Test names should be descriptive: `test_estimate_incidence_with_washout`.
- Each test should test one thing.
- Use the `data/synthea.duckdb` test database for integration tests.

### Test structure

```python
import polars as pl
import pytest

from omopy.connector import cdm_from_con
from omopy.incidence import estimate_incidence


@pytest.fixture
def cdm():
    """Provide a CDM connection for tests."""
    return cdm_from_con("data/synthea.duckdb", cdm_schema="base")


def test_estimate_incidence_returns_summarised_result(cdm):
    result = estimate_incidence(cdm, "denominator", "outcomes")
    assert isinstance(result, SummarisedResult)
    assert result.data.height > 0


def test_estimate_incidence_raises_on_missing_table(cdm):
    with pytest.raises(KeyError, match="not found"):
        estimate_incidence(cdm, "nonexistent", "outcomes")
```

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/)
specification:

```
<type>(<scope>): <short summary>

<optional body explaining the "why">
```

### Types

| Type       | Use case                                      |
|------------|-----------------------------------------------|
| `feat`     | New feature or capability                     |
| `fix`      | Bug fix                                       |
| `docs`     | Documentation changes only                    |
| `test`     | Adding or updating tests                      |
| `refactor` | Code changes that neither fix bugs nor add features |
| `ci`       | CI/CD configuration changes                   |
| `chore`    | Dependency updates, tooling changes           |
| `perf`     | Performance improvements                      |

### Examples

```
feat(incidence): add support for age-stratified denominators
fix(vis): check column existence before GT groupname_col assignment
docs: add comprehensive CONTRIBUTING.md with style guide
test(profiles): add property-based tests for add_demographics
ci: add GitHub Actions workflows for CI, docs, and PyPI publishing
```

## Pull Request Process

1. **Branch from `main`:** Create a feature branch with a descriptive name
   (e.g., `feat/age-stratified-denominators`).
2. **Write tests first** (or alongside) for any new functionality.
3. **Ensure all checks pass:**
   ```bash
   uv run pre-commit run --all-files
   uv run pytest
   uv run python docs/_build.py build --strict
   ```
4. **Write a clear PR description** summarising what changed and why.
5. **Keep PRs focused:** one logical change per PR. Large features should be
   broken into a stack of smaller PRs.
6. **Address review feedback** promptly. Force-push to your feature branch
   is fine during review.

## Architecture Overview

OMOPy is structured as a single Python package with submodules that mirror the
[DARWIN EU](https://www.darwin-eu.org/) R package ecosystem:

```
src/omopy/
├── generics/          # Core types: CdmReference, CdmTable, Codelist, SummarisedResult
├── connector/         # Database connections, CDM loading, cohort generation
├── profiles/          # Patient-level enrichment (demographics, intersections)
├── codelist/          # Vocabulary search and codelist operations
├── vis/               # Visualisation: tables (great_tables) and plots (Plotly)
├── characteristics/   # Cohort characterisation summaries
├── incidence/         # Incidence rates and prevalence proportions
├── drug/              # Drug utilisation metrics
├── survival/          # Kaplan-Meier survival analysis
├── treatment/         # Treatment pathway analysis
├── drug_diagnostics/  # Drug exposure quality checks
├── pregnancy/         # Pregnancy identification (HIPPS algorithm)
└── testing/           # Test data generation
```

### Key design decisions

- **Ibis for lazy SQL:** All database queries are constructed lazily via
  [Ibis](https://ibis-project.org/). Nothing executes until `.collect()` or
  an equivalent materialisation call.
- **Polars for collected data:** Once data leaves the database, it lives in
  [Polars](https://pola.rs/) DataFrames — never Pandas in the core pipeline.
- **Pydantic for models:** All domain objects (CDM references, codelist
  definitions, summarised results) are frozen Pydantic models.
- **Keyword-only arguments:** Beyond the first one or two positional
  arguments, all function parameters are keyword-only (`*` separator).
