# Agent Instructions for OMOPy

This document provides instructions for AI coding agents working on the
OMOPy codebase. Follow these rules strictly.

## Code Quality

### Before every commit

Run pre-commit hooks before every git commit:

```bash
uv run pre-commit run --all-files
```

If any hooks fail, fix the issues and re-run until all hooks pass.
Only then proceed with `git add` and `git commit`.

### Ruff (linting and formatting)

OMOPy uses [Ruff](https://docs.astral.sh/ruff/) for both linting and
formatting. Both must pass cleanly — zero errors — before any commit.

**Check formatting:**

```bash
uv run ruff format --check src/ tests/
```

**Fix formatting:**

```bash
uv run ruff format src/ tests/
```

**Check lint rules:**

```bash
uv run ruff check src/ tests/
```

**Auto-fix lint errors (safe fixes only):**

```bash
uv run ruff check --fix src/ tests/
```

Both commands must report zero errors before committing. The CI
workflow (`.github/workflows/ci.yml`) runs these exact checks and will
reject any PR that fails.

### Ruff configuration

Ruff is configured in `pyproject.toml`:

- **Line length:** 88 characters
- **Target:** Python 3.14
- **Enabled rule sets:** E, F, W, I, N, UP, B, A, SIM, RUF
- **Ignored rules:** A001, A002 (builtin shadowing — `type` param is
  an API convention), RUF002/RUF003 (intentional Unicode in
  docstrings), RUF022 (`__all__` grouped by category)
- **Per-file ignores:** B017 in tests (broad `assertRaises` OK)

### Tests

Run the full test suite after any code change:

```bash
uv run pytest
```

All 1619+ tests must pass. Do not commit code that breaks existing tests.

### Docs

Build docs with strict mode to catch warnings:

```bash
uv run python docs/_build.py build --strict
```

## Workflow summary

Before every commit, run this sequence:

```bash
uv run ruff format src/ tests/
uv run ruff check --fix src/ tests/
uv run pytest
uv run pre-commit run --all-files
git add <files>
git commit -m "..."
```
