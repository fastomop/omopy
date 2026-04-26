# Agent Instructions for OMOPy

OMOPy is a single Python package that reimplements the
[DARWIN EU](https://www.darwin-eu.org/) R package ecosystem for working with OMOP databases. It provides lazy database access via [Ibis](https://ibis-project.org/), type-safe data structures via Pydantic and Polars, and a clean Pythonic API with full type hints.

Follow these rules strictly.

## Commit Workflow

Run this sequence before every commit:

```bash
uv run ruff format src/ tests/
uv run ruff check --fix src/ tests/
uv run pytest
uv run pre-commit run --all-files
git add <files>
git commit -m "..."
```

All checks must pass with zero errors. CI (`.github/workflows/ci.yml`) enforces the same.

## Ruff Config (`pyproject.toml`)

- Line length: 88 · Target: Python 3.14
- Rules: E, F, W, I, N, UP, B, A, SIM, RUF
- Ignored: A001/A002 (`type` param is API convention), RUF002/RUF003 (intentional Unicode), RUF022 (`__all__` grouped by category)
- Per-file: B017 ignored in tests

## Tests

```bash
uv run pytest
```

All 1619+ tests must pass. Never commit code that breaks existing tests.

## Docs

```bash
uv run python docs/_build.py build --strict
```

---

<!-- gitnexus:start -->
## GitNexus — Code Intelligence

Indexed as **omopy** (8411 symbols, 11919 relationships, 122 execution flows). If any tool warns the index is stale, run `npx gitnexus analyze` first.

### Required Before Edits

- **Run `gitnexus_impact({target: "symbolName", direction: "upstream"})` before modifying any symbol.** Report blast radius to user. Warn on HIGH/CRITICAL risk.
- **Run `gitnexus_detect_changes()` before committing** to verify only expected symbols/flows are affected.
- Use `gitnexus_query({query: "concept"})` to explore unfamiliar code (not grep).
- Use `gitnexus_context({name: "symbolName"})` for full symbol context (callers, callees, flows).
- Use `gitnexus_rename` for renames — never find-and-replace.

### Resources

| Resource | Purpose |
|----------|---------|
| `gitnexus://repo/omopy/context` | Overview, index freshness |
| `gitnexus://repo/omopy/clusters` | Functional areas |
| `gitnexus://repo/omopy/processes` | Execution flows |
| `gitnexus://repo/omopy/process/{name}` | Step-by-step trace |

### Skills

| Task | Skill file |
|------|------------|
| Architecture | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Debugging | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Refactoring | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tool reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |
<!-- gitnexus:end -->