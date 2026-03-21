"""Import / export utilities for codelists, concept set expressions, and summarised results.

Mirrors R's ``exportCodelist``, ``importCodelist``, ``exportSummarisedResult``,
``importSummarisedResult``, ``exportConceptSetExpression``, ``importConceptSetExpression``.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.codelist import Codelist, ConceptEntry, ConceptSetExpression
from omopy.generics.summarised_result import (
    SETTINGS_REQUIRED_COLUMNS,
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)

__all__ = [
    "export_codelist",
    "import_codelist",
    "export_concept_set_expression",
    "import_concept_set_expression",
    "export_summarised_result",
    "import_summarised_result",
]


# ---------------------------------------------------------------------------
# Codelist I/O
# ---------------------------------------------------------------------------


def export_codelist(
    codelist: Codelist,
    path: str | Path,
    *,
    format: str = "csv",
) -> Path:
    """Export a Codelist to a file.

    Args:
        codelist: The codelist to export.
        path: Directory to write files to.
        format: ``'csv'`` writes a single CSV; ``'json'`` writes one JSON per codelist
                entry (ATLAS format).
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        rows = []
        for name, ids in codelist.items():
            for cid in ids:
                rows.append({"codelist_name": name, "concept_id": cid})
        out = path / "codelist.csv"
        _write_csv(rows, out)
        return out

    if format == "json":
        for name, ids in codelist.items():
            # ATLAS-style JSON: array of concept objects
            items = [{"concept": {"CONCEPT_ID": cid}} for cid in ids]
            out = path / f"{name}.json"
            out.write_text(json.dumps(items, indent=2), encoding="utf-8")
        return path

    msg = f"Unsupported format: {format!r}. Use 'csv' or 'json'."
    raise ValueError(msg)


def import_codelist(
    path: str | Path,
    *,
    format: str | None = None,
) -> Codelist:
    """Import a Codelist from file(s).

    If *path* is a CSV file, reads it (expects ``codelist_name``, ``concept_id``).
    If *path* is a directory, reads all ``.json`` files as individual concept sets.
    """
    path = Path(path)

    if path.is_file() and (format == "csv" or path.suffix == ".csv"):
        df = pl.read_csv(path)
        result: dict[str, list[int]] = {}
        for row in df.iter_rows(named=True):
            name = str(row["codelist_name"])
            cid = int(row["concept_id"])
            result.setdefault(name, []).append(cid)
        return Codelist(result)

    if path.is_dir() or format == "json":
        target = path if path.is_dir() else path.parent
        result = {}
        for json_file in sorted(target.glob("*.json")):
            name = json_file.stem
            data = json.loads(json_file.read_text(encoding="utf-8"))
            ids = []
            for item in data:
                concept = item.get("concept", item)
                cid = concept.get("CONCEPT_ID", concept.get("concept_id"))
                if cid is not None:
                    ids.append(int(cid))
            result[name] = ids
        return Codelist(result)

    msg = f"Cannot determine format for {path}. Use format='csv' or format='json'."
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# ConceptSetExpression I/O
# ---------------------------------------------------------------------------


def export_concept_set_expression(
    cse: ConceptSetExpression,
    path: str | Path,
    *,
    format: str = "json",
) -> Path:
    """Export a ConceptSetExpression to JSON files (one per concept set)."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    if format == "json":
        for name, entries in cse.items():
            items = []
            for e in entries:
                items.append({
                    "concept": {
                        "CONCEPT_ID": e.concept_id,
                        "CONCEPT_NAME": e.concept_name,
                        "DOMAIN_ID": e.domain_id,
                        "VOCABULARY_ID": e.vocabulary_id,
                        "CONCEPT_CLASS_ID": e.concept_class_id,
                        "STANDARD_CONCEPT": e.standard_concept,
                        "CONCEPT_CODE": e.concept_code,
                    },
                    "isExcluded": e.is_excluded,
                    "includeDescendants": e.include_descendants,
                    "includeMapped": e.include_mapped,
                })
            out = path / f"{name}.json"
            out.write_text(json.dumps(items, indent=2), encoding="utf-8")
        return path

    if format == "csv":
        rows = []
        for name, entries in cse.items():
            for e in entries:
                rows.append({
                    "codelist_name": name,
                    "concept_id": e.concept_id,
                    "concept_name": e.concept_name,
                    "is_excluded": e.is_excluded,
                    "include_descendants": e.include_descendants,
                    "include_mapped": e.include_mapped,
                })
        out = path / "concept_set_expression.csv"
        _write_csv(rows, out)
        return out

    msg = f"Unsupported format: {format!r}. Use 'json' or 'csv'."
    raise ValueError(msg)


def import_concept_set_expression(
    path: str | Path,
    *,
    format: str | None = None,
) -> ConceptSetExpression:
    """Import a ConceptSetExpression from JSON file(s) or a CSV."""
    path = Path(path)

    if path.is_file() and (format == "csv" or path.suffix == ".csv"):
        df = pl.read_csv(path)
        result: dict[str, list[ConceptEntry]] = {}
        for row in df.iter_rows(named=True):
            name = str(row["codelist_name"])
            entry = ConceptEntry(
                concept_id=int(row["concept_id"]),
                concept_name=str(row.get("concept_name", "")),
                is_excluded=bool(row.get("is_excluded", False)),
                include_descendants=bool(row.get("include_descendants", True)),
                include_mapped=bool(row.get("include_mapped", False)),
            )
            result.setdefault(name, []).append(entry)
        return ConceptSetExpression(result)

    # JSON: single file or directory of files
    if path.is_file() and (format == "json" or path.suffix == ".json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        return _parse_cse_json(path.stem, data)

    if path.is_dir() or format == "json":
        target = path if path.is_dir() else path.parent
        result = {}
        for json_file in sorted(target.glob("*.json")):
            name = json_file.stem
            data = json.loads(json_file.read_text(encoding="utf-8"))
            entries = _parse_cse_entries(data)
            result[name] = entries
        return ConceptSetExpression(result)

    msg = f"Cannot determine format for {path}. Use format='csv' or format='json'."
    raise ValueError(msg)


def _parse_cse_json(name: str, data: list[dict[str, Any]]) -> ConceptSetExpression:
    entries = _parse_cse_entries(data)
    return ConceptSetExpression({name: entries})


def _parse_cse_entries(data: list[dict[str, Any]]) -> list[ConceptEntry]:
    entries = []
    for item in data:
        concept = item.get("concept", item)
        entries.append(ConceptEntry(
            concept_id=int(concept.get("CONCEPT_ID", concept.get("concept_id", 0))),
            concept_name=str(concept.get("CONCEPT_NAME", concept.get("concept_name", ""))),
            domain_id=str(concept.get("DOMAIN_ID", concept.get("domain_id", ""))),
            vocabulary_id=str(concept.get("VOCABULARY_ID", concept.get("vocabulary_id", ""))),
            concept_class_id=str(
                concept.get("CONCEPT_CLASS_ID", concept.get("concept_class_id", ""))
            ),
            standard_concept=str(
                concept.get("STANDARD_CONCEPT", concept.get("standard_concept", ""))
            ),
            concept_code=str(concept.get("CONCEPT_CODE", concept.get("concept_code", ""))),
            is_excluded=bool(item.get("isExcluded", item.get("is_excluded", False))),
            include_descendants=bool(
                item.get("includeDescendants", item.get("include_descendants", True))
            ),
            include_mapped=bool(
                item.get("includeMapped", item.get("include_mapped", False))
            ),
        ))
    return entries


# ---------------------------------------------------------------------------
# SummarisedResult I/O
# ---------------------------------------------------------------------------


def export_summarised_result(
    result: SummarisedResult,
    path: str | Path,
    *,
    min_cell_count: int = 5,
) -> Path:
    """Export a SummarisedResult to a CSV file.

    Applies suppression before export. Settings are stored as additional
    rows in the same CSV with a special marker column.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    suppressed = result.suppress(min_cell_count)
    data = suppressed.add_settings()
    data.write_csv(path)
    return path


def import_summarised_result(
    path: str | Path,
) -> SummarisedResult:
    """Import a SummarisedResult from a CSV file."""
    path = Path(path)
    df = pl.read_csv(path, infer_schema_length=10000)

    # Determine which columns belong to the result vs settings
    result_cols = [c for c in SUMMARISED_RESULT_COLUMNS if c in df.columns]
    settings_cols = [c for c in df.columns if c not in SUMMARISED_RESULT_COLUMNS]
    settings_cols = ["result_id"] + [c for c in settings_cols if c != "result_id"]

    data = df.select(result_cols)

    # Extract settings
    if all(c in df.columns for c in SETTINGS_REQUIRED_COLUMNS):
        settings = (
            df.select([c for c in settings_cols if c in df.columns])
            .unique(subset=["result_id"])
        )
    else:
        settings = None

    return SummarisedResult(data, settings=settings)


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
