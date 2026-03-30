"""Tests for omopy.generics._io — import/export round-trips."""

import json
from pathlib import Path

import polars as pl
import pytest

from omopy.generics._io import (
    export_codelist,
    export_concept_set_expression,
    export_summarised_result,
    import_codelist,
    import_concept_set_expression,
    import_summarised_result,
)
from omopy.generics._types import OVERALL
from omopy.generics.codelist import Codelist, ConceptEntry, ConceptSetExpression
from omopy.generics.summarised_result import SummarisedResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_codelist() -> Codelist:
    return Codelist(
        {
            "diabetes": [201826, 442793],
            "hypertension": [316866, 4028741],
        }
    )


def _sample_cse() -> ConceptSetExpression:
    return ConceptSetExpression(
        {
            "diabetes": [
                ConceptEntry(
                    concept_id=201826, concept_name="Type 2 DM", include_descendants=True
                ),
                ConceptEntry(concept_id=442793, concept_name="DM NOS", is_excluded=True),
            ],
            "hypertension": [
                ConceptEntry(concept_id=316866, concept_name="HTN"),
            ],
        }
    )


def _sample_summarised_result() -> SummarisedResult:
    data = pl.DataFrame(
        {
            "result_id": [1, 1, 1],
            "cdm_name": ["test"] * 3,
            "group_name": [OVERALL] * 3,
            "group_level": [OVERALL] * 3,
            "strata_name": [OVERALL] * 3,
            "strata_level": [OVERALL] * 3,
            "variable_name": ["number subjects", "age", "age"],
            "variable_level": [None, None, None],
            "estimate_name": ["count", "mean", "sd"],
            "estimate_type": ["integer", "numeric", "numeric"],
            "estimate_value": ["100", "55.3", "12.1"],
            "additional_name": [OVERALL] * 3,
            "additional_level": [OVERALL] * 3,
        }
    )
    settings = pl.DataFrame(
        {
            "result_id": [1],
            "result_type": ["demographics"],
            "package_name": ["omopy"],
            "package_version": ["0.1.0"],
        }
    )
    return SummarisedResult(data, settings=settings)


# ---------------------------------------------------------------------------
# Codelist CSV round-trip
# ---------------------------------------------------------------------------


class TestCodelistCsv:
    def test_round_trip(self, tmp_path: Path):
        cl = _sample_codelist()
        out = export_codelist(cl, tmp_path, format="csv")
        assert out.exists()
        assert out.suffix == ".csv"

        imported = import_codelist(out)
        assert isinstance(imported, Codelist)
        assert set(imported.keys()) == {"diabetes", "hypertension"}
        assert sorted(imported["diabetes"]) == sorted(cl["diabetes"])
        assert sorted(imported["hypertension"]) == sorted(cl["hypertension"])


# ---------------------------------------------------------------------------
# Codelist JSON round-trip
# ---------------------------------------------------------------------------


class TestCodelistJson:
    def test_round_trip(self, tmp_path: Path):
        cl = _sample_codelist()
        export_codelist(cl, tmp_path, format="json")

        # Should have one JSON per codelist name
        json_files = list(tmp_path.glob("*.json"))
        assert len(json_files) == 2

        imported = import_codelist(tmp_path, format="json")
        assert set(imported.keys()) == {"diabetes", "hypertension"}
        assert sorted(imported["diabetes"]) == sorted(cl["diabetes"])


# ---------------------------------------------------------------------------
# Codelist unsupported format
# ---------------------------------------------------------------------------


class TestCodelistFormats:
    def test_export_unsupported(self, tmp_path: Path):
        with pytest.raises(ValueError, match="Unsupported format"):
            export_codelist(_sample_codelist(), tmp_path, format="xml")

    def test_import_unrecognised(self, tmp_path: Path):
        (tmp_path / "test.xyz").write_text("data")
        with pytest.raises(ValueError, match="Cannot determine format"):
            import_codelist(tmp_path / "test.xyz")


# ---------------------------------------------------------------------------
# ConceptSetExpression JSON round-trip
# ---------------------------------------------------------------------------


class TestConceptSetExpressionJson:
    def test_round_trip(self, tmp_path: Path):
        cse = _sample_cse()
        export_concept_set_expression(cse, tmp_path)

        imported = import_concept_set_expression(tmp_path)
        assert set(imported.keys()) == {"diabetes", "hypertension"}
        assert len(imported["diabetes"]) == 2

        # Check that flags are preserved
        dm_entries = imported["diabetes"]
        excluded = [e for e in dm_entries if e.is_excluded]
        assert len(excluded) == 1
        assert excluded[0].concept_id == 442793

    def test_single_file_import(self, tmp_path: Path):
        cse = _sample_cse()
        export_concept_set_expression(cse, tmp_path)

        # Import a single JSON file
        single = import_concept_set_expression(tmp_path / "diabetes.json")
        assert "diabetes" in single
        assert len(single["diabetes"]) == 2


# ---------------------------------------------------------------------------
# ConceptSetExpression CSV round-trip
# ---------------------------------------------------------------------------


class TestConceptSetExpressionCsv:
    def test_round_trip(self, tmp_path: Path):
        cse = _sample_cse()
        out = export_concept_set_expression(cse, tmp_path, format="csv")
        assert out.exists()

        imported = import_concept_set_expression(out)
        assert set(imported.keys()) == {"diabetes", "hypertension"}

    def test_export_unsupported(self, tmp_path: Path):
        with pytest.raises(ValueError, match="Unsupported format"):
            export_concept_set_expression(_sample_cse(), tmp_path, format="xml")


# ---------------------------------------------------------------------------
# SummarisedResult round-trip
# ---------------------------------------------------------------------------


class TestSummarisedResultIO:
    def test_round_trip(self, tmp_path: Path):
        sr = _sample_summarised_result()
        out = export_summarised_result(sr, tmp_path / "result.csv", min_cell_count=0)
        assert out.exists()

        imported = import_summarised_result(out)
        assert isinstance(imported, SummarisedResult)
        assert len(imported) == 3

    def test_round_trip_with_suppression(self, tmp_path: Path):
        sr = _sample_summarised_result()
        out = export_summarised_result(sr, tmp_path / "result.csv", min_cell_count=200)
        imported = import_summarised_result(out)
        # The "number subjects" count=100 should be suppressed (0 < 100 < 200)
        vals = [str(v) for v in imported.data["estimate_value"].to_list()]
        assert "-" in vals

    def test_settings_preserved(self, tmp_path: Path):
        sr = _sample_summarised_result()
        out = export_summarised_result(sr, tmp_path / "result.csv", min_cell_count=0)
        imported = import_summarised_result(out)
        assert "result_type" in imported.settings.columns
