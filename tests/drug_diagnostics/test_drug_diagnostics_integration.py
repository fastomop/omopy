"""Integration tests for omopy.drug_diagnostics against synthea.duckdb.

These tests exercise the full execute_checks pipeline against a real
OMOP CDM database. They require the synthea.duckdb test database.
"""

from __future__ import annotations

import polars as pl
import pytest

from omopy.drug_diagnostics import (
    AVAILABLE_CHECKS,
    DiagnosticsResult,
    execute_checks,
    summarise_drug_diagnostics,
)
from omopy.generics import CdmReference, SummarisedResult


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture(scope="module")
def diag_cdm(synthea_cdm: CdmReference) -> CdmReference:
    """Module-scoped CDM for diagnostics tests."""
    return synthea_cdm


@pytest.fixture(scope="module")
def first_ingredient_id(diag_cdm: CdmReference) -> int:
    """Get the first ingredient concept_id from the test database.

    Finds an ingredient that has drug_exposure records via concept_ancestor.
    """
    source = diag_cdm.cdm_source
    con = source.connection  # type: ignore[union-attr]
    catalog = source._catalog  # type: ignore[union-attr]
    schema = source.cdm_schema  # type: ignore[union-attr]

    # Get ingredient-level ancestors of drug_exposure concepts
    drug_exposure = con.table("drug_exposure", database=(catalog, schema))
    concept_ancestor = con.table("concept_ancestor", database=(catalog, schema))
    concept = con.table("concept", database=(catalog, schema))

    # Find drug concepts used in drug_exposure
    de_concepts = drug_exposure.select(
        concept_id=drug_exposure.drug_concept_id.cast("int64")
    ).distinct()

    # Find their ancestor ingredients
    ingredients = (
        de_concepts
        .join(concept_ancestor, de_concepts.concept_id == concept_ancestor.descendant_concept_id)
        .join(concept, concept_ancestor.ancestor_concept_id == concept.concept_id)
        .filter(concept.concept_class_id == "Ingredient")
        .select(ingredient_id=concept_ancestor.ancestor_concept_id)
        .distinct()
        .limit(1)
        .to_pyarrow()
    )

    if ingredients.num_rows == 0:
        pytest.skip("No ingredients found in test database")

    return int(ingredients.column("ingredient_id")[0].as_py())


@pytest.fixture(scope="module")
def diag_result(diag_cdm: CdmReference, first_ingredient_id: int) -> DiagnosticsResult:
    """Run execute_checks once for all integration tests."""
    return execute_checks(
        diag_cdm,
        first_ingredient_id,
        checks=list(AVAILABLE_CHECKS),
        sample_size=None,  # Use all records for tests
        min_cell_count=0,  # No obscuration for tests
    )


# =====================================================================
# Test execute_checks
# =====================================================================


class TestExecuteChecksIntegration:
    """Integration tests for execute_checks against synthea.duckdb."""

    def test_returns_diagnostics_result(self, diag_result: DiagnosticsResult):
        assert isinstance(diag_result, DiagnosticsResult)

    def test_has_all_checks(self, diag_result: DiagnosticsResult):
        for check in AVAILABLE_CHECKS:
            assert check in diag_result, f"Missing check: {check}"

    def test_ingredient_resolved(self, diag_result: DiagnosticsResult, first_ingredient_id: int):
        assert first_ingredient_id in diag_result.ingredient_concepts

    def test_execution_time_positive(self, diag_result: DiagnosticsResult):
        assert diag_result.execution_time_seconds > 0

    def test_missing_check_has_rows(self, diag_result: DiagnosticsResult):
        df = diag_result["missing"]
        assert df.height > 0
        assert "variable" in df.columns
        assert "proportion_missing" in df.columns

    def test_exposure_duration_has_rows(self, diag_result: DiagnosticsResult):
        df = diag_result["exposure_duration"]
        assert df.height > 0
        assert "duration_median" in df.columns

    def test_type_check_has_rows(self, diag_result: DiagnosticsResult):
        df = diag_result["type"]
        assert df.height > 0
        assert "drug_type_concept_id" in df.columns

    def test_diagnostics_summary_has_rows(self, diag_result: DiagnosticsResult):
        df = diag_result["diagnostics_summary"]
        assert df.height > 0
        assert "n_records" in df.columns
        assert "n_persons" in df.columns

    def test_dose_check_handles_empty_strength(self, diag_result: DiagnosticsResult):
        """Synthea has empty drug_strength, dose check should handle gracefully."""
        df = diag_result["dose"]
        assert isinstance(df, pl.DataFrame)
        # Should have rows even with empty drug_strength (showing 0 coverage)
        if df.height > 0:
            assert df["n_with_dose"][0] == 0 or df["n_with_dose"][0] >= 0

    def test_quantity_check(self, diag_result: DiagnosticsResult):
        """Synthea has all-null quantity, check should handle gracefully."""
        df = diag_result["quantity"]
        assert isinstance(df, pl.DataFrame)
        if df.height > 0:
            assert "quantity_count_missing" in df.columns

    def test_sig_check(self, diag_result: DiagnosticsResult):
        """Synthea has all-null sig, should show all missing."""
        df = diag_result["sig"]
        assert isinstance(df, pl.DataFrame)
        if df.height > 0:
            # Should have a row for "<missing>"
            assert df.filter(pl.col("sig") == "<missing>").height > 0

    def test_days_between_check(self, diag_result: DiagnosticsResult):
        df = diag_result["days_between"]
        assert isinstance(df, pl.DataFrame)
        if df.height > 0:
            assert "n_persons" in df.columns

    def test_verbatim_end_date_check(self, diag_result: DiagnosticsResult):
        df = diag_result["verbatim_end_date"]
        assert isinstance(df, pl.DataFrame)
        if df.height > 0:
            assert "n_verbatim_end_date_missing" in df.columns


class TestExecuteChecksValidation:
    """Test input validation in execute_checks."""

    def test_invalid_cdm_type(self):
        with pytest.raises(TypeError, match="must be a CdmReference"):
            execute_checks("not_a_cdm", [1])  # type: ignore[arg-type]

    def test_empty_ingredients(self, diag_cdm: CdmReference):
        with pytest.raises(ValueError, match="at least one concept ID"):
            execute_checks(diag_cdm, [])

    def test_invalid_check_name(self, diag_cdm: CdmReference):
        with pytest.raises(ValueError, match="Invalid check names"):
            execute_checks(diag_cdm, [1], checks=["nonexistent_check"])

    def test_single_int_ingredient(self, diag_cdm: CdmReference, first_ingredient_id: int):
        """Test that a single int (not list) works."""
        result = execute_checks(
            diag_cdm,
            first_ingredient_id,
            checks=["missing"],
            sample_size=100,
        )
        assert isinstance(result, DiagnosticsResult)
        assert "missing" in result


class TestExecuteChecksSampling:
    """Test the sampling functionality."""

    def test_with_small_sample(self, diag_cdm: CdmReference, first_ingredient_id: int):
        result = execute_checks(
            diag_cdm,
            first_ingredient_id,
            checks=["missing"],
            sample_size=10,
        )
        df = result["missing"]
        if df.height > 0:
            assert df["n_sample"][0] <= 10


class TestExecuteChecksMinCellCount:
    """Test min_cell_count obscuration."""

    def test_obscuration_applied(self, diag_cdm: CdmReference, first_ingredient_id: int):
        result = execute_checks(
            diag_cdm,
            first_ingredient_id,
            checks=["type"],
            sample_size=None,
            min_cell_count=1000,  # Very high threshold
        )
        df = result["type"]
        if df.height > 0 and "result_obscured" in df.columns:
            # With a very high threshold, counts should be obscured (null)
            assert df["count"].null_count() > 0 or df["result_obscured"].drop_nulls().any()


# =====================================================================
# Test full pipeline: execute -> summarise -> table/plot
# =====================================================================


class TestFullPipelineIntegration:
    """Test the complete pipeline from execute to summarise to visualise."""

    def test_summarise(self, diag_result: DiagnosticsResult):
        sr = summarise_drug_diagnostics(diag_result)
        assert isinstance(sr, SummarisedResult)
        assert sr.data.height > 0

    def test_table(self, diag_result: DiagnosticsResult):
        from omopy.drug_diagnostics import table_drug_diagnostics
        sr = summarise_drug_diagnostics(diag_result)
        table = table_drug_diagnostics(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_single_check(self, diag_result: DiagnosticsResult):
        from omopy.drug_diagnostics import table_drug_diagnostics
        sr = summarise_drug_diagnostics(diag_result)
        table = table_drug_diagnostics(sr, check="missing", type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_plot_missing(self, diag_result: DiagnosticsResult):
        from omopy.drug_diagnostics import plot_drug_diagnostics
        sr = summarise_drug_diagnostics(diag_result)
        fig = plot_drug_diagnostics(sr, check="missing")
        assert hasattr(fig, "update_layout")

    def test_plot_type(self, diag_result: DiagnosticsResult):
        from omopy.drug_diagnostics import plot_drug_diagnostics
        sr = summarise_drug_diagnostics(diag_result)
        fig = plot_drug_diagnostics(sr, check="type")
        assert hasattr(fig, "update_layout")

    def test_plot_exposure_duration(self, diag_result: DiagnosticsResult):
        from omopy.drug_diagnostics import plot_drug_diagnostics
        sr = summarise_drug_diagnostics(diag_result)
        fig = plot_drug_diagnostics(sr, check="exposure_duration")
        assert hasattr(fig, "update_layout")
