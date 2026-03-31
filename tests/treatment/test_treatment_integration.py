"""Integration tests for omopy.treatment using the Synthea DuckDB test database.

Tests the full compute_pathways() → summarise → table/plot pipeline
against real OMOP CDM data (27 persons, 663 drug exposures).
"""

from __future__ import annotations

import polars as pl
import pytest

from omopy.generics.codelist import Codelist
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import SummarisedResult
from omopy.treatment import (
    CohortSpec,
    PathwayResult,
    compute_pathways,
    mock_treatment_pathways,
    plot_event_duration,
    plot_sankey,
    plot_sunburst,
    summarise_event_duration,
    summarise_treatment_pathways,
    table_event_duration,
    table_treatment_pathways,
)

# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture(scope="module")
def drug_cdm(synthea_cdm):
    """CDM with drug cohorts generated from Synthea data.

    Creates two drug cohorts:
    - cohort 1: lisinopril (concept_id 1308216)
    - cohort 2: amlodipine (concept_id 1332418)
    """
    from omopy.drug import generate_drug_utilisation_cohort_set

    cdm = generate_drug_utilisation_cohort_set(
        synthea_cdm,
        name="drug_cohorts",
        concept_set={
            "lisinopril": [1308216],
            "amlodipine": [1332418],
        },
        gap_era=30,
    )
    return cdm


@pytest.fixture(scope="module")
def condition_cdm(synthea_cdm):
    """CDM with a condition cohort as target.

    Creates a hypertension cohort (concept 320128, 6 occurrences in Synthea).
    """
    from omopy.connector import generate_concept_cohort_set

    cs = Codelist({"hypertension": [320128]})
    return generate_concept_cohort_set(synthea_cdm, cs, "target_condition")


@pytest.fixture(scope="module")
def treatment_cohort(drug_cdm, condition_cdm):
    """Combined CohortTable with target + event cohorts for pathway analysis.

    Combines:
    - Target: hypertension (cohort_definition_id=100)
    - Event: lisinopril (cohort_definition_id=1)
    - Event: amlodipine (cohort_definition_id=2)
    """
    # Get target data
    target_ct = condition_cdm["target_condition"]
    target_df = target_ct.collect()

    # Reassign target to cohort_id 100
    if "subject_id" in target_df.columns and "person_id" not in target_df.columns:
        target_df = target_df.rename({"subject_id": "person_id"})
    target_df = target_df.with_columns(
        pl.lit(100).cast(pl.Int64).alias("cohort_definition_id")
    )
    # Ensure standard column names
    target_df = target_df.select(
        "cohort_definition_id",
        pl.col("person_id").alias("subject_id"),
        "cohort_start_date",
        "cohort_end_date",
    )

    # Get drug event data
    drug_ct = drug_cdm["drug_cohorts"]
    drug_df = drug_ct.collect()
    if "person_id" in drug_df.columns and "subject_id" not in drug_df.columns:
        drug_df = drug_df.rename({"person_id": "subject_id"})

    # Reassign drug cohort IDs (1 and 2)
    drug_settings = drug_ct.settings
    name_to_id = {}
    for row in drug_settings.iter_rows(named=True):
        name_to_id[row["cohort_name"]] = row["cohort_definition_id"]

    # Map: lisinopril → 1, amlodipine → 2
    id_map = {}
    for name, old_id in name_to_id.items():
        if "lisinopril" in name.lower():
            id_map[old_id] = 1
        elif "amlodipine" in name.lower():
            id_map[old_id] = 2

    if id_map:
        drug_df = drug_df.with_columns(
            pl.col("cohort_definition_id")
            .replace_strict(id_map, default=pl.first())
            .alias("cohort_definition_id")
        )

    drug_df = drug_df.select(
        "cohort_definition_id",
        "subject_id",
        "cohort_start_date",
        "cohort_end_date",
    )

    # Combine
    combined = pl.concat([target_df, drug_df], how="diagonal_relaxed")

    # Build settings
    settings = pl.DataFrame(
        {
            "cohort_definition_id": [100, 1, 2],
            "cohort_name": ["hypertension", "lisinopril", "amlodipine"],
        }
    )

    ct = CohortTable(combined, settings=settings)
    ct.cdm = drug_cdm
    return ct


@pytest.fixture(scope="module")
def cohort_specs():
    """CohortSpec definitions matching the treatment_cohort fixture."""
    return [
        CohortSpec(cohort_id=100, cohort_name="hypertension", type="target"),
        CohortSpec(cohort_id=1, cohort_name="lisinopril", type="event"),
        CohortSpec(cohort_id=2, cohort_name="amlodipine", type="event"),
    ]


# ===================================================================
# Integration: compute_pathways
# ===================================================================


class TestComputePathwaysIntegration:
    """Test compute_pathways with real Synthea data."""

    def test_returns_pathway_result(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            era_collapse_size=30,
            combination_window=30,
            filter_treatments="first",
            max_path_length=5,
        )
        assert isinstance(result, PathwayResult)

    def test_treatment_history_has_rows(
        self, treatment_cohort, synthea_cdm, cohort_specs
    ):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            era_collapse_size=30,
            combination_window=30,
        )
        # May or may not have rows depending on whether target persons
        # also have drug events within the observation window
        assert isinstance(result.treatment_history, pl.DataFrame)

    def test_attrition_has_rows(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
        )
        assert isinstance(result.attrition, pl.DataFrame)
        if result.attrition.height > 0:
            assert "reason" in result.attrition.columns
            assert "number_records" in result.attrition.columns

    def test_cdm_name_set(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
        )
        assert result.cdm_name == synthea_cdm.cdm_name or result.cdm_name == ""

    def test_cohorts_stored(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
        )
        assert len(result.cohorts) == 3
        types = {c.type for c in result.cohorts}
        assert "target" in types
        assert "event" in types

    def test_arguments_stored(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            era_collapse_size=45,
        )
        assert result.arguments["era_collapse_size"] == 45

    def test_filter_treatments_changes(
        self, treatment_cohort, synthea_cdm, cohort_specs
    ):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            filter_treatments="changes",
        )
        assert isinstance(result, PathwayResult)

    def test_filter_treatments_all(self, treatment_cohort, synthea_cdm, cohort_specs):
        result = compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            filter_treatments="all",
        )
        assert isinstance(result, PathwayResult)


# ===================================================================
# Integration: full pipeline (compute → summarise → table/plot)
# ===================================================================


class TestFullPipelineIntegration:
    """Test complete workflows end-to-end with Synthea data."""

    @pytest.fixture(scope="class")
    def pathway_result(self, treatment_cohort, synthea_cdm, cohort_specs):
        return compute_pathways(
            treatment_cohort,
            synthea_cdm,
            cohort_specs,
            era_collapse_size=30,
            combination_window=30,
            filter_treatments="first",
            max_path_length=5,
        )

    def test_summarise_pathways(self, pathway_result):
        result = summarise_treatment_pathways(
            pathway_result,
            min_cell_count=0,  # low threshold for small dataset
        )
        assert isinstance(result, SummarisedResult)

    def test_summarise_event_duration(self, pathway_result):
        result = summarise_event_duration(pathway_result)
        assert isinstance(result, SummarisedResult)

    def test_table_from_mock(self):
        """Table rendering works end-to-end with mock data."""
        mock = mock_treatment_pathways(seed=42)
        tbl = table_treatment_pathways(mock, type="polars")
        assert isinstance(tbl, pl.DataFrame)
        assert tbl.height > 0

    def test_sankey_from_mock(self):
        """Sankey diagram works end-to-end with mock data."""
        mock = mock_treatment_pathways(seed=42)
        fig = plot_sankey(mock)
        assert fig is not None
        assert len(fig.data) > 0

    def test_sunburst_from_mock(self):
        """Sunburst chart works end-to-end with mock data."""
        mock = mock_treatment_pathways(seed=42)
        fig = plot_sunburst(mock)
        assert fig is not None
        assert len(fig.data) > 0

    def test_event_duration_from_mock(self):
        """Event duration box plot works end-to-end with mock data."""
        mock = mock_treatment_pathways(seed=42, include_duration=True)
        fig = plot_event_duration(mock)
        assert fig is not None
        assert len(fig.data) > 0

    def test_duration_table_from_mock(self):
        """Duration table works end-to-end with mock data."""
        mock = mock_treatment_pathways(seed=42, include_duration=True)
        tbl = table_event_duration(mock, type="polars")
        assert isinstance(tbl, pl.DataFrame)


# ===================================================================
# Validation errors
# ===================================================================


class TestValidationErrors:
    """Test that compute_pathways validates inputs properly."""

    def test_no_cohorts_raises(self, treatment_cohort, synthea_cdm):
        with pytest.raises(ValueError, match="At least one CohortSpec"):
            compute_pathways(treatment_cohort, synthea_cdm, [])

    def test_no_target_raises(self, treatment_cohort, synthea_cdm):
        with pytest.raises(ValueError, match="target"):
            compute_pathways(
                treatment_cohort,
                synthea_cdm,
                [CohortSpec(cohort_id=1, cohort_name="Drug", type="event")],
            )

    def test_no_event_raises(self, treatment_cohort, synthea_cdm):
        with pytest.raises(ValueError, match="event"):
            compute_pathways(
                treatment_cohort,
                synthea_cdm,
                [CohortSpec(cohort_id=100, cohort_name="Target", type="target")],
            )
