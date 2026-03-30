"""Tests for omopy.treatment — Treatment pathway analysis module.

Tests cover:
- Module imports and exports
- CohortSpec and PathwayResult type validation
- Mock data generation
- Core pipeline steps:
  - Era collapse algorithm
  - Combination window algorithm
  - Filter treatments (first / changes / all)
  - Pathway string building
  - Finalize pathways (name resolution, max path length)
- summarise_treatment_pathways()
- summarise_event_duration()
- Table rendering functions
- Plot rendering functions (Sankey, sunburst, event duration)
- Edge cases (empty data, single person, etc.)
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

import omopy
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)
from omopy.treatment._pathway import (
    CohortSpec,
    PathwayResult,
    _combination_window,
    _era_collapse,
    _filter_treatments,
    _finalize_pathways,
    _make_combination_id,
)
from omopy.treatment._summarise import (
    _add_age_group,
    _build_pathway_strings,
)


# ===================================================================
# Helpers — small DataFrames for unit-testing pipeline steps
# ===================================================================


def _make_events(
    rows: list[dict],
    *,
    type_col: str = "event",
) -> pl.DataFrame:
    """Build a minimal treatment history DataFrame from row dicts.

    Each dict should contain at least: person_id, event_cohort_id,
    event_start_date, event_end_date.  Missing columns get defaults.
    """
    for r in rows:
        r.setdefault("person_id", 1)
        r.setdefault("index_year", 2020)
        r.setdefault("age", 50)
        r.setdefault("sex", "Male")
        r.setdefault("target_cohort_id", 100)
        r.setdefault("target_cohort_name", "target")
        r.setdefault("n_target", 1)
        r.setdefault("type", type_col)
        # Compute duration if not set
        if "duration_era" not in r:
            r["duration_era"] = (r["event_end_date"] - r["event_start_date"]).days

    return pl.DataFrame(
        rows,
        schema_overrides={
            "person_id": pl.Int64,
            "event_cohort_id": pl.Utf8,
            "event_start_date": pl.Date,
            "event_end_date": pl.Date,
            "duration_era": pl.Int64,
            "index_year": pl.Int32,
            "age": pl.Int64,
            "target_cohort_id": pl.Int64,
            "n_target": pl.Int32,
        },
    )


D = datetime.date


# ===================================================================
# Module imports
# ===================================================================


class TestModuleImports:
    """Verify all exports are importable and callable."""

    def test_import_module(self):
        import omopy.treatment

        assert hasattr(omopy.treatment, "__all__")

    def test_export_count(self):
        import omopy.treatment

        assert len(omopy.treatment.__all__) == 11

    def test_all_exports_accessible(self):
        import omopy.treatment

        for name in omopy.treatment.__all__:
            obj = getattr(omopy.treatment, name)
            assert obj is not None, f"{name} is None"

    def test_callable_exports(self):
        """Non-type exports should be callable."""
        import omopy.treatment

        for name in omopy.treatment.__all__:
            obj = getattr(omopy.treatment, name)
            assert callable(obj), f"{name} is not callable"

    def test_individual_imports(self):
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

        assert callable(compute_pathways)
        assert callable(summarise_treatment_pathways)
        assert callable(summarise_event_duration)
        assert callable(table_treatment_pathways)
        assert callable(table_event_duration)
        assert callable(plot_sankey)
        assert callable(plot_sunburst)
        assert callable(plot_event_duration)
        assert callable(mock_treatment_pathways)


# ===================================================================
# CohortSpec
# ===================================================================


class TestCohortSpec:
    """Test CohortSpec Pydantic model."""

    def test_target_cohort(self):
        cs = CohortSpec(cohort_id=1, cohort_name="HTN", type="target")
        assert cs.cohort_id == 1
        assert cs.cohort_name == "HTN"
        assert cs.type == "target"

    def test_event_cohort(self):
        cs = CohortSpec(cohort_id=2, cohort_name="DrugA", type="event")
        assert cs.type == "event"

    def test_exit_cohort(self):
        cs = CohortSpec(cohort_id=3, cohort_name="Death", type="exit")
        assert cs.type == "exit"

    def test_invalid_type_raises(self):
        with pytest.raises(Exception):
            CohortSpec(cohort_id=1, cohort_name="X", type="invalid")

    def test_frozen(self):
        cs = CohortSpec(cohort_id=1, cohort_name="X", type="target")
        with pytest.raises(Exception):
            cs.cohort_id = 99

    def test_equality(self):
        a = CohortSpec(cohort_id=1, cohort_name="X", type="target")
        b = CohortSpec(cohort_id=1, cohort_name="X", type="target")
        assert a == b

    def test_hash(self):
        a = CohortSpec(cohort_id=1, cohort_name="X", type="target")
        b = CohortSpec(cohort_id=1, cohort_name="X", type="target")
        assert hash(a) == hash(b)
        assert len({a, b}) == 1


# ===================================================================
# PathwayResult
# ===================================================================


class TestPathwayResult:
    """Test PathwayResult Pydantic model."""

    def test_construction(self):
        th = pl.DataFrame({"person_id": [1], "event_seq": [1]})
        att = pl.DataFrame({"reason": ["start"]})
        pr = PathwayResult(
            treatment_history=th,
            attrition=att,
            cohorts=(CohortSpec(cohort_id=1, cohort_name="T", type="target"),),
            cdm_name="test",
            arguments={"key": "val"},
        )
        assert pr.cdm_name == "test"
        assert pr.treatment_history.height == 1
        assert len(pr.cohorts) == 1

    def test_frozen(self):
        th = pl.DataFrame({"person_id": [1]})
        att = pl.DataFrame()
        pr = PathwayResult(
            treatment_history=th,
            attrition=att,
            cohorts=(),
            cdm_name="x",
            arguments={},
        )
        with pytest.raises(Exception):
            pr.cdm_name = "y"


# ===================================================================
# _make_combination_id
# ===================================================================


class TestMakeCombinationId:
    """Test combination ID creation."""

    def test_simple_pair(self):
        assert _make_combination_id("1", "3") == "1+3"

    def test_sorted(self):
        assert _make_combination_id("3", "1") == "1+3"

    def test_nested_combination(self):
        # If one is already a combo like "1+2", merge all unique parts
        assert _make_combination_id("1+2", "3") == "1+2+3"

    def test_overlapping_ids(self):
        # "1+2" combined with "2" should deduplicate
        assert _make_combination_id("1+2", "2") == "1+2"

    def test_same_id(self):
        assert _make_combination_id("5", "5") == "5"


# ===================================================================
# _era_collapse
# ===================================================================


class TestEraCollapse:
    """Test the era collapse algorithm."""

    def test_no_events(self):
        df = _make_events([])
        result = _era_collapse(df, era_collapse_size=30)
        assert result.height == 0

    def test_single_event_unchanged(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=30)
        assert result.height == 1

    def test_merge_same_drug_within_gap(self):
        """Two eras of same drug separated by 10 days (< 30) should merge."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 2, 10),
                    "event_end_date": D(2020, 3, 10),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=30)
        events = result.filter(pl.col("type") == "event")
        assert events.height == 1
        assert events["event_start_date"][0] == D(2020, 1, 1)
        assert events["event_end_date"][0] == D(2020, 3, 10)

    def test_no_merge_different_drugs(self):
        """Two eras of different drugs should NOT merge even if gap < threshold."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 5),
                    "event_end_date": D(2020, 3, 5),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=30)
        events = result.filter(pl.col("type") == "event")
        assert events.height == 2

    def test_no_merge_large_gap(self):
        """Same drug but gap > threshold should NOT merge."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 4, 1),
                    "event_end_date": D(2020, 5, 1),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=30)
        events = result.filter(pl.col("type") == "event")
        assert events.height == 2

    def test_merge_three_eras_chained(self):
        """Three eras of same drug, each within gap of previous — all merge."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 20),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 30),
                    "event_end_date": D(2020, 2, 15),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 2, 25),
                    "event_end_date": D(2020, 3, 15),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=15)
        events = result.filter(pl.col("type") == "event")
        assert events.height == 1
        assert events["event_start_date"][0] == D(2020, 1, 1)
        assert events["event_end_date"][0] == D(2020, 3, 15)

    def test_exits_preserved(self):
        """Exit-type rows should pass through unchanged."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        exit_df = _make_events(
            [
                {
                    "event_cohort_id": "99",
                    "event_start_date": D(2020, 6, 1),
                    "event_end_date": D(2020, 6, 30),
                },
            ],
            type_col="exit",
        )
        combined = pl.concat([df, exit_df], how="diagonal_relaxed")
        result = _era_collapse(combined, era_collapse_size=30)
        exits = result.filter(pl.col("type") == "exit")
        assert exits.height == 1

    def test_zero_era_collapse_size(self):
        """With era_collapse_size=0, only overlapping eras merge."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 31),
                    "event_end_date": D(2020, 2, 28),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=0)
        events = result.filter(pl.col("type") == "event")
        # Gap is 0 days, which is <= 0 so should merge
        assert events.height == 1

    def test_multiple_persons(self):
        """Collapse should respect person boundaries."""
        df = _make_events(
            [
                {
                    "person_id": 1,
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 20),
                },
                {
                    "person_id": 1,
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 25),
                    "event_end_date": D(2020, 2, 15),
                },
                {
                    "person_id": 2,
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 20),
                },
                {
                    "person_id": 2,
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 25),
                    "event_end_date": D(2020, 2, 15),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=10)
        events = result.filter(pl.col("type") == "event")
        # Each person's two eras merge → 2 total
        assert events.height == 2


# ===================================================================
# _combination_window
# ===================================================================


class TestCombinationWindow:
    """Test the combination window algorithm."""

    def test_no_events(self):
        df = _make_events([])
        result = _combination_window(
            df, combination_window=30, min_post_combination_duration=0, overlap_method="truncate"
        )
        assert result.height == 0

    def test_no_overlap(self):
        """Sequential eras with no overlap should pass through."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
            ]
        )
        result = _combination_window(
            df, combination_window=30, min_post_combination_duration=0, overlap_method="truncate"
        )
        events = result.filter(pl.col("type") == "event")
        assert events.height == 2

    def test_small_overlap_truncate(self):
        """Overlap < combination_window with truncate method should clip previous era."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 2, 15),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 10),
                    "event_end_date": D(2020, 3, 10),
                },
            ]
        )
        # Overlap is 5 days (Feb 10-15), combination_window=30 → switch
        result = _combination_window(
            df, combination_window=30, min_post_combination_duration=0, overlap_method="truncate"
        )
        events = result.filter(pl.col("type") == "event")
        # Previous era should be truncated to end at Feb 10
        drug1 = events.filter(pl.col("event_cohort_id") == "1")
        assert drug1.height == 1
        assert drug1["event_end_date"][0] == D(2020, 2, 10)

    def test_large_overlap_creates_combination(self):
        """Overlap >= combination_window creates combo segment (FRFS case)."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 3, 1),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 1, 15),
                    "event_end_date": D(2020, 4, 1),
                },
            ]
        )
        # Overlap: Jan 15 to Mar 1 = 45 days, combination_window=30 → combination
        result = _combination_window(
            df, combination_window=30, min_post_combination_duration=0, overlap_method="truncate"
        )
        events = result.filter(pl.col("type") == "event")
        combo_rows = events.filter(pl.col("event_cohort_id").str.contains(r"\+"))
        assert combo_rows.height >= 1

    def test_exits_preserved(self):
        """Exit rows should pass through combination window unchanged."""
        event_df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        exit_df = _make_events(
            [
                {
                    "event_cohort_id": "99",
                    "event_start_date": D(2020, 6, 1),
                    "event_end_date": D(2020, 6, 30),
                },
            ],
            type_col="exit",
        )
        combined = pl.concat([event_df, exit_df], how="diagonal_relaxed")
        result = _combination_window(
            combined,
            combination_window=30,
            min_post_combination_duration=0,
            overlap_method="truncate",
        )
        exits = result.filter(pl.col("type") == "exit")
        assert exits.height == 1


# ===================================================================
# _filter_treatments
# ===================================================================


class TestFilterTreatments:
    """Test treatment filtering strategies."""

    def _sorted_events(self) -> pl.DataFrame:
        return _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 3, 1),
                    "event_end_date": D(2020, 3, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 4, 1),
                    "event_end_date": D(2020, 4, 30),
                },
            ]
        )

    def test_all_keeps_everything(self):
        df = self._sorted_events()
        result = _filter_treatments(df, "all")
        events = result.filter(pl.col("type") == "event")
        assert events.height == 4

    def test_first_keeps_first_occurrence(self):
        df = self._sorted_events()
        result = _filter_treatments(df, "first")
        events = result.filter(pl.col("type") == "event")
        # drug "1" appears twice, drug "2" twice → keep first of each = 2
        assert events.height == 2

    def test_changes_removes_consecutive_dupes(self):
        """A-A-B-B → A-B"""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 3, 1),
                    "event_end_date": D(2020, 3, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 4, 1),
                    "event_end_date": D(2020, 4, 30),
                },
            ]
        )
        result = _filter_treatments(df, "changes")
        events = result.filter(pl.col("type") == "event")
        assert events.height == 2

    def test_changes_keeps_non_consecutive(self):
        """A-B-A keeps all 3 because A→B→A are all changes."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 3, 1),
                    "event_end_date": D(2020, 3, 31),
                },
            ]
        )
        result = _filter_treatments(df, "changes")
        events = result.filter(pl.col("type") == "event")
        assert events.height == 3

    def test_empty_input(self):
        df = _make_events([])
        for strategy in ("all", "first", "changes"):
            result = _filter_treatments(df, strategy)
            assert result.height == 0

    def test_exits_preserved(self):
        """Exits should pass through all filter strategies."""
        event_df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        exit_df = _make_events(
            [
                {
                    "event_cohort_id": "99",
                    "event_start_date": D(2020, 6, 1),
                    "event_end_date": D(2020, 6, 30),
                },
            ],
            type_col="exit",
        )
        combined = pl.concat([event_df, exit_df], how="diagonal_relaxed")
        for strategy in ("all", "first", "changes"):
            result = _filter_treatments(combined, strategy)
            exits = result.filter(pl.col("type") == "exit")
            assert exits.height == 1


# ===================================================================
# _finalize_pathways
# ===================================================================


class TestFinalizePathways:
    """Test pathway finalisation (sequencing, name resolution, truncation)."""

    def test_assigns_event_seq(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
            ]
        )
        cohort_names = {1: "DrugA", 2: "DrugB"}
        result = _finalize_pathways(df, max_path_length=5, cohort_names=cohort_names)
        assert "event_seq" in result.columns
        seqs = result.sort("event_start_date")["event_seq"].to_list()
        assert seqs == [1, 2]

    def test_resolves_names(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        cohort_names = {1: "Aspirin"}
        result = _finalize_pathways(df, max_path_length=5, cohort_names=cohort_names)
        assert "event_cohort_name" in result.columns
        assert result["event_cohort_name"][0] == "Aspirin"

    def test_resolves_combination_names(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1+2",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        cohort_names = {1: "Aspirin", 2: "Metformin"}
        result = _finalize_pathways(df, max_path_length=5, cohort_names=cohort_names)
        assert result["event_cohort_name"][0] == "Aspirin+Metformin"

    def test_max_path_length_truncation(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "2",
                    "event_start_date": D(2020, 2, 1),
                    "event_end_date": D(2020, 2, 28),
                },
                {
                    "event_cohort_id": "3",
                    "event_start_date": D(2020, 3, 1),
                    "event_end_date": D(2020, 3, 31),
                },
            ]
        )
        cohort_names = {1: "A", 2: "B", 3: "C"}
        result = _finalize_pathways(df, max_path_length=2, cohort_names=cohort_names)
        assert result.height == 2  # Only first 2 events

    def test_empty_df(self):
        df = _make_events([])
        result = _finalize_pathways(df, max_path_length=5, cohort_names={})
        assert result.height == 0
        assert "event_seq" in result.columns
        assert "event_cohort_name" in result.columns


# ===================================================================
# _build_pathway_strings
# ===================================================================


class TestBuildPathwayStrings:
    """Test pathway string construction."""

    def test_basic_pathway(self):
        df = pl.DataFrame(
            {
                "person_id": [1, 1],
                "target_cohort_id": [100, 100],
                "event_cohort_name": ["DrugA", "DrugB"],
                "event_seq": [1, 2],
                "age": [50, 50],
                "sex": ["Male", "Male"],
                "index_year": [2020, 2020],
            }
        )
        result = _build_pathway_strings(df)
        assert result.height == 1
        assert result["pathway"][0] == "DrugA-DrugB"

    def test_multiple_persons(self):
        df = pl.DataFrame(
            {
                "person_id": [1, 1, 2],
                "target_cohort_id": [100, 100, 100],
                "event_cohort_name": ["DrugA", "DrugB", "DrugC"],
                "event_seq": [1, 2, 1],
                "age": [50, 50, 60],
                "sex": ["Male", "Male", "Female"],
                "index_year": [2020, 2020, 2021],
            }
        )
        result = _build_pathway_strings(df)
        assert result.height == 2

    def test_empty_history(self):
        df = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "target_cohort_id": pl.Int64,
                "event_cohort_name": pl.Utf8,
                "event_seq": pl.Int32,
                "age": pl.Int64,
                "sex": pl.Utf8,
                "index_year": pl.Int32,
            }
        )
        result = _build_pathway_strings(df)
        assert result.height == 0

    def test_single_step_pathway(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "target_cohort_id": [100],
                "event_cohort_name": ["DrugA"],
                "event_seq": [1],
                "age": [50],
                "sex": ["Male"],
                "index_year": [2020],
            }
        )
        result = _build_pathway_strings(df)
        assert result.height == 1
        assert result["pathway"][0] == "DrugA"


# ===================================================================
# _add_age_group
# ===================================================================


class TestAddAgeGroup:
    """Test age binning."""

    def test_integer_window(self):
        df = pl.DataFrame({"age": [25, 35, 45, 55, 65]})
        result = _add_age_group(df, age_window=10)
        assert "age_group" in result.columns
        assert result.height == 5

    def test_missing_age_column(self):
        df = pl.DataFrame({"x": [1, 2, 3]})
        result = _add_age_group(df, age_window=10)
        assert "age_group" in result.columns
        assert result["age_group"][0] == "all"

    def test_list_breakpoints(self):
        df = pl.DataFrame({"age": [25, 45, 65]})
        result = _add_age_group(df, age_window=[0, 30, 60, 100])
        assert "age_group" in result.columns
        assert result.height == 3


# ===================================================================
# Mock data
# ===================================================================


class TestMockTreatmentPathways:
    """Test mock_treatment_pathways generator."""

    def test_returns_summarised_result(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        assert isinstance(result, SummarisedResult)

    def test_has_standard_columns(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in result.data.columns, f"Missing column: {col}"

    def test_has_pathway_result_type(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        types = result.settings["result_type"].to_list()
        assert "summarise_treatment_pathways" in types

    def test_has_duration_result_type_by_default(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        types = result.settings["result_type"].to_list()
        assert "summarise_event_duration" in types

    def test_no_duration_when_disabled(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways(include_duration=False)
        types = result.settings["result_type"].to_list()
        assert "summarise_event_duration" not in types

    def test_deterministic(self):
        from omopy.treatment import mock_treatment_pathways

        r1 = mock_treatment_pathways(seed=42)
        r2 = mock_treatment_pathways(seed=42)
        assert r1.data.equals(r2.data)

    def test_different_seeds(self):
        from omopy.treatment import mock_treatment_pathways

        r1 = mock_treatment_pathways(seed=1)
        r2 = mock_treatment_pathways(seed=2)
        assert not r1.data.equals(r2.data)

    def test_custom_n_targets(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways(n_targets=3)
        group_levels = result.data["group_level"].unique().to_list()
        target_names = [g for g in group_levels if g.startswith("target_")]
        assert len(target_names) == 3

    def test_has_pathway_rows(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        pathway_rows = result.data.filter(pl.col("variable_name") == "treatment_pathway")
        assert pathway_rows.height > 0

    def test_has_count_rows(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        count_rows = result.data.filter(pl.col("variable_name") == "Number records")
        assert count_rows.height > 0

    def test_has_percentage_rows(self):
        from omopy.treatment import mock_treatment_pathways

        result = mock_treatment_pathways()
        pct_rows = result.data.filter(pl.col("estimate_name") == "percentage")
        assert pct_rows.height > 0


# ===================================================================
# summarise_treatment_pathways
# ===================================================================


class TestSummariseTreatmentPathways:
    """Test summarise_treatment_pathways (from PathwayResult)."""

    @pytest.fixture()
    def simple_pathway_result(self):
        """Minimal PathwayResult for testing summarise."""
        history = pl.DataFrame(
            {
                "person_id": [1, 1, 2, 2, 3],
                "index_year": [2020, 2020, 2020, 2020, 2021],
                "event_cohort_id": ["1", "2", "1", "3", "1"],
                "event_cohort_name": ["DrugA", "DrugB", "DrugA", "DrugC", "DrugA"],
                "event_start_date": [
                    D(2020, 1, 1),
                    D(2020, 2, 1),
                    D(2020, 1, 15),
                    D(2020, 3, 1),
                    D(2021, 1, 1),
                ],
                "event_end_date": [
                    D(2020, 1, 31),
                    D(2020, 2, 28),
                    D(2020, 2, 15),
                    D(2020, 3, 31),
                    D(2021, 1, 31),
                ],
                "duration_era": [30, 27, 31, 30, 30],
                "event_seq": [1, 2, 1, 2, 1],
                "age": [50, 50, 60, 60, 70],
                "sex": ["Male", "Male", "Female", "Female", "Male"],
                "target_cohort_id": [100, 100, 100, 100, 100],
                "target_cohort_name": ["Target", "Target", "Target", "Target", "Target"],
            }
        )
        return PathwayResult(
            treatment_history=history,
            attrition=pl.DataFrame(),
            cohorts=(
                CohortSpec(cohort_id=100, cohort_name="Target", type="target"),
                CohortSpec(cohort_id=1, cohort_name="DrugA", type="event"),
                CohortSpec(cohort_id=2, cohort_name="DrugB", type="event"),
                CohortSpec(cohort_id=3, cohort_name="DrugC", type="event"),
            ),
            cdm_name="test_cdm",
            arguments={},
        )

    def test_returns_summarised_result(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        assert isinstance(result, SummarisedResult)

    def test_has_standard_columns(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in result.data.columns

    def test_has_pathway_variable(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        var_names = result.data["variable_name"].unique().to_list()
        assert "treatment_pathway" in var_names

    def test_has_count_estimates(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        counts = result.data.filter(
            (pl.col("variable_name") == "treatment_pathway") & (pl.col("estimate_name") == "count")
        )
        assert counts.height > 0

    def test_has_percentage_estimates(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        pcts = result.data.filter(
            (pl.col("variable_name") == "treatment_pathway")
            & (pl.col("estimate_name") == "percentage")
        )
        assert pcts.height > 0

    def test_min_cell_count_filters(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        # With min_cell_count=0, should have pathways
        result_all = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        # With high min_cell_count, should have fewer/no pathways
        result_filtered = summarise_treatment_pathways(simple_pathway_result, min_cell_count=100)
        assert (
            result_filtered.data.filter(pl.col("variable_name") == "treatment_pathway").height
            <= result_all.data.filter(pl.col("variable_name") == "treatment_pathway").height
        )

    def test_settings_result_type(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        assert result.settings["result_type"][0] == "summarise_treatment_pathways"

    def test_cdm_name_propagated(self, simple_pathway_result):
        from omopy.treatment import summarise_treatment_pathways

        result = summarise_treatment_pathways(simple_pathway_result, min_cell_count=0)
        cdm_names = result.data["cdm_name"].unique().to_list()
        assert "test_cdm" in cdm_names

    def test_empty_history(self):
        from omopy.treatment import summarise_treatment_pathways

        pr = PathwayResult(
            treatment_history=pl.DataFrame(
                schema={
                    "person_id": pl.Int64,
                    "event_cohort_id": pl.Utf8,
                    "event_cohort_name": pl.Utf8,
                    "event_seq": pl.Int32,
                    "age": pl.Int64,
                    "sex": pl.Utf8,
                    "index_year": pl.Int32,
                    "target_cohort_id": pl.Int64,
                    "target_cohort_name": pl.Utf8,
                    "event_start_date": pl.Date,
                    "event_end_date": pl.Date,
                    "duration_era": pl.Int64,
                }
            ),
            attrition=pl.DataFrame(),
            cohorts=(CohortSpec(cohort_id=1, cohort_name="T", type="target"),),
            cdm_name="empty",
            arguments={},
        )
        result = summarise_treatment_pathways(pr)
        assert isinstance(result, SummarisedResult)


# ===================================================================
# summarise_event_duration
# ===================================================================


class TestSummariseEventDuration:
    """Test summarise_event_duration (from PathwayResult)."""

    @pytest.fixture()
    def duration_pathway_result(self):
        """PathwayResult with known durations."""
        history = pl.DataFrame(
            {
                "person_id": [1, 1, 2, 2, 3],
                "index_year": [2020, 2020, 2020, 2020, 2021],
                "event_cohort_id": ["1", "2", "1", "2", "1"],
                "event_cohort_name": ["DrugA", "DrugB", "DrugA", "DrugB", "DrugA"],
                "event_start_date": [
                    D(2020, 1, 1),
                    D(2020, 2, 1),
                    D(2020, 1, 15),
                    D(2020, 3, 1),
                    D(2021, 1, 1),
                ],
                "event_end_date": [
                    D(2020, 1, 31),
                    D(2020, 2, 28),
                    D(2020, 2, 15),
                    D(2020, 3, 31),
                    D(2021, 1, 31),
                ],
                "duration_era": [30, 27, 31, 30, 30],
                "event_seq": [1, 2, 1, 2, 1],
                "age": [50, 50, 60, 60, 70],
                "sex": ["Male", "Male", "Female", "Female", "Male"],
                "target_cohort_id": [100, 100, 100, 100, 100],
                "target_cohort_name": ["Target", "Target", "Target", "Target", "Target"],
                "type": ["event"] * 5,
            }
        )
        return PathwayResult(
            treatment_history=history,
            attrition=pl.DataFrame(),
            cohorts=(
                CohortSpec(cohort_id=100, cohort_name="Target", type="target"),
                CohortSpec(cohort_id=1, cohort_name="DrugA", type="event"),
                CohortSpec(cohort_id=2, cohort_name="DrugB", type="event"),
            ),
            cdm_name="test_cdm",
            arguments={},
        )

    def test_returns_summarised_result(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        assert isinstance(result, SummarisedResult)

    def test_has_standard_columns(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        for col in SUMMARISED_RESULT_COLUMNS:
            assert col in result.data.columns

    def test_has_duration_stats(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        est_names = set(result.data["estimate_name"].unique().to_list())
        for expected in ("min", "q25", "median", "q75", "max", "mean"):
            assert expected in est_names, f"Missing estimate: {expected}"

    def test_has_line_additional(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        addl_names = result.data["additional_name"].unique().to_list()
        assert "line" in addl_names

    def test_has_overall_line(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        lines = result.data["additional_level"].unique().to_list()
        assert "overall" in lines

    def test_has_per_line_stats(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        lines = result.data["additional_level"].unique().to_list()
        # Should have line "1" and "2"
        assert "1" in lines
        assert "2" in lines

    def test_settings_result_type(self, duration_pathway_result):
        from omopy.treatment import summarise_event_duration

        result = summarise_event_duration(duration_pathway_result)
        assert result.settings["result_type"][0] == "summarise_event_duration"

    def test_empty_history(self):
        from omopy.treatment import summarise_event_duration

        pr = PathwayResult(
            treatment_history=pl.DataFrame(
                schema={
                    "person_id": pl.Int64,
                    "event_cohort_id": pl.Utf8,
                    "event_cohort_name": pl.Utf8,
                    "event_seq": pl.Int32,
                    "duration_era": pl.Int64,
                    "age": pl.Int64,
                    "sex": pl.Utf8,
                    "index_year": pl.Int32,
                    "target_cohort_id": pl.Int64,
                    "target_cohort_name": pl.Utf8,
                    "event_start_date": pl.Date,
                    "event_end_date": pl.Date,
                    "type": pl.Utf8,
                }
            ),
            attrition=pl.DataFrame(),
            cohorts=(CohortSpec(cohort_id=1, cohort_name="T", type="target"),),
            cdm_name="empty",
            arguments={},
        )
        result = summarise_event_duration(pr)
        assert isinstance(result, SummarisedResult)


# ===================================================================
# Table functions
# ===================================================================


class TestTableFunctions:
    """Test table rendering functions."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.treatment import mock_treatment_pathways

        return mock_treatment_pathways(seed=42)

    def test_table_treatment_pathways_returns_data(self, mock_result):
        from omopy.treatment import table_treatment_pathways

        result = table_treatment_pathways(mock_result, type="polars")
        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    def test_table_event_duration_returns_data(self, mock_result):
        from omopy.treatment import table_event_duration

        result = table_event_duration(mock_result, type="polars")
        assert isinstance(result, pl.DataFrame)

    def test_table_treatment_pathways_custom_header(self, mock_result):
        from omopy.treatment import table_treatment_pathways

        result = table_treatment_pathways(
            mock_result,
            type="polars",
            header=["cdm_name"],
        )
        assert isinstance(result, pl.DataFrame)

    def test_table_event_duration_custom_group(self, mock_result):
        from omopy.treatment import table_event_duration

        result = table_event_duration(
            mock_result,
            type="polars",
            group_column=["line"],
        )
        assert isinstance(result, pl.DataFrame)


# ===================================================================
# Plot functions
# ===================================================================


class TestPlotSankey:
    """Test Sankey diagram generation."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.treatment import mock_treatment_pathways

        return mock_treatment_pathways(seed=42)

    def test_returns_figure(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result)
        assert fig is not None

    def test_has_data(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result)
        assert len(fig.data) > 0

    def test_custom_title(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result, title="My Title")
        assert fig.layout.title.text == "My Title"

    def test_max_paths(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result, max_paths=3)
        assert fig is not None

    def test_group_combinations(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result, group_combinations=True)
        assert fig is not None

    def test_custom_colors_dict(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result, colors={"Aspirin": "#ff0000"})
        assert fig is not None

    def test_custom_colors_list(self, mock_result):
        from omopy.treatment import plot_sankey

        fig = plot_sankey(mock_result, colors=["#ff0000", "#00ff00"])
        assert fig is not None

    def test_empty_result(self):
        from omopy.treatment import plot_sankey

        empty = SummarisedResult(
            pl.DataFrame(schema={col: pl.Utf8 for col in SUMMARISED_RESULT_COLUMNS}),
            settings=pl.DataFrame(
                {
                    "result_id": [1],
                    "result_type": ["summarise_treatment_pathways"],
                    "package_name": ["omopy.treatment"],
                    "package_version": ["0.1.0"],
                }
            ),
        )
        fig = plot_sankey(empty)
        assert fig is not None  # Should return empty figure


class TestPlotSunburst:
    """Test sunburst chart generation."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.treatment import mock_treatment_pathways

        return mock_treatment_pathways(seed=42)

    def test_returns_figure(self, mock_result):
        from omopy.treatment import plot_sunburst

        fig = plot_sunburst(mock_result)
        assert fig is not None

    def test_has_data(self, mock_result):
        from omopy.treatment import plot_sunburst

        fig = plot_sunburst(mock_result)
        assert len(fig.data) > 0

    def test_custom_title(self, mock_result):
        from omopy.treatment import plot_sunburst

        fig = plot_sunburst(mock_result, title="Sunburst Test")
        assert fig.layout.title.text == "Sunburst Test"

    def test_unit_count(self, mock_result):
        from omopy.treatment import plot_sunburst

        fig = plot_sunburst(mock_result, unit="count")
        assert fig is not None

    def test_group_combinations(self, mock_result):
        from omopy.treatment import plot_sunburst

        fig = plot_sunburst(mock_result, group_combinations=True)
        assert fig is not None


class TestPlotEventDuration:
    """Test event duration box plot generation."""

    @pytest.fixture()
    def mock_result(self):
        from omopy.treatment import mock_treatment_pathways

        return mock_treatment_pathways(seed=42, include_duration=True)

    def test_returns_figure(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result)
        assert fig is not None

    def test_has_traces(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result)
        assert len(fig.data) > 0

    def test_custom_title(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result, title="Duration Test")
        assert fig.layout.title.text == "Duration Test"

    def test_treatment_groups_group(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result, treatment_groups="group")
        assert fig is not None

    def test_treatment_groups_individual(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result, treatment_groups="individual")
        assert fig is not None

    def test_exclude_overall(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result, include_overall=False)
        assert fig is not None

    def test_specific_event_lines(self, mock_result):
        from omopy.treatment import plot_event_duration

        fig = plot_event_duration(mock_result, event_lines=[1, 2])
        assert fig is not None

    def test_no_duration_data(self):
        from omopy.treatment import mock_treatment_pathways, plot_event_duration

        result = mock_treatment_pathways(include_duration=False)
        fig = plot_event_duration(result)
        assert fig is not None  # Should return empty figure


# ===================================================================
# Edge cases
# ===================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_era_collapse_adjacent_eras(self):
        """Adjacent eras (end == start of next) should merge with collapse_size=0."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 31),
                    "event_end_date": D(2020, 2, 28),
                },
            ]
        )
        result = _era_collapse(df, era_collapse_size=0)
        events = result.filter(pl.col("type") == "event")
        assert events.height == 1

    def test_filter_treatments_single_event(self):
        df = _make_events(
            [
                {
                    "event_cohort_id": "1",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        for strategy in ("all", "first", "changes"):
            result = _filter_treatments(df, strategy)
            events = result.filter(pl.col("type") == "event")
            assert events.height == 1

    def test_combination_id_triple(self):
        """Three-way combination IDs."""
        combo = _make_combination_id("1+2", "3")
        assert combo == "1+2+3"

    def test_finalize_unknown_cohort_name(self):
        """Cohort ID with no name mapping should use the raw ID."""
        df = _make_events(
            [
                {
                    "event_cohort_id": "999",
                    "event_start_date": D(2020, 1, 1),
                    "event_end_date": D(2020, 1, 31),
                },
            ]
        )
        result = _finalize_pathways(df, max_path_length=5, cohort_names={})
        assert result["event_cohort_name"][0] == "999"

    def test_summarise_empty_pathway_result(self):
        from omopy.treatment import summarise_treatment_pathways

        pr = PathwayResult(
            treatment_history=pl.DataFrame(
                schema={
                    "person_id": pl.Int64,
                    "event_cohort_id": pl.Utf8,
                    "event_cohort_name": pl.Utf8,
                    "event_seq": pl.Int32,
                    "age": pl.Int64,
                    "sex": pl.Utf8,
                    "index_year": pl.Int32,
                    "target_cohort_id": pl.Int64,
                    "target_cohort_name": pl.Utf8,
                    "event_start_date": pl.Date,
                    "event_end_date": pl.Date,
                    "duration_era": pl.Int64,
                }
            ),
            attrition=pl.DataFrame(),
            cohorts=(
                CohortSpec(cohort_id=1, cohort_name="T", type="target"),
                CohortSpec(cohort_id=2, cohort_name="D", type="event"),
            ),
            cdm_name="test",
            arguments={},
        )
        result = summarise_treatment_pathways(pr)
        assert isinstance(result, SummarisedResult)
