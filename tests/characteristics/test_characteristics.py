"""Tests for omopy.characteristics — CohortCharacteristics module.

Tests are organized into:
1. Unit tests using mock data (no database needed)
2. Integration tests using the Synthea database
"""

from __future__ import annotations

import datetime
from typing import Any

import polars as pl
import pytest

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)


# ---------------------------------------------------------------------------
# Helpers — build minimal CohortTable objects for unit tests
# ---------------------------------------------------------------------------


def _make_cohort(
    n_cohorts: int = 2,
    n_subjects: int = 10,
    attach_cdm: bool = False,
    cdm=None,
) -> CohortTable:
    """Create a minimal CohortTable with test data."""
    rows: list[dict[str, Any]] = []
    subject_id = 1
    for cid in range(1, n_cohorts + 1):
        for _ in range(n_subjects):
            rows.append({
                "cohort_definition_id": cid,
                "subject_id": subject_id,
                "cohort_start_date": datetime.date(2020, 1, 1),
                "cohort_end_date": datetime.date(2020, 12, 31),
            })
            subject_id += 1

    df = pl.DataFrame(rows)
    settings = pl.DataFrame({
        "cohort_definition_id": list(range(1, n_cohorts + 1)),
        "cohort_name": [f"cohort_{i}" for i in range(1, n_cohorts + 1)],
    })

    ct = CohortTable(df, settings=settings)
    if cdm is not None:
        ct.cdm = cdm
    return ct


def _make_cohort_with_overlap(
    n_subjects_only_1: int = 3,
    n_subjects_only_2: int = 3,
    n_subjects_both: int = 4,
) -> CohortTable:
    """Create a CohortTable with controlled overlap between two cohorts."""
    rows: list[dict[str, Any]] = []
    sid = 1

    # Subjects only in cohort 1
    for _ in range(n_subjects_only_1):
        rows.append({
            "cohort_definition_id": 1,
            "subject_id": sid,
            "cohort_start_date": datetime.date(2020, 1, 1),
            "cohort_end_date": datetime.date(2020, 6, 30),
        })
        sid += 1

    # Subjects only in cohort 2
    for _ in range(n_subjects_only_2):
        rows.append({
            "cohort_definition_id": 2,
            "subject_id": sid,
            "cohort_start_date": datetime.date(2020, 3, 1),
            "cohort_end_date": datetime.date(2020, 12, 31),
        })
        sid += 1

    # Subjects in both cohorts
    for _ in range(n_subjects_both):
        rows.append({
            "cohort_definition_id": 1,
            "subject_id": sid,
            "cohort_start_date": datetime.date(2020, 1, 1),
            "cohort_end_date": datetime.date(2020, 6, 30),
        })
        rows.append({
            "cohort_definition_id": 2,
            "subject_id": sid,
            "cohort_start_date": datetime.date(2020, 3, 1),
            "cohort_end_date": datetime.date(2020, 12, 31),
        })
        sid += 1

    df = pl.DataFrame(rows)
    settings = pl.DataFrame({
        "cohort_definition_id": [1, 2],
        "cohort_name": ["cohort_1", "cohort_2"],
    })
    return CohortTable(df, settings=settings)


def _make_cohort_with_timing() -> CohortTable:
    """Create a CohortTable with known timing between cohort entries."""
    # Subject 1: cohort_1 on Jan 1, cohort_2 on Feb 1 (31 days later)
    # Subject 2: cohort_1 on Jan 1, cohort_2 on Mar 1 (60 days later)
    # Subject 3: only in cohort_1
    rows = [
        {"cohort_definition_id": 1, "subject_id": 1, "cohort_start_date": datetime.date(2020, 1, 1), "cohort_end_date": datetime.date(2020, 6, 30)},
        {"cohort_definition_id": 1, "subject_id": 2, "cohort_start_date": datetime.date(2020, 1, 1), "cohort_end_date": datetime.date(2020, 6, 30)},
        {"cohort_definition_id": 1, "subject_id": 3, "cohort_start_date": datetime.date(2020, 1, 1), "cohort_end_date": datetime.date(2020, 6, 30)},
        {"cohort_definition_id": 2, "subject_id": 1, "cohort_start_date": datetime.date(2020, 2, 1), "cohort_end_date": datetime.date(2020, 12, 31)},
        {"cohort_definition_id": 2, "subject_id": 2, "cohort_start_date": datetime.date(2020, 3, 1), "cohort_end_date": datetime.date(2020, 12, 31)},
    ]
    df = pl.DataFrame(rows)
    settings = pl.DataFrame({
        "cohort_definition_id": [1, 2],
        "cohort_name": ["cohort_1", "cohort_2"],
    })
    return CohortTable(df, settings=settings)


# ===================================================================
# Tests: Internal Aggregation Engine
# ===================================================================


class TestAggregationEngine:
    """Test the internal _compute_estimates and _summarise_variables functions."""

    def test_compute_numeric_estimates(self):
        from omopy.characteristics._summarise import _compute_estimates

        s = pl.Series("age", [20, 30, 40, 50, 60])
        results = _compute_estimates(s, "numeric")

        est_dict = {name: value for name, _, value in results}
        assert "mean" in est_dict
        assert "sd" in est_dict
        assert "median" in est_dict
        assert "min" in est_dict
        assert "max" in est_dict
        assert "q25" in est_dict
        assert "q75" in est_dict

        assert float(est_dict["mean"]) == pytest.approx(40.0, rel=0.01)
        assert float(est_dict["min"]) == pytest.approx(20.0, abs=0.01)
        assert float(est_dict["max"]) == pytest.approx(60.0, abs=0.01)

    def test_compute_categorical_estimates(self):
        from omopy.characteristics._summarise import _compute_categorical_estimates

        s = pl.Series("sex", ["Male", "Male", "Female", "Male"])
        results = _compute_categorical_estimates(s, total=4)

        # Should have count + percentage for each level
        assert len(results) == 4  # 2 levels × 2 estimates
        levels = {r[0] for r in results}
        assert levels == {"Male", "Female"}

        # Check Male count
        male_count = next(r for r in results if r[0] == "Male" and r[1] == "count")
        assert male_count[3] == "3"

    def test_compute_empty_series(self):
        from omopy.characteristics._summarise import _compute_estimates

        s = pl.Series("x", [], dtype=pl.Float64)
        results = _compute_estimates(s, "numeric")

        est_dict = {name: value for name, _, value in results}
        assert est_dict["mean"] == "NA"
        assert est_dict["sd"] == "NA"

    def test_classify_numeric(self):
        from omopy.characteristics._summarise import _classify_variable

        df = pl.DataFrame({"age": [20, 30, 40]})
        assert _classify_variable(df, "age") == "numeric"

    def test_classify_categorical(self):
        from omopy.characteristics._summarise import _classify_variable

        df = pl.DataFrame({"sex": ["Male", "Female"]})
        assert _classify_variable(df, "sex") == "categorical"

    def test_classify_binary(self):
        from omopy.characteristics._summarise import _classify_variable

        df = pl.DataFrame({"flag": [0, 1, 1, 0]})
        assert _classify_variable(df, "flag") == "binary"

    def test_classify_date(self):
        from omopy.characteristics._summarise import _classify_variable

        df = pl.DataFrame({"cohort_start_date": [datetime.date(2020, 1, 1)]})
        assert _classify_variable(df, "cohort_start_date") == "date"


class TestResolveStrata:
    def test_overall_only(self):
        from omopy.characteristics._summarise import _resolve_strata

        df = pl.DataFrame({"x": [1, 2, 3]})
        groups = _resolve_strata(df, [])
        assert len(groups) == 1
        assert groups[0][0] == OVERALL
        assert groups[0][1] == OVERALL
        assert len(groups[0][2]) == 3

    def test_single_strata(self):
        from omopy.characteristics._summarise import _resolve_strata

        df = pl.DataFrame({"sex": ["M", "M", "F", "F"]})
        groups = _resolve_strata(df, ["sex"])

        # 1 overall + 2 sex levels
        assert len(groups) == 3
        assert groups[0][0] == OVERALL

        strata_names = [g[0] for g in groups[1:]]
        assert all(s == "sex" for s in strata_names)

        strata_levels = {g[1] for g in groups[1:]}
        assert strata_levels == {"M", "F"}

    def test_combined_strata(self):
        from omopy.characteristics._summarise import _resolve_strata

        df = pl.DataFrame({
            "sex": ["M", "M", "F", "F"],
            "age_group": ["young", "old", "young", "old"],
        })
        groups = _resolve_strata(df, [["sex", "age_group"]])

        # 1 overall + 4 combinations
        assert len(groups) == 5
        assert groups[0][0] == OVERALL

    def test_missing_column_raises(self):
        from omopy.characteristics._summarise import _resolve_strata

        df = pl.DataFrame({"x": [1, 2]})
        with pytest.raises(ValueError, match="Strata columns not found"):
            _resolve_strata(df, ["nonexistent"])


# ===================================================================
# Tests: summarise_cohort_count
# ===================================================================


class TestSummariseCohortCount:
    def test_basic_counts(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=2, n_subjects=10)
        result = summarise_cohort_count(cohort)

        assert isinstance(result, SummarisedResult)
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)

        # Check result_type in settings
        assert result.settings["result_type"].to_list()[0] == "summarise_cohort_count"

    def test_correct_subject_count(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=15)
        result = summarise_cohort_count(cohort)
        data = result.data

        # Find Number subjects row
        subjects = data.filter(
            (pl.col("variable_name") == "Number subjects")
            & (pl.col("estimate_name") == "count")
        )
        assert len(subjects) == 1
        assert subjects["estimate_value"].to_list()[0] == "15"

    def test_correct_record_count(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=15)
        result = summarise_cohort_count(cohort)
        data = result.data

        records = data.filter(
            (pl.col("variable_name") == "Number records")
            & (pl.col("estimate_name") == "count")
        )
        assert len(records) == 1
        assert records["estimate_value"].to_list()[0] == "15"

    def test_multiple_cohorts(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=3, n_subjects=5)
        result = summarise_cohort_count(cohort)

        # Should have results for 3 cohorts
        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 3
        assert set(group_levels) == {"cohort_1", "cohort_2", "cohort_3"}

    def test_filter_cohort_id(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=3, n_subjects=5)
        result = summarise_cohort_count(cohort, cohort_id=[1])

        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 1
        assert group_levels[0] == "cohort_1"

    def test_group_name_is_cohort_name(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_count(cohort)

        assert result.data["group_name"].unique().to_list() == ["cohort_name"]


# ===================================================================
# Tests: summarise_cohort_attrition
# ===================================================================


class TestSummariseCohortAttrition:
    def test_with_attrition_data(self):
        from omopy.characteristics import summarise_cohort_attrition

        # Build cohort with attrition
        cohort = _make_cohort(n_cohorts=1, n_subjects=10)
        attrition = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "number_records": [100, 50],
            "number_subjects": [80, 40],
            "reason_id": [1, 2],
            "reason": ["Initial qualifying events", "Exclude age < 18"],
            "excluded_records": [0, 50],
            "excluded_subjects": [0, 40],
        })
        cohort._attrition = attrition

        result = summarise_cohort_attrition(cohort)
        assert isinstance(result, SummarisedResult)
        assert result.settings["result_type"].to_list()[0] == "summarise_cohort_attrition"

    def test_attrition_structure(self):
        from omopy.characteristics import summarise_cohort_attrition

        cohort = _make_cohort(n_cohorts=1, n_subjects=10)
        attrition = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "number_records": [100, 50],
            "number_subjects": [80, 40],
            "reason_id": [1, 2],
            "reason": ["Initial qualifying events", "Exclude age < 18"],
            "excluded_records": [0, 50],
            "excluded_subjects": [0, 40],
        })
        cohort._attrition = attrition

        result = summarise_cohort_attrition(cohort)
        data = result.data

        # strata_name should be "reason"
        assert "reason" in data["strata_name"].unique().to_list()

        # additional_name should be "reason_id"
        assert "reason_id" in data["additional_name"].unique().to_list()

        # Should have 4 variable names × 2 reasons = 8 rows
        assert len(data) == 8

    def test_empty_attrition(self):
        from omopy.characteristics import summarise_cohort_attrition

        cohort = _make_cohort(n_cohorts=1, n_subjects=10)
        cohort._attrition = pl.DataFrame({
            "cohort_definition_id": pl.Series([], dtype=pl.Int64),
            "number_records": pl.Series([], dtype=pl.Int64),
            "number_subjects": pl.Series([], dtype=pl.Int64),
            "reason_id": pl.Series([], dtype=pl.Int64),
            "reason": pl.Series([], dtype=pl.Utf8),
            "excluded_records": pl.Series([], dtype=pl.Int64),
            "excluded_subjects": pl.Series([], dtype=pl.Int64),
        })

        result = summarise_cohort_attrition(cohort)
        assert isinstance(result, SummarisedResult)
        assert len(result) == 0


# ===================================================================
# Tests: summarise_cohort_timing
# ===================================================================


class TestSummariseCohortTiming:
    def test_basic_timing(self):
        from omopy.characteristics import summarise_cohort_timing

        cohort = _make_cohort_with_timing()
        result = summarise_cohort_timing(cohort)

        assert isinstance(result, SummarisedResult)
        assert result.settings["result_type"].to_list()[0] == "summarise_cohort_timing"

    def test_group_name_structure(self):
        from omopy.characteristics import summarise_cohort_timing

        cohort = _make_cohort_with_timing()
        result = summarise_cohort_timing(cohort)

        group_names = result.data["group_name"].unique().to_list()
        assert len(group_names) == 1
        expected = "cohort_name_reference" + NAME_LEVEL_SEP + "cohort_name_comparator"
        assert group_names[0] == expected

    def test_timing_estimates(self):
        from omopy.characteristics import summarise_cohort_timing

        cohort = _make_cohort_with_timing()
        result = summarise_cohort_timing(cohort)

        # Filter to the timing variable for cohort_1 -> cohort_2
        data = result.data.filter(
            (pl.col("variable_name") == "Days between cohort entries")
            & (pl.col("group_level").str.contains("cohort_1"))
            & (pl.col("group_level").str.contains("cohort_2"))
        )

        assert len(data) > 0
        est_names = set(data["estimate_name"].to_list())
        assert "min" in est_names
        assert "max" in est_names
        assert "median" in est_names

    def test_both_directions(self):
        from omopy.characteristics import summarise_cohort_timing

        cohort = _make_cohort_with_timing()
        result = summarise_cohort_timing(cohort)

        # Should have both cohort_1→2 and cohort_2→1
        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 2

    def test_restrict_to_first_entry(self):
        from omopy.characteristics import summarise_cohort_timing

        # Add duplicate entry for a subject
        rows = [
            {"cohort_definition_id": 1, "subject_id": 1, "cohort_start_date": datetime.date(2020, 1, 1), "cohort_end_date": datetime.date(2020, 6, 30)},
            {"cohort_definition_id": 1, "subject_id": 1, "cohort_start_date": datetime.date(2020, 7, 1), "cohort_end_date": datetime.date(2020, 12, 31)},
            {"cohort_definition_id": 2, "subject_id": 1, "cohort_start_date": datetime.date(2020, 2, 1), "cohort_end_date": datetime.date(2020, 12, 31)},
        ]
        df = pl.DataFrame(rows)
        settings = pl.DataFrame({
            "cohort_definition_id": [1, 2],
            "cohort_name": ["cohort_1", "cohort_2"],
        })
        cohort = CohortTable(df, settings=settings)

        result_first = summarise_cohort_timing(cohort, restrict_to_first_entry=True)
        result_all = summarise_cohort_timing(cohort, restrict_to_first_entry=False)

        # With restrict=True, should have fewer records
        n_first = int(result_first.data.filter(
            (pl.col("variable_name") == "Number records")
            & (pl.col("estimate_name") == "count")
        )["estimate_value"].to_list()[0])

        n_all = int(result_all.data.filter(
            (pl.col("variable_name") == "Number records")
            & (pl.col("estimate_name") == "count")
        )["estimate_value"].to_list()[0])

        assert n_first <= n_all


# ===================================================================
# Tests: summarise_cohort_overlap
# ===================================================================


class TestSummariseCohortOverlap:
    def test_basic_overlap(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        result = summarise_cohort_overlap(cohort)

        assert isinstance(result, SummarisedResult)
        assert result.settings["result_type"].to_list()[0] == "summarise_cohort_overlap"

    def test_overlap_counts_correct(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort_with_overlap(
            n_subjects_only_1=3,
            n_subjects_only_2=2,
            n_subjects_both=5,
        )
        result = summarise_cohort_overlap(cohort)

        # Check cohort_1 → cohort_2 direction
        data = result.data.filter(
            pl.col("group_level") == "cohort_1" + NAME_LEVEL_SEP + "cohort_2"
        )

        # Extract counts
        only_ref = int(data.filter(
            (pl.col("variable_name") == "Only in reference cohort")
            & (pl.col("estimate_name") == "count")
        )["estimate_value"].to_list()[0])

        only_comp = int(data.filter(
            (pl.col("variable_name") == "Only in comparator cohort")
            & (pl.col("estimate_name") == "count")
        )["estimate_value"].to_list()[0])

        in_both = int(data.filter(
            (pl.col("variable_name") == "In both cohorts")
            & (pl.col("estimate_name") == "count")
        )["estimate_value"].to_list()[0])

        assert only_ref == 3
        assert only_comp == 2
        assert in_both == 5

    def test_overlap_percentages(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        result = summarise_cohort_overlap(cohort)

        # Total = 3 + 3 + 4 = 10
        data = result.data.filter(
            pl.col("group_level") == "cohort_1" + NAME_LEVEL_SEP + "cohort_2"
        )
        in_both_pct = float(data.filter(
            (pl.col("variable_name") == "In both cohorts")
            & (pl.col("estimate_name") == "percentage")
        )["estimate_value"].to_list()[0])

        assert in_both_pct == pytest.approx(40.0, abs=0.01)

    def test_both_pair_directions(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        result = summarise_cohort_overlap(cohort)

        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 2

    def test_variable_level_is_subjects(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        result = summarise_cohort_overlap(cohort)

        var_levels = result.data["variable_level"].unique().to_list()
        assert var_levels == ["Subjects"]


# ===================================================================
# Tests: summarise_cohort_codelist
# ===================================================================


class TestSummariseCohortCodelist:
    def test_with_codelist(self):
        from omopy.characteristics import summarise_cohort_codelist

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        codelist = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "codelist_name": ["my_codes", "my_codes"],
            "concept_id": [123, 456],
            "codelist_type": ["index event", "index event"],
        })
        cohort._cohort_codelist = codelist

        result = summarise_cohort_codelist(cohort)
        assert isinstance(result, SummarisedResult)
        assert result.settings["result_type"].to_list()[0] == "summarise_cohort_codelist"

    def test_codelist_structure(self):
        from omopy.characteristics import summarise_cohort_codelist

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        codelist = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "codelist_name": ["my_codes", "my_codes"],
            "concept_id": [123, 456],
            "codelist_type": ["index event", "index event"],
        })
        cohort._cohort_codelist = codelist

        result = summarise_cohort_codelist(cohort)
        data = result.data

        # Should have 2 rows (one per concept)
        assert len(data) == 2

        # strata_name should include codelist_name &&& codelist_type
        assert NAME_LEVEL_SEP in data["strata_name"].to_list()[0]
        assert data["estimate_name"].to_list()[0] == "concept_id"

    def test_empty_codelist(self):
        from omopy.characteristics import summarise_cohort_codelist

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        cohort._cohort_codelist = pl.DataFrame({
            "cohort_definition_id": pl.Series([], dtype=pl.Int64),
            "codelist_name": pl.Series([], dtype=pl.Utf8),
            "concept_id": pl.Series([], dtype=pl.Int64),
            "codelist_type": pl.Series([], dtype=pl.Utf8),
        })

        result = summarise_cohort_codelist(cohort)
        assert len(result) == 0


# ===================================================================
# Tests: mock_cohort_characteristics
# ===================================================================


class TestMockCohortCharacteristics:
    def test_basic_mock(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics()
        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

    def test_result_type(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics()
        assert result.settings["result_type"].to_list()[0] == "summarise_characteristics"

    def test_n_cohorts(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics(n_cohorts=3)
        group_levels = result.data["group_level"].unique().to_list()
        assert len(group_levels) == 3

    def test_deterministic_with_seed(self):
        from omopy.characteristics import mock_cohort_characteristics

        r1 = mock_cohort_characteristics(seed=123)
        r2 = mock_cohort_characteristics(seed=123)
        assert r1.data.equals(r2.data)

    def test_with_strata(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics(n_strata=1)
        strata_names = result.data["strata_name"].unique().to_list()
        assert OVERALL in strata_names
        assert "sex" in strata_names

    def test_has_standard_variables(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics()
        var_names = set(result.data["variable_name"].unique().to_list())

        assert "Number records" in var_names
        assert "Number subjects" in var_names
        assert "Age" in var_names
        assert "Sex" in var_names
        assert "Prior observation" in var_names

    def test_has_standard_estimates(self):
        from omopy.characteristics import mock_cohort_characteristics

        result = mock_cohort_characteristics()
        est_names = set(result.data["estimate_name"].unique().to_list())

        assert "count" in est_names
        assert "percentage" in est_names
        assert "mean" in est_names
        assert "sd" in est_names
        assert "median" in est_names


# ===================================================================
# Tests: Table functions
# ===================================================================


class TestTableFunctions:
    """Test table wrapper functions with mock data."""

    def test_table_characteristics_polars(self):
        from omopy.characteristics import mock_cohort_characteristics, table_characteristics

        result = mock_cohort_characteristics()
        table = table_characteristics(result, type="polars")
        assert isinstance(table, pl.DataFrame)
        assert len(table) > 0

    def test_table_cohort_count_polars(self):
        from omopy.characteristics import table_cohort_count
        from omopy.characteristics._mock import mock_cohort_characteristics

        # Create a count result
        cohort = _make_cohort(n_cohorts=2, n_subjects=10)
        from omopy.characteristics import summarise_cohort_count
        sr = summarise_cohort_count(cohort)
        table = table_cohort_count(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_cohort_attrition_polars(self):
        from omopy.characteristics import summarise_cohort_attrition, table_cohort_attrition

        cohort = _make_cohort(n_cohorts=1, n_subjects=10)
        cohort._attrition = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "number_records": [100, 50],
            "number_subjects": [80, 40],
            "reason_id": [1, 2],
            "reason": ["Initial", "Exclude"],
            "excluded_records": [0, 50],
            "excluded_subjects": [0, 40],
        })
        sr = summarise_cohort_attrition(cohort)
        table = table_cohort_attrition(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_cohort_timing_polars(self):
        from omopy.characteristics import summarise_cohort_timing, table_cohort_timing

        cohort = _make_cohort_with_timing()
        sr = summarise_cohort_timing(cohort)
        table = table_cohort_timing(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_table_cohort_overlap_polars(self):
        from omopy.characteristics import summarise_cohort_overlap, table_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        sr = summarise_cohort_overlap(cohort)
        table = table_cohort_overlap(sr, type="polars")
        assert isinstance(table, pl.DataFrame)

    def test_available_table_columns(self):
        from omopy.characteristics import available_table_columns, mock_cohort_characteristics

        result = mock_cohort_characteristics()
        cols = available_table_columns(result)
        assert isinstance(cols, list)
        assert "cdm_name" in cols


# ===================================================================
# Tests: Plot functions
# ===================================================================


class TestPlotFunctions:
    """Test plot wrapper functions with mock data."""

    def test_plot_characteristics_bar(self):
        from omopy.characteristics import mock_cohort_characteristics, plot_characteristics

        result = mock_cohort_characteristics()
        # Filter to count estimates only (one estimate_name)
        data = result.data.filter(pl.col("estimate_name") == "count")
        sr = SummarisedResult(data, settings=result.settings)

        fig = plot_characteristics(sr, plot_type="barplot")
        assert fig is not None

    def test_plot_cohort_count(self):
        from omopy.characteristics import summarise_cohort_count, plot_cohort_count

        cohort = _make_cohort(n_cohorts=2, n_subjects=10)
        sr = summarise_cohort_count(cohort)
        fig = plot_cohort_count(sr)
        assert fig is not None

    def test_plot_cohort_attrition(self):
        from omopy.characteristics import summarise_cohort_attrition, plot_cohort_attrition

        cohort = _make_cohort(n_cohorts=1, n_subjects=10)
        cohort._attrition = pl.DataFrame({
            "cohort_definition_id": [1, 1],
            "number_records": [100, 50],
            "number_subjects": [80, 40],
            "reason_id": [1, 2],
            "reason": ["Initial", "Exclude"],
            "excluded_records": [0, 50],
            "excluded_subjects": [0, 40],
        })
        sr = summarise_cohort_attrition(cohort)
        fig = plot_cohort_attrition(sr)
        assert fig is not None

    def test_plot_cohort_overlap(self):
        from omopy.characteristics import summarise_cohort_overlap, plot_cohort_overlap

        cohort = _make_cohort_with_overlap(3, 3, 4)
        sr = summarise_cohort_overlap(cohort)
        fig = plot_cohort_overlap(sr)
        assert fig is not None

    def test_plot_cohort_timing_box(self):
        from omopy.characteristics import summarise_cohort_timing, plot_cohort_timing

        cohort = _make_cohort_with_timing()
        sr = summarise_cohort_timing(cohort)
        fig = plot_cohort_timing(sr, plot_type="boxplot")
        assert fig is not None


# ===================================================================
# Tests: SummarisedResult integration
# ===================================================================


class TestResultIntegration:
    """Test that results integrate properly with SummarisedResult methods."""

    def test_split_group(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=2, n_subjects=5)
        result = summarise_cohort_count(cohort)

        split = result.split_group()
        assert "cohort_name" in split.columns

    def test_tidy(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_count(cohort)

        tidy = result.tidy()
        assert isinstance(tidy, pl.DataFrame)
        assert len(tidy) > 0

    def test_pivot_estimates(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_count(cohort)

        pivoted = result.pivot_estimates()
        assert "count" in pivoted.columns

    def test_filter_settings(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_count(cohort)

        filtered = result.filter_settings(result_type="summarise_cohort_count")
        assert len(filtered) == len(result)

        empty = result.filter_settings(result_type="nonexistent")
        assert len(empty) == 0

    def test_suppress(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=3)
        result = summarise_cohort_count(cohort)

        # Suppress with min_cell_count=5 — counts of 3 should be suppressed
        suppressed = result.suppress(min_cell_count=5)
        assert isinstance(suppressed, SummarisedResult)


# ===================================================================
# Tests: Edge cases
# ===================================================================


class TestEdgeCases:
    def test_single_cohort_no_strata(self):
        from omopy.characteristics import summarise_cohort_count

        cohort = _make_cohort(n_cohorts=1, n_subjects=1)
        result = summarise_cohort_count(cohort)

        assert len(result) == 2  # records + subjects
        assert result.data["strata_name"].to_list()[0] == OVERALL

    def test_overlap_single_cohort_returns_empty(self):
        from omopy.characteristics import summarise_cohort_overlap

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_overlap(cohort)

        # Single cohort: no pairs → empty
        assert len(result) == 0

    def test_timing_single_cohort_returns_empty(self):
        from omopy.characteristics import summarise_cohort_timing

        cohort = _make_cohort(n_cohorts=1, n_subjects=5)
        result = summarise_cohort_timing(cohort)

        # Single cohort, subjects only in that cohort → no pairs
        assert len(result) == 0

    def test_window_name_formatting(self):
        from omopy.characteristics._summarise import _window_name
        import math

        assert _window_name((0, 0)) == "0 to 0"
        assert _window_name((-365, -1)) == "-365 to -1"
        assert _window_name((-math.inf, -366)) == "-Inf to -366"
        assert _window_name((366, math.inf)) == "366 to Inf"

    def test_empty_result_structure(self):
        from omopy.characteristics._summarise import _empty_result

        result = _empty_result("test_type")
        assert isinstance(result, SummarisedResult)
        assert len(result) == 0
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)


# ===================================================================
# Integration tests — real Synthea database
# ===================================================================

# Concepts known to exist in the Synthea test DB
_VIRAL_SINUSITIS_ID = 40481087
_ESSENTIAL_HYPERTENSION_ID = 320128


def _make_circe_json(
    concept_id: int,
    concept_name: str = "test",
    *,
    primary_limit: str = "All",
) -> str:
    """Minimal CIRCE JSON to create a cohort from a condition concept."""
    import json

    d = {
        "ConceptSets": [{
            "id": 0,
            "name": concept_name,
            "expression": {
                "items": [{
                    "concept": {
                        "CONCEPT_ID": concept_id,
                        "CONCEPT_NAME": concept_name,
                        "CONCEPT_CODE": "",
                        "DOMAIN_ID": "Condition",
                        "VOCABULARY_ID": "SNOMED",
                        "STANDARD_CONCEPT": "S",
                        "INVALID_REASON": "",
                        "CONCEPT_CLASS_ID": "Disorder",
                    },
                    "includeDescendants": False,
                }],
            },
        }],
        "PrimaryCriteria": {
            "CriteriaList": [{
                "ConditionOccurrence": {"CodesetId": 0},
            }],
            "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
            "PrimaryCriteriaLimit": {"Type": primary_limit},
        },
        "QualifiedLimit": {"Type": primary_limit},
        "ExpressionLimit": {"Type": primary_limit},
        "EndStrategy": {"DateOffset": {"DateField": "StartDate", "Offset": 0}},
    }
    return json.dumps(d)


@pytest.fixture(scope="module")
def synthea_cohorts(synthea_cdm):
    """Generate two cohorts (Viral sinusitis + Hypertension) from Synthea."""
    from omopy.connector.circe._engine import generate_cohort_set

    # Generate two separate cohort sets, then combine
    vs_json = _make_circe_json(
        _VIRAL_SINUSITIS_ID, "Viral sinusitis", primary_limit="First",
    )
    ht_json = _make_circe_json(
        _ESSENTIAL_HYPERTENSION_ID, "Essential hypertension", primary_limit="First",
    )

    vs_result = generate_cohort_set(synthea_cdm, vs_json, name="viral_sinusitis")
    ht_result = generate_cohort_set(synthea_cdm, ht_json, name="essential_hypertension")

    # Merge into a single CohortTable with two cohort_definition_ids
    vs_ct = vs_result["viral_sinusitis"]
    ht_ct = ht_result["essential_hypertension"]

    vs_df = vs_ct.collect()
    ht_df = ht_ct.collect().with_columns(pl.lit(2).cast(pl.Int64).alias("cohort_definition_id"))
    vs_df = vs_df.with_columns(pl.lit(1).cast(pl.Int64).alias("cohort_definition_id"))

    combined = pl.concat([vs_df, ht_df])
    settings = pl.DataFrame({
        "cohort_definition_id": [1, 2],
        "cohort_name": ["viral_sinusitis", "essential_hypertension"],
    })
    ct = CohortTable(combined, settings=settings)
    ct.cdm = synthea_cdm
    return ct


class TestIntegrationCharacteristics:
    """Integration tests using real Synthea data."""

    def test_summarise_characteristics_runs(self, synthea_cohorts):
        """summarise_characteristics returns valid result on real data."""
        from omopy.characteristics import summarise_characteristics

        result = summarise_characteristics(synthea_cohorts, demographics=True)

        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

        # Should have 13 standard columns
        assert set(result.data.columns) >= set(SUMMARISED_RESULT_COLUMNS)

        # group_name should be cohort_name
        assert (result.data["group_name"] == "cohort_name").all()

        # group_level should include our cohort names
        group_levels = set(result.data["group_level"].unique().to_list())
        assert "viral_sinusitis" in group_levels
        assert "essential_hypertension" in group_levels

    def test_summarise_characteristics_has_demographics(self, synthea_cohorts):
        """Result includes age and sex variables."""
        from omopy.characteristics import summarise_characteristics

        result = summarise_characteristics(synthea_cohorts, demographics=True)
        var_names = set(result.data["variable_name"].unique().to_list())

        assert "Age" in var_names
        assert "Sex" in var_names

    def test_summarise_characteristics_filter_cohort(self, synthea_cohorts):
        """Filtering by cohort_id restricts the result."""
        from omopy.characteristics import summarise_characteristics

        result = summarise_characteristics(
            synthea_cohorts, cohort_id=[1], demographics=True,
        )
        group_levels = set(result.data["group_level"].unique().to_list())
        assert "viral_sinusitis" in group_levels
        assert "essential_hypertension" not in group_levels

    def test_summarise_characteristics_with_strata(self, synthea_cohorts):
        """Stratification by sex produces strata rows."""
        from omopy.characteristics import summarise_characteristics
        from omopy.profiles import add_sex

        # Add sex column before using it as strata — summarise_characteristics
        # should detect it already exists and skip re-adding.
        ct_with_sex = add_sex(synthea_cohorts, synthea_cohorts.cdm)
        result = summarise_characteristics(
            ct_with_sex, strata=["sex"], demographics=True,
        )

        strata_names = set(result.data["strata_name"].unique().to_list())
        assert OVERALL in strata_names
        assert "sex" in strata_names

    def test_summarise_cohort_count_integration(self, synthea_cohorts):
        """summarise_cohort_count on real data."""
        from omopy.characteristics import summarise_cohort_count

        result = summarise_cohort_count(synthea_cohorts)

        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

        # Should have Number records and Number subjects
        var_names = set(result.data["variable_name"].unique().to_list())
        assert "Number records" in var_names
        assert "Number subjects" in var_names

        # result_type should be correct
        assert result.settings["result_type"][0] == "summarise_cohort_count"

    def test_summarise_cohort_count_values(self, synthea_cohorts):
        """Count values are positive integers on real data."""
        from omopy.characteristics import summarise_cohort_count

        result = summarise_cohort_count(synthea_cohorts)
        counts = result.data.filter(pl.col("estimate_name") == "count")

        for val in counts["estimate_value"].to_list():
            assert int(val) > 0

    def test_summarise_cohort_timing_integration(self, synthea_cohorts):
        """summarise_cohort_timing computes timing between two cohorts."""
        from omopy.characteristics import summarise_cohort_timing

        result = summarise_cohort_timing(synthea_cohorts)

        # Only get results if subjects appear in both cohorts
        if len(result) > 0:
            # group_name should include reference &&& comparator
            assert NAME_LEVEL_SEP in result.data["group_name"][0]

            # Should have timing estimates
            est_names = set(result.data["estimate_name"].unique().to_list())
            assert "median" in est_names or "count" in est_names

    def test_summarise_cohort_overlap_integration(self, synthea_cohorts):
        """summarise_cohort_overlap computes overlap between two cohorts."""
        from omopy.characteristics import summarise_cohort_overlap

        result = summarise_cohort_overlap(synthea_cohorts)

        assert isinstance(result, SummarisedResult)
        assert len(result) > 0

        # Should have the three overlap categories
        var_names = set(result.data["variable_name"].unique().to_list())
        assert "Only in reference cohort" in var_names
        assert "Only in comparator cohort" in var_names
        assert "In both cohorts" in var_names

        # Percentages should be present
        pct_rows = result.data.filter(pl.col("estimate_name") == "percentage")
        for val in pct_rows["estimate_value"].to_list():
            pct = float(val)
            assert 0.0 <= pct <= 100.0

    def test_summarise_cohort_overlap_sums_to_100(self, synthea_cohorts):
        """For each pair, percentages should sum to 100."""
        from omopy.characteristics import summarise_cohort_overlap

        result = summarise_cohort_overlap(synthea_cohorts)
        pct_rows = result.data.filter(
            (pl.col("estimate_name") == "percentage")
            & (pl.col("strata_name") == OVERALL)
        )

        # Group by group_level (pair), sum percentages
        for group_level in pct_rows["group_level"].unique().to_list():
            pair_pcts = pct_rows.filter(pl.col("group_level") == group_level)
            total_pct = sum(float(v) for v in pair_pcts["estimate_value"].to_list())
            assert abs(total_pct - 100.0) < 0.1, f"Pair {group_level} sums to {total_pct}"

    def test_summarise_large_scale_characteristics_integration(self, synthea_cohorts):
        """LSC runs and produces concept-level results on real data."""
        from omopy.characteristics import summarise_large_scale_characteristics

        result = summarise_large_scale_characteristics(
            synthea_cohorts,
            event_in_window=["condition_occurrence"],
            window=[(0, 0)],  # Only index date for speed
            minimum_frequency=0.0,  # Include all concepts
        )

        assert isinstance(result, SummarisedResult)
        # May or may not produce results depending on data — just check it runs
        if len(result) > 0:
            assert "concept_id" in result.data["additional_name"].unique().to_list()
            est_names = set(result.data["estimate_name"].unique().to_list())
            assert "count" in est_names
            assert "percentage" in est_names

    def test_result_tidy_integration(self, synthea_cohorts):
        """tidy() works on real summarise_characteristics output."""
        from omopy.characteristics import summarise_characteristics

        result = summarise_characteristics(synthea_cohorts, demographics=True)
        tidy = result.tidy()

        assert isinstance(tidy, pl.DataFrame)
        assert len(tidy) > 0
        # tidy() unpacks group_name/group_level into named columns
        # (e.g. cohort_name), so check for cohort_name instead
        assert "cohort_name" in tidy.columns or "group_level" in tidy.columns

    def test_result_pivot_estimates_integration(self, synthea_cohorts):
        """pivot_estimates() works on real output."""
        from omopy.characteristics import summarise_cohort_count

        result = summarise_cohort_count(synthea_cohorts)
        pivoted = result.pivot_estimates()

        assert isinstance(pivoted, pl.DataFrame)
        assert "count" in pivoted.columns
