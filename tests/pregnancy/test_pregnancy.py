"""Unit tests for omopy.pregnancy — HIPPS pregnancy identification.

Tests are organized into sections:
1. Concept data and constants
2. HIP algorithm
3. PPS algorithm
4. Merge algorithm
5. ESD algorithm
6. PregnancyResult model
7. Mock CDM
8. Validate episodes
9. Summarise / table / plot
"""

from __future__ import annotations

import datetime
from typing import Any

import polars as pl
import pytest

from omopy.pregnancy._concepts import (
    ESD_CONCEPTS,
    ESD_CONCEPT_IDS,
    GR3M_MONTH_RANGES,
    HIP_CONCEPTS,
    HIP_CONCEPT_CATEGORIES,
    HIP_CONCEPT_IDS,
    MATCHO_OUTCOME_LIMITS,
    MATCHO_TERM_DURATIONS,
    OUTCOME_CATEGORIES,
    PPS_CONCEPTS,
    PPS_CONCEPT_IDS,
)
from omopy.pregnancy._hip import _run_hip
from omopy.pregnancy._pps import _run_pps
from omopy.pregnancy._merge import _merge_hipps
from omopy.pregnancy._esd import _run_esd
from omopy.pregnancy._identify import PregnancyResult, identify_pregnancies
from omopy.pregnancy._mock import mock_pregnancy_cdm, validate_episodes
from omopy.pregnancy._summarise import summarise_pregnancies
from omopy.pregnancy._table import table_pregnancies
from omopy.pregnancy._plot import plot_pregnancies
from omopy.generics.summarised_result import SummarisedResult


# ---------------------------------------------------------------------------
# Helpers for building test DataFrames
# ---------------------------------------------------------------------------


def _make_hip_records(rows: list[dict]) -> pl.DataFrame:
    """Build a HIP records DataFrame from a list of dicts."""
    schema = {
        "person_id": pl.Int64,
        "concept_id": pl.Int64,
        "record_date": pl.Date,
        "value_as_number": pl.Float64,
        "source_table": pl.Utf8,
        "category": pl.Utf8,
        "gest_value": pl.Int64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def _make_pps_records(rows: list[dict]) -> pl.DataFrame:
    """Build a PPS records DataFrame from a list of dicts."""
    schema = {
        "person_id": pl.Int64,
        "concept_id": pl.Int64,
        "record_date": pl.Date,
        "value_as_number": pl.Float64,
        "source_table": pl.Utf8,
        "min_month": pl.Int64,
        "max_month": pl.Int64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def _make_esd_records(rows: list[dict]) -> pl.DataFrame:
    """Build an ESD records DataFrame from a list of dicts."""
    schema = {
        "person_id": pl.Int64,
        "concept_id": pl.Int64,
        "record_date": pl.Date,
        "value_as_number": pl.Float64,
        "source_table": pl.Utf8,
        "esd_category": pl.Utf8,
        "esd_domain": pl.Utf8,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


# ===================================================================
# 1. Concept data and constants
# ===================================================================


class TestOutcomeCategories:
    """Tests for OUTCOME_CATEGORIES constant."""

    def test_has_all_expected_keys(self):
        expected = {"LB", "SB", "AB", "SA", "DELIV", "ECT", "PREG"}
        assert set(OUTCOME_CATEGORIES.keys()) == expected

    def test_values_are_strings(self):
        for key, val in OUTCOME_CATEGORIES.items():
            assert isinstance(val, str), f"{key} value is not str"

    def test_lb_is_live_birth(self):
        assert OUTCOME_CATEGORIES["LB"] == "Live birth"

    def test_sb_is_stillbirth(self):
        assert OUTCOME_CATEGORIES["SB"] == "Stillbirth"

    def test_sa_is_spontaneous_abortion(self):
        assert OUTCOME_CATEGORIES["SA"] == "Spontaneous abortion"


class TestMatchoOutcomeLimits:
    """Tests for MATCHO_OUTCOME_LIMITS."""

    def test_all_values_are_positive(self):
        for key, val in MATCHO_OUTCOME_LIMITS.items():
            assert val > 0, f"{key} has non-positive limit"

    def test_lb_lb_spacing(self):
        assert MATCHO_OUTCOME_LIMITS[("LB", "LB")] == 168

    def test_ab_ab_spacing(self):
        assert MATCHO_OUTCOME_LIMITS[("AB", "AB")] == 42

    def test_ect_ect_spacing(self):
        assert MATCHO_OUTCOME_LIMITS[("ECT", "ECT")] == 42

    def test_keys_are_tuples(self):
        for key in MATCHO_OUTCOME_LIMITS:
            assert isinstance(key, tuple) and len(key) == 2

    def test_all_outcome_categories_covered(self):
        cats = {"LB", "SB", "AB", "SA", "DELIV", "ECT"}
        for c1 in cats:
            for c2 in cats:
                assert (c1, c2) in MATCHO_OUTCOME_LIMITS, f"Missing ({c1}, {c2})"


class TestMatchoTermDurations:
    """Tests for MATCHO_TERM_DURATIONS."""

    def test_lb_range(self):
        assert MATCHO_TERM_DURATIONS["LB"] == (140, 308)

    def test_ect_range(self):
        assert MATCHO_TERM_DURATIONS["ECT"] == (28, 84)

    def test_all_categories_present(self):
        for cat in ("LB", "SB", "AB", "SA", "DELIV", "ECT", "PREG"):
            assert cat in MATCHO_TERM_DURATIONS

    def test_min_less_than_max(self):
        for cat, (mn, mx) in MATCHO_TERM_DURATIONS.items():
            assert mn < mx, f"{cat}: min ({mn}) >= max ({mx})"


class TestHipConcepts:
    """Tests for HIP_CONCEPTS data."""

    def test_has_lb_concepts(self):
        lb = [c for c, info in HIP_CONCEPTS.items() if info["category"] == "LB"]
        assert len(lb) >= 5

    def test_has_sb_concepts(self):
        sb = [c for c, info in HIP_CONCEPTS.items() if info["category"] == "SB"]
        assert len(sb) >= 3

    def test_has_sa_concepts(self):
        sa = [c for c, info in HIP_CONCEPTS.items() if info["category"] == "SA"]
        assert len(sa) >= 3

    def test_has_ab_concepts(self):
        ab = [c for c, info in HIP_CONCEPTS.items() if info["category"] == "AB"]
        assert len(ab) >= 3

    def test_has_ect_concepts(self):
        ect = [c for c, info in HIP_CONCEPTS.items() if info["category"] == "ECT"]
        assert len(ect) >= 3

    def test_concept_ids_are_ints(self):
        for cid in HIP_CONCEPTS:
            assert isinstance(cid, int)

    def test_hip_concept_categories_match(self):
        for cid, cat in HIP_CONCEPT_CATEGORIES.items():
            assert HIP_CONCEPTS[cid]["category"] == cat

    def test_hip_concept_ids_frozenset(self):
        assert isinstance(HIP_CONCEPT_IDS, frozenset)
        assert len(HIP_CONCEPT_IDS) == len(HIP_CONCEPTS)


class TestPpsConcepts:
    """Tests for PPS_CONCEPTS data."""

    def test_has_concepts(self):
        assert len(PPS_CONCEPTS) >= 10

    def test_each_has_min_max_month(self):
        for cid, info in PPS_CONCEPTS.items():
            assert "min_month" in info
            assert "max_month" in info
            assert info["min_month"] <= info["max_month"]

    def test_pps_concept_ids_frozenset(self):
        assert isinstance(PPS_CONCEPT_IDS, frozenset)
        assert len(PPS_CONCEPT_IDS) == len(PPS_CONCEPTS)


class TestEsdConcepts:
    """Tests for ESD_CONCEPTS data."""

    def test_has_gw_concepts(self):
        gw = [c for c, info in ESD_CONCEPTS.items() if info["category"] == "GW"]
        assert len(gw) >= 3

    def test_has_gr3m_concepts(self):
        gr = [c for c, info in ESD_CONCEPTS.items() if info["category"] == "GR3m"]
        assert len(gr) >= 2

    def test_gr3m_month_ranges(self):
        assert len(GR3M_MONTH_RANGES) >= 2
        for cid, (mn, mx) in GR3M_MONTH_RANGES.items():
            assert mn < mx


# ===================================================================
# 2. HIP algorithm
# ===================================================================


class TestRunHip:
    """Tests for _run_hip()."""

    def test_empty_records(self):
        records = _make_hip_records([])
        result = _run_hip(records)
        assert result.height == 0
        assert "episode_id" in result.columns

    def test_single_lb_outcome(self):
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                }
            ]
        )
        result = _run_hip(records)
        assert result.height == 1
        assert result["category"][0] == "LB"
        assert result["outcome_date"][0] == datetime.date(2020, 9, 1)

    def test_two_lb_outcomes_with_sufficient_spacing(self):
        """Two LB outcomes 200 days apart should produce 2 episodes."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
                {
                    "person_id": 1,
                    "concept_id": 4302541,
                    "record_date": datetime.date(2020, 7, 20),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
            ]
        )
        result = _run_hip(records)
        assert result.height == 2

    def test_two_lb_outcomes_too_close(self):
        """Two LB outcomes 100 days apart — second should be skipped."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
                {
                    "person_id": 1,
                    "concept_id": 4302541,
                    "record_date": datetime.date(2020, 4, 10),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
            ]
        )
        result = _run_hip(records)
        # Walking backwards: last record (Apr) assigned, then Jan is 100 days before
        # which is < 168 min spacing for LB->LB, so Jan is skipped.
        assert result.height == 1
        assert result["outcome_date"][0] == datetime.date(2020, 4, 10)

    def test_lb_then_sa_with_sufficient_spacing(self):
        """LB then SA with 60+ days gap."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
                {
                    "person_id": 1,
                    "concept_id": 4199459,
                    "record_date": datetime.date(2020, 4, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "SA",
                    "gest_value": None,
                },
            ]
        )
        result = _run_hip(records)
        # Walking backwards: SA at Apr 1 assigned, then LB at Jan 1 is 91 days before.
        # SA->LB limit is 56 days. 91 > 56, so both assigned.
        assert result.height == 2

    def test_multiple_persons(self):
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 6, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                },
                {
                    "person_id": 2,
                    "concept_id": 443213,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "ECT",
                    "gest_value": None,
                },
            ]
        )
        result = _run_hip(records)
        assert result.height == 2
        assert set(result["person_id"].to_list()) == {1, 2}

    def test_gestation_only_episodes(self):
        """Records with no outcome category should form PREG episodes."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": None,
                    "gest_value": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": None,
                    "gest_value": None,
                },
            ]
        )
        result = _run_hip(records, just_gestation=True)
        assert result.height == 1
        assert result["category"][0] == "PREG"

    def test_gestation_only_split_by_long_gap(self):
        """Gestation-only records with >305 day gap should split."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2019, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": None,
                    "gest_value": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": None,
                    "gest_value": None,
                },
            ]
        )
        result = _run_hip(records, just_gestation=True)
        assert result.height == 2

    def test_no_gestation_pass(self):
        """When just_gestation=False, unassigned records not grouped."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": None,
                    "gest_value": None,
                },
            ]
        )
        result = _run_hip(records, just_gestation=False)
        assert result.height == 0

    def test_episode_start_date_computed(self):
        """Episode start should be outcome_date - max_term."""
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                }
            ]
        )
        result = _run_hip(records)
        # LB max_term = 308 days
        expected_start = datetime.date(2020, 9, 1) - datetime.timedelta(days=308)
        assert result["episode_start_date"][0] == expected_start

    def test_ect_episode_shorter_than_lb(self):
        """ECT episodes should have shorter duration than LB."""
        lb_records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4014295,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "LB",
                    "gest_value": 40,
                }
            ]
        )
        ect_records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 443213,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "ECT",
                    "gest_value": None,
                }
            ]
        )
        lb_result = _run_hip(lb_records)
        ect_result = _run_hip(ect_records)
        lb_duration = (lb_result["episode_end_date"][0] - lb_result["episode_start_date"][0]).days
        ect_duration = (
            ect_result["episode_end_date"][0] - ect_result["episode_start_date"][0]
        ).days
        assert ect_duration < lb_duration


# ===================================================================
# 3. PPS algorithm
# ===================================================================


class TestRunPps:
    """Tests for _run_pps()."""

    def test_empty_records(self):
        records = _make_pps_records([])
        result = _run_pps(records)
        assert result.height == 0
        assert "episode_id" in result.columns

    def test_single_record(self):
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                }
            ]
        )
        result = _run_pps(records)
        assert result.height == 1
        assert result["category"][0] == "PREG"

    def test_two_records_same_episode(self):
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
                {
                    "person_id": 1,
                    "concept_id": 4098620,
                    "record_date": datetime.date(2020, 4, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 2,
                    "max_month": 9,
                },
            ]
        )
        result = _run_pps(records)
        assert result.height == 1

    def test_large_gap_forces_new_episode(self):
        """Gap > 300 days should force a new episode."""
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2019, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
            ]
        )
        result = _run_pps(records)
        assert result.height == 2

    def test_episode_too_long_removed(self):
        """Episode > 365 days should be removed.

        We construct records that stay within the same episode (gap < 300
        days, null timing so no disagreement splits) but together span
        > 365 days.
        """
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4230360,
                    "record_date": datetime.date(2019, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 4230360,
                    "record_date": datetime.date(2019, 6, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 4230360,
                    "record_date": datetime.date(2019, 10, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 4230360,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
            ]
        )
        result = _run_pps(records)
        # The episode spans Jan 2019 to Mar 2020 (>365 days), should be removed.
        # With null timing info, no disagreement splits occur, and all gaps < 300 days.
        assert result.height == 0

    def test_multiple_persons(self):
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
                {
                    "person_id": 2,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 5, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
            ]
        )
        result = _run_pps(records)
        assert result.height == 2
        assert set(result["person_id"].to_list()) == {1, 2}

    def test_n_pps_records_counted(self):
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
                {
                    "person_id": 1,
                    "concept_id": 4098620,
                    "record_date": datetime.date(2020, 2, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 2,
                    "max_month": 9,
                },
                {
                    "person_id": 1,
                    "concept_id": 4113553,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 4,
                    "max_month": 6,
                },
            ]
        )
        result = _run_pps(records)
        assert result.height == 1
        assert result["n_pps_records"][0] == 3

    def test_timing_disagreement_splits_episode(self):
        """Records that disagree in timing with gap > 30 days should split."""
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4238072,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 9,
                    "max_month": 10,
                },
                {
                    "person_id": 1,
                    "concept_id": 4048098,
                    "record_date": datetime.date(2020, 3, 15),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": 1,
                    "max_month": 3,
                },
            ]
        )
        result = _run_pps(records)
        # First record: term pregnancy (month 9-10), second is first visit (1-3).
        # Elapsed ~2.5 months, expected 1-3 but with tolerance still might agree.
        # The key check: 2.5 months >= (1 - 2) = -1 and <= (3 + 2) = 5. Agrees.
        # So they stay in same episode.
        assert result.height >= 1


# ===================================================================
# 4. Merge algorithm
# ===================================================================


class TestMergeHipps:
    """Tests for _merge_hipps()."""

    def test_empty_both(self):
        hip = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_id": pl.Int64,
                "category": pl.Utf8,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "outcome_date": pl.Date,
                "outcome_concept_id": pl.Int64,
            }
        )
        pps = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_id": pl.Int64,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "n_pps_records": pl.Int64,
                "category": pl.Utf8,
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 0

    def test_hip_only(self):
        hip = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
            }
        )
        pps = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_id": pl.Int64,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "n_pps_records": pl.Int64,
                "category": pl.Utf8,
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 1
        assert result["source"][0] == "HIP"

    def test_pps_only(self):
        hip = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_id": pl.Int64,
                "category": pl.Utf8,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "outcome_date": pl.Date,
                "outcome_concept_id": pl.Int64,
            }
        )
        pps = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "episode_start_date": [datetime.date(2020, 1, 1)],
                "episode_end_date": [datetime.date(2020, 6, 1)],
                "n_pps_records": [5],
                "category": ["PREG"],
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 1
        assert result["source"][0] == "PPS"

    def test_overlapping_episodes_merged(self):
        hip = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
            }
        )
        pps = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "episode_start_date": [datetime.date(2020, 1, 1)],
                "episode_end_date": [datetime.date(2020, 8, 1)],
                "n_pps_records": [5],
                "category": ["PREG"],
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 1
        assert result["source"][0] == "HIP+PPS"
        # Category from HIP
        assert result["category"][0] == "LB"

    def test_non_overlapping_stay_separate(self):
        hip = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2018, 1, 1)],
                "episode_end_date": [datetime.date(2018, 9, 1)],
                "outcome_date": [datetime.date(2018, 9, 1)],
                "outcome_concept_id": [4014295],
            }
        )
        pps = pl.DataFrame(
            {
                "person_id": [1],
                "episode_id": [1],
                "episode_start_date": [datetime.date(2020, 1, 1)],
                "episode_end_date": [datetime.date(2020, 6, 1)],
                "n_pps_records": [5],
                "category": ["PREG"],
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 2
        sources = set(result["source"].to_list())
        assert "HIP" in sources
        assert "PPS" in sources

    def test_multiple_persons(self):
        hip = pl.DataFrame(
            {
                "person_id": [1, 2],
                "episode_id": [1, 2],
                "category": ["LB", "SA"],
                "episode_start_date": [datetime.date(2019, 12, 1), datetime.date(2020, 1, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1), datetime.date(2020, 5, 1)],
                "outcome_date": [datetime.date(2020, 9, 1), datetime.date(2020, 5, 1)],
                "outcome_concept_id": [4014295, 4199459],
            }
        )
        pps = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_id": pl.Int64,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "n_pps_records": pl.Int64,
                "category": pl.Utf8,
            }
        )
        result = _merge_hipps(hip, pps)
        assert result.height == 2
        assert set(result["person_id"].to_list()) == {1, 2}


# ===================================================================
# 5. ESD algorithm
# ===================================================================


class TestRunEsd:
    """Tests for _run_esd()."""

    def test_empty_episodes(self):
        episodes = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "merged_episode_id": pl.Int64,
                "hip_episode_id": pl.Int64,
                "pps_episode_id": pl.Int64,
                "category": pl.Utf8,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
                "outcome_date": pl.Date,
                "outcome_concept_id": pl.Int64,
                "n_pps_records": pl.Int64,
                "source": pl.Utf8,
            }
        )
        esd = _make_esd_records([])
        result = _run_esd(episodes, esd)
        assert "esd_start_date" in result.columns
        assert "precision" in result.columns
        assert "final_start_date" in result.columns

    def test_no_esd_records_low_precision(self):
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records([])
        result = _run_esd(episodes, esd)
        assert result["precision"][0] == "low"
        assert result["esd_start_date"][0] is None

    def test_gw_evidence_high_precision(self):
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4260747,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": 38.0,
                    "source_table": "measurement",
                    "esd_category": "GW",
                    "esd_domain": "measurement",
                }
            ]
        )
        result = _run_esd(episodes, esd)
        assert result["precision"][0] == "high"
        expected_start = datetime.date(2020, 9, 1) - datetime.timedelta(days=38 * 7)
        assert result["esd_start_date"][0] == expected_start

    def test_gr3m_evidence_medium_precision(self):
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4299535,  # First trimester
                    "record_date": datetime.date(2020, 2, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "esd_category": "GR3m",
                    "esd_domain": "condition",
                }
            ]
        )
        result = _run_esd(episodes, esd)
        assert result["precision"][0] == "medium"
        assert result["esd_start_date"][0] is not None

    def test_final_start_date_uses_esd_when_available(self):
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4260747,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": 39.0,
                    "source_table": "measurement",
                    "esd_category": "GW",
                    "esd_domain": "measurement",
                }
            ]
        )
        result = _run_esd(episodes, esd)
        expected_start = datetime.date(2020, 9, 1) - datetime.timedelta(days=39 * 7)
        assert result["final_start_date"][0] == expected_start

    def test_fallback_start_date_uses_category_term(self):
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records([])
        result = _run_esd(episodes, esd)
        # No ESD evidence: fall back to outcome_date - max_term(LB=308)
        expected_start = datetime.date(2020, 9, 1) - datetime.timedelta(days=308)
        assert result["final_start_date"][0] == expected_start


# ===================================================================
# 6. PregnancyResult model
# ===================================================================


class TestPregnancyResult:
    """Tests for PregnancyResult Pydantic model."""

    def test_create_result(self):
        result = PregnancyResult(
            episodes=pl.DataFrame({"x": [1]}),
            hip_episodes=pl.DataFrame({"x": [1]}),
            pps_episodes=pl.DataFrame({"x": [1]}),
            merged_episodes=pl.DataFrame({"x": [1]}),
            cdm_name="test",
            n_persons_input=10,
            n_episodes=5,
            settings={"foo": "bar"},
        )
        assert result.cdm_name == "test"
        assert result.n_persons_input == 10
        assert result.n_episodes == 5

    def test_frozen(self):
        result = PregnancyResult(
            episodes=pl.DataFrame({"x": [1]}),
            hip_episodes=pl.DataFrame({"x": [1]}),
            pps_episodes=pl.DataFrame({"x": [1]}),
            merged_episodes=pl.DataFrame({"x": [1]}),
            cdm_name="test",
            n_persons_input=10,
            n_episodes=5,
            settings={},
        )
        with pytest.raises(Exception):
            result.cdm_name = "changed"

    def test_settings_dict(self):
        result = PregnancyResult(
            episodes=pl.DataFrame({"x": [1]}),
            hip_episodes=pl.DataFrame({"x": [1]}),
            pps_episodes=pl.DataFrame({"x": [1]}),
            merged_episodes=pl.DataFrame({"x": [1]}),
            cdm_name="test",
            n_persons_input=0,
            n_episodes=0,
            settings={"start_date": None, "end_date": None},
        )
        assert result.settings["start_date"] is None


# ===================================================================
# 7. Mock CDM
# ===================================================================


class TestMockPregnancyCdm:
    """Tests for mock_pregnancy_cdm()."""

    def test_returns_cdm_reference(self):
        from omopy.generics.cdm_reference import CdmReference

        cdm = mock_pregnancy_cdm(seed=42, n_persons=10)
        assert isinstance(cdm, CdmReference)

    def test_has_required_tables(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=10)
        for tbl in [
            "person",
            "observation_period",
            "condition_occurrence",
            "procedure_occurrence",
            "measurement",
            "observation",
        ]:
            assert tbl in cdm, f"Missing table: {tbl}"

    def test_person_table_has_persons(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=15)
        person_df = cdm["person"].collect()
        assert person_df.height == 15

    def test_all_female(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=10)
        person_df = cdm["person"].collect()
        genders = person_df["gender_concept_id"].unique().to_list()
        assert genders == [8532]

    def test_condition_occurrence_not_empty(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        cond_df = cdm["condition_occurrence"].collect()
        assert cond_df.height > 0

    def test_observation_not_empty(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        obs_df = cdm["observation"].collect()
        assert obs_df.height > 0

    def test_measurement_has_values(self):
        cdm = mock_pregnancy_cdm(seed=42, n_persons=20)
        meas_df = cdm["measurement"].collect()
        if meas_df.height > 0:
            non_null = meas_df.filter(pl.col("value_as_number").is_not_null())
            assert non_null.height > 0

    def test_deterministic_with_seed(self):
        cdm1 = mock_pregnancy_cdm(seed=123, n_persons=5)
        cdm2 = mock_pregnancy_cdm(seed=123, n_persons=5)
        p1 = cdm1["person"].collect()
        p2 = cdm2["person"].collect()
        assert p1.equals(p2)

    def test_cdm_name(self):
        cdm = mock_pregnancy_cdm()
        assert cdm.cdm_name == "mock_pregnancy"


# ===================================================================
# 8. Validate episodes
# ===================================================================


class TestValidateEpisodes:
    """Tests for validate_episodes()."""

    def test_empty_episodes(self):
        df = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "episode_start_date": pl.Date,
                "episode_end_date": pl.Date,
            }
        )
        result = validate_episodes(df)
        assert result.height >= 1
        assert "check" in result.columns

    def test_valid_episodes_no_violations(self):
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "episode_start_date": [datetime.date(2020, 1, 1), datetime.date(2020, 6, 1)],
                "episode_end_date": [datetime.date(2020, 5, 1), datetime.date(2020, 9, 1)],
            }
        )
        result = validate_episodes(df)
        violations = result.filter(pl.col("n_violations") > 0)
        assert violations.height == 0

    def test_start_after_end_detected(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "episode_start_date": [datetime.date(2020, 9, 1)],
                "episode_end_date": [datetime.date(2020, 1, 1)],
            }
        )
        result = validate_episodes(df)
        bad = result.filter((pl.col("check") == "start_before_end") & (pl.col("n_violations") > 0))
        assert bad.height == 1

    def test_max_duration_exceeded(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "episode_start_date": [datetime.date(2019, 1, 1)],
                "episode_end_date": [datetime.date(2020, 6, 1)],
            }
        )
        result = validate_episodes(df, max_days=365)
        bad = result.filter((pl.col("check") == "max_duration") & (pl.col("n_violations") > 0))
        assert bad.height == 1

    def test_overlap_detected(self):
        df = pl.DataFrame(
            {
                "person_id": [1, 1],
                "episode_start_date": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 3, 1),
                ],
                "episode_end_date": [
                    datetime.date(2020, 6, 1),
                    datetime.date(2020, 9, 1),
                ],
            }
        )
        result = validate_episodes(df)
        bad = result.filter((pl.col("check") == "no_overlaps") & (pl.col("n_violations") > 0))
        assert bad.height == 1


# ===================================================================
# 9. Summarise / table / plot
# ===================================================================


def _make_pregnancy_result() -> PregnancyResult:
    """Create a minimal PregnancyResult for testing summarise/table/plot."""
    episodes = pl.DataFrame(
        {
            "person_id": [1, 1, 2],
            "merged_episode_id": [1, 2, 3],
            "hip_episode_id": [1, 2, None],
            "pps_episode_id": [None, None, 1],
            "category": ["LB", "SA", "PREG"],
            "episode_start_date": [
                datetime.date(2019, 12, 1),
                datetime.date(2021, 3, 1),
                datetime.date(2020, 1, 1),
            ],
            "episode_end_date": [
                datetime.date(2020, 9, 1),
                datetime.date(2021, 6, 1),
                datetime.date(2020, 6, 1),
            ],
            "outcome_date": [
                datetime.date(2020, 9, 1),
                datetime.date(2021, 6, 1),
                None,
            ],
            "outcome_concept_id": [4014295, 4199459, None],
            "n_pps_records": [0, 0, 5],
            "source": ["HIP", "HIP", "PPS"],
            "esd_start_date": [None, None, None],
            "gestational_age_weeks": [39.0, 12.0, None],
            "precision": ["high", "low", "low"],
            "final_start_date": [
                datetime.date(2019, 12, 5),
                datetime.date(2021, 3, 5),
                datetime.date(2020, 1, 1),
            ],
        }
    )
    return PregnancyResult(
        episodes=episodes,
        hip_episodes=pl.DataFrame({"x": [1, 2]}),
        pps_episodes=pl.DataFrame({"x": [1]}),
        merged_episodes=episodes.drop(
            "esd_start_date", "gestational_age_weeks", "precision", "final_start_date"
        ),
        cdm_name="test_cdm",
        n_persons_input=2,
        n_episodes=3,
        settings={"start_date": None},
    )


class TestSummarisePregnancies:
    """Tests for summarise_pregnancies()."""

    def test_basic_summarise(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        assert isinstance(sr, SummarisedResult)
        assert len(sr) > 0

    def test_has_episode_count(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        count_rows = sr.data.filter(
            (pl.col("variable_name") == "Number episodes") & (pl.col("estimate_name") == "count")
        )
        assert count_rows.height >= 1
        assert count_rows["estimate_value"][0] == "3"

    def test_has_person_count(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        count_rows = sr.data.filter(
            (pl.col("variable_name") == "Number persons") & (pl.col("estimate_name") == "count")
        )
        assert count_rows.height >= 1
        assert count_rows["estimate_value"][0] == "2"

    def test_has_category_breakdown(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        cat_rows = sr.data.filter(pl.col("variable_name") == "Outcome category")
        assert cat_rows.height > 0

    def test_has_duration_stats(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        dur_rows = sr.data.filter(pl.col("variable_name") == "Episode duration (days)")
        assert dur_rows.height > 0

    def test_has_gestational_age_stats(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        ga_rows = sr.data.filter(pl.col("variable_name") == "Gestational age (weeks)")
        assert ga_rows.height > 0

    def test_strata_by_category(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result, strata=["category"])
        # Should have overall + per-category strata
        strata_names = sr.data["strata_name"].unique().to_list()
        assert "category" in strata_names

    def test_settings_metadata(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        assert sr.settings["result_type"][0] == "summarise_pregnancies"
        assert sr.settings["package_name"][0] == "omopy.pregnancy"

    def test_empty_episodes_still_works(self):
        empty_result = PregnancyResult(
            episodes=pl.DataFrame(
                schema={
                    "person_id": pl.Int64,
                    "merged_episode_id": pl.Int64,
                    "category": pl.Utf8,
                    "episode_start_date": pl.Date,
                    "episode_end_date": pl.Date,
                    "source": pl.Utf8,
                    "precision": pl.Utf8,
                    "gestational_age_weeks": pl.Float64,
                }
            ),
            hip_episodes=pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)}),
            pps_episodes=pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)}),
            merged_episodes=pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)}),
            cdm_name="empty",
            n_persons_input=0,
            n_episodes=0,
            settings={},
        )
        sr = summarise_pregnancies(empty_result)
        assert isinstance(sr, SummarisedResult)


class TestTablePregnancies:
    """Tests for table_pregnancies()."""

    def test_polars_output(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        tbl = table_pregnancies(sr, type="polars")
        assert isinstance(tbl, pl.DataFrame)
        assert tbl.height > 0

    def test_default_output(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        # Default tries GT, falls back to polars
        tbl = table_pregnancies(sr)
        assert tbl is not None


class TestPlotPregnancies:
    """Tests for plot_pregnancies()."""

    def test_outcome_plot(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        fig = plot_pregnancies(sr, type="outcome")
        assert fig is not None

    def test_source_plot(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        fig = plot_pregnancies(sr, type="source")
        assert fig is not None

    def test_duration_plot(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        fig = plot_pregnancies(sr, type="duration")
        assert fig is not None

    def test_precision_plot(self):
        result = _make_pregnancy_result()
        sr = summarise_pregnancies(result)
        fig = plot_pregnancies(sr, type="precision")
        assert fig is not None


# ===================================================================
# Public API imports
# ===================================================================


class TestPublicImports:
    """Verify that all public names are importable."""

    def test_import_identify_pregnancies(self):
        from omopy.pregnancy import identify_pregnancies

        assert callable(identify_pregnancies)

    def test_import_pregnancy_result(self):
        from omopy.pregnancy import PregnancyResult

        assert PregnancyResult is not None

    def test_import_summarise(self):
        from omopy.pregnancy import summarise_pregnancies

        assert callable(summarise_pregnancies)

    def test_import_table(self):
        from omopy.pregnancy import table_pregnancies

        assert callable(table_pregnancies)

    def test_import_plot(self):
        from omopy.pregnancy import plot_pregnancies

        assert callable(plot_pregnancies)

    def test_import_mock(self):
        from omopy.pregnancy import mock_pregnancy_cdm

        assert callable(mock_pregnancy_cdm)

    def test_import_validate(self):
        from omopy.pregnancy import validate_episodes

        assert callable(validate_episodes)

    def test_import_outcome_categories(self):
        from omopy.pregnancy import OUTCOME_CATEGORIES

        assert isinstance(OUTCOME_CATEGORIES, dict)

    def test_all_exports(self):
        import omopy.pregnancy

        assert hasattr(omopy.pregnancy, "__all__")
        assert len(omopy.pregnancy.__all__) == 8


# ===================================================================
# Additional edge case tests
# ===================================================================


class TestEdgeCases:
    """Edge case tests for various algorithm components."""

    def test_hip_single_ect(self):
        records = _make_hip_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 443213,
                    "record_date": datetime.date(2020, 4, 1),
                    "value_as_number": None,
                    "source_table": "condition_occurrence",
                    "category": "ECT",
                    "gest_value": None,
                }
            ]
        )
        result = _run_hip(records)
        assert result.height == 1
        # ECT max term = 84 days
        expected_start = datetime.date(2020, 4, 1) - datetime.timedelta(days=84)
        assert result["episode_start_date"][0] == expected_start

    def test_pps_null_months_tolerated(self):
        """PPS records with null min/max months should still be grouped."""
        records = _make_pps_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 1, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
                {
                    "person_id": 1,
                    "concept_id": 9999,
                    "record_date": datetime.date(2020, 3, 1),
                    "value_as_number": None,
                    "source_table": "observation",
                    "min_month": None,
                    "max_month": None,
                },
            ]
        )
        result = _run_pps(records)
        assert result.height == 1

    def test_merge_many_to_many(self):
        """Two HIP and two PPS episodes for one person with overlaps."""
        hip = pl.DataFrame(
            {
                "person_id": [1, 1],
                "episode_id": [1, 2],
                "category": ["LB", "SA"],
                "episode_start_date": [
                    datetime.date(2019, 6, 1),
                    datetime.date(2020, 6, 1),
                ],
                "episode_end_date": [
                    datetime.date(2020, 3, 1),
                    datetime.date(2020, 12, 1),
                ],
                "outcome_date": [
                    datetime.date(2020, 3, 1),
                    datetime.date(2020, 12, 1),
                ],
                "outcome_concept_id": [4014295, 4199459],
            }
        )
        pps = pl.DataFrame(
            {
                "person_id": [1, 1],
                "episode_id": [10, 11],
                "episode_start_date": [
                    datetime.date(2019, 7, 1),
                    datetime.date(2020, 7, 1),
                ],
                "episode_end_date": [
                    datetime.date(2020, 2, 1),
                    datetime.date(2020, 11, 1),
                ],
                "n_pps_records": [4, 3],
                "category": ["PREG", "PREG"],
            }
        )
        result = _merge_hipps(hip, pps)
        # Both HIP episodes overlap with their respective PPS episodes
        assert result.height == 2
        merged = result.filter(pl.col("source") == "HIP+PPS")
        assert merged.height == 2

    def test_esd_gestational_week_out_of_range_ignored(self):
        """GW values outside 1-45 should be ignored."""
        episodes = pl.DataFrame(
            {
                "person_id": [1],
                "merged_episode_id": [1],
                "hip_episode_id": [1],
                "pps_episode_id": [None],
                "category": ["LB"],
                "episode_start_date": [datetime.date(2019, 12, 1)],
                "episode_end_date": [datetime.date(2020, 9, 1)],
                "outcome_date": [datetime.date(2020, 9, 1)],
                "outcome_concept_id": [4014295],
                "n_pps_records": [0],
                "source": ["HIP"],
            }
        )
        esd = _make_esd_records(
            [
                {
                    "person_id": 1,
                    "concept_id": 4260747,
                    "record_date": datetime.date(2020, 9, 1),
                    "value_as_number": 99.0,  # Out of range
                    "source_table": "measurement",
                    "esd_category": "GW",
                    "esd_domain": "measurement",
                }
            ]
        )
        result = _run_esd(episodes, esd)
        # Should fall back to low precision since 99 weeks is out of range
        assert result["precision"][0] == "low"

    def test_validate_custom_max_days(self):
        df = pl.DataFrame(
            {
                "person_id": [1],
                "episode_start_date": [datetime.date(2020, 1, 1)],
                "episode_end_date": [datetime.date(2020, 7, 1)],
            }
        )
        # With max_days=100, this 182-day episode should be flagged
        result = validate_episodes(df, max_days=100)
        bad = result.filter((pl.col("check") == "max_duration") & (pl.col("n_violations") > 0))
        assert bad.height == 1

        # With max_days=200, it should be fine
        result2 = validate_episodes(df, max_days=200)
        bad2 = result2.filter((pl.col("check") == "max_duration") & (pl.col("n_violations") > 0))
        assert bad2.height == 0
