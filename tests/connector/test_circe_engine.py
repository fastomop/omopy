"""Integration tests for the CIRCE engine against the Synthea DuckDB.

These tests run the full CIRCE pipeline: parse JSON → resolve concepts →
build events → apply filters → compute end dates → collapse eras → output cohort.

NOTE: The Synthea test DB does NOT contain concept 192671 (GI hemorrhage)
which is used in the R CDMConnector example JSONs. Tests use concepts
that exist in the DB: 40481087 (Viral sinusitis), 320128 (Essential
hypertension), 260139 (Acute bronchitis), etc.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from omopy.connector.cdm_from_con import cdm_from_con
from omopy.connector.circe._concept_resolver import resolve_concept_sets
from omopy.connector.circe._criteria import (
    apply_limit,
    apply_observation_window,
)
from omopy.connector.circe._domain_queries import build_domain_query
from omopy.connector.circe._engine import generate_cohort_set
from omopy.connector.circe._era import collapse_eras
from omopy.connector.circe._types import (
    Concept,
    ConceptItem,
    ConceptSet,
    CriteriaLimit,
    DomainCriteria,
)

# Concepts known to exist in the Synthea test DB
VIRAL_SINUSITIS_ID = 40481087
ESSENTIAL_HYPERTENSION_ID = 320128
ACUTE_BRONCHITIS_ID = 260139
MALE_CONCEPT_ID = 8507


def _make_concept_set(
    cid: int,
    name: str,
    *,
    include_descendants: bool = False,
    concept_name: str = "",
    concept_code: str = "",
    domain_id: str = "Condition",
    vocabulary_id: str = "SNOMED",
    concept_class_id: str = "Disorder",
) -> ConceptSet:
    return ConceptSet(
        id=0,
        name=name,
        items=(
            ConceptItem(
                concept=Concept(
                    concept_id=cid,
                    concept_name=concept_name or name,
                    concept_code=concept_code,
                    domain_id=domain_id,
                    vocabulary_id=vocabulary_id,
                    standard_concept="S",
                    invalid_reason="",
                    concept_class_id=concept_class_id,
                ),
                include_descendants=include_descendants,
                include_mapped=False,
                is_excluded=False,
            ),
        ),
    )


def _make_cohort_json(
    concept_id: int,
    concept_name: str = "test",
    *,
    include_descendants: bool = False,
    primary_limit: str = "First",
    qualified_limit: str = "First",
    expression_limit: str = "First",
    end_strategy: dict | None = None,
    inclusion_rules: list | None = None,
    era_pad: int = 0,
) -> str:
    """Build a minimal CIRCE JSON string for testing."""
    d: dict = {
        "ConceptSets": [
            {
                "id": 0,
                "name": concept_name,
                "expression": {
                    "items": [
                        {
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
                            "includeDescendants": include_descendants,
                        }
                    ],
                },
            }
        ],
        "PrimaryCriteria": {
            "CriteriaList": [
                {
                    "ConditionOccurrence": {"CodesetId": 0},
                }
            ],
            "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
            "PrimaryCriteriaLimit": {"Type": primary_limit},
        },
        "QualifiedLimit": {"Type": qualified_limit},
        "ExpressionLimit": {"Type": expression_limit},
        "InclusionRules": inclusion_rules or [],
        "CensoringCriteria": [],
        "CollapseSettings": {"CollapseType": "ERA", "EraPad": era_pad},
        "CensorWindow": {},
    }
    if end_strategy is not None:
        d["EndStrategy"] = end_strategy
    return json.dumps(d)


# ---------------------------------------------------------------------------
# Concept resolver tests
# ---------------------------------------------------------------------------


class TestConceptResolver:
    """Test concept set resolution against Synthea DB."""

    def test_resolve_single_concept(self, synthea_con):
        """Resolve a concept set with a single concept (no descendants)."""
        cs = _make_concept_set(VIRAL_SINUSITIS_ID, "Viral sinusitis")
        result = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        assert 0 in result
        ids = result[0].execute()
        assert len(ids) > 0
        assert VIRAL_SINUSITIS_ID in ids["concept_id"].tolist()

    def test_resolve_with_descendants(self, synthea_con):
        """Resolve with descendants should return at least the concept itself."""
        cs_no_desc = _make_concept_set(
            VIRAL_SINUSITIS_ID, "VS", include_descendants=False
        )
        cs_desc = ConceptSet(
            id=1,
            name="VS_desc",
            items=(
                ConceptItem(
                    concept=Concept(
                        concept_id=VIRAL_SINUSITIS_ID,
                        concept_name="Viral sinusitis",
                        concept_code="",
                        domain_id="Condition",
                        vocabulary_id="SNOMED",
                        standard_concept="S",
                        invalid_reason="",
                        concept_class_id="Disorder",
                    ),
                    include_descendants=True,
                    include_mapped=False,
                    is_excluded=False,
                ),
            ),
        )
        result = resolve_concept_sets(
            (cs_no_desc, cs_desc), synthea_con, "synthea", "base"
        )
        no_desc_count = result[0].count().execute()
        desc_count = result[1].count().execute()
        assert desc_count >= no_desc_count
        assert no_desc_count >= 1

    def test_resolve_empty_concept_set(self, synthea_con):
        """Empty concept set returns empty table."""
        cs = ConceptSet(id=0, name="empty", items=())
        result = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        assert 0 in result
        ids = result[0].execute()
        assert len(ids) == 0


# ---------------------------------------------------------------------------
# Domain query tests
# ---------------------------------------------------------------------------


class TestDomainQueries:
    """Test domain query building against Synthea DB."""

    def test_condition_occurrence_query(self, synthea_con):
        """Build a ConditionOccurrence query for viral sinusitis."""
        cs = _make_concept_set(VIRAL_SINUSITIS_ID, "Viral sinusitis")
        codeset_tables = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        dc = DomainCriteria(domain_type="ConditionOccurrence", codeset_id=0)
        events = build_domain_query(dc, synthea_con, "synthea", "base", codeset_tables)

        result = events.execute()
        assert "person_id" in result.columns
        assert "event_id" in result.columns
        assert "start_date" in result.columns
        assert "end_date" in result.columns
        assert "sort_date" in result.columns
        assert "visit_occurrence_id" in result.columns
        assert len(result) > 0  # Should find viral sinusitis events

    def test_condition_occurrence_count(self, synthea_con):
        """Viral sinusitis should have 4 occurrences in the Synthea DB."""
        cs = _make_concept_set(VIRAL_SINUSITIS_ID, "Viral sinusitis")
        codeset_tables = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        dc = DomainCriteria(domain_type="ConditionOccurrence", codeset_id=0)
        events = build_domain_query(dc, synthea_con, "synthea", "base", codeset_tables)
        count = events.count().execute()
        assert count == 4


# ---------------------------------------------------------------------------
# Observation window + limit tests
# ---------------------------------------------------------------------------


class TestObservationWindowAndLimit:
    """Test observation window filtering and limits."""

    def test_observation_window_basic(self, synthea_con):
        """Events within observation periods pass through."""
        cs = _make_concept_set(VIRAL_SINUSITIS_ID, "Viral sinusitis")
        codeset_tables = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        dc = DomainCriteria(domain_type="ConditionOccurrence", codeset_id=0)
        events = build_domain_query(dc, synthea_con, "synthea", "base", codeset_tables)

        obs_period = synthea_con.table(
            "observation_period", database=("synthea", "base")
        )

        filtered = apply_observation_window(events, obs_period)
        result = filtered.execute()

        assert "op_start_date" in result.columns
        assert "op_end_date" in result.columns
        assert len(result) > 0

    def test_first_limit_one_per_person(self, synthea_con):
        """First limit keeps one event per person."""
        cs = _make_concept_set(ESSENTIAL_HYPERTENSION_ID, "Essential hypertension")
        codeset_tables = resolve_concept_sets((cs,), synthea_con, "synthea", "base")
        dc = DomainCriteria(domain_type="ConditionOccurrence", codeset_id=0)
        events = build_domain_query(dc, synthea_con, "synthea", "base", codeset_tables)

        obs_period = synthea_con.table(
            "observation_period", database=("synthea", "base")
        )
        events = apply_observation_window(events, obs_period)

        limit = CriteriaLimit(type="First")
        limited = apply_limit(events, limit)
        result = limited.execute()

        # Each person should appear at most once
        if len(result) > 0:
            person_counts = result.groupby("person_id").size()
            assert person_counts.max() == 1


# ---------------------------------------------------------------------------
# Era collapse tests
# ---------------------------------------------------------------------------


class TestEraCollapse:
    """Test the era collapse algorithm."""

    def test_no_overlap(self, synthea_con):
        """Non-overlapping periods stay separate."""
        import pyarrow as pa

        data = pa.table(
            {
                "person_id": pa.array([1, 1, 2], type=pa.int64()),
                "start_date": pa.array(["2020-01-01", "2020-06-01", "2020-01-01"]),
                "end_date": pa.array(["2020-01-31", "2020-06-30", "2020-12-31"]),
            }
        )
        synthea_con.con.register("__test_era_no_overlap", data)
        tbl = synthea_con.table("__test_era_no_overlap")
        tbl = tbl.cast({"start_date": "date", "end_date": "date"})

        result = collapse_eras(tbl, era_pad=0).execute()
        assert len(result) == 3

    def test_overlapping_periods(self, synthea_con):
        """Overlapping periods for same person get merged."""
        import pyarrow as pa

        data = pa.table(
            {
                "person_id": pa.array([1, 1, 1], type=pa.int64()),
                "start_date": pa.array(["2020-01-01", "2020-01-15", "2020-02-01"]),
                "end_date": pa.array(["2020-01-31", "2020-02-15", "2020-03-01"]),
            }
        )
        synthea_con.con.register("__test_era_overlap", data)
        tbl = synthea_con.table("__test_era_overlap")
        tbl = tbl.cast({"start_date": "date", "end_date": "date"})

        result = collapse_eras(tbl, era_pad=0).execute()
        assert len(result) == 1
        row = result.iloc[0]
        assert str(row["start_date"])[:10] == "2020-01-01"
        assert str(row["end_date"])[:10] == "2020-03-01"

    def test_era_pad_bridges_gaps(self, synthea_con):
        """Era pad bridges small gaps between periods."""
        import pyarrow as pa

        data = pa.table(
            {
                "person_id": pa.array([1, 1], type=pa.int64()),
                "start_date": pa.array(["2020-01-01", "2020-02-05"]),
                "end_date": pa.array(["2020-01-31", "2020-03-01"]),
            }
        )
        synthea_con.con.register("__test_era_pad", data)
        tbl = synthea_con.table("__test_era_pad")
        tbl = tbl.cast({"start_date": "date", "end_date": "date"})

        # Gap is 5 days (Feb 1-4). era_pad=0 should keep them separate.
        result_no_pad = collapse_eras(tbl, era_pad=0).execute()
        assert len(result_no_pad) == 2

        # era_pad=5 should merge them
        result_pad = collapse_eras(tbl, era_pad=5).execute()
        assert len(result_pad) == 1


# ---------------------------------------------------------------------------
# Full engine integration tests
# ---------------------------------------------------------------------------


class TestGenerateCohortSet:
    """End-to-end CIRCE engine tests against Synthea DB."""

    @pytest.fixture
    def cdm(self, synthea_con):
        """Fresh CDM reference for each test."""
        return cdm_from_con(synthea_con, cdm_schema="base")

    def test_simple_cohort_first_per_person(self, cdm):
        """Simple condition cohort with First limit per person."""
        json_str = _make_cohort_json(
            VIRAL_SINUSITIS_ID,
            "Viral sinusitis",
            primary_limit="First",
            qualified_limit="First",
            expression_limit="First",
        )

        result = generate_cohort_set(cdm, json_str, name="vs_first")
        cohort = result["vs_first"]
        df = cohort.collect()

        assert isinstance(df, pl.DataFrame)
        assert set(df.columns) >= {
            "cohort_definition_id",
            "subject_id",
            "cohort_start_date",
            "cohort_end_date",
        }

        # There are 3 persons with viral sinusitis
        assert len(df) == 3

        # First-per-person: each subject appears exactly once
        subject_counts = df.group_by("subject_id").len()
        assert subject_counts["len"].max() == 1

    def test_all_events_cohort(self, cdm):
        """Cohort with All limits keeps all events."""
        json_str = _make_cohort_json(
            VIRAL_SINUSITIS_ID,
            "Viral sinusitis",
            primary_limit="All",
            qualified_limit="All",
            expression_limit="All",
        )

        result = generate_cohort_set(cdm, json_str, name="vs_all")
        cohort = result["vs_all"]
        df = cohort.collect()

        # Should have at least 4 events (one person has 2 viral sinusitis)
        # (may be fewer after era collapse)
        assert len(df) >= 3

    def test_date_offset_end_strategy(self, cdm):
        """DateOffset end strategy caps end date."""
        json_str = _make_cohort_json(
            ESSENTIAL_HYPERTENSION_ID,
            "Essential hypertension",
            primary_limit="All",
            qualified_limit="All",
            expression_limit="All",
            end_strategy={
                "DateOffset": {"DateField": "StartDate", "Offset": 10},
            },
        )

        result = generate_cohort_set(cdm, json_str, name="ht_end10")
        cohort = result["ht_end10"]
        df = cohort.collect()

        assert len(df) > 0
        for row in df.iter_rows(named=True):
            delta = (row["cohort_end_date"] - row["cohort_start_date"]).days
            assert delta <= 10

    def test_inclusion_rule_gender(self, cdm):
        """Inclusion rule filtering by gender (male only)."""
        json_str = _make_cohort_json(
            ESSENTIAL_HYPERTENSION_ID,
            "Essential hypertension",
            primary_limit="All",
            qualified_limit="First",
            expression_limit="First",
            inclusion_rules=[
                {
                    "name": "Male",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [],
                        "DemographicCriteriaList": [
                            {
                                "Gender": [
                                    {
                                        "CONCEPT_CODE": "M",
                                        "CONCEPT_ID": MALE_CONCEPT_ID,
                                        "CONCEPT_NAME": "MALE",
                                        "DOMAIN_ID": "Gender",
                                        "INVALID_REASON_CAPTION": "Unknown",
                                        "STANDARD_CONCEPT_CAPTION": "Unknown",
                                        "VOCABULARY_ID": "Gender",
                                    }
                                ],
                            }
                        ],
                        "Groups": [],
                    },
                }
            ],
        )

        result = generate_cohort_set(cdm, json_str, name="ht_male")
        cohort = result["ht_male"]
        df = cohort.collect()

        if len(df) > 0:
            person = cdm["person"].collect()
            male_ids = set(
                person.filter(pl.col("gender_concept_id") == MALE_CONCEPT_ID)[
                    "person_id"
                ].to_list()
            )
            cohort_ids = set(df["subject_id"].to_list())
            assert cohort_ids.issubset(male_ids)

    def test_empty_cohort_nonexistent_concept(self, cdm):
        """A cohort with a non-existent concept produces empty result."""
        json_str = _make_cohort_json(
            999999999,
            "Nonexistent",
            primary_limit="First",
        )

        result = generate_cohort_set(cdm, json_str, name="empty_cohort")
        cohort = result["empty_cohort"]
        df = cohort.collect()
        assert len(df) == 0

    def test_dict_input(self, cdm):
        """Accept a dict with expression key."""
        raw = json.loads(
            _make_cohort_json(
                VIRAL_SINUSITIS_ID,
                "Viral sinusitis",
            )
        )
        defn = {
            "cohort_definition_id": 42,
            "cohort_name": "vs_dict",
            "expression": raw,
        }

        result = generate_cohort_set(cdm, defn, name="dict_cohort")
        cohort = result["dict_cohort"]
        settings = cohort.settings
        assert settings["cohort_definition_id"][0] == 42
        assert settings["cohort_name"][0] == "vs_dict"

    def test_multiple_cohorts_as_list(self, cdm):
        """Pass multiple cohort definitions as a list."""
        defns = [
            {
                "cohort_definition_id": 1,
                "cohort_name": "viral_sinusitis",
                "expression": json.loads(
                    _make_cohort_json(
                        VIRAL_SINUSITIS_ID,
                        "Viral sinusitis",
                    )
                ),
            },
            {
                "cohort_definition_id": 2,
                "cohort_name": "hypertension",
                "expression": json.loads(
                    _make_cohort_json(
                        ESSENTIAL_HYPERTENSION_ID,
                        "Essential hypertension",
                    )
                ),
            },
        ]

        result = generate_cohort_set(cdm, defns, name="multi")
        cohort = result["multi"]

        settings = cohort.settings
        assert len(settings) == 2

        df = cohort.collect()
        assert len(df) > 0
        # Both cohort IDs should be present
        cids = df["cohort_definition_id"].unique().sort().to_list()
        assert 1 in cids
        assert 2 in cids


# ---------------------------------------------------------------------------
# Cohort validity checks
# ---------------------------------------------------------------------------


class TestCohortValidity:
    """Validate cohort output meets OMOP CDM requirements."""

    @pytest.fixture
    def cohort(self, synthea_con):
        """Generate a test cohort."""
        cdm = cdm_from_con(synthea_con, cdm_schema="base")
        json_str = _make_cohort_json(
            VIRAL_SINUSITIS_ID,
            "Viral sinusitis",
        )
        result = generate_cohort_set(cdm, json_str, name="validity_test")
        return result["validity_test"]

    def test_end_date_gte_start_date(self, cohort):
        """Cohort end dates must be >= start dates."""
        df = cohort.collect()
        if len(df) > 0:
            invalid = df.filter(pl.col("cohort_end_date") < pl.col("cohort_start_date"))
            assert len(invalid) == 0

    def test_all_subjects_in_person(self, cohort, synthea_con):
        """All cohort subjects must exist in the person table."""
        df = cohort.collect()
        if len(df) > 0:
            person = synthea_con.table("person", database=("synthea", "base")).execute()
            person_ids = set(person["person_id"].tolist())
            cohort_ids = set(df["subject_id"].to_list())
            assert cohort_ids.issubset(person_ids)

    def test_has_attrition(self, cohort):
        """Cohort should have attrition tracking."""
        attrition = cohort.attrition
        assert len(attrition) > 0
        assert "reason" in attrition.columns
        assert "number_records" in attrition.columns

    def test_has_settings(self, cohort):
        """Cohort should have settings with name."""
        settings = cohort.settings
        assert len(settings) == 1
        assert "cohort_name" in settings.columns
