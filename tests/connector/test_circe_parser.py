"""Tests for the CIRCE JSON parser.

Tests parsing of CIRCE cohort definition JSON files into the
CohortExpression dataclass hierarchy.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omopy.connector.circe._parser import (
    parse_cohort_expression,
    parse_cohort_json,
    read_cohort_set,
)
from omopy.connector.circe._types import (
    CohortExpression,
)

# Path to example cohort JSONs in the R package
R_INST = Path(__file__).resolve().parent.parent.parent.parent / "CDMConnector" / "inst"
EXAMPLE_COHORTS = R_INST / "example_cohorts"
COHORTS3 = R_INST / "cohorts3"
COHORTS5 = R_INST / "cohorts5"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _load_json(path: Path) -> dict:
    """Load a JSON file as a dict."""
    return json.loads(path.read_text())


def _skip_if_no_r_inst():
    """Skip test if R package inst directory is not available."""
    if not R_INST.exists():
        pytest.skip("R CDMConnector inst directory not found")


# ---------------------------------------------------------------------------
# Basic parsing tests
# ---------------------------------------------------------------------------


class TestParseMinimal:
    """Parse minimal / synthetic JSON."""

    def test_empty_expression(self):
        """Empty dict produces defaults."""
        expr = parse_cohort_expression({})
        assert isinstance(expr, CohortExpression)
        assert expr.concept_sets == ()
        assert expr.primary_criteria.criteria_list == ()
        assert expr.inclusion_rules == ()

    def test_parse_json_string(self):
        """parse_cohort_json accepts a JSON string."""
        raw = json.dumps(
            {
                "ConceptSets": [],
                "PrimaryCriteria": {
                    "CriteriaList": [],
                    "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                    "PrimaryCriteriaLimit": {"Type": "All"},
                },
                "QualifiedLimit": {"Type": "First"},
                "ExpressionLimit": {"Type": "First"},
                "InclusionRules": [],
                "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
                "CensorWindow": {},
            }
        )
        expr = parse_cohort_json(raw)
        assert isinstance(expr, CohortExpression)
        assert expr.qualified_limit.type == "First"
        assert expr.expression_limit.type == "First"


# ---------------------------------------------------------------------------
# GiBleed_default.json
# ---------------------------------------------------------------------------


class TestGiBleedDefault:
    """Parse the simplest example cohort: GiBleed_default."""

    @pytest.fixture(autouse=True)
    def _load(self):
        _skip_if_no_r_inst()
        self.path = EXAMPLE_COHORTS / "GiBleed_default.json"
        if not self.path.exists():
            pytest.skip(f"File not found: {self.path}")
        self.expr = parse_cohort_expression(_load_json(self.path))

    def test_is_cohort_expression(self):
        assert isinstance(self.expr, CohortExpression)

    def test_concept_sets(self):
        assert len(self.expr.concept_sets) == 1
        cs = self.expr.concept_sets[0]
        assert cs.id == 0
        assert cs.name == "gibleed"
        assert len(cs.items) == 1
        assert cs.items[0].concept.concept_id == 192671
        assert cs.items[0].include_descendants is False

    def test_primary_criteria(self):
        pc = self.expr.primary_criteria
        assert len(pc.criteria_list) == 1
        dc = pc.criteria_list[0]
        assert dc.domain_type == "ConditionOccurrence"
        assert dc.codeset_id == 0

    def test_observation_window(self):
        ow = self.expr.primary_criteria.observation_window
        assert ow.prior_days == 0
        assert ow.post_days == 0

    def test_primary_limit(self):
        assert self.expr.primary_criteria.primary_limit.type == "First"

    def test_qualified_limit(self):
        assert self.expr.qualified_limit.type == "First"

    def test_expression_limit(self):
        assert self.expr.expression_limit.type == "First"

    def test_no_inclusion_rules(self):
        assert self.expr.inclusion_rules == ()

    def test_no_end_strategy(self):
        assert self.expr.end_strategy is None

    def test_collapse_settings(self):
        assert self.expr.collapse_settings.collapse_type == "ERA"
        assert self.expr.collapse_settings.era_pad == 0

    def test_no_censoring_criteria(self):
        assert self.expr.censoring_criteria == ()


# ---------------------------------------------------------------------------
# gibleed_end_10.json — with DateOffset end strategy
# ---------------------------------------------------------------------------


class TestGiBleedEnd10:
    """Parse cohort with DateOffset end strategy."""

    @pytest.fixture(autouse=True)
    def _load(self):
        _skip_if_no_r_inst()
        self.path = COHORTS3 / "gibleed_end_10.json"
        if not self.path.exists():
            pytest.skip(f"File not found: {self.path}")
        self.expr = parse_cohort_expression(_load_json(self.path))

    def test_has_end_strategy(self):
        assert self.expr.end_strategy is not None

    def test_date_offset(self):
        do = self.expr.end_strategy.date_offset
        assert do is not None
        assert do.date_field == "StartDate"
        assert do.offset == 10

    def test_all_limits(self):
        """All three limits should be 'All'."""
        assert self.expr.primary_criteria.primary_limit.type == "All"
        assert self.expr.qualified_limit.type == "All"
        assert self.expr.expression_limit.type == "All"


# ---------------------------------------------------------------------------
# GIBleed_male.json — with inclusion rules (gender + observation period)
# ---------------------------------------------------------------------------


class TestGiBleedMale:
    """Parse cohort with inclusion rules."""

    @pytest.fixture(autouse=True)
    def _load(self):
        _skip_if_no_r_inst()
        self.path = EXAMPLE_COHORTS / "GIBleed_male.json"
        if not self.path.exists():
            pytest.skip(f"File not found: {self.path}")
        self.expr = parse_cohort_expression(_load_json(self.path))

    def test_has_inclusion_rules(self):
        assert len(self.expr.inclusion_rules) == 2

    def test_first_rule_is_male(self):
        rule = self.expr.inclusion_rules[0]
        assert rule.name == "Male"
        # Should have a demographic criteria with gender filter
        group = rule.expression
        assert group.type == "ALL"
        assert len(group.demographic_criteria_list) == 1
        dc = group.demographic_criteria_list[0]
        assert dc.gender is not None
        assert 8507 in dc.gender.concept_ids

    def test_second_rule_is_prior_observation(self):
        rule = self.expr.inclusion_rules[1]
        assert rule.name == "30 days prior observation"
        group = rule.expression
        assert group.type == "ALL"
        assert len(group.criteria_list) == 1
        cc = group.criteria_list[0]
        assert cc.criteria.domain_type == "ObservationPeriod"

    def test_concept_set_has_descendants(self):
        cs = self.expr.concept_sets[0]
        assert cs.items[0].include_descendants is True


# ---------------------------------------------------------------------------
# read_cohort_set — directory of JSONs
# ---------------------------------------------------------------------------


class TestReadCohortSet:
    """Test reading a directory of JSON files."""

    def test_read_example_cohorts(self):
        _skip_if_no_r_inst()
        if not EXAMPLE_COHORTS.exists():
            pytest.skip("Example cohorts directory not found")
        result = read_cohort_set(EXAMPLE_COHORTS)
        assert isinstance(result, list)
        assert len(result) >= 2  # At least GiBleed_default and GIBleed_male
        for entry in result:
            assert "cohort_definition_id" in entry
            assert "cohort_name" in entry
            assert "expression" in entry
            assert isinstance(entry["expression"], CohortExpression)

    def test_read_cohorts3(self):
        _skip_if_no_r_inst()
        if not COHORTS3.exists():
            pytest.skip("cohorts3 directory not found")
        result = read_cohort_set(COHORTS3)
        assert isinstance(result, list)
        assert len(result) >= 1
        # All should parse successfully
        for entry in result:
            assert isinstance(entry["expression"], CohortExpression)

    def test_cohort_ids_are_sequential(self):
        _skip_if_no_r_inst()
        if not EXAMPLE_COHORTS.exists():
            pytest.skip("Example cohorts directory not found")
        result = read_cohort_set(EXAMPLE_COHORTS)
        ids = [e["cohort_definition_id"] for e in result]
        assert ids == list(range(1, len(ids) + 1))


# ---------------------------------------------------------------------------
# Viral sinusitis — with EndStrategy
# ---------------------------------------------------------------------------


class TestViralSinusitis:
    """Parse viral sinusitis cohort (more complex features)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        _skip_if_no_r_inst()
        self.path = COHORTS5 / "viral_sinusitus.json"
        if not self.path.exists():
            pytest.skip(f"File not found: {self.path}")
        self.expr = parse_cohort_expression(_load_json(self.path))

    def test_has_end_strategy(self):
        assert self.expr.end_strategy is not None

    def test_has_inclusion_rule(self):
        assert len(self.expr.inclusion_rules) >= 1

    def test_concept_set_name(self):
        assert len(self.expr.concept_sets) >= 1


# ---------------------------------------------------------------------------
# GiBleed_default_with_descendants.json
# ---------------------------------------------------------------------------


class TestGiBleedWithDescendants:
    """Parse cohort with descendant expansion."""

    @pytest.fixture(autouse=True)
    def _load(self):
        _skip_if_no_r_inst()
        self.path = COHORTS3 / "GiBleed_default_with_descendants.json"
        if not self.path.exists():
            pytest.skip(f"File not found: {self.path}")
        self.expr = parse_cohort_expression(_load_json(self.path))

    def test_has_descendants(self):
        cs = self.expr.concept_sets[0]
        assert cs.items[0].include_descendants is True

    def test_same_concept_id(self):
        cs = self.expr.concept_sets[0]
        assert cs.items[0].concept.concept_id == 192671


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Parser edge cases."""

    def test_camel_case_keys(self):
        """camelCase keys should be accepted."""
        raw = {
            "conceptSets": [],
            "primaryCriteria": {
                "criteriaList": [],
                "observationWindow": {"priorDays": 5, "postDays": 10},
                "primaryCriteriaLimit": {"type": "Last"},
            },
            "qualifiedLimit": {"type": "All"},
            "expressionLimit": {"type": "First"},
            "inclusionRules": [],
            "collapseSettings": {"collapseType": "ERA", "eraPad": 30},
            "censorWindow": {},
        }
        expr = parse_cohort_expression(raw)
        assert expr.primary_criteria.observation_window.prior_days == 5
        assert expr.primary_criteria.observation_window.post_days == 10
        assert expr.primary_criteria.primary_limit.type == "Last"
        assert expr.collapse_settings.era_pad == 30

    def test_missing_optional_fields(self):
        """Missing optional fields default to sensible values."""
        raw = {
            "ConceptSets": [
                {
                    "id": 0,
                    "name": "test",
                    "expression": {"items": []},
                }
            ],
            "PrimaryCriteria": {
                "CriteriaList": [],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
        expr = parse_cohort_expression(raw)
        assert expr.qualified_limit.type == "All"  # default
        assert expr.inclusion_rules == ()
        assert expr.end_strategy is None
        assert expr.collapse_settings.era_pad == 0
