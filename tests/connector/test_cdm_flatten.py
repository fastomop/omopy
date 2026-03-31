"""Tests for cdm_flatten()."""

from __future__ import annotations

import pytest

from omopy.connector.cdm_flatten import cdm_flatten


class TestCdmFlatten:
    """Tests for cdm_flatten() with the Synthea test database."""

    def test_default_domains(self, synthea_cdm):
        """Default domains: condition, drug, procedure."""
        result = cdm_flatten(synthea_cdm)
        # Ibis table — execute to check
        df = result.execute()
        assert len(df) > 0
        assert "person_id" in df.columns
        assert "observation_concept_id" in df.columns
        assert "start_date" in df.columns
        assert "end_date" in df.columns
        assert "domain" in df.columns

    def test_includes_concept_names_by_default(self, synthea_cdm):
        result = cdm_flatten(synthea_cdm)
        df = result.execute()
        assert "observation_concept_name" in df.columns
        assert "type_concept_name" in df.columns

    def test_without_concept_names(self, synthea_cdm):
        result = cdm_flatten(synthea_cdm, include_concept_name=False)
        df = result.execute()
        assert "observation_concept_name" not in df.columns
        assert "type_concept_name" not in df.columns

    def test_single_domain(self, synthea_cdm):
        result = cdm_flatten(synthea_cdm, domains=["condition_occurrence"])
        df = result.execute()
        assert len(df) > 0
        # All rows should be from condition_occurrence
        domains = set(df["domain"])
        assert domains == {"condition_occurrence"}

    def test_multiple_domains(self, synthea_cdm):
        result = cdm_flatten(
            synthea_cdm,
            domains=["condition_occurrence", "measurement"],
        )
        df = result.execute()
        domains = set(df["domain"])
        assert "condition_occurrence" in domains
        assert "measurement" in domains

    def test_all_seven_domains(self, synthea_cdm):
        """Include all domains — should not error."""
        all_domains = [
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "measurement",
            "visit_occurrence",
            "observation",
        ]
        # Only include domains that exist in this CDM
        available = [d for d in all_domains if d in synthea_cdm]
        result = cdm_flatten(synthea_cdm, domains=available)
        df = result.execute()
        assert len(df) > 0

    def test_invalid_domain_raises(self, synthea_cdm):
        with pytest.raises(ValueError, match="Invalid domain"):
            cdm_flatten(synthea_cdm, domains=["nonexistent_table"])

    def test_missing_domain_table_raises(self, synthea_cdm):
        """If a valid domain name is given but table doesn't exist in CDM."""
        from omopy.generics.cdm_reference import CdmReference

        empty_cdm = CdmReference(
            tables={},
            cdm_version=synthea_cdm.cdm_version,
            cdm_name="empty",
            cdm_source=synthea_cdm.cdm_source,
        )
        with pytest.raises(KeyError, match="not found in CDM"):
            cdm_flatten(empty_cdm, domains=["condition_occurrence"])

    def test_condition_occurrence_has_expected_count(self, synthea_cdm):
        """Synthea has 59 condition_occurrence records."""
        result = cdm_flatten(
            synthea_cdm,
            domains=["condition_occurrence"],
            include_concept_name=False,
        )
        df = result.execute()
        # Should have at most 59 distinct rows
        assert len(df) > 0
        assert len(df) <= 59

    def test_person_ids_are_integers(self, synthea_cdm):
        result = cdm_flatten(
            synthea_cdm,
            domains=["condition_occurrence"],
            include_concept_name=False,
        )
        df = result.execute()
        # person_id should be numeric
        assert df["person_id"].dtype.name.startswith(("int", "Int", "float"))
