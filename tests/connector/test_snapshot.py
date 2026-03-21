"""Tests for snapshot()."""

from __future__ import annotations

import datetime

import pytest

from omopy.connector.snapshot import CdmSnapshot, snapshot


class TestSnapshot:
    """Tests for snapshot() with the Synthea test database."""

    def test_returns_cdm_snapshot(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        assert isinstance(result, CdmSnapshot)

    def test_person_count(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        assert result.person_count == 27

    def test_observation_period_count(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        assert result.observation_period_count == 27

    def test_cdm_name(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        # cdm_name comes from cdm_from_con detection
        assert result.cdm_name is not None

    def test_cdm_source_name(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        # Synthea CDM has "dbt-synthea" as cdm_source_name
        assert result.cdm_source_name == "dbt-synthea"

    def test_observation_period_dates_are_dates(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        if result.earliest_observation_period_start_date is not None:
            assert isinstance(
                result.earliest_observation_period_start_date, datetime.date
            )
        if result.latest_observation_period_end_date is not None:
            assert isinstance(
                result.latest_observation_period_end_date, datetime.date
            )

    def test_earliest_before_latest(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        if (
            result.earliest_observation_period_start_date is not None
            and result.latest_observation_period_end_date is not None
        ):
            assert (
                result.earliest_observation_period_start_date
                <= result.latest_observation_period_end_date
            )

    def test_snapshot_date_is_today(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        today = datetime.date.today().isoformat()
        assert result.snapshot_date == today

    def test_to_dict(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        d = result.to_dict()
        assert isinstance(d, dict)
        assert "person_count" in d
        assert d["person_count"] == "27"

    def test_to_polars(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        df = result.to_polars()
        assert len(df) == 1
        assert "person_count" in df.columns

    def test_cdm_version_field(self, synthea_cdm):
        result = snapshot(synthea_cdm)
        assert result.cdm_version != ""

    def test_missing_person_table_raises(self, synthea_cdm):
        """Snapshot requires person table."""
        from omopy.generics.cdm_reference import CdmReference

        empty_cdm = CdmReference(
            tables={},
            cdm_version=synthea_cdm.cdm_version,
            cdm_name="empty",
            cdm_source=synthea_cdm.cdm_source,
        )
        with pytest.raises(KeyError, match="person"):
            snapshot(empty_cdm)
