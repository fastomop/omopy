"""Tests for compute_data_hash()."""

from __future__ import annotations

import polars as pl

from omopy.connector.data_hash import compute_data_hash


class TestComputeDataHash:
    """Tests for compute_data_hash() with the Synthea test database."""

    def test_returns_polars_dataframe(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        assert isinstance(result, pl.DataFrame)

    def test_has_expected_columns(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        expected_cols = {
            "cdm_name",
            "table_name",
            "table_row_count",
            "unique_column",
            "n_unique_values",
            "table_hash",
            "compute_time_secs",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_has_rows_for_all_standard_tables(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        # Should have one row per table in _TABLE_KEY_COLUMNS (22 tables)
        assert len(result) >= 20

    def test_person_table_row_count(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        person_row = result.filter(pl.col("table_name") == "person")
        assert len(person_row) == 1
        assert person_row["table_row_count"][0] == 27

    def test_person_table_has_positive_unique(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        person_row = result.filter(pl.col("table_name") == "person")
        assert person_row["n_unique_values"][0] > 0

    def test_hash_is_md5_hex(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        # For tables present in CDM, hash should be a 32-char hex string
        for row in result.iter_rows(named=True):
            h = row["table_hash"]
            if h != "Table not found in CDM":
                assert len(h) == 32
                assert all(c in "0123456789abcdef" for c in h)

    def test_missing_table_gets_not_found(self, synthea_cdm):
        """Tables not in CDM should get 'Table not found' hash."""
        result = compute_data_hash(synthea_cdm)
        # dose_era is unlikely to be in synthea
        dose_era = result.filter(pl.col("table_name") == "dose_era")
        if len(dose_era) > 0 and dose_era["table_row_count"][0] == -1:
            assert dose_era["table_hash"][0] == "Table not found in CDM"

    def test_hash_deterministic(self, synthea_cdm):
        """Running twice should produce the same hashes."""
        result1 = compute_data_hash(synthea_cdm)
        result2 = compute_data_hash(synthea_cdm)
        hashes1 = result1["table_hash"].to_list()
        hashes2 = result2["table_hash"].to_list()
        assert hashes1 == hashes2

    def test_compute_time_non_negative(self, synthea_cdm):
        result = compute_data_hash(synthea_cdm)
        for t in result["compute_time_secs"].to_list():
            assert t >= 0
