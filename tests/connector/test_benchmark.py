"""Tests for benchmark()."""

from __future__ import annotations

import pytest
import polars as pl

from omopy.connector.benchmark import benchmark


class TestBenchmark:
    """Tests for benchmark() with the Synthea test database."""

    def test_returns_polars_dataframe(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        assert isinstance(result, pl.DataFrame)

    def test_has_expected_columns(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        expected = {"task", "time_taken_secs", "time_taken_mins", "dbms", "person_n"}
        assert expected.issubset(set(result.columns))

    def test_has_multiple_tasks(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        assert len(result) >= 4

    def test_timings_non_negative(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        for t in result["time_taken_secs"].to_list():
            assert t >= 0

    def test_dbms_column(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        dbms_values = result["dbms"].unique().to_list()
        assert len(dbms_values) == 1
        assert dbms_values[0] == "duckdb"

    def test_person_n_is_27(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        person_n_values = result["person_n"].unique().to_list()
        assert 27 in person_n_values

    def test_tasks_are_strings(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        for task in result["task"].to_list():
            assert isinstance(task, str)
            assert len(task) > 0

    def test_time_taken_mins_consistent(self, synthea_cdm):
        result = benchmark(synthea_cdm)
        for row in result.iter_rows(named=True):
            expected_mins = round(row["time_taken_secs"] / 60.0, 4)
            assert abs(row["time_taken_mins"] - expected_mins) < 0.001
