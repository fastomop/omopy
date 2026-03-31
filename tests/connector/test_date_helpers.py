"""Tests for omopy.connector.date_helpers — dateadd, datediff, datepart."""

from __future__ import annotations

import datetime

import ibis
import polars as pl
import pytest

from omopy.connector.date_helpers import (
    dateadd,
    dateadd_polars,
    datediff,
    datediff_polars,
    datepart,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mem_con():
    """In-memory DuckDB connection for date helper tests."""
    return ibis.duckdb.connect()


@pytest.fixture(scope="module")
def date_table(mem_con):
    """Ibis table with sample date data."""
    import pyarrow as pa

    data = pa.table(
        {
            "id": [1, 2, 3],
            "start_date": [
                datetime.date(2020, 1, 15),
                datetime.date(2021, 6, 30),
                datetime.date(2019, 12, 31),
            ],
            "end_date": [
                datetime.date(2020, 2, 15),
                datetime.date(2022, 1, 1),
                datetime.date(2020, 1, 1),
            ],
            "days_offset": [10, -5, 365],
        }
    )
    mem_con.con.register("test_dates", data)
    return mem_con.table("test_dates")


# ---------------------------------------------------------------------------
# dateadd tests
# ---------------------------------------------------------------------------


class TestDateadd:
    """Tests for the dateadd function."""

    def test_add_days_literal(self, date_table):
        result = date_table.mutate(
            new_date=dateadd(date_table, "start_date", 10)
        ).to_pyarrow()
        expected = datetime.date(2020, 1, 25)
        actual = result.column("new_date")[0].as_py()
        assert actual == expected

    def test_add_negative_days(self, date_table):
        result = date_table.mutate(
            new_date=dateadd(date_table, "start_date", -10)
        ).to_pyarrow()
        expected = datetime.date(2020, 1, 5)
        actual = result.column("new_date")[0].as_py()
        assert actual == expected

    def test_add_days_from_column(self, date_table):
        result = date_table.mutate(
            new_date=dateadd(date_table, "start_date", "days_offset")
        ).to_pyarrow()
        # Row 0: 2020-01-15 + 10 days = 2020-01-25
        assert result.column("new_date")[0].as_py() == datetime.date(2020, 1, 25)
        # Row 1: 2021-06-30 + (-5) days = 2021-06-25
        assert result.column("new_date")[1].as_py() == datetime.date(2021, 6, 25)
        # Row 2: 2019-12-31 + 365 days = 2020-12-30
        assert result.column("new_date")[2].as_py() == datetime.date(2020, 12, 30)

    def test_add_years(self, date_table):
        result = date_table.mutate(
            new_date=dateadd(date_table, "start_date", 2, interval="year")
        ).to_pyarrow()
        expected = datetime.date(2022, 1, 15)
        actual = result.column("new_date")[0].as_py()
        assert actual == expected

    def test_add_months(self, date_table):
        result = date_table.mutate(
            new_date=dateadd(date_table, "start_date", 3, interval="month")
        ).to_pyarrow()
        expected = datetime.date(2020, 4, 15)
        actual = result.column("new_date")[0].as_py()
        assert actual == expected

    def test_invalid_interval(self, date_table):
        with pytest.raises(ValueError, match="interval must be"):
            dateadd(date_table, "start_date", 1, interval="week")

    def test_column_number_requires_table(self):
        """When number is a column name, expr must be an Ibis Table."""
        col = ibis.literal(datetime.date(2020, 1, 1))
        with pytest.raises(TypeError, match="number is a column name"):
            dateadd(col, "start_date", "some_col")


# ---------------------------------------------------------------------------
# datediff tests
# ---------------------------------------------------------------------------


class TestDatediff:
    """Tests for the datediff function."""

    def test_diff_days(self, date_table):
        result = date_table.mutate(
            diff=datediff(date_table, "start_date", "end_date")
        ).to_pyarrow()
        # Row 0: 2020-02-15 - 2020-01-15 = 31 days
        assert result.column("diff")[0].as_py() == 31
        # Row 2: 2020-01-01 - 2019-12-31 = 1 day
        assert result.column("diff")[2].as_py() == 1

    def test_diff_days_negative(self, date_table):
        """Reverse order should give negative days."""
        result = date_table.mutate(
            diff=datediff(date_table, "end_date", "start_date")
        ).to_pyarrow()
        assert result.column("diff")[0].as_py() == -31

    def test_diff_years(self, date_table):
        result = date_table.mutate(
            diff=datediff(date_table, "start_date", "end_date", interval="year")
        ).to_pyarrow()
        # Row 0: 2020-01-15 to 2020-02-15 = 0 years
        assert int(result.column("diff")[0].as_py()) == 0
        # Row 1: 2021-06-30 to 2022-01-01 = 0 years (not a full year)
        assert int(result.column("diff")[1].as_py()) == 0

    def test_diff_months(self, date_table):
        result = date_table.mutate(
            diff=datediff(date_table, "start_date", "end_date", interval="month")
        ).to_pyarrow()
        # Row 0: 2020-01-15 to 2020-02-15 = 1 month
        assert int(result.column("diff")[0].as_py()) == 1

    def test_invalid_interval(self, date_table):
        with pytest.raises(ValueError, match="interval must be"):
            datediff(date_table, "start_date", "end_date", interval="week")


# ---------------------------------------------------------------------------
# datepart tests
# ---------------------------------------------------------------------------


class TestDatepart:
    """Tests for the datepart function."""

    def test_extract_year(self, date_table):
        result = date_table.mutate(
            yr=datepart(date_table, "start_date", "year")
        ).to_pyarrow()
        assert result.column("yr")[0].as_py() == 2020
        assert result.column("yr")[1].as_py() == 2021
        assert result.column("yr")[2].as_py() == 2019

    def test_extract_month(self, date_table):
        result = date_table.mutate(
            mo=datepart(date_table, "start_date", "month")
        ).to_pyarrow()
        assert result.column("mo")[0].as_py() == 1
        assert result.column("mo")[1].as_py() == 6
        assert result.column("mo")[2].as_py() == 12

    def test_extract_day(self, date_table):
        result = date_table.mutate(
            dy=datepart(date_table, "start_date", "day")
        ).to_pyarrow()
        assert result.column("dy")[0].as_py() == 15
        assert result.column("dy")[1].as_py() == 30
        assert result.column("dy")[2].as_py() == 31

    def test_invalid_part(self, date_table):
        with pytest.raises(ValueError, match="part must be"):
            datepart(date_table, "start_date", "hour")


# ---------------------------------------------------------------------------
# dateadd_polars tests
# ---------------------------------------------------------------------------


class TestDateaddPolars:
    """Tests for the Polars variant of dateadd."""

    def test_add_days(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 1, 15)],
            }
        )
        result = dateadd_polars(df, "d", 10)
        assert result["d"][0] == datetime.date(2020, 1, 25)

    def test_add_negative_days(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 1, 15)],
            }
        )
        result = dateadd_polars(df, "d", -5)
        assert result["d"][0] == datetime.date(2020, 1, 10)

    def test_add_years(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 3, 15)],
            }
        )
        result = dateadd_polars(df, "d", 2, interval="year")
        assert result["d"][0] == datetime.date(2022, 3, 15)

    def test_add_months(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 1, 15)],
            }
        )
        result = dateadd_polars(df, "d", 3, interval="month")
        assert result["d"][0] == datetime.date(2020, 4, 15)

    def test_result_col(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 1, 15)],
            }
        )
        result = dateadd_polars(df, "d", 10, result_col="new_d")
        assert "new_d" in result.columns
        assert result["new_d"][0] == datetime.date(2020, 1, 25)

    def test_column_number(self):
        df = pl.DataFrame(
            {
                "d": [datetime.date(2020, 1, 1), datetime.date(2020, 6, 15)],
                "n": [10, 20],
            }
        )
        result = dateadd_polars(df, "d", "n")
        assert result["d"][0] == datetime.date(2020, 1, 11)
        assert result["d"][1] == datetime.date(2020, 7, 5)


# ---------------------------------------------------------------------------
# datediff_polars tests
# ---------------------------------------------------------------------------


class TestDatediffPolars:
    """Tests for the Polars variant of datediff."""

    def test_diff_days(self):
        df = pl.DataFrame(
            {
                "s": [datetime.date(2020, 1, 1)],
                "e": [datetime.date(2020, 2, 1)],
            }
        )
        result = datediff_polars(df, "s", "e")
        assert result["date_diff"][0] == 31

    def test_diff_months(self):
        df = pl.DataFrame(
            {
                "s": [datetime.date(2020, 1, 15)],
                "e": [datetime.date(2020, 4, 15)],
            }
        )
        result = datediff_polars(df, "s", "e", interval="month")
        assert result["date_diff"][0] == 3

    def test_diff_years(self):
        df = pl.DataFrame(
            {
                "s": [datetime.date(2020, 1, 15)],
                "e": [datetime.date(2023, 1, 15)],
            }
        )
        result = datediff_polars(df, "s", "e", interval="year")
        assert result["date_diff"][0] == 3

    def test_diff_years_partial(self):
        """A partial year should floor to 0."""
        df = pl.DataFrame(
            {
                "s": [datetime.date(2020, 6, 1)],
                "e": [datetime.date(2021, 3, 1)],
            }
        )
        result = datediff_polars(df, "s", "e", interval="year")
        assert result["date_diff"][0] == 0

    def test_custom_result_col(self):
        df = pl.DataFrame(
            {
                "s": [datetime.date(2020, 1, 1)],
                "e": [datetime.date(2020, 1, 11)],
            }
        )
        result = datediff_polars(df, "s", "e", result_col="gap")
        assert "gap" in result.columns
        assert result["gap"][0] == 10


# ---------------------------------------------------------------------------
# Integration with Synthea database
# ---------------------------------------------------------------------------


class TestDateHelpersWithSynthea:
    """Integration tests using the Synthea test database."""

    def test_dateadd_on_real_table(self, synthea_con):
        """dateadd works on real OMOP tables."""
        person = synthea_con.table("person", database=("synthea", "base"))
        # Add 365 days to birth_datetime (if exists) or just test the expression builds
        if "birth_datetime" in person.columns:
            result = (
                person.select(
                    person.person_id,
                    new_date=dateadd(person, "birth_datetime", 365),
                )
                .limit(5)
                .to_pyarrow()
            )
            assert result.num_rows == 5

    def test_datepart_on_observation_period(self, synthea_con):
        """datepart extracts year from observation_period."""
        obs = synthea_con.table("observation_period", database=("synthea", "base"))
        result = (
            obs.mutate(
                start_year=datepart(obs, "observation_period_start_date", "year"),
            )
            .limit(5)
            .to_pyarrow()
        )
        assert result.num_rows == 5
        # All years should be reasonable (>1900, <2100)
        for i in range(result.num_rows):
            yr = result.column("start_year")[i].as_py()
            assert 1900 < yr < 2100

    def test_datediff_on_observation_period(self, synthea_con):
        """datediff computes observation period duration."""
        obs = synthea_con.table("observation_period", database=("synthea", "base"))
        result = (
            obs.mutate(
                duration=datediff(
                    obs, "observation_period_start_date", "observation_period_end_date"
                ),
            )
            .limit(5)
            .to_pyarrow()
        )
        assert result.num_rows == 5
        # All durations should be non-negative
        for i in range(result.num_rows):
            d = result.column("duration")[i].as_py()
            assert d >= 0
