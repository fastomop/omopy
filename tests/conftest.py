"""Shared test fixtures for the OMOPy test suite."""

from __future__ import annotations

import omopy  # noqa: F401 — triggers CPython 3.14 typing compat shim before Pydantic loads
from pathlib import Path

import ibis
import pytest

# Path to the synthea DuckDB test database
SYNTHEA_DB = Path(__file__).resolve().parent.parent / "data" / "synthea.duckdb"


@pytest.fixture(scope="session")
def synthea_con() -> ibis.BaseBackend:
    """Session-scoped Ibis DuckDB connection to the Synthea test database.

    Opened read-only so tests can run in parallel without conflicts.
    """
    if not SYNTHEA_DB.exists():
        pytest.skip(f"Synthea test database not found at {SYNTHEA_DB}")
    con = ibis.duckdb.connect(str(SYNTHEA_DB), read_only=True)
    yield con
    # Ibis connections don't always have .close(), but DuckDB does
    if hasattr(con, "disconnect"):
        con.disconnect()


@pytest.fixture(scope="session")
def synthea_db_source(synthea_con: ibis.BaseBackend):
    """Session-scoped DbSource wrapping the Synthea test database."""
    from omopy.connector.db_source import DbSource
    return DbSource(synthea_con, cdm_schema="base")


@pytest.fixture(scope="session")
def synthea_cdm(synthea_con: ibis.BaseBackend):
    """Session-scoped CdmReference from the Synthea test database."""
    from omopy.connector.cdm_from_con import cdm_from_con
    return cdm_from_con(synthea_con, cdm_schema="base")
