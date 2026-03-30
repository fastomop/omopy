"""CdmReference — the top-level container for an OMOP CDM.

Mirrors R's ``cdm_reference`` S3 class. A CdmReference holds:
- A collection of named :class:`CdmTable` instances
- Metadata about the CDM version, source name, and backend type
- A :class:`CdmSource` protocol reference for backend operations

The CdmReference acts as a dict-like container: ``cdm["person"]`` returns
the ``person`` CdmTable.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from omopy.generics._types import CdmVersion
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable

__all__ = ["CdmReference", "CdmSource"]


# ---------------------------------------------------------------------------
# CdmSource protocol — backend interface
# ---------------------------------------------------------------------------


@runtime_checkable
class CdmSource(Protocol):
    """Protocol for CDM data sources (database backends, local files, etc.).

    Implementations in later phases:
    - ``DbSource`` (Phase 1): database-backed via Ibis/SQLAlchemy
    - ``LocalCdm`` (Phase 0): in-memory Polars DataFrames

    This protocol defines the minimal interface that CdmReference needs
    from its backend.
    """

    @property
    def source_type(self) -> str:
        """Backend identifier (e.g. ``'local'``, ``'duckdb'``, ``'postgres'``)."""
        ...

    def list_tables(self) -> list[str]:
        """Return names of all available tables in the source."""
        ...

    def read_table(self, table_name: str) -> CdmTable:
        """Read a table from the source, returning a CdmTable."""
        ...

    def write_table(self, table: CdmTable, table_name: str | None = None) -> None:
        """Write/compute a table into the source."""
        ...

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the source."""
        ...


# ---------------------------------------------------------------------------
# CdmReference
# ---------------------------------------------------------------------------


class CdmReference:
    """Top-level container for an OMOP CDM instance.

    Holds a collection of named CDM tables and optional source metadata.
    Behaves like a dict: ``cdm["person"]`` returns the person CdmTable.

    Usage::

        cdm = CdmReference(
            tables={"person": person_tbl, "observation_period": obs_tbl},
            cdm_version=CdmVersion.V5_4,
            cdm_name="my_cdm",
        )
        person = cdm["person"]
        cdm["my_cohort"] = my_cohort_table  # insert new table
    """

    __slots__ = ("_tables", "_cdm_version", "_cdm_name", "_cdm_source")

    def __init__(
        self,
        tables: dict[str, CdmTable] | None = None,
        *,
        cdm_version: CdmVersion = CdmVersion.V5_4,
        cdm_name: str = "",
        cdm_source: CdmSource | None = None,
    ) -> None:
        self._tables: dict[str, CdmTable] = {}
        self._cdm_version = cdm_version
        self._cdm_name = cdm_name
        self._cdm_source = cdm_source

        if tables:
            for name, tbl in tables.items():
                self._tables[name] = tbl
                tbl.cdm = self

    # -- Dict-like access ---------------------------------------------------

    def __getitem__(self, table_name: str) -> CdmTable:
        try:
            return self._tables[table_name]
        except KeyError:
            msg = f"Table '{table_name}' not found in CDM. Available: {self.table_names}"
            raise KeyError(msg) from None

    def __setitem__(self, table_name: str, table: CdmTable) -> None:
        table.cdm = self
        self._tables[table_name] = table

    def __delitem__(self, table_name: str) -> None:
        if table_name not in self._tables:
            msg = f"Table '{table_name}' not found in CDM"
            raise KeyError(msg)
        del self._tables[table_name]

    def __contains__(self, table_name: object) -> bool:
        return table_name in self._tables

    def __iter__(self):
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def get(self, table_name: str, default: CdmTable | None = None) -> CdmTable | None:
        return self._tables.get(table_name, default)

    # -- Properties ---------------------------------------------------------

    @property
    def cdm_version(self) -> CdmVersion:
        """The OMOP CDM version (5.3 or 5.4)."""
        return self._cdm_version

    @property
    def cdm_name(self) -> str:
        """Human-readable name for this CDM instance."""
        return self._cdm_name

    @cdm_name.setter
    def cdm_name(self, value: str) -> None:
        self._cdm_name = value

    @property
    def cdm_source(self) -> CdmSource | None:
        """The backend source, if any."""
        return self._cdm_source

    @property
    def table_names(self) -> list[str]:
        """Names of all tables currently in the CDM."""
        return list(self._tables.keys())

    @property
    def cohort_tables(self) -> dict[str, CohortTable]:
        """All tables that are CohortTable instances."""
        return {name: tbl for name, tbl in self._tables.items() if isinstance(tbl, CohortTable)}

    # -- Snapshot / summary -------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a summary snapshot of the CDM (table names, row counts, etc.)."""
        info: dict[str, Any] = {
            "cdm_name": self._cdm_name,
            "cdm_version": str(self._cdm_version),
            "source_type": self._cdm_source.source_type if self._cdm_source else "local",
            "tables": {},
        }
        for name, tbl in self._tables.items():
            try:
                nrows = tbl.count()
            except Exception:
                nrows = None
            info["tables"][name] = {
                "columns": tbl.columns,
                "nrows": nrows,
                "type": type(tbl).__name__,
            }
        return info

    # -- Table selection helpers --------------------------------------------

    def select_tables(self, names: list[str]) -> CdmReference:
        """Create a new CdmReference with only the specified tables."""
        tables = {n: self._tables[n] for n in names if n in self._tables}
        return CdmReference(
            tables=tables,
            cdm_version=self._cdm_version,
            cdm_name=self._cdm_name,
            cdm_source=self._cdm_source,
        )

    # -- Repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        n = len(self._tables)
        name = self._cdm_name or "(unnamed)"
        ver = self._cdm_version
        source = self._cdm_source.source_type if self._cdm_source else "local"
        return f"CdmReference(name={name!r}, version={ver}, source={source}, tables={n})"
