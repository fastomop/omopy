"""CdmTable — metadata-preserving wrapper around a Polars or Ibis table.

This is the Python equivalent of R's ``cdm_table`` S3 class. It wraps a
data source (Polars DataFrame, LazyFrame, or Ibis Table expression) and
carries metadata (table name, source reference, CDM reference back-pointer)
through all transformations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

import polars as pl

if TYPE_CHECKING:
    from omopy.generics.cdm_reference import CdmReference

__all__ = ["CdmTable", "TableData"]


# ---------------------------------------------------------------------------
# Table data protocol — what CdmTable wraps
# ---------------------------------------------------------------------------

@runtime_checkable
class TableData(Protocol):
    """Protocol for data backends that CdmTable can wrap.

    Any object that has a ``columns`` property (returning column names)
    and ``schema`` (returning a mapping of name -> dtype) qualifies.
    In practice: ``polars.DataFrame``, ``polars.LazyFrame``, or
    ``ibis.expr.types.Table``.
    """

    @property
    def columns(self) -> list[str]: ...


# ---------------------------------------------------------------------------
# CdmTable
# ---------------------------------------------------------------------------


class CdmTable:
    """A named table in an OMOP CDM, wrapping a concrete data source.

    The class preserves three key pieces of metadata through transformations:

    * ``tbl_name``  — canonical CDM table name (e.g. ``"person"``).
    * ``tbl_source`` — string identifier for the source (e.g. ``"duckdb"``).
    * ``cdm``       — weak back-reference to the parent :class:`CdmReference`.

    Creating derived tables (filter, join, etc.) should use :meth:`_with_data`
    to produce a new CdmTable that inherits the metadata.
    """

    __slots__ = ("_data", "_tbl_name", "_tbl_source", "_cdm_ref")

    def __init__(
        self,
        data: pl.DataFrame | pl.LazyFrame | Any,
        *,
        tbl_name: str,
        tbl_source: str = "local",
        cdm: CdmReference | None = None,
    ) -> None:
        self._data = data
        self._tbl_name = tbl_name
        self._tbl_source = tbl_source
        self._cdm_ref = cdm

    # -- Properties ---------------------------------------------------------

    @property
    def data(self) -> pl.DataFrame | pl.LazyFrame | Any:
        """The underlying data (Polars DF/LF or Ibis table expression)."""
        return self._data

    @property
    def tbl_name(self) -> str:
        """Canonical CDM table name."""
        return self._tbl_name

    @property
    def tbl_source(self) -> str:
        """Source identifier (e.g. ``'local'``, ``'duckdb'``, ``'postgres'``)."""
        return self._tbl_source

    @property
    def cdm(self) -> CdmReference | None:
        """Back-reference to the parent CDM reference, if any."""
        return self._cdm_ref

    @cdm.setter
    def cdm(self, value: CdmReference | None) -> None:
        self._cdm_ref = value

    @property
    def columns(self) -> list[str]:
        """Column names of the underlying data."""
        if isinstance(self._data, pl.DataFrame):
            return self._data.columns
        if isinstance(self._data, pl.LazyFrame):
            return self._data.collect_schema().names()
        # Ibis table
        if hasattr(self._data, "columns"):
            cols = self._data.columns
            return list(cols) if not isinstance(cols, list) else cols
        msg = f"Cannot determine columns for data type {type(self._data).__name__}"
        raise TypeError(msg)

    @property
    def schema(self) -> dict[str, Any]:
        """Column name -> dtype mapping."""
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return dict(self._data.schema)
        if hasattr(self._data, "schema"):
            return dict(self._data.schema())
        msg = f"Cannot determine schema for data type {type(self._data).__name__}"
        raise TypeError(msg)

    # -- Derived table creation ---------------------------------------------

    def _with_data(self, new_data: pl.DataFrame | pl.LazyFrame | Any) -> Self:
        """Create a new CdmTable with the same metadata but different data."""
        new = self.__class__.__new__(self.__class__)
        new._data = new_data
        new._tbl_name = self._tbl_name
        new._tbl_source = self._tbl_source
        new._cdm_ref = self._cdm_ref
        return new

    # -- Polars-compatible transform methods --------------------------------

    def filter(self, *predicates: Any, **named_predicates: Any) -> Self:
        """Filter rows, preserving CdmTable metadata."""
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return self._with_data(self._data.filter(*predicates, **named_predicates))
        # Ibis
        if hasattr(self._data, "filter"):
            return self._with_data(self._data.filter(*predicates))
        msg = f"filter not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    def select(self, *exprs: Any, **named_exprs: Any) -> Self:
        """Select columns, preserving CdmTable metadata."""
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return self._with_data(self._data.select(*exprs, **named_exprs))
        if hasattr(self._data, "select"):
            return self._with_data(self._data.select(*exprs, **named_exprs))
        msg = f"select not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    def rename(self, mapping: dict[str, str]) -> Self:
        """Rename columns, preserving CdmTable metadata."""
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return self._with_data(self._data.rename(mapping))
        if hasattr(self._data, "rename"):
            return self._with_data(self._data.rename(mapping))
        msg = f"rename not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    def join(
        self,
        other: CdmTable | pl.DataFrame | pl.LazyFrame | Any,
        on: str | list[str] | None = None,
        how: str = "inner",
        **kwargs: Any,
    ) -> Self:
        """Join with another table, preserving this table's metadata."""
        other_data = other.data if isinstance(other, CdmTable) else other
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return self._with_data(self._data.join(other_data, on=on, how=how, **kwargs))
        if hasattr(self._data, "join"):
            return self._with_data(self._data.join(other_data, on, how=how, **kwargs))
        msg = f"join not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    def head(self, n: int = 5) -> Self:
        """Return first *n* rows, preserving metadata."""
        if isinstance(self._data, (pl.DataFrame, pl.LazyFrame)):
            return self._with_data(self._data.head(n))
        if hasattr(self._data, "head"):
            return self._with_data(self._data.head(n))
        msg = f"head not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    def collect(self) -> pl.DataFrame:
        """Materialize the data to a Polars DataFrame.

        For lazy sources (LazyFrame, Ibis), this triggers execution.
        Uses PyArrow as the zero-copy interchange format when available.
        """
        if isinstance(self._data, pl.DataFrame):
            return self._data
        if isinstance(self._data, pl.LazyFrame):
            return self._data.collect()
        # Ibis table -> PyArrow -> Polars (zero-copy path, avoids pandas)
        if hasattr(self._data, "to_pyarrow"):
            arrow_table = self._data.to_pyarrow()
            return pl.from_arrow(arrow_table)
        if hasattr(self._data, "execute"):
            # Fallback: ibis returns pandas by default; convert
            import pandas as pd

            result = self._data.execute()
            if isinstance(result, pd.DataFrame):
                return pl.from_pandas(result)
            return result
        msg = f"collect not supported for {type(self._data).__name__}"
        raise TypeError(msg)

    # -- Row count ----------------------------------------------------------

    def count(self) -> int:
        """Return the number of rows.

        For Ibis-backed tables, uses the database's COUNT(*) rather than
        materialising the full table.
        """
        if isinstance(self._data, pl.DataFrame):
            return len(self._data)
        if isinstance(self._data, pl.LazyFrame):
            return self._data.select(pl.len()).collect().item()
        # Ibis table — use native count (single scalar query)
        if hasattr(self._data, "count"):
            try:
                return int(self._data.count().execute())
            except Exception:
                pass
        # Fallback: materialise
        return len(self.collect())

    def __len__(self) -> int:
        return self.count()

    # -- Repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        ncols = len(self.columns)
        source = self._tbl_source
        return f"CdmTable('{self._tbl_name}', source='{source}', columns={ncols})"
