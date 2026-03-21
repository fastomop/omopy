"""Per-table data hash computation for CDM change tracking.

Provides ``compute_data_hash()`` to compute an MD5 hash per CDM table
based on row count and distinct-value count of a key column.
Equivalent to R's ``computeDataHashByTable()``.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

import polars as pl

from omopy.generics.cdm_reference import CdmReference

__all__ = ["compute_data_hash"]


# Table name → key column used for n_distinct count
_TABLE_KEY_COLUMNS: dict[str, str | None] = {
    "person": "person_id",
    "observation_period": "person_id",
    "visit_occurrence": "visit_concept_id",
    "condition_occurrence": "condition_concept_id",
    "drug_exposure": "drug_concept_id",
    "procedure_occurrence": "procedure_concept_id",
    "measurement": "measurement_concept_id",
    "observation": "observation_concept_id",
    "death": "person_id",
    "location": None,
    "care_site": None,
    "provider": None,
    "drug_era": "drug_concept_id",
    "dose_era": None,
    "condition_era": "condition_concept_id",
    "concept": "concept_id",
    "vocabulary": "vocabulary_id",
    "concept_relationship": "concept_id_1",
    "concept_ancestor": "ancestor_concept_id",
    "concept_synonym": "concept_id",
    "drug_strength": "drug_concept_id",
    "cdm_source": None,
}


@dataclass(frozen=True, slots=True)
class TableHash:
    """Hash result for a single CDM table."""

    cdm_name: str | None
    table_name: str
    table_row_count: int
    unique_column: str
    n_unique_values: int
    table_hash: str
    compute_time_secs: float


def compute_data_hash(cdm: CdmReference) -> pl.DataFrame:
    """Compute an MD5 hash per CDM table for change tracking.

    For each standard CDM table, computes the row count and the number
    of distinct values in a key column, then hashes those values to
    produce a fingerprint.  A change in the hash between runs indicates
    the data has changed.

    Parameters
    ----------
    cdm
        A CdmReference.

    Returns
    -------
    pl.DataFrame
        A DataFrame with columns: ``cdm_name``, ``table_name``,
        ``table_row_count``, ``unique_column``, ``n_unique_values``,
        ``table_hash``, ``compute_time_secs``.
    """
    rows: list[dict] = []

    for table_name, key_col in _TABLE_KEY_COLUMNS.items():
        start = time.monotonic()

        if table_name not in cdm:
            rows.append(
                {
                    "cdm_name": cdm.cdm_name or "",
                    "table_name": table_name,
                    "table_row_count": -1,
                    "unique_column": key_col or "NA",
                    "n_unique_values": -1,
                    "table_hash": "Table not found in CDM",
                    "compute_time_secs": time.monotonic() - start,
                }
            )
            continue

        tbl = cdm[table_name]
        data = tbl.data

        try:
            n, n_unique = _count_and_distinct(data, key_col)
        except Exception:
            n, n_unique = -1, -1

        unique_col_str = key_col if key_col is not None else "NA"

        hash_input = f"{table_name}{n}{unique_col_str}{n_unique}"
        table_hash = hashlib.md5(hash_input.encode()).hexdigest()

        rows.append(
            {
                "cdm_name": cdm.cdm_name or "",
                "table_name": table_name,
                "table_row_count": n,
                "unique_column": unique_col_str,
                "n_unique_values": n_unique,
                "table_hash": table_hash,
                "compute_time_secs": round(time.monotonic() - start, 4),
            }
        )

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _count_and_distinct(
    data: object,
    key_col: str | None,
) -> tuple[int, int]:
    """Return (row_count, n_distinct) for a table."""
    import ibis.expr.types as ir

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        n = len(data)
        if key_col is not None and key_col in data.columns:
            n_unique = data[key_col].n_unique()
        else:
            n_unique = -1
        return (n, n_unique)

    if isinstance(data, ir.Table):
        # Ibis table — compute row count and n_distinct in a single query
        if key_col is not None:
            agg = data.aggregate(
                n=data.count(),
                n_unique=data[key_col].nunique(),
            )
        else:
            agg = data.aggregate(n=data.count())

        result = agg.execute()
        if hasattr(result, "iloc"):
            row = result.iloc[0]
            n = int(row["n"])
            n_unique = int(row.get("n_unique", -1)) if key_col is not None else -1
        else:
            n = int(result.get("n", -1))
            n_unique = int(result.get("n_unique", -1)) if key_col is not None else -1
        return (n, n_unique)

    msg = f"Unsupported data type: {type(data)}"
    raise TypeError(msg)
