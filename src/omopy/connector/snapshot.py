"""CDM snapshot — metadata extraction.

Provides ``snapshot()`` to extract summary metadata from a CDM,
including person count, observation period range, vocabulary version,
and CDM source information.  Equivalent to R's ``snapshot()``.
"""

from __future__ import annotations

import datetime

import polars as pl
from pydantic import BaseModel, ConfigDict

from omopy.generics.cdm_reference import CdmReference

__all__ = ["CdmSnapshot", "snapshot"]


class CdmSnapshot(BaseModel):
    """Immutable container for CDM snapshot metadata."""

    model_config = ConfigDict(frozen=True)

    cdm_name: str | None
    cdm_source_name: str
    cdm_description: str
    cdm_documentation_reference: str
    cdm_version: str
    cdm_holder: str
    cdm_release_date: str
    vocabulary_version: str
    person_count: int
    observation_period_count: int
    earliest_observation_period_start_date: datetime.date | None
    latest_observation_period_end_date: datetime.date | None
    snapshot_date: str

    def to_dict(self) -> dict[str, str]:
        """Return all fields as a ``{name: str_value}`` dict."""
        return {
            "cdm_name": str(self.cdm_name or ""),
            "cdm_source_name": self.cdm_source_name,
            "cdm_description": self.cdm_description,
            "cdm_documentation_reference": self.cdm_documentation_reference,
            "cdm_version": self.cdm_version,
            "cdm_holder": self.cdm_holder,
            "cdm_release_date": self.cdm_release_date,
            "vocabulary_version": self.vocabulary_version,
            "person_count": str(self.person_count),
            "observation_period_count": str(self.observation_period_count),
            "earliest_observation_period_start_date": str(
                self.earliest_observation_period_start_date or ""
            ),
            "latest_observation_period_end_date": str(
                self.latest_observation_period_end_date or ""
            ),
            "snapshot_date": self.snapshot_date,
        }

    def to_polars(self) -> pl.DataFrame:
        """Return snapshot as a single-row Polars DataFrame (all string cols)."""
        return pl.DataFrame([self.to_dict()])


def snapshot(cdm: CdmReference) -> CdmSnapshot:
    """Extract summary metadata from a CDM.

    Queries the ``person``, ``observation_period``, ``cdm_source``, and
    ``vocabulary`` tables to produce a concise summary of the CDM
    contents and provenance.

    Parameters
    ----------
    cdm
        A CdmReference (database-backed or local).

    Returns
    -------
    CdmSnapshot
        A frozen dataclass with all metadata fields.

    Raises
    ------
    KeyError
        If required tables (``person``, ``observation_period``) are
        missing from the CDM.
    """
    for required in ("person", "observation_period"):
        if required not in cdm:
            msg = f"CDM is missing required table '{required}' for snapshot"
            raise KeyError(msg)

    # -- Person count --
    person_count = _table_count(cdm, "person")

    # -- Observation period count + date range --
    observation_period_count = _table_count(cdm, "observation_period")
    obs_range = _observation_period_range(cdm)

    # -- Vocabulary version --
    vocab_version = _vocabulary_version(cdm)

    # -- CDM source info --
    src_info = _cdm_source_info(cdm, vocab_version)

    snapshot_date = datetime.date.today().isoformat()

    return CdmSnapshot(
        cdm_name=cdm.cdm_name,
        cdm_source_name=src_info.get("cdm_source_name", ""),
        cdm_description=src_info.get("source_description", ""),
        cdm_documentation_reference=src_info.get("source_documentation_reference", ""),
        cdm_version=src_info.get("cdm_version", str(cdm.cdm_version)),
        cdm_holder=src_info.get("cdm_holder", ""),
        cdm_release_date=src_info.get("cdm_release_date", ""),
        vocabulary_version=vocab_version,
        person_count=person_count,
        observation_period_count=observation_period_count,
        earliest_observation_period_start_date=obs_range[0],
        latest_observation_period_end_date=obs_range[1],
        snapshot_date=snapshot_date,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _table_count(cdm: CdmReference, table_name: str) -> int:
    """Return row count for a CDM table."""
    tbl = cdm[table_name]
    data = tbl.data
    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        return len(data)
    # Ibis expression
    count_expr = data.count()
    result = count_expr.execute()
    if hasattr(result, "item"):
        return int(result.item())
    return int(result)


def _observation_period_range(
    cdm: CdmReference,
) -> tuple[datetime.date | None, datetime.date | None]:
    """Return (earliest_start, latest_end) from observation_period."""
    tbl = cdm["observation_period"]
    data = tbl.data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        if len(data) == 0:
            return (None, None)
        earliest = data["observation_period_start_date"].min()
        latest = data["observation_period_end_date"].max()
        return (earliest, latest)

    # Ibis
    agg = data.aggregate(
        earliest=data.observation_period_start_date.min(),
        latest=data.observation_period_end_date.max(),
    )
    row = agg.execute()
    if hasattr(row, "iloc"):
        # Pandas DataFrame from .execute()
        if len(row) == 0:
            return (None, None)
        earliest = row.iloc[0]["earliest"]
        latest = row.iloc[0]["latest"]
    else:
        earliest = row.get("earliest")
        latest = row.get("latest")

    # Convert numpy/pandas dates to Python dates
    if earliest is not None and hasattr(earliest, "date"):
        earliest = earliest.date() if callable(earliest.date) else earliest
    if latest is not None and hasattr(latest, "date"):
        latest = latest.date() if callable(latest.date) else latest
    # Handle pandas Timestamp
    if earliest is not None and not isinstance(earliest, datetime.date):
        try:
            earliest = datetime.date.fromisoformat(str(earliest)[:10])
        except ValueError, TypeError:
            earliest = None
    if latest is not None and not isinstance(latest, datetime.date):
        try:
            latest = datetime.date.fromisoformat(str(latest)[:10])
        except ValueError, TypeError:
            latest = None

    return (earliest, latest)


def _vocabulary_version(cdm: CdmReference) -> str:
    """Extract vocabulary version from the vocabulary table."""
    if "vocabulary" not in cdm:
        return ""

    data = cdm["vocabulary"].data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        filtered = data.filter(pl.col("vocabulary_id") == "None")
        if len(filtered) == 0:
            return ""
        val = filtered["vocabulary_version"][0]
        return str(val) if val is not None else ""

    # Ibis
    filtered = data.filter(data.vocabulary_id == "None")
    result = filtered.select("vocabulary_version").execute()
    if hasattr(result, "iloc"):
        if len(result) == 0:
            return ""
        val = result.iloc[0]["vocabulary_version"]
        return str(val) if val is not None else ""
    return ""


def _cdm_source_info(cdm: CdmReference, vocab_version: str) -> dict[str, str]:
    """Extract CDM source metadata from the cdm_source table."""
    if "cdm_source" not in cdm:
        return {
            "vocabulary_version": vocab_version,
            "cdm_source_name": "",
            "cdm_holder": "",
            "cdm_release_date": "",
            "cdm_version": str(cdm.cdm_version),
            "source_description": "",
            "source_documentation_reference": "",
        }

    data = cdm["cdm_source"].data

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        if len(data) == 0:
            return {
                "vocabulary_version": vocab_version,
                "cdm_source_name": "",
                "cdm_holder": "",
                "cdm_release_date": "",
                "cdm_version": str(cdm.cdm_version),
                "source_description": "",
                "source_documentation_reference": "",
            }
        row = data.row(0, named=True)
    else:
        result = data.execute()
        if hasattr(result, "iloc"):
            if len(result) == 0:
                return {
                    "vocabulary_version": vocab_version,
                    "cdm_source_name": "",
                    "cdm_holder": "",
                    "cdm_release_date": "",
                    "cdm_version": str(cdm.cdm_version),
                    "source_description": "",
                    "source_documentation_reference": "",
                }
            row = result.iloc[0].to_dict()
        else:
            row = {}

    # Normalise column names to lowercase
    row = {k.lower(): v for k, v in row.items()}

    def _str(key: str) -> str:
        val = row.get(key)
        if val is None:
            return ""
        return str(val)

    return {
        "cdm_source_name": _str("cdm_source_name"),
        "cdm_holder": _str("cdm_holder"),
        "cdm_release_date": _str("cdm_release_date"),
        "cdm_version": _str("cdm_version") or str(cdm.cdm_version),
        "source_description": _str("source_description"),
        "source_documentation_reference": _str("source_documentation_reference"),
    }
