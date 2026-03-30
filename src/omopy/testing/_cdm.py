"""CDM construction from JSON patient data and synthetic mock generation."""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from omopy.generics import CdmReference, CdmSchema, CdmTable, CdmVersion, CohortTable

__all__ = ["patients_cdm", "mock_test_cdm"]


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

_CDM_VERSION_MAP: dict[str, CdmVersion] = {
    "5.3": CdmVersion.V5_3,
    "5.4": CdmVersion.V5_4,
}


def _resolve_version(cdm_version: str) -> CdmVersion:
    try:
        return _CDM_VERSION_MAP[cdm_version]
    except KeyError:
        msg = f"Unsupported CDM version {cdm_version!r}. Supported: {list(_CDM_VERSION_MAP)}"
        raise ValueError(msg) from None


# ---------------------------------------------------------------------------
# Cohort table names (to detect when wrapping as CohortTable)
# ---------------------------------------------------------------------------

_COHORT_TABLES = frozenset({"cohort"})
_COHORT_COLUMNS = {"cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"}


def _is_cohort_like(table_name: str, df: pl.DataFrame) -> bool:
    """Check if a table should be wrapped as a CohortTable."""
    if table_name in _COHORT_TABLES:
        return True
    return _COHORT_COLUMNS.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# patients_cdm
# ---------------------------------------------------------------------------


def patients_cdm(
    json_path: str | Path,
    *,
    cdm_version: str = "5.4",
    cdm_name: str | None = None,
) -> CdmReference:
    """Load patient data from a JSON file into a ``CdmReference``.

    Reads a JSON file with format::

        {
            "_meta": {"test_name": "...", "cdm_version": "5.4"},
            "person": [{"person_id": 1, ...}, ...],
            "observation_period": [...]
        }

    Creates Polars DataFrames for each table and wraps them as
    ``CdmTable`` (or ``CohortTable`` when appropriate).

    Unlike the R equivalent which downloads an empty Eunomia CDM, this
    function creates in-memory tables from the JSON data only. Vocabulary
    tables are *not* included; use ``cdm_from_con`` with a real database
    if vocabulary tables are needed.

    Args:
        json_path: Path to the JSON file.
        cdm_version: CDM version string (``"5.3"`` or ``"5.4"``).
            Overridden by the ``_meta.cdm_version`` field in the JSON
            if present.
        cdm_name: Human-readable name for this CDM. Defaults to the
            JSON file stem or ``_meta.test_name``.

    Returns:
        A ``CdmReference`` backed by in-memory Polars DataFrames.
    """
    p = Path(json_path)
    if not p.exists():
        msg = f"JSON file not found: {p}"
        raise FileNotFoundError(msg)

    raw: dict[str, Any] = json.loads(p.read_text(encoding="utf-8"))

    # Extract metadata
    meta = raw.pop("_meta", {})
    version_str = meta.get("cdm_version", cdm_version)
    version = _resolve_version(version_str)
    name = cdm_name or meta.get("test_name") or p.stem

    # Build tables
    tables: dict[str, CdmTable] = {}
    for table_name, records in raw.items():
        if not isinstance(records, list):
            continue
        if not records:
            # Empty table — create DataFrame with no rows
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(records)

        if _is_cohort_like(table_name, df) and len(df) > 0:
            tables[table_name] = CohortTable(df, tbl_name=table_name)
        else:
            tables[table_name] = CdmTable(df, tbl_name=table_name)

    return CdmReference(tables=tables, cdm_version=version, cdm_name=name)


# ---------------------------------------------------------------------------
# mock_test_cdm
# ---------------------------------------------------------------------------


def mock_test_cdm(
    *,
    seed: int = 42,
    n_persons: int = 5,
    cdm_version: str = "5.4",
    include_conditions: bool = True,
    include_drugs: bool = True,
    include_measurements: bool = False,
) -> CdmReference:
    """Create a small mock CDM with synthetic data for testing.

    Generates realistic-looking synthetic data for ``person``,
    ``observation_period``, and optionally ``condition_occurrence``,
    ``drug_exposure``, and ``measurement`` tables.

    This requires **no** database or file I/O — everything is created
    in-memory as Polars DataFrames.

    Args:
        seed: Random seed for reproducibility.
        n_persons: Number of persons to generate.
        cdm_version: CDM version string (``"5.3"`` or ``"5.4"``).
        include_conditions: Whether to generate ``condition_occurrence``.
        include_drugs: Whether to generate ``drug_exposure``.
        include_measurements: Whether to generate ``measurement``.

    Returns:
        A ``CdmReference`` backed by in-memory Polars DataFrames.
    """
    version = _resolve_version(cdm_version)
    rng = random.Random(seed)

    # -- person table -------------------------------------------------------
    person_ids = list(range(1, n_persons + 1))
    genders = [rng.choice([8507, 8532]) for _ in person_ids]
    years = [rng.randint(1940, 2005) for _ in person_ids]
    months = [rng.randint(1, 12) for _ in person_ids]
    days = [rng.randint(1, 28) for _ in person_ids]

    person_df = pl.DataFrame({
        "person_id": person_ids,
        "gender_concept_id": genders,
        "year_of_birth": years,
        "month_of_birth": months,
        "day_of_birth": days,
        "race_concept_id": [rng.choice([8515, 8516, 8527]) for _ in person_ids],
        "ethnicity_concept_id": [rng.choice([38003563, 38003564]) for _ in person_ids],
    })

    # -- observation_period table -------------------------------------------
    obs_records: list[dict[str, Any]] = []
    for i, pid in enumerate(person_ids):
        birth_year = years[i]
        start = date(max(birth_year + 18, 2000), 1, 1)
        end = date(2024, 12, 31)
        obs_records.append({
            "observation_period_id": pid,
            "person_id": pid,
            "observation_period_start_date": start,
            "observation_period_end_date": end,
            "period_type_concept_id": 44814724,
        })
    obs_df = pl.DataFrame(obs_records)

    tables: dict[str, CdmTable] = {
        "person": CdmTable(person_df, tbl_name="person"),
        "observation_period": CdmTable(obs_df, tbl_name="observation_period"),
    }

    # -- condition_occurrence -----------------------------------------------
    if include_conditions:
        cond_records: list[dict[str, Any]] = []
        cond_id = 1
        condition_concepts = [31967, 313217, 4329847, 255573, 4112343]
        for pid in person_ids:
            n_conds = rng.randint(0, 3)
            obs = obs_records[pid - 1]
            obs_start = obs["observation_period_start_date"]
            obs_end = obs["observation_period_end_date"]
            span = (obs_end - obs_start).days
            for _ in range(n_conds):
                start_offset = rng.randint(0, max(span - 30, 0))
                cond_start = obs_start + timedelta(days=start_offset)
                cond_end = cond_start + timedelta(days=rng.randint(1, 30))
                if cond_end > obs_end:
                    cond_end = obs_end
                cond_records.append({
                    "condition_occurrence_id": cond_id,
                    "person_id": pid,
                    "condition_concept_id": rng.choice(condition_concepts),
                    "condition_start_date": cond_start,
                    "condition_type_concept_id": 32020,
                })
                cond_id += 1
        tables["condition_occurrence"] = CdmTable(
            pl.DataFrame(cond_records) if cond_records else pl.DataFrame(
                schema={
                    "condition_occurrence_id": pl.Int64,
                    "person_id": pl.Int64,
                    "condition_concept_id": pl.Int64,
                    "condition_start_date": pl.Date,
                    "condition_type_concept_id": pl.Int64,
                }
            ),
            tbl_name="condition_occurrence",
        )

    # -- drug_exposure ------------------------------------------------------
    if include_drugs:
        drug_records: list[dict[str, Any]] = []
        drug_id = 1
        drug_concepts = [1127078, 1127433, 1154343, 19078461, 40163924]
        for pid in person_ids:
            n_drugs = rng.randint(0, 3)
            obs = obs_records[pid - 1]
            obs_start = obs["observation_period_start_date"]
            obs_end = obs["observation_period_end_date"]
            span = (obs_end - obs_start).days
            for _ in range(n_drugs):
                start_offset = rng.randint(0, max(span - 90, 0))
                drug_start = obs_start + timedelta(days=start_offset)
                drug_end = drug_start + timedelta(days=rng.randint(7, 90))
                if drug_end > obs_end:
                    drug_end = obs_end
                drug_records.append({
                    "drug_exposure_id": drug_id,
                    "person_id": pid,
                    "drug_concept_id": rng.choice(drug_concepts),
                    "drug_exposure_start_date": drug_start,
                    "drug_exposure_end_date": drug_end,
                    "drug_type_concept_id": 32838,
                })
                drug_id += 1
        tables["drug_exposure"] = CdmTable(
            pl.DataFrame(drug_records) if drug_records else pl.DataFrame(
                schema={
                    "drug_exposure_id": pl.Int64,
                    "person_id": pl.Int64,
                    "drug_concept_id": pl.Int64,
                    "drug_exposure_start_date": pl.Date,
                    "drug_exposure_end_date": pl.Date,
                    "drug_type_concept_id": pl.Int64,
                }
            ),
            tbl_name="drug_exposure",
        )

    # -- measurement --------------------------------------------------------
    if include_measurements:
        meas_records: list[dict[str, Any]] = []
        meas_id = 1
        meas_concepts = [3006322, 3004249, 3013290, 3016723, 3027114]
        for pid in person_ids:
            n_meas = rng.randint(0, 4)
            obs = obs_records[pid - 1]
            obs_start = obs["observation_period_start_date"]
            obs_end = obs["observation_period_end_date"]
            span = (obs_end - obs_start).days
            for _ in range(n_meas):
                meas_offset = rng.randint(0, max(span - 1, 0))
                meas_date = obs_start + timedelta(days=meas_offset)
                meas_records.append({
                    "measurement_id": meas_id,
                    "person_id": pid,
                    "measurement_concept_id": rng.choice(meas_concepts),
                    "measurement_date": meas_date,
                    "measurement_type_concept_id": 32856,
                    "value_as_number": round(rng.uniform(0.5, 300.0), 2),
                })
                meas_id += 1
        tables["measurement"] = CdmTable(
            pl.DataFrame(meas_records) if meas_records else pl.DataFrame(
                schema={
                    "measurement_id": pl.Int64,
                    "person_id": pl.Int64,
                    "measurement_concept_id": pl.Int64,
                    "measurement_date": pl.Date,
                    "measurement_type_concept_id": pl.Int64,
                    "value_as_number": pl.Float64,
                }
            ),
            tbl_name="measurement",
        )

    return CdmReference(
        tables=tables,
        cdm_version=version,
        cdm_name="mock_test",
    )
