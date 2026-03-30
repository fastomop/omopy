"""``_init_pregnancies`` — extract pregnancy-related records from the CDM.

Collects HIP, PPS, and ESD concept records from the relevant CDM clinical
tables (condition_occurrence, procedure_occurrence, observation, measurement)
into Polars DataFrames ready for the algorithmic pipeline steps.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from omopy.generics.cdm_reference import CdmReference

from omopy.pregnancy._concepts import (
    ESD_CONCEPT_IDS,
    ESD_CONCEPTS,
    HIP_CONCEPT_CATEGORIES,
    HIP_CONCEPT_IDS,
    HIP_CONCEPTS,
    PPS_CONCEPT_IDS,
    PPS_CONCEPTS,
)

__all__ = ["_init_pregnancies"]

log = logging.getLogger(__name__)

# Domain -> (table_name, concept_id_col, date_col)
_DOMAIN_MAP: list[tuple[str, str, str]] = [
    ("condition_occurrence", "condition_concept_id", "condition_start_date"),
    ("procedure_occurrence", "procedure_concept_id", "procedure_date"),
    ("observation", "observation_concept_id", "observation_date"),
    ("measurement", "measurement_concept_id", "measurement_date"),
]

# measurement table also has a value column useful for ESD
_MEASUREMENT_VALUE_COL = "value_as_number"


def _collect_concept_records(
    cdm: CdmReference,
    concept_ids: frozenset[int],
) -> pl.DataFrame:
    """Extract records from all clinical tables matching *concept_ids*.

    Returns a DataFrame with columns:
        person_id, concept_id, record_date, value_as_number (nullable),
        source_table
    """
    frames: list[pl.DataFrame] = []

    for tbl_name, cid_col, date_col in _DOMAIN_MAP:
        if tbl_name not in cdm:
            log.debug("Table %s not in CDM, skipping.", tbl_name)
            continue

        tbl = cdm[tbl_name]
        df = tbl.collect()

        if cid_col not in df.columns or date_col not in df.columns:
            log.debug(
                "Table %s missing column(s) %s/%s, skipping.",
                tbl_name,
                cid_col,
                date_col,
            )
            continue

        # Filter to matching concept IDs
        concept_id_list = list(concept_ids)
        filtered = df.filter(pl.col(cid_col).is_in(concept_id_list))

        if filtered.height == 0:
            continue

        # Build uniform schema
        cols: dict[str, pl.Expr] = {
            "person_id": pl.col("person_id").cast(pl.Int64),
            "concept_id": pl.col(cid_col).cast(pl.Int64),
            "record_date": pl.col(date_col).cast(pl.Date),
            "source_table": pl.lit(tbl_name),
        }

        # value_as_number only from measurement
        if tbl_name == "measurement" and _MEASUREMENT_VALUE_COL in df.columns:
            cols["value_as_number"] = pl.col(_MEASUREMENT_VALUE_COL).cast(pl.Float64)
        else:
            cols["value_as_number"] = pl.lit(None, dtype=pl.Float64)

        selected = filtered.select(**cols)
        frames.append(selected)

    if not frames:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "concept_id": pl.Int64,
                "record_date": pl.Date,
                "value_as_number": pl.Float64,
                "source_table": pl.Utf8,
            }
        )

    return pl.concat(frames, how="vertical_relaxed")


def _init_pregnancies(
    cdm: CdmReference,
) -> dict[str, Any]:
    """Load pregnancy concept sets, build initial record tables.

    Parameters
    ----------
    cdm
        A CdmReference with clinical tables.

    Returns
    -------
    dict
        ``"hip_records"``  — DataFrame of HIP concept records
        ``"pps_records"``  — DataFrame of PPS concept records
        ``"esd_records"``  — DataFrame of ESD concept records
        ``"person"``       — Person table as DataFrame
        ``"n_persons"``    — Number of distinct persons with any record
    """
    log.info("Initialising pregnancy records from CDM.")

    # Collect records per concept set
    hip_records = _collect_concept_records(cdm, HIP_CONCEPT_IDS)
    pps_records = _collect_concept_records(cdm, PPS_CONCEPT_IDS)
    esd_records = _collect_concept_records(cdm, ESD_CONCEPT_IDS)

    # Add category information to HIP records
    if hip_records.height > 0:
        cat_map = pl.DataFrame(
            {
                "concept_id": list(HIP_CONCEPT_CATEGORIES.keys()),
                "category": list(HIP_CONCEPT_CATEGORIES.values()),
            }
        ).cast({"concept_id": pl.Int64})
        hip_records = hip_records.join(cat_map, on="concept_id", how="left")

        # Also add gest_value from HIP_CONCEPTS
        gv_rows = [
            {"concept_id": cid, "gest_value": info["gest_value"]}
            for cid, info in HIP_CONCEPTS.items()
        ]
        gv_df = pl.DataFrame(gv_rows).cast(
            {
                "concept_id": pl.Int64,
                "gest_value": pl.Int64,
            }
        )
        hip_records = hip_records.join(gv_df, on="concept_id", how="left")
    else:
        hip_records = hip_records.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("category"),
            pl.lit(None, dtype=pl.Int64).alias("gest_value"),
        )

    # Add timing info to PPS records
    if pps_records.height > 0:
        pps_meta_rows = [
            {
                "concept_id": cid,
                "min_month": info["min_month"],
                "max_month": info["max_month"],
            }
            for cid, info in PPS_CONCEPTS.items()
        ]
        pps_meta = pl.DataFrame(pps_meta_rows).cast(
            {
                "concept_id": pl.Int64,
                "min_month": pl.Int64,
                "max_month": pl.Int64,
            }
        )
        pps_records = pps_records.join(pps_meta, on="concept_id", how="left")
    else:
        pps_records = pps_records.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("min_month"),
            pl.lit(None, dtype=pl.Int64).alias("max_month"),
        )

    # Add ESD category info
    if esd_records.height > 0:
        esd_meta_rows = [
            {
                "concept_id": cid,
                "esd_category": info["category"],
                "esd_domain": info["domain"],
            }
            for cid, info in ESD_CONCEPTS.items()
        ]
        esd_meta = pl.DataFrame(esd_meta_rows).cast({"concept_id": pl.Int64})
        esd_records = esd_records.join(esd_meta, on="concept_id", how="left")
    else:
        esd_records = esd_records.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("esd_category"),
            pl.lit(None, dtype=pl.Utf8).alias("esd_domain"),
        )

    # Person table
    person_df: pl.DataFrame
    if "person" in cdm:
        person_df = cdm["person"].collect()
    else:
        person_df = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "year_of_birth": pl.Int32,
                "gender_concept_id": pl.Int32,
            }
        )

    # Count distinct persons across all records
    all_person_ids: set[int] = set()
    for records in (hip_records, pps_records, esd_records):
        if records.height > 0:
            all_person_ids.update(records["person_id"].unique().to_list())

    log.info(
        "Found %d HIP records, %d PPS records, %d ESD records for %d persons.",
        hip_records.height,
        pps_records.height,
        esd_records.height,
        len(all_person_ids),
    )

    return {
        "hip_records": hip_records,
        "pps_records": pps_records,
        "esd_records": esd_records,
        "person": person_df,
        "n_persons": len(all_person_ids),
    }
