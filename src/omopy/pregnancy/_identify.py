"""``identify_pregnancies`` — main orchestrator for the HIPPS pipeline.

Runs the full pipeline:
1. ``_init_pregnancies()`` — extract concept records
2. ``_run_hip()`` — HIP outcome-anchored identification
3. ``_run_pps()`` — PPS gestational-timing identification
4. ``_merge_hipps()`` — merge HIP and PPS episodes
5. ``_run_esd()`` — ESD start-date refinement

Returns a :class:`PregnancyResult` container.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import polars as pl
from pydantic import BaseModel, ConfigDict

from omopy.generics.cdm_reference import CdmReference
from omopy.pregnancy._esd import _run_esd
from omopy.pregnancy._hip import _run_hip
from omopy.pregnancy._init import _init_pregnancies
from omopy.pregnancy._merge import _merge_hipps
from omopy.pregnancy._pps import _run_pps

__all__ = ["PregnancyResult", "identify_pregnancies"]

log = logging.getLogger(__name__)


class PregnancyResult(BaseModel):
    """Container for pregnancy identification results.

    Attributes
    ----------
    episodes
        Final pregnancy episodes (one row per episode) after ESD refinement.
    hip_episodes
        HIP-only episodes before merging.
    pps_episodes
        PPS-only episodes before merging.
    merged_episodes
        Merged HIP+PPS episodes before ESD refinement.
    cdm_name
        Name of the CDM instance.
    n_persons_input
        Number of distinct persons with any pregnancy-related record.
    n_episodes
        Total number of final episodes.
    settings
        Parameters used for the analysis.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    episodes: Any  # pl.DataFrame
    hip_episodes: Any  # pl.DataFrame
    pps_episodes: Any  # pl.DataFrame
    merged_episodes: Any  # pl.DataFrame
    cdm_name: str
    n_persons_input: int
    n_episodes: int
    settings: dict[str, Any]


def identify_pregnancies(
    cdm: CdmReference,
    *,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    age_bounds: tuple[int, int] = (10, 55),
    just_gestation: bool = True,
    min_cell_count: int = 5,
) -> PregnancyResult:
    """Identify pregnancy episodes from an OMOP CDM.

    Main entry point for the HIPPS pregnancy identification algorithm.
    Runs the full pipeline: init → HIP → PPS → merge → ESD.

    Parameters
    ----------
    cdm
        A :class:`CdmReference` with clinical tables.
    start_date
        Restrict to records on or after this date.
    end_date
        Restrict to records on or before this date.
    age_bounds
        ``(min_age, max_age)`` for filtering persons by age at record date.
    just_gestation
        If True, run HIP Pass 2 for gestation-only episodes.
    min_cell_count
        Minimum cell count for suppression.

    Returns
    -------
    PregnancyResult
        Container with episodes, intermediate results, and metadata.
    """
    log.info("Starting pregnancy identification pipeline.")

    settings: dict[str, Any] = {
        "start_date": str(start_date) if start_date else None,
        "end_date": str(end_date) if end_date else None,
        "age_bounds": list(age_bounds),
        "just_gestation": just_gestation,
        "min_cell_count": min_cell_count,
    }

    # Step 1: Initialize — extract records
    init_data = _init_pregnancies(cdm)

    hip_records = init_data["hip_records"]
    pps_records = init_data["pps_records"]
    esd_records = init_data["esd_records"]
    n_persons = init_data["n_persons"]

    # Apply date filters
    if start_date is not None:
        for name in ("hip_records", "pps_records", "esd_records"):
            df = init_data[name]
            if df.height > 0:
                init_data[name] = df.filter(pl.col("record_date") >= start_date)
        hip_records = init_data["hip_records"]
        pps_records = init_data["pps_records"]
        esd_records = init_data["esd_records"]

    if end_date is not None:
        for name in ("hip_records", "pps_records", "esd_records"):
            df = init_data[name]
            if df.height > 0:
                init_data[name] = df.filter(pl.col("record_date") <= end_date)
        hip_records = init_data["hip_records"]
        pps_records = init_data["pps_records"]
        esd_records = init_data["esd_records"]

    # Step 2: HIP
    hip_episodes = _run_hip(hip_records, just_gestation=just_gestation)

    # Step 3: PPS
    pps_episodes = _run_pps(pps_records)

    # Step 4: Merge
    merged_episodes = _merge_hipps(hip_episodes, pps_episodes)

    # Step 5: ESD refinement
    final_episodes = _run_esd(merged_episodes, esd_records)

    n_episodes = final_episodes.height

    log.info(
        "Pipeline complete: %d episodes from %d persons.",
        n_episodes,
        n_persons,
    )

    return PregnancyResult(
        episodes=final_episodes,
        hip_episodes=hip_episodes,
        pps_episodes=pps_episodes,
        merged_episodes=merged_episodes,
        cdm_name=cdm.cdm_name,
        n_persons_input=n_persons,
        n_episodes=n_episodes,
        settings=settings,
    )
