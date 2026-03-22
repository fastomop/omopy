"""Drug utilisation metric add functions.

Enriches a drug cohort with per-record utilisation metrics such as
number of exposures, days exposed, quantities, and doses by joining
back to ``drug_exposure`` records via the cohort codelist.

All public functions delegate to the shared internal engine
:func:`_add_drug_use_internal`, which builds the ``drugData`` intermediate
table and computes the requested metrics in one pass.

This is the Python equivalent of R's ``addDrugUtilisation()``,
``addNumberExposures()``, ``addNumberEras()``, ``addDaysExposed()``,
``addDaysPrescribed()``, ``addTimeToExposure()``,
``addInitialExposureDuration()``, ``addInitialQuantity()``,
``addCumulativeQuantity()``, ``addInitialDailyDose()``,
``addCumulativeDose()``, and ``addDrugRestart()``.
"""

from __future__ import annotations

from typing import Any, Literal

import ibis
import ibis.expr.types as ir
import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.codelist import Codelist
from omopy.generics.cohort_table import CohortTable
from omopy.connector.db_source import DbSource

__all__ = [
    "add_drug_utilisation",
    "add_number_exposures",
    "add_number_eras",
    "add_days_exposed",
    "add_days_prescribed",
    "add_time_to_exposure",
    "add_initial_exposure_duration",
    "add_initial_quantity",
    "add_cumulative_quantity",
    "add_initial_daily_dose",
    "add_cumulative_dose",
    "add_drug_restart",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def add_drug_utilisation(
    cohort: CohortTable,
    gap_era: int,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    ingredient_concept_id: int | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    number_exposures: bool = True,
    number_eras: bool = True,
    days_exposed: bool = True,
    days_prescribed: bool = True,
    time_to_exposure: bool = True,
    initial_exposure_duration: bool = True,
    initial_quantity: bool = True,
    cumulative_quantity: bool = True,
    initial_daily_dose: bool = True,
    cumulative_dose: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add all drug utilisation metrics to a drug cohort.

    This is the all-in-one convenience wrapper. Each metric can be
    toggled individually via the boolean parameters.

    Parameters
    ----------
    cohort
        A CohortTable (typically from drug cohort generation).
    gap_era
        Maximum gap in days for era collapse (used by ``number_eras``
        and ``days_exposed``).
    concept_set
        Named concept set mapping. If ``None``, inferred from the
        cohort's codelist.
    ingredient_concept_id
        Ingredient concept ID for dose calculations. Required if
        ``initial_daily_dose`` or ``cumulative_dose`` is ``True``.
    index_date
        Column for the start of the observation window.
    censor_date
        Column for the end of the observation window.
    restrict_incident
        If ``True``, only exposures starting within the window are
        counted. If ``False``, any overlapping exposure is included.
    number_exposures, number_eras, days_exposed, days_prescribed,
    time_to_exposure, initial_exposure_duration, initial_quantity,
    cumulative_quantity, initial_daily_dose, cumulative_dose
        Metric flags.
    name
        Unused (reserved for API compatibility).

    Returns
    -------
    CohortTable
        The cohort with metric columns added.
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        ingredient_concept_id=ingredient_concept_id,
        gap_era=gap_era,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        number_exposures=number_exposures,
        number_eras=number_eras,
        days_exposed=days_exposed,
        days_prescribed=days_prescribed,
        time_to_exposure=time_to_exposure,
        initial_exposure_duration=initial_exposure_duration,
        initial_quantity=initial_quantity,
        cumulative_quantity=cumulative_quantity,
        initial_daily_dose=initial_daily_dose,
        cumulative_dose=cumulative_dose,
    )


def add_number_exposures(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add the number of original drug exposure records per era.

    Counts ``drug_exposure`` records within the ``[index_date, censor_date]``
    window for each person and concept set entry.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set. If ``None``, inferred from the cohort codelist.
    index_date
        Start of observation window.
    censor_date
        End of observation window.
    restrict_incident
        Only count exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``number_exposures_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        number_exposures=True,
    )


def add_number_eras(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    gap_era: int = 0,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add the number of exposure eras (after collapse) per person.

    Collapses overlapping/adjacent ``drug_exposure`` records separated by
    at most ``gap_era`` days, then counts the resulting eras.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    gap_era
        Gap in days for era collapse.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``number_eras_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        gap_era=gap_era,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        number_eras=True,
    )


def add_days_exposed(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    gap_era: int = 0,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add total days exposed (era-based) within the observation window.

    Collapses exposures into eras, clips to the window, and sums the
    covered days.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    gap_era
        Gap in days for era collapse.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``days_exposed_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        gap_era=gap_era,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        days_exposed=True,
    )


def add_days_prescribed(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add total days prescribed (raw exposure-based) within the window.

    Sums the clipped duration of each original ``drug_exposure`` record
    without era collapse. Overlapping records contribute independently,
    so the total may exceed calendar days.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``days_prescribed_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        days_prescribed=True,
    )


def add_time_to_exposure(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add days from index date to first drug exposure start.

    Returns ``0`` if the first exposure starts on or before the index
    date. Returns ``None`` if no exposure is found.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``time_to_exposure_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        time_to_exposure=True,
    )


def add_initial_exposure_duration(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add the duration of the first drug exposure record.

    If multiple records share the same earliest start date, the longest
    duration is used.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``initial_exposure_duration_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        initial_exposure_duration=True,
    )


def add_initial_quantity(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add the quantity from the first drug exposure record.

    If multiple records share the same earliest start date, quantities
    are summed.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``initial_quantity_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        initial_quantity=True,
    )


def add_cumulative_quantity(
    cohort: CohortTable,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    *,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add cumulative quantity across all drug exposure records.

    Parameters
    ----------
    cohort
        A CohortTable.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``cumulative_quantity_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        cumulative_quantity=True,
    )


def add_initial_daily_dose(
    cohort: CohortTable,
    ingredient_concept_id: int,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add the daily dose of the first drug exposure record.

    Uses the ``drug_strength`` table and dose calculation formulas to
    compute the daily dose for the initial exposure.

    Parameters
    ----------
    cohort
        A CohortTable.
    ingredient_concept_id
        Ingredient concept ID for dose lookup.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``initial_daily_dose_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        ingredient_concept_id=ingredient_concept_id,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        initial_daily_dose=True,
    )


def add_cumulative_dose(
    cohort: CohortTable,
    ingredient_concept_id: int,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add cumulative dose across all drug exposure records.

    Computes ``sum(daily_dose * exposure_duration)`` across all records
    in the observation window.

    Parameters
    ----------
    cohort
        A CohortTable.
    ingredient_concept_id
        Ingredient concept ID for dose lookup.
    concept_set
        Named concept set.
    index_date, censor_date
        Observation window.
    restrict_incident
        Only consider exposures starting within the window.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``cumulative_dose_{concept_name}`` column(s).
    """
    return _add_drug_use_internal(
        cohort,
        concept_set=concept_set,
        ingredient_concept_id=ingredient_concept_id,
        index_date=index_date,
        censor_date=censor_date,
        restrict_incident=restrict_incident,
        cumulative_dose=True,
    )


def add_drug_restart(
    cohort: CohortTable,
    switch_cohort_table: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    switch_cohort_id: list[int] | None = None,
    follow_up_days: int | float | list[int | float] = float("inf"),
    censor_date: str | None = None,
    incident: bool = True,
    name: str | None = None,
) -> CohortTable:
    """Add drug restart/switch classification after cohort exit.

    Checks whether, after each cohort entry ends, the patient restarts
    the same drug or switches to an alternative drug within a follow-up
    window.

    Parameters
    ----------
    cohort
        A CohortTable.
    switch_cohort_table
        Name of the switch/alternative cohort table in the CDM, or a
        CohortTable directly.
    cdm
        CDM reference. If ``None``, uses ``cohort.cdm``.
    switch_cohort_id
        Which cohort definition IDs in the switch table to consider.
        ``None`` = all.
    follow_up_days
        Follow-up window(s) in days. Can be a single value or a list.
        Use ``float('inf')`` for unlimited.
    censor_date
        Column name for censoring. If ``None``, uses observation period
        end date.
    incident
        If ``True``, switch must start after cohort end (incident).
        If ``False``, switch can have started before cohort end.
    name
        Unused.

    Returns
    -------
    CohortTable
        Cohort with ``drug_restart_{follow_up_days}`` column(s) containing
        values: ``"restart"``, ``"switch"``, ``"restart and switch"``,
        or ``"untreated"``.
    """
    cdm = cdm or cohort.cdm
    if cdm is None:
        msg = "CdmReference is required for add_drug_restart"
        raise ValueError(msg)

    # Normalize follow_up_days to list
    if isinstance(follow_up_days, (int, float)):
        follow_up_days_list = [follow_up_days]
    else:
        follow_up_days_list = list(follow_up_days)

    # Collect cohort
    cohort_df = cohort.collect() if not isinstance(cohort.data, pl.DataFrame) else cohort.data

    # Step 1: Compute censor_days (days of remaining observation after cohort_end_date)
    from omopy.profiles import add_future_observation
    from omopy.generics.cdm_table import CdmTable

    temp_table = CdmTable(
        data=cohort_df.rename({"cohort_end_date": "cohort_start_date_tmp"})
        if False  # keep original
        else cohort_df,
        tbl_name="_temp_restart",
    )
    temp_table.cdm = cdm

    # We need future observation from cohort_end_date
    enriched = add_future_observation(
        temp_table, cdm, index_date="cohort_end_date",
        future_observation_name="_censor_days",
    )
    enriched_df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    # If censor_date column provided, take minimum
    if censor_date is not None and censor_date in enriched_df.columns:
        enriched_df = enriched_df.with_columns(
            _censor_days=pl.min_horizontal(
                pl.col("_censor_days"),
                (pl.col(censor_date) - pl.col("cohort_end_date")).dt.total_days(),
            ),
        )

    # Step 2: Compute restart_days (days to next entry of same drug in same cohort)
    restart_df = (
        enriched_df
        .sort("cohort_definition_id", "subject_id", "cohort_start_date")
        .with_columns(
            _next_start=pl.col("cohort_start_date")
            .shift(-1)
            .over("cohort_definition_id", "subject_id"),
        )
        .with_columns(
            _restart_days=pl.when(pl.col("_next_start").is_not_null())
            .then((pl.col("_next_start") - pl.col("cohort_end_date")).dt.total_days())
            .otherwise(None),
        )
    )

    # Censor restart: if restart_days > censor_days, set to null
    restart_df = restart_df.with_columns(
        _restart_days=pl.when(
            pl.col("_restart_days").is_not_null()
            & pl.col("_censor_days").is_not_null()
            & (pl.col("_restart_days") > pl.col("_censor_days"))
        )
        .then(None)
        .otherwise(pl.col("_restart_days")),
    )

    # Step 3: Compute switch_days
    # Resolve switch cohort
    if isinstance(switch_cohort_table, str):
        switch_ct = cdm[switch_cohort_table]
        if not isinstance(switch_ct, CohortTable):
            msg = f"Table '{switch_cohort_table}' is not a CohortTable"
            raise TypeError(msg)
    else:
        switch_ct = switch_cohort_table

    switch_df = switch_ct.collect() if not isinstance(switch_ct.data, pl.DataFrame) else switch_ct.data

    if switch_cohort_id is not None:
        switch_df = switch_df.filter(pl.col("cohort_definition_id").is_in(switch_cohort_id))

    # Distinct switch entries
    switch_entries = switch_df.select(
        pl.col("subject_id"),
        pl.col("cohort_start_date").alias("switch_start"),
        pl.col("cohort_end_date").alias("switch_end"),
    ).unique()

    # Join with cohort entries
    cohort_for_switch = restart_df.select(
        "cohort_definition_id", "subject_id",
        "cohort_start_date", "cohort_end_date",
        "_censor_days", "_restart_days",
    )

    switched = cohort_for_switch.join(switch_entries, on="subject_id", how="left")

    if incident:
        # Switch must start after cohort end
        switched = switched.with_columns(
            _switch_start_valid=pl.when(
                pl.col("switch_start").is_not_null()
                & (pl.col("switch_start") > pl.col("cohort_end_date"))
                & (pl.col("switch_end") >= pl.col("cohort_end_date"))
            )
            .then(pl.col("switch_start"))
            .otherwise(None),
        )
    else:
        # Switch can overlap — just needs to extend past cohort end
        switched = switched.with_columns(
            _switch_start_valid=pl.when(
                pl.col("switch_start").is_not_null()
                & (pl.col("switch_end") >= pl.col("cohort_end_date"))
            )
            .then(pl.col("switch_start"))
            .otherwise(None),
        )

    # Get earliest valid switch per cohort entry
    switch_agg = (
        switched
        .group_by("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
        .agg(
            pl.col("_switch_start_valid").drop_nulls().min().alias("_earliest_switch"),
            pl.col("_censor_days").first(),
            pl.col("_restart_days").first(),
        )
    )

    switch_agg = switch_agg.with_columns(
        _switch_days=pl.when(pl.col("_earliest_switch").is_not_null())
        .then((pl.col("_earliest_switch") - pl.col("cohort_end_date")).dt.total_days())
        .otherwise(None),
    )

    # Censor switch: if switch_days > censor_days, set to null
    switch_agg = switch_agg.with_columns(
        _switch_days=pl.when(
            pl.col("_switch_days").is_not_null()
            & pl.col("_censor_days").is_not_null()
            & (pl.col("_switch_days") > pl.col("_censor_days"))
        )
        .then(None)
        .otherwise(pl.col("_switch_days")),
    )

    # Step 4: Classify per follow-up window
    result_df = switch_agg
    for fud in follow_up_days_list:
        fud_effective = fud if not (isinstance(fud, float) and fud == float("inf")) else 99999999999999
        col_name = f"drug_restart_{_format_fud(fud)}"

        result_df = result_df.with_columns(
            pl.when(
                pl.col("_restart_days").is_not_null()
                & (pl.col("_restart_days") <= fud_effective)
                & pl.col("_switch_days").is_not_null()
                & (pl.col("_switch_days") <= fud_effective)
            )
            .then(pl.lit("restart and switch"))
            .when(
                pl.col("_restart_days").is_not_null()
                & (pl.col("_restart_days") <= fud_effective)
            )
            .then(pl.lit("restart"))
            .when(
                pl.col("_switch_days").is_not_null()
                & (pl.col("_switch_days") <= fud_effective)
            )
            .then(pl.lit("switch"))
            .otherwise(pl.lit("untreated"))
            .alias(col_name),
        )

    # Join back to original cohort
    join_keys = ["cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"]
    restart_cols = [f"drug_restart_{_format_fud(fud)}" for fud in follow_up_days_list]
    result_slim = result_df.select(*join_keys, *restart_cols)

    final_df = cohort_df.join(result_slim, on=join_keys, how="left")

    # Fill nulls in restart columns
    for col_name in restart_cols:
        final_df = final_df.with_columns(
            pl.col(col_name).fill_null(pl.lit("untreated")),
        )

    return CohortTable(
        data=final_df,
        tbl_name=cohort._tbl_name,
        tbl_source=cohort._tbl_source if hasattr(cohort, "_tbl_source") else "local",
        settings=cohort.settings.clone(),
        attrition=cohort.attrition.clone(),
        cohort_codelist=cohort.cohort_codelist.clone() if len(cohort.cohort_codelist) > 0 else None,
    )


# ---------------------------------------------------------------------------
# Shared internal engine
# ---------------------------------------------------------------------------


def _add_drug_use_internal(
    cohort: CohortTable,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    ingredient_concept_id: int | None = None,
    gap_era: int = 0,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    number_exposures: bool = False,
    number_eras: bool = False,
    days_exposed: bool = False,
    days_prescribed: bool = False,
    time_to_exposure: bool = False,
    initial_exposure_duration: bool = False,
    initial_quantity: bool = False,
    cumulative_quantity: bool = False,
    initial_daily_dose: bool = False,
    cumulative_dose: bool = False,
) -> CohortTable:
    """Shared engine for all drug utilisation add functions.

    Builds a ``drugData`` intermediate table by joining the cohort back
    to ``drug_exposure`` via the concept set, then computes each requested
    metric and joins the results back.
    """
    cdm = cohort.cdm
    if cdm is None:
        msg = "CdmReference is required (cohort.cdm must be set)"
        raise ValueError(msg)

    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "Drug utilisation functions require a database-backed CDM (DbSource)"
        raise TypeError(msg)

    # Resolve concept set from cohort codelist if not provided
    if concept_set is None:
        concept_set = _concept_set_from_codelist(cohort)

    if isinstance(concept_set, dict) and not isinstance(concept_set, Codelist):
        concept_set = Codelist(concept_set)

    if not concept_set:
        msg = (
            "No concept set available. Provide concept_set explicitly or ensure "
            "the cohort has a codelist."
        )
        raise ValueError(msg)

    # Validate dose requirements
    need_dose = initial_daily_dose or cumulative_dose
    if need_dose and ingredient_concept_id is None:
        msg = "ingredient_concept_id is required for dose calculations"
        raise ValueError(msg)

    # Collect cohort data
    cohort_df = cohort.collect() if not isinstance(cohort.data, pl.DataFrame) else cohort.data

    # Build drug data per concept set entry and compute metrics
    con = source.connection
    catalog = source._catalog
    schema = source.cdm_schema

    all_metrics: dict[str, pl.DataFrame] = {}

    for concept_name, concept_ids in concept_set.items():
        if not concept_ids:
            continue

        # Get drug_exposure records matching this concept set
        drug_data = _build_drug_data(
            cohort_df,
            concept_ids=list(concept_ids),
            concept_name=concept_name,
            con=con,
            catalog=catalog,
            schema=schema,
            index_date=index_date,
            censor_date=censor_date,
            restrict_incident=restrict_incident,
        )

        if drug_data is None or len(drug_data) == 0:
            # No matching exposures — will produce NULLs/zeros when joined
            continue

        # Compute each requested metric
        if number_exposures:
            _compute_number_exposures(drug_data, concept_name, all_metrics)

        if number_eras:
            _compute_number_eras(drug_data, concept_name, gap_era, all_metrics)

        if days_exposed:
            _compute_days_exposed(drug_data, concept_name, gap_era, index_date, censor_date, all_metrics)

        if days_prescribed:
            _compute_days_prescribed(drug_data, concept_name, index_date, censor_date, all_metrics)

        if time_to_exposure:
            _compute_time_to_exposure(drug_data, concept_name, index_date, all_metrics)

        if initial_exposure_duration:
            _compute_initial_exposure_duration(drug_data, concept_name, all_metrics)

        if initial_quantity:
            _compute_initial_quantity(drug_data, concept_name, all_metrics)

        if cumulative_quantity:
            _compute_cumulative_quantity(drug_data, concept_name, all_metrics)

        if initial_daily_dose:
            _compute_initial_daily_dose(
                drug_data, concept_name, ingredient_concept_id,
                con, catalog, schema, index_date, censor_date, all_metrics,
            )

        if cumulative_dose:
            _compute_cumulative_dose(
                drug_data, concept_name, ingredient_concept_id,
                con, catalog, schema, index_date, censor_date, all_metrics,
            )

    # Join all metric results back to the cohort
    result_df = cohort_df
    join_keys = ["subject_id", index_date, censor_date]

    for metric_name, metric_df in all_metrics.items():
        result_df = result_df.join(metric_df, on=join_keys, how="left")

    # Fill nulls with defaults
    for col in result_df.columns:
        if col in cohort_df.columns:
            continue
        # Integer metrics: fill null with 0
        if any(col.startswith(prefix) for prefix in [
            "number_exposures_", "number_eras_",
            "days_exposed_", "days_prescribed_",
        ]):
            result_df = result_df.with_columns(
                pl.col(col).fill_null(0),
            )
        # Float metrics with 0 default
        elif any(col.startswith(prefix) for prefix in [
            "initial_quantity_", "cumulative_quantity_",
            "initial_daily_dose_", "cumulative_dose_",
        ]):
            result_df = result_df.with_columns(
                pl.col(col).fill_null(0.0),
            )
        # time_to_exposure and initial_exposure_duration: leave as NULL

    return CohortTable(
        data=result_df,
        tbl_name=cohort._tbl_name,
        tbl_source=cohort._tbl_source if hasattr(cohort, "_tbl_source") else "local",
        settings=cohort.settings.clone(),
        attrition=cohort.attrition.clone(),
        cohort_codelist=cohort.cohort_codelist.clone() if len(cohort.cohort_codelist) > 0 else None,
    )


# ---------------------------------------------------------------------------
# Drug data builder
# ---------------------------------------------------------------------------


def _build_drug_data(
    cohort_df: pl.DataFrame,
    *,
    concept_ids: list[int],
    concept_name: str,
    con: Any,
    catalog: str | None,
    schema: str,
    index_date: str,
    censor_date: str,
    restrict_incident: bool,
) -> pl.DataFrame | None:
    """Build the intermediate drug data table.

    Joins cohort records with ``drug_exposure`` records that match the
    concept set, filtered by the observation window.

    Returns a Polars DataFrame with columns:
    ``subject_id``, ``{index_date}``, ``{censor_date}``,
    ``drug_exposure_start_date``, ``drug_exposure_end_date``,
    ``quantity``, ``drug_concept_id``.
    """
    import pyarrow as pa

    # Get distinct (subject_id, index_date, censor_date) from cohort
    persons = cohort_df.select("subject_id", index_date, censor_date).unique()

    if len(persons) == 0:
        return None

    # Upload concept IDs and cohort persons to DB as temp tables
    arrow_ids = pa.table({
        "concept_id": pa.array(concept_ids, type=pa.int64()),
    })
    tmp_ids = f"__omopy_du_ids_{concept_name}"
    con.con.register(tmp_ids, arrow_ids)

    # Upload cohort persons
    arrow_persons = persons.to_arrow()
    tmp_persons = f"__omopy_du_persons_{concept_name}"
    con.con.register(tmp_persons, arrow_persons)

    try:
        ids_tbl = con.table(tmp_ids)
        persons_tbl = con.table(tmp_persons)

        # Expand descendants
        concept_ancestor = con.table("concept_ancestor", database=(catalog, schema))
        descendants = (
            ids_tbl
            .join(concept_ancestor, ids_tbl.concept_id == concept_ancestor.ancestor_concept_id)
            .select(concept_id=concept_ancestor.descendant_concept_id.cast("int64"))
        )
        all_concept_ids = ids_tbl.select(
            concept_id=ids_tbl.concept_id.cast("int64")
        ).union(descendants).distinct()

        # Get drug_exposure records
        drug_exposure = con.table("drug_exposure", database=(catalog, schema))

        # Join drug_exposure with concept IDs
        drug_records = (
            drug_exposure
            .join(all_concept_ids, drug_exposure.drug_concept_id.cast("int64") == all_concept_ids.concept_id)
            .select(
                subject_id=drug_exposure.person_id,
                drug_exposure_start_date=drug_exposure.drug_exposure_start_date,
                drug_exposure_end_date=ibis.coalesce(
                    drug_exposure.drug_exposure_end_date,
                    drug_exposure.drug_exposure_start_date,
                ),
                quantity=drug_exposure.quantity.cast("float64"),
                drug_concept_id=drug_exposure.drug_concept_id.cast("int64"),
            )
            # Ensure end >= start
            .filter(
                lambda t: t.drug_exposure_end_date >= t.drug_exposure_start_date
            )
        )

        # Join with cohort persons
        joined = drug_records.join(persons_tbl, "subject_id")

        # Apply window filter
        if restrict_incident:
            # Only exposures starting within [index_date, censor_date]
            joined = joined.filter(
                (joined.drug_exposure_start_date >= joined[index_date])
                & (joined.drug_exposure_start_date <= joined[censor_date])
            )
        else:
            # Any overlap with [index_date, censor_date]
            joined = joined.filter(
                (joined.drug_exposure_start_date <= joined[censor_date])
                & (joined.drug_exposure_end_date >= joined[index_date])
            )

        result = joined.select(
            "subject_id", index_date, censor_date,
            "drug_exposure_start_date", "drug_exposure_end_date",
            "quantity", "drug_concept_id",
        )

        # Materialise
        arrow = result.to_pyarrow()
        df = pl.from_arrow(arrow)

        # Ensure consistent types
        cast_map = {
            "subject_id": pl.Int64,
            "drug_concept_id": pl.Int64,
        }
        for col_name, dtype in cast_map.items():
            if col_name in df.columns:
                df = df.with_columns(pl.col(col_name).cast(dtype))

        # Ensure date columns
        for date_col in [index_date, censor_date, "drug_exposure_start_date", "drug_exposure_end_date"]:
            if date_col in df.columns and df[date_col].dtype != pl.Date:
                df = df.with_columns(pl.col(date_col).cast(pl.Date))

        return df if len(df) > 0 else None

    finally:
        for tmp in [tmp_ids, tmp_persons]:
            try:
                con.con.unregister(tmp)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Metric computation functions
# ---------------------------------------------------------------------------


def _compute_number_exposures(
    drug_data: pl.DataFrame,
    concept_name: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Count of drug_exposure records per person within window."""
    join_keys = _get_join_keys(drug_data)
    col = f"number_exposures_{concept_name}"

    result = (
        drug_data
        .group_by(join_keys)
        .agg(pl.len().cast(pl.Int64).alias(col))
    )
    all_metrics[col] = result


def _compute_number_eras(
    drug_data: pl.DataFrame,
    concept_name: str,
    gap_era: int,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Count of eras after collapse per person within window."""
    join_keys = _get_join_keys(drug_data)
    col = f"number_eras_{concept_name}"

    # Erafy the drug_data
    erafied = _erafy_drug_data(drug_data, join_keys, gap_era)

    result = (
        erafied
        .group_by(join_keys)
        .agg(pl.len().cast(pl.Int64).alias(col))
    )
    all_metrics[col] = result


def _compute_days_exposed(
    drug_data: pl.DataFrame,
    concept_name: str,
    gap_era: int,
    index_date: str,
    censor_date: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Total days covered by eras (clipped to window) per person."""
    join_keys = _get_join_keys(drug_data)
    col = f"days_exposed_{concept_name}"

    # Erafy the drug_data
    erafied = _erafy_drug_data(drug_data, join_keys, gap_era)

    # Clip era dates to window
    erafied = erafied.with_columns(
        _era_start=pl.max_horizontal(pl.col("drug_exposure_start_date"), pl.col(index_date)),
        _era_end=pl.min_horizontal(pl.col("drug_exposure_end_date"), pl.col(censor_date)),
    )

    # Duration = end - start + 1
    erafied = erafied.with_columns(
        _era_days=((pl.col("_era_end") - pl.col("_era_start")).dt.total_days() + 1)
        .cast(pl.Int64)
        .clip(lower_bound=0),
    )

    result = (
        erafied
        .group_by(join_keys)
        .agg(pl.col("_era_days").sum().cast(pl.Int64).alias(col))
    )
    all_metrics[col] = result


def _compute_days_prescribed(
    drug_data: pl.DataFrame,
    concept_name: str,
    index_date: str,
    censor_date: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Total days prescribed (raw, no era collapse) per person."""
    join_keys = _get_join_keys(drug_data)
    col = f"days_prescribed_{concept_name}"

    # Clip exposure dates to window
    clipped = drug_data.with_columns(
        _clip_start=pl.max_horizontal(pl.col("drug_exposure_start_date"), pl.col(index_date)),
        _clip_end=pl.min_horizontal(pl.col("drug_exposure_end_date"), pl.col(censor_date)),
    )

    clipped = clipped.with_columns(
        _exp_days=((pl.col("_clip_end") - pl.col("_clip_start")).dt.total_days() + 1)
        .cast(pl.Int64)
        .clip(lower_bound=0),
    )

    result = (
        clipped
        .group_by(join_keys)
        .agg(pl.col("_exp_days").sum().cast(pl.Int64).alias(col))
    )
    all_metrics[col] = result


def _compute_time_to_exposure(
    drug_data: pl.DataFrame,
    concept_name: str,
    index_date: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Days from index date to first exposure start."""
    join_keys = _get_join_keys(drug_data)
    col = f"time_to_exposure_{concept_name}"

    result = (
        drug_data
        .group_by(join_keys)
        .agg(pl.col("drug_exposure_start_date").min().alias("_first_exp_start"))
    )

    result = result.with_columns(
        pl.when(pl.col("_first_exp_start") <= pl.col(index_date))
        .then(0)
        .otherwise(
            (pl.col("_first_exp_start") - pl.col(index_date)).dt.total_days()
        )
        .cast(pl.Int64)
        .alias(col),
    ).drop("_first_exp_start")

    all_metrics[col] = result


def _compute_initial_exposure_duration(
    drug_data: pl.DataFrame,
    concept_name: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Duration of the first (earliest) exposure record."""
    join_keys = _get_join_keys(drug_data)
    col = f"initial_exposure_duration_{concept_name}"

    # Get earliest start per group
    min_starts = (
        drug_data
        .group_by(join_keys)
        .agg(pl.col("drug_exposure_start_date").min().alias("_min_start"))
    )

    # Filter to records matching earliest start
    first_records = drug_data.join(min_starts, on=join_keys).filter(
        pl.col("drug_exposure_start_date") == pl.col("_min_start")
    )

    # Duration = end - start + 1, take max among ties
    first_records = first_records.with_columns(
        _duration=(
            (pl.col("drug_exposure_end_date") - pl.col("drug_exposure_start_date")).dt.total_days() + 1
        ).cast(pl.Int64),
    )

    result = (
        first_records
        .group_by(join_keys)
        .agg(pl.col("_duration").max().alias(col))
    )
    all_metrics[col] = result


def _compute_initial_quantity(
    drug_data: pl.DataFrame,
    concept_name: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Quantity from the first exposure record(s)."""
    join_keys = _get_join_keys(drug_data)
    col = f"initial_quantity_{concept_name}"

    # Get earliest start per group
    min_starts = (
        drug_data
        .group_by(join_keys)
        .agg(pl.col("drug_exposure_start_date").min().alias("_min_start"))
    )

    # Filter to records matching earliest start
    first_records = drug_data.join(min_starts, on=join_keys).filter(
        pl.col("drug_exposure_start_date") == pl.col("_min_start")
    )

    result = (
        first_records
        .group_by(join_keys)
        .agg(pl.col("quantity").drop_nulls().sum().cast(pl.Float64).alias(col))
    )
    all_metrics[col] = result


def _compute_cumulative_quantity(
    drug_data: pl.DataFrame,
    concept_name: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Cumulative quantity across all exposure records."""
    join_keys = _get_join_keys(drug_data)
    col = f"cumulative_quantity_{concept_name}"

    result = (
        drug_data
        .group_by(join_keys)
        .agg(pl.col("quantity").drop_nulls().sum().cast(pl.Float64).alias(col))
    )
    all_metrics[col] = result


def _compute_initial_daily_dose(
    drug_data: pl.DataFrame,
    concept_name: str,
    ingredient_concept_id: int | None,
    con: Any,
    catalog: str | None,
    schema_name: str,
    index_date: str,
    censor_date: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Daily dose from the first exposure record(s)."""
    if ingredient_concept_id is None:
        return

    join_keys = _get_join_keys(drug_data)
    col = f"initial_daily_dose_{concept_name}"

    # Get earliest start per group
    min_starts = (
        drug_data
        .group_by(join_keys)
        .agg(pl.col("drug_exposure_start_date").min().alias("_min_start"))
    )

    # Filter to first records
    first_records = drug_data.join(min_starts, on=join_keys).filter(
        pl.col("drug_exposure_start_date") == pl.col("_min_start")
    ).drop("_min_start")

    # Clip to window
    first_records = first_records.with_columns(
        _clip_start=pl.max_horizontal(pl.col("drug_exposure_start_date"), pl.col(index_date)),
        _clip_end=pl.min_horizontal(pl.col("drug_exposure_end_date"), pl.col(censor_date)),
    )
    first_records = first_records.with_columns(
        _exp_duration=(
            (pl.col("_clip_end") - pl.col("_clip_start")).dt.total_days() + 1
        ).cast(pl.Int64).clip(lower_bound=1),
    )

    # Compute daily dose
    dose_df = _compute_daily_dose_polars(
        first_records, ingredient_concept_id, con, catalog, schema_name,
    )

    if dose_df is None or len(dose_df) == 0:
        return

    # Filter to valid dose records
    dose_df = dose_df.filter(
        pl.col("_daily_dose").is_not_null() & pl.col("_dose_unit").is_not_null()
    )

    if len(dose_df) == 0:
        return

    # Sum daily dose (multiple strength matches) per person
    result = (
        dose_df
        .group_by(join_keys)
        .agg(pl.col("_daily_dose").sum().cast(pl.Float64).alias(col))
    )
    all_metrics[col] = result


def _compute_cumulative_dose(
    drug_data: pl.DataFrame,
    concept_name: str,
    ingredient_concept_id: int | None,
    con: Any,
    catalog: str | None,
    schema_name: str,
    index_date: str,
    censor_date: str,
    all_metrics: dict[str, pl.DataFrame],
) -> None:
    """Cumulative dose = sum(daily_dose * exposure_duration) across all records."""
    if ingredient_concept_id is None:
        return

    join_keys = _get_join_keys(drug_data)
    col = f"cumulative_dose_{concept_name}"

    # Clip all records to window
    clipped = drug_data.with_columns(
        _clip_start=pl.max_horizontal(pl.col("drug_exposure_start_date"), pl.col(index_date)),
        _clip_end=pl.min_horizontal(pl.col("drug_exposure_end_date"), pl.col(censor_date)),
    )
    clipped = clipped.with_columns(
        _exp_duration=(
            (pl.col("_clip_end") - pl.col("_clip_start")).dt.total_days() + 1
        ).cast(pl.Int64).clip(lower_bound=1),
    )

    # Compute daily dose for all records
    dose_df = _compute_daily_dose_polars(
        clipped, ingredient_concept_id, con, catalog, schema_name,
    )

    if dose_df is None or len(dose_df) == 0:
        return

    # Filter to valid dose records
    dose_df = dose_df.filter(
        pl.col("_daily_dose").is_not_null() & pl.col("_dose_unit").is_not_null()
    )

    if len(dose_df) == 0:
        return

    # cumulative_dose = sum(daily_dose * exposure_duration)
    dose_df = dose_df.with_columns(
        _record_dose=(pl.col("_daily_dose") * pl.col("_exp_duration").cast(pl.Float64)),
    )

    result = (
        dose_df
        .group_by(join_keys)
        .agg(pl.col("_record_dose").sum().cast(pl.Float64).alias(col))
    )
    all_metrics[col] = result


# ---------------------------------------------------------------------------
# Dose computation helper
# ---------------------------------------------------------------------------


def _compute_daily_dose_polars(
    df: pl.DataFrame,
    ingredient_concept_id: int,
    con: Any,
    catalog: str | None,
    schema_name: str,
) -> pl.DataFrame | None:
    """Compute daily dose for each drug_exposure record via drug_strength.

    Joins with the drug_strength table, matches patterns, and applies
    dose formulas. Returns the input with ``_daily_dose`` and
    ``_dose_unit`` columns added.
    """
    from omopy.drug._daily_dose import (
        _get_ibis_or_memtable,
        _patterns_to_arrow,
        _join_with_patterns,
        _join_exposure_with_strength,
        _standardise_units,
        _apply_formula,
    )

    if len(df) == 0:
        return None

    # Upload drug data to Ibis
    tbl = ibis.memtable(df.to_arrow())

    # Get drug_strength for the ingredient
    drug_strength = con.table("drug_strength", database=(catalog, schema_name))
    ds = drug_strength.filter(
        drug_strength.ingredient_concept_id.cast("int64") == ingredient_concept_id
    )

    # Build pattern indicators
    ds = ds.mutate(
        _amount_numeric=ibis.cases(
            (ds.amount_value.notnull(), 1),
            else_=0,
        ),
        _numerator_numeric=ibis.cases(
            (ds.numerator_value.notnull(), 1),
            else_=0,
        ),
        _denominator_numeric=ibis.cases(
            (ds.denominator_value.notnull(), 1),
            else_=0,
        ),
    )

    # Upload patterns
    patterns_arrow = _patterns_to_arrow()
    tmp_patterns = "__omopy_du_dose_patterns"
    con.con.register(tmp_patterns, patterns_arrow)

    try:
        patterns_tbl = con.table(tmp_patterns)
        matched = _join_with_patterns(ds, patterns_tbl)

        # Join with exposure records
        result = _join_exposure_with_strength(tbl, matched)

        # Standardise units
        result = _standardise_units(result)

        # Compute days for formula (use _exp_duration if available, else calculate)
        if "_exp_duration" in tbl.columns:
            result = result.mutate(_days_exposed=result._exp_duration.cast("int64"))
        else:
            result = result.mutate(
                _days_exposed=(
                    (result.drug_exposure_end_date - result.drug_exposure_start_date).cast("int64") + 1
                ),
            )

        # Apply formula
        result = _apply_formula(result)

        # Materialise and rename
        arrow = result.to_pyarrow()
        result_df = pl.from_arrow(arrow)

        # Keep original columns plus dose info
        orig_cols = df.columns
        keep_cols = [c for c in orig_cols if c in result_df.columns]
        dose_cols = ["daily_dose", "unit"]
        keep_cols.extend([c for c in dose_cols if c in result_df.columns])

        result_df = result_df.select(keep_cols)

        # Rename dose columns with underscore prefix to avoid conflicts
        if "daily_dose" in result_df.columns:
            result_df = result_df.rename({"daily_dose": "_daily_dose"})
        if "unit" in result_df.columns:
            result_df = result_df.rename({"unit": "_dose_unit"})

        return result_df

    finally:
        try:
            con.con.unregister(tmp_patterns)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Era collapse helper (Polars-based, for drug_data)
# ---------------------------------------------------------------------------


def _erafy_drug_data(
    drug_data: pl.DataFrame,
    join_keys: list[str],
    gap_era: int,
) -> pl.DataFrame:
    """Collapse drug_exposure records into eras within each group.

    Groups by ``join_keys`` and collapses overlapping/adjacent exposure
    records separated by at most ``gap_era`` days.
    """
    if len(drug_data) == 0:
        return drug_data

    df = drug_data.sort(*join_keys, "drug_exposure_start_date")

    # Extended end = end + gap_era days
    df = df.with_columns(
        _ext_end=(pl.col("drug_exposure_end_date") + pl.duration(days=gap_era)),
    )

    # Running max of extended end within group
    df = df.with_columns(
        _cum_max=pl.col("_ext_end")
        .cum_max()
        .over(join_keys),
    )

    # Lag cum_max to get prev max
    df = df.with_columns(
        _prev_max=pl.col("_cum_max")
        .shift(1)
        .over(join_keys),
    )

    # New island flag
    df = df.with_columns(
        _is_new=pl.when(pl.col("_prev_max").is_null())
        .then(1)
        .when(pl.col("drug_exposure_start_date") > pl.col("_prev_max"))
        .then(1)
        .otherwise(0),
    )

    # Island ID = cumulative sum of is_new
    df = df.with_columns(
        _island=pl.col("_is_new")
        .cum_sum()
        .over(join_keys),
    )

    # Aggregate per island
    result = (
        df
        .group_by([*join_keys, "_island"])
        .agg(
            pl.col("drug_exposure_start_date").min(),
            pl.col("drug_exposure_end_date").max(),
        )
        .drop("_island")
        .sort(*join_keys, "drug_exposure_start_date")
    )

    return result


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _concept_set_from_codelist(cohort: CohortTable) -> Codelist | None:
    """Extract a Codelist from the cohort's cohort_codelist metadata."""
    cl = cohort.cohort_codelist
    if cl is None or len(cl) == 0:
        return None

    # Group by codelist_name
    result: dict[str, list[int]] = {}
    for row in cl.to_dicts():
        name = row.get("codelist_name", "")
        cid = row.get("concept_id")
        if cid is not None:
            result.setdefault(name, []).append(int(cid))

    return Codelist(result) if result else None


def _get_join_keys(drug_data: pl.DataFrame) -> list[str]:
    """Get the join keys for aggregating drug data back to cohort."""
    # Standard keys: subject_id + index_date + censor_date
    # Detect which date columns are present (besides drug_ columns)
    keys = ["subject_id"]
    for col in drug_data.columns:
        if col in ("cohort_start_date", "cohort_end_date") or (
            col.startswith("cohort_") and col.endswith("_date")
        ):
            keys.append(col)
    # If no cohort date columns found, use all non-drug date columns
    if len(keys) == 1:
        for col in drug_data.columns:
            if col not in (
                "subject_id", "drug_exposure_start_date",
                "drug_exposure_end_date", "quantity", "drug_concept_id",
            ) and not col.startswith("_"):
                keys.append(col)
    return keys


def _format_fud(fud: int | float) -> str:
    """Format follow_up_days for column naming."""
    if isinstance(fud, float) and fud == float("inf"):
        return "inf"
    return str(int(fud))
