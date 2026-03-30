"""Summarise functions for drug utilisation results.

Converts drug cohort enrichments (from the ``add_*`` functions) into
standardised :class:`SummarisedResult` objects suitable for table and
plot rendering.

Implements six summarise functions:

- :func:`summarise_drug_utilisation` — distribution stats for drug use metrics
- :func:`summarise_indication` — count/percentage per indication
- :func:`summarise_treatment` — count/percentage per treatment
- :func:`summarise_drug_restart` — count/percentage per restart category
- :func:`summarise_dose_coverage` — distribution stats for daily dose
- :func:`summarise_proportion_of_patients_covered` — PPC over time

This is the Python equivalent of R's DrugUtilisation summarise family.
"""

from __future__ import annotations

import math
from typing import Any, Literal

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.codelist import Codelist
from omopy.generics.cohort_table import CohortTable
from omopy.generics.summarised_result import SummarisedResult

__all__ = [
    "summarise_drug_utilisation",
    "summarise_indication",
    "summarise_treatment",
    "summarise_drug_restart",
    "summarise_dose_coverage",
    "summarise_proportion_of_patients_covered",
]

# ---------------------------------------------------------------------------
# Package metadata
# ---------------------------------------------------------------------------

_PACKAGE_NAME = "omopy.drug"
_PACKAGE_VERSION = "0.1.0"


# ===================================================================
# summarise_drug_utilisation
# ===================================================================


def summarise_drug_utilisation(
    cohort: CohortTable,
    gap_era: int,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    ingredient_concept_id: int | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str = "cohort_end_date",
    restrict_incident: bool = True,
    strata: list[str | list[str]] | None = None,
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
    estimates: tuple[str, ...] = (
        "min",
        "q25",
        "median",
        "q75",
        "max",
        "mean",
        "sd",
        "count_missing",
        "percentage_missing",
    ),
) -> SummarisedResult:
    """Summarise drug utilisation metrics as a SummarisedResult.

    Calls :func:`add_drug_utilisation` to enrich the cohort with metric
    columns, then aggregates per cohort × strata into distribution
    statistics.

    Parameters
    ----------
    cohort
        A CohortTable (typically from drug cohort generation).
    gap_era
        Maximum gap in days for era collapse.
    concept_set
        Named concept set. If ``None``, inferred from cohort codelist.
    ingredient_concept_id
        Ingredient concept ID for dose calculations.
    index_date, censor_date
        Observation window columns.
    restrict_incident
        Only count exposures starting within the window.
    strata
        Stratification columns.
    number_exposures, number_eras, days_exposed, days_prescribed,
    time_to_exposure, initial_exposure_duration, initial_quantity,
    cumulative_quantity, initial_daily_dose, cumulative_dose
        Which metrics to include.
    estimates
        Statistics to compute per metric.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_drug_utilisation"``.
    """
    from omopy.drug._add_drug_use import add_drug_utilisation

    if strata is None:
        strata = []

    # Enrich the cohort with all requested metrics
    enriched = add_drug_utilisation(
        cohort,
        gap_era,
        concept_set=concept_set,
        ingredient_concept_id=ingredient_concept_id,
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

    # Collect data
    df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    # Identify metric columns (all new columns not in original cohort)
    base_cols = {"cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"}
    metric_cols = [c for c in df.columns if c not in base_cols and not c.startswith("_")]
    # Filter to actual metric columns (number_*, days_*, time_*, initial_*, cumulative_*)
    metric_prefixes = (
        "number_exposures_",
        "number_eras_",
        "days_exposed_",
        "days_prescribed_",
        "time_to_exposure_",
        "initial_exposure_duration_",
        "initial_quantity_",
        "cumulative_quantity_",
        "initial_daily_dose_",
        "cumulative_dose_",
    )
    metric_cols = [c for c in metric_cols if any(c.startswith(p) for p in metric_prefixes)]

    if not metric_cols:
        return _empty_result("summarise_drug_utilisation")

    # Get cohort metadata
    settings = enriched.settings
    id_to_name = dict(
        zip(
            settings["cohort_definition_id"].to_list(),
            settings["cohort_name"].to_list(),
        )
    )
    cdm = enriched.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    # For each metric column, extract the concept_set name
    # Column format: metric_name_conceptsetname
    # We need to split into (metric_display_name, concept_set_name)
    def _parse_metric_col(col: str) -> tuple[str, str]:
        """Parse 'number_exposures_aspirin' → ('number exposures', 'aspirin')."""
        for prefix in metric_prefixes:
            if col.startswith(prefix):
                concept_name = col[len(prefix) :]
                metric_name = prefix.rstrip("_").replace("_", " ")
                return metric_name, concept_name
        return col.replace("_", " "), ""

    # Build result rows
    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for cid, cname in id_to_name.items():
        cohort_df = df.filter(pl.col("cohort_definition_id") == cid)
        strata_groups = _resolve_strata(cohort_df, strata)

        for sname, slevel, sdf in strata_groups:
            # Add count rows
            all_rows.extend(
                _add_count_rows(
                    sdf,
                    cdm_name=cdm_name,
                    result_id=result_id,
                    group_name="cohort_name",
                    group_level=cname,
                    strata_name=sname,
                    strata_level=slevel,
                )
            )

            for col in metric_cols:
                metric_display, concept_name = _parse_metric_col(col)
                additional_name = "concept_set" if concept_name else OVERALL
                additional_level = concept_name if concept_name else OVERALL

                est_rows = _compute_numeric_estimates(
                    sdf[col],
                    metric_display,
                    estimates,
                )
                for row in est_rows:
                    all_rows.append(
                        {
                            "result_id": result_id,
                            "cdm_name": cdm_name,
                            "group_name": "cohort_name",
                            "group_level": cname,
                            "strata_name": sname,
                            "strata_level": slevel,
                            "additional_name": additional_name,
                            "additional_level": additional_level,
                            **row,
                        }
                    )

    if not all_rows:
        return _empty_result("summarise_drug_utilisation")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_drug_utilisation")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_indication
# ===================================================================


def summarise_indication(
    cohort: CohortTable,
    indication_cohort_name: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    indication_cohort_id: list[int] | None = None,
    indication_window: Any = (0, 0),
    unknown_indication_table: str | list[str] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    mutually_exclusive: bool = True,
    strata: list[str | list[str]] | None = None,
) -> SummarisedResult:
    """Summarise indication prevalence across a drug cohort.

    Calls :func:`add_indication` to classify each record's indication,
    then computes count and percentage per indication level per
    cohort × strata × window.

    Parameters
    ----------
    cohort
        A CohortTable.
    indication_cohort_name
        Name of the indication cohort table or a CohortTable.
    cdm
        CDM reference.
    indication_cohort_id
        Which indication cohort IDs to consider.
    indication_window
        Time window(s) relative to index_date.
    unknown_indication_table
        OMOP table name(s) for unknown indication detection.
    index_date, censor_date
        Reference date columns.
    mutually_exclusive
        Collapse to labels (``True``) or keep flags (``False``).
    strata
        Stratification columns.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_indication"``.
    """
    from omopy.drug._add_intersect import add_indication
    from omopy.profiles._windows import Window, validate_windows, window_name

    if strata is None:
        strata = []

    # Always use mutually_exclusive=True for summarise so we get label columns
    enriched = add_indication(
        cohort,
        indication_cohort_name,
        cdm=cdm,
        indication_cohort_id=indication_cohort_id,
        indication_window=indication_window,
        unknown_indication_table=unknown_indication_table,
        index_date=index_date,
        censor_date=censor_date,
        mutually_exclusive=True,
    )

    df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    # Identify indication columns
    windows = validate_windows(indication_window)
    indication_cols = [c for c in df.columns if c.startswith("indication_")]

    return _summarise_categorical_intersect(
        enriched,
        df,
        indication_cols,
        windows,
        strata,
        result_type="summarise_indication",
        window_name_fn=window_name,
        variable_prefix="Indication",
        additional_key="window_name",
    )


# ===================================================================
# summarise_treatment
# ===================================================================


def summarise_treatment(
    cohort: CohortTable,
    treatment_cohort_name: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    treatment_cohort_id: list[int] | None = None,
    window: Any = (0, 0),
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    mutually_exclusive: bool = True,
    strata: list[str | list[str]] | None = None,
) -> SummarisedResult:
    """Summarise treatment prevalence across a drug cohort.

    Calls :func:`add_treatment` to classify each record's treatment,
    then computes count and percentage per treatment level per
    cohort × strata × window.

    Parameters
    ----------
    cohort
        A CohortTable.
    treatment_cohort_name
        Name of the treatment cohort table or a CohortTable.
    cdm
        CDM reference.
    treatment_cohort_id
        Which treatment cohort IDs to consider.
    window
        Time window(s) relative to index_date.
    index_date, censor_date
        Reference date columns.
    mutually_exclusive
        Collapse to labels (``True``) or keep flags (``False``).
    strata
        Stratification columns.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_treatment"``.
    """
    from omopy.drug._add_intersect import add_treatment
    from omopy.profiles._windows import Window, validate_windows, window_name

    if strata is None:
        strata = []

    enriched = add_treatment(
        cohort,
        treatment_cohort_name,
        cdm=cdm,
        treatment_cohort_id=treatment_cohort_id,
        window=window,
        index_date=index_date,
        censor_date=censor_date,
        mutually_exclusive=True,
    )

    df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    windows = validate_windows(window)
    treatment_cols = [c for c in df.columns if c.startswith("treatment_")]

    return _summarise_categorical_intersect(
        enriched,
        df,
        treatment_cols,
        windows,
        strata,
        result_type="summarise_treatment",
        window_name_fn=window_name,
        variable_prefix="Medication",
        additional_key="window_name",
    )


# ===================================================================
# summarise_drug_restart
# ===================================================================


def summarise_drug_restart(
    cohort: CohortTable,
    switch_cohort_table: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    switch_cohort_id: list[int] | None = None,
    follow_up_days: int | float | list[int | float] = float("inf"),
    censor_date: str | None = None,
    incident: bool = True,
    strata: list[str | list[str]] | None = None,
) -> SummarisedResult:
    """Summarise drug restart/switch classification.

    Calls :func:`add_drug_restart` to classify each record, then
    computes count and percentage per category per cohort × strata ×
    follow-up window.

    Parameters
    ----------
    cohort
        A CohortTable.
    switch_cohort_table
        Name of switch cohort table or a CohortTable.
    cdm
        CDM reference.
    switch_cohort_id
        Which switch cohort IDs to consider.
    follow_up_days
        Follow-up window(s) in days.
    censor_date
        Censoring date column.
    incident
        If ``True``, switch must start after cohort end.
    strata
        Stratification columns.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_drug_restart"``.
    """
    from omopy.drug._add_drug_use import add_drug_restart, _format_fud

    if strata is None:
        strata = []

    # Normalize follow_up_days
    if isinstance(follow_up_days, (int, float)):
        fud_list = [follow_up_days]
    else:
        fud_list = list(follow_up_days)

    enriched = add_drug_restart(
        cohort,
        switch_cohort_table,
        cdm=cdm,
        switch_cohort_id=switch_cohort_id,
        follow_up_days=fud_list,
        censor_date=censor_date,
        incident=incident,
    )

    df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    # Identify restart columns
    restart_cols = [f"drug_restart_{_format_fud(fud)}" for fud in fud_list]

    # Get cohort metadata
    settings = enriched.settings
    id_to_name = dict(
        zip(
            settings["cohort_definition_id"].to_list(),
            settings["cohort_name"].to_list(),
        )
    )
    cdm_ref = enriched.cdm
    cdm_name = cdm_ref.cdm_name if cdm_ref else "unknown"

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for cid, cname in id_to_name.items():
        cohort_df = df.filter(pl.col("cohort_definition_id") == cid)
        strata_groups = _resolve_strata(cohort_df, strata)

        for sname, slevel, sdf in strata_groups:
            # Add count rows
            all_rows.extend(
                _add_count_rows(
                    sdf,
                    cdm_name=cdm_name,
                    result_id=result_id,
                    group_name="cohort_name",
                    group_level=cname,
                    strata_name=sname,
                    strata_level=slevel,
                )
            )

            for fud, col in zip(fud_list, restart_cols):
                if col not in sdf.columns:
                    continue

                fud_label = _format_fud(fud)
                variable_name = f"Drug restart in {fud_label} days"
                total = len(sdf)

                # Count per category
                value_counts = sdf[col].value_counts().sort("count", descending=True)
                for vc_row in value_counts.iter_rows(named=True):
                    level = str(vc_row[col])
                    count = vc_row["count"]
                    pct = (count / total * 100.0) if total > 0 else 0.0

                    base = {
                        "result_id": result_id,
                        "cdm_name": cdm_name,
                        "group_name": "cohort_name",
                        "group_level": cname,
                        "strata_name": sname,
                        "strata_level": slevel,
                        "additional_name": "follow_up_days",
                        "additional_level": fud_label,
                    }
                    all_rows.append(
                        {
                            **base,
                            "variable_name": variable_name,
                            "variable_level": level,
                            "estimate_name": "count",
                            "estimate_type": "integer",
                            "estimate_value": str(count),
                        }
                    )
                    all_rows.append(
                        {
                            **base,
                            "variable_name": variable_name,
                            "variable_level": level,
                            "estimate_name": "percentage",
                            "estimate_type": "percentage",
                            "estimate_value": f"{pct:.2f}",
                        }
                    )

    if not all_rows:
        return _empty_result("summarise_drug_restart")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_drug_restart")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_dose_coverage
# ===================================================================


def summarise_dose_coverage(
    cohort: CohortTable,
    ingredient_concept_id: int,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    strata: list[str | list[str]] | None = None,
    estimates: tuple[str, ...] = (
        "min",
        "q25",
        "median",
        "q75",
        "max",
        "mean",
        "sd",
        "count_missing",
        "percentage_missing",
    ),
) -> SummarisedResult:
    """Summarise dose coverage (daily dose distribution and missing rate).

    Calls :func:`add_daily_dose` and summarises the resulting
    ``daily_dose`` column per ingredient.

    Parameters
    ----------
    cohort
        A CohortTable (or CdmTable backed by drug_exposure data).
    ingredient_concept_id
        Ingredient concept ID for dose lookup.
    concept_set
        Named concept set. If ``None``, inferred from cohort codelist.
    strata
        Stratification columns.
    estimates
        Statistics to compute.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_dose_coverage"``.
    """
    from omopy.drug._daily_dose import add_daily_dose

    if strata is None:
        strata = []

    cdm = cohort.cdm
    if cdm is None:
        msg = "CdmReference is required for summarise_dose_coverage"
        raise ValueError(msg)

    cdm_name = cdm.cdm_name if cdm else "unknown"

    # Apply add_daily_dose to the drug exposure records
    # First, we need the actual drug_exposure data with the dose columns
    enriched = add_daily_dose(cohort, cdm, ingredient_concept_id=ingredient_concept_id)
    df = enriched.collect() if not isinstance(enriched.data, pl.DataFrame) else enriched.data

    # Look up ingredient name from concept table
    try:
        concept_tbl = cdm["concept"].collect()
        match = concept_tbl.filter(pl.col("concept_id") == ingredient_concept_id)
        ingredient_name = (
            match["concept_name"][0] if len(match) > 0 else str(ingredient_concept_id)
        )
    except Exception:
        ingredient_name = str(ingredient_concept_id)

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    # Group is ingredient_name
    strata_groups = _resolve_strata(df, strata)

    for sname, slevel, sdf in strata_groups:
        n_total = len(sdf)

        # Daily dose distribution
        if "daily_dose" in sdf.columns:
            dose_series = sdf["daily_dose"]
        else:
            dose_series = pl.Series("daily_dose", [], dtype=pl.Float64)

        est_rows = _compute_numeric_estimates(dose_series, "daily_dose", estimates)
        for row in est_rows:
            all_rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "ingredient_name",
                    "group_level": ingredient_name,
                    "strata_name": sname,
                    "strata_level": slevel,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                    **row,
                }
            )

        # Missing dose count
        if "daily_dose" in sdf.columns:
            n_missing = sdf["daily_dose"].null_count()
        else:
            n_missing = n_total

        pct_missing = (n_missing / n_total * 100.0) if n_total > 0 else 0.0

        base = {
            "result_id": result_id,
            "cdm_name": cdm_name,
            "group_name": "ingredient_name",
            "group_level": ingredient_name,
            "strata_name": sname,
            "strata_level": slevel,
            "additional_name": OVERALL,
            "additional_level": OVERALL,
        }
        all_rows.append(
            {
                **base,
                "variable_name": "Missing dose",
                "variable_level": "",
                "estimate_name": "count",
                "estimate_type": "integer",
                "estimate_value": str(n_missing),
            }
        )
        all_rows.append(
            {
                **base,
                "variable_name": "Missing dose",
                "variable_level": "",
                "estimate_name": "percentage",
                "estimate_type": "percentage",
                "estimate_value": f"{pct_missing:.2f}",
            }
        )

    if not all_rows:
        return _empty_result("summarise_dose_coverage")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_dose_coverage")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_proportion_of_patients_covered
# ===================================================================


def summarise_proportion_of_patients_covered(
    cohort: CohortTable,
    *,
    concept_set: Codelist | dict[str, list[int]] | None = None,
    follow_up_days: int = 365,
    strata: list[str | list[str]] | None = None,
    index_date: str = "cohort_start_date",
) -> SummarisedResult:
    """Summarise proportion of patients covered (PPC) over time.

    For each day from 0 to ``follow_up_days``, computes the proportion
    of patients still covered by a drug exposure (era) on that day.

    Parameters
    ----------
    cohort
        A CohortTable from drug cohort generation.
    concept_set
        Named concept set. If ``None``, inferred from cohort codelist.
    follow_up_days
        Number of days to compute PPC over.
    strata
        Stratification columns.
    index_date
        Column for the start of follow-up.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_proportion_of_patients_covered"``.

    Notes
    -----
    The PPC at day *t* is defined as:

        PPC(t) = n_covered(t) / n_at_risk(t)

    where ``n_at_risk(t)`` is the number of subjects still under
    observation at day *t* and ``n_covered(t)`` is the number still
    receiving the drug.

    95% Wilson confidence intervals are computed using
    :func:`scipy.stats.binom` when scipy is available.
    """
    if strata is None:
        strata = []

    cdm = cohort.cdm
    if cdm is None:
        msg = "CdmReference is required for summarise_proportion_of_patients_covered"
        raise ValueError(msg)

    from omopy.connector.db_source import DbSource

    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "PPC requires a database-backed CDM"
        raise TypeError(msg)

    cdm_name = cdm.cdm_name if cdm else "unknown"

    # Collect cohort data
    df = cohort.collect() if not isinstance(cohort.data, pl.DataFrame) else cohort.data

    settings = cohort.settings
    id_to_name = dict(
        zip(
            settings["cohort_definition_id"].to_list(),
            settings["cohort_name"].to_list(),
        )
    )

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for cid, cname in id_to_name.items():
        cohort_df = df.filter(pl.col("cohort_definition_id") == cid)
        strata_groups = _resolve_strata(cohort_df, strata)

        for sname, slevel, sdf in strata_groups:
            if len(sdf) == 0:
                continue

            # For each subject, determine how long they are covered
            # Use cohort_start_date as drug coverage start, cohort_end_date as end
            # PPC denominator: subjects still observable at day t
            # We need observation_period end to know when subjects leave
            from omopy.profiles import add_future_observation

            temp = CdmTable(data=sdf, tbl_name="_temp_ppc")
            temp.cdm = cdm
            enriched = add_future_observation(
                temp,
                cdm,
                index_date=index_date,
                future_observation_name="_max_followup",
            )
            edf = (
                enriched.collect()
                if not isinstance(enriched.data, pl.DataFrame)
                else enriched.data
            )

            # For each subject, compute:
            # - coverage_days: cohort_end_date - index_date (days covered by drug)
            # - observation_days: _max_followup (total observable days)
            edf = edf.with_columns(
                _coverage_days=(
                    (pl.col("cohort_end_date") - pl.col(index_date)).dt.total_days()
                ).cast(pl.Int64),
            )

            # For PPC, we need subject-level summary (first entry per subject)
            # If multiple entries, take the union of coverage
            # Simplified: use first entry (consistent with R)
            subject_df = edf.sort(index_date).group_by("subject_id").first()

            n_total = len(subject_df)

            for day in range(follow_up_days + 1):
                # n_at_risk: subjects with _max_followup >= day
                at_risk = subject_df.filter(pl.col("_max_followup") >= day)
                n_at_risk = len(at_risk)

                if n_at_risk == 0:
                    break

                # n_covered: subjects with _coverage_days >= day
                n_covered = at_risk.filter(pl.col("_coverage_days") >= day).height

                ppc = n_covered / n_at_risk if n_at_risk > 0 else 0.0

                # Wilson CI
                ppc_lower, ppc_upper = _wilson_ci(n_covered, n_at_risk)

                base = {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "cohort_name",
                    "group_level": cname,
                    "strata_name": sname,
                    "strata_level": slevel,
                    "additional_name": "time",
                    "additional_level": str(day),
                    "variable_name": OVERALL,
                    "variable_level": "",
                }

                all_rows.append(
                    {
                        **base,
                        "estimate_name": "outcome_count",
                        "estimate_type": "integer",
                        "estimate_value": str(n_covered),
                    }
                )
                all_rows.append(
                    {
                        **base,
                        "estimate_name": "denominator_count",
                        "estimate_type": "integer",
                        "estimate_value": str(n_at_risk),
                    }
                )
                all_rows.append(
                    {
                        **base,
                        "estimate_name": "ppc",
                        "estimate_type": "numeric",
                        "estimate_value": f"{ppc:.6f}",
                    }
                )
                all_rows.append(
                    {
                        **base,
                        "estimate_name": "ppc_lower",
                        "estimate_type": "numeric",
                        "estimate_value": f"{ppc_lower:.6f}",
                    }
                )
                all_rows.append(
                    {
                        **base,
                        "estimate_name": "ppc_upper",
                        "estimate_type": "numeric",
                        "estimate_value": f"{ppc_upper:.6f}",
                    }
                )

    if not all_rows:
        return _empty_result("summarise_proportion_of_patients_covered")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_proportion_of_patients_covered")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# Internal helpers
# ===================================================================


def _make_settings(
    result_id: int | list[int],
    result_type: str,
    **extra: str,
) -> pl.DataFrame:
    """Create a settings DataFrame for a SummarisedResult."""
    if isinstance(result_id, int):
        result_id = [result_id]

    data: dict[str, list[Any]] = {
        "result_id": result_id,
        "result_type": [result_type] * len(result_id),
        "package_name": [_PACKAGE_NAME] * len(result_id),
        "package_version": [_PACKAGE_VERSION] * len(result_id),
    }

    for key, value in extra.items():
        data[key] = [value] * len(result_id)

    return pl.DataFrame(data)


def _empty_result(result_type: str) -> SummarisedResult:
    """Create an empty SummarisedResult with the given result_type."""
    data = pl.DataFrame(
        schema={
            "result_id": pl.Int64,
            "cdm_name": pl.Utf8,
            "group_name": pl.Utf8,
            "group_level": pl.Utf8,
            "strata_name": pl.Utf8,
            "strata_level": pl.Utf8,
            "variable_name": pl.Utf8,
            "variable_level": pl.Utf8,
            "estimate_name": pl.Utf8,
            "estimate_type": pl.Utf8,
            "estimate_value": pl.Utf8,
            "additional_name": pl.Utf8,
            "additional_level": pl.Utf8,
        }
    )
    settings_df = _make_settings(1, result_type)
    return SummarisedResult(data, settings=settings_df)


def _resolve_strata(
    df: pl.DataFrame,
    strata: list[str | list[str]],
) -> list[tuple[str, str, pl.DataFrame]]:
    """Generate (strata_name, strata_level, filtered_df) for each stratum.

    Always includes the ``"overall"`` stratum first.
    """
    groups: list[tuple[str, str, pl.DataFrame]] = []
    groups.append((OVERALL, OVERALL, df))

    for s in strata:
        if isinstance(s, str):
            s = [s]

        missing = [c for c in s if c not in df.columns]
        if missing:
            msg = f"Strata columns not found in data: {missing}"
            raise ValueError(msg)

        strata_name = NAME_LEVEL_SEP.join(s)

        for keys, group_df in df.group_by(s):
            if not isinstance(keys, tuple):
                keys = (keys,)
            strata_level = NAME_LEVEL_SEP.join(str(k) for k in keys)
            groups.append((strata_name, strata_level, group_df))

    return groups


def _add_count_rows(
    df: pl.DataFrame,
    *,
    cdm_name: str,
    result_id: int,
    group_name: str,
    group_level: str,
    strata_name: str,
    strata_level: str,
    additional_name: str = OVERALL,
    additional_level: str = OVERALL,
) -> list[dict[str, Any]]:
    """Add 'Number subjects' and 'Number records' rows."""
    n_records = len(df)
    n_subjects = df["subject_id"].n_unique() if "subject_id" in df.columns else n_records

    base = {
        "result_id": result_id,
        "cdm_name": cdm_name,
        "group_name": group_name,
        "group_level": group_level,
        "strata_name": strata_name,
        "strata_level": strata_level,
        "additional_name": additional_name,
        "additional_level": additional_level,
    }

    return [
        {
            **base,
            "variable_name": "Number records",
            "variable_level": "",
            "estimate_name": "count",
            "estimate_type": "integer",
            "estimate_value": str(n_records),
        },
        {
            **base,
            "variable_name": "Number subjects",
            "variable_level": "",
            "estimate_name": "count",
            "estimate_type": "integer",
            "estimate_value": str(n_subjects),
        },
    ]


def _compute_numeric_estimates(
    series: pl.Series,
    variable_name: str,
    estimates: tuple[str, ...],
) -> list[dict[str, str]]:
    """Compute numeric distribution estimates for a series.

    Returns a list of dicts with keys: variable_name, variable_level,
    estimate_name, estimate_type, estimate_value.
    """
    rows: list[dict[str, str]] = []
    non_null = series.drop_nulls()
    n = len(non_null)
    total = len(series)

    for est in estimates:
        if est == "count_missing":
            n_missing = total - n
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "count_missing",
                    "estimate_type": "integer",
                    "estimate_value": str(n_missing),
                }
            )
        elif est == "percentage_missing":
            n_missing = total - n
            pct = (n_missing / total * 100.0) if total > 0 else 0.0
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "percentage_missing",
                    "estimate_type": "percentage",
                    "estimate_value": f"{pct:.2f}",
                }
            )
        elif est == "mean":
            val = non_null.mean() if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "mean",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val:.2f}" if val is not None else "NA",
                }
            )
        elif est == "sd":
            if n > 1:
                val = non_null.std()
                rows.append(
                    {
                        "variable_name": variable_name,
                        "variable_level": "",
                        "estimate_name": "sd",
                        "estimate_type": "numeric",
                        "estimate_value": f"{val:.2f}" if val is not None else "NA",
                    }
                )
            else:
                rows.append(
                    {
                        "variable_name": variable_name,
                        "variable_level": "",
                        "estimate_name": "sd",
                        "estimate_type": "numeric",
                        "estimate_value": "NA",
                    }
                )
        elif est == "median":
            val = non_null.median() if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "median",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val:.2f}" if val is not None else "NA",
                }
            )
        elif est == "q25":
            val = non_null.quantile(0.25, interpolation="nearest") if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "q25",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val:.2f}" if val is not None else "NA",
                }
            )
        elif est == "q75":
            val = non_null.quantile(0.75, interpolation="nearest") if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "q75",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val:.2f}" if val is not None else "NA",
                }
            )
        elif est == "min":
            val = non_null.min() if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "min",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val}" if val is not None else "NA",
                }
            )
        elif est == "max":
            val = non_null.max() if n > 0 else None
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "max",
                    "estimate_type": "numeric",
                    "estimate_value": f"{val}" if val is not None else "NA",
                }
            )
        elif est == "count":
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(n),
                }
            )
        elif est == "percentage":
            pct = (n / total * 100.0) if total > 0 else 0.0
            rows.append(
                {
                    "variable_name": variable_name,
                    "variable_level": "",
                    "estimate_name": "percentage",
                    "estimate_type": "percentage",
                    "estimate_value": f"{pct:.2f}",
                }
            )

    return rows


def _summarise_categorical_intersect(
    enriched: CohortTable,
    df: pl.DataFrame,
    label_cols: list[str],
    windows: list[tuple[float, float]],
    strata: list[str | list[str]],
    *,
    result_type: str,
    window_name_fn: Any,
    variable_prefix: str,
    additional_key: str,
) -> SummarisedResult:
    """Shared summarise engine for indication and treatment.

    Takes the enriched cohort with label columns (one per window) and
    computes count/percentage per level per cohort × strata.
    """
    settings = enriched.settings
    id_to_name = dict(
        zip(
            settings["cohort_definition_id"].to_list(),
            settings["cohort_name"].to_list(),
        )
    )
    cdm = enriched.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for cid, cname in id_to_name.items():
        cohort_df = df.filter(pl.col("cohort_definition_id") == cid)
        strata_groups = _resolve_strata(cohort_df, strata)

        for sname, slevel, sdf in strata_groups:
            total = len(sdf)

            # Add count rows
            all_rows.extend(
                _add_count_rows(
                    sdf,
                    cdm_name=cdm_name,
                    result_id=result_id,
                    group_name="cohort_name",
                    group_level=cname,
                    strata_name=sname,
                    strata_level=slevel,
                )
            )

            for w in windows:
                wn = window_name_fn(w)
                variable_name = f"{variable_prefix} {wn}"

                # Find the matching label column
                col = None
                for c in label_cols:
                    if c.endswith(wn) or c.endswith(f"_{wn}"):
                        col = c
                        break

                if col is None or col not in sdf.columns:
                    continue

                # Count per level
                value_counts = sdf[col].value_counts().sort("count", descending=True)
                for vc_row in value_counts.iter_rows(named=True):
                    level = str(vc_row[col])
                    count = vc_row["count"]
                    pct = (count / total * 100.0) if total > 0 else 0.0

                    base = {
                        "result_id": result_id,
                        "cdm_name": cdm_name,
                        "group_name": "cohort_name",
                        "group_level": cname,
                        "strata_name": sname,
                        "strata_level": slevel,
                        "additional_name": additional_key,
                        "additional_level": wn,
                    }
                    all_rows.append(
                        {
                            **base,
                            "variable_name": variable_name,
                            "variable_level": level,
                            "estimate_name": "count",
                            "estimate_type": "integer",
                            "estimate_value": str(count),
                        }
                    )
                    all_rows.append(
                        {
                            **base,
                            "variable_name": variable_name,
                            "variable_level": level,
                            "estimate_name": "percentage",
                            "estimate_type": "percentage",
                            "estimate_value": f"{pct:.2f}",
                        }
                    )

    if not all_rows:
        return _empty_result(result_type)

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, result_type)
    return SummarisedResult(data, settings=settings_df)


def _wilson_ci(
    successes: int,
    total: int,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """Compute Wilson score confidence interval for a proportion.

    Falls back to the normal approximation if scipy is unavailable.
    """
    if total == 0:
        return (0.0, 0.0)

    p_hat = successes / total

    try:
        from scipy.stats import norm as _norm

        z = _norm.ppf(1 - (1 - confidence) / 2)
    except ImportError:
        # Fallback: z ≈ 1.96 for 95%
        z = 1.96

    denom = 1 + z * z / total
    centre = p_hat + z * z / (2 * total)
    spread = z * math.sqrt(p_hat * (1 - p_hat) / total + z * z / (4 * total * total))

    lower = (centre - spread) / denom
    upper = (centre + spread) / denom

    return (max(0.0, lower), min(1.0, upper))
