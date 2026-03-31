"""Cohort requirement filters for drug utilisation.

Applies inclusion criteria to drug cohorts, recording attrition at each step.

This is the Python equivalent of R's ``requireIsFirstDrugEntry()``,
``requirePriorDrugWashout()``, ``requireObservationBeforeDrug()``,
and ``requireDrugInDateRange()``.
"""

from __future__ import annotations

import datetime

import polars as pl

from omopy.generics.cohort_table import CohortTable

__all__ = [
    "require_drug_in_date_range",
    "require_is_first_drug_entry",
    "require_observation_before_drug",
    "require_prior_drug_washout",
]


def require_is_first_drug_entry(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    name: str | None = None,
) -> CohortTable:
    """Keep only the first cohort entry per subject per cohort definition.

    Parameters
    ----------
    cohort
        A CohortTable (typically from drug cohort generation).
    cohort_id
        If provided, only apply to these cohort definition IDs.
    name
        Name for the resulting cohort table.

    Returns
    -------
    CohortTable
        Filtered cohort with only first entries.
    """
    df = _collect(cohort)
    prev_counts = _count_by_cohort(df)

    ids = cohort_id or df["cohort_definition_id"].unique().sort().to_list()

    parts = []
    for cid in ids:
        subset = df.filter(pl.col("cohort_definition_id") == cid)
        first_only = (
            subset.sort("cohort_start_date")
            .group_by("cohort_definition_id", "subject_id")
            .first()
        )
        parts.append(first_only)

    # Keep unmodified cohorts
    rest = df.filter(~pl.col("cohort_definition_id").is_in(ids))
    if len(rest) > 0:
        parts.append(rest)

    result = pl.concat(parts) if parts else df.clear()
    result = result.sort("cohort_definition_id", "subject_id", "cohort_start_date")

    return _build_result(
        cohort,
        result,
        prev_counts,
        ids,
        "Restricted to first drug entry",
        name,
    )


def require_prior_drug_washout(
    cohort: CohortTable,
    days: int | float,
    *,
    cohort_id: list[int] | None = None,
    name: str | None = None,
) -> CohortTable:
    """Require a minimum washout period before each drug entry.

    Drops records where fewer than ``days`` have passed since the previous
    cohort entry ended. If ``days`` is ``inf``, delegates to
    :func:`require_is_first_drug_entry`.

    Parameters
    ----------
    cohort
        A CohortTable.
    days
        Minimum days between prior entry end and current entry start.
        Use ``float('inf')`` for first-entry-only.
    cohort_id
        If provided, only apply to these cohort definition IDs.
    name
        Name for the resulting cohort table.

    Returns
    -------
    CohortTable
        Filtered cohort.
    """
    if days == float("inf"):
        return require_is_first_drug_entry(cohort, cohort_id=cohort_id, name=name)

    df = _collect(cohort)
    prev_counts = _count_by_cohort(df)
    ids = cohort_id or df["cohort_definition_id"].unique().sort().to_list()

    parts = []
    for cid in ids:
        subset = df.filter(pl.col("cohort_definition_id") == cid).sort(
            "cohort_start_date"
        )

        # Compute lag of cohort_end_date within each subject
        subset = subset.with_columns(
            _prev_end=pl.col("cohort_end_date").shift(1).over("subject_id"),
        )

        # Keep rows where there is no prior entry or gap >= days
        subset = subset.filter(
            pl.col("_prev_end").is_null()
            | (
                (pl.col("cohort_start_date") - pl.col("_prev_end")).dt.total_days()
                >= days
            )
        ).drop("_prev_end")

        parts.append(subset)

    rest = df.filter(~pl.col("cohort_definition_id").is_in(ids))
    if len(rest) > 0:
        parts.append(rest)

    result = pl.concat(parts) if parts else df.clear()
    result = result.sort("cohort_definition_id", "subject_id", "cohort_start_date")

    return _build_result(
        cohort,
        result,
        prev_counts,
        ids,
        f"Prior drug washout >= {days} days",
        name,
    )


def require_observation_before_drug(
    cohort: CohortTable,
    days: int,
    *,
    cdm: object | None = None,
    cohort_id: list[int] | None = None,
    name: str | None = None,
) -> CohortTable:
    """Require minimum prior observation before drug entry.

    Drops records with fewer than ``days`` of observation before the
    cohort start date.

    Parameters
    ----------
    cohort
        A CohortTable.
    days
        Minimum days of prior observation required.
    cdm
        Optional CdmReference (used to access observation_period if cohort
        doesn't already have prior_observation). If not provided, uses
        ``cohort.cdm``.
    cohort_id
        If provided, only apply to these cohort definition IDs.
    name
        Name for the resulting cohort table.

    Returns
    -------
    CohortTable
        Filtered cohort.
    """
    df = _collect(cohort)
    prev_counts = _count_by_cohort(df)
    ids = cohort_id or df["cohort_definition_id"].unique().sort().to_list()

    # Get observation periods to compute prior observation
    ref_cdm = cdm or (cohort.cdm if hasattr(cohort, "cdm") else None)
    if ref_cdm is None:
        msg = "CdmReference is required for require_observation_before_drug"
        raise ValueError(msg)

    from omopy.generics.cdm_table import CdmTable
    from omopy.profiles import add_prior_observation

    # Add prior observation column
    temp_table = CdmTable(data=df, tbl_name="_temp")
    temp_table.cdm = ref_cdm
    enriched_table = add_prior_observation(temp_table, ref_cdm)
    enriched = (
        enriched_table.collect()
        if not isinstance(enriched_table.data, pl.DataFrame)
        else enriched_table.data
    )

    # Filter
    to_filter = enriched.filter(pl.col("cohort_definition_id").is_in(ids))
    filtered = to_filter.filter(pl.col("prior_observation") >= days).drop(
        "prior_observation"
    )

    rest = enriched.filter(~pl.col("cohort_definition_id").is_in(ids))
    if "prior_observation" in rest.columns:
        rest = rest.drop("prior_observation")

    parts = [filtered]
    if len(rest) > 0:
        parts.append(rest)

    result = pl.concat(parts) if parts else df.clear()
    result = result.sort("cohort_definition_id", "subject_id", "cohort_start_date")

    return _build_result(
        cohort,
        result,
        prev_counts,
        ids,
        f"Prior observation >= {days} days",
        name,
    )


def require_drug_in_date_range(
    cohort: CohortTable,
    date_range: tuple[str | datetime.date | None, str | datetime.date | None],
    *,
    index_date: str = "cohort_start_date",
    cohort_id: list[int] | None = None,
    name: str | None = None,
) -> CohortTable:
    """Require that the drug entry falls within a date range.

    Parameters
    ----------
    cohort
        A CohortTable.
    date_range
        ``(start, end)`` tuple. Either can be ``None`` for no bound.
        Dates can be strings (``"YYYY-MM-DD"``) or ``datetime.date``.
    index_date
        Column to check against the range. Default is ``"cohort_start_date"``.
    cohort_id
        If provided, only apply to these cohort definition IDs.
    name
        Name for the resulting cohort table.

    Returns
    -------
    CohortTable
        Filtered cohort.
    """
    df = _collect(cohort)
    prev_counts = _count_by_cohort(df)
    ids = cohort_id or df["cohort_definition_id"].unique().sort().to_list()

    start, end = date_range
    if isinstance(start, str):
        start = datetime.date.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.date.fromisoformat(end)

    to_filter = df.filter(pl.col("cohort_definition_id").is_in(ids))
    filtered = to_filter

    if start is not None:
        filtered = filtered.filter(pl.col(index_date) >= start)
    if end is not None:
        filtered = filtered.filter(pl.col(index_date) <= end)

    rest = df.filter(~pl.col("cohort_definition_id").is_in(ids))

    parts = [filtered]
    if len(rest) > 0:
        parts.append(rest)

    result = pl.concat(parts) if parts else df.clear()
    result = result.sort("cohort_definition_id", "subject_id", "cohort_start_date")

    range_str = f"{start or '*'} to {end or '*'}"
    return _build_result(
        cohort,
        result,
        prev_counts,
        ids,
        f"Drug in date range {range_str}",
        name,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _collect(cohort: CohortTable) -> pl.DataFrame:
    """Materialise cohort data to Polars DataFrame."""
    if isinstance(cohort.data, pl.DataFrame):
        return cohort.data
    return cohort.collect()


def _count_by_cohort(df: pl.DataFrame) -> pl.DataFrame:
    """Count records and subjects per cohort_definition_id."""
    return df.group_by("cohort_definition_id").agg(
        pl.len().alias("number_records"),
        pl.col("subject_id").n_unique().alias("number_subjects"),
    )


def _build_result(
    original: CohortTable,
    result_df: pl.DataFrame,
    prev_counts: pl.DataFrame,
    affected_ids: list[int],
    reason: str,
    name: str | None,
) -> CohortTable:
    """Build a new CohortTable with updated attrition."""
    new_counts = _count_by_cohort(result_df)

    # Build attrition rows for the applied filter
    existing_attrition = original.attrition.clone()
    max_reason_id = (
        existing_attrition["reason_id"].max() if len(existing_attrition) > 0 else 0
    )

    new_attrition_rows = []
    for cid in affected_ids:
        prev = prev_counts.filter(pl.col("cohort_definition_id") == cid)
        cur = new_counts.filter(pl.col("cohort_definition_id") == cid)

        prev_nr = prev["number_records"][0] if len(prev) > 0 else 0
        prev_ns = prev["number_subjects"][0] if len(prev) > 0 else 0
        cur_nr = cur["number_records"][0] if len(cur) > 0 else 0
        cur_ns = cur["number_subjects"][0] if len(cur) > 0 else 0

        new_attrition_rows.append(
            {
                "cohort_definition_id": cid,
                "number_records": cur_nr,
                "number_subjects": cur_ns,
                "reason_id": max_reason_id + 1,
                "reason": reason,
                "excluded_records": prev_nr - cur_nr,
                "excluded_subjects": prev_ns - cur_ns,
            }
        )

    if new_attrition_rows:
        new_attrition_df = pl.DataFrame(new_attrition_rows).cast(
            {
                "cohort_definition_id": pl.Int64,
                "number_records": pl.Int64,
                "number_subjects": pl.Int64,
                "reason_id": pl.Int64,
                "excluded_records": pl.Int64,
                "excluded_subjects": pl.Int64,
            }
        )
        attrition = pl.concat([existing_attrition, new_attrition_df])
    else:
        attrition = existing_attrition

    tbl_name = name or original._tbl_name

    return CohortTable(
        data=result_df,
        tbl_name=tbl_name,
        tbl_source=original._tbl_source
        if hasattr(original, "_tbl_source")
        else "local",
        settings=original.settings.clone(),
        attrition=attrition,
        cohort_codelist=original.cohort_codelist.clone()
        if len(original.cohort_codelist) > 0
        else None,
    )
