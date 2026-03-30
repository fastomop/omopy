"""Summarise treatment pathways into SummarisedResult objects.

Converts patient-level ``PathwayResult`` into aggregate treatment pathway
summaries and event duration statistics.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics import SummarisedResult
from omopy.generics._types import NAME_LEVEL_SEP, OVERALL

from omopy.treatment._pathway import PathwayResult

_PACKAGE_NAME = "omopy.treatment"
_PACKAGE_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(
    result_id: int | list[int],
    result_type: str,
    **extra: str,
) -> pl.DataFrame:
    """Build a settings DataFrame."""
    if isinstance(result_id, int):
        result_id = [result_id]
    n = len(result_id)
    data: dict[str, Any] = {
        "result_id": result_id,
        "result_type": [result_type] * n,
        "package_name": [_PACKAGE_NAME] * n,
        "package_version": [_PACKAGE_VERSION] * n,
    }
    for key, value in extra.items():
        data[key] = [value] * n
    return pl.DataFrame(data)


def _resolve_strata(
    df: pl.DataFrame,
    strata: list[str | list[str]] | None,
) -> list[tuple[str, str, pl.DataFrame]]:
    """Generate (strata_name, strata_level, filtered_df) tuples."""
    groups: list[tuple[str, str, pl.DataFrame]] = [(OVERALL, OVERALL, df)]
    if not strata:
        return groups
    for s in strata:
        if isinstance(s, str):
            s = [s]
        strata_name = NAME_LEVEL_SEP.join(s)
        for keys, group_df in df.group_by(s):
            if not isinstance(keys, tuple):
                keys = (keys,)
            strata_level = NAME_LEVEL_SEP.join(str(k) for k in keys)
            groups.append((strata_name, strata_level, group_df))
    return groups


def _empty_result(result_type: str) -> SummarisedResult:
    """Create empty SummarisedResult with correct schema."""
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
    settings = _make_settings(1, result_type)
    return SummarisedResult(data, settings=settings)


def _add_count_rows(
    df: pl.DataFrame,
    result_id: int,
    cdm_name: str,
    group_name: str,
    group_level: str,
    strata_name: str,
    strata_level: str,
    additional_name: str = OVERALL,
    additional_level: str = OVERALL,
) -> list[dict[str, Any]]:
    """Emit Number records and Number subjects rows."""
    n_records = df.height
    n_subjects = df["person_id"].n_unique() if df.height > 0 else 0
    base = {
        "result_id": result_id,
        "cdm_name": cdm_name,
        "group_name": group_name,
        "group_level": group_level,
        "strata_name": strata_name,
        "strata_level": strata_level,
        "variable_level": "",
        "additional_name": additional_name,
        "additional_level": additional_level,
    }
    return [
        {
            **base,
            "variable_name": "Number records",
            "estimate_name": "count",
            "estimate_type": "integer",
            "estimate_value": str(n_records),
        },
        {
            **base,
            "variable_name": "Number subjects",
            "estimate_name": "count",
            "estimate_type": "integer",
            "estimate_value": str(n_subjects),
        },
    ]


# ---------------------------------------------------------------------------
# Pathway aggregation
# ---------------------------------------------------------------------------


def _build_pathway_strings(history: pl.DataFrame) -> pl.DataFrame:
    """Build dash-separated pathway strings per person/target.

    Returns DataFrame with columns: person_id, target_cohort_id, pathway,
    age, sex, index_year.
    """
    if history.height == 0:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "target_cohort_id": pl.Int64,
                "pathway": pl.Utf8,
                "age": pl.Int64,
                "sex": pl.Utf8,
                "index_year": pl.Int32,
            }
        )

    # Filter to events only (not exits)
    events = history
    if "type" in history.columns:
        events = history.filter(pl.col("type") == "event")

    if events.height == 0:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "target_cohort_id": pl.Int64,
                "pathway": pl.Utf8,
                "age": pl.Int64,
                "sex": pl.Utf8,
                "index_year": pl.Int32,
            }
        )

    # Sort by event sequence
    events = events.sort("person_id", "target_cohort_id", "event_seq")

    # Build pathway string per person/target
    # Use n_target if present for grouping
    group_cols = ["person_id", "target_cohort_id"]

    pathways = events.group_by(group_cols).agg(
        pl.col("event_cohort_name").str.join(delimiter="-").alias("pathway"),
        pl.col("age").first(),
        pl.col("sex").first(),
        pl.col("index_year").first(),
    )

    return pathways


# ---------------------------------------------------------------------------
# Public API: summarise_treatment_pathways
# ---------------------------------------------------------------------------


def summarise_treatment_pathways(
    result: PathwayResult,
    *,
    age_window: int | list[int] = 10,
    min_cell_count: int = 5,
    strata: list[str | list[str]] | None = None,
    include_none_paths: bool = False,
) -> SummarisedResult:
    """Aggregate treatment pathways into a ``SummarisedResult``.

    Converts patient-level ``PathwayResult`` from :func:`compute_pathways`
    into aggregate pathway frequency counts, optionally stratified by
    age group, sex, and/or index year.

    Parameters
    ----------
    result
        Output from :func:`compute_pathways`.
    age_window
        Age bin size (single int) or list of breakpoints for age groups.
    min_cell_count
        Minimum frequency for a pathway to be included (privacy).
    strata
        Additional stratification columns. Built-in strata for age, sex,
        and index_year are always available via the ``PathwayResult``
        demographics.
    include_none_paths
        If ``True``, include persons with no treatment events as a
        ``"None"`` pathway.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_treatment_pathways"``.
    """
    history = result.treatment_history
    cdm_name = result.cdm_name

    if history.height == 0:
        return _empty_result("summarise_treatment_pathways")

    # Build pathway strings
    pathways = _build_pathway_strings(history)

    if pathways.height == 0 and not include_none_paths:
        return _empty_result("summarise_treatment_pathways")

    # Add age_group column
    if pathways.height > 0:
        pathways = _add_age_group(pathways, age_window)

    # Aggregate by pathway + strata
    all_rows: list[dict[str, Any]] = []
    result_id = 1

    # Get target cohort info
    target_specs = [c for c in result.cohorts if c.type == "target"]

    for ts in target_specs:
        target_df = pathways.filter(pl.col("target_cohort_id") == ts.cohort_id)

        # Default strata combos: overall, age_group, sex, index_year
        strata_combos: list[str | list[str]] = []
        if strata:
            strata_combos.extend(strata)

        strata_groups = _resolve_strata(target_df, strata_combos)

        for sname, slevel, sdf in strata_groups:
            if sdf.height == 0:
                continue

            # Count pathways
            pathway_counts = (
                sdf.group_by("pathway").agg(pl.len().alias("freq")).sort("freq", descending=True)
            )

            # Apply min cell count
            pathway_counts = pathway_counts.filter(pl.col("freq") >= min_cell_count)

            for row in pathway_counts.iter_rows(named=True):
                all_rows.append(
                    {
                        "result_id": result_id,
                        "cdm_name": cdm_name,
                        "group_name": "target_cohort_name",
                        "group_level": ts.cohort_name,
                        "strata_name": sname,
                        "strata_level": slevel,
                        "variable_name": "treatment_pathway",
                        "variable_level": row["pathway"],
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(row["freq"]),
                        "additional_name": OVERALL,
                        "additional_level": OVERALL,
                    }
                )

            # Also emit percentage
            total = sdf.height
            for row in pathway_counts.iter_rows(named=True):
                pct = row["freq"] / total * 100 if total > 0 else 0.0
                all_rows.append(
                    {
                        "result_id": result_id,
                        "cdm_name": cdm_name,
                        "group_name": "target_cohort_name",
                        "group_level": ts.cohort_name,
                        "strata_name": sname,
                        "strata_level": slevel,
                        "variable_name": "treatment_pathway",
                        "variable_level": row["pathway"],
                        "estimate_name": "percentage",
                        "estimate_type": "percentage",
                        "estimate_value": f"{pct:.2f}",
                        "additional_name": OVERALL,
                        "additional_level": OVERALL,
                    }
                )

            # Count rows
            all_rows.extend(
                _add_count_rows(
                    sdf,
                    result_id,
                    cdm_name,
                    "target_cohort_name",
                    ts.cohort_name,
                    sname,
                    slevel,
                )
            )

    if not all_rows:
        return _empty_result("summarise_treatment_pathways")

    data = pl.DataFrame(all_rows)
    settings = _make_settings(result_id, "summarise_treatment_pathways")
    return SummarisedResult(data, settings=settings)


def _add_age_group(df: pl.DataFrame, age_window: int | list[int]) -> pl.DataFrame:
    """Add age_group column based on age bins."""
    if isinstance(age_window, int):
        # Create bins: 0-age_window, age_window-2*age_window, etc.
        if "age" not in df.columns:
            return df.with_columns(pl.lit("all").alias("age_group"))
        max_age = df["age"].max() or 100
        breaks = list(range(0, int(max_age) + age_window + 1, age_window))
    else:
        breaks = sorted(age_window)

    if "age" not in df.columns:
        return df.with_columns(pl.lit("all").alias("age_group"))

    # Build age group labels
    labels = []
    for i in range(len(breaks) - 1):
        lo = breaks[i]
        hi = breaks[i + 1] - 1
        labels.append(f"{lo}-{hi}")

    # Use cut for binning
    df = df.with_columns(pl.col("age").cut(breaks[1:-1], labels=labels).alias("age_group"))

    return df


# ---------------------------------------------------------------------------
# Public API: summarise_event_duration
# ---------------------------------------------------------------------------


def summarise_event_duration(
    result: PathwayResult,
    *,
    min_cell_count: int = 0,
) -> SummarisedResult:
    """Summarise duration statistics of treatment events.

    Computes min, Q1, median, Q3, max, mean, and SD of event durations,
    broken down by:

    - Overall: all events combined
    - Per treatment line (1st-line, 2nd-line, etc.)
    - Per individual treatment (drug name)
    - Per individual treatment per line

    Parameters
    ----------
    result
        Output from :func:`compute_pathways`.
    min_cell_count
        Minimum number of events required for a statistic to be included.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_event_duration"``.
    """
    history = result.treatment_history
    cdm_name = result.cdm_name

    if history.height == 0:
        return _empty_result("summarise_event_duration")

    # Filter to events only
    events = history
    if "type" in events.columns:
        events = events.filter(pl.col("type") == "event")

    if events.height == 0:
        return _empty_result("summarise_event_duration")

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    target_specs = [c for c in result.cohorts if c.type == "target"]

    for ts in target_specs:
        target_events = events.filter(pl.col("target_cohort_id") == ts.cohort_id)

        if target_events.height == 0:
            continue

        # Classify events
        target_events = target_events.with_columns(
            pl.when(pl.col("event_cohort_name").str.contains(r"\+"))
            .then(pl.lit("combination"))
            .otherwise(pl.lit("mono"))
            .alias("_event_class")
        )

        # 1. Overall aggregation
        _emit_duration_rows(
            all_rows,
            target_events,
            result_id,
            cdm_name,
            ts.cohort_name,
            ts.cohort_id,
            line="overall",
            min_cell_count=min_cell_count,
        )

        # 2. Per-line aggregation
        if "event_seq" in target_events.columns:
            for seq in sorted(target_events["event_seq"].unique().to_list()):
                line_events = target_events.filter(pl.col("event_seq") == seq)
                _emit_duration_rows(
                    all_rows,
                    line_events,
                    result_id,
                    cdm_name,
                    ts.cohort_name,
                    ts.cohort_id,
                    line=str(seq),
                    min_cell_count=min_cell_count,
                )

    if not all_rows:
        return _empty_result("summarise_event_duration")

    data = pl.DataFrame(all_rows)
    settings = _make_settings(result_id, "summarise_event_duration")
    return SummarisedResult(data, settings=settings)


def _emit_duration_rows(
    all_rows: list[dict[str, Any]],
    events: pl.DataFrame,
    result_id: int,
    cdm_name: str,
    target_name: str,
    target_id: int,
    *,
    line: str,
    min_cell_count: int,
) -> None:
    """Emit duration statistic rows for a set of events."""
    estimates = ("min", "q25", "median", "q75", "max", "mean", "sd", "count")

    # Helper to emit stats for a subset
    def _emit(event_name: str, durations: pl.Series) -> None:
        n = len(durations)
        if n < min_cell_count:
            return

        vals = durations.drop_nulls()
        if len(vals) == 0:
            return

        stats = {
            "min": str(int(vals.min())),
            "q25": str(int(vals.quantile(0.25, interpolation="nearest"))),
            "median": str(int(vals.median())),
            "q75": str(int(vals.quantile(0.75, interpolation="nearest"))),
            "max": str(int(vals.max())),
            "mean": f"{vals.mean():.2f}",
            "sd": f"{vals.std():.2f}" if len(vals) > 1 else "NA",
            "count": str(n),
        }

        for est_name in estimates:
            all_rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": "target_cohort_name",
                    "group_level": target_name,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": event_name,
                    "variable_level": "",
                    "estimate_name": est_name,
                    "estimate_type": "numeric" if est_name not in ("count",) else "integer",
                    "estimate_value": stats[est_name],
                    "additional_name": "line",
                    "additional_level": line,
                }
            )

    # Overall mono/combination
    if "_event_class" in events.columns:
        for cls in ("mono", "combination"):
            cls_events = events.filter(pl.col("_event_class") == cls)
            if cls_events.height > 0:
                _emit(
                    f"{cls}-event",
                    cls_events["duration_era"],
                )

    # Per individual drug/combination
    for name in sorted(events["event_cohort_name"].unique().to_list()):
        name_events = events.filter(pl.col("event_cohort_name") == name)
        if name_events.height > 0:
            _emit(name, name_events["duration_era"])
