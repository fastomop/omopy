"""Core treatment pathway computation engine.

Implements the sequential pipeline for constructing treatment pathways
from OMOP CDM cohort data:

1. Ingest cohort data and enrich with demographics
2. Build treatment history (match events to target windows)
3. Optional: split event cohorts into acute/therapy
4. Era collapse (merge same-drug gaps within threshold)
5. Combination window (detect overlaps, create combination treatments)
6. Filter treatments (first/changes/all)
7. Assign pathway sequences
"""

from __future__ import annotations

import datetime
from typing import Any, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from omopy.generics import CdmReference, CohortTable


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class CohortSpec(BaseModel):
    """Specification for a cohort used in pathway computation.

    Parameters
    ----------
    cohort_id
        The cohort_definition_id in the cohort table.
    cohort_name
        Human-readable name for this cohort.
    type
        Role: ``"target"`` (defines observation window), ``"event"``
        (treatment to track), or ``"exit"`` (appended after processing).
    """

    model_config = ConfigDict(frozen=True)

    cohort_id: int
    cohort_name: str
    type: Literal["target", "event", "exit"]


class PathwayResult(BaseModel):
    """Result container from :func:`compute_pathways`.

    Contains patient-level treatment history, attrition tracking,
    and metadata. This is **not** a ``SummarisedResult``; it is an
    intermediate representation that :func:`summarise_treatment_pathways`
    converts into the standardised format.

    Attributes
    ----------
    treatment_history
        Patient-level treatment history with columns:
        ``person_id``, ``index_year``, ``event_cohort_id``,
        ``event_cohort_name``, ``event_start_date``, ``event_end_date``,
        ``duration_era``, ``event_seq``, ``age``, ``sex``,
        ``target_cohort_id``, ``target_cohort_name``.
    attrition
        Step-by-step record/subject counts through the pipeline.
    cohorts
        The cohort specifications used.
    cdm_name
        Name of the CDM database.
    arguments
        Dictionary of all arguments passed to ``compute_pathways()``.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    treatment_history: pl.DataFrame
    attrition: pl.DataFrame
    cohorts: tuple[CohortSpec, ...]
    cdm_name: str
    arguments: dict[str, Any]


# ---------------------------------------------------------------------------
# Epoch helpers
# ---------------------------------------------------------------------------

_EPOCH = datetime.date(1970, 1, 1)


def _date_to_days(d: datetime.date) -> int:
    return (d - _EPOCH).days


def _days_to_date(days: int) -> datetime.date:
    return _EPOCH + datetime.timedelta(days=int(days))


# ---------------------------------------------------------------------------
# Attrition tracking
# ---------------------------------------------------------------------------


def _attrition_row(
    df: pl.DataFrame,
    reason_id: int,
    reason: str,
    target_cohort_id: int,
    target_cohort_name: str,
) -> dict[str, Any]:
    """Build a single attrition row from current state of treatment history."""
    if df.height == 0:
        n_records, n_subjects = 0, 0
    else:
        n_records = df.height
        n_subjects = df["person_id"].n_unique()
    return {
        "number_records": n_records,
        "number_subjects": n_subjects,
        "reason_id": reason_id,
        "reason": reason,
        "target_cohort_id": target_cohort_id,
        "target_cohort_name": target_cohort_name,
    }


# ---------------------------------------------------------------------------
# Step 0: Data ingestion
# ---------------------------------------------------------------------------


def _ingest_cohort_data(
    cohort: CohortTable,
    cdm: CdmReference,
    cohorts: list[CohortSpec],
    min_era_duration: int,
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Pull cohort data and enrich with demographics.

    Returns (treatment_df, attrition_rows).
    """
    # Collect cohort data to Polars
    cohort_ids = [c.cohort_id for c in cohorts]
    df = cohort.collect()
    # Filter to specified cohort IDs
    df = df.filter(pl.col("cohort_definition_id").is_in(cohort_ids))

    if df.height == 0:
        return df, []

    # Ensure date columns are Date type
    for col in ("cohort_start_date", "cohort_end_date"):
        if df[col].dtype != pl.Date:
            df = df.with_columns(pl.col(col).cast(pl.Date))

    # Convert subject_id to person_id
    if "subject_id" in df.columns and "person_id" not in df.columns:
        df = df.rename({"subject_id": "person_id"})

    # Join with person table for demographics
    person_df = cdm["person"].collect().select("person_id", "year_of_birth", "gender_concept_id")

    # Map gender concept to sex label
    gender_map = {8507: "Male", 8532: "Female"}
    person_df = person_df.with_columns(
        pl.col("gender_concept_id").replace_strict(gender_map, default="Unknown").alias("sex")
    )

    df = df.join(person_df, on="person_id", how="left")

    # Compute age at cohort start
    df = df.with_columns(
        (pl.col("cohort_start_date").dt.year() - pl.col("year_of_birth")).alias("age")
    )

    # Compute duration in days
    df = df.with_columns(
        ((pl.col("cohort_end_date") - pl.col("cohort_start_date")).dt.total_days()).alias(
            "duration_era"
        )
    )

    # Build attrition: initial counts per target cohort
    attrition_rows: list[dict[str, Any]] = []
    target_specs = [c for c in cohorts if c.type == "target"]
    for ts in target_specs:
        target_df = df.filter(pl.col("cohort_definition_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(target_df, 1, "Qualifying records", ts.cohort_id, ts.cohort_name)
        )

    # Filter by min_era_duration
    if min_era_duration > 0:
        df = df.filter(pl.col("duration_era") >= min_era_duration)
        for ts in target_specs:
            target_df = df.filter(pl.col("cohort_definition_id") == ts.cohort_id)
            attrition_rows.append(
                _attrition_row(
                    target_df,
                    2,
                    f"Removing records < {min_era_duration} days",
                    ts.cohort_id,
                    ts.cohort_name,
                )
            )

    return df, attrition_rows


# ---------------------------------------------------------------------------
# Step 1: Create treatment history
# ---------------------------------------------------------------------------


def _create_treatment_history(
    df: pl.DataFrame,
    cohorts: list[CohortSpec],
    *,
    start_anchor: Literal["start_date", "end_date"],
    window_start: int,
    end_anchor: Literal["start_date", "end_date"],
    window_end: int,
    concat_targets: bool,
) -> pl.DataFrame:
    """Match event cohorts to target observation windows."""
    target_specs = [c for c in cohorts if c.type == "target"]
    event_specs = [c for c in cohorts if c.type == "event"]
    exit_specs = [c for c in cohorts if c.type == "exit"]

    target_ids = {c.cohort_id for c in target_specs}
    event_ids = {c.cohort_id for c in event_specs}
    exit_ids = {c.cohort_id for c in exit_specs}

    id_to_name = {c.cohort_id: c.cohort_name for c in cohorts}

    # Split into target and event data
    targets = df.filter(pl.col("cohort_definition_id").is_in(target_ids))
    events = df.filter(pl.col("cohort_definition_id").is_in(event_ids | exit_ids))

    if targets.height == 0 or events.height == 0:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "index_year": pl.Int32,
                "event_cohort_id": pl.Utf8,
                "event_start_date": pl.Date,
                "event_end_date": pl.Date,
                "duration_era": pl.Int64,
                "age": pl.Int64,
                "sex": pl.Utf8,
                "target_cohort_id": pl.Int64,
                "target_cohort_name": pl.Utf8,
                "n_target": pl.Int32,
                "type": pl.Utf8,
            }
        )

    # Compute observation window for each target entry
    start_col = "cohort_start_date" if start_anchor == "start_date" else "cohort_end_date"
    end_col = "cohort_end_date" if end_anchor == "end_date" else "cohort_start_date"

    targets = targets.with_columns(
        (pl.col(start_col) + pl.duration(days=window_start)).alias("index_date"),
        (pl.col(end_col) + pl.duration(days=window_end)).alias("window_end_date"),
    )

    # Index year
    targets = targets.with_columns(
        pl.col("index_date").dt.year().cast(pl.Int32).alias("index_year")
    )

    # Assign n_target (per person, sequential target entries)
    if concat_targets:
        targets = targets.with_columns(pl.lit(1).cast(pl.Int32).alias("n_target"))
    else:
        targets = targets.with_columns(
            pl.col("cohort_start_date")
            .rank("ordinal")
            .over("person_id", pl.col("cohort_definition_id"))
            .cast(pl.Int32)
            .alias("n_target")
        )

    # Rename target columns to avoid collision
    targets_join = targets.select(
        "person_id",
        pl.col("cohort_definition_id").alias("target_cohort_id"),
        "index_date",
        "window_end_date",
        "index_year",
        "n_target",
        pl.col("age").alias("target_age"),
        pl.col("sex").alias("target_sex"),
    )

    # Rename event columns
    events_join = events.select(
        "person_id",
        pl.col("cohort_definition_id").alias("event_cid"),
        pl.col("cohort_start_date").alias("event_start_date"),
        pl.col("cohort_end_date").alias("event_end_date"),
        "duration_era",
    )

    # Join: events to targets on person_id, event starts within window
    joined = targets_join.join(events_join, on="person_id", how="inner")

    # Filter events within the observation window
    joined = joined.filter(
        (pl.col("event_start_date") >= pl.col("index_date"))
        & (pl.col("event_start_date") <= pl.col("window_end_date"))
    )

    # Clip event end to window end
    joined = joined.with_columns(
        pl.when(pl.col("event_end_date") > pl.col("window_end_date"))
        .then(pl.col("window_end_date"))
        .otherwise(pl.col("event_end_date"))
        .alias("event_end_date")
    )

    # Recompute duration after clipping
    joined = joined.with_columns(
        ((pl.col("event_end_date") - pl.col("event_start_date")).dt.total_days()).alias(
            "duration_era"
        )
    )

    # Build result
    id_to_name_map = {str(k): v for k, v in id_to_name.items()}
    result = joined.select(
        "person_id",
        "index_year",
        pl.col("event_cid").cast(pl.Utf8).alias("event_cohort_id"),
        "event_start_date",
        "event_end_date",
        "duration_era",
        pl.col("target_age").alias("age"),
        pl.col("target_sex").alias("sex"),
        "target_cohort_id",
        "n_target",
    ).with_columns(
        # Assign type: event or exit
        pl.when(pl.col("event_cohort_id").cast(pl.Int64).is_in(list(exit_ids)))
        .then(pl.lit("exit"))
        .otherwise(pl.lit("event"))
        .alias("type"),
        # Target cohort name
        pl.col("target_cohort_id")
        .cast(pl.Utf8)
        .replace_strict(
            {str(k): v for k, v in id_to_name.items() if k in target_ids},
            default="Unknown",
        )
        .alias("target_cohort_name"),
    )

    return result


# ---------------------------------------------------------------------------
# Step 2: Split event cohorts (optional)
# ---------------------------------------------------------------------------


def _split_event_cohorts(
    df: pl.DataFrame,
    split_event_cohorts: list[int] | None,
    split_time: list[int] | None,
    cohort_names: dict[int, str],
) -> pl.DataFrame:
    """Split specified event cohorts into acute/therapy based on duration."""
    if not split_event_cohorts or not split_time:
        return df
    if len(split_event_cohorts) != len(split_time):
        msg = "split_event_cohorts and split_time must have the same length"
        raise ValueError(msg)

    for cid, cutoff in zip(split_event_cohorts, split_time):
        cid_str = str(cid)
        name = cohort_names.get(cid, cid_str)
        mask = pl.col("event_cohort_id") == cid_str
        acute_id = f"{cid}a"
        therapy_id = f"{cid}t"

        # Acute: duration < cutoff
        df = df.with_columns(
            pl.when(mask & (pl.col("duration_era") < cutoff))
            .then(pl.lit(acute_id))
            .when(mask & (pl.col("duration_era") >= cutoff))
            .then(pl.lit(therapy_id))
            .otherwise(pl.col("event_cohort_id"))
            .alias("event_cohort_id")
        )

        # Update the cohort_names mapping (for later name resolution)
        cohort_names[acute_id] = f"{name} (acute)"  # type: ignore[assignment]
        cohort_names[therapy_id] = f"{name} (therapy)"  # type: ignore[assignment]

    return df


# ---------------------------------------------------------------------------
# Step 3: Era collapse
# ---------------------------------------------------------------------------


def _era_collapse(
    df: pl.DataFrame,
    era_collapse_size: int,
) -> pl.DataFrame:
    """Merge consecutive same-drug eras separated by <= era_collapse_size days.

    Iterates until no more merges occur.
    """
    if df.height == 0:
        return df

    events = df.filter(pl.col("type") == "event")
    exits = df.filter(pl.col("type") == "exit")

    max_iterations = 100
    for _ in range(max_iterations):
        # Sort by person, drug, target, date
        events = events.sort(
            "person_id", "event_cohort_id", "n_target", "event_start_date", "event_end_date"
        )

        # Compute gap to previous row of same drug for same person/target
        events = events.with_columns(
            (pl.col("event_start_date") - pl.col("event_end_date").shift(1))
            .dt.total_days()
            .over("person_id", "event_cohort_id", "n_target")
            .alias("_gap_to_prev")
        )

        # Identify rows that should merge with previous
        # A row merges if gap <= era_collapse_size (and gap is not null)
        events = events.with_columns(
            pl.when(
                pl.col("_gap_to_prev").is_not_null()
                & (pl.col("_gap_to_prev") <= era_collapse_size)
            )
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
            .alias("_new_era")
        )

        # If no merges needed, we're done
        if events["_new_era"].all():
            events = events.drop("_gap_to_prev", "_new_era")
            break

        # Assign era group IDs
        events = events.with_columns(
            pl.col("_new_era")
            .cum_sum()
            .over("person_id", "event_cohort_id", "n_target")
            .alias("_era_group")
        )

        # Collapse: take min start, max end per group
        events = (
            events.group_by("person_id", "event_cohort_id", "n_target", "_era_group")
            .agg(
                pl.col("event_start_date").min(),
                pl.col("event_end_date").max(),
                pl.col("index_year").first(),
                pl.col("age").first(),
                pl.col("sex").first(),
                pl.col("target_cohort_id").first(),
                pl.col("target_cohort_name").first(),
                pl.col("type").first(),
            )
            .drop("_era_group")
        )

        # Recompute duration
        events = events.with_columns(
            ((pl.col("event_end_date") - pl.col("event_start_date")).dt.total_days()).alias(
                "duration_era"
            )
        )
    else:
        # Remove temp columns if we hit max iterations
        for c in ("_gap_to_prev", "_new_era", "_era_group"):
            if c in events.columns:
                events = events.drop(c)

    # Recombine
    return pl.concat([events, exits], how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# Step 4: Combination window
# ---------------------------------------------------------------------------


def _combination_window(
    df: pl.DataFrame,
    combination_window: int,
    min_post_combination_duration: int,
    overlap_method: Literal["truncate", "keep"],
) -> pl.DataFrame:
    """Detect overlapping treatment eras and create combination segments.

    Iteratively resolves overlaps until none remain.
    """
    if df.height == 0:
        return df

    events = df.filter(pl.col("type") == "event")
    exits = df.filter(pl.col("type") == "exit")

    max_iterations = 200

    for _ in range(max_iterations):
        events = events.sort(
            "person_id", "n_target", "event_start_date", "event_end_date", "event_cohort_id"
        )

        # Check for overlaps: within same person/target, does any row overlap
        # with the previous row?
        events = events.with_columns(
            pl.col("event_end_date").shift(1).over("person_id", "n_target").alias("_prev_end"),
            pl.col("event_start_date").shift(1).over("person_id", "n_target").alias("_prev_start"),
            pl.col("event_cohort_id")
            .shift(1)
            .over("person_id", "n_target")
            .alias("_prev_cohort_id"),
        )

        # Overlap exists if current start < previous end
        events = events.with_columns(
            pl.when(
                pl.col("_prev_end").is_not_null()
                & (pl.col("event_start_date") < pl.col("_prev_end"))
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("_has_overlap")
        )

        if not events["_has_overlap"].any():
            events = events.drop("_prev_end", "_prev_start", "_prev_cohort_id", "_has_overlap")
            break

        # Process overlaps one at a time per person/target
        # Find the first overlapping row per person/target
        overlap_rows = events.filter(pl.col("_has_overlap"))

        # Get one overlap per person/target (the first one)
        first_overlaps = overlap_rows.group_by("person_id", "n_target").first()

        new_rows: list[dict[str, Any]] = []
        rows_to_remove: list[int] = []
        rows_to_update: dict[int, dict[str, Any]] = {}

        # Process using the DataFrame with row indices
        events = events.with_row_index("_row_idx")

        for overlap_row in first_overlaps.iter_rows(named=True):
            pid = overlap_row["person_id"]
            nt = overlap_row["n_target"]
            curr_start = overlap_row["event_start_date"]
            curr_end = overlap_row["event_end_date"]
            curr_cohort = overlap_row["event_cohort_id"]
            prev_end = overlap_row["_prev_end"]
            prev_start = overlap_row["_prev_start"]
            prev_cohort = overlap_row["_prev_cohort_id"]

            # Find the actual row index of this overlap in events
            mask = (
                (events["person_id"] == pid)
                & (events["n_target"] == nt)
                & (events["event_start_date"] == curr_start)
                & (events["event_cohort_id"] == curr_cohort)
                & (events["_has_overlap"])
            )
            curr_indices = events.filter(mask)["_row_idx"].to_list()
            if not curr_indices:
                continue
            curr_idx = curr_indices[0]

            # Find the previous row
            prev_mask = (
                (events["person_id"] == pid)
                & (events["n_target"] == nt)
                & (events["event_start_date"] == prev_start)
                & (events["event_cohort_id"] == prev_cohort)
                & (events["_row_idx"] < curr_idx)
            )
            prev_indices = events.filter(prev_mask)["_row_idx"].to_list()
            if not prev_indices:
                continue
            prev_idx = prev_indices[-1]  # Closest preceding row

            # Compute overlap size in days
            overlap_days = (prev_end - curr_start).days

            # Determine if it's a switch or combination
            is_switch = (
                overlap_days < combination_window
                or prev_cohort == curr_cohort  # Same drug overlap is always collapsed
            )

            if is_switch:
                if overlap_method == "truncate":
                    # Truncate previous era to end at current start
                    rows_to_update[prev_idx] = {
                        "event_end_date": curr_start,
                    }
                # else keep: do nothing, just mark as not overlapping
                # Remove the overlap flag from current row so we don't re-process
            elif prev_end <= curr_end:
                # FRFS: First Received, First Stopped
                # Previous ends before current ends
                # Split into: [prev-only] [combination] [curr-only]
                combo_id = _make_combination_id(prev_cohort, curr_cohort)

                # Previous era: truncate to end at curr_start
                rows_to_update[prev_idx] = {
                    "event_end_date": curr_start,
                }

                # Current era: starts at prev_end (the original prev end)
                rows_to_update[curr_idx] = {
                    "event_start_date": prev_end,
                }

                # New combination row
                base = events.filter(pl.col("_row_idx") == curr_idx).row(0, named=True)
                new_rows.append(
                    {
                        "person_id": pid,
                        "index_year": base["index_year"],
                        "event_cohort_id": combo_id,
                        "event_start_date": curr_start,
                        "event_end_date": prev_end,
                        "duration_era": (prev_end - curr_start).days,
                        "age": base["age"],
                        "sex": base["sex"],
                        "target_cohort_id": base["target_cohort_id"],
                        "target_cohort_name": base.get("target_cohort_name", ""),
                        "n_target": nt,
                        "type": "event",
                    }
                )
            else:
                # LRFS: Last Received, First Stopped
                # Current is entirely within previous
                combo_id = _make_combination_id(prev_cohort, curr_cohort)

                # Current era becomes the combination
                rows_to_update[curr_idx] = {
                    "event_cohort_id": combo_id,
                }

                # Previous era: truncate to end at curr_start
                rows_to_update[prev_idx] = {
                    "event_end_date": curr_start,
                }

                # New row for remainder of previous after current ends
                base = events.filter(pl.col("_row_idx") == prev_idx).row(0, named=True)
                if curr_end < prev_end:
                    new_rows.append(
                        {
                            "person_id": pid,
                            "index_year": base["index_year"],
                            "event_cohort_id": prev_cohort,
                            "event_start_date": curr_end,
                            "event_end_date": prev_end,
                            "duration_era": (prev_end - curr_end).days,
                            "age": base["age"],
                            "sex": base["sex"],
                            "target_cohort_id": base["target_cohort_id"],
                            "target_cohort_name": base.get("target_cohort_name", ""),
                            "n_target": nt,
                            "type": "event",
                        }
                    )

        # Apply updates
        if rows_to_update:
            for idx, updates in rows_to_update.items():
                for col, val in updates.items():
                    events = events.with_columns(
                        pl.when(pl.col("_row_idx") == idx)
                        .then(pl.lit(val))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

        # Remove temp columns
        events = events.drop(
            "_row_idx", "_prev_end", "_prev_start", "_prev_cohort_id", "_has_overlap"
        )

        # Add new rows
        if new_rows:
            new_df = pl.DataFrame(
                new_rows,
                schema_overrides={
                    "person_id": events["person_id"].dtype,
                    "event_start_date": pl.Date,
                    "event_end_date": pl.Date,
                    "target_cohort_id": events["target_cohort_id"].dtype,
                },
            )
            events = pl.concat([events, new_df], how="diagonal_relaxed")

        # Recompute duration
        events = events.with_columns(
            ((pl.col("event_end_date") - pl.col("event_start_date")).dt.total_days()).alias(
                "duration_era"
            )
        )

        # Filter by min_post_combination_duration
        if min_post_combination_duration > 0:
            events = events.filter(pl.col("duration_era") >= min_post_combination_duration)

    else:
        # Clean up temp columns on max iterations
        for c in ("_row_idx", "_prev_end", "_prev_start", "_prev_cohort_id", "_has_overlap"):
            if c in events.columns:
                events = events.drop(c)

    return pl.concat([events, exits], how="diagonal_relaxed")


def _make_combination_id(id1: str, id2: str) -> str:
    """Create a sorted combination ID like '1+3'."""
    parts = set()
    for x in (id1, id2):
        parts.update(x.split("+"))
    return "+".join(sorted(parts))


# ---------------------------------------------------------------------------
# Step 5: Filter treatments
# ---------------------------------------------------------------------------


def _filter_treatments(
    df: pl.DataFrame,
    filter_treatments: Literal["first", "changes", "all"],
) -> pl.DataFrame:
    """Filter treatment history based on strategy.

    Parameters
    ----------
    filter_treatments
        ``"first"`` — keep only first occurrence of each drug per person.
        ``"changes"`` — keep only rows where treatment changes from previous.
        ``"all"`` — keep everything.
    """
    if df.height == 0:
        return df
    if filter_treatments == "all":
        # Sort combination IDs for consistency
        df = df.with_columns(
            pl.col("event_cohort_id")
            .map_elements(
                lambda x: "+".join(sorted(x.split("+"))),
                return_dtype=pl.Utf8,
            )
            .alias("event_cohort_id")
        )
        return df

    events = df.filter(pl.col("type") == "event")
    exits = df.filter(pl.col("type") == "exit")

    # Sort combination IDs
    events = events.with_columns(
        pl.col("event_cohort_id")
        .map_elements(
            lambda x: "+".join(sorted(x.split("+"))),
            return_dtype=pl.Utf8,
        )
        .alias("event_cohort_id")
    )

    # Sort by person, target, date
    events = events.sort("person_id", "n_target", "event_start_date", "event_end_date")

    if filter_treatments == "first":
        # Keep only the first occurrence of each drug per person/target
        events = events.unique(
            subset=["person_id", "event_cohort_id", "n_target"],
            keep="first",
        )
    elif filter_treatments == "changes":
        # Remove consecutive duplicates of the same drug
        events = events.with_columns(
            pl.col("event_cohort_id").shift(1).over("person_id", "n_target").alias("_prev_drug")
        )
        events = events.filter(
            pl.col("_prev_drug").is_null() | (pl.col("event_cohort_id") != pl.col("_prev_drug"))
        ).drop("_prev_drug")

    return pl.concat([events, exits], how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# Step 6: Finalize
# ---------------------------------------------------------------------------


def _finalize_pathways(
    df: pl.DataFrame,
    max_path_length: int,
    cohort_names: dict[str | int, str],
) -> pl.DataFrame:
    """Assign event sequences, apply max path length, resolve names."""
    if df.height == 0:
        schema = dict(df.schema)
        schema["event_seq"] = pl.Int32
        schema["event_cohort_name"] = pl.Utf8
        return pl.DataFrame(schema=schema)

    # Sort and assign event_seq
    df = df.sort("person_id", "n_target", "event_start_date", "event_end_date")

    df = df.with_columns(
        pl.col("event_start_date")
        .rank("ordinal")
        .over("person_id", "n_target")
        .cast(pl.Int32)
        .alias("event_seq")
    )

    # Truncate to max path length
    if max_path_length > 0:
        df = df.filter(pl.col("event_seq") <= max_path_length)

    # Resolve cohort names
    # Build name mapping: both simple IDs and combination IDs
    str_name_map = {str(k): v for k, v in cohort_names.items()}

    def _resolve_name(cid: str) -> str:
        if cid in str_name_map:
            return str_name_map[cid]
        # Try splitting as combination
        parts = cid.split("+")
        if len(parts) > 1:
            names = sorted(str_name_map.get(p, p) for p in parts)
            return "+".join(names)
        return cid

    df = df.with_columns(
        pl.col("event_cohort_id")
        .map_elements(_resolve_name, return_dtype=pl.Utf8)
        .alias("event_cohort_name")
    )

    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_pathways(
    cohort: CohortTable,
    cdm: CdmReference,
    cohorts: list[CohortSpec],
    *,
    start_anchor: Literal["start_date", "end_date"] = "start_date",
    window_start: int = 0,
    end_anchor: Literal["start_date", "end_date"] = "end_date",
    window_end: int = 0,
    min_era_duration: int = 0,
    split_event_cohorts: list[int] | None = None,
    split_time: list[int] | None = None,
    era_collapse_size: int = 30,
    combination_window: int = 30,
    min_post_combination_duration: int = 30,
    filter_treatments: Literal["first", "changes", "all"] = "first",
    max_path_length: int = 5,
    overlap_method: Literal["truncate", "keep"] = "truncate",
    concat_targets: bool = True,
) -> PathwayResult:
    """Compute treatment pathways from cohort data.

    Takes a ``CohortTable`` with target, event, and optionally exit
    cohorts, and computes sequential treatment pathways through a
    multi-step pipeline: data ingestion, treatment history construction,
    optional event splitting, era collapse, combination detection,
    treatment filtering, and pathway sequencing.

    Parameters
    ----------
    cohort
        A ``CohortTable`` containing all target, event, and exit cohorts.
    cdm
        The ``CdmReference`` for demographic data and CDM metadata.
    cohorts
        List of :class:`CohortSpec` defining the role of each cohort.
    start_anchor
        Anchor for observation window start: ``"start_date"`` or
        ``"end_date"`` of the target cohort.
    window_start
        Day offset from ``start_anchor`` for the observation window start.
    end_anchor
        Anchor for observation window end: ``"start_date"`` or
        ``"end_date"`` of the target cohort.
    window_end
        Day offset from ``end_anchor`` for the observation window end.
    min_era_duration
        Minimum duration in days for an event era to be included.
    split_event_cohorts
        Cohort IDs to split into acute/therapy based on duration.
    split_time
        Day cutoffs for splitting (parallel to ``split_event_cohorts``).
    era_collapse_size
        Maximum gap in days within which consecutive same-drug eras are
        merged.
    combination_window
        Minimum overlap in days for two drugs to be considered a
        combination treatment.
    min_post_combination_duration
        Minimum duration in days for eras flanking combinations.
    filter_treatments
        Strategy: ``"first"`` (keep first occurrence of each drug),
        ``"changes"`` (remove consecutive duplicates), ``"all"`` (keep
        everything).
    max_path_length
        Maximum number of treatment steps in a pathway.
    overlap_method
        How to handle short overlaps (not combinations): ``"truncate"``
        clips the earlier era, ``"keep"`` preserves original dates.
    concat_targets
        If ``True``, treat multiple target entries per person as a
        single continuous observation.

    Returns
    -------
    PathwayResult
        Contains patient-level treatment history, attrition, cohort
        specifications, and metadata.
    """
    # Validate cohorts
    if not cohorts:
        msg = "At least one CohortSpec is required"
        raise ValueError(msg)
    target_specs = [c for c in cohorts if c.type == "target"]
    if not target_specs:
        msg = "At least one cohort with type='target' is required"
        raise ValueError(msg)
    event_specs = [c for c in cohorts if c.type == "event"]
    if not event_specs:
        msg = "At least one cohort with type='event' is required"
        raise ValueError(msg)

    cdm_name = cdm.cdm_name or ""

    # Build name mapping
    cohort_names: dict[str | int, str] = {c.cohort_id: c.cohort_name for c in cohorts}

    # Step 0: Ingest data
    raw_df, attrition_rows = _ingest_cohort_data(cohort, cdm, cohorts, min_era_duration)

    # Step 1: Create treatment history
    history = _create_treatment_history(
        raw_df,
        cohorts,
        start_anchor=start_anchor,
        window_start=window_start,
        end_anchor=end_anchor,
        window_end=window_end,
        concat_targets=concat_targets,
    )

    # Attrition: after event matching
    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                3,
                "Events within observation window",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Step 2: Split event cohorts (optional)
    history = _split_event_cohorts(history, split_event_cohorts, split_time, cohort_names)

    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                4,
                "After split event cohorts",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Step 3: Era collapse
    history = _era_collapse(history, era_collapse_size)

    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                5,
                "After era collapse",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Step 4: Combination window
    history = _combination_window(
        history, combination_window, min_post_combination_duration, overlap_method
    )

    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                6,
                "After combination window",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Step 5: Filter treatments
    history = _filter_treatments(history, filter_treatments)

    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                7,
                f"After filter treatments ({filter_treatments})",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Step 6: Finalize
    history = _finalize_pathways(history, max_path_length, cohort_names)

    for ts in target_specs:
        target_history = history.filter(pl.col("target_cohort_id") == ts.cohort_id)
        attrition_rows.append(
            _attrition_row(
                target_history,
                8,
                f"After max path length ({max_path_length})",
                ts.cohort_id,
                ts.cohort_name,
            )
        )

    # Build attrition DataFrame
    attrition_df = (
        pl.DataFrame(attrition_rows)
        if attrition_rows
        else pl.DataFrame(
            schema={
                "number_records": pl.Int64,
                "number_subjects": pl.Int64,
                "reason_id": pl.Int32,
                "reason": pl.Utf8,
                "target_cohort_id": pl.Int64,
                "target_cohort_name": pl.Utf8,
            }
        )
    )

    # Select final columns for treatment_history
    final_cols = [
        "person_id",
        "index_year",
        "event_cohort_id",
        "event_cohort_name",
        "event_start_date",
        "event_end_date",
        "duration_era",
        "event_seq",
        "age",
        "sex",
        "target_cohort_id",
        "target_cohort_name",
    ]
    available_cols = [c for c in final_cols if c in history.columns]
    history = history.select(available_cols)

    return PathwayResult(
        treatment_history=history,
        attrition=attrition_df,
        cohorts=tuple(cohorts),
        cdm_name=cdm_name,
        arguments={
            "start_anchor": start_anchor,
            "window_start": window_start,
            "end_anchor": end_anchor,
            "window_end": window_end,
            "min_era_duration": min_era_duration,
            "split_event_cohorts": split_event_cohorts,
            "split_time": split_time,
            "era_collapse_size": era_collapse_size,
            "combination_window": combination_window,
            "min_post_combination_duration": min_post_combination_duration,
            "filter_treatments": filter_treatments,
            "max_path_length": max_path_length,
            "overlap_method": overlap_method,
            "concat_targets": concat_targets,
        },
    )
