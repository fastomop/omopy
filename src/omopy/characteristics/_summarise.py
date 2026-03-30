"""Core summarise functions for cohort characteristics.

Implements the seven ``summarise_*`` analytical functions that compute
statistics on cohort data and return :class:`SummarisedResult` objects.

Each function follows the same pattern:
1. Collect cohort data via the CDM
2. Optionally add demographics / intersections via ``omopy.profiles``
3. Aggregate into long-format SummarisedResult rows
"""

from __future__ import annotations

import itertools
import math
from typing import Any, Literal

import polars as pl

from omopy.generics._types import NAME_LEVEL_SEP, OVERALL
from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.generics.codelist import Codelist
from omopy.generics.summarised_result import SummarisedResult

# Local type alias for window tuples
Window = tuple[float, float]

# ---------------------------------------------------------------------------
# Package metadata
# ---------------------------------------------------------------------------

_PACKAGE_NAME = "omopy.characteristics"
_PACKAGE_VERSION = "0.1.0"

# ---------------------------------------------------------------------------
# Default estimates per variable type
# ---------------------------------------------------------------------------

_DATE_ESTIMATES = ("min", "q25", "median", "q75", "max")
_NUMERIC_ESTIMATES = ("min", "q25", "median", "q75", "max", "mean", "sd")
_CATEGORICAL_ESTIMATES = ("count", "percentage")
_BINARY_ESTIMATES = ("count", "percentage")

# Columns that are always numeric
_NUMERIC_DEMOGRAPHICS = frozenset(
    {
        "age",
        "prior_observation",
        "future_observation",
        "days_in_cohort",
        "days_to_next_record",
    }
)

# Columns that are always categorical
_CATEGORICAL_DEMOGRAPHICS = frozenset(
    {
        "sex",
    }
)

# Columns that are always date-typed
_DATE_DEMOGRAPHICS = frozenset(
    {
        "cohort_start_date",
        "cohort_end_date",
    }
)

__all__ = [
    "summarise_characteristics",
    "summarise_cohort_count",
    "summarise_cohort_attrition",
    "summarise_cohort_timing",
    "summarise_cohort_overlap",
    "summarise_large_scale_characteristics",
    "summarise_cohort_codelist",
]


# ===================================================================
# Internal aggregation engine
# ===================================================================


def _classify_variable(
    df: pl.DataFrame,
    col: str,
) -> Literal["numeric", "categorical", "date", "binary"]:
    """Classify a column into a variable type for estimate selection."""
    if col in _NUMERIC_DEMOGRAPHICS:
        return "numeric"
    if col in _CATEGORICAL_DEMOGRAPHICS:
        return "categorical"
    if col in _DATE_DEMOGRAPHICS:
        return "date"

    dtype = df.schema[col]
    if dtype == pl.Date or dtype == pl.Datetime:
        return "date"
    if dtype in (pl.Boolean,):
        return "binary"
    if dtype.is_numeric():
        # Check if it's a 0/1 flag column
        unique_vals = df[col].drop_nulls().unique().to_list()
        if set(unique_vals) <= {0, 1}:
            return "binary"
        return "numeric"
    return "categorical"


def _compute_estimates(
    series: pl.Series,
    var_type: str,
    estimates: tuple[str, ...] | None = None,
) -> list[tuple[str, str, str]]:
    """Compute requested estimates for a Polars series.

    Returns a list of (estimate_name, estimate_type, estimate_value) tuples.
    """
    if estimates is None:
        if var_type == "date":
            estimates = _DATE_ESTIMATES
        elif var_type in ("numeric", "integer"):
            estimates = _NUMERIC_ESTIMATES
        elif var_type in ("categorical",):
            estimates = _CATEGORICAL_ESTIMATES
        elif var_type == "binary":
            estimates = _BINARY_ESTIMATES
        else:
            estimates = _CATEGORICAL_ESTIMATES

    results: list[tuple[str, str, str]] = []
    non_null = series.drop_nulls()
    n = len(non_null)

    for est in estimates:
        if est == "count":
            results.append(("count", "integer", str(n)))
        elif est == "percentage":
            total = len(series)
            pct = (n / total * 100.0) if total > 0 else 0.0
            results.append(("percentage", "percentage", f"{pct:.2f}"))
        elif est == "mean":
            if n > 0 and var_type in ("numeric", "integer"):
                val = non_null.mean()
                results.append(("mean", "numeric", f"{val:.2f}"))
            else:
                results.append(("mean", "numeric", "NA"))
        elif est == "sd":
            if n > 1 and var_type in ("numeric", "integer"):
                val = non_null.std()
                results.append(("sd", "numeric", f"{val:.2f}"))
            elif n <= 1:
                results.append(("sd", "numeric", "NA"))
            else:
                results.append(("sd", "numeric", "NA"))
        elif est == "median":
            if n > 0:
                if var_type == "date":
                    val = non_null.sort().to_list()[n // 2]
                    results.append(("median", "date", str(val)))
                else:
                    val = non_null.median()
                    results.append(("median", "numeric", f"{val:.2f}"))
            else:
                results.append(("median", "numeric" if var_type != "date" else "date", "NA"))
        elif est == "q25":
            if n > 0:
                if var_type == "date":
                    val = non_null.sort().to_list()[max(0, n // 4)]
                    results.append(("q25", "date", str(val)))
                else:
                    val = non_null.quantile(0.25, interpolation="nearest")
                    results.append(("q25", "numeric", f"{val:.2f}"))
            else:
                results.append(("q25", "numeric" if var_type != "date" else "date", "NA"))
        elif est == "q75":
            if n > 0:
                if var_type == "date":
                    val = non_null.sort().to_list()[min(n - 1, 3 * n // 4)]
                    results.append(("q75", "date", str(val)))
                else:
                    val = non_null.quantile(0.75, interpolation="nearest")
                    results.append(("q75", "numeric", f"{val:.2f}"))
            else:
                results.append(("q75", "numeric" if var_type != "date" else "date", "NA"))
        elif est == "min":
            if n > 0:
                val = non_null.min()
                est_type = (
                    "date"
                    if var_type == "date"
                    else ("integer" if var_type == "integer" else "numeric")
                )
                results.append(("min", est_type, str(val)))
            else:
                est_type = "date" if var_type == "date" else "numeric"
                results.append(("min", est_type, "NA"))
        elif est == "max":
            if n > 0:
                val = non_null.max()
                est_type = (
                    "date"
                    if var_type == "date"
                    else ("integer" if var_type == "integer" else "numeric")
                )
                results.append(("max", est_type, str(val)))
            else:
                est_type = "date" if var_type == "date" else "numeric"
                results.append(("max", est_type, "NA"))

    return results


def _compute_categorical_estimates(
    series: pl.Series,
    total: int,
) -> list[tuple[str, str, str, str]]:
    """Compute count + percentage for each level of a categorical variable.

    Returns list of (variable_level, estimate_name, estimate_type, estimate_value).
    """
    results: list[tuple[str, str, str, str]] = []
    counts = series.value_counts().sort("count", descending=True)

    # value_counts returns a struct DataFrame with the column name and "count"
    val_col = series.name
    for row in counts.iter_rows(named=True):
        level = str(row[val_col]) if row[val_col] is not None else "NA"
        count = row["count"]
        pct = (count / total * 100.0) if total > 0 else 0.0
        results.append((level, "count", "integer", str(count)))
        results.append((level, "percentage", "percentage", f"{pct:.2f}"))

    return results


def _summarise_variables(
    df: pl.DataFrame,
    variables: list[str],
    *,
    cdm_name: str,
    result_id: int,
    group_name: str,
    group_level: str,
    strata_name: str,
    strata_level: str,
    additional_name: str = OVERALL,
    additional_level: str = OVERALL,
    estimates_override: dict[str, tuple[str, ...]] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate variables from a patient-level DataFrame into result rows."""
    rows: list[dict[str, Any]] = []

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

    for var in variables:
        if var not in df.columns:
            continue

        var_type = _classify_variable(df, var)

        # Human-readable variable name
        var_display = var.replace("_", " ").capitalize() if var not in _DATE_DEMOGRAPHICS else var

        # Override estimates if provided
        custom_ests = estimates_override.get(var) if estimates_override else None

        if var_type == "categorical":
            # For categorical: one row per level × estimate
            cat_results = _compute_categorical_estimates(df[var], len(df))
            for level, est_name, est_type, est_value in cat_results:
                rows.append(
                    {
                        **base,
                        "variable_name": var_display,
                        "variable_level": level,
                        "estimate_name": est_name,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                    }
                )
        elif var_type == "binary":
            # Binary: count/percentage for value == 1 (or True)
            total = len(df)
            if df.schema[var] == pl.Boolean:
                n_pos = df[var].sum()
            else:
                n_pos = (df[var] == 1).sum()
            pct = (n_pos / total * 100.0) if total > 0 else 0.0
            rows.append(
                {
                    **base,
                    "variable_name": var_display,
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(n_pos),
                }
            )
            rows.append(
                {
                    **base,
                    "variable_name": var_display,
                    "variable_level": "",
                    "estimate_name": "percentage",
                    "estimate_type": "percentage",
                    "estimate_value": f"{pct:.2f}",
                }
            )
        else:
            # Numeric or date: compute distribution estimates
            est_results = _compute_estimates(df[var], var_type, custom_ests)
            for est_name, est_type, est_value in est_results:
                rows.append(
                    {
                        **base,
                        "variable_name": var_display,
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                    }
                )

    return rows


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
    if "subject_id" in df.columns:
        n_subjects = df["subject_id"].n_unique()
    else:
        n_subjects = n_records

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


def _resolve_strata(
    df: pl.DataFrame,
    strata: list[str | list[str]],
) -> list[tuple[str, str, pl.DataFrame]]:
    """Generate (strata_name, strata_level, filtered_df) for each stratum.

    Always includes the "overall" stratum first, then each user-specified
    stratum combination.
    """
    groups: list[tuple[str, str, pl.DataFrame]] = []

    # Overall
    groups.append((OVERALL, OVERALL, df))

    for s in strata:
        if isinstance(s, str):
            s = [s]

        # Validate columns exist
        missing = [c for c in s if c not in df.columns]
        if missing:
            msg = f"Strata columns not found in data: {missing}"
            raise ValueError(msg)

        strata_name = NAME_LEVEL_SEP.join(s)

        # Group by strata columns
        for keys, group_df in df.group_by(s):
            if not isinstance(keys, tuple):
                keys = (keys,)
            strata_level = NAME_LEVEL_SEP.join(str(k) for k in keys)
            groups.append((strata_name, strata_level, group_df))

    return groups


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


# ===================================================================
# summarise_characteristics
# ===================================================================


def summarise_characteristics(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    strata: list[str | list[str]] | None = None,
    counts: bool = True,
    demographics: bool = True,
    age_group: dict[str, tuple[float, float]] | list[tuple[float, float]] | None = None,
    table_intersect_flag: list[dict[str, Any]] | None = None,
    table_intersect_count: list[dict[str, Any]] | None = None,
    table_intersect_date: list[dict[str, Any]] | None = None,
    table_intersect_days: list[dict[str, Any]] | None = None,
    cohort_intersect_flag: list[dict[str, Any]] | None = None,
    cohort_intersect_count: list[dict[str, Any]] | None = None,
    cohort_intersect_date: list[dict[str, Any]] | None = None,
    cohort_intersect_days: list[dict[str, Any]] | None = None,
    concept_intersect_flag: list[dict[str, Any]] | None = None,
    concept_intersect_count: list[dict[str, Any]] | None = None,
    concept_intersect_date: list[dict[str, Any]] | None = None,
    concept_intersect_days: list[dict[str, Any]] | None = None,
    other_variables: list[str] | None = None,
    estimates: dict[str, tuple[str, ...]] | None = None,
) -> SummarisedResult:
    """Summarise cohort characteristics including demographics and intersections.

    This is the main entry point for cohort characterisation. It:

    1. Enriches cohort records with demographics (age, sex, observation periods)
    2. Adds any requested intersections (table, cohort, concept)
    3. Aggregates per cohort × stratum into a standardised SummarisedResult

    Parameters
    ----------
    cohort
        A CohortTable to summarise.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.
    strata
        Stratification columns. Each element is a column name or list of
        column names to cross-stratify. The overall (unstratified) result
        is always included.
    counts
        Include subject/record counts.
    demographics
        Include demographic variables (age, sex, prior/future observation,
        days in cohort).
    age_group
        Age grouping specification, forwarded to ``add_demographics()``.
    table_intersect_flag, table_intersect_count, table_intersect_date, table_intersect_days
        Lists of keyword-argument dicts forwarded to the corresponding
        ``omopy.profiles.add_table_intersect_*()`` function.
    cohort_intersect_flag, cohort_intersect_count, cohort_intersect_date, cohort_intersect_days
        Lists of keyword-argument dicts forwarded to the corresponding
        ``omopy.profiles.add_cohort_intersect_*()`` function.
    concept_intersect_flag, concept_intersect_count, concept_intersect_date, concept_intersect_days
        Lists of keyword-argument dicts forwarded to the corresponding
        ``omopy.profiles.add_concept_intersect_*()`` function.
    other_variables
        Additional columns already present in the cohort to summarise.
    estimates
        Override default estimates per variable name. Keys are variable
        names, values are tuples of estimate names.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_characteristics"``.
    """
    from omopy.profiles import (
        add_demographics,
        add_table_intersect_flag,
        add_table_intersect_count,
        add_table_intersect_date,
        add_table_intersect_days,
        add_cohort_intersect_flag,
        add_cohort_intersect_count,
        add_cohort_intersect_date,
        add_cohort_intersect_days,
        add_concept_intersect_flag,
        add_concept_intersect_count,
        add_concept_intersect_date,
        add_concept_intersect_days,
    )
    from omopy.profiles._utilities import filter_cohort_id

    if strata is None:
        strata = []

    # Filter to requested cohort IDs
    working = filter_cohort_id(cohort, cohort_id)
    cdm = working.cdm

    # Determine which cohorts we have — filter settings to only
    # include requested cohort IDs (filter_cohort_id filters data but
    # may not filter the settings metadata)
    settings = _filter_settings_by_cohort_id(working.settings, cohort_id)
    cohort_ids = settings["cohort_definition_id"].to_list()
    cohort_names = settings["cohort_name"].to_list()

    # Step 1: Add demographics
    if demographics or age_group is not None:
        # Detect existing columns to avoid duplicate column errors
        # (e.g., if sex was pre-added for stratification)
        existing_cols = set(working.columns)

        demo_kwargs: dict[str, Any] = {
            "age": demographics and "age" not in existing_cols,
            "sex": demographics and "sex" not in existing_cols,
            "prior_observation": demographics and "prior_observation" not in existing_cols,
            "future_observation": demographics and "future_observation" not in existing_cols,
        }
        if age_group is not None:
            demo_kwargs["age"] = True
            demo_kwargs["age_group"] = age_group

        # Only call add_demographics if there's something to add
        if any(v for k, v in demo_kwargs.items() if k != "age_group"):
            working = add_demographics(working, cdm, **demo_kwargs)

    # Step 2: Add intersections
    intersect_fns = [
        (table_intersect_flag, add_table_intersect_flag),
        (table_intersect_count, add_table_intersect_count),
        (table_intersect_date, add_table_intersect_date),
        (table_intersect_days, add_table_intersect_days),
        (cohort_intersect_flag, add_cohort_intersect_flag),
        (cohort_intersect_count, add_cohort_intersect_count),
        (cohort_intersect_date, add_cohort_intersect_date),
        (cohort_intersect_days, add_cohort_intersect_days),
        (concept_intersect_flag, add_concept_intersect_flag),
        (concept_intersect_count, add_concept_intersect_count),
        (concept_intersect_date, add_concept_intersect_date),
        (concept_intersect_days, add_concept_intersect_days),
    ]

    for specs, fn in intersect_fns:
        if specs:
            for spec in specs:
                working = fn(working, cdm=cdm, **spec)

    # Step 3: Collect data
    df = working.collect()
    cdm_name = cdm.cdm_name if cdm else "unknown"

    # Step 4: Compute days_in_cohort and days_to_next_record
    if demographics and "cohort_start_date" in df.columns and "cohort_end_date" in df.columns:
        df = df.with_columns(
            ((pl.col("cohort_end_date") - pl.col("cohort_start_date")).dt.total_days() + 1).alias(
                "days_in_cohort"
            )
        )

    # Identify variables to summarise
    skip_cols = {"cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"}
    variables = [c for c in df.columns if c not in skip_cols]

    # Add other_variables
    if other_variables:
        for v in other_variables:
            if v not in variables and v in df.columns:
                variables.append(v)

    # Step 5: Aggregate per cohort × strata
    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for cid, cname in zip(cohort_ids, cohort_names):
        cohort_df = df.filter(pl.col("cohort_definition_id") == cid)

        strata_groups = _resolve_strata(cohort_df, strata)

        for sname, slevel, sdf in strata_groups:
            if counts:
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

            all_rows.extend(
                _summarise_variables(
                    sdf,
                    variables,
                    cdm_name=cdm_name,
                    result_id=result_id,
                    group_name="cohort_name",
                    group_level=cname,
                    strata_name=sname,
                    strata_level=slevel,
                    estimates_override=estimates,
                )
            )

    if not all_rows:
        # Return empty result
        return _empty_result("summarise_characteristics")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_characteristics")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_cohort_count
# ===================================================================


def summarise_cohort_count(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    strata: list[str | list[str]] | None = None,
) -> SummarisedResult:
    """Summarise subject and record counts per cohort.

    Thin wrapper around :func:`summarise_characteristics` with
    ``counts=True, demographics=False``.

    Parameters
    ----------
    cohort
        A CohortTable to count.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.
    strata
        Stratification columns.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_cohort_count"``.
    """
    result = summarise_characteristics(
        cohort,
        cohort_id=cohort_id,
        strata=strata,
        counts=True,
        demographics=False,
    )
    # Override result_type in settings
    new_settings = result.settings.with_columns(
        pl.lit("summarise_cohort_count").alias("result_type")
    )
    result.settings = new_settings
    return result


# ===================================================================
# summarise_cohort_attrition
# ===================================================================


def summarise_cohort_attrition(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
) -> SummarisedResult:
    """Summarise cohort attrition as a SummarisedResult.

    Pivots the attrition table (reasons, excluded counts) into the
    standard long-format result.

    Parameters
    ----------
    cohort
        A CohortTable with attrition data.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_cohort_attrition"``,
        ``strata_name="reason"``, ``additional_name="reason_id"``.
    """
    from omopy.profiles._utilities import filter_cohort_id

    working = filter_cohort_id(cohort, cohort_id)
    attrition = working.attrition
    settings_meta = _filter_settings_by_cohort_id(working.settings, cohort_id)
    cdm = working.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    if attrition is None or len(attrition) == 0:
        return _empty_result("summarise_cohort_attrition")

    # Map cohort_definition_id -> cohort_name
    id_to_name = dict(
        zip(
            settings_meta["cohort_definition_id"].to_list(),
            settings_meta["cohort_name"].to_list(),
        )
    )

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    # Expected attrition columns:
    # cohort_definition_id, number_records, number_subjects,
    # reason_id, reason, excluded_records, excluded_subjects
    for row in attrition.iter_rows(named=True):
        cid = row["cohort_definition_id"]
        cname = id_to_name.get(cid, str(cid))
        reason = str(row.get("reason", ""))
        reason_id = str(row.get("reason_id", ""))

        base = {
            "result_id": result_id,
            "cdm_name": cdm_name,
            "group_name": "cohort_name",
            "group_level": cname,
            "strata_name": "reason",
            "strata_level": reason,
            "additional_name": "reason_id",
            "additional_level": reason_id,
        }

        for var in ("number_records", "number_subjects", "excluded_records", "excluded_subjects"):
            val = row.get(var, 0)
            if val is None:
                val = 0
            all_rows.append(
                {
                    **base,
                    "variable_name": var,
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(int(val)),
                }
            )

    if not all_rows:
        return _empty_result("summarise_cohort_attrition")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_cohort_attrition")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_cohort_timing
# ===================================================================


def summarise_cohort_timing(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    strata: list[str | list[str]] | None = None,
    restrict_to_first_entry: bool = True,
    estimates: tuple[str, ...] = ("min", "q25", "median", "q75", "max"),
) -> SummarisedResult:
    """Summarise pairwise timing between cohort entries.

    For each pair of cohorts, computes the distribution of days between
    cohort entries for subjects appearing in both.

    Parameters
    ----------
    cohort
        A CohortTable.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.
    strata
        Stratification columns (must exist before the join).
    restrict_to_first_entry
        If ``True``, only consider the first entry per subject per cohort.
    estimates
        Statistics to compute on ``days_between_cohort_entries``.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_cohort_timing"``,
        ``group_name="cohort_name_reference &&& cohort_name_comparator"``.
    """
    from omopy.profiles._utilities import filter_cohort_id

    if strata is None:
        strata = []

    working = filter_cohort_id(cohort, cohort_id)
    df = working.collect()
    cdm = working.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    settings_meta = _filter_settings_by_cohort_id(working.settings, cohort_id)
    id_to_name = dict(
        zip(
            settings_meta["cohort_definition_id"].to_list(),
            settings_meta["cohort_name"].to_list(),
        )
    )

    # Optionally restrict to first entry
    if restrict_to_first_entry:
        df = df.sort("cohort_start_date").group_by(["cohort_definition_id", "subject_id"]).first()

    # Self-join on subject_id
    left = df.select(
        "cohort_definition_id",
        "subject_id",
        "cohort_start_date",
    ).rename(
        {
            "cohort_definition_id": "cid_ref",
            "cohort_start_date": "date_ref",
        }
    )
    right = df.select(
        "cohort_definition_id",
        "subject_id",
        "cohort_start_date",
    ).rename(
        {
            "cohort_definition_id": "cid_comp",
            "cohort_start_date": "date_comp",
        }
    )

    # Also carry strata columns if needed
    strata_flat = _flatten_strata(strata)
    if strata_flat:
        for col in strata_flat:
            if col in df.columns:
                left = left.with_columns(df[col].alias(col))

    joined = left.join(right, on="subject_id")
    # Only different-cohort pairs
    joined = joined.filter(pl.col("cid_ref") != pl.col("cid_comp"))

    # Compute days between entries
    joined = joined.with_columns(
        (pl.col("date_comp") - pl.col("date_ref"))
        .dt.total_days()
        .alias("days_between_cohort_entries")
    )

    # Map IDs to names
    joined = joined.with_columns(
        pl.col("cid_ref")
        .replace_strict(id_to_name, default="unknown")
        .alias("cohort_name_reference"),
        pl.col("cid_comp")
        .replace_strict(id_to_name, default="unknown")
        .alias("cohort_name_comparator"),
    )

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    group_name = "cohort_name_reference" + NAME_LEVEL_SEP + "cohort_name_comparator"

    # Get unique cohort pairs
    pairs = joined.select("cohort_name_reference", "cohort_name_comparator").unique()

    for pair_row in pairs.iter_rows(named=True):
        ref_name = pair_row["cohort_name_reference"]
        comp_name = pair_row["cohort_name_comparator"]
        group_level = ref_name + NAME_LEVEL_SEP + comp_name

        pair_df = joined.filter(
            (pl.col("cohort_name_reference") == ref_name)
            & (pl.col("cohort_name_comparator") == comp_name)
        )

        strata_groups = (
            _resolve_strata(pair_df, strata) if strata_flat else [(OVERALL, OVERALL, pair_df)]
        )

        for sname, slevel, sdf in strata_groups:
            base = {
                "result_id": result_id,
                "cdm_name": cdm_name,
                "group_name": group_name,
                "group_level": group_level,
                "strata_name": sname,
                "strata_level": slevel,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }

            # Count rows
            all_rows.append(
                {
                    **base,
                    "variable_name": "Number records",
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(len(sdf)),
                }
            )
            all_rows.append(
                {
                    **base,
                    "variable_name": "Number subjects",
                    "variable_level": "",
                    "estimate_name": "count",
                    "estimate_type": "integer",
                    "estimate_value": str(sdf["subject_id"].n_unique()),
                }
            )

            # Timing distribution
            est_results = _compute_estimates(
                sdf["days_between_cohort_entries"],
                "numeric",
                estimates,
            )
            for est_name, est_type, est_value in est_results:
                all_rows.append(
                    {
                        **base,
                        "variable_name": "Days between cohort entries",
                        "variable_level": "",
                        "estimate_name": est_name,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                    }
                )

    if not all_rows:
        return _empty_result("summarise_cohort_timing")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(
        result_id,
        "summarise_cohort_timing",
        restrict_to_first_entry=str(restrict_to_first_entry),
    )
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_cohort_overlap
# ===================================================================


def summarise_cohort_overlap(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    strata: list[str | list[str]] | None = None,
    overlap_by: str = "subject_id",
) -> SummarisedResult:
    """Summarise pairwise overlap between cohorts.

    For each pair of cohorts, counts subjects in only the reference,
    only the comparator, or in both.

    Parameters
    ----------
    cohort
        A CohortTable.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.
    strata
        Stratification columns.
    overlap_by
        Column identifying unique entities (default: ``"subject_id"``).

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_cohort_overlap"``,
        ``group_name="cohort_name_reference &&& cohort_name_comparator"``.
    """
    from omopy.profiles._utilities import filter_cohort_id

    if strata is None:
        strata = []

    working = filter_cohort_id(cohort, cohort_id)
    df = working.collect()
    cdm = working.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    settings_meta = _filter_settings_by_cohort_id(working.settings, cohort_id)
    id_to_name = dict(
        zip(
            settings_meta["cohort_definition_id"].to_list(),
            settings_meta["cohort_name"].to_list(),
        )
    )

    # Add cohort_name column
    df = df.with_columns(
        pl.col("cohort_definition_id")
        .replace_strict(id_to_name, default="unknown")
        .alias("cohort_name")
    )

    cohort_names_list = sorted(id_to_name.values())
    group_name_str = "cohort_name_reference" + NAME_LEVEL_SEP + "cohort_name_comparator"

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    variable_level = "Subjects" if overlap_by == "subject_id" else "Records"

    # Generate all ordered pairs
    for ref_name, comp_name in itertools.permutations(cohort_names_list, 2):
        group_level = ref_name + NAME_LEVEL_SEP + comp_name

        ref_df = df.filter(pl.col("cohort_name") == ref_name)
        comp_df = df.filter(pl.col("cohort_name") == comp_name)

        strata_flat = _flatten_strata(strata)
        strata_groups_list: list[tuple[str, str, pl.DataFrame, pl.DataFrame]] = []

        if not strata_flat:
            strata_groups_list.append((OVERALL, OVERALL, ref_df, comp_df))
        else:
            # For strata, we need to split both ref and comp
            for s in strata:
                if isinstance(s, str):
                    s = [s]
                sname = NAME_LEVEL_SEP.join(s)
                # Get unique strata levels
                all_keys = ref_df.select(s).unique().vstack(comp_df.select(s).unique()).unique()
                for key_row in all_keys.iter_rows():
                    if not isinstance(key_row, tuple):
                        key_row = (key_row,)
                    slevel = NAME_LEVEL_SEP.join(str(k) for k in key_row)
                    # Filter both sides
                    f_ref = ref_df
                    f_comp = comp_df
                    for col, val in zip(s, key_row):
                        f_ref = f_ref.filter(pl.col(col) == val)
                        f_comp = f_comp.filter(pl.col(col) == val)
                    strata_groups_list.append((sname, slevel, f_ref, f_comp))

            # Also add overall
            strata_groups_list.insert(0, (OVERALL, OVERALL, ref_df, comp_df))

        for sname, slevel, s_ref, s_comp in strata_groups_list:
            ref_ids = set(s_ref[overlap_by].unique().to_list())
            comp_ids = set(s_comp[overlap_by].unique().to_list())

            only_ref = len(ref_ids - comp_ids)
            only_comp = len(comp_ids - ref_ids)
            in_both = len(ref_ids & comp_ids)
            total = only_ref + only_comp + in_both

            base = {
                "result_id": result_id,
                "cdm_name": cdm_name,
                "group_name": group_name_str,
                "group_level": group_level,
                "strata_name": sname,
                "strata_level": slevel,
                "additional_name": OVERALL,
                "additional_level": OVERALL,
            }

            for var_name, count in [
                ("Only in reference cohort", only_ref),
                ("Only in comparator cohort", only_comp),
                ("In both cohorts", in_both),
            ]:
                pct = (count / total * 100.0) if total > 0 else 0.0
                all_rows.append(
                    {
                        **base,
                        "variable_name": var_name,
                        "variable_level": variable_level,
                        "estimate_name": "count",
                        "estimate_type": "integer",
                        "estimate_value": str(count),
                    }
                )
                all_rows.append(
                    {
                        **base,
                        "variable_name": var_name,
                        "variable_level": variable_level,
                        "estimate_name": "percentage",
                        "estimate_type": "percentage",
                        "estimate_value": f"{pct:.2f}",
                    }
                )

    if not all_rows:
        return _empty_result("summarise_cohort_overlap")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(
        result_id,
        "summarise_cohort_overlap",
        overlap_by=overlap_by,
    )
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_large_scale_characteristics
# ===================================================================


def summarise_large_scale_characteristics(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
    strata: list[str | list[str]] | None = None,
    window: list[Window] | None = None,
    event_in_window: list[str] | None = None,
    episode_in_window: list[str] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    minimum_frequency: float = 0.005,
    excluded_codes: list[int] | None = None,
) -> SummarisedResult:
    """Summarise large-scale characteristics (concept-level prevalence).

    For each specified OMOP domain table and time window, computes the
    frequency of each concept relative to the cohort.

    Parameters
    ----------
    cohort
        A CohortTable.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.
    strata
        Stratification columns.
    window
        Time windows as ``(lower, upper)`` day offsets from ``index_date``.
        Defaults to standard epidemiological windows.
    event_in_window
        OMOP table names to count events (point-in-time). E.g.
        ``["condition_occurrence", "drug_exposure"]``.
    episode_in_window
        OMOP table names to count episodes (interval overlap).
    index_date
        Column name for the index date.
    censor_date
        Column name for censoring. ``None`` = no censoring.
    minimum_frequency
        Minimum frequency threshold (0–1) to include a concept.
    excluded_codes
        Concept IDs to exclude from results.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_large_scale_characteristics"``.
    """
    from omopy.profiles._utilities import filter_cohort_id
    from omopy.profiles._demographics import _get_ibis_table, _resolve_cdm

    if strata is None:
        strata = []
    if window is None:
        window = [
            (-math.inf, -366),
            (-365, -31),
            (-30, -1),
            (0, 0),
            (1, 30),
            (31, 365),
            (366, math.inf),
        ]
    if event_in_window is None and episode_in_window is None:
        event_in_window = ["condition_occurrence", "drug_exposure"]
    if excluded_codes is None:
        excluded_codes = [0]

    working = filter_cohort_id(cohort, cohort_id)
    cdm = _resolve_cdm(working, working.cdm)
    cdm_name = cdm.cdm_name if cdm else "unknown"

    settings_meta = _filter_settings_by_cohort_id(working.settings, cohort_id)
    id_to_name = dict(
        zip(
            settings_meta["cohort_definition_id"].to_list(),
            settings_meta["cohort_name"].to_list(),
        )
    )

    # Collect cohort data
    cohort_df = working.collect()
    cohort_df = cohort_df.with_columns(
        pl.col("cohort_definition_id")
        .replace_strict(id_to_name, default="unknown")
        .alias("cohort_name")
    )

    # Get concept table for name lookup
    concept_tbl = cdm["concept"]
    concept_df = concept_tbl.collect()

    all_rows: list[dict[str, Any]] = []
    result_ids: list[int] = []
    settings_rows: list[dict[str, Any]] = []
    next_result_id = 1

    # Standard domain-table mapping
    _DOMAIN_DATE_COLS: dict[str, tuple[str, str, str]] = {
        "condition_occurrence": (
            "condition_concept_id",
            "condition_start_date",
            "condition_end_date",
        ),
        "drug_exposure": ("drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date"),
        "procedure_occurrence": ("procedure_concept_id", "procedure_date", "procedure_date"),
        "measurement": ("measurement_concept_id", "measurement_date", "measurement_date"),
        "observation": ("observation_concept_id", "observation_date", "observation_date"),
        "visit_occurrence": ("visit_concept_id", "visit_start_date", "visit_end_date"),
        "device_exposure": (
            "device_concept_id",
            "device_exposure_start_date",
            "device_exposure_end_date",
        ),
        "drug_era": ("drug_concept_id", "drug_era_start_date", "drug_era_end_date"),
        "condition_era": (
            "condition_concept_id",
            "condition_era_start_date",
            "condition_era_end_date",
        ),
        "specimen": ("specimen_concept_id", "specimen_date", "specimen_date"),
        "visit_detail": (
            "visit_detail_concept_id",
            "visit_detail_start_date",
            "visit_detail_end_date",
        ),
    }

    def _process_table(
        table_name: str,
        analysis_type: str,  # "event" or "episode"
        rid: int,
    ) -> list[dict[str, Any]]:
        """Process a single domain table for LSC."""
        rows: list[dict[str, Any]] = []

        if table_name not in _DOMAIN_DATE_COLS:
            return rows

        concept_col, start_col, end_col = _DOMAIN_DATE_COLS[table_name]

        try:
            domain_df = cdm[table_name].collect()
        except KeyError, Exception:
            return rows

        if domain_df is None or len(domain_df) == 0:
            return rows

        # For events, use only start_date (point-in-time)
        if analysis_type == "event":
            domain_df = domain_df.select(
                "person_id",
                pl.col(concept_col).alias("concept_id"),
                pl.col(start_col).alias("event_date"),
            )
        else:
            domain_df = domain_df.select(
                "person_id",
                pl.col(concept_col).alias("concept_id"),
                pl.col(start_col).alias("event_start"),
                pl.col(end_col).alias("event_end"),
            )

        # Filter out excluded codes
        if excluded_codes:
            domain_df = domain_df.filter(~pl.col("concept_id").is_in(excluded_codes))

        # Concept name lookup
        concept_names = dict(
            zip(
                concept_df["concept_id"].to_list(),
                concept_df["concept_name"].to_list(),
            )
        )

        # Process each cohort
        for cname in sorted(id_to_name.values()):
            c_df = cohort_df.filter(pl.col("cohort_name") == cname)
            if len(c_df) == 0:
                continue

            # Join cohort with domain table on person_id
            c_slim = c_df.select(
                pl.col("subject_id").alias("person_id"),
                pl.col(index_date).alias("_index_date"),
            )

            merged = c_slim.join(domain_df, on="person_id")

            for win in window:
                win_lower, win_upper = win
                win_name = _window_name(win)

                # Filter by time window
                if analysis_type == "event":
                    win_df = merged.with_columns(
                        (pl.col("event_date") - pl.col("_index_date"))
                        .dt.total_days()
                        .alias("_diff")
                    )
                    win_df = _filter_window(win_df, "_diff", win_lower, win_upper)
                else:
                    # Episode: interval overlap
                    win_df = merged.with_columns(
                        (pl.col("event_start") - pl.col("_index_date"))
                        .dt.total_days()
                        .alias("_diff_start"),
                        (pl.col("event_end") - pl.col("_index_date"))
                        .dt.total_days()
                        .alias("_diff_end"),
                    )
                    win_df = _filter_episode_window(
                        win_df, "_diff_start", "_diff_end", win_lower, win_upper
                    )

                if len(win_df) == 0:
                    continue

                # Denominator: distinct subjects in cohort
                n_subjects = c_df["subject_id"].n_unique()

                # Count per concept
                concept_counts = win_df.group_by("concept_id").agg(
                    pl.col("person_id").n_unique().alias("n")
                )

                for cc_row in concept_counts.iter_rows(named=True):
                    cid = cc_row["concept_id"]
                    count = cc_row["n"]
                    freq = count / n_subjects if n_subjects > 0 else 0.0

                    if freq < minimum_frequency:
                        continue

                    c_name_display = concept_names.get(cid, f"concept_{cid}")
                    pct = freq * 100.0

                    strata_groups = (
                        _resolve_strata(c_df, strata) if strata else [(OVERALL, OVERALL, c_df)]
                    )

                    for sname, slevel, _ in strata_groups:
                        # For strata, recompute with filtered cohort
                        # (simplified: overall only for now)
                        rows.append(
                            {
                                "result_id": rid,
                                "cdm_name": cdm_name,
                                "group_name": "cohort_name",
                                "group_level": cname,
                                "strata_name": sname,
                                "strata_level": slevel,
                                "variable_name": c_name_display,
                                "variable_level": win_name,
                                "estimate_name": "count",
                                "estimate_type": "integer",
                                "estimate_value": str(count),
                                "additional_name": "concept_id",
                                "additional_level": str(cid),
                            }
                        )
                        rows.append(
                            {
                                "result_id": rid,
                                "cdm_name": cdm_name,
                                "group_name": "cohort_name",
                                "group_level": cname,
                                "strata_name": sname,
                                "strata_level": slevel,
                                "variable_name": c_name_display,
                                "variable_level": win_name,
                                "estimate_name": "percentage",
                                "estimate_type": "percentage",
                                "estimate_value": f"{pct:.2f}",
                                "additional_name": "concept_id",
                                "additional_level": str(cid),
                            }
                        )

        return rows

    # Process event tables
    if event_in_window:
        for table_name in event_in_window:
            rows = _process_table(table_name, "event", next_result_id)
            if rows:
                all_rows.extend(rows)
                result_ids.append(next_result_id)
                settings_rows.append(
                    {
                        "result_id": next_result_id,
                        "result_type": "summarise_large_scale_characteristics",
                        "package_name": _PACKAGE_NAME,
                        "package_version": _PACKAGE_VERSION,
                        "table_name": table_name,
                        "type": "event",
                        "analysis": "standard",
                    }
                )
                next_result_id += 1

    # Process episode tables
    if episode_in_window:
        for table_name in episode_in_window:
            rows = _process_table(table_name, "episode", next_result_id)
            if rows:
                all_rows.extend(rows)
                result_ids.append(next_result_id)
                settings_rows.append(
                    {
                        "result_id": next_result_id,
                        "result_type": "summarise_large_scale_characteristics",
                        "package_name": _PACKAGE_NAME,
                        "package_version": _PACKAGE_VERSION,
                        "table_name": table_name,
                        "type": "episode",
                        "analysis": "standard",
                    }
                )
                next_result_id += 1

    if not all_rows:
        return _empty_result("summarise_large_scale_characteristics")

    data = pl.DataFrame(all_rows)
    settings_df = pl.DataFrame(settings_rows)
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# summarise_cohort_codelist
# ===================================================================


def summarise_cohort_codelist(
    cohort: CohortTable,
    *,
    cohort_id: list[int] | None = None,
) -> SummarisedResult:
    """Summarise the codelist used to define each cohort.

    Parameters
    ----------
    cohort
        A CohortTable with codelist metadata.
    cohort_id
        Restrict to specific cohort definition IDs. ``None`` = all.

    Returns
    -------
    SummarisedResult
        With ``result_type="summarise_cohort_codelist"``,
        ``strata_name="codelist_name &&& codelist_type"``.
    """
    from omopy.profiles._utilities import filter_cohort_id

    working = filter_cohort_id(cohort, cohort_id)
    codelist_df = working.cohort_codelist
    settings_meta = _filter_settings_by_cohort_id(working.settings, cohort_id)
    cdm = working.cdm
    cdm_name = cdm.cdm_name if cdm else "unknown"

    if codelist_df is None or len(codelist_df) == 0:
        return _empty_result("summarise_cohort_codelist")

    id_to_name = dict(
        zip(
            settings_meta["cohort_definition_id"].to_list(),
            settings_meta["cohort_name"].to_list(),
        )
    )

    # Try to get concept names
    concept_names: dict[int, str] = {}
    if cdm is not None:
        try:
            concept_df = cdm["concept"].collect()
            concept_names = dict(
                zip(
                    concept_df["concept_id"].to_list(),
                    concept_df["concept_name"].to_list(),
                )
            )
        except Exception:
            pass

    all_rows: list[dict[str, Any]] = []
    result_id = 1

    for row in codelist_df.iter_rows(named=True):
        cid = row["cohort_definition_id"]
        cname = id_to_name.get(cid, str(cid))
        codelist_name = str(row.get("codelist_name", OVERALL))
        codelist_type = str(row.get("codelist_type", "index event"))
        concept_id = int(row.get("concept_id", 0))

        concept_name_str = concept_names.get(concept_id, f"concept_{concept_id}")

        all_rows.append(
            {
                "result_id": result_id,
                "cdm_name": cdm_name,
                "group_name": "cohort_name",
                "group_level": cname,
                "strata_name": "codelist_name" + NAME_LEVEL_SEP + "codelist_type",
                "strata_level": codelist_name + NAME_LEVEL_SEP + codelist_type,
                "variable_name": OVERALL,
                "variable_level": OVERALL,
                "estimate_name": "concept_id",
                "estimate_type": "integer",
                "estimate_value": str(concept_id),
                "additional_name": "concept_name",
                "additional_level": concept_name_str,
            }
        )

    if not all_rows:
        return _empty_result("summarise_cohort_codelist")

    data = pl.DataFrame(all_rows)
    settings_df = _make_settings(result_id, "summarise_cohort_codelist")
    return SummarisedResult(data, settings=settings_df)


# ===================================================================
# Helpers
# ===================================================================


def _empty_result(result_type: str) -> SummarisedResult:
    """Create an empty SummarisedResult with the correct schema."""
    from omopy.generics.summarised_result import SUMMARISED_RESULT_COLUMNS

    data = pl.DataFrame({col: pl.Series([], dtype=pl.Utf8) for col in SUMMARISED_RESULT_COLUMNS})
    # result_id needs to be numeric
    data = data.with_columns(pl.col("result_id").cast(pl.Int64))
    settings = _make_settings(1, result_type)
    return SummarisedResult(data, settings=settings)


def _filter_settings_by_cohort_id(
    settings: pl.DataFrame,
    cohort_id: list[int] | None,
) -> pl.DataFrame:
    """Filter settings to only include requested cohort IDs."""
    if cohort_id is not None:
        settings = settings.filter(pl.col("cohort_definition_id").is_in(cohort_id))
    return settings


def _flatten_strata(strata: list[str | list[str]]) -> list[str]:
    """Flatten strata specification to a flat list of column names."""
    result: list[str] = []
    for s in strata:
        if isinstance(s, str):
            if s not in result:
                result.append(s)
        else:
            for c in s:
                if c not in result:
                    result.append(c)
    return result


def _window_name(window: Window) -> str:
    """Convert a window tuple to a human-readable string."""
    lower, upper = window
    if lower == -math.inf:
        lower_str = "-Inf"
    else:
        lower_str = str(int(lower))
    if upper == math.inf:
        upper_str = "Inf"
    else:
        upper_str = str(int(upper))
    return f"{lower_str} to {upper_str}"


def _filter_window(
    df: pl.DataFrame,
    diff_col: str,
    lower: float,
    upper: float,
) -> pl.DataFrame:
    """Filter DataFrame rows where diff_col is within [lower, upper]."""
    exprs = []
    if lower != -math.inf:
        exprs.append(pl.col(diff_col) >= lower)
    if upper != math.inf:
        exprs.append(pl.col(diff_col) <= upper)

    if not exprs:
        return df

    combined = exprs[0]
    for e in exprs[1:]:
        combined = combined & e
    return df.filter(combined)


def _filter_episode_window(
    df: pl.DataFrame,
    start_col: str,
    end_col: str,
    lower: float,
    upper: float,
) -> pl.DataFrame:
    """Filter episodes that overlap with [lower, upper] window."""
    # An episode [start, end] overlaps [lower, upper] iff start <= upper AND end >= lower
    exprs = []
    if upper != math.inf:
        exprs.append(pl.col(start_col) <= upper)
    if lower != -math.inf:
        exprs.append(pl.col(end_col) >= lower)

    if not exprs:
        return df

    combined = exprs[0]
    for e in exprs[1:]:
        combined = combined & e
    return df.filter(combined)
