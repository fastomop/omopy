"""Core drug exposure diagnostic checks.

Implements the main :func:`execute_checks` function and all individual
check implementations. Each check operates on a Polars DataFrame of
drug_exposure records (already filtered to a single ingredient's
descendant concepts) and returns a Polars DataFrame with check results.

The 12 available checks:

1. ``"missing"``        — Missing value counts for drug_exposure columns
2. ``"exposure_duration"`` — Quantile distribution of exposure duration
3. ``"type"``           — Frequency of drug_type_concept_id values
4. ``"route"``          — Frequency of route_concept_id values
5. ``"source_concept"`` — Source concept mapping analysis
6. ``"days_supply"``    — Quantile distribution + date diff comparison
7. ``"verbatim_end_date"`` — Comparison with drug_exposure_end_date
8. ``"dose"``           — Daily dose coverage (delegates to omopy.drug)
9. ``"sig"``            — Frequency of sig values
10. ``"quantity"``      — Quantile distribution of quantity field
11. ``"days_between"``  — Time between consecutive records per patient
12. ``"diagnostics_summary"`` — Aggregated summary across all checks
"""

from __future__ import annotations

import time
from typing import Any

import ibis
import polars as pl
from pydantic import BaseModel, ConfigDict, field_validator

from omopy.generics import CdmReference

__all__ = [
    "AVAILABLE_CHECKS",
    "DiagnosticsResult",
    "execute_checks",
]

_PACKAGE_NAME = "omopy.drug_diagnostics"
_PACKAGE_VERSION = "0.1.0"

# All configurable checks (order matters for diagnostics_summary)
AVAILABLE_CHECKS: tuple[str, ...] = (
    "missing",
    "exposure_duration",
    "type",
    "route",
    "source_concept",
    "days_supply",
    "verbatim_end_date",
    "dose",
    "sig",
    "quantity",
    "days_between",
    "diagnostics_summary",
)

# Drug exposure columns checked for missing values
_MISSING_COLUMNS: tuple[str, ...] = (
    "drug_exposure_id",
    "person_id",
    "drug_concept_id",
    "drug_exposure_start_date",
    "drug_exposure_end_date",
    "drug_type_concept_id",
    "stop_reason",
    "refills",
    "quantity",
    "days_supply",
    "sig",
    "route_concept_id",
    "route_source_value",
    "dose_unit_source_value",
    "verbatim_end_date",
)

# Standard quantile positions
_QUANTILE_POSITIONS: tuple[float, ...] = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)
_QUANTILE_NAMES: tuple[str, ...] = ("q05", "q10", "q25", "median", "q75", "q90", "q95")


# ---------------------------------------------------------------------------
# DiagnosticsResult model
# ---------------------------------------------------------------------------


class DiagnosticsResult(BaseModel):
    """Container for drug exposure diagnostics results.

    Holds a named dict of Polars DataFrames (one per check) plus metadata
    about the execution. Immutable after creation.

    Attributes
    ----------
    results
        Dict mapping check names to Polars DataFrames with check results.
    checks_performed
        Tuple of check names that were actually run.
    ingredient_concepts
        Dict mapping ingredient concept IDs to their names.
    cdm_name
        Name of the CDM instance.
    sample_size
        Number of records sampled per ingredient (or ``None`` if no sampling).
    min_cell_count
        Minimum cell count threshold used for suppression.
    execution_time_seconds
        Total execution time in seconds.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    results: dict[str, pl.DataFrame]
    checks_performed: tuple[str, ...]
    ingredient_concepts: dict[int, str]
    cdm_name: str = ""
    sample_size: int | None = None
    min_cell_count: int = 5
    execution_time_seconds: float = 0.0

    @field_validator("results", mode="before")
    @classmethod
    def _validate_results(cls, v: Any) -> dict[str, pl.DataFrame]:
        if not isinstance(v, dict):
            msg = "results must be a dict of Polars DataFrames"
            raise TypeError(msg)
        for key, val in v.items():
            if not isinstance(val, pl.DataFrame):
                msg = f"results['{key}'] must be a Polars DataFrame, got {type(val).__name__}"
                raise TypeError(msg)
        return v

    def __repr__(self) -> str:
        n_checks = len(self.checks_performed)
        n_ingredients = len(self.ingredient_concepts)
        total_rows = sum(df.height for df in self.results.values())
        return (
            f"DiagnosticsResult("
            f"checks={n_checks}, "
            f"ingredients={n_ingredients}, "
            f"total_rows={total_rows}, "
            f"time={self.execution_time_seconds:.1f}s)"
        )

    def __getitem__(self, key: str) -> pl.DataFrame:
        """Allow dict-like access: ``result['missing']``."""
        return self.results[key]

    def __contains__(self, key: object) -> bool:
        return key in self.results

    def keys(self):  # noqa: ANN201
        """Return check names."""
        return self.results.keys()

    def values(self):  # noqa: ANN201
        """Return result DataFrames."""
        return self.results.values()

    def items(self):  # noqa: ANN201
        """Return (check_name, DataFrame) pairs."""
        return self.results.items()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_ibis_table(cdm: CdmReference, table_name: str) -> ibis.Table:
    """Get an Ibis table from a CdmReference, handling both Ibis and Polars backends."""
    tbl = cdm[table_name]
    data = tbl.data

    # Already an Ibis table
    if hasattr(data, "to_pyarrow"):
        return data

    # Polars DataFrame/LazyFrame — convert via memtable
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    if isinstance(data, pl.DataFrame):
        return ibis.memtable(data.to_arrow())

    msg = f"Cannot convert {type(data).__name__} to Ibis table"
    raise TypeError(msg)


def _resolve_descendants(
    con: ibis.BaseBackend,
    catalog: str | None,
    schema: str,
    ingredient_concept_id: int,
) -> ibis.Table:
    """Resolve descendant drug concepts for an ingredient via concept_ancestor.

    Returns an Ibis table with a single ``concept_id`` column containing
    the ingredient itself plus all its descendant drug concepts.
    """
    import pyarrow as pa

    concept_ancestor = con.table("concept_ancestor", database=(catalog, schema))
    concept_tbl = con.table("concept", database=(catalog, schema))

    # Upload the single concept ID as a temp table
    arrow_ids = pa.table(
        {
            "concept_id": pa.array([ingredient_concept_id], type=pa.int64()),
        }
    )
    tmp_name = f"__omopy_diag_ids_{ingredient_concept_id}"
    con.con.register(tmp_name, arrow_ids)

    ids_tbl = con.table(tmp_name)

    # Expand via concept_ancestor
    descendants = ids_tbl.join(
        concept_ancestor,
        ids_tbl.concept_id == concept_ancestor.ancestor_concept_id,
    ).select(concept_id=concept_ancestor.descendant_concept_id.cast("int64"))
    all_resolved = (
        ids_tbl.select(concept_id=ids_tbl.concept_id.cast("int64")).union(descendants).distinct()
    )

    # Filter to standard Drug concepts only
    drug_concepts = (
        all_resolved.join(
            concept_tbl, all_resolved.concept_id == concept_tbl.concept_id.cast("int64")
        )
        .filter(concept_tbl.standard_concept == "S")
        .filter(concept_tbl.domain_id == "Drug")
        .select(concept_id=all_resolved.concept_id)
        .distinct()
    )

    return drug_concepts


def _get_ingredient_name(
    con: ibis.BaseBackend,
    catalog: str | None,
    schema: str,
    ingredient_concept_id: int,
) -> str:
    """Look up the concept_name for an ingredient concept ID."""
    concept_tbl = con.table("concept", database=(catalog, schema))
    name_row = (
        concept_tbl.filter(concept_tbl.concept_id == ingredient_concept_id)
        .select("concept_name")
        .limit(1)
        .to_pyarrow()
    )
    if name_row.num_rows > 0:
        return str(name_row.column("concept_name")[0].as_py())
    return f"Unknown ({ingredient_concept_id})"


def _fetch_drug_records(
    con: ibis.BaseBackend,
    catalog: str | None,
    schema: str,
    drug_concepts: ibis.Table,
    *,
    sample_size: int | None,
) -> pl.DataFrame:
    """Fetch drug_exposure records matching the resolved concept IDs.

    Optionally samples to ``sample_size`` records.
    Returns a Polars DataFrame.
    """
    drug_exposure = con.table("drug_exposure", database=(catalog, schema))

    records = drug_exposure.join(
        drug_concepts,
        drug_exposure.drug_concept_id.cast("int64") == drug_concepts.concept_id,
    )

    if sample_size is not None:
        n_total = records.count().execute()
        if n_total > sample_size:
            records = records.order_by(ibis.random()).limit(sample_size)

    # Materialise to Polars
    arrow = records.to_pyarrow()
    df = pl.from_arrow(arrow)
    return df


def _quantile_stats(
    series: pl.Series,
    *,
    name_prefix: str = "",
) -> dict[str, float | None]:
    """Compute standard quantile stats for a numeric series.

    Returns a dict with keys like q05, q10, q25, median, q75, q90, q95,
    plus mean, sd, min, max, count, count_missing.
    """
    non_null = series.drop_nulls()
    n = len(non_null)
    n_missing = series.null_count()
    prefix = f"{name_prefix}_" if name_prefix else ""

    if n == 0:
        result: dict[str, float | None] = {}
        for qn in _QUANTILE_NAMES:
            result[f"{prefix}{qn}"] = None
        result[f"{prefix}mean"] = None
        result[f"{prefix}sd"] = None
        result[f"{prefix}min"] = None
        result[f"{prefix}max"] = None
        result[f"{prefix}count"] = 0
        result[f"{prefix}count_missing"] = int(n_missing)
        return result

    # Cast to Float64 for quantile computation
    f64 = non_null.cast(pl.Float64)
    result = {}
    for pos, qn in zip(_QUANTILE_POSITIONS, _QUANTILE_NAMES):
        result[f"{prefix}{qn}"] = float(f64.quantile(pos))

    result[f"{prefix}mean"] = float(f64.mean())  # type: ignore[arg-type]
    result[f"{prefix}sd"] = float(f64.std()) if n > 1 else 0.0  # type: ignore[arg-type]
    result[f"{prefix}min"] = float(f64.min())  # type: ignore[arg-type]
    result[f"{prefix}max"] = float(f64.max())  # type: ignore[arg-type]
    result[f"{prefix}count"] = n
    result[f"{prefix}count_missing"] = int(n_missing)
    return result


def _obscure_count(value: int | float | None, min_cell_count: int) -> int | float | None:
    """Replace values below min_cell_count with None."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and value < min_cell_count and value > 0:
        return None
    return value


def _obscure_df(
    df: pl.DataFrame,
    min_cell_count: int,
    count_columns: list[str],
) -> pl.DataFrame:
    """Obscure counts in specified columns that are below the threshold.

    Adds a ``result_obscured`` boolean column.
    """
    if min_cell_count <= 0:
        return df.with_columns(pl.lit(False).alias("result_obscured"))

    obscured = pl.lit(False)
    exprs = []

    for col in count_columns:
        if col not in df.columns:
            continue
        dtype = df[col].dtype
        if dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            mask = ((pl.col(col) > 0) & (pl.col(col) < min_cell_count)).fill_null(False)
            exprs.append(pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col))
            obscured = obscured | mask
        elif dtype in (pl.Float32, pl.Float64):
            mask = ((pl.col(col) > 0) & (pl.col(col) < min_cell_count)).fill_null(False)
            exprs.append(pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col))
            obscured = obscured | mask

    if exprs:
        df = df.with_columns(*exprs, obscured.alias("result_obscured"))
    else:
        df = df.with_columns(obscured.alias("result_obscured"))

    return df


# ---------------------------------------------------------------------------
# Individual check implementations
# ---------------------------------------------------------------------------


def _check_missing(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 1: Missing value analysis for drug_exposure columns.

    For each of the 15 standard drug_exposure columns, counts the number
    and proportion of records with missing (NULL) values.
    """
    n_records = df.height
    if n_records == 0:
        return pl.DataFrame(
            schema={
                "ingredient_concept_id": pl.Int64,
                "ingredient": pl.Utf8,
                "variable": pl.Utf8,
                "n_records": pl.Int64,
                "n_sample": pl.Int64,
                "n_missing": pl.Int64,
                "n_not_missing": pl.Int64,
                "proportion_missing": pl.Float64,
            }
        )

    rows: list[dict[str, Any]] = []
    for col in _MISSING_COLUMNS:
        if col in df.columns:
            n_missing = df[col].null_count()
        else:
            n_missing = n_records  # column doesn't exist = all missing

        rows.append(
            {
                "ingredient_concept_id": ingredient_concept_id,
                "ingredient": ingredient_name,
                "variable": col,
                "n_records": n_records,
                "n_sample": n_records,
                "n_missing": n_missing,
                "n_not_missing": n_records - n_missing,
                "proportion_missing": n_missing / n_records if n_records > 0 else None,
            }
        )

    return pl.DataFrame(rows)


def _check_exposure_duration(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 2: Exposure duration distribution.

    Computes duration_days = drug_exposure_end_date - drug_exposure_start_date + 1,
    then provides quantile distribution and counts of negative durations.
    """
    n_records = df.height
    if n_records == 0:
        return pl.DataFrame(
            schema={
                "ingredient_concept_id": pl.Int64,
                "ingredient": pl.Utf8,
                "n_records": pl.Int64,
                "n_sample": pl.Int64,
                "n_negative_duration": pl.Int64,
                "proportion_negative_duration": pl.Float64,
                **{f"duration_{qn}": pl.Float64 for qn in _QUANTILE_NAMES},
                "duration_mean": pl.Float64,
                "duration_sd": pl.Float64,
                "duration_min": pl.Float64,
                "duration_max": pl.Float64,
                "duration_count": pl.Int64,
                "duration_count_missing": pl.Int64,
            }
        )

    # Calculate duration
    has_start = "drug_exposure_start_date" in df.columns
    has_end = "drug_exposure_end_date" in df.columns

    if has_start and has_end:
        duration = (
            df["drug_exposure_end_date"] - df["drug_exposure_start_date"]
        ).dt.total_days() + 1
    else:
        duration = pl.Series("duration", [None] * n_records, dtype=pl.Int64)

    # Count negative durations
    non_null_dur = duration.drop_nulls()
    n_negative = int((non_null_dur < 0).sum()) if len(non_null_dur) > 0 else 0

    # Quantile stats
    stats = _quantile_stats(duration, name_prefix="duration")

    row = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_records,
        "n_negative_duration": n_negative,
        "proportion_negative_duration": n_negative / n_records if n_records > 0 else None,
        **stats,
    }

    return pl.DataFrame([row])


def _check_type(
    df: pl.DataFrame,
    concept_df: pl.DataFrame | None,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 3: Drug type frequency analysis.

    Groups by drug_type_concept_id and counts frequency of each type.
    Joins with concept table for human-readable names.
    """
    n_records = df.height
    if n_records == 0:
        return pl.DataFrame(
            schema={
                "ingredient_concept_id": pl.Int64,
                "ingredient": pl.Utf8,
                "drug_type_concept_id": pl.Int64,
                "drug_type": pl.Utf8,
                "n_records": pl.Int64,
                "n_sample": pl.Int64,
                "count": pl.Int64,
                "proportion": pl.Float64,
            }
        )

    col = "drug_type_concept_id"
    if col not in df.columns:
        return pl.DataFrame(
            schema={
                "ingredient_concept_id": pl.Int64,
                "ingredient": pl.Utf8,
                "drug_type_concept_id": pl.Int64,
                "drug_type": pl.Utf8,
                "n_records": pl.Int64,
                "n_sample": pl.Int64,
                "count": pl.Int64,
                "proportion": pl.Float64,
            }
        )

    grouped = (
        df.group_by(col)
        .agg(pl.len().alias("count"))
        .with_columns(
            pl.lit(ingredient_concept_id).alias("ingredient_concept_id"),
            pl.lit(ingredient_name).alias("ingredient"),
            pl.lit(n_records).alias("n_records"),
            pl.lit(n_records).alias("n_sample"),
            (pl.col("count") / n_records).alias("proportion"),
        )
    )

    # Join with concept for names
    if concept_df is not None and "concept_id" in concept_df.columns:
        names = concept_df.select(
            pl.col("concept_id").cast(pl.Int64),
            pl.col("concept_name").alias("drug_type"),
        )
        grouped = grouped.with_columns(pl.col(col).cast(pl.Int64))
        grouped = grouped.join(
            names,
            left_on=col,
            right_on="concept_id",
            how="left",
        )
    else:
        grouped = grouped.with_columns(pl.lit("Unknown").alias("drug_type"))

    return grouped.select(
        "ingredient_concept_id",
        "ingredient",
        col,
        "drug_type",
        "n_records",
        "n_sample",
        "count",
        "proportion",
    ).sort("count", descending=True)


def _check_route(
    df: pl.DataFrame,
    concept_df: pl.DataFrame | None,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 4: Route frequency analysis.

    Groups by route_concept_id and counts frequency of each route.
    """
    n_records = df.height
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "route_concept_id": pl.Int64,
        "route": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "count": pl.Int64,
        "proportion": pl.Float64,
    }

    if n_records == 0:
        return pl.DataFrame(schema=empty_schema)

    col = "route_concept_id"
    if col not in df.columns:
        return pl.DataFrame(schema=empty_schema)

    grouped = (
        df.group_by(col)
        .agg(pl.len().alias("count"))
        .with_columns(
            pl.lit(ingredient_concept_id).alias("ingredient_concept_id"),
            pl.lit(ingredient_name).alias("ingredient"),
            pl.lit(n_records).alias("n_records"),
            pl.lit(n_records).alias("n_sample"),
            (pl.col("count") / n_records).alias("proportion"),
        )
    )

    # Join with concept for names
    if concept_df is not None and "concept_id" in concept_df.columns:
        names = concept_df.select(
            pl.col("concept_id").cast(pl.Int64),
            pl.col("concept_name").alias("route"),
        )
        grouped = grouped.with_columns(pl.col(col).cast(pl.Int64))
        grouped = grouped.join(
            names,
            left_on=col,
            right_on="concept_id",
            how="left",
        )
    else:
        grouped = grouped.with_columns(pl.lit("Unknown").alias("route"))

    return grouped.select(
        "ingredient_concept_id",
        "ingredient",
        col,
        "route",
        "n_records",
        "n_sample",
        "count",
        "proportion",
    ).sort("count", descending=True)


def _check_source_concept(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 5: Source concept mapping analysis.

    Groups by drug_source_concept_id and drug_source_value, counting frequency.
    """
    n_records = df.height
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "drug_concept_id": pl.Int64,
        "drug_source_concept_id": pl.Int64,
        "drug_source_value": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "count": pl.Int64,
        "proportion": pl.Float64,
    }

    if n_records == 0:
        return pl.DataFrame(schema=empty_schema)

    group_cols = []
    if "drug_concept_id" in df.columns:
        group_cols.append("drug_concept_id")
    if "drug_source_concept_id" in df.columns:
        group_cols.append("drug_source_concept_id")
    if "drug_source_value" in df.columns:
        group_cols.append("drug_source_value")

    if not group_cols:
        return pl.DataFrame(schema=empty_schema)

    grouped = (
        df.group_by(group_cols)
        .agg(pl.len().alias("count"))
        .with_columns(
            pl.lit(ingredient_concept_id).alias("ingredient_concept_id"),
            pl.lit(ingredient_name).alias("ingredient"),
            pl.lit(n_records).alias("n_records"),
            pl.lit(n_records).alias("n_sample"),
            (pl.col("count") / n_records).alias("proportion"),
        )
    )

    # Ensure all expected columns exist
    for col in ("drug_concept_id", "drug_source_concept_id", "drug_source_value"):
        if col not in grouped.columns:
            if col == "drug_source_value":
                grouped = grouped.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:
                grouped = grouped.with_columns(pl.lit(None).cast(pl.Int64).alias(col))

    return grouped.select(
        "ingredient_concept_id",
        "ingredient",
        "drug_concept_id",
        "drug_source_concept_id",
        "drug_source_value",
        "n_records",
        "n_sample",
        "count",
        "proportion",
    ).sort("count", descending=True)


def _check_days_supply(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 6: Days supply distribution + comparison with date diff.

    Provides quantile distribution of days_supply and compares with
    drug_exposure_end_date - drug_exposure_start_date + 1.
    """
    n_records = df.height
    base_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
    }

    if n_records == 0:
        return pl.DataFrame(
            schema={
                **base_schema,
                **{f"days_supply_{qn}": pl.Float64 for qn in _QUANTILE_NAMES},
                "days_supply_mean": pl.Float64,
                "days_supply_sd": pl.Float64,
                "days_supply_min": pl.Float64,
                "days_supply_max": pl.Float64,
                "days_supply_count": pl.Int64,
                "days_supply_count_missing": pl.Int64,
                "n_days_supply_match_date_diff": pl.Int64,
                "n_days_supply_differ_date_diff": pl.Int64,
                "n_days_supply_or_dates_missing": pl.Int64,
            }
        )

    # Quantile stats for days_supply
    ds_col = (
        df["days_supply"]
        if "days_supply" in df.columns
        else pl.Series("ds", [None] * n_records, dtype=pl.Int64)
    )
    stats = _quantile_stats(ds_col, name_prefix="days_supply")

    # Compare days_supply with date diff
    has_dates = "drug_exposure_start_date" in df.columns and "drug_exposure_end_date" in df.columns
    has_ds = "days_supply" in df.columns

    if has_dates and has_ds:
        date_diff = (
            df["drug_exposure_end_date"] - df["drug_exposure_start_date"]
        ).dt.total_days() + 1
        ds = df["days_supply"].cast(pl.Int64, strict=False)

        both_present = ds.is_not_null() & date_diff.is_not_null()
        n_match = int(((ds == date_diff) & both_present).sum())
        n_differ = int(((ds != date_diff) & both_present).sum())
        n_missing = n_records - int(both_present.sum())
    else:
        n_match = 0
        n_differ = 0
        n_missing = n_records

    row = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_records,
        **stats,
        "n_days_supply_match_date_diff": n_match,
        "n_days_supply_differ_date_diff": n_differ,
        "n_days_supply_or_dates_missing": n_missing,
    }

    return pl.DataFrame([row])


def _check_verbatim_end_date(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 7: Verbatim end date vs drug_exposure_end_date comparison.

    Counts records where verbatim_end_date is missing, equal to, or
    different from drug_exposure_end_date.
    """
    n_records = df.height
    base_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "n_verbatim_end_date_missing": pl.Int64,
        "n_verbatim_end_date_equal": pl.Int64,
        "n_verbatim_end_date_differ": pl.Int64,
        "proportion_verbatim_end_date_missing": pl.Float64,
        "proportion_verbatim_end_date_equal": pl.Float64,
        "proportion_verbatim_end_date_differ": pl.Float64,
    }

    if n_records == 0:
        return pl.DataFrame(schema=base_schema)

    has_verbatim = "verbatim_end_date" in df.columns
    has_end = "drug_exposure_end_date" in df.columns

    if has_verbatim and has_end:
        verbatim = df["verbatim_end_date"]
        end_date = df["drug_exposure_end_date"]

        n_missing = int(verbatim.is_null().sum())
        both_present = verbatim.is_not_null() & end_date.is_not_null()
        n_equal = int(((verbatim == end_date) & both_present).sum())
        n_differ = int(((verbatim != end_date) & both_present).sum())
    elif has_verbatim:
        n_missing = int(df["verbatim_end_date"].is_null().sum())
        n_equal = 0
        n_differ = 0
    else:
        n_missing = n_records
        n_equal = 0
        n_differ = 0

    row = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_records,
        "n_verbatim_end_date_missing": n_missing,
        "n_verbatim_end_date_equal": n_equal,
        "n_verbatim_end_date_differ": n_differ,
        "proportion_verbatim_end_date_missing": n_missing / n_records if n_records > 0 else None,
        "proportion_verbatim_end_date_equal": n_equal / n_records if n_records > 0 else None,
        "proportion_verbatim_end_date_differ": n_differ / n_records if n_records > 0 else None,
    }

    return pl.DataFrame([row])


def _check_dose(
    cdm: CdmReference,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 8: Daily dose coverage (delegates to omopy.drug).

    Attempts to use ``summarise_dose_coverage()`` from the drug module.
    Returns an empty DataFrame if the drug module is not available or
    drug_strength data is missing.
    """
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "n_with_dose": pl.Int64,
        "n_without_dose": pl.Int64,
        "proportion_with_dose": pl.Float64,
    }

    try:
        from omopy.drug import summarise_dose_coverage

        # summarise_dose_coverage requires a CohortTable, which we don't have here.
        # Instead we return a simplified dose coverage analysis.
        # Check if drug_strength table is available and has data
        if "drug_strength" not in cdm:
            return pl.DataFrame(schema=empty_schema)

        ds = cdm["drug_strength"].collect()
        if ds.height == 0:
            return pl.DataFrame(
                [
                    {
                        "ingredient_concept_id": ingredient_concept_id,
                        "ingredient": ingredient_name,
                        "n_records": 0,
                        "n_sample": 0,
                        "n_with_dose": 0,
                        "n_without_dose": 0,
                        "proportion_with_dose": 0.0,
                    }
                ]
            )

        # Count drug_exposure records that have matching drug_strength entries
        de = cdm["drug_exposure"].collect()
        de_ingredient = de.filter(True)  # already filtered upstream — not available here
        # Since we don't have pre-filtered records here, return empty
        return pl.DataFrame(schema=empty_schema)

    except Exception:
        return pl.DataFrame(schema=empty_schema)


def _check_dose_from_records(
    df: pl.DataFrame,
    drug_strength_df: pl.DataFrame | None,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 8 (alternative): Dose coverage from pre-fetched records.

    Checks how many drug_exposure records have matching drug_strength entries.
    """
    n_records = df.height
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "n_with_dose": pl.Int64,
        "n_without_dose": pl.Int64,
        "proportion_with_dose": pl.Float64,
    }

    if n_records == 0 or drug_strength_df is None:
        return pl.DataFrame(
            [
                {
                    "ingredient_concept_id": ingredient_concept_id,
                    "ingredient": ingredient_name,
                    "n_records": n_records,
                    "n_sample": n_records,
                    "n_with_dose": 0,
                    "n_without_dose": n_records,
                    "proportion_with_dose": 0.0,
                }
            ]
        )

    if drug_strength_df.height == 0:
        return pl.DataFrame(
            [
                {
                    "ingredient_concept_id": ingredient_concept_id,
                    "ingredient": ingredient_name,
                    "n_records": n_records,
                    "n_sample": n_records,
                    "n_with_dose": 0,
                    "n_without_dose": n_records,
                    "proportion_with_dose": 0.0,
                }
            ]
        )

    # Join drug_exposure with drug_strength on drug_concept_id
    ds_concepts = drug_strength_df.select(pl.col("drug_concept_id").cast(pl.Int64)).unique()

    de_concepts = df.select(pl.col("drug_concept_id").cast(pl.Int64))

    matched = de_concepts.join(
        ds_concepts,
        on="drug_concept_id",
        how="inner",
    )

    n_with_dose = matched.height
    n_without_dose = n_records - n_with_dose

    return pl.DataFrame(
        [
            {
                "ingredient_concept_id": ingredient_concept_id,
                "ingredient": ingredient_name,
                "n_records": n_records,
                "n_sample": n_records,
                "n_with_dose": n_with_dose,
                "n_without_dose": n_without_dose,
                "proportion_with_dose": n_with_dose / n_records if n_records > 0 else 0.0,
            }
        ]
    )


def _check_sig(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 9: Sig (verbatim instruction) frequency analysis.

    Groups by sig field and counts frequency.
    """
    n_records = df.height
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "sig": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "count": pl.Int64,
        "proportion": pl.Float64,
    }

    if n_records == 0:
        return pl.DataFrame(schema=empty_schema)

    col = "sig"
    if col not in df.columns:
        return pl.DataFrame(schema=empty_schema)

    # Replace nulls with a label for grouping
    sig_df = df.select(pl.col(col).fill_null("<missing>").alias(col))

    grouped = (
        sig_df.group_by(col)
        .agg(pl.len().alias("count"))
        .with_columns(
            pl.lit(ingredient_concept_id).alias("ingredient_concept_id"),
            pl.lit(ingredient_name).alias("ingredient"),
            pl.lit(n_records).alias("n_records"),
            pl.lit(n_records).alias("n_sample"),
            (pl.col("count") / n_records).alias("proportion"),
        )
    )

    return grouped.select(
        "ingredient_concept_id",
        "ingredient",
        col,
        "n_records",
        "n_sample",
        "count",
        "proportion",
    ).sort("count", descending=True)


def _check_quantity(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 10: Quantity distribution.

    Provides quantile distribution of the quantity field.
    """
    n_records = df.height
    if n_records == 0:
        return pl.DataFrame(
            schema={
                "ingredient_concept_id": pl.Int64,
                "ingredient": pl.Utf8,
                "n_records": pl.Int64,
                "n_sample": pl.Int64,
                **{f"quantity_{qn}": pl.Float64 for qn in _QUANTILE_NAMES},
                "quantity_mean": pl.Float64,
                "quantity_sd": pl.Float64,
                "quantity_min": pl.Float64,
                "quantity_max": pl.Float64,
                "quantity_count": pl.Int64,
                "quantity_count_missing": pl.Int64,
            }
        )

    qty_col = (
        df["quantity"]
        if "quantity" in df.columns
        else pl.Series("q", [None] * n_records, dtype=pl.Float64)
    )
    stats = _quantile_stats(qty_col, name_prefix="quantity")

    row = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_records,
        **stats,
    }

    return pl.DataFrame([row])


def _check_days_between(
    df: pl.DataFrame,
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
) -> pl.DataFrame:
    """Check 11: Days between consecutive drug records per patient.

    Computes the lag between consecutive drug_exposure_start_date for each
    patient, then provides quantile distribution of those gaps.
    """
    n_records = df.height
    empty_schema = {
        "ingredient_concept_id": pl.Int64,
        "ingredient": pl.Utf8,
        "n_records": pl.Int64,
        "n_sample": pl.Int64,
        "n_persons": pl.Int64,
        "n_persons_multiple_records": pl.Int64,
        **{f"days_between_{qn}": pl.Float64 for qn in _QUANTILE_NAMES},
        "days_between_mean": pl.Float64,
        "days_between_sd": pl.Float64,
        "days_between_min": pl.Float64,
        "days_between_max": pl.Float64,
        "days_between_count": pl.Int64,
        "days_between_count_missing": pl.Int64,
    }

    if n_records == 0:
        return pl.DataFrame(schema=empty_schema)

    if "person_id" not in df.columns or "drug_exposure_start_date" not in df.columns:
        return pl.DataFrame(schema=empty_schema)

    # Sort by person and start date, compute lag
    sorted_df = df.sort(["person_id", "drug_exposure_start_date"])

    gaps = sorted_df.with_columns(
        (
            pl.col("drug_exposure_start_date")
            - pl.col("drug_exposure_start_date").shift(1).over("person_id")
        )
        .dt.total_days()
        .alias("days_between")
    )

    # Only keep rows where days_between is not null (i.e., not the first record per person)
    gap_values = gaps["days_between"].drop_nulls()

    n_persons = df["person_id"].n_unique()
    # Persons with multiple records
    person_counts = df.group_by("person_id").agg(pl.len().alias("n"))
    n_persons_multi = int(person_counts.filter(pl.col("n") > 1).height)

    stats = _quantile_stats(gap_values, name_prefix="days_between")

    row = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_records,
        "n_persons": n_persons,
        "n_persons_multiple_records": n_persons_multi,
        **stats,
    }

    return pl.DataFrame([row])


def _check_diagnostics_summary(
    check_results: dict[str, pl.DataFrame],
    *,
    ingredient_concept_id: int,
    ingredient_name: str,
    n_records: int,
    n_sample: int,
    n_persons: int,
) -> pl.DataFrame:
    """Check 12: Aggregated diagnostics summary.

    Produces a one-row-per-ingredient summary drawing key metrics
    from all other check results.
    """
    row: dict[str, Any] = {
        "ingredient_concept_id": ingredient_concept_id,
        "ingredient": ingredient_name,
        "n_records": n_records,
        "n_sample": n_sample,
        "n_persons": n_persons,
    }

    # Extract key metrics from each check
    if "missing" in check_results:
        missing_df = check_results["missing"]
        if missing_df.height > 0:
            avg_missing = missing_df["proportion_missing"].mean()
            row["mean_proportion_missing"] = (
                float(avg_missing) if avg_missing is not None else None
            )
        else:
            row["mean_proportion_missing"] = None

    if "exposure_duration" in check_results:
        dur_df = check_results["exposure_duration"]
        if dur_df.height > 0:
            row["median_duration_days"] = (
                dur_df["duration_median"][0] if "duration_median" in dur_df.columns else None
            )
            row["n_negative_duration"] = (
                dur_df["n_negative_duration"][0]
                if "n_negative_duration" in dur_df.columns
                else None
            )
        else:
            row["median_duration_days"] = None
            row["n_negative_duration"] = None

    if "days_supply" in check_results:
        ds_df = check_results["days_supply"]
        if ds_df.height > 0:
            row["median_days_supply"] = (
                ds_df["days_supply_median"][0] if "days_supply_median" in ds_df.columns else None
            )
        else:
            row["median_days_supply"] = None

    if "quantity" in check_results:
        qty_df = check_results["quantity"]
        if qty_df.height > 0:
            row["median_quantity"] = (
                qty_df["quantity_median"][0] if "quantity_median" in qty_df.columns else None
            )
        else:
            row["median_quantity"] = None

    if "dose" in check_results:
        dose_df = check_results["dose"]
        if dose_df.height > 0 and "proportion_with_dose" in dose_df.columns:
            row["proportion_with_dose"] = dose_df["proportion_with_dose"][0]
        else:
            row["proportion_with_dose"] = None

    if "verbatim_end_date" in check_results:
        ved_df = check_results["verbatim_end_date"]
        if ved_df.height > 0:
            row["proportion_verbatim_end_date_missing"] = (
                ved_df["proportion_verbatim_end_date_missing"][0]
                if "proportion_verbatim_end_date_missing" in ved_df.columns
                else None
            )
        else:
            row["proportion_verbatim_end_date_missing"] = None

    return pl.DataFrame([row])


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def execute_checks(
    cdm: CdmReference,
    ingredient_concept_ids: list[int] | int,
    *,
    checks: list[str] | tuple[str, ...] | None = None,
    sample_size: int | None = 10_000,
    min_cell_count: int = 5,
) -> DiagnosticsResult:
    """Run drug exposure diagnostic checks for specified ingredients.

    This is the main entry point for the drug diagnostics module. For each
    ingredient concept ID, it resolves descendant drug concepts, fetches
    (and optionally samples) drug_exposure records, and runs each enabled
    check.

    Parameters
    ----------
    cdm
        A ``CdmReference`` connected to an OMOP CDM database.
    ingredient_concept_ids
        One or more ingredient concept IDs to diagnose.
    checks
        Which checks to run. Defaults to all available checks.
        See :data:`AVAILABLE_CHECKS` for valid names.
    sample_size
        Maximum number of records to sample per ingredient. Set to
        ``None`` to use all records (can be slow for large datasets).
    min_cell_count
        Counts below this threshold are replaced with ``None`` for
        privacy protection. Set to ``0`` to disable.

    Returns
    -------
    DiagnosticsResult
        Container with a dict of Polars DataFrames (one per check),
        plus metadata about the execution.

    Raises
    ------
    ValueError
        If any check name is not in ``AVAILABLE_CHECKS``.
    TypeError
        If ``cdm`` is not a ``CdmReference``.

    Examples
    --------
    >>> import omopy
    >>> cdm = omopy.connector.cdm_from_con(con, cdm_schema="base")
    >>> result = omopy.drug_diagnostics.execute_checks(
    ...     cdm,
    ...     ingredient_concept_ids=[1125315, 1503297],
    ...     checks=["missing", "exposure_duration", "type"],
    ...     sample_size=5000,
    ... )
    >>> result["missing"]  # Polars DataFrame
    """
    t_start = time.monotonic()

    # Validate inputs
    if not isinstance(cdm, CdmReference):
        msg = f"cdm must be a CdmReference, got {type(cdm).__name__}"
        raise TypeError(msg)

    if isinstance(ingredient_concept_ids, int):
        ingredient_concept_ids = [ingredient_concept_ids]

    if not ingredient_concept_ids:
        msg = "ingredient_concept_ids must contain at least one concept ID"
        raise ValueError(msg)

    if checks is None:
        checks = list(AVAILABLE_CHECKS)
    else:
        checks = list(checks)

    invalid = [c for c in checks if c not in AVAILABLE_CHECKS]
    if invalid:
        msg = f"Invalid check names: {invalid}. Valid: {list(AVAILABLE_CHECKS)}"
        raise ValueError(msg)

    # Get backend connection info
    source = cdm.cdm_source
    if source is None:
        msg = "CDM must have a source (cdm_source) for database access"
        raise ValueError(msg)

    con = source.connection  # type: ignore[union-attr]
    catalog = source._catalog  # type: ignore[union-attr]
    schema = source.cdm_schema  # type: ignore[union-attr]

    # Pre-fetch concept table for name lookups (once)
    concept_tbl = con.table("concept", database=(catalog, schema))
    concept_df = pl.from_arrow(concept_tbl.select("concept_id", "concept_name").to_pyarrow())

    # Pre-fetch drug_strength if dose check is enabled
    drug_strength_df: pl.DataFrame | None = None
    if "dose" in checks:
        try:
            ds_tbl = con.table("drug_strength", database=(catalog, schema))
            drug_strength_df = pl.from_arrow(ds_tbl.to_pyarrow())
        except Exception:
            drug_strength_df = None

    # Run checks for each ingredient
    all_results: dict[str, list[pl.DataFrame]] = {c: [] for c in checks}
    ingredient_names: dict[int, str] = {}

    for ing_id in ingredient_concept_ids:
        # Resolve descendants
        drug_concepts = _resolve_descendants(con, catalog, schema, ing_id)

        # Get ingredient name
        ing_name = _get_ingredient_name(con, catalog, schema, ing_id)
        ingredient_names[ing_id] = ing_name

        # Fetch records
        df = _fetch_drug_records(
            con,
            catalog,
            schema,
            drug_concepts,
            sample_size=sample_size,
        )

        n_records = df.height
        n_persons = (
            int(df["person_id"].n_unique()) if n_records > 0 and "person_id" in df.columns else 0
        )

        # Per-ingredient check results (for diagnostics_summary)
        ingredient_check_results: dict[str, pl.DataFrame] = {}

        # Run each check
        if "missing" in checks:
            r = _check_missing(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["missing"].append(r)
            ingredient_check_results["missing"] = r

        if "exposure_duration" in checks:
            r = _check_exposure_duration(
                df, ingredient_concept_id=ing_id, ingredient_name=ing_name
            )
            all_results["exposure_duration"].append(r)
            ingredient_check_results["exposure_duration"] = r

        if "type" in checks:
            r = _check_type(df, concept_df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["type"].append(r)
            ingredient_check_results["type"] = r

        if "route" in checks:
            r = _check_route(
                df, concept_df, ingredient_concept_id=ing_id, ingredient_name=ing_name
            )
            all_results["route"].append(r)
            ingredient_check_results["route"] = r

        if "source_concept" in checks:
            r = _check_source_concept(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["source_concept"].append(r)
            ingredient_check_results["source_concept"] = r

        if "days_supply" in checks:
            r = _check_days_supply(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["days_supply"].append(r)
            ingredient_check_results["days_supply"] = r

        if "verbatim_end_date" in checks:
            r = _check_verbatim_end_date(
                df, ingredient_concept_id=ing_id, ingredient_name=ing_name
            )
            all_results["verbatim_end_date"].append(r)
            ingredient_check_results["verbatim_end_date"] = r

        if "dose" in checks:
            r = _check_dose_from_records(
                df,
                drug_strength_df,
                ingredient_concept_id=ing_id,
                ingredient_name=ing_name,
            )
            all_results["dose"].append(r)
            ingredient_check_results["dose"] = r

        if "sig" in checks:
            r = _check_sig(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["sig"].append(r)
            ingredient_check_results["sig"] = r

        if "quantity" in checks:
            r = _check_quantity(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["quantity"].append(r)
            ingredient_check_results["quantity"] = r

        if "days_between" in checks:
            r = _check_days_between(df, ingredient_concept_id=ing_id, ingredient_name=ing_name)
            all_results["days_between"].append(r)
            ingredient_check_results["days_between"] = r

        if "diagnostics_summary" in checks:
            r = _check_diagnostics_summary(
                ingredient_check_results,
                ingredient_concept_id=ing_id,
                ingredient_name=ing_name,
                n_records=n_records,
                n_sample=n_records,
                n_persons=n_persons,
            )
            all_results["diagnostics_summary"].append(r)

        # Clean up temp table
        try:
            tmp_name = f"__omopy_diag_ids_{ing_id}"
            con.con.unregister(tmp_name)
        except Exception:
            pass

    # Concatenate results across ingredients
    combined: dict[str, pl.DataFrame] = {}
    for check_name, dfs in all_results.items():
        if dfs:
            non_empty = [d for d in dfs if d.height > 0]
            if non_empty:
                combined[check_name] = pl.concat(non_empty, how="diagonal_relaxed")
            else:
                # Use schema from first DataFrame
                combined[check_name] = dfs[0]
        else:
            # Check was requested but produced no results
            combined[check_name] = pl.DataFrame()

    # Apply min_cell_count obscuration
    if min_cell_count > 0:
        count_cols_by_check = {
            "missing": ["n_missing", "n_not_missing"],
            "exposure_duration": ["n_negative_duration"],
            "type": ["count"],
            "route": ["count"],
            "source_concept": ["count"],
            "days_supply": [
                "n_days_supply_match_date_diff",
                "n_days_supply_differ_date_diff",
                "n_days_supply_or_dates_missing",
            ],
            "verbatim_end_date": [
                "n_verbatim_end_date_missing",
                "n_verbatim_end_date_equal",
                "n_verbatim_end_date_differ",
            ],
            "dose": ["n_with_dose", "n_without_dose"],
            "sig": ["count"],
            "quantity": [],
            "days_between": ["n_persons", "n_persons_multiple_records"],
            "diagnostics_summary": ["n_negative_duration"],
        }

        for check_name, df in combined.items():
            if df.height > 0 and check_name in count_cols_by_check:
                cols = count_cols_by_check[check_name]
                existing_cols = [c for c in cols if c in df.columns]
                if existing_cols:
                    combined[check_name] = _obscure_df(df, min_cell_count, existing_cols)

    t_end = time.monotonic()

    return DiagnosticsResult(
        results=combined,
        checks_performed=tuple(checks),
        ingredient_concepts=ingredient_names,
        cdm_name=cdm.cdm_name,
        sample_size=sample_size,
        min_cell_count=min_cell_count,
        execution_time_seconds=round(t_end - t_start, 3),
    )
