"""Summarise drug diagnostics results into SummarisedResult format.

Converts the dict-of-DataFrames output from :func:`execute_checks` into
the standard 13-column :class:`~omopy.generics.SummarisedResult` format
used across OMOPy for interoperability with table and plot functions.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.drug_diagnostics._checks import DiagnosticsResult
from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import (
    SUMMARISED_RESULT_COLUMNS,
    SummarisedResult,
)

__all__ = ["summarise_drug_diagnostics"]

_PACKAGE_NAME = "omopy.drug_diagnostics"
_PACKAGE_VERSION = "0.1.0"

# Result types for each check
_RESULT_TYPE_PREFIX = "drug_diagnostics"


def _make_settings(
    result_id: int,
    check_name: str,
    *,
    sample_size: int | None,
    min_cell_count: int,
) -> pl.DataFrame:
    """Build a settings row for a check."""
    return pl.DataFrame(
        {
            "result_id": [result_id],
            "result_type": [f"{_RESULT_TYPE_PREFIX}_{check_name}"],
            "package_name": [_PACKAGE_NAME],
            "package_version": [_PACKAGE_VERSION],
            "sample_size": [str(sample_size) if sample_size is not None else "all"],
            "min_cell_count": [str(min_cell_count)],
        }
    )


def _rows_from_wide(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
    check_name: str,
    group_cols: list[str],
    metric_cols: list[str],
    additional_cols: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Convert a wide DataFrame into SummarisedResult rows.

    Each metric column becomes a (variable_name, estimate_name, estimate_value) triple.
    """
    rows: list[dict[str, Any]] = []

    for record in df.iter_rows(named=True):
        # Build group name/level
        group_parts_name = []
        group_parts_level = []
        for gc in group_cols:
            val = record.get(gc)
            if val is not None:
                group_parts_name.append(gc)
                group_parts_level.append(str(val))

        group_name = " &&& ".join(group_parts_name) if group_parts_name else OVERALL
        group_level = " &&& ".join(group_parts_level) if group_parts_level else OVERALL

        # Build additional name/level
        add_parts_name = []
        add_parts_level = []
        if additional_cols:
            for ac in additional_cols:
                val = record.get(ac)
                if val is not None:
                    add_parts_name.append(ac)
                    add_parts_level.append(str(val))

        additional_name = " &&& ".join(add_parts_name) if add_parts_name else OVERALL
        additional_level = " &&& ".join(add_parts_level) if add_parts_level else OVERALL

        # Each metric becomes a row
        for mc in metric_cols:
            val = record.get(mc)
            if val is None:
                est_value = "NA"
            elif isinstance(val, float):
                est_value = f"{val:.6f}" if abs(val) < 1e6 else f"{val:.2e}"
            else:
                est_value = str(val)

            # Determine estimate_type
            if isinstance(val, int):
                est_type = "integer"
            elif isinstance(val, float):
                est_type = "numeric"
            elif isinstance(val, bool):
                est_type = "logical"
            else:
                est_type = "character"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": check_name,
                    "variable_level": mc,
                    "estimate_name": mc,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": additional_name,
                    "additional_level": additional_level,
                }
            )

    return rows


def _summarise_missing(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
) -> list[dict[str, Any]]:
    """Convert missing check results into SummarisedResult rows."""
    rows: list[dict[str, Any]] = []

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")
        variable = record.get("variable", "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        for est_name in (
            "n_records",
            "n_missing",
            "n_not_missing",
            "proportion_missing",
        ):
            val = record.get(est_name)
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if est_name.startswith("n_") else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": "missing",
                    "variable_level": variable,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

    return rows


def _summarise_categorical(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
    check_name: str,
    category_col: str,
) -> list[dict[str, Any]]:
    """Convert categorical check results into rows.

    Handles type, route, sig, source_concept.
    """
    rows: list[dict[str, Any]] = []

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")
        category_val = record.get(category_col, "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        for est_name in ("count", "proportion"):
            val = record.get(est_name)
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if est_name == "count" else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": check_name,
                    "variable_level": str(category_val),
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

    return rows


def _summarise_quantile(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
    check_name: str,
    prefix: str,
) -> list[dict[str, Any]]:
    """Convert quantile check results into rows.

    Handles exposure_duration, days_supply, etc.
    """
    from omopy.drug_diagnostics._checks import _QUANTILE_NAMES

    rows: list[dict[str, Any]] = []
    stat_names = [
        *list(_QUANTILE_NAMES),
        "mean",
        "sd",
        "min",
        "max",
        "count",
        "count_missing",
    ]

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        for stat in stat_names:
            col = f"{prefix}_{stat}"
            val = record.get(col)
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if stat in ("count", "count_missing") else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": check_name,
                    "variable_level": stat,
                    "estimate_name": stat,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

        # Add extra columns specific to certain checks
        for extra in (
            "n_negative_duration",
            "proportion_negative_duration",
            "n_days_supply_match_date_diff",
            "n_days_supply_differ_date_diff",
            "n_days_supply_or_dates_missing",
            "n_persons",
            "n_persons_multiple_records",
        ):
            if extra in record:
                val = record[extra]
                est_value = "NA" if val is None else str(val)
                est_type = "integer" if extra.startswith("n_") else "numeric"
                rows.append(
                    {
                        "result_id": result_id,
                        "cdm_name": cdm_name,
                        "group_name": group_name,
                        "group_level": group_level,
                        "strata_name": OVERALL,
                        "strata_level": OVERALL,
                        "variable_name": check_name,
                        "variable_level": extra,
                        "estimate_name": extra,
                        "estimate_type": est_type,
                        "estimate_value": est_value,
                        "additional_name": OVERALL,
                        "additional_level": OVERALL,
                    }
                )

    return rows


def _summarise_verbatim_end_date(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
) -> list[dict[str, Any]]:
    """Convert verbatim_end_date check results into rows."""
    rows: list[dict[str, Any]] = []
    metrics = [
        "n_verbatim_end_date_missing",
        "n_verbatim_end_date_equal",
        "n_verbatim_end_date_differ",
        "proportion_verbatim_end_date_missing",
        "proportion_verbatim_end_date_equal",
        "proportion_verbatim_end_date_differ",
    ]

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        for est_name in metrics:
            val = record.get(est_name)
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if est_name.startswith("n_") else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": "verbatim_end_date",
                    "variable_level": est_name,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

    return rows


def _summarise_dose(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
) -> list[dict[str, Any]]:
    """Convert dose check results into rows."""
    rows: list[dict[str, Any]] = []
    metrics = [
        "n_records",
        "n_with_dose",
        "n_without_dose",
        "proportion_with_dose",
    ]

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        for est_name in metrics:
            val = record.get(est_name)
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if est_name.startswith("n_") else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": "dose",
                    "variable_level": est_name,
                    "estimate_name": est_name,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

    return rows


def _summarise_summary(
    df: pl.DataFrame,
    *,
    result_id: int,
    cdm_name: str,
) -> list[dict[str, Any]]:
    """Convert diagnostics_summary check results into rows."""
    rows: list[dict[str, Any]] = []

    for record in df.iter_rows(named=True):
        ing_id = record.get("ingredient_concept_id", "")
        ing_name = record.get("ingredient", "")

        group_name = "ingredient_concept_id &&& ingredient"
        group_level = f"{ing_id} &&& {ing_name}"

        # All columns except the group identifiers
        skip = {"ingredient_concept_id", "ingredient"}
        for col, val in record.items():
            if col in skip:
                continue
            est_value = "NA" if val is None else str(val)
            est_type = "integer" if isinstance(val, int) else "numeric"

            rows.append(
                {
                    "result_id": result_id,
                    "cdm_name": cdm_name,
                    "group_name": group_name,
                    "group_level": group_level,
                    "strata_name": OVERALL,
                    "strata_level": OVERALL,
                    "variable_name": "diagnostics_summary",
                    "variable_level": col,
                    "estimate_name": col,
                    "estimate_type": est_type,
                    "estimate_value": est_value,
                    "additional_name": OVERALL,
                    "additional_level": OVERALL,
                }
            )

    return rows


def summarise_drug_diagnostics(
    result: DiagnosticsResult,
) -> SummarisedResult:
    """Convert drug diagnostics results to SummarisedResult format.

    Transforms the dict-of-DataFrames output from :func:`execute_checks`
    into the standard 13-column :class:`~omopy.generics.SummarisedResult`
    format used by ``table_drug_diagnostics()`` and ``plot_drug_diagnostics()``.

    Parameters
    ----------
    result
        Output from :func:`execute_checks`.

    Returns
    -------
    SummarisedResult
        Standardised result with one ``result_id`` per check type.

    Examples
    --------
    >>> diag = omopy.drug_diagnostics.execute_checks(cdm, [1125315])
    >>> sr = omopy.drug_diagnostics.summarise_drug_diagnostics(diag)
    >>> sr.settings
    """
    if not isinstance(result, DiagnosticsResult):
        msg = f"Expected DiagnosticsResult, got {type(result).__name__}"
        raise TypeError(msg)

    cdm_name = result.cdm_name
    all_rows: list[dict[str, Any]] = []
    all_settings: list[pl.DataFrame] = []
    rid = 0

    # Map each check to its converter
    for check_name, df in result.results.items():
        rid += 1

        if df.height == 0:
            # Still create a settings entry
            all_settings.append(
                _make_settings(
                    rid,
                    check_name,
                    sample_size=result.sample_size,
                    min_cell_count=result.min_cell_count,
                )
            )
            continue

        # Route to appropriate converter
        if check_name == "missing":
            rows = _summarise_missing(df, result_id=rid, cdm_name=cdm_name)
        elif check_name == "exposure_duration":
            rows = _summarise_quantile(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="exposure_duration",
                prefix="duration",
            )
        elif check_name == "type":
            rows = _summarise_categorical(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="type",
                category_col="drug_type",
            )
        elif check_name == "route":
            rows = _summarise_categorical(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="route",
                category_col="route",
            )
        elif check_name == "source_concept":
            rows = _summarise_categorical(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="source_concept",
                category_col="drug_source_value",
            )
        elif check_name == "days_supply":
            rows = _summarise_quantile(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="days_supply",
                prefix="days_supply",
            )
        elif check_name == "verbatim_end_date":
            rows = _summarise_verbatim_end_date(df, result_id=rid, cdm_name=cdm_name)
        elif check_name == "dose":
            rows = _summarise_dose(df, result_id=rid, cdm_name=cdm_name)
        elif check_name == "sig":
            rows = _summarise_categorical(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="sig",
                category_col="sig",
            )
        elif check_name == "quantity":
            rows = _summarise_quantile(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="quantity",
                prefix="quantity",
            )
        elif check_name == "days_between":
            rows = _summarise_quantile(
                df,
                result_id=rid,
                cdm_name=cdm_name,
                check_name="days_between",
                prefix="days_between",
            )
        elif check_name == "diagnostics_summary":
            rows = _summarise_summary(df, result_id=rid, cdm_name=cdm_name)
        else:
            rows = []

        all_rows.extend(rows)
        all_settings.append(
            _make_settings(
                rid,
                check_name,
                sample_size=result.sample_size,
                min_cell_count=result.min_cell_count,
            )
        )

    # Build final SummarisedResult
    if all_rows:
        data = pl.DataFrame(all_rows)
    else:
        data = pl.DataFrame(schema={col: pl.Utf8 for col in SUMMARISED_RESULT_COLUMNS})
        data = data.cast({"result_id": pl.Int64})

    settings = (
        pl.concat(all_settings, how="diagonal_relaxed")
        if all_settings
        else pl.DataFrame(
            {
                "result_id": [],
                "result_type": [],
                "package_name": [],
                "package_version": [],
            }
        )
    )

    return SummarisedResult(data, settings=settings)
