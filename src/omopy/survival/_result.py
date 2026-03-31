"""Convert between SummarisedResult and wide-format survival result.

Implements ``as_survival_result()`` — converts a SummarisedResult
(long format with 13 columns) into a wide-format DataFrame with
separate estimates, events, summary, and attrition sections.

This is the Python equivalent of R's ``asSurvivalResult()`` from
the CohortSurvival package.
"""

from __future__ import annotations

import polars as pl

from omopy.generics._types import OVERALL
from omopy.generics.summarised_result import SummarisedResult

__all__ = ["as_survival_result"]


def as_survival_result(result: SummarisedResult) -> dict[str, pl.DataFrame]:
    """Convert a SummarisedResult to a structured survival result.

    Extracts and pivots the four types of survival data embedded in
    the SummarisedResult:

    - **estimates**: time-point survival/CIF values in wide format
    - **events**: risk table data per interval
    - **summary**: summary statistics (median, RMST, quantiles)
    - **attrition**: attrition tracking

    Parameters
    ----------
    result
        A SummarisedResult from ``estimate_single_event_survival()``
        or ``estimate_competing_risk_survival()``.

    Returns
    -------
    dict
        Dictionary with keys ``"estimates"``, ``"events"``, ``"summary"``,
        ``"attrition"``, each containing a wide-format Polars DataFrame.
    """
    data = result.data

    # Identify estimate type rows
    key_cols = [
        "result_id",
        "cdm_name",
        "group_name",
        "group_level",
        "strata_name",
        "strata_level",
        "variable_name",
        "variable_level",
    ]

    # --- Estimates ---
    est_mask = data["additional_name"].str.contains("time") & ~data[
        "additional_name"
    ].str.contains("eventgap")
    estimates_long = data.filter(est_mask)
    estimates = _pivot_wide(
        estimates_long, [*key_cols, "additional_name", "additional_level"]
    )

    # --- Events ---
    evt_mask = data["additional_name"].str.contains("eventgap")
    events_long = data.filter(evt_mask)
    events = _pivot_wide(
        events_long, [*key_cols, "additional_name", "additional_level"]
    )

    # --- Summary ---
    sum_mask = (data["additional_name"] == OVERALL) & ~data["strata_name"].str.contains(
        "reason"
    )
    summary_long = data.filter(sum_mask)
    summary = _pivot_wide(
        summary_long, [*key_cols, "additional_name", "additional_level"]
    )

    # --- Attrition ---
    attr_mask = data["strata_name"].str.contains("reason")
    attrition_long = data.filter(attr_mask)
    attrition = _pivot_wide(
        attrition_long, [*key_cols, "additional_name", "additional_level"]
    )

    return {
        "estimates": estimates,
        "events": events,
        "summary": summary,
        "attrition": attrition,
    }


def _pivot_wide(
    df: pl.DataFrame,
    index_cols: list[str],
) -> pl.DataFrame:
    """Pivot estimate_name/estimate_value into wide format."""
    if len(df) == 0:
        return df

    available_index = [c for c in index_cols if c in df.columns]

    try:
        return df.pivot(
            on="estimate_name",
            index=available_index,
            values="estimate_value",
            aggregate_function="first",
        )
    except Exception:
        return df
