"""Indication and treatment intersection functions.

Adds columns to a drug cohort indicating whether each subject had a
matching indication or treatment within specified time windows relative
to the cohort index date.

Both :func:`add_indication` and :func:`add_treatment` delegate to a
shared internal engine :func:`_add_intersect` that uses the existing
``add_cohort_intersect_flag`` from ``omopy.profiles``.

This is the Python equivalent of R's ``addIndication()`` and
``addTreatment()`` from the DrugUtilisation package.
"""

from __future__ import annotations

import polars as pl

from omopy.generics.cdm_reference import CdmReference
from omopy.generics.cdm_table import CdmTable
from omopy.generics.cohort_table import CohortTable
from omopy.profiles._cohort_intersect import add_cohort_intersect_flag
from omopy.profiles._windows import Window, validate_windows, window_name

__all__ = ["add_indication", "add_treatment"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def add_indication(
    cohort: CohortTable,
    indication_cohort_name: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    indication_cohort_id: list[int] | None = None,
    indication_window: Window | list[Window] = (0, 0),
    unknown_indication_table: str | list[str] | None = None,
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    mutually_exclusive: bool = True,
    name_style: str | None = None,
    name: str | None = None,
) -> CohortTable:
    """Add indication columns to a drug cohort.

    Checks whether each subject had a matching indication (was present in
    the indication cohort) within the specified ``indication_window``
    relative to the ``index_date``.

    Parameters
    ----------
    cohort
        A CohortTable.
    indication_cohort_name
        Name of the indication cohort table in the CDM, or a CohortTable.
    cdm
        CDM reference. If ``None``, uses ``cohort.cdm``.
    indication_cohort_id
        Which cohort IDs in the indication table to consider. ``None`` = all.
    indication_window
        Time window(s) relative to ``index_date`` (e.g., ``(-30, 0)``).
    unknown_indication_table
        OMOP clinical table name(s) (e.g., ``"condition_occurrence"``) to
        detect unknown indications. If ``None``, no unknown detection.
    index_date
        Reference date column in the cohort.
    censor_date
        Optional censoring date column.
    mutually_exclusive
        If ``True``, produce a single character column per window with
        combined labels. If ``False``, produce a binary flag column per
        (window, indication) pair.
    name_style
        Column naming template. Supports ``{window_name}`` and
        ``{cohort_name}`` placeholders.
    name
        Unused (API compatibility).

    Returns
    -------
    CohortTable
        The cohort with indication column(s) added.

    Notes
    -----
    When ``mutually_exclusive=True``, the column values are:

    - Individual cohort name (e.g., ``"headache"``)
    - Combined names (e.g., ``"headache and asthma"``)
    - ``"unknown"`` — not in any indication cohort but found in
      ``unknown_indication_table``
    - ``"none"`` — not in any indication/unknown table

    When ``mutually_exclusive=False``, each column is an integer flag (0/1).
    """
    return _add_intersect(
        cohort,
        cohort_table=indication_cohort_name,
        cdm=cdm,
        cohort_table_id=indication_cohort_id,
        window=indication_window,
        unknown_table=unknown_indication_table,
        index_date=index_date,
        censor_date=censor_date,
        mutually_exclusive=mutually_exclusive,
        name_style=name_style,
        kind="indication",
    )


def add_treatment(
    cohort: CohortTable,
    treatment_cohort_name: str | CohortTable,
    *,
    cdm: CdmReference | None = None,
    treatment_cohort_id: list[int] | None = None,
    window: Window | list[Window] = (0, 0),
    index_date: str = "cohort_start_date",
    censor_date: str | None = None,
    mutually_exclusive: bool = True,
    name_style: str | None = None,
    name: str | None = None,
) -> CohortTable:
    """Add treatment columns to a drug cohort.

    Checks whether each subject was receiving a treatment (present in
    the treatment cohort) within the specified ``window`` relative to
    the ``index_date``.

    Parameters
    ----------
    cohort
        A CohortTable.
    treatment_cohort_name
        Name of the treatment cohort table in the CDM, or a CohortTable.
    cdm
        CDM reference. If ``None``, uses ``cohort.cdm``.
    treatment_cohort_id
        Which cohort IDs in the treatment table to consider. ``None`` = all.
    window
        Time window(s) relative to ``index_date``.
    index_date
        Reference date column in the cohort.
    censor_date
        Optional censoring date column.
    mutually_exclusive
        If ``True``, produce a single character column per window with
        combined labels. If ``False``, produce a binary flag column per
        (window, treatment) pair.
    name_style
        Column naming template.
    name
        Unused (API compatibility).

    Returns
    -------
    CohortTable
        The cohort with treatment column(s) added.

    Notes
    -----
    When ``mutually_exclusive=True``, the column values are:

    - Individual cohort name (e.g., ``"metformin"``)
    - Combined names (e.g., ``"metformin and simvastatin"``)
    - ``"untreated"`` — not in any treatment cohort

    When ``mutually_exclusive=False``, each column is an integer flag (0/1).
    """
    return _add_intersect(
        cohort,
        cohort_table=treatment_cohort_name,
        cdm=cdm,
        cohort_table_id=treatment_cohort_id,
        window=window,
        unknown_table=None,
        index_date=index_date,
        censor_date=censor_date,
        mutually_exclusive=mutually_exclusive,
        name_style=name_style,
        kind="treatment",
    )


# ---------------------------------------------------------------------------
# Shared engine
# ---------------------------------------------------------------------------


def _add_intersect(
    cohort: CohortTable,
    *,
    cohort_table: str | CohortTable,
    cdm: CdmReference | None,
    cohort_table_id: list[int] | None,
    window: Window | list[Window],
    unknown_table: str | list[str] | None,
    index_date: str,
    censor_date: str | None,
    mutually_exclusive: bool,
    name_style: str | None,
    kind: str,  # "indication" or "treatment"
) -> CohortTable:
    """Shared engine for add_indication and add_treatment."""
    cdm = cdm or cohort.cdm
    if cdm is None:
        msg = "CdmReference is required"
        raise ValueError(msg)

    windows = validate_windows(window)

    # Resolve target cohort
    if isinstance(cohort_table, str):
        target_ct = cdm[cohort_table]
        if not isinstance(target_ct, CohortTable):
            msg = f"Table '{cohort_table}' is not a CohortTable"
            raise TypeError(msg)
    else:
        target_ct = cohort_table

    # Get target cohort IDs and names
    settings = target_ct.settings
    all_ids = settings["cohort_definition_id"].to_list()
    all_names = settings["cohort_name"].to_list()

    if cohort_table_id is not None:
        ids = [i for i in cohort_table_id if i in all_ids]
        names = [all_names[all_ids.index(i)] for i in ids]
    else:
        ids = all_ids
        names = all_names

    if not ids:
        # No matching IDs — just return the cohort with empty columns
        return _add_empty_columns(
            cohort, windows, names, mutually_exclusive, name_style, kind
        )

    # Default name style
    if name_style is None:
        if mutually_exclusive:
            name_style = f"{kind}_{{window_name}}"
        else:
            name_style = f"{kind}_{{window_name}}_{{cohort_name}}"

    # Step 1: Add cohort intersection flags using profiles module
    # We use a temporary CdmTable name_style to get flags
    flag_style = "_{cohort_name}_{window_name}"

    # Add flags to cohort
    enriched = add_cohort_intersect_flag(
        cohort,
        target_ct,
        cdm,
        target_cohort_id=ids,
        index_date=index_date,
        censor_date=censor_date or "cohort_end_date",
        target_start_date="cohort_start_date",
        target_end_date="cohort_end_date",
        window=windows,
        name_style=flag_style,
    )

    # Collect the enriched data
    enriched_df = (
        enriched.collect()
        if not isinstance(enriched.data, pl.DataFrame)
        else enriched.data
    )

    # Step 2: Handle unknown indication table
    unknown_flag_cols: dict[str, str] = {}  # window_name -> column_name
    if unknown_table is not None and kind == "indication":
        unknown_tables = (
            [unknown_table] if isinstance(unknown_table, str) else list(unknown_table)
        )
        enriched_df = _add_unknown_flags(
            enriched_df,
            cdm,
            unknown_tables,
            windows,
            index_date,
            censor_date,
            unknown_flag_cols,
        )

    # Step 3: Build output columns
    if mutually_exclusive:
        result_df = _collapse_to_labels(
            enriched_df,
            windows,
            names,
            unknown_flag_cols,
            name_style,
            kind,
        )
    else:
        result_df = _rename_flag_columns(
            enriched_df,
            windows,
            names,
            name_style,
        )

    # Clean up temporary flag columns
    temp_cols = [
        c for c in result_df.columns if c.startswith("_") and c not in cohort.columns
    ]
    if temp_cols:
        result_df = result_df.drop(temp_cols)

    return CohortTable(
        data=result_df,
        tbl_name=cohort._tbl_name,
        tbl_source=cohort._tbl_source if hasattr(cohort, "_tbl_source") else "local",
        settings=cohort.settings.clone(),
        attrition=cohort.attrition.clone(),
        cohort_codelist=cohort.cohort_codelist.clone()
        if len(cohort.cohort_codelist) > 0
        else None,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _collapse_to_labels(
    df: pl.DataFrame,
    windows: list[tuple[float, float]],
    names: list[str],
    unknown_flag_cols: dict[str, str],
    name_style: str,
    kind: str,
) -> pl.DataFrame:
    """Collapse binary flags into a single label column per window.

    Combines matched cohort names with " and " separator.
    """
    no_match_label = "none" if kind == "indication" else "untreated"

    for w in windows:
        wn = window_name(w)
        col_name = name_style.format(window_name=wn, cohort_name="")

        # Build label by checking each cohort flag
        sorted_names = sorted(names)
        label_expr = pl.lit("")

        for cname in sorted_names:
            flag_col = f"_{cname}_{wn}"
            if flag_col not in df.columns:
                continue

            label_expr = (
                pl.when((pl.col(flag_col) == 1) & (label_expr == pl.lit("")))
                .then(pl.lit(cname))
                .when(pl.col(flag_col) == 1)
                .then(label_expr + pl.lit(" and ") + pl.lit(cname))
                .otherwise(label_expr)
            )

        # Handle empty labels (no match)
        unknown_col = unknown_flag_cols.get(wn)
        if unknown_col is not None and unknown_col in df.columns:
            label_expr = (
                pl.when(label_expr == pl.lit(""))
                .then(
                    pl.when(pl.col(unknown_col) == 1)
                    .then(pl.lit("unknown"))
                    .otherwise(pl.lit(no_match_label))
                )
                .otherwise(label_expr)
            )
        else:
            label_expr = (
                pl.when(label_expr == pl.lit(""))
                .then(pl.lit(no_match_label))
                .otherwise(label_expr)
            )

        df = df.with_columns(label_expr.alias(col_name))

    # Drop the temporary flag columns
    [c for c in df.columns if c.startswith("_")]
    original_flag_cols = [
        f"_{cname}_{window_name(w)}"
        for cname in names
        for w in windows
        if f"_{cname}_{window_name(w)}" in df.columns
    ]
    if original_flag_cols:
        df = df.drop(original_flag_cols)

    # Drop unknown flag columns
    for uc in unknown_flag_cols.values():
        if uc in df.columns:
            df = df.drop(uc)

    return df


def _rename_flag_columns(
    df: pl.DataFrame,
    windows: list[tuple[float, float]],
    names: list[str],
    name_style: str,
) -> pl.DataFrame:
    """Rename temporary flag columns to the user-specified name_style."""
    rename_map: dict[str, str] = {}
    for w in windows:
        wn = window_name(w)
        for cname in names:
            old_col = f"_{cname}_{wn}"
            new_col = name_style.format(window_name=wn, cohort_name=cname)
            if old_col in df.columns:
                rename_map[old_col] = new_col

    if rename_map:
        df = df.rename(rename_map)

    return df


def _add_unknown_flags(
    df: pl.DataFrame,
    cdm: CdmReference,
    unknown_tables: list[str],
    windows: list[tuple[float, float]],
    index_date: str,
    censor_date: str | None,
    unknown_flag_cols: dict[str, str],
) -> pl.DataFrame:
    """Add unknown indication flags from clinical tables.

    Checks for any records in the specified OMOP tables within each
    window and combines with OR logic.
    """
    from omopy.connector.db_source import DbSource
    from omopy.profiles import add_table_intersect_flag

    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        return df

    # For each window, create a combined unknown flag
    for w in windows:
        wn = window_name(w)
        unknown_col = f"_unknown_{wn}"
        unknown_flag_cols[wn] = unknown_col

        # Start with all zeros
        df = df.with_columns(pl.lit(0).alias(unknown_col))

        for table_name in unknown_tables:
            if table_name not in cdm:
                continue

            # Use profiles add_table_intersect_flag
            temp = CdmTable(data=df, tbl_name="_temp_unknown")
            temp.cdm = cdm

            try:
                enriched = add_table_intersect_flag(
                    temp,
                    cdm,
                    table_name=table_name,
                    index_date=index_date,
                    window=w,
                    name_style=f"_unk_{table_name}_{{window_name}}",
                )
                enriched_data = (
                    enriched.collect()
                    if not isinstance(enriched.data, pl.DataFrame)
                    else enriched.data
                )

                unk_flag = f"_unk_{table_name}_{wn}"
                if unk_flag in enriched_data.columns:
                    # OR with existing unknown flag
                    df = enriched_data.with_columns(
                        pl.max_horizontal(pl.col(unknown_col), pl.col(unk_flag)).alias(
                            unknown_col
                        ),
                    ).drop(unk_flag)
            except Exception:
                # Skip if table intersection fails
                pass

    return df


def _add_empty_columns(
    cohort: CohortTable,
    windows: list[tuple[float, float]],
    names: list[str],
    mutually_exclusive: bool,
    name_style: str | None,
    kind: str,
) -> CohortTable:
    """Add empty columns when no matching cohort IDs found."""
    if name_style is None:
        if mutually_exclusive:
            name_style = f"{kind}_{{window_name}}"
        else:
            name_style = f"{kind}_{{window_name}}_{{cohort_name}}"

    df = cohort.collect() if not isinstance(cohort.data, pl.DataFrame) else cohort.data
    no_match_label = "none" if kind == "indication" else "untreated"

    if mutually_exclusive:
        for w in windows:
            wn = window_name(w)
            col_name = name_style.format(window_name=wn, cohort_name="")
            df = df.with_columns(pl.lit(no_match_label).alias(col_name))
    else:
        for w in windows:
            wn = window_name(w)
            for cname in names:
                col_name = name_style.format(window_name=wn, cohort_name=cname)
                df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(col_name))

    return CohortTable(
        data=df,
        tbl_name=cohort._tbl_name,
        tbl_source=cohort._tbl_source if hasattr(cohort, "_tbl_source") else "local",
        settings=cohort.settings.clone(),
        attrition=cohort.attrition.clone(),
        cohort_codelist=cohort.cohort_codelist.clone()
        if len(cohort.cohort_codelist) > 0
        else None,
    )
