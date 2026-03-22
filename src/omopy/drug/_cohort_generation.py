"""Drug utilisation cohort generation.

Creates drug cohorts from concept sets, ingredient names, or ATC codes.
Records in ``drug_exposure`` are grouped and collapsed into eras using a
configurable gap (``gap_era`` days).

This is the Python equivalent of R's ``generateDrugUtilisationCohortSet()``,
``generateIngredientCohortSet()``, ``generateAtcCohortSet()``, and
``erafyCohort()``.
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
    "generate_drug_utilisation_cohort_set",
    "generate_ingredient_cohort_set",
    "generate_atc_cohort_set",
    "erafy_cohort",
    "cohort_gap_era",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_drug_utilisation_cohort_set(
    cdm: CdmReference,
    concept_set: Codelist | dict[str, list[int]],
    name: str,
    *,
    gap_era: int = 1,
    subset_cohort: str | None = None,
    subset_cohort_id: list[int] | None = None,
    number_exposures: bool = False,
    days_prescribed: bool = False,
) -> CdmReference:
    """Generate a drug utilisation cohort from concept sets.

    Subsets ``drug_exposure`` to records matching the provided concept IDs
    (including descendants), validates against observation periods, and
    collapses overlapping records separated by at most ``gap_era`` days
    into eras.

    Parameters
    ----------
    cdm
        A database-backed CdmReference.
    concept_set
        Named mapping of concept ID lists. Each entry becomes one cohort.
    name
        Name for the resulting cohort table in the CDM.
    gap_era
        Maximum number of days between exposure records for them to be
        collapsed into the same era. Default is 1 (adjacent records merge).
    subset_cohort
        If provided, restricts to persons present in this cohort table.
    subset_cohort_id
        If provided with ``subset_cohort``, restricts to specific cohort
        definition IDs within the subset cohort.
    number_exposures
        If ``True``, track the number of original exposure records per era
        in the settings.
    days_prescribed
        If ``True``, track the total days prescribed per era in the settings.

    Returns
    -------
    CdmReference
        The CDM with a new CohortTable under ``cdm[name]``.
    """
    source = _get_source(cdm)
    con = source.connection
    catalog = source._catalog
    schema = source.cdm_schema

    # Normalize concept_set
    if isinstance(concept_set, dict) and not isinstance(concept_set, Codelist):
        concept_set = Codelist(concept_set)

    # Build cohort definitions
    cohort_defs = []
    for idx, (cname, concept_ids) in enumerate(concept_set.items(), start=1):
        cohort_defs.append({
            "cohort_definition_id": idx,
            "cohort_name": cname,
            "concept_ids": list(concept_ids),
        })

    if not cohort_defs:
        return _empty_drug_cohort(cdm, name, [], gap_era)

    # Resolve descendants for each concept set
    concept_ancestor = con.table("concept_ancestor", database=(catalog, schema))
    concept_tbl = con.table("concept", database=(catalog, schema))
    drug_exposure = con.table("drug_exposure", database=(catalog, schema))
    obs_period = con.table("observation_period", database=(catalog, schema))

    all_parts: list[ir.Table] = []
    codelist_rows: list[dict] = []
    temp_tables: list[str] = []  # track temp tables for cleanup after materialise

    for cdef in cohort_defs:
        cid = cdef["cohort_definition_id"]
        cids = cdef["concept_ids"]

        if not cids:
            continue

        # Upload concept IDs as a temp table
        import pyarrow as pa
        arrow_ids = pa.table({
            "concept_id": pa.array(cids, type=pa.int64()),
        })
        tmp_name = f"__omopy_drug_ids_{name}_{cid}"
        con.con.register(tmp_name, arrow_ids)
        temp_tables.append(tmp_name)

        ids_tbl = con.table(tmp_name)

        # Expand descendants
        descendants = (
            ids_tbl
            .join(concept_ancestor, ids_tbl.concept_id == concept_ancestor.ancestor_concept_id)
            .select(concept_id=concept_ancestor.descendant_concept_id.cast("int64"))
        )
        all_resolved = ids_tbl.select(concept_id=ids_tbl.concept_id.cast("int64")).union(descendants).distinct()

        # Filter to standard Drug concepts only
        drug_concepts = (
            all_resolved
            .join(concept_tbl, all_resolved.concept_id == concept_tbl.concept_id.cast("int64"))
            .filter(concept_tbl.standard_concept == "S")
            .filter(concept_tbl.domain_id == "Drug")
            .select(concept_id=all_resolved.concept_id)
            .distinct()
        )

        # Collect codelist (materialise eagerly — small data)
        cl_arrow = drug_concepts.to_pyarrow()
        for row_cid in cl_arrow.column("concept_id").to_pylist():
            codelist_rows.append({
                "cohort_definition_id": cid,
                "codelist_name": cdef["cohort_name"],
                "concept_id": int(row_cid),
                "codelist_type": "index event",
            })

        # Join with drug_exposure
        events = (
            drug_exposure
            .join(drug_concepts, drug_exposure.drug_concept_id.cast("int64") == drug_concepts.concept_id)
            .select(
                subject_id=drug_exposure.person_id,
                cohort_start_date=drug_exposure.drug_exposure_start_date,
                cohort_end_date=ibis.coalesce(
                    drug_exposure.drug_exposure_end_date,
                    drug_exposure.drug_exposure_start_date,
                ),
            )
            .mutate(cohort_definition_id=ibis.literal(cid, type="int64"))
        )

        # Filter to within observation period
        obs = obs_period.select(
            subject_id=obs_period.person_id,
            obs_start=obs_period.observation_period_start_date,
            obs_end=obs_period.observation_period_end_date,
        )
        events = (
            events
            .join(obs, "subject_id")
            .filter(
                (ibis._.obs_start <= ibis._.cohort_start_date)
                & (ibis._.cohort_start_date <= ibis._.obs_end)
            )
            .mutate(
                cohort_end_date=ibis.least(ibis._.cohort_end_date, ibis._.obs_end),
            )
            .select("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
        )

        all_parts.append(events)

    if not all_parts:
        # Clean up temp tables before returning empty cohort
        for tmp in temp_tables:
            try:
                con.con.unregister(tmp)
            except Exception:
                pass
        return _empty_drug_cohort(cdm, name, cohort_defs, gap_era)

    # Union all parts
    combined = all_parts[0]
    for p in all_parts[1:]:
        combined = combined.union(p)

    # Subset to specific cohort if requested
    if subset_cohort is not None and subset_cohort in cdm:
        subset_tbl = cdm[subset_cohort]
        if hasattr(subset_tbl, "data"):
            subset_data = subset_tbl.data
            if isinstance(subset_data, pl.DataFrame):
                subset_data = ibis.memtable(subset_data.to_arrow())
            elif isinstance(subset_data, pl.LazyFrame):
                subset_data = ibis.memtable(subset_data.collect().to_arrow())
        else:
            subset_data = subset_tbl

        subset_persons = subset_data.select(subject_id=subset_data.subject_id)
        if subset_cohort_id is not None:
            subset_persons = (
                subset_data
                .filter(subset_data.cohort_definition_id.isin(subset_cohort_id))
                .select(subject_id=subset_data.subject_id)
            )
        subset_persons = subset_persons.distinct()
        combined = combined.join(subset_persons, "subject_id").select(
            "cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"
        )

    # Collapse into eras with gap_era
    cohort_result = _erafy_ibis(combined, gap_era)

    # Materialise — temp tables must still be registered at this point
    try:
        cohort_arrow = cohort_result.to_pyarrow()
    finally:
        # Clean up temp tables after materialisation
        for tmp in temp_tables:
            try:
                con.con.unregister(tmp)
            except Exception:
                pass

    cohort_df = pl.from_arrow(cohort_arrow).cast({
        "cohort_definition_id": pl.Int64,
        "subject_id": pl.Int64,
        "cohort_start_date": pl.Date,
        "cohort_end_date": pl.Date,
    })

    # Build settings
    settings_rows = []
    for cdef in cohort_defs:
        row = {
            "cohort_definition_id": cdef["cohort_definition_id"],
            "cohort_name": cdef["cohort_name"],
            "gap_era": gap_era,
        }
        settings_rows.append(row)
    settings_df = pl.DataFrame(settings_rows).cast({"cohort_definition_id": pl.Int64})

    # Build attrition
    attrition_df = _build_attrition(cohort_df, cohort_defs, "Initial qualifying events")

    # Build codelist
    if codelist_rows:
        codelist_df = pl.DataFrame(codelist_rows).cast({
            "cohort_definition_id": pl.Int64,
            "concept_id": pl.Int64,
        })
    else:
        codelist_df = pl.DataFrame(schema={
            "cohort_definition_id": pl.Int64,
            "codelist_name": pl.Utf8,
            "concept_id": pl.Int64,
            "codelist_type": pl.Utf8,
        })

    cohort_table = CohortTable(
        data=cohort_df,
        tbl_name=name,
        tbl_source=source.source_type,
        settings=settings_df,
        attrition=attrition_df,
        cohort_codelist=codelist_df,
    )
    cdm[name] = cohort_table
    return cdm


def generate_ingredient_cohort_set(
    cdm: CdmReference,
    name: str,
    ingredient: str | list[str] | int | list[int] | None = None,
    *,
    gap_era: int = 1,
    subset_cohort: str | None = None,
    subset_cohort_id: list[int] | None = None,
    number_exposures: bool = False,
    days_prescribed: bool = False,
) -> CdmReference:
    """Generate a drug cohort by ingredient name or concept ID.

    Resolves ingredient names/IDs to concept sets via the vocabulary,
    then delegates to :func:`generate_drug_utilisation_cohort_set`.

    Parameters
    ----------
    cdm
        A database-backed CdmReference.
    name
        Name for the cohort table.
    ingredient
        Ingredient(s) to search for. Can be:
        - ``str`` or ``list[str]``: keyword search on concept_name
        - ``int`` or ``list[int]``: direct concept IDs
        - ``None``: all available Drug Ingredient concepts
    gap_era
        Days for era collapse.
    subset_cohort
        Restrict to persons in this cohort.
    subset_cohort_id
        Restrict to specific IDs within subset cohort.
    number_exposures
        Track number of exposures per era.
    days_prescribed
        Track days prescribed per era.

    Returns
    -------
    CdmReference
        The CDM with a new CohortTable.
    """
    from omopy.codelist import get_drug_ingredient_codes

    codelist = get_drug_ingredient_codes(cdm, ingredient)
    if not codelist:
        return _empty_drug_cohort(cdm, name, [], gap_era)

    return generate_drug_utilisation_cohort_set(
        cdm,
        codelist,
        name,
        gap_era=gap_era,
        subset_cohort=subset_cohort,
        subset_cohort_id=subset_cohort_id,
        number_exposures=number_exposures,
        days_prescribed=days_prescribed,
    )


def generate_atc_cohort_set(
    cdm: CdmReference,
    name: str,
    atc_name: str | list[str] | None = None,
    *,
    level: str | list[str] | None = None,
    gap_era: int = 1,
    subset_cohort: str | None = None,
    subset_cohort_id: list[int] | None = None,
    number_exposures: bool = False,
    days_prescribed: bool = False,
) -> CdmReference:
    """Generate a drug cohort by ATC code name.

    Resolves ATC names to concept sets via the vocabulary, then delegates
    to :func:`generate_drug_utilisation_cohort_set`.

    Parameters
    ----------
    cdm
        A database-backed CdmReference.
    name
        Name for the cohort table.
    atc_name
        ATC classification name(s) to search for. ``None`` returns all.
    level
        ATC hierarchy level(s) to filter by (e.g., ``"ATC 1st"``).
    gap_era
        Days for era collapse.
    subset_cohort
        Restrict to persons in this cohort.
    subset_cohort_id
        Restrict to specific IDs within subset cohort.
    number_exposures
        Track number of exposures per era.
    days_prescribed
        Track days prescribed per era.

    Returns
    -------
    CdmReference
        The CDM with a new CohortTable.
    """
    from omopy.codelist import get_atc_codes

    # Handle list of ATC names by querying each and merging
    if isinstance(atc_name, list):
        from omopy.generics.codelist import Codelist
        codelist = Codelist()
        for an in atc_name:
            cl = get_atc_codes(cdm, an, level=level)
            for k, v in cl.items():
                codelist[k] = v
    else:
        codelist = get_atc_codes(cdm, atc_name, level=level)
    if not codelist:
        return _empty_drug_cohort(cdm, name, [], gap_era)

    return generate_drug_utilisation_cohort_set(
        cdm,
        codelist,
        name,
        gap_era=gap_era,
        subset_cohort=subset_cohort,
        subset_cohort_id=subset_cohort_id,
        number_exposures=number_exposures,
        days_prescribed=days_prescribed,
    )


def erafy_cohort(
    cohort: CohortTable,
    gap_era: int,
    *,
    cohort_id: list[int] | None = None,
    name: str | None = None,
) -> CohortTable:
    """Collapse cohort records into eras.

    Records separated by at most ``gap_era`` days are merged into a
    single era per (cohort_definition_id, subject_id) group.

    Parameters
    ----------
    cohort
        The cohort to era-fy.
    gap_era
        Maximum gap in days for merging records.
    cohort_id
        If provided, only era-fy these cohort definition IDs.
    name
        Name for the resulting cohort table.

    Returns
    -------
    CohortTable
        New cohort with collapsed eras.
    """
    df = cohort.collect() if not isinstance(cohort.data, pl.DataFrame) else cohort.data

    if cohort_id is not None:
        to_erafy = df.filter(pl.col("cohort_definition_id").is_in(cohort_id))
        rest = df.filter(~pl.col("cohort_definition_id").is_in(cohort_id))
    else:
        to_erafy = df
        rest = None

    erafied = _erafy_polars(to_erafy, gap_era)

    if rest is not None and len(rest) > 0:
        erafied = pl.concat([erafied, rest])

    new_settings = cohort.settings.clone()
    new_attrition = _build_attrition_from_df(erafied, cohort.settings, "Era-fy cohort records")
    tbl_name = name or cohort._tbl_name

    return CohortTable(
        data=erafied,
        tbl_name=tbl_name,
        tbl_source=cohort._tbl_source if hasattr(cohort, "_tbl_source") else "local",
        settings=new_settings,
        attrition=new_attrition,
        cohort_codelist=cohort.cohort_codelist.clone() if len(cohort.cohort_codelist) > 0 else None,
    )


def cohort_gap_era(cohort: CohortTable, cohort_id: list[int] | None = None) -> dict[int, int]:
    """Retrieve the gap_era setting for each cohort definition.

    Parameters
    ----------
    cohort
        A drug utilisation cohort.
    cohort_id
        If provided, only return gap_era for these IDs.

    Returns
    -------
    dict[int, int]
        Mapping of cohort_definition_id to gap_era value.
    """
    settings = cohort.settings
    if "gap_era" not in settings.columns:
        msg = "Cohort settings do not contain 'gap_era'. Was this cohort created by generate_drug_utilisation_cohort_set?"
        raise ValueError(msg)

    if cohort_id is not None:
        settings = settings.filter(pl.col("cohort_definition_id").is_in(cohort_id))

    return dict(
        zip(
            settings["cohort_definition_id"].to_list(),
            settings["gap_era"].to_list(),
        )
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_source(cdm: CdmReference) -> DbSource:
    """Extract DbSource from CDM, raising if not database-backed."""
    source = cdm.cdm_source
    if not isinstance(source, DbSource):
        msg = "Drug utilisation functions require a database-backed CDM (DbSource)"
        raise TypeError(msg)
    return source


def _erafy_ibis(tbl: ir.Table, gap_era: int) -> ir.Table:
    """Collapse overlapping/adjacent records into eras using Ibis.

    Records for the same (cohort_definition_id, subject_id) are merged
    if they are separated by at most ``gap_era`` days.
    """
    grp_window = ibis.window(
        group_by=["cohort_definition_id", "subject_id"],
        order_by="cohort_start_date",
    )

    # Extend each end date by gap_era for gap detection
    extended_end = tbl.cohort_end_date + ibis.interval(days=gap_era)

    # Running max of extended end date
    cum_max = extended_end.max().over(
        ibis.window(
            group_by=["cohort_definition_id", "subject_id"],
            order_by="cohort_start_date",
            following=0,
        )
    )
    tbl = tbl.mutate(_cum_max_end=cum_max)

    # Lag to get previous max
    prev_max = tbl._cum_max_end.lag(1).over(grp_window)
    tbl = tbl.mutate(_prev_max_end=prev_max)

    # New island when start > previous max (gap > gap_era days)
    tbl = tbl.mutate(
        _is_new=ibis.cases(
            (tbl._prev_max_end.isnull(), 1),
            (tbl.cohort_start_date > tbl._prev_max_end, 1),
            else_=0,
        )
    )

    island_id = tbl._is_new.sum().over(grp_window)
    tbl = tbl.mutate(_island_id=island_id)

    collapsed = (
        tbl
        .group_by("cohort_definition_id", "subject_id", "_island_id")
        .agg(
            cohort_start_date=tbl.cohort_start_date.min(),
            cohort_end_date=tbl.cohort_end_date.max(),
        )
        .drop("_island_id")
    )

    return collapsed


def _erafy_polars(df: pl.DataFrame, gap_era: int) -> pl.DataFrame:
    """Collapse overlapping/adjacent records into eras using Polars."""
    if len(df) == 0:
        return df

    df = df.sort("cohort_definition_id", "subject_id", "cohort_start_date")

    # Extended end = end + gap_era days
    df = df.with_columns(
        _ext_end=(pl.col("cohort_end_date") + pl.duration(days=gap_era)),
    )

    # Running max of extended end within group
    df = df.with_columns(
        _cum_max=pl.col("_ext_end")
        .cum_max()
        .over("cohort_definition_id", "subject_id"),
    )

    # Lag cum_max to get prev max
    df = df.with_columns(
        _prev_max=pl.col("_cum_max")
        .shift(1)
        .over("cohort_definition_id", "subject_id"),
    )

    # New island flag
    df = df.with_columns(
        _is_new=pl.when(pl.col("_prev_max").is_null())
        .then(1)
        .when(pl.col("cohort_start_date") > pl.col("_prev_max"))
        .then(1)
        .otherwise(0),
    )

    # Island ID = cumulative sum of is_new
    df = df.with_columns(
        _island=pl.col("_is_new")
        .cum_sum()
        .over("cohort_definition_id", "subject_id"),
    )

    # Aggregate per island
    result = (
        df
        .group_by("cohort_definition_id", "subject_id", "_island")
        .agg(
            pl.col("cohort_start_date").min(),
            pl.col("cohort_end_date").max(),
        )
        .drop("_island")
        .sort("cohort_definition_id", "subject_id", "cohort_start_date")
    )

    return result


def _build_attrition(
    cohort_df: pl.DataFrame,
    cohort_defs: list[dict],
    reason: str,
) -> pl.DataFrame:
    """Build initial attrition DataFrame from materialised cohort."""
    counts = (
        cohort_df
        .group_by("cohort_definition_id")
        .agg(
            pl.len().alias("number_records"),
            pl.col("subject_id").n_unique().alias("number_subjects"),
        )
    )
    rows = []
    for cdef in cohort_defs:
        cid = cdef["cohort_definition_id"]
        match = counts.filter(pl.col("cohort_definition_id") == cid)
        nr = match["number_records"][0] if len(match) > 0 else 0
        ns = match["number_subjects"][0] if len(match) > 0 else 0
        rows.append({
            "cohort_definition_id": cid,
            "number_records": nr,
            "number_subjects": ns,
            "reason_id": 1,
            "reason": reason,
            "excluded_records": 0,
            "excluded_subjects": 0,
        })
    return pl.DataFrame(rows).cast({
        "cohort_definition_id": pl.Int64,
        "number_records": pl.Int64,
        "number_subjects": pl.Int64,
        "reason_id": pl.Int64,
        "excluded_records": pl.Int64,
        "excluded_subjects": pl.Int64,
    })


def _build_attrition_from_df(
    cohort_df: pl.DataFrame,
    settings: pl.DataFrame,
    reason: str,
) -> pl.DataFrame:
    """Build attrition from cohort DataFrame and existing settings."""
    cohort_defs = [
        {"cohort_definition_id": row["cohort_definition_id"]}
        for row in settings.to_dicts()
    ]
    return _build_attrition(cohort_df, cohort_defs, reason)


def _empty_drug_cohort(
    cdm: CdmReference,
    name: str,
    cohort_defs: list[dict],
    gap_era: int,
) -> CdmReference:
    """Create an empty drug utilisation cohort."""
    empty_df = pl.DataFrame(schema={
        "cohort_definition_id": pl.Int64,
        "subject_id": pl.Int64,
        "cohort_start_date": pl.Date,
        "cohort_end_date": pl.Date,
    })

    if cohort_defs:
        settings_rows = [
            {
                "cohort_definition_id": cdef["cohort_definition_id"],
                "cohort_name": cdef.get("cohort_name", f"cohort_{cdef['cohort_definition_id']}"),
                "gap_era": gap_era,
            }
            for cdef in cohort_defs
        ]
    else:
        settings_rows = [{"cohort_definition_id": 1, "cohort_name": name, "gap_era": gap_era}]

    settings_df = pl.DataFrame(settings_rows).cast({"cohort_definition_id": pl.Int64})

    attrition_df = _build_attrition(empty_df, cohort_defs or [{"cohort_definition_id": 1}], "Initial qualifying events")

    cohort_table = CohortTable(
        data=empty_df,
        tbl_name=name,
        tbl_source="local",
        settings=settings_df,
        attrition=attrition_df,
    )
    cdm[name] = cohort_table
    return cdm
