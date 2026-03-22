"""Column name helpers for OMOP CDM domain tables.

Provides functions to look up the standard start/end date, concept ID,
and source concept ID column names for any OMOP CDM table. For non-OMOP
tables (e.g. cohorts), returns sensible defaults.

These are the Python equivalents of R's ``startDateColumn()``,
``endDateColumn()``, ``standardConceptIdColumn()``, and
``sourceConceptIdColumn()`` from PatientProfiles.
"""

from __future__ import annotations

__all__ = [
    "end_date_column",
    "person_id_column",
    "source_concept_id_column",
    "standard_concept_id_column",
    "start_date_column",
]


# ---------------------------------------------------------------------------
# Canonical column name mapping per OMOP domain table
# ---------------------------------------------------------------------------

_TABLE_COLUMNS: dict[str, dict[str, str]] = {
    "condition_occurrence": {
        "start_date": "condition_start_date",
        "end_date": "condition_end_date",
        "concept_id": "condition_concept_id",
        "source_concept_id": "condition_source_concept_id",
    },
    "drug_exposure": {
        "start_date": "drug_exposure_start_date",
        "end_date": "drug_exposure_end_date",
        "concept_id": "drug_concept_id",
        "source_concept_id": "drug_source_concept_id",
    },
    "procedure_occurrence": {
        "start_date": "procedure_date",
        "end_date": "procedure_date",
        "concept_id": "procedure_concept_id",
        "source_concept_id": "procedure_source_concept_id",
    },
    "observation": {
        "start_date": "observation_date",
        "end_date": "observation_date",
        "concept_id": "observation_concept_id",
        "source_concept_id": "observation_source_concept_id",
    },
    "measurement": {
        "start_date": "measurement_date",
        "end_date": "measurement_date",
        "concept_id": "measurement_concept_id",
        "source_concept_id": "measurement_source_concept_id",
    },
    "visit_occurrence": {
        "start_date": "visit_start_date",
        "end_date": "visit_end_date",
        "concept_id": "visit_concept_id",
        "source_concept_id": "visit_source_concept_id",
    },
    "device_exposure": {
        "start_date": "device_exposure_start_date",
        "end_date": "device_exposure_end_date",
        "concept_id": "device_concept_id",
        "source_concept_id": "device_source_concept_id",
    },
    "death": {
        "start_date": "death_date",
        "end_date": "death_date",
        "concept_id": "cause_concept_id",
        "source_concept_id": "cause_source_concept_id",
    },
    "specimen": {
        "start_date": "specimen_date",
        "end_date": "specimen_date",
        "concept_id": "specimen_concept_id",
        "source_concept_id": "specimen_source_concept_id",
    },
    "episode": {
        "start_date": "episode_start_date",
        "end_date": "episode_end_date",
        "concept_id": "episode_concept_id",
        "source_concept_id": "episode_source_concept_id",
    },
    "observation_period": {
        "start_date": "observation_period_start_date",
        "end_date": "observation_period_end_date",
        "concept_id": "period_type_concept_id",
        "source_concept_id": "period_type_concept_id",
    },
}

# Cohort / non-OMOP defaults
_COHORT_COLUMNS: dict[str, str] = {
    "start_date": "cohort_start_date",
    "end_date": "cohort_end_date",
    "concept_id": "cohort_definition_id",
    "source_concept_id": "cohort_definition_id",
}


def _lookup(table_name: str, key: str) -> str:
    """Look up a column name for a table, falling back to cohort defaults."""
    if table_name in _TABLE_COLUMNS:
        return _TABLE_COLUMNS[table_name][key]
    return _COHORT_COLUMNS[key]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def start_date_column(table_name: str) -> str:
    """Return the canonical start-date column name for an OMOP table.

    Parameters
    ----------
    table_name
        CDM table name (e.g. ``"condition_occurrence"``).

    Returns
    -------
    str
        Column name (e.g. ``"condition_start_date"``).
        For non-OMOP tables, returns ``"cohort_start_date"``.

    Examples
    --------
    >>> start_date_column("drug_exposure")
    'drug_exposure_start_date'
    >>> start_date_column("my_cohort")
    'cohort_start_date'
    """
    return _lookup(table_name, "start_date")


def end_date_column(table_name: str) -> str:
    """Return the canonical end-date column name for an OMOP table.

    Parameters
    ----------
    table_name
        CDM table name (e.g. ``"condition_occurrence"``).

    Returns
    -------
    str
        Column name (e.g. ``"condition_end_date"``).
        For non-OMOP tables, returns ``"cohort_end_date"``.

    Examples
    --------
    >>> end_date_column("drug_exposure")
    'drug_exposure_end_date'
    >>> end_date_column("my_cohort")
    'cohort_end_date'
    """
    return _lookup(table_name, "end_date")


def standard_concept_id_column(table_name: str) -> str:
    """Return the standard concept-ID column name for an OMOP table.

    Parameters
    ----------
    table_name
        CDM table name (e.g. ``"condition_occurrence"``).

    Returns
    -------
    str
        Column name (e.g. ``"condition_concept_id"``).
        For non-OMOP tables, returns ``"cohort_definition_id"``.
    """
    return _lookup(table_name, "concept_id")


def source_concept_id_column(table_name: str) -> str:
    """Return the source concept-ID column name for an OMOP table.

    Parameters
    ----------
    table_name
        CDM table name (e.g. ``"condition_occurrence"``).

    Returns
    -------
    str
        Column name (e.g. ``"condition_source_concept_id"``).
        For non-OMOP tables, returns ``"cohort_definition_id"``.
    """
    return _lookup(table_name, "source_concept_id")


def person_id_column(table_columns: list[str] | tuple[str, ...]) -> str:
    """Detect the person identifier column from a table's column list.

    Checks for ``"person_id"`` first, then ``"subject_id"`` (used in
    cohort tables). Raises if neither is found.

    Parameters
    ----------
    table_columns
        Column names of the table.

    Returns
    -------
    str
        ``"person_id"`` or ``"subject_id"``.

    Raises
    ------
    ValueError
        If neither column is found.
    """
    if "person_id" in table_columns:
        return "person_id"
    if "subject_id" in table_columns:
        return "subject_id"
    msg = (
        "Table has no person identifier column. "
        "Expected 'person_id' or 'subject_id' in columns: "
        f"{list(table_columns)}"
    )
    raise ValueError(msg)
