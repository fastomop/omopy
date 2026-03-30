"""CohortTable — a CdmTable with cohort settings, attrition, and codelist metadata.

Mirrors R's ``cohort_table`` S3 class from omopgenerics.
A cohort table must have columns: ``cohort_definition_id``, ``subject_id``,
``cohort_start_date``, ``cohort_end_date``.

Associated metadata:
- **settings** (cohort_set): definition ID -> name (+ custom columns)
- **attrition** (cohort_attrition): step-by-step inclusion/exclusion counts
- **codelist** (cohort_codelist): concept IDs used per cohort definition
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import polars as pl

from omopy.generics.cdm_table import CdmTable
from omopy.generics.codelist import Codelist

if TYPE_CHECKING:
    from omopy.generics.cdm_reference import CdmReference

__all__ = ["CohortTable", "COHORT_REQUIRED_COLUMNS"]

COHORT_REQUIRED_COLUMNS: tuple[str, ...] = (
    "cohort_definition_id",
    "subject_id",
    "cohort_start_date",
    "cohort_end_date",
)


class CohortTable(CdmTable):
    """A specialised CDM table representing a generated cohort.

    Extends :class:`CdmTable` with three pieces of companion metadata:

    * ``settings``  — A DataFrame mapping ``cohort_definition_id`` to
      ``cohort_name`` (and possibly other columns).
    * ``attrition`` — A DataFrame tracking inclusion/exclusion at each step.
    * ``cohort_codelist`` — A :class:`Codelist` of concept IDs used to
      generate each cohort.

    These mirror the R ``cohort_set``, ``cohort_attrition``, and
    ``cohort_codelist`` attributes.
    """

    __slots__ = ("_settings", "_attrition", "_cohort_codelist")

    def __init__(
        self,
        data: pl.DataFrame | pl.LazyFrame | Any,
        *,
        tbl_name: str = "cohort",
        tbl_source: str = "local",
        cdm: CdmReference | None = None,
        settings: pl.DataFrame | None = None,
        attrition: pl.DataFrame | None = None,
        cohort_codelist: pl.DataFrame | None = None,
    ) -> None:
        super().__init__(data, tbl_name=tbl_name, tbl_source=tbl_source, cdm=cdm)
        self._settings = settings if settings is not None else self._default_settings()
        self._attrition = attrition if attrition is not None else self._default_attrition()
        self._cohort_codelist = (
            cohort_codelist
            if cohort_codelist is not None
            else (
                pl.DataFrame(
                    schema={
                        "cohort_definition_id": pl.Int64,
                        "codelist_name": pl.Utf8,
                        "concept_id": pl.Int64,
                        "codelist_type": pl.Utf8,
                    }
                )
            )
        )
        self._validate_cohort()

    def _validate_cohort(self) -> None:
        """Validate that required cohort columns are present."""
        cols = set(self.columns)
        missing = [c for c in COHORT_REQUIRED_COLUMNS if c not in cols]
        if missing:
            msg = f"CohortTable is missing required columns: {missing}"
            raise ValueError(msg)

    def _default_settings(self) -> pl.DataFrame:
        """Create default settings from distinct cohort_definition_ids."""
        try:
            df = self.collect()
            ids = df.select("cohort_definition_id").unique().sort("cohort_definition_id")
            return ids.with_columns(
                pl.col("cohort_definition_id").cast(pl.Int64).alias("cohort_definition_id"),
            ).with_columns(
                pl.concat_str(
                    [pl.lit("cohort_"), pl.col("cohort_definition_id").cast(pl.Utf8)]
                ).alias("cohort_name"),
            )
        except Exception:
            return pl.DataFrame(
                schema={
                    "cohort_definition_id": pl.Int64,
                    "cohort_name": pl.Utf8,
                }
            )

    def _default_attrition(self) -> pl.DataFrame:
        """Create an empty default attrition table."""
        return pl.DataFrame(
            schema={
                "cohort_definition_id": pl.Int64,
                "number_records": pl.Int64,
                "number_subjects": pl.Int64,
                "reason_id": pl.Int64,
                "reason": pl.Utf8,
                "excluded_records": pl.Int64,
                "excluded_subjects": pl.Int64,
            }
        )

    # -- Properties ---------------------------------------------------------

    @property
    def settings(self) -> pl.DataFrame:
        """Cohort settings (cohort_set) DataFrame."""
        return self._settings

    @settings.setter
    def settings(self, value: pl.DataFrame) -> None:
        if "cohort_definition_id" not in value.columns:
            msg = "Settings must have a 'cohort_definition_id' column"
            raise ValueError(msg)
        if "cohort_name" not in value.columns:
            msg = "Settings must have a 'cohort_name' column"
            raise ValueError(msg)
        self._settings = value

    @property
    def attrition(self) -> pl.DataFrame:
        """Cohort attrition DataFrame."""
        return self._attrition

    @attrition.setter
    def attrition(self, value: pl.DataFrame) -> None:
        self._attrition = value

    @property
    def cohort_codelist(self) -> pl.DataFrame:
        """Cohort codelist DataFrame."""
        return self._cohort_codelist

    @cohort_codelist.setter
    def cohort_codelist(self, value: pl.DataFrame) -> None:
        self._cohort_codelist = value

    @property
    def cohort_ids(self) -> list[int]:
        """Distinct cohort definition IDs from the settings."""
        return self._settings["cohort_definition_id"].to_list()

    @property
    def cohort_names(self) -> list[str]:
        """Cohort names from the settings."""
        return self._settings["cohort_name"].to_list()

    def cohort_count(self) -> pl.DataFrame:
        """Compute number of records and subjects per cohort definition."""
        df = self.collect()
        return (
            df.group_by("cohort_definition_id")
            .agg(
                pl.len().alias("number_records"),
                pl.col("subject_id").n_unique().alias("number_subjects"),
            )
            .sort("cohort_definition_id")
        )

    # -- Override _with_data to preserve cohort metadata --------------------

    def _with_data(self, new_data: pl.DataFrame | pl.LazyFrame | Any) -> Self:
        """Create new CohortTable preserving cohort metadata.

        If the new data no longer has ``cohort_definition_id``, falls back
        to a plain CdmTable.
        """
        new_cols: list[str] = []
        if isinstance(new_data, (pl.DataFrame, pl.LazyFrame)):
            new_cols = new_data.columns
        elif hasattr(new_data, "columns"):
            new_cols = list(new_data.columns)

        # If cohort_definition_id is lost, downgrade to CdmTable
        if "cohort_definition_id" not in new_cols:
            return CdmTable._with_data(self, new_data)  # type: ignore[return-value]

        new = self.__class__.__new__(self.__class__)
        new._data = new_data
        new._tbl_name = self._tbl_name
        new._tbl_source = self._tbl_source
        new._cdm_ref = self._cdm_ref
        new._settings = self._settings
        new._attrition = self._attrition
        new._cohort_codelist = self._cohort_codelist
        return new

    # -- Repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        n_cohorts = len(self.cohort_ids)
        source = self._tbl_source
        return f"CohortTable('{self._tbl_name}', source='{source}', cohorts={n_cohorts})"
