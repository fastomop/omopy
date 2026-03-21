"""SummarisedResult — the standard OHDSI result format.

Mirrors R's ``summarised_result`` S3 class from omopgenerics.
A SummarisedResult is a tabular structure with fixed columns:

- ``result_id``, ``cdm_name``
- ``group_name``, ``group_level``
- ``strata_name``, ``strata_level``
- ``variable_name``, ``variable_level``
- ``estimate_name``, ``estimate_type``, ``estimate_value``
- ``additional_name``, ``additional_level``

Plus a companion **settings** table keyed by ``result_id``.

Name-level pair columns use ``" &&& "`` as separator for multiple entries.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics._types import GROUP_COUNT_VARIABLES, NAME_LEVEL_SEP, OVERALL

__all__ = ["SummarisedResult", "SUMMARISED_RESULT_COLUMNS", "SETTINGS_REQUIRED_COLUMNS"]

SUMMARISED_RESULT_COLUMNS: tuple[str, ...] = (
    "result_id",
    "cdm_name",
    "group_name",
    "group_level",
    "strata_name",
    "strata_level",
    "variable_name",
    "variable_level",
    "estimate_name",
    "estimate_type",
    "estimate_value",
    "additional_name",
    "additional_level",
)

SETTINGS_REQUIRED_COLUMNS: tuple[str, ...] = (
    "result_id",
    "result_type",
    "package_name",
    "package_version",
)


class SummarisedResult:
    """Standard OHDSI summarised result format.

    Wraps a Polars DataFrame with the 13 required columns plus a companion
    settings DataFrame. Provides methods for:

    * Suppression (``suppress``)
    * Splitting name-level pairs (``split_group``, ``split_strata``, etc.)
    * Uniting columns into name-level pairs (``unite_group``, ``unite_strata``, etc.)
    * Pivoting estimates (``pivot_estimates``)
    * Adding settings (``add_settings``)
    * Filtering by settings, strata, or group values
    """

    __slots__ = ("_data", "_settings")

    def __init__(
        self,
        data: pl.DataFrame,
        *,
        settings: pl.DataFrame | None = None,
    ) -> None:
        self._validate_data(data)
        self._data = data
        self._settings = settings if settings is not None else self._default_settings(data)
        self._validate_settings(self._settings)

    @staticmethod
    def _validate_data(data: pl.DataFrame) -> None:
        missing = [c for c in SUMMARISED_RESULT_COLUMNS if c not in data.columns]
        if missing:
            msg = f"SummarisedResult is missing required columns: {missing}"
            raise ValueError(msg)

    @staticmethod
    def _validate_settings(settings: pl.DataFrame) -> None:
        missing = [c for c in SETTINGS_REQUIRED_COLUMNS if c not in settings.columns]
        if missing:
            msg = f"Settings is missing required columns: {missing}"
            raise ValueError(msg)

    @staticmethod
    def _default_settings(data: pl.DataFrame) -> pl.DataFrame:
        ids = data.select("result_id").unique().sort("result_id")
        return ids.with_columns(
            pl.lit("").alias("result_type"),
            pl.lit("omopy").alias("package_name"),
            pl.lit("0.1.0").alias("package_version"),
        )

    # -- Properties ---------------------------------------------------------

    @property
    def data(self) -> pl.DataFrame:
        """The underlying result DataFrame."""
        return self._data

    @property
    def settings(self) -> pl.DataFrame:
        """The companion settings DataFrame."""
        return self._settings

    @settings.setter
    def settings(self, value: pl.DataFrame) -> None:
        self._validate_settings(value)
        self._settings = value

    # -- Suppression --------------------------------------------------------

    def suppress(self, min_cell_count: int = 5) -> SummarisedResult:
        """Suppress estimate values where counts are below *min_cell_count*.

        Following the R implementation:
        1. Identify rows where ``variable_name`` is in GROUP_COUNT_VARIABLES
           and ``estimate_value`` < ``min_cell_count``.
        2. Mark those result_id + group + strata + variable combinations.
        3. Set ``estimate_value`` to ``"-"`` (suppressed sentinel) for those
           rows and linked percentage rows.
        """
        if min_cell_count < 1:
            return self._clone(self._data)

        df = self._data

        # Find rows that are count variables
        count_mask = df["variable_name"].is_in(list(GROUP_COUNT_VARIABLES))

        # Among count rows, find those with values below threshold
        def _parse_value(val: str | None) -> float | None:
            if val is None or val == "-" or val == "":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Build suppression mask
        values = df["estimate_value"].to_list()
        suppress_mask = []
        for i in range(len(df)):
            if count_mask[i]:
                parsed = _parse_value(values[i])
                suppress_mask.append(parsed is not None and 0 < parsed < min_cell_count)
            else:
                suppress_mask.append(False)

        suppress_series = pl.Series("_suppress", suppress_mask)

        # Get the keys of rows to suppress
        keys_to_suppress = (
            df.filter(suppress_series)
            .select("result_id", "group_name", "group_level", "strata_name", "strata_level",
                    "variable_name", "variable_level")
            .unique()
        )

        if keys_to_suppress.is_empty():
            return self._clone(df)

        # Mark rows matching suppressed keys (all estimates for those combos)
        _join_cols = ["result_id", "group_name", "group_level", "strata_name", "strata_level",
                      "variable_name", "variable_level"]
        matched_indices = (
            df.with_row_index("_idx")
            .join(
                keys_to_suppress,
                on=_join_cols,
                how="semi",
                nulls_equal=True,
            )
            .select("_idx")
        )

        idx_set = set(matched_indices["_idx"].to_list())

        # Replace estimate_value with "-" for suppressed rows
        new_values = [
            "-" if i in idx_set else values[i]
            for i in range(len(df))
        ]

        result = df.with_columns(pl.Series("estimate_value", new_values))
        return self._clone(result)

    # -- Split name-level pairs ---------------------------------------------

    @staticmethod
    def _split_name_level(
        df: pl.DataFrame,
        name_col: str,
        level_col: str,
    ) -> pl.DataFrame:
        """Split a name-level pair column into individual columns.

        ``group_name="age &&& sex"`` and ``group_level="50 &&& female"``
        becomes two new columns: ``age="50"`` and ``sex="female"``.
        """
        # Get unique name patterns
        name_patterns = df[name_col].unique().to_list()

        for pattern in name_patterns:
            if pattern == OVERALL or pattern is None:
                continue
            names = [n.strip() for n in pattern.split(NAME_LEVEL_SEP)]

            # Pre-filter rows that match this pattern, split them, then map back
            mask = df[name_col] == pattern
            matching = df.filter(mask)
            split_levels = matching[level_col].str.split(NAME_LEVEL_SEP)

            for idx, col_name in enumerate(names):
                if col_name in df.columns:
                    continue
                # Extract values for matching rows at this index
                extracted = split_levels.list.get(idx, null_on_oob=True).str.strip_chars()
                # Create a full-length null series, then place extracted values at matching positions
                full_col = pl.Series(col_name, [None] * len(df), dtype=pl.Utf8)
                # Use row indices to scatter extracted values
                match_indices = df.with_row_index("__idx").filter(mask)["__idx"]
                values = extracted.to_list()
                full_list = full_col.to_list()
                for i, mi in enumerate(match_indices):
                    full_list[mi] = values[i]
                df = df.with_columns(pl.Series(col_name, full_list, dtype=pl.Utf8))

        return df.drop(name_col, level_col)

    def split_group(self) -> pl.DataFrame:
        """Split ``group_name``/``group_level`` into individual columns."""
        return self._split_name_level(self._data, "group_name", "group_level")

    def split_strata(self) -> pl.DataFrame:
        """Split ``strata_name``/``strata_level`` into individual columns."""
        return self._split_name_level(self._data, "strata_name", "strata_level")

    def split_additional(self) -> pl.DataFrame:
        """Split ``additional_name``/``additional_level`` into individual columns."""
        return self._split_name_level(self._data, "additional_name", "additional_level")

    def split_all(self) -> pl.DataFrame:
        """Split all name-level pair columns."""
        df = self._split_name_level(self._data, "group_name", "group_level")
        df = self._split_name_level(df, "strata_name", "strata_level")
        df = self._split_name_level(df, "additional_name", "additional_level")
        return df

    # -- Unite columns into name-level pairs --------------------------------

    @staticmethod
    def _unite_name_level(
        df: pl.DataFrame,
        columns: list[str],
        name_col: str,
        level_col: str,
    ) -> pl.DataFrame:
        """Unite multiple columns into a single name-level pair.

        Inverse of ``_split_name_level``.
        """
        if not columns:
            return df.with_columns(
                pl.lit(OVERALL).alias(name_col),
                pl.lit(OVERALL).alias(level_col),
            )

        existing = [c for c in columns if c in df.columns]
        if not existing:
            return df.with_columns(
                pl.lit(OVERALL).alias(name_col),
                pl.lit(OVERALL).alias(level_col),
            )

        name_value = NAME_LEVEL_SEP.join(existing)

        # Build the level column by concatenating values
        if len(existing) == 1:
            level_expr = pl.col(existing[0]).cast(pl.Utf8)
        else:
            level_expr = pl.concat_str(
                [pl.col(c).cast(pl.Utf8) for c in existing],
                separator=NAME_LEVEL_SEP,
            )

        return (
            df.with_columns(
                pl.lit(name_value).alias(name_col),
                level_expr.alias(level_col),
            )
            .drop(existing)
        )

    def unite_group(self, columns: list[str]) -> SummarisedResult:
        """Unite columns into ``group_name``/``group_level``."""
        df = self._unite_name_level(self._data, columns, "group_name", "group_level")
        return self._clone(df)

    def unite_strata(self, columns: list[str]) -> SummarisedResult:
        """Unite columns into ``strata_name``/``strata_level``."""
        df = self._unite_name_level(self._data, columns, "strata_name", "strata_level")
        return self._clone(df)

    def unite_additional(self, columns: list[str]) -> SummarisedResult:
        """Unite columns into ``additional_name``/``additional_level``."""
        df = self._unite_name_level(self._data, columns, "additional_name", "additional_level")
        return self._clone(df)

    # -- Pivot estimates ----------------------------------------------------

    def pivot_estimates(self) -> pl.DataFrame:
        """Pivot ``estimate_name``/``estimate_value`` into wide format.

        Each unique ``estimate_name`` becomes a column, with values from
        ``estimate_value``, cast according to ``estimate_type``.
        """
        df = self._data
        key_cols = [c for c in SUMMARISED_RESULT_COLUMNS
                    if c not in ("estimate_name", "estimate_type", "estimate_value")]

        # Pivot
        pivoted = df.pivot(
            on="estimate_name",
            index=key_cols,
            values="estimate_value",
            aggregate_function="first",
        )
        return pivoted

    # -- Add settings -------------------------------------------------------

    def add_settings(self, columns: list[str] | None = None) -> pl.DataFrame:
        """Join settings columns to the result data.

        If *columns* is None, all settings columns are joined.
        """
        if columns is not None:
            # Always include result_id for join
            cols_to_select = ["result_id"] + [c for c in columns if c != "result_id"]
            settings = self._settings.select(
                [c for c in cols_to_select if c in self._settings.columns]
            )
        else:
            settings = self._settings

        return self._data.join(settings, on="result_id", how="left")

    # -- Filtering ----------------------------------------------------------

    def filter_settings(self, **kwargs: Any) -> SummarisedResult:
        """Filter by settings values.

        Example::

            result.filter_settings(result_type="cohort_count")
        """
        settings = self._settings
        for key, value in kwargs.items():
            if key in settings.columns:
                if isinstance(value, (list, tuple)):
                    settings = settings.filter(pl.col(key).is_in(list(value)))
                else:
                    settings = settings.filter(pl.col(key) == value)

        valid_ids = settings["result_id"].to_list()
        data = self._data.filter(pl.col("result_id").is_in(valid_ids))
        return SummarisedResult(data, settings=settings)

    def filter_group(self, **kwargs: str) -> SummarisedResult:
        """Filter by group name-level pairs."""
        return self._filter_name_level("group_name", "group_level", kwargs)

    def filter_strata(self, **kwargs: str) -> SummarisedResult:
        """Filter by strata name-level pairs."""
        return self._filter_name_level("strata_name", "strata_level", kwargs)

    def filter_additional(self, **kwargs: str) -> SummarisedResult:
        """Filter by additional name-level pairs."""
        return self._filter_name_level("additional_name", "additional_level", kwargs)

    def _filter_name_level(
        self,
        name_col: str,
        level_col: str,
        pairs: dict[str, str],
    ) -> SummarisedResult:
        """Internal: filter rows where name-level pairs match."""
        df = self._data
        for name, level in pairs.items():
            # The name must appear in the name column (possibly among &&& separated values)
            df = df.filter(pl.col(name_col).str.contains(name))
            # And the corresponding level must appear in the level column
            df = df.filter(pl.col(level_col).str.contains(level))
        return self._clone(df)

    # -- Tidy ---------------------------------------------------------------

    def tidy(self) -> pl.DataFrame:
        """Convert to a tidy DataFrame: add settings + split all name-level pairs + pivot."""
        df = self.add_settings()
        df = self._split_name_level(df, "group_name", "group_level")
        df = self._split_name_level(df, "strata_name", "strata_level")
        df = self._split_name_level(df, "additional_name", "additional_level")
        return df

    # -- Cloning ------------------------------------------------------------

    def _clone(self, data: pl.DataFrame) -> SummarisedResult:
        return SummarisedResult(data, settings=self._settings)

    # -- Repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        nrows = len(self._data)
        n_result_ids = self._data["result_id"].n_unique()
        return f"SummarisedResult({nrows} rows, {n_result_ids} result_id(s))"

    def __len__(self) -> int:
        return len(self._data)
