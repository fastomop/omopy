"""``omopy.profiles`` — Patient-level enrichment for OMOP CDM tables.

This subpackage provides functions to add demographic information,
cohort/table/concept intersections, death data, and categorical binning
to CDM tables. It is the Python equivalent of the R PatientProfiles
package.

Primary functions::

    from omopy.profiles import (
        add_age, add_sex, add_demographics,
        add_prior_observation, add_future_observation,
        add_date_of_birth, add_in_observation,
        add_cohort_intersect_flag, add_cohort_intersect_count,
        add_table_intersect_flag, add_table_intersect_count,
        add_death_flag, add_death_date, add_death_days,
        add_categories,
    )
"""

from omopy.profiles._categories import add_categories
from omopy.profiles._cohort_intersect import (
    add_cohort_intersect_count,
    add_cohort_intersect_date,
    add_cohort_intersect_days,
    add_cohort_intersect_field,
    add_cohort_intersect_flag,
)
from omopy.profiles._columns import (
    end_date_column,
    person_id_column,
    source_concept_id_column,
    standard_concept_id_column,
    start_date_column,
)
from omopy.profiles._concept_intersect import (
    add_concept_intersect_count,
    add_concept_intersect_date,
    add_concept_intersect_days,
    add_concept_intersect_field,
    add_concept_intersect_flag,
)
from omopy.profiles._death import (
    add_death_date,
    add_death_days,
    add_death_flag,
)
from omopy.profiles._demographics import (
    add_age,
    add_date_of_birth,
    add_demographics,
    add_future_observation,
    add_in_observation,
    add_prior_observation,
    add_sex,
)
from omopy.profiles._table_intersect import (
    add_table_intersect_count,
    add_table_intersect_date,
    add_table_intersect_days,
    add_table_intersect_field,
    add_table_intersect_flag,
)
from omopy.profiles._utilities import (
    add_cdm_name,
    add_cohort_name,
    add_concept_name,
    filter_cohort_id,
    filter_in_observation,
)
from omopy.profiles._windows import (
    Window,
    format_name_style,
    validate_windows,
    window_name,
)

__all__ = [
    # windows
    "Window",
    # demographics
    "add_age",
    # categories
    "add_categories",
    # utilities
    "add_cdm_name",
    # cohort intersect
    "add_cohort_intersect_count",
    "add_cohort_intersect_date",
    "add_cohort_intersect_days",
    "add_cohort_intersect_field",
    "add_cohort_intersect_flag",
    "add_cohort_name",
    # concept intersect
    "add_concept_intersect_count",
    "add_concept_intersect_date",
    "add_concept_intersect_days",
    "add_concept_intersect_field",
    "add_concept_intersect_flag",
    "add_concept_name",
    "add_date_of_birth",
    # death
    "add_death_date",
    "add_death_days",
    "add_death_flag",
    "add_demographics",
    "add_future_observation",
    "add_in_observation",
    "add_prior_observation",
    "add_sex",
    # table intersect
    "add_table_intersect_count",
    "add_table_intersect_date",
    "add_table_intersect_days",
    "add_table_intersect_field",
    "add_table_intersect_flag",
    # columns
    "end_date_column",
    "filter_cohort_id",
    "filter_in_observation",
    "format_name_style",
    "person_id_column",
    "source_concept_id_column",
    "standard_concept_id_column",
    "start_date_column",
    "validate_windows",
    "window_name",
]
