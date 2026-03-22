"""``omopy.drug`` — Drug utilisation analysis for OMOP CDM.

This subpackage provides functions for drug cohort generation,
utilisation metrics, dose calculation, indication/treatment analysis,
and summarisation. It is the Python equivalent of the R
``DrugUtilisation`` package.

Core workflow::

    import omopy

    # Generate a drug cohort
    cdm = omopy.drug.generate_drug_utilisation_cohort_set(
        cdm, name="my_drug",
        concept_set={"aspirin": [1112807]},
    )

    # Enrich with metrics
    enriched = omopy.drug.add_drug_utilisation(
        cdm["my_drug"], gap_era=30,
    )

    # Summarise and render
    result = omopy.drug.summarise_drug_utilisation(
        cdm["my_drug"], gap_era=30,
    )
    table = omopy.drug.table_drug_utilisation(result)
"""

# -- Cohort generation --------------------------------------------------
from omopy.drug._cohort_generation import (
    cohort_gap_era,
    erafy_cohort,
    generate_atc_cohort_set,
    generate_drug_utilisation_cohort_set,
    generate_ingredient_cohort_set,
)

# -- Daily dose ----------------------------------------------------------
from omopy.drug._daily_dose import (
    add_daily_dose,
    pattern_table,
)

# -- Requirement / filter functions --------------------------------------
from omopy.drug._require import (
    require_drug_in_date_range,
    require_is_first_drug_entry,
    require_observation_before_drug,
    require_prior_drug_washout,
)

# -- Add drug use metrics -----------------------------------------------
from omopy.drug._add_drug_use import (
    add_cumulative_dose,
    add_cumulative_quantity,
    add_days_exposed,
    add_days_prescribed,
    add_drug_restart,
    add_drug_utilisation,
    add_initial_daily_dose,
    add_initial_exposure_duration,
    add_initial_quantity,
    add_number_eras,
    add_number_exposures,
    add_time_to_exposure,
)

# -- Add intersect (indication / treatment) -----------------------------
from omopy.drug._add_intersect import (
    add_indication,
    add_treatment,
)

# -- Summarise -----------------------------------------------------------
from omopy.drug._summarise import (
    summarise_dose_coverage,
    summarise_drug_restart,
    summarise_drug_utilisation,
    summarise_indication,
    summarise_proportion_of_patients_covered,
    summarise_treatment,
)

# -- Table rendering -----------------------------------------------------
from omopy.drug._table import (
    table_dose_coverage,
    table_drug_restart,
    table_drug_utilisation,
    table_indication,
    table_proportion_of_patients_covered,
    table_treatment,
)

# -- Plot rendering ------------------------------------------------------
from omopy.drug._plot import (
    plot_drug_restart,
    plot_drug_utilisation,
    plot_indication,
    plot_proportion_of_patients_covered,
    plot_treatment,
)

# -- Mock / benchmark ----------------------------------------------------
from omopy.drug._mock import (
    benchmark_drug_utilisation,
    mock_drug_utilisation,
)

__all__ = [
    # Cohort generation (5)
    "generate_drug_utilisation_cohort_set",
    "generate_ingredient_cohort_set",
    "generate_atc_cohort_set",
    "erafy_cohort",
    "cohort_gap_era",
    # Daily dose (2)
    "add_daily_dose",
    "pattern_table",
    # Require / filter (4)
    "require_is_first_drug_entry",
    "require_prior_drug_washout",
    "require_observation_before_drug",
    "require_drug_in_date_range",
    # Add drug use metrics (12)
    "add_drug_utilisation",
    "add_number_exposures",
    "add_number_eras",
    "add_days_exposed",
    "add_days_prescribed",
    "add_time_to_exposure",
    "add_initial_exposure_duration",
    "add_initial_quantity",
    "add_cumulative_quantity",
    "add_initial_daily_dose",
    "add_cumulative_dose",
    "add_drug_restart",
    # Add intersect (2)
    "add_indication",
    "add_treatment",
    # Summarise (6)
    "summarise_drug_utilisation",
    "summarise_indication",
    "summarise_treatment",
    "summarise_drug_restart",
    "summarise_dose_coverage",
    "summarise_proportion_of_patients_covered",
    # Table (6)
    "table_drug_utilisation",
    "table_indication",
    "table_treatment",
    "table_drug_restart",
    "table_dose_coverage",
    "table_proportion_of_patients_covered",
    # Plot (5)
    "plot_drug_utilisation",
    "plot_indication",
    "plot_treatment",
    "plot_drug_restart",
    "plot_proportion_of_patients_covered",
    # Mock / benchmark (2)
    "mock_drug_utilisation",
    "benchmark_drug_utilisation",
]
