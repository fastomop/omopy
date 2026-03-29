"""``omopy.survival`` — Cohort survival analysis for OMOP CDM.

This subpackage provides functions for Kaplan-Meier survival estimation,
competing risk cumulative incidence, risk tables, and survival plots.
It is the Python equivalent of the R ``CohortSurvival`` package.

Core workflow::

    import omopy

    # Estimate survival
    result = omopy.survival.estimate_single_event_survival(
        cdm,
        target_cohort_table="target",
        outcome_cohort_table="outcome",
    )

    # Visualise
    fig = omopy.survival.plot_survival(result)

    # Summary table
    tbl = omopy.survival.table_survival(result)
"""

# -- Core estimation ---------------------------------------------------------
from omopy.survival._estimate import (
    estimate_competing_risk_survival,
    estimate_single_event_survival,
)

# -- Add survival columns ---------------------------------------------------
from omopy.survival._add_survival import (
    add_cohort_survival,
)

# -- Result conversion -------------------------------------------------------
from omopy.survival._result import (
    as_survival_result,
)

# -- Table rendering ---------------------------------------------------------
from omopy.survival._table import (
    options_table_survival,
    table_survival,
    table_survival_attrition,
    table_survival_events,
)

# -- Plot rendering ----------------------------------------------------------
from omopy.survival._plot import (
    available_survival_grouping,
    plot_survival,
)

# -- Mock / testing ----------------------------------------------------------
from omopy.survival._mock import (
    mock_survival,
)

__all__ = [
    # Estimation (2)
    "estimate_single_event_survival",
    "estimate_competing_risk_survival",
    # Add columns (1)
    "add_cohort_survival",
    # Result conversion (1)
    "as_survival_result",
    # Table (4)
    "table_survival",
    "table_survival_events",
    "table_survival_attrition",
    "options_table_survival",
    # Plot (2)
    "plot_survival",
    "available_survival_grouping",
    # Mock (1)
    "mock_survival",
]
