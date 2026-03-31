"""``omopy.treatment`` — Treatment pathway analysis for OMOP CDM.

This subpackage provides functions for computing, summarising, and
visualising sequential treatment pathways from OMOP CDM cohort data.
It is the Python equivalent of the R ``TreatmentPatterns`` package.

Core workflow::

    import omopy

    # Define cohorts
    cohorts = [
        omopy.treatment.CohortSpec(cohort_id=1, cohort_name="Target", type="target"),
        omopy.treatment.CohortSpec(cohort_id=2, cohort_name="DrugA", type="event"),
        omopy.treatment.CohortSpec(cohort_id=3, cohort_name="DrugB", type="event"),
    ]

    # Compute pathways
    result = omopy.treatment.compute_pathways(
        cohort, cdm, cohorts,
        era_collapse_size=30,
        combination_window=30,
    )

    # Summarise
    summary = omopy.treatment.summarise_treatment_pathways(result)

    # Visualise
    fig = omopy.treatment.plot_sankey(summary)
"""

# -- Core computation -------------------------------------------------------
# -- Mock / testing ----------------------------------------------------------
from omopy.treatment._mock import (
    mock_treatment_pathways,
)
from omopy.treatment._pathway import (
    CohortSpec,
    PathwayResult,
    compute_pathways,
)

# -- Plot rendering ----------------------------------------------------------
from omopy.treatment._plot import (
    plot_event_duration,
    plot_sankey,
    plot_sunburst,
)

# -- Summarise ---------------------------------------------------------------
from omopy.treatment._summarise import (
    summarise_event_duration,
    summarise_treatment_pathways,
)

# -- Table rendering ---------------------------------------------------------
from omopy.treatment._table import (
    table_event_duration,
    table_treatment_pathways,
)

__all__ = [
    # Core types (2)
    "CohortSpec",
    "PathwayResult",
    # Computation (1)
    "compute_pathways",
    # Mock (1)
    "mock_treatment_pathways",
    "plot_event_duration",
    # Plot (3)
    "plot_sankey",
    "plot_sunburst",
    "summarise_event_duration",
    # Summarise (2)
    "summarise_treatment_pathways",
    "table_event_duration",
    # Table (2)
    "table_treatment_pathways",
]
