"""``omopy.drug_diagnostics`` — Drug exposure diagnostics for OMOP CDM.

This subpackage provides functions for running, summarising, and
visualising diagnostic checks on drug exposure records. It is the
Python equivalent of the R ``DrugExposureDiagnostics`` package.

Core workflow::

    import omopy

    # Run diagnostics for specific ingredients
    result = omopy.drug_diagnostics.execute_checks(
        cdm,
        ingredient_concept_ids=[1125315, 1503297],
        checks=["missing", "exposure_duration", "type", "route"],
        sample_size=10_000,
    )

    # Access individual check results
    result["missing"]  # Polars DataFrame

    # Convert to SummarisedResult for interop
    summary = omopy.drug_diagnostics.summarise_drug_diagnostics(result)

    # Visualise
    fig = omopy.drug_diagnostics.plot_drug_diagnostics(summary, check="missing")
"""

# -- Core types and computation ---------------------------------------------
from omopy.drug_diagnostics._checks import (
    AVAILABLE_CHECKS,
    DiagnosticsResult,
    execute_checks,
)

# -- Mock / testing ----------------------------------------------------------
from omopy.drug_diagnostics._mock import (
    benchmark_drug_diagnostics,
    mock_drug_exposure,
)

# -- Plot rendering ----------------------------------------------------------
from omopy.drug_diagnostics._plot import (
    plot_drug_diagnostics,
)

# -- Summarise ---------------------------------------------------------------
from omopy.drug_diagnostics._summarise import (
    summarise_drug_diagnostics,
)

# -- Table rendering ---------------------------------------------------------
from omopy.drug_diagnostics._table import (
    table_drug_diagnostics,
)

__all__ = [
    # Constants (1)
    "AVAILABLE_CHECKS",
    # Core types (1)
    "DiagnosticsResult",
    "benchmark_drug_diagnostics",
    # Computation (1)
    "execute_checks",
    # Mock (2)
    "mock_drug_exposure",
    # Plot (1)
    "plot_drug_diagnostics",
    # Summarise (1)
    "summarise_drug_diagnostics",
    # Table (1)
    "table_drug_diagnostics",
]
