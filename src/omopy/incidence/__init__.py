"""``omopy.incidence`` — Incidence and prevalence estimation.

Python equivalent of the DARWIN-EU **IncidencePrevalence** R package.
Provides functions to generate denominator cohorts, estimate incidence
rates and prevalence proportions from an OMOP CDM, and present results
as tables and plots.

Exports (21)
------------

**Denominator generation (2)**

- :func:`generate_denominator_cohort_set`
- :func:`generate_target_denominator_cohort_set`

**Core estimation (3)**

- :func:`estimate_incidence`
- :func:`estimate_point_prevalence`
- :func:`estimate_period_prevalence`

**Result conversion (2)**

- :func:`as_incidence_result`
- :func:`as_prevalence_result`

**Tables (6)**

- :func:`table_incidence`
- :func:`table_prevalence`
- :func:`table_incidence_attrition`
- :func:`table_prevalence_attrition`
- :func:`options_table_incidence`
- :func:`options_table_prevalence`

**Plots (4)**

- :func:`plot_incidence`
- :func:`plot_prevalence`
- :func:`plot_incidence_population`
- :func:`plot_prevalence_population`

**Grouping helpers (2)**

- :func:`available_incidence_grouping`
- :func:`available_prevalence_grouping`

**Utilities (2)**

- :func:`mock_incidence_prevalence`
- :func:`benchmark_incidence_prevalence`
"""

from __future__ import annotations

from omopy.incidence._denominator import (
    generate_denominator_cohort_set,
    generate_target_denominator_cohort_set,
)
from omopy.incidence._estimate import (
    estimate_incidence,
    estimate_period_prevalence,
    estimate_point_prevalence,
)
from omopy.incidence._result import (
    as_incidence_result,
    as_prevalence_result,
)
from omopy.incidence._table import (
    options_table_incidence,
    options_table_prevalence,
    table_incidence,
    table_incidence_attrition,
    table_prevalence,
    table_prevalence_attrition,
)
from omopy.incidence._plot import (
    available_incidence_grouping,
    available_prevalence_grouping,
    plot_incidence,
    plot_incidence_population,
    plot_prevalence,
    plot_prevalence_population,
)
from omopy.incidence._mock import (
    benchmark_incidence_prevalence,
    mock_incidence_prevalence,
)

__all__ = [
    # Denominator generation
    "generate_denominator_cohort_set",
    "generate_target_denominator_cohort_set",
    # Core estimation
    "estimate_incidence",
    "estimate_point_prevalence",
    "estimate_period_prevalence",
    # Result conversion
    "as_incidence_result",
    "as_prevalence_result",
    # Tables
    "table_incidence",
    "table_prevalence",
    "table_incidence_attrition",
    "table_prevalence_attrition",
    "options_table_incidence",
    "options_table_prevalence",
    # Plots
    "plot_incidence",
    "plot_prevalence",
    "plot_incidence_population",
    "plot_prevalence_population",
    # Grouping helpers
    "available_incidence_grouping",
    "available_prevalence_grouping",
    # Utilities
    "mock_incidence_prevalence",
    "benchmark_incidence_prevalence",
]
