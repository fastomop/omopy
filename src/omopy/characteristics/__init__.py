"""``omopy.characteristics`` — Cohort characteristics analysis.

Python equivalent of the R **CohortCharacteristics** package. Provides
functions for summarising cohort demographics, intersections, timing,
overlap, large-scale characteristics, and codelist usage. Results are
returned as :class:`~omopy.generics.SummarisedResult` objects which
integrate with ``omopy.vis`` for table and plot rendering.

Exports
-------
**Summarise functions** (7):

- :func:`summarise_characteristics` — demographics + intersections
- :func:`summarise_cohort_count` — subject/record counts
- :func:`summarise_cohort_attrition` — attrition flowchart data
- :func:`summarise_cohort_timing` — pairwise timing between cohorts
- :func:`summarise_cohort_overlap` — pairwise overlap between cohorts
- :func:`summarise_large_scale_characteristics` — concept-level prevalence
- :func:`summarise_cohort_codelist` — codelist concept summaries

**Table functions** (8):

- :func:`table_characteristics`
- :func:`table_cohort_count`
- :func:`table_cohort_attrition`
- :func:`table_cohort_timing`
- :func:`table_cohort_overlap`
- :func:`table_top_large_scale_characteristics`
- :func:`table_large_scale_characteristics`
- :func:`available_table_columns`

**Plot functions** (7):

- :func:`plot_characteristics`
- :func:`plot_cohort_count`
- :func:`plot_cohort_attrition`
- :func:`plot_cohort_timing`
- :func:`plot_cohort_overlap`
- :func:`plot_large_scale_characteristics`
- :func:`plot_compared_large_scale_characteristics`

**Utilities** (1):

- :func:`mock_cohort_characteristics`
"""

from __future__ import annotations

from omopy.characteristics._summarise import (
    summarise_characteristics,
    summarise_cohort_attrition,
    summarise_cohort_codelist,
    summarise_cohort_count,
    summarise_cohort_overlap,
    summarise_cohort_timing,
    summarise_large_scale_characteristics,
)
from omopy.characteristics._table import (
    available_table_columns,
    table_characteristics,
    table_cohort_attrition,
    table_cohort_count,
    table_cohort_overlap,
    table_cohort_timing,
    table_large_scale_characteristics,
    table_top_large_scale_characteristics,
)
from omopy.characteristics._plot import (
    plot_characteristics,
    plot_cohort_attrition,
    plot_cohort_count,
    plot_cohort_overlap,
    plot_cohort_timing,
    plot_compared_large_scale_characteristics,
    plot_large_scale_characteristics,
)
from omopy.characteristics._mock import mock_cohort_characteristics

__all__ = [
    # Summarise
    "summarise_characteristics",
    "summarise_cohort_count",
    "summarise_cohort_attrition",
    "summarise_cohort_timing",
    "summarise_cohort_overlap",
    "summarise_large_scale_characteristics",
    "summarise_cohort_codelist",
    # Tables
    "table_characteristics",
    "table_cohort_count",
    "table_cohort_attrition",
    "table_cohort_timing",
    "table_cohort_overlap",
    "table_top_large_scale_characteristics",
    "table_large_scale_characteristics",
    "available_table_columns",
    # Plots
    "plot_characteristics",
    "plot_cohort_count",
    "plot_cohort_attrition",
    "plot_cohort_timing",
    "plot_cohort_overlap",
    "plot_large_scale_characteristics",
    "plot_compared_large_scale_characteristics",
    # Utilities
    "mock_cohort_characteristics",
]
