"""``omopy.pregnancy`` — Pregnancy episode identification (HIPPS algorithm).

Python equivalent of the DARWIN-EU **PregnancyIdentifier** R package.
Identifies pregnancy episodes from OMOP CDM data using the HIPPS algorithm
(Smith et al. 2024), which combines outcome-anchored (HIP) and
gestational-timing (PPS) approaches with Episode Start Date (ESD)
refinement.

Exports (8)
-----------

**Core pipeline (1)**

- :func:`identify_pregnancies` — Main entry point

**Result container (1)**

- :class:`PregnancyResult` — Pydantic model for results

**Summarise / table / plot (3)**

- :func:`summarise_pregnancies` — Summarise to SummarisedResult
- :func:`table_pregnancies` — Table wrapper
- :func:`plot_pregnancies` — Plot wrapper

**Utilities (2)**

- :func:`mock_pregnancy_cdm` — Mock CDM for testing
- :func:`validate_episodes` — Validate episode periods

**Constants (1)**

- :data:`OUTCOME_CATEGORIES` — Outcome code-to-name mapping
"""

from __future__ import annotations

from omopy.pregnancy._concepts import OUTCOME_CATEGORIES
from omopy.pregnancy._identify import PregnancyResult, identify_pregnancies
from omopy.pregnancy._mock import mock_pregnancy_cdm, validate_episodes
from omopy.pregnancy._plot import plot_pregnancies
from omopy.pregnancy._summarise import summarise_pregnancies
from omopy.pregnancy._table import table_pregnancies

__all__ = [
    # Constants
    "OUTCOME_CATEGORIES",
    # Result container
    "PregnancyResult",
    # Core pipeline
    "identify_pregnancies",
    # Utilities
    "mock_pregnancy_cdm",
    "plot_pregnancies",
    # Summarise / table / plot
    "summarise_pregnancies",
    "table_pregnancies",
    "validate_episodes",
]
