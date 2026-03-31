"""``omopy.testing`` — Test data generation for OMOP CDM studies.

This subpackage provides utilities for creating small, hand-crafted test
patient populations for OMOP CDM studies. It is the Python equivalent of
the R ``TestGenerator`` package.

Core workflow::

    from omopy.testing import (
        read_patients,
        patients_cdm,
        generate_test_tables,
        mock_test_cdm,
        graph_cohort,
    )

    # 1. Generate blank Excel template
    path = generate_test_tables(
        ["person", "observation_period", "condition_occurrence"]
    )

    # 2. Fill in the template, then read and validate
    data = read_patients("my_patients.xlsx", output_path="patients.json")

    # 3. Load into a CdmReference
    cdm = patients_cdm("patients.json")

    # 4. Or skip files and use a mock CDM for quick tests
    cdm = mock_test_cdm(n_persons=10)

    # 5. Visualize cohort timelines
    fig = graph_cohort(subject_id=1, cohorts={"target": cohort_df})
"""

# -- Read / validate -------------------------------------------------------
# -- CDM construction ------------------------------------------------------
from omopy.testing._cdm import (
    mock_test_cdm,
    patients_cdm,
)

# -- Template generation ----------------------------------------------------
from omopy.testing._generate import generate_test_tables

# -- Plotting ---------------------------------------------------------------
from omopy.testing._plot import graph_cohort
from omopy.testing._read import (
    read_patients,
    validate_patient_data,
)

__all__ = [
    # Template generation
    "generate_test_tables",
    # Plotting
    "graph_cohort",
    "mock_test_cdm",
    # CDM construction
    "patients_cdm",
    # Read / validate
    "read_patients",
    "validate_patient_data",
]
