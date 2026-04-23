# Benchmarks: OHDSI R Packages vs OMOPy

This directory contains a benchmark suite that runs identical analyses
using both the OHDSI R packages and OMOPy (Python), then compares the
results.

## Prerequisites

- **Python** with OMOPy installed (`uv sync`)
- **R** 4.5+ (`C:\Users\Ian.Farr\AppData\Local\Programs\R\R-4.5.2\bin`)
- **Java** 11+ (for Synthea data generation)

## Quick Start

```bash
# 1. Install R packages (one-time)
Rscript benchmarks/r/install_packages.R

# 2. Generate the 10K-patient test database (one-time, ~800MB download)
Rscript benchmarks/generate_synthea_1k.R

# 3. Run R benchmarks
Rscript benchmarks/r/run_all.R

# 4. Run Python benchmarks
python benchmarks/python/run_all.py

# 5. Generate comparison report
python benchmarks/compare.py
```

The comparison report is written to `docs/comparison.md` and appears in
the mkdocs site under **Project → R vs Python Comparison**.

## Directory Structure

```
benchmarks/
├── README.md                  # This file
├── generate_synthea_1k.R      # Downloads Eunomia dataset → data/synthea_1k.duckdb
├── compare.py                 # Generates docs/comparison.md from results
├── r/
│   ├── install_packages.R     # Install OHDSI R packages
│   ├── 00_helpers.R           # Shared R helpers
│   ├── 01_snapshot.R          # CDMConnector::snapshot()
│   ├── 02_cohort_generation.R # CDMConnector::generateConceptCohortSet()
│   ├── 03_patient_profiles.R  # PatientProfiles::addDemographics()
│   ├── 04_characteristics.R   # CohortCharacteristics::summariseCharacteristics()
│   ├── 05_incidence.R         # IncidencePrevalence::estimateIncidence()
│   ├── 06_drug_utilisation.R  # DrugUtilisation::summariseDrugUtilisation()
│   ├── 07_survival.R          # CohortSurvival::estimateSingleEventSurvival()
│   ├── 08_codelist.R          # CodelistGenerator::getCandidateCodes()
│   ├── 09_treatment_patterns.R# TreatmentPatterns::computePathways()
│   ├── 10_drug_diagnostics.R  # DrugExposureDiagnostics::executeChecks()
│   ├── run_all.R              # Run all R scripts
│   └── results/               # Auto-generated CSV outputs
└── python/
    ├── helpers.py             # Shared Python helpers
    ├── 01_snapshot.py         # omopy.connector.snapshot()
    ├── 02_cohort_generation.py# omopy.connector.generate_concept_cohort_set()
    ├── 03_patient_profiles.py # omopy.profiles.add_demographics()
    ├── 04_characteristics.py  # omopy.characteristics.summarise_characteristics()
    ├── 05_incidence.py        # omopy.incidence.estimate_incidence()
    ├── 06_drug_utilisation.py # omopy.drug.summarise_drug_utilisation()
    ├── 07_survival.py         # omopy.survival.estimate_single_event_survival()
    ├── 08_codelist.py         # omopy.codelist.get_candidate_codes()
    ├── 09_treatment_patterns.py # omopy.treatment.compute_pathways()
    ├── 10_drug_diagnostics.py # omopy.drug_diagnostics.execute_checks()
    ├── run_all.py             # Run all Python scripts
    └── results/               # Auto-generated CSV outputs
```

## Test Dataset

The dataset (`data/synthea_1k.duckdb`) is downloaded from the OHDSI
Eunomia project (`synthea-medications-10k`):

- **~10,681 patients**, OMOP CDM v5.3
- Schema: `main`
- 37 tables including full vocabulary (~5.9M concepts)
- Key conditions: coronary arteriosclerosis, cerebrovascular accident,
  atrial fibrillation, cardiac arrest, myocardial infarction
- Key drugs: clopidogrel, nitroglycerin, simvastatin, amlodipine,
  verapamil, digoxin, warfarin

The database file is `.gitignore`d — regenerate with
`Rscript benchmarks/generate_synthea_1k.R`.

## Benchmarks Run

Each benchmark pair (R + Python) performs the same analysis:

| # | Analysis | Condition/Drug |
|---|----------|---------------|
| 01 | CDM snapshot | — |
| 02 | Concept cohort generation | Coronary arteriosclerosis (317576) |
| 03 | Add demographics | Coronary arteriosclerosis cohort |
| 04 | Summarise characteristics | Coronary arteriosclerosis cohort |
| 05 | Estimate incidence | Coronary arteriosclerosis |
| 06 | Drug utilisation | Clopidogrel (1322184) |
| 07 | Survival analysis | Target: coronary artery → Outcome: MI |
| 08 | Codelist generation | "coronary" keyword search |
| 09 | Treatment patterns | Clopidogrel + simvastatin |
| 10 | Drug diagnostics | Clopidogrel |