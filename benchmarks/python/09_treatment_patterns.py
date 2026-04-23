"""Benchmark 09: Treatment Patterns — omopy.treatment.compute_pathways()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 09: Treatment Patterns ===")
t = Timer()
cdm = connect_cdm()

from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set
from omopy.treatment import compute_pathways, summarise_treatment_pathways, CohortSpec

cdm = generate_concept_cohort_set(
    cdm,
    Codelist({
        "coronary_artery": [317576],
        "clopidogrel": [1322184],
        "simvastatin": [1539403],
    }),
    name="tp_cohorts",
)

cohort = cdm["tp_cohorts"]
specs = [
    CohortSpec(cohort_id=1, cohort_name="coronary_artery", type="target"),
    CohortSpec(cohort_id=2, cohort_name="clopidogrel", type="event"),
    CohortSpec(cohort_id=3, cohort_name="simvastatin", type="event"),
]

pathway_result = compute_pathways(cohort, cdm, cohorts=specs)
result = summarise_treatment_pathways(pathway_result)

save_result(result.data, "09_treatment_patterns")
save_timing("09_treatment_patterns", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")