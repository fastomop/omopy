"""Benchmark 06: Drug Utilisation — omopy.drug"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 06: Drug Utilisation ===")
t = Timer()
cdm = connect_cdm()

from omopy.drug import generate_ingredient_cohort_set, summarise_drug_utilisation

cdm = generate_ingredient_cohort_set(cdm, name="drug_cohort", ingredient="clopidogrel")
cohort = cdm["drug_cohort"]

result = summarise_drug_utilisation(cohort, ingredient_concept_id=1322184, gap_era=30)
save_result(result.data, "06_drug_utilisation")
save_timing("06_drug_utilisation", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")