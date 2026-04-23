"""Benchmark 04: Cohort Characteristics — omopy.characteristics.summarise_characteristics()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 04: Cohort Characteristics ===")
t = Timer()
cdm = connect_cdm()

from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set
from omopy.characteristics import summarise_characteristics

cdm = generate_concept_cohort_set(cdm, Codelist({"coronary_artery": [317576]}), name="target_cohort")
cohort = cdm["target_cohort"]

result = summarise_characteristics(cohort)
save_result(result.data, "04_characteristics")
save_timing("04_characteristics", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")