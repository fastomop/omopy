"""Benchmark 03: Patient Profiles — omopy.profiles.add_demographics()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 03: Patient Profiles ===")
t = Timer()
cdm = connect_cdm()

from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set
from omopy.profiles import add_demographics

cdm = generate_concept_cohort_set(cdm, Codelist({"coronary_artery": [317576]}), name="target_cohort")
cohort = cdm["target_cohort"]

enriched = add_demographics(cohort, cdm)
df = enriched.collect()
save_result(df.head(100), "03_patient_profiles")
save_timing("03_patient_profiles", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")