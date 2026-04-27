"""Benchmark 04: Cohort Characteristics."""

from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.characteristics import summarise_characteristics
from omopy.connector import generate_concept_cohort_set
from omopy.generics import Codelist

print("=== 04: Cohort Characteristics ===")
t = Timer()
cdm = connect_cdm()

cdm = generate_concept_cohort_set(
    cdm, Codelist({"coronary_artery": [317576]}), name="target_cohort"
)
cohort = cdm.cohort_tables["target_cohort"]

result = summarise_characteristics(cohort)
save_result(result.data, "04_characteristics")
save_timing("04_characteristics", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
