"""Benchmark 07: Cohort Survival — omopy.survival.estimate_single_event_survival()"""

from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.connector import generate_concept_cohort_set
from omopy.generics import Codelist
from omopy.survival import estimate_single_event_survival

print("=== 07: Survival ===")
t = Timer()
cdm = connect_cdm()

cdm = generate_concept_cohort_set(
    cdm,
    Codelist({"coronary_artery": [317576], "mi": [4329847]}),
    name="survival_cohorts",
)

result = estimate_single_event_survival(
    cdm,
    target_cohort_table="survival_cohorts",
    target_cohort_id=1,
    outcome_cohort_table="survival_cohorts",
    outcome_cohort_id=2,
)

save_result(result.data, "07_survival")
save_timing("07_survival", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
