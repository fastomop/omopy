"""Benchmark 05: Incidence — omopy.incidence.estimate_incidence()"""

from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.connector import generate_concept_cohort_set
from omopy.generics import Codelist
from omopy.incidence import estimate_incidence, generate_denominator_cohort_set

print("=== 05: Incidence ===")
t = Timer()
cdm = connect_cdm()

cdm = generate_concept_cohort_set(
    cdm, Codelist({"coronary_artery": [317576]}), name="outcome_cohort"
)
cdm = generate_denominator_cohort_set(cdm, name="denominator", days_prior_observation=0)

result = estimate_incidence(
    cdm,
    denominator_table="denominator",
    outcome_table="outcome_cohort",
    interval="years",
    repeated_events=False,
)

save_result(result.data, "05_incidence")
save_timing("05_incidence", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
