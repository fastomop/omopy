"""Benchmark 02: Cohort Generation — omopy.connector.generate_concept_cohort_set()"""

import polars as pl
from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.connector import generate_concept_cohort_set
from omopy.generics import Codelist

print("=== 02: Cohort Generation ===")
t = Timer()
cdm = connect_cdm()

codelist = Codelist({"coronary_artery": [317576]})
cdm = generate_concept_cohort_set(cdm, codelist, name="target_cohort")
cohort = cdm["target_cohort"]

df = cohort.collect()
counts = df.group_by("cohort_definition_id").agg(
    pl.len().alias("n_records"),
    pl.col("subject_id").n_unique().alias("n_subjects"),
)
save_result(counts, "02_cohort_generation")
save_timing("02_cohort_generation", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
