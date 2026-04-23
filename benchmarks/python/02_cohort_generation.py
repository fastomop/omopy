"""Benchmark 02: Cohort Generation — omopy.connector.generate_concept_cohort_set()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer
import polars as pl

print("=== 02: Cohort Generation ===")
t = Timer()
cdm = connect_cdm()

from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set

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