"""Benchmark 10: Drug Exposure Diagnostics — omopy.drug_diagnostics.execute_checks()"""

import polars as pl
from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.drug_diagnostics import execute_checks

print("=== 10: Drug Diagnostics ===")
t = Timer()
cdm = connect_cdm()

result = execute_checks(
    cdm,
    ingredient_concept_ids=[1322184],  # clopidogrel
    checks=["missing", "exposure_duration", "type", "route", "quantity"],
)

# Combine all check DataFrames
frames = []
for check_name in result:
    df = result[check_name]
    if df is not None and df.height > 0:
        df = df.with_columns(pl.lit(check_name).alias("check_name"))
        frames.append(df)

if frames:
    # All frames may have different schemas, so save individually
    combined = pl.concat(frames, how="diagonal_relaxed")
    save_result(combined, "10_drug_diagnostics")
else:
    save_result(pl.DataFrame({"check_name": [], "note": []}), "10_drug_diagnostics")

save_timing("10_drug_diagnostics", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
