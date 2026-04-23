"""Benchmark 08: Codelist Generation — omopy.codelist.get_candidate_codes()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 08: Codelist ===")
t = Timer()
cdm = connect_cdm()

from omopy.codelist import get_candidate_codes

codes = get_candidate_codes(cdm, keywords=["coronary"], domains=["Condition"], include_descendants=True)
# codes is a Codelist (dict-like) — flatten to a DataFrame
import polars as pl
rows = []
for name, concept_ids in codes.items():
    for cid in concept_ids:
        rows.append({"codelist_name": name, "concept_id": cid})
df = pl.DataFrame(rows)

save_result(df, "08_codelist")
save_timing("08_codelist", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")