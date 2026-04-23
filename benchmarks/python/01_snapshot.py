"""Benchmark 01: CDM Snapshot — omopy.connector.snapshot()"""
import sys; sys.path.insert(0, "benchmarks/python")
from helpers import connect_cdm, save_result, save_timing, Timer

print("=== 01: CDM Snapshot ===")
t = Timer()
cdm = connect_cdm()

from omopy.connector import snapshot
snap = snapshot(cdm)
save_result(snap.to_polars(), "01_snapshot")
save_timing("01_snapshot", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")