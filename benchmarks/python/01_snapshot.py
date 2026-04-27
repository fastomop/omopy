"""Benchmark 01: CDM Snapshot — omopy.connector.snapshot()"""

from helpers import Timer, connect_cdm, save_result, save_timing

from omopy.connector import snapshot

print("=== 01: CDM Snapshot ===")
t = Timer()
cdm = connect_cdm()

snap = snapshot(cdm)
save_result(snap.to_polars(), "01_snapshot")
save_timing("01_snapshot", t.elapsed())
print(f"Done in {t.elapsed():.2f} seconds")
