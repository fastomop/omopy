"""Shared helpers for Python benchmark scripts."""

import csv
import time
from pathlib import Path

from omopy.connector import cdm_from_con

DB_PATH = Path("data/synthea_1k.duckdb")
RESULTS_DIR = Path("benchmarks/python/results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def connect_cdm():
    return cdm_from_con(DB_PATH, cdm_schema="main", cdm_name="synthea_1k")


def save_result(df, name: str):
    """Save a Polars DataFrame as CSV."""
    path = RESULTS_DIR / f"{name}.csv"
    df.write_csv(path)
    print(f"Saved: {path} ({df.height} rows)")


def save_timing(name: str, elapsed: float):
    path = RESULTS_DIR / f"{name}_timing.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["benchmark", "elapsed_s", "language"])
        w.writerow([name, f"{elapsed:.3f}", "Python"])


class Timer:
    def __init__(self):
        self.start = time.perf_counter()

    def elapsed(self):
        return time.perf_counter() - self.start
