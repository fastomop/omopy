"""Run all Python benchmark scripts sequentially."""

import subprocess
import sys
from pathlib import Path

scripts = sorted(Path("benchmarks/python").glob("[0-9][0-9]_*.py"))

print("=" * 40)
print("Running all Python benchmarks")
print("=" * 40)

for script in scripts:
    print(f"\n--- Running: {script} ---")
    result = subprocess.run([sys.executable, str(script)], capture_output=False)
    if result.returncode != 0:
        print(f"ERROR: {script} failed with exit code {result.returncode}")

print("\n" + "=" * 40)
print("All Python benchmarks complete.")
print("Results in: benchmarks/python/results/")
print("=" * 40)
