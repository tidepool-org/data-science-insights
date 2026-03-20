"""
Run all test_*.py files in this directory.

Run on Databricks.
"""

import glob
import os
import runpy
import sys
import time

test_dir = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing"
sys.path.insert(0, test_dir)

test_files = sorted(glob.glob(os.path.join(test_dir, "test_*.py")))

passed = []
failed = []

for test_file in test_files:
    name = os.path.basename(test_file)
    print(f"\n{'=' * 60}")
    print(f"RUNNING: {name}")
    print(f"{'=' * 60}\n")

    start = time.time()
    try:
        runpy.run_path(test_file, run_name="__main__")
        elapsed = time.time() - start
        passed.append((name, elapsed))
    except Exception as e:
        elapsed = time.time() - start
        failed.append((name, elapsed, str(e)))
        print(f"\nFAILED: {name} — {e}")

# --- Summary ---
print(f"\n{'=' * 60}")
print("TEST SUMMARY")
print(f"{'=' * 60}")

for name, elapsed in passed:
    print(f"  PASS  {name} ({elapsed:.1f}s)")
for name, elapsed, err in failed:
    print(f"  FAIL  {name} ({elapsed:.1f}s) — {err}")

print(f"\n{len(passed)} passed, {len(failed)} failed, {len(test_files)} total")

if failed:
    raise AssertionError(f"{len(failed)} test(s) failed")
