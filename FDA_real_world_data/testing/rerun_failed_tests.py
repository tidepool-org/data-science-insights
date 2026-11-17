"""Rerun only the tests the last run of testing/run_all_tests.py recorded as failed.

The Run-button form of `run_all_tests.py --failed`: the Run button passes no
arguments, so this file sets the environment variable and runs the runner.
Set ONLY to a comma-separated substring list to narrow it further, or leave
it empty; when the record is gone (the driver's temp folder does not survive a
cluster restart) the runner runs ONLY instead, so ONLY is also the way to name
the tests by hand. Deliberately not named test_*.py, so the runner does not
run it.

Run on Databricks.
"""
import os
import runpy

RERUN_FAILED = "1"
ONLY = ""

RUNNER = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/run_all_tests.py"

os.environ["RERUN_FAILED"] = RERUN_FAILED
if ONLY:
    os.environ["ONLY"] = ONLY
else:
    os.environ.pop("ONLY", None)
_ = runpy.run_path(RUNNER, run_name="__main__")
