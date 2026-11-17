"""Show the run times recorded over time (outputs/run_times/run_times.csv).

For every kind of run (the test suite, the harness rebuild, each staging and
analysis selection): how many runs are recorded, the latest total against the
median, the last few totals with their dates, and the steps whose latest duration
moved most against their own median. Set LABEL_CONTAINS to narrow to one kind.

Run it as a file, on Databricks or locally; it reads the file of the environment it
runs in (the Workspace copy on Databricks, the local outputs folder here).
"""
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/production_runs"
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import run_progress  # noqa: E402

LABEL_CONTAINS = ""   # e.g. "test_suite", "production_staging", "integration_harness"
LAST_RUNS = 5

rows = [row for row in run_progress.read_history() if LABEL_CONTAINS in row["run_label"]]
if not rows:
    print(f"no run times recorded yet at {run_progress.history_path()}")
else:
    print(f"{len(rows)} step(s) recorded at {run_progress.history_path()}")
    for line in run_progress.summarize(rows, last=LAST_RUNS):
        print(line)
