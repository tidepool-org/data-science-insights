"""Run the exploratory probes of the loop_settings_distribution subproject.

Results-style SQL files under exploratory/ measure the therapy settings Loop
users run — insulin sensitivity factor, carbohydrate ratio, scheduled basal
rate, and the scheduled basal total per day — by age band (6-11, 12-17, 18-64,
65+, age unknown), on the confirmed type-1 Loop population of the FDA pipeline
with its version and age gates applied to each settings record's own day.
Every result is an aggregate table; nothing per-user is written.

Each file runs through the FDA exporter (FDA_real_world_data/analysis/
export_sql_results_to_csv.py, reused by import): its statements execute in
order, every SELECT lands as result_<label>.csv, and manifest.csv records the
SQL file's SHA-256, the run time, and the Spark version. Outputs go to
loop_settings_distribution/outputs/<sql file stem>/ (git-ignored); download
each folder to the project's Drive data folder.

Run on Databricks the repo's normal way: open this file in the Workspace and
press Run. The RUN_* constants select the probes; a probe already run and filed
is left off so a re-run does not repeat its scan. From a terminal:
    python run_settings_probes.py
Databricks only.
"""

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Workspace paths (the Run button executes a Workspace file as a notebook
# command, so there is no __file__ to derive them from; same convention as
# loopeval_export/run_source_probes.py).
REPO_DIR = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
PROJECT_DIR = os.path.join(REPO_DIR, "loop_settings_distribution")
FDA_ANALYSIS_DIR = os.path.join(REPO_DIR, "FDA_real_world_data", "analysis")

# The probe files, in run order. Each is self-contained (the population view is
# copied verbatim from the RFAI by-age file) and each drops its own scratch
# table first and at the end.
PROBE_SETTINGS = "loop_settings_distribution_probe.sql"

# Which probes this Run executes. A probe already run, verified, and filed on
# Drive is switched off so a re-run does not repeat its scan; switch one back
# on to reproduce it. State on 2026-10-05: written, not yet run.
RUN_SETTINGS_PROBE = True


def probe_jobs(run_settings=RUN_SETTINGS_PROBE):
    """The SQL files to run, in order, as absolute Workspace paths."""
    files = [PROBE_SETTINGS] if run_settings else []
    return [os.path.join(PROJECT_DIR, "exploratory", name) for name in files]


def run(spark, run_settings=RUN_SETTINGS_PROBE):
    """Run each selected probe file through the FDA exporter; return the output folders."""
    sys.path.insert(0, FDA_ANALYSIS_DIR)
    import export_sql_results_to_csv as exporter  # reuse by import, not by fork

    out_dirs = []
    jobs = probe_jobs(run_settings)
    if not jobs:
        print("no probe selected: set one of the RUN_* constants to True")
    for sql_path in jobs:
        stem = os.path.splitext(os.path.basename(sql_path))[0]
        out_dir = os.path.join(PROJECT_DIR, "outputs", stem)
        print(f"\n=== {stem} ===")
        out_dirs.append(exporter.run(spark, sql_path, out_dir=out_dir, skip="none"))
    for name, flag in ((PROBE_SETTINGS, run_settings),):
        if not flag:
            print(f"{name} skipped (its RUN_* constant is False)")
    return out_dirs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _arguments, _unknown = parser.parse_known_args()
    try:
        spark = spark  # type: ignore[name-defined]  # noqa: F841 - notebook global
    except NameError:
        sys.path.insert(0, FDA_ANALYSIS_DIR)
        from export_sql_results_to_csv import get_spark
        spark = get_spark()
    run(spark)
