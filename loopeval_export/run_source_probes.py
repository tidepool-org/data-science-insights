"""Run the Phase 0 source probes of the LoopEval transition-cohort export.

Results-style SQL files under exploratory/ measure what each candidate source
table holds for the cohort's export windows (plan of 2026-09-24, decision 1: the
donated extract dev.default.bddp_sample_all_2, which defines the cohort, or the
production table prod.default.device_data, which LoopEval's ETL was written
against), and how the donated extract encodes the fields the ETL decodes. Every
result is an aggregate table; nothing per-user is written.

Each file runs through the FDA exporter (FDA_real_world_data/analysis/
export_sql_results_to_csv.py, reused by import): its statements execute in
order, every SELECT lands as result_<label>.csv, and manifest.csv records the
SQL file's SHA-256, the run time, and the Spark version. Outputs go to
loopeval_export/outputs/<sql file stem>/ (git-ignored); download each folder
to the project's Drive data folder.

Run on Databricks the repo's normal way: open this file in the Workspace and
press Run. The RUN_* constants select the probes; a probe already run and filed
is left off so a re-run does not repeat its scan. From a terminal:
    python run_source_probes.py [--skip-production]
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
# FDA_real_world_data/simulation/export/*.py).
REPO_DIR = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
PROJECT_DIR = os.path.join(REPO_DIR, "loopeval_export")
FDA_ANALYSIS_DIR = os.path.join(REPO_DIR, "FDA_real_world_data", "analysis")

# The probe files, in run order. Each is self-contained (each copies the
# cohort views verbatim from the RFAI by-age file) and each drops its own
# scratch table first and at the end.
PROBE_BDDP = "loopeval_source_probe_bddp.sql"
PROBE_BDDP_ENCODING = "loopeval_source_probe_bddp_encoding.sql"
PROBE_PRODUCTION = "loopeval_source_probe_prod.sql"

# Which probes this Run executes. A probe already run, verified, and filed on
# Drive is switched off so a re-run does not repeat its scan; switch one back
# on to reproduce it. State on 2026-09-24: the donated-extract probe and the
# production probe ran clean that day (both filed / to be filed); the encoding
# follow-up is the one to run next.
RUN_BDDP_PROBE = False
RUN_BDDP_ENCODING_PROBE = True
RUN_PRODUCTION_PROBE = False   # the production scan is the long one


def probe_jobs(run_bddp=RUN_BDDP_PROBE, run_encoding=RUN_BDDP_ENCODING_PROBE,
               run_production=RUN_PRODUCTION_PROBE):
    """The SQL files to run, in order, as absolute Workspace paths."""
    files = ([PROBE_BDDP] if run_bddp else []) \
        + ([PROBE_BDDP_ENCODING] if run_encoding else []) \
        + ([PROBE_PRODUCTION] if run_production else [])
    return [os.path.join(PROJECT_DIR, "exploratory", name) for name in files]


def run(spark, run_bddp=RUN_BDDP_PROBE, run_encoding=RUN_BDDP_ENCODING_PROBE,
        run_production=RUN_PRODUCTION_PROBE):
    """Run each selected probe file through the FDA exporter; return the output folders."""
    sys.path.insert(0, FDA_ANALYSIS_DIR)
    import export_sql_results_to_csv as exporter  # reuse by import, not by fork

    out_dirs = []
    jobs = probe_jobs(run_bddp, run_encoding, run_production)
    if not jobs:
        print("no probe selected: set one of the RUN_* constants to True")
    for sql_path in jobs:
        stem = os.path.splitext(os.path.basename(sql_path))[0]
        out_dir = os.path.join(PROJECT_DIR, "outputs", stem)
        print(f"\n=== {stem} ===")
        out_dirs.append(exporter.run(spark, sql_path, out_dir=out_dir, skip="none"))
    for name, flag in ((PROBE_BDDP, run_bddp), (PROBE_BDDP_ENCODING, run_encoding),
                       (PROBE_PRODUCTION, run_production)):
        if not flag:
            print(f"{name} skipped (its RUN_* constant is False)")
    return out_dirs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-production", action="store_true",
                        help="leave the production probe out whatever RUN_PRODUCTION_PROBE says")
    arguments, _unknown = parser.parse_known_args()
    try:
        spark = spark  # type: ignore[name-defined]  # noqa: F841 - notebook global
    except NameError:
        sys.path.insert(0, FDA_ANALYSIS_DIR)
        from export_sql_results_to_csv import get_spark
        spark = get_spark()
    run(spark, run_production=RUN_PRODUCTION_PROBE and not arguments.skip_production)
