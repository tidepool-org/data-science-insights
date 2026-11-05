"""One click: the §8-1 cohort re-selected with the Apple Health rate-key basal rule withheld.

The rate-key sensitivity of 2026-11-05 (exploratory/transition_cohort_rate_key_sensitivity.py)
dropped users from the production cohort by the window the production
search had chosen. This driver answers the other question MC asked the same
day: what cohort the pipeline selects when the classifier never had the rule.
It builds the variant as the window-rule variants are built, under one
suffix, and leaves production untouched:

  loop_recommendations_rate_key_withheld      the day table with health_rate_loop_app
                                              left out of the basal rules (the
                                              bolus rules unchanged)
  valid_transition_segments_rate_key_withheld the window search on that day table,
                                              12 of 14, and the rest of the
                                              box-affected subtree (run_transition_variant.BOX_TABLES)
  outputs/analysis_8_1_rate_key_withheld/     analysis 8-1: Table 8.1a, Figures 8.1a
                                              to 8.1c, the by-diagnosis-group
                                              appendix with the LADA series, every
                                              figure titled with the build

Only analysis 8-1 runs. The 6-3a cohort flow reads the production day table
for its first stages, so its funnel for this build waits for the Step 3 cohort
tables; the other §8 analyses are not the question.

Run (Databricks Run button), after production_runs/run_production_staging.py:
  production_runs/run_rate_key_withheld.py                 # build + analysis 8-1
  production_runs/run_rate_key_withheld.py --skip-analysis # tables only
  production_runs/run_rate_key_withheld.py --teardown      # drop this build's tables
"""

import argparse
import datetime as dt
import hashlib
import os
import sys

import pandas as pd

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "production_runs",
    )
_FDA_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_FDA_ROOT, "exploratory"))

import run_transition_variant as variant  # noqa: E402

SUFFIX = "_rate_key_withheld"
WITHHELD_BASAL_TAGS = ("health_rate_loop_app",)
ANALYSES = [
    "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py",
]
# This build's tables, newest dependency last: the day table and the subtree.
TABLES = ("loop_recommendations",) + variant.BOX_TABLES


def _sha256(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest() if os.path.exists(path) else ""


def write_manifest(spark, output_dir):
    """Provenance beside the analysis outputs: the run time, Spark, the build,
    and the hashes of the driver and of the day-table export it ran."""
    os.makedirs(output_dir, exist_ok=True)
    pd.DataFrame([{
        "run_time_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "spark_version": spark.version,
        "script": "production_runs/run_rate_key_withheld.py",
        "script_sha256": _sha256(os.path.join(_HERE, "run_rate_key_withheld.py")),
        "day_table_export_sha256": _sha256(os.path.join(_FDA_ROOT, "data_staging", "export_loop_recommendations.py")),
        "suffix": SUFFIX,
        "withheld_basal_tags": ", ".join(WITHHELD_BASAL_TAGS),
    }]).to_csv(os.path.join(output_dir, "manifest.csv"), index=False)


def run(spark, run_analysis: bool = True):
    print("=" * 72)
    print(f"[rate key withheld] suffix {SUFFIX}; basal rules withheld: {', '.join(WITHHELD_BASAL_TAGS)}")
    print("=" * 72)
    variant.run(spark, suffix=SUFFIX, min_period_days=variant.DEFAULT_MIN_PERIOD_DAYS,
                run_analysis=run_analysis, withheld_basal_tags=WITHHELD_BASAL_TAGS, analyses=ANALYSES)
    if run_analysis:
        # beside analysis 8-1's files, which it writes relative to the working
        # directory (its OUTPUT_DIR plus the suffix)
        write_manifest(spark, os.path.abspath(f"outputs/analysis_8_1{SUFFIX}"))


def teardown(spark, dry_run: bool = False):
    """Drop this build's tables and nothing else (the suffix keeps it off production)."""
    for name in TABLES:
        statement = f"DROP TABLE IF EXISTS {variant.CATALOG}.{name}{SUFFIX}"
        print(f"[rate key withheld] {statement}")
        if not dry_run:
            spark.sql(statement)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--skip-analysis", action="store_true", help="build the tables only")
    _parser.add_argument("--teardown", action="store_true", help="drop this build's tables instead of building")
    _args, _ = _parser.parse_known_args()
    if _args.teardown:
        teardown(spark)
    else:
        run(spark, run_analysis=not _args.skip_analysis)
