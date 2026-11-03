"""Build the new classification, day table, and windows on the real extract under scratch names, for Gate B.

Exploratory driver, NOT part of fda_analysis_pipeline.yml (execution plan
2026-10-27, Step 2, Gate B). Gate B's last two checks need the new tables on
the real extract: the windows excluded on 2026-10-13 must fall out of the
cohort by the detector alone, and the Step 1 estimate's counts for the rule
set the classifier implements must be reproduced. Writing to the production
names would replace loop_recommendations and valid_transition_segments before
Step 4 rebuilds what reads them, so this driver writes the three tables under
the gate_b_ prefix and touches no production table. exploratory/gate_b_check.sql
reads them.

Each step is the production staging run() with its output table renamed:
  export_loop_dose_classification.py   -> {CATALOG}.gate_b_loop_dose_classification
  export_loop_recommendations.py       -> {CATALOG}.gate_b_loop_recommendations
  export_valid_transition_segments.py  -> {CATALOG}.gate_b_valid_transition_segments
                                          (the default window rule, 12 of 14)

Run on Databricks (Run button, or python exploratory/gate_b_build.py). A run
does what DROP_TABLES says: True drops the three tables (the check ran and was
filed 2026-11-03, so the Run button now cleans up); False, or `--build`,
rebuilds them for gate_b_check.sql.
"""

import argparse
import os
import sys

# Repo root. As a Databricks python_file task __file__ is defined; in a notebook
# cell it is not, so fall back to the Workspace checkout.
try:
    _FDA_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _FDA_ROOT = os.environ.get(
        "FDA_RWD_ROOT",
        "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
    )
sys.path.insert(0, os.path.join(_FDA_ROOT, "data_staging"))

from export_loop_dose_classification import run as build_dose_classification  # noqa: E402
from export_loop_recommendations import run as build_day_table  # noqa: E402
from export_valid_transition_segments import CATALOG, run as build_windows  # noqa: E402

INPUT_TABLE = "dev.default.bddp_sample_all_2"
DOSE_TABLE = f"{CATALOG}.gate_b_loop_dose_classification"
DAY_TABLE = f"{CATALOG}.gate_b_loop_recommendations"
WINDOW_TABLE = f"{CATALOG}.gate_b_valid_transition_segments"

# True: a run drops the three tables. False: a run builds them. The check ran and
# was filed on Drive 2026-11-03 (rebuild/data/gate_b_check/).
DROP_TABLES = True


def main(spark, drop=DROP_TABLES):
    if drop:
        for table in (WINDOW_TABLE, DAY_TABLE, DOSE_TABLE):
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"dropped {table}")
        return

    print(f"classifying every dose of {INPUT_TABLE} -> {DOSE_TABLE}")
    build_dose_classification(spark, output_table=DOSE_TABLE, input_table=INPUT_TABLE)
    print(f"day table -> {DAY_TABLE}")
    build_day_table(spark, output_table=DAY_TABLE, dose_table=DOSE_TABLE, input_table=INPUT_TABLE)
    print(f"transition windows, default rule -> {WINDOW_TABLE}")
    build_windows(spark, output_table=WINDOW_TABLE, loop_recommendations_table=DAY_TABLE)
    print("done; next: gate_b_check.sql through analysis/export_sql_results_to_csv.py")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="build the three gate_b_ tables whatever DROP_TABLES says")
    args, _ = parser.parse_known_args()
    main(spark, drop=DROP_TABLES and not args.build)
