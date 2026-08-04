"""
Unit test for export_cbg_from_ab_days.py.

Tests: only readings on outcome days survive (eligible-but-low-coverage days
drop), the plausibility filter, the DATE(cbg_timestamp) day join, and exclusion
of users with no cohort rows or non-outcome cohort rows.

Run on Databricks.
"""

import sys
from datetime import date, datetime

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_cbg_from_ab_days import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
LOOP_CBG_TABLE = f"{TEST_SCHEMA}._test_abcbg_cbg"
COHORT_TABLE = f"{TEST_SCHEMA}._test_abcbg_cohort"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_abcbg_output"

ALL_TABLES = [LOOP_CBG_TABLE, COHORT_TABLE, OUTPUT_TABLE]

# --- Test data ---
cohort_rows = [
    {"_userId": "user_a", "day": date(2024, 1, 1), "is_outcome_day": True},
    {"_userId": "user_a", "day": date(2024, 1, 2), "is_outcome_day": False},  # eligible, low coverage
    {"_userId": "user_b", "day": date(2024, 1, 1), "is_outcome_day": False},
]

loop_cbg_rows = [
    # user_a Jan 1 (outcome day): three plausible readings kept, one
    # implausible dropped.
    {"_userId": "user_a", "cbg_timestamp": datetime(2024, 1, 1, 10, 0), "cbg_mg_dl": 100.0, "is_plausible": True},
    {"_userId": "user_a", "cbg_timestamp": datetime(2024, 1, 1, 10, 5), "cbg_mg_dl": 110.0, "is_plausible": True},
    {"_userId": "user_a", "cbg_timestamp": datetime(2024, 1, 1, 23, 55), "cbg_mg_dl": 120.0, "is_plausible": True},
    {"_userId": "user_a", "cbg_timestamp": datetime(2024, 1, 1, 11, 0), "cbg_mg_dl": 600.0, "is_plausible": False},
    # user_a Jan 2 (not an outcome day): dropped.
    {"_userId": "user_a", "cbg_timestamp": datetime(2024, 1, 2, 10, 0), "cbg_mg_dl": 100.0, "is_plausible": True},
    # user_b Jan 1 (cohort row is not an outcome day): dropped.
    {"_userId": "user_b", "cbg_timestamp": datetime(2024, 1, 1, 10, 0), "cbg_mg_dl": 100.0, "is_plausible": True},
    # user_c: no cohort rows at all: dropped.
    {"_userId": "user_c", "cbg_timestamp": datetime(2024, 1, 1, 10, 0), "cbg_mg_dl": 100.0, "is_plausible": True},
]

# --- Run test ---
try:
    setup_test_table(spark, LOOP_CBG_TABLE, loop_cbg_rows)
    setup_test_table(spark, COHORT_TABLE, cohort_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_cbg_table=LOOP_CBG_TABLE,
        ab_day_cohort_table=COHORT_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only user_a's three plausible Jan 1 readings survive.
    assert_row_count(result, 3, "ab_day_cbg rows")
    assert set(result["_userId"]) == {"user_a"}, f"unexpected users: {set(result['_userId'])}"
    print("PASS: outcome-day, plausibility, and user filters")

    # 2. The day column is DATE(cbg_timestamp) — all Jan 1, including the
    #    23:55 reading.
    assert all(result["day"] == date(2024, 1, 1)), f"days: {set(result['day'])}"
    assert sorted(float(v) for v in result["cbg_mg_dl"]) == [100.0, 110.0, 120.0], (
        f"values: {sorted(result['cbg_mg_dl'])}"
    )
    print("PASS: day column from DATE(cbg_timestamp); implausible reading dropped")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
