"""
Unit test for export_valid_transition_segments.py.

Tests: sliding window transition detection, coverage thresholds, autobolus percentage
filtering, segment scoring, and age filtering.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession

sys.path.insert(0, "../data_staging")

from export_valid_transition_segments import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
LOOP_RECS_TABLE = f"{TEST_SCHEMA}._test_vts_loop_recs"
USER_DATES_TABLE = f"{TEST_SCHEMA}._test_vts_user_dates"
USER_GENDER_TABLE = f"{TEST_SCHEMA}._test_vts_user_gender"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_vts_output"

ALL_TABLES = [LOOP_RECS_TABLE, USER_DATES_TABLE, USER_GENDER_TABLE, OUTPUT_TABLE]

# --- Test data ---

# User A: clean transition at day 15 (14 days TB → 14 days AB, 288 rows/day = 100% coverage)
START = date(2025, 1, 1)
ROWS_PER_DAY = 288

loop_recs_rows = (
    make_loop_recs("user_transition", START, n_days=14, rows_per_day=ROWS_PER_DAY, is_autobolus=0)
    + make_loop_recs("user_transition", date(2025, 1, 15), n_days=14, rows_per_day=ROWS_PER_DAY, is_autobolus=1)
    # User B: 28 days all temp basal — no transition, should be excluded
    + make_loop_recs("user_no_transition", START, n_days=28, rows_per_day=ROWS_PER_DAY, is_autobolus=0)
    # User C: transition but under 6 years old — should be excluded by age filter
    + make_loop_recs("user_child", START, n_days=14, rows_per_day=ROWS_PER_DAY, is_autobolus=0)
    + make_loop_recs("user_child", date(2025, 1, 15), n_days=14, rows_per_day=ROWS_PER_DAY, is_autobolus=1)
)

user_dates_rows = [
    {"userid": "user_transition", "dob": "1990-01-01", "diagnosis_date": "2000-01-01"},
    {"userid": "user_no_transition", "dob": "1990-01-01", "diagnosis_date": "2000-01-01"},
    {"userid": "user_child", "dob": "2020-01-01", "diagnosis_date": "2023-01-01"},
]

user_gender_rows = [
    {"userid": "user_transition", "gender": "female"},
    {"userid": "user_no_transition", "gender": "male"},
    {"userid": "user_child", "gender": "female"},
]

# --- Run test ---
try:
    setup_test_table(spark, LOOP_RECS_TABLE, loop_recs_rows)
    setup_test_table(spark, USER_DATES_TABLE, user_dates_rows)
    setup_test_table(spark, USER_GENDER_TABLE, user_gender_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=LOOP_RECS_TABLE,
        user_dates_table=USER_DATES_TABLE,
        user_gender_table=USER_GENDER_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only user_transition passes (no_transition has no AB, child excluded by age)
    assert_row_count(result, 1, "valid transition users")

    # 2. Correct user
    assert result.iloc[0]["_userId"] == "user_transition", (
        f"expected user_transition, got {result.iloc[0]['_userId']}"
    )
    print("PASS: correct user selected")

    # 3. Segment 1 had low autobolus (< 0.30)
    pct_seg1 = result.iloc[0]["tb_to_ab_pct_seg1"]
    assert pct_seg1 < 0.30, f"seg1 autobolus_pct should be < 0.30, got {pct_seg1}"
    print(f"PASS: seg1 autobolus_pct = {pct_seg1} (< 0.30)")

    # 4. Segment 2 had high autobolus (> 0.70)
    pct_seg2 = result.iloc[0]["tb_to_ab_pct_seg2"]
    assert pct_seg2 > 0.70, f"seg2 autobolus_pct should be > 0.70, got {pct_seg2}"
    print(f"PASS: seg2 autobolus_pct = {pct_seg2} (> 0.70)")

    # 5. Coverage thresholds met (both segments should be ~1.0)
    cov_seg1 = result.iloc[0]["tb_to_ab_coverage_seg1"]
    cov_seg2 = result.iloc[0]["tb_to_ab_coverage_seg2"]
    assert cov_seg1 >= 0.70, f"seg1 coverage should be >= 0.70, got {cov_seg1}"
    assert cov_seg2 >= 0.70, f"seg2 coverage should be >= 0.70, got {cov_seg2}"
    print(f"PASS: coverage seg1={cov_seg1}, seg2={cov_seg2} (both >= 0.70)")

    # 6. Demographics joined correctly
    assert result.iloc[0]["gender"] == "female", "gender should be female"
    assert result.iloc[0]["tb_to_ab_age_years"] > 6, "age should be > 6"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
