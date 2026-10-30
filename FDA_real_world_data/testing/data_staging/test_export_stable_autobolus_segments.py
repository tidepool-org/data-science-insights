"""
Unit test for export_stable_autobolus_segments.py.

Tests: a user's segment is the earliest 14-day window in which every calendar
day is an autobolus day, starting at least 30 days after the user's first
autobolus day (PLN-1001 Analysis 6; MC, 2026-10-30). One row per qualifying
user; every other user is absent.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_stable_autobolus_segments import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
RECS_TABLE = f"{TEST_SCHEMA}._test_sas_recs"
DATES_TABLE = f"{TEST_SCHEMA}._test_sas_dates"
GENDER_TABLE = f"{TEST_SCHEMA}._test_sas_gender"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_sas_output"

ALL_TABLES = [RECS_TABLE, DATES_TABLE, GENDER_TABLE, OUTPUT_TABLE]

# --- Test data ---
# Production WHERE clause (export_stable_autobolus_segments.py:105-106) filters on:
#   (a) autobolus_days = 14 — all 14 calendar days of the window are AB days; a
#       day with no loop_recommendations row fails it
#   (b) days_since_first_ab >= 30 — segment starts ≥30 days after the user's first AB day
# Each negative-control user below targets exactly ONE of those gates so a regression
# in either is attributable. Days are counted from 2025-01-01 (day 0).
#
# user_qualifies:       45 consecutive AB days (days 0-44) → passes both gates; the
#                       earliest window is days 30-43.
# user_gap_then_window: AB days 0-34 and 36-55, no row on day 35 → the windows starting
#                       on days 30-35 hold 13 days, so the earliest is days 36-49.
# user_short_followup:  5 consecutive AB days → days_since_first_ab never reaches 30.
# user_starts_day_29:   43 consecutive AB days (days 0-42) → the latest full window
#                       starts on day 29, one day short; it passed the former 28-day gate.
# user_missing_day:     AB days 0-44 with no row on day 37 → both windows starting on or
#                       after day 30 hold 13 days; it passed the former rule, which
#                       counted only the days with data.
# user_partial_ab:      1 AB day then 44 TB days → ≥30 days post-first-AB, but no 14-day
#                       window is fully AB.
# user_no_ab:           45 consecutive TB days → no first_ab_day at all; safety net for
#                       the LEFT JOIN path.
recs_rows = (
    make_loop_recs("user_qualifies", date(2025, 1, 1), n_days=45, dosing_mode="autobolus")
    + make_loop_recs("user_gap_then_window", date(2025, 1, 1), n_days=35, dosing_mode="autobolus")
    + make_loop_recs("user_gap_then_window", date(2025, 2, 6), n_days=20, dosing_mode="autobolus")
    + make_loop_recs("user_short_followup", date(2025, 1, 1), n_days=5, dosing_mode="autobolus")
    + make_loop_recs("user_starts_day_29", date(2025, 1, 1), n_days=43, dosing_mode="autobolus")
    + make_loop_recs("user_missing_day", date(2025, 1, 1), n_days=37, dosing_mode="autobolus")
    + make_loop_recs("user_missing_day", date(2025, 2, 8), n_days=7, dosing_mode="autobolus")
    + make_loop_recs("user_partial_ab", date(2025, 1, 1), n_days=1, dosing_mode="autobolus")
    + make_loop_recs("user_partial_ab", date(2025, 1, 2), n_days=44, dosing_mode="temp_basal")
    + make_loop_recs("user_no_ab", date(2025, 1, 1), n_days=45, dosing_mode="temp_basal")
)

dates_rows = [
    {"userid": "user_qualifies", "dob": date(1990, 6, 15), "diagnosis_date": date(2005, 3, 1)},
    {"userid": "user_gap_then_window", "dob": date(1987, 1, 1), "diagnosis_date": date(2001, 1, 1)},
    {"userid": "user_short_followup", "dob": date(1985, 1, 1), "diagnosis_date": date(2000, 1, 1)},
    {"userid": "user_starts_day_29", "dob": date(1991, 1, 1), "diagnosis_date": date(2006, 1, 1)},
    {"userid": "user_missing_day", "dob": date(1989, 1, 1), "diagnosis_date": date(2004, 1, 1)},
    {"userid": "user_partial_ab", "dob": date(1988, 1, 1), "diagnosis_date": date(2003, 1, 1)},
    {"userid": "user_no_ab", "dob": date(1992, 1, 1), "diagnosis_date": date(2010, 1, 1)},
]

gender_rows = [
    {"userid": "user_qualifies", "gender": "F"},
    {"userid": "user_gap_then_window", "gender": "M"},
    {"userid": "user_short_followup", "gender": "M"},
    {"userid": "user_starts_day_29", "gender": "F"},
    {"userid": "user_missing_day", "gender": "F"},
    {"userid": "user_partial_ab", "gender": "M"},
    {"userid": "user_no_ab", "gender": "F"},
]

# --- Run test ---
try:
    setup_test_table(spark, RECS_TABLE, recs_rows)
    setup_test_table(spark, DATES_TABLE, dates_rows)
    setup_test_table(spark, GENDER_TABLE, gender_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=RECS_TABLE,
        user_dates_table=DATES_TABLE,
        user_gender_table=GENDER_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only the two users with a full window starting ≥30 days after first AB survive.
    user_ids = set(result["_userId"].tolist())
    assert user_ids == {"user_qualifies", "user_gap_then_window"}, (
        f"expected user_qualifies and user_gap_then_window, got {user_ids}"
    )
    print(f"PASS: only the two qualifying users after filters ({len(result)} row(s))")

    # 2. Exactly one row per user (QUALIFY picks earliest qualifying segment).
    for user in ("user_qualifies", "user_gap_then_window"):
        n_rows = int((result["_userId"] == user).sum())
        assert n_rows == 1, f"expected 1 row for {user}, got {n_rows}"
    print("PASS: one row per user")

    # 3. Earliest qualifying window: first_ab_day=2025-01-01, so segment_start must be
    #    >= 2025-01-31 (30 days later). For user_qualifies that window is
    #    2025-01-31..2025-02-13; for user_gap_then_window every window that holds the
    #    missing day 2025-02-05 fails, so it is 2025-02-06..2025-02-19.
    expected = {
        "user_qualifies": ("2025-01-31", "2025-02-13", 30),
        "user_gap_then_window": ("2025-02-06", "2025-02-19", 36),
    }
    for user, (start, end, days_since) in expected.items():
        row = result[result["_userId"] == user].iloc[0]
        assert str(row["segment_start"]) == start, (
            f"{user}: expected segment_start {start}, got {row['segment_start']}"
        )
        assert str(row["segment_end"]) == end, (
            f"{user}: expected segment_end {end}, got {row['segment_end']}"
        )
        assert int(row["days_since_first_ab"]) == days_since, (
            f"{user}: expected days_since_first_ab={days_since}, got {row['days_since_first_ab']}"
        )
        print(f"PASS: {user} earliest full window {start}..{end} ({days_since} days after first AB)")

    # 4. Full 14/14 days of data, all autobolus.
    for user in expected:
        row = result[result["_userId"] == user].iloc[0]
        assert int(row["days_with_data"]) == 14, f"{user}: days_with_data {row['days_with_data']} != 14"
        assert float(row["coverage"]) == 1.0, f"{user}: coverage {row['coverage']} != 1.0"
        assert float(row["autobolus_pct"]) == 1.0, f"{user}: autobolus_pct {row['autobolus_pct']} != 1.0"
    print("PASS: days_with_data=14, coverage=1.0, autobolus_pct=1.0")
    row = result[result["_userId"] == "user_qualifies"].iloc[0]

    # 5. Demographics joined correctly.
    assert row["gender"] == "F", f"expected gender F, got {row['gender']}"
    assert row["dob"] is not None, "dob should be populated"
    assert float(row["age_years"]) > 30, f"age_years {row['age_years']} should be > 30"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
