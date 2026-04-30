"""
Unit test for export_stable_autobolus_segments.py.

Tests: 14-day sliding window produces all scored windows (unfiltered,
auditable) with coverage, autobolus_pct, and days_since_first_ab.
All users appear — including low coverage and no-autobolus users.
Rows are ranked by segment_start per user.

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
# Production WHERE clause (export_stable_autobolus_segments.py:98-99) filters on:
#   (a) autobolus_pct = 1.0 — every day in the 14-day window is AB
#   (b) days_since_first_ab >= 28 — segment starts ≥28 days after the user's first AB day
# Each negative-control user below targets exactly ONE of those gates so a regression
# in either is attributable.
#
# user_qualifies:      45 consecutive AB days → passes both gates.
# user_short_followup: 5 consecutive AB days → days_since_first_ab never reaches 28.
# user_partial_ab:     1 AB day then 44 TB days → ≥28 days post-first-AB, but no 14-day
#                      window is fully AB → autobolus_pct < 1.0.
# user_no_ab:          45 consecutive TB days → no first_ab_day at all; safety net for
#                      the LEFT JOIN path.
recs_rows = (
    make_loop_recs("user_qualifies", date(2025, 1, 1), n_days=45, dosing_mode="autobolus")
    + make_loop_recs("user_short_followup", date(2025, 1, 1), n_days=5, dosing_mode="autobolus")
    + make_loop_recs("user_partial_ab", date(2025, 1, 1), n_days=1, dosing_mode="autobolus")
    + make_loop_recs("user_partial_ab", date(2025, 1, 2), n_days=44, dosing_mode="temp_basal")
    + make_loop_recs("user_no_ab", date(2025, 1, 1), n_days=45, dosing_mode="temp_basal")
)

dates_rows = [
    {"userid": "user_qualifies", "dob": date(1990, 6, 15), "diagnosis_date": date(2005, 3, 1)},
    {"userid": "user_short_followup", "dob": date(1985, 1, 1), "diagnosis_date": date(2000, 1, 1)},
    {"userid": "user_partial_ab", "dob": date(1988, 1, 1), "diagnosis_date": date(2003, 1, 1)},
    {"userid": "user_no_ab", "dob": date(1992, 1, 1), "diagnosis_date": date(2010, 1, 1)},
]

gender_rows = [
    {"userid": "user_qualifies", "gender": "F"},
    {"userid": "user_short_followup", "gender": "M"},
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

    # 1. Only user_qualifies survives both filter gates.
    user_ids = set(result["_userId"].tolist())
    assert user_ids == {"user_qualifies"}, (
        f"expected only user_qualifies, got {user_ids}"
    )
    print(f"PASS: only user_qualifies after filters ({len(result)} row(s))")

    # 2. Exactly one row per user (QUALIFY picks earliest qualifying segment).
    uq = result[result["_userId"] == "user_qualifies"]
    assert len(uq) == 1, f"expected 1 row for user_qualifies, got {len(uq)}"
    row = uq.iloc[0]
    print("PASS: one row per user")

    # 3. Earliest qualifying window: first_ab_day=2025-01-01, so segment_start must be
    #    >= 2025-01-29 (28 days later). The earliest sliding window ending on a day where
    #    segment_start crosses that boundary is day 2025-02-11 (window 2025-01-29..2025-02-11).
    assert str(row["segment_start"]) == "2025-01-29", (
        f"expected segment_start 2025-01-29, got {row['segment_start']}"
    )
    assert int(row["days_since_first_ab"]) == 28, (
        f"expected days_since_first_ab=28, got {row['days_since_first_ab']}"
    )
    print(f"PASS: earliest window after 28-day gap — segment_start={row['segment_start']}")

    # 4. Full 14/14 coverage and 100% autobolus.
    assert float(row["coverage"]) == 1.0, f"coverage {row['coverage']} != 1.0"
    assert float(row["autobolus_pct"]) == 1.0, f"autobolus_pct {row['autobolus_pct']} != 1.0"
    print("PASS: coverage=1.0, autobolus_pct=1.0")

    # 5. Demographics joined correctly.
    assert row["gender"] == "F", f"expected gender F, got {row['gender']}"
    assert row["dob"] is not None, "dob should be populated"
    assert float(row["age_years"]) > 30, f"age_years {row['age_years']} should be > 30"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
