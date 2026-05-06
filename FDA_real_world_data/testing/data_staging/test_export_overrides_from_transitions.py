"""
Unit test for export_overrides_from_transitions.py.

Tests: override extraction, segment assignment, dosing_mode classification,
name-only and full-config validity checks, exclusion of out-of-range and
null-preset rows, and starting-glucose attachment + in-range flag.

Run on Databricks.
"""

import sys
from datetime import date, datetime

import pandas as pd
from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_overrides_from_transitions import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
BDDP_TABLE = f"{TEST_SCHEMA}._test_or_trans_bddp"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_or_trans_segments"
LOOP_CBG_TABLE = f"{TEST_SCHEMA}._test_or_trans_loop_cbg"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_or_trans_output"

ALL_TABLES = [BDDP_TABLE, SEGMENTS_TABLE, LOOP_CBG_TABLE, OUTPUT_TABLE]

# --- Test data ---
segments_rows = [
    {
        "_userId": "user_a",
        "tb_to_ab_seg1_start": date(2025, 1, 1),
        "tb_to_ab_seg1_end": date(2025, 1, 14),
        "tb_to_ab_seg2_start": date(2025, 1, 15),
        "tb_to_ab_seg2_end": date(2025, 1, 28),
        "tb_to_ab_seg3_start": date(2025, 1, 29),
        "tb_to_ab_seg3_end": date(2025, 2, 11),
        "segment_rank": 1,
    },
]

# Exercise preset: 2 in seg1 + 2 in seg2 + 2 in seg3, same params → valid for
# both seg2 and seg3 pairings (name + full).
# Sleep preset: 1 in seg1 + 1 in seg2 + 0 in seg3 → invalid for both pairings.
EXERCISE = {
    "overridePreset": "Exercise",
    "basalRateScaleFactor": "0.5",
    "bgTarget": '{"low": 8.3, "high": 8.9}',
    "carbRatioScaleFactor": "1.0",
    "insulinSensitivityScaleFactor": "1.5",
    "duration": "3600",
}

SLEEP = {
    "overridePreset": "Sleep",
    "basalRateScaleFactor": "0.8",
    "bgTarget": '{"low": 5.6, "high": 6.1}',
    "carbRatioScaleFactor": "1.0",
    "insulinSensitivityScaleFactor": "1.0",
    "duration": "28800",
}

bddp_rows = [
    # Exercise in seg1 (×2)
    # Jan 3 10:00: starting glucose 120 (in range)
    # Jan 5 10:00: starting glucose 200 (out of range)
    {"_userId": "user_a", "time_string": "2025-01-03 10:00:00", "created_timestamp": "2025-01-03 10:00:01", **EXERCISE},
    {"_userId": "user_a", "time_string": "2025-01-05 10:00:00", "created_timestamp": "2025-01-05 10:00:01", **EXERCISE},
    # Exercise in seg2 (×2)
    # Jan 17 10:00: starting glucose 90 (in range)
    # Jan 19 10:00: no CBG within 30 min (NULL starting glucose, not in range)
    {"_userId": "user_a", "time_string": "2025-01-17 10:00:00", "created_timestamp": "2025-01-17 10:00:01", **EXERCISE},
    {"_userId": "user_a", "time_string": "2025-01-19 10:00:00", "created_timestamp": "2025-01-19 10:00:01", **EXERCISE},
    # Exercise in seg3 (×2): both with in-range starting glucose
    {"_userId": "user_a", "time_string": "2025-01-31 10:00:00", "created_timestamp": "2025-01-31 10:00:01", **EXERCISE},
    {"_userId": "user_a", "time_string": "2025-02-02 10:00:00", "created_timestamp": "2025-02-02 10:00:01", **EXERCISE},
    # Sleep in seg1 (×1)
    {"_userId": "user_a", "time_string": "2025-01-04 22:00:00", "created_timestamp": "2025-01-04 22:00:01", **SLEEP},
    # Sleep in seg2 (×1)
    {"_userId": "user_a", "time_string": "2025-01-16 22:00:00", "created_timestamp": "2025-01-16 22:00:01", **SLEEP},
    # Outside segments — excluded (Feb 12 is past seg3 end Feb 11)
    {"_userId": "user_a", "time_string": "2025-02-12 10:00:00", "created_timestamp": "2025-02-12 10:00:01", **EXERCISE},
    # Null preset — excluded by WHERE overridePreset IS NOT NULL
    {
        "_userId": "user_a",
        "time_string": "2025-01-05 14:00:00",
        "created_timestamp": "2025-01-05 14:00:01",
        "overridePreset": None,
        "basalRateScaleFactor": None,
        "bgTarget": None,
        "carbRatioScaleFactor": None,
        "insulinSensitivityScaleFactor": None,
        "duration": None,
    },
]

# Loop CBG: covers some but not all overrides; stays well within the 30-min
# lookback window for the activations we want to test, and is intentionally
# absent for Jan 19 (the "no CBG in window" case).
loop_cbg_rows = [
    # Jan 3 10:00 Exercise activation — CBG at 09:55, 120 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 3, 9, 55), "cbg_mg_dl": 120.0},
    # Jan 5 10:00 Exercise activation — CBG at 09:50, 200 mg/dL (out of range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 5, 9, 50), "cbg_mg_dl": 200.0},
    # Jan 4 22:00 Sleep activation — CBG at 21:45, 110 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 4, 21, 45), "cbg_mg_dl": 110.0},
    # Jan 16 22:00 Sleep activation — CBG at 21:50, 95 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 16, 21, 50), "cbg_mg_dl": 95.0},
    # Jan 17 10:00 Exercise activation — CBG at 09:45, 90 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 17, 9, 45), "cbg_mg_dl": 90.0},
    # Stray reading just outside the 30-min window for Jan 19 10:00 (at 09:25,
    # 35 minutes earlier) — should NOT be picked up as starting glucose.
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 19, 9, 25), "cbg_mg_dl": 130.0},
    # Jan 31 10:00 Exercise activation — CBG at 09:50, 130 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 1, 31, 9, 50), "cbg_mg_dl": 130.0},
    # Feb 2 10:00 Exercise activation — CBG at 09:50, 100 mg/dL (in range)
    {"_userId": "user_a", "cbg_timestamp": datetime(2025, 2, 2, 9, 50), "cbg_mg_dl": 100.0},
]

# --- Run test ---
try:
    setup_test_table(spark, SEGMENTS_TABLE, segments_rows)
    setup_test_table(spark, BDDP_TABLE, bddp_rows)
    setup_test_table(spark, LOOP_CBG_TABLE, loop_cbg_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        bddp_table=BDDP_TABLE,
        transition_segments_table=SEGMENTS_TABLE,
        loop_cbg_table=LOOP_CBG_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 8 rows: 6 Exercise (2 seg1 + 2 seg2 + 2 seg3) + 2 Sleep (1 seg1 + 1 seg2);
    #    outside-segments and null-preset excluded.
    assert_row_count(result, 8, "overrides within transition segments")

    # 2. Exercise: valid for both seg2 and seg3 pairings (≥2 each in seg1, seg2, seg3).
    exercise = result[result["overridePreset"] == "Exercise"]
    assert len(exercise) == 6, f"expected 6 Exercise rows, got {len(exercise)}"
    assert all(exercise["is_valid_name_only_seg2"]), "Exercise should be valid_name_only_seg2"
    assert all(exercise["is_valid_name_only_seg3"]), "Exercise should be valid_name_only_seg3"
    assert all(exercise["is_valid_full_seg2"]), "Exercise should be valid_full_seg2"
    assert all(exercise["is_valid_full_seg3"]), "Exercise should be valid_full_seg3"
    print("PASS: Exercise preset valid for both seg2 and seg3 pairings")

    # 3. Sleep: 1 in seg1 + 1 in seg2 + 0 in seg3 → invalid for both pairings.
    sleep = result[result["overridePreset"] == "Sleep"]
    assert len(sleep) == 2, f"expected 2 Sleep rows, got {len(sleep)}"
    assert not any(sleep["is_valid_name_only_seg2"]), "Sleep should be invalid (seg2)"
    assert not any(sleep["is_valid_name_only_seg3"]), "Sleep should be invalid (seg3)"
    print("PASS: Sleep preset invalid for both pairings")

    # 4. Correct dosing_mode: seg1 = temp_basal, seg2 + seg3 = autobolus.
    seg1 = result[result["segment"] == "tb_to_ab_seg1"]
    seg2 = result[result["segment"] == "tb_to_ab_seg2"]
    seg3 = result[result["segment"] == "tb_to_ab_seg3"]
    assert all(seg1["dosing_mode"] == "temp_basal"), "seg1 dosing_mode should be temp_basal"
    assert all(seg2["dosing_mode"] == "autobolus"), "seg2 dosing_mode should be autobolus"
    assert all(seg3["dosing_mode"] == "autobolus"), "seg3 dosing_mode should be autobolus"
    assert len(seg3) == 2, f"expected 2 seg3 rows, got {len(seg3)}"
    print("PASS: correct dosing_mode per segment (seg1 / seg2 / seg3)")

    # 5. bg_target values converted to mg/dL (deterministic mmol → mg/dL conversion).
    ex_row = exercise.iloc[0]
    btl = float(ex_row["bg_target_low"])
    expected_btl = 8.3 * 18.018
    assert abs(btl - expected_btl) < 0.001, f"bg_target_low: expected {expected_btl}, got {btl}"
    print(f"PASS: bg_target_low converted to mg/dL ({btl:.4f})")

    # 6. Starting glucose attached + in-range flag works for all four cases.
    by_time = {r["override_time"]: r for _, r in result.iterrows()}

    jan3 = by_time[pd.Timestamp(2025, 1, 3, 10, 0)]
    assert float(jan3["starting_glucose"]) == 120.0, f"Jan 3 starting_glucose: {jan3['starting_glucose']}"
    assert bool(jan3["is_starting_glucose_in_range"]), "Jan 3 should be in-range (120)"

    jan5 = by_time[pd.Timestamp(2025, 1, 5, 10, 0)]
    assert float(jan5["starting_glucose"]) == 200.0, f"Jan 5 starting_glucose: {jan5['starting_glucose']}"
    assert not bool(jan5["is_starting_glucose_in_range"]), "Jan 5 should be out-of-range (200)"

    jan17 = by_time[pd.Timestamp(2025, 1, 17, 10, 0)]
    assert float(jan17["starting_glucose"]) == 90.0, f"Jan 17 starting_glucose: {jan17['starting_glucose']}"
    assert bool(jan17["is_starting_glucose_in_range"]), "Jan 17 should be in-range (90)"

    jan19 = by_time[pd.Timestamp(2025, 1, 19, 10, 0)]
    # 35-minute-earlier reading is outside the 30-min lookback window, so NULL.
    assert pd.isna(jan19["starting_glucose"]), (
        f"Jan 19 starting_glucose should be NULL, got {jan19['starting_glucose']}"
    )
    assert not bool(jan19["is_starting_glucose_in_range"]), "Jan 19 should be out-of-range (NULL)"
    print("PASS: starting_glucose + is_starting_glucose_in_range correctly attached")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
