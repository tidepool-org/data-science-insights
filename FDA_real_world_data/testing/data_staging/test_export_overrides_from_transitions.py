"""
Unit test for export_overrides_from_transitions.py.

Tests: override extraction, segment assignment, dosing_mode classification,
name-only and full-config validity checks, and exclusion of out-of-range
and null-preset rows.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

import os
_here = os.path.dirname(os.path.abspath(__file__))
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
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_or_trans_output"

ALL_TABLES = [BDDP_TABLE, SEGMENTS_TABLE, OUTPUT_TABLE]

# --- Test data ---
segments_rows = [
    {
        "_userId": "user_a",
        "tb_to_ab_seg1_start": date(2025, 1, 1),
        "tb_to_ab_seg1_end": date(2025, 1, 14),
        "tb_to_ab_seg2_start": date(2025, 1, 15),
        "tb_to_ab_seg2_end": date(2025, 1, 28),
    },
]

# Exercise preset: 2 in seg1 + 2 in seg2, same params → valid (name + full)
# Sleep preset: 1 in seg1 + 1 in seg2 → invalid (< 2 per segment)
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
    {"_userId": "user_a", "time_string": "2025-01-03 10:00:00", **EXERCISE},
    {"_userId": "user_a", "time_string": "2025-01-05 10:00:00", **EXERCISE},
    # Exercise in seg2 (×2)
    {"_userId": "user_a", "time_string": "2025-01-17 10:00:00", **EXERCISE},
    {"_userId": "user_a", "time_string": "2025-01-19 10:00:00", **EXERCISE},
    # Sleep in seg1 (×1)
    {"_userId": "user_a", "time_string": "2025-01-04 22:00:00", **SLEEP},
    # Sleep in seg2 (×1)
    {"_userId": "user_a", "time_string": "2025-01-16 22:00:00", **SLEEP},
    # Outside segments — excluded
    {"_userId": "user_a", "time_string": "2025-01-30 10:00:00", **EXERCISE},
    # Null preset — excluded by WHERE overridePreset IS NOT NULL
    {
        "_userId": "user_a",
        "time_string": "2025-01-05 14:00:00",
        "overridePreset": None,
        "basalRateScaleFactor": None,
        "bgTarget": None,
        "carbRatioScaleFactor": None,
        "insulinSensitivityScaleFactor": None,
        "duration": None,
    },
]

# --- Run test ---
try:
    setup_test_table(spark, SEGMENTS_TABLE, segments_rows)
    setup_test_table(spark, BDDP_TABLE, bddp_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        bddp_table=BDDP_TABLE,
        transition_segments_table=SEGMENTS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 6 rows: 4 Exercise + 2 Sleep (outside-segments and null-preset excluded)
    assert_row_count(result, 6, "overrides within transition segments")

    # 2. Exercise: is_valid_name_only=True, is_valid_full=True
    exercise = result[result["overridePreset"] == "Exercise"]
    assert len(exercise) == 4, f"expected 4 Exercise rows, got {len(exercise)}"
    assert all(exercise["is_valid_name_only"]), "Exercise should be valid (name only)"
    assert all(exercise["is_valid_full"]), "Exercise should be valid (full config)"
    print("PASS: Exercise preset valid (name + full)")

    # 3. Sleep: is_valid_name_only=False, is_valid_full=False
    sleep = result[result["overridePreset"] == "Sleep"]
    assert len(sleep) == 2, f"expected 2 Sleep rows, got {len(sleep)}"
    assert not any(sleep["is_valid_name_only"]), "Sleep should be invalid (name only)"
    assert not any(sleep["is_valid_full"]), "Sleep should be invalid (full config)"
    print("PASS: Sleep preset invalid (< 2 per segment)")

    # 4. Correct dosing_mode: seg1 = temp_basal, seg2 = autobolus
    seg1 = result[result["segment"] == "tb_to_ab_seg1"]
    seg2 = result[result["segment"] == "tb_to_ab_seg2"]
    assert all(seg1["dosing_mode"] == "temp_basal"), "seg1 dosing_mode should be temp_basal"
    assert all(seg2["dosing_mode"] == "autobolus"), "seg2 dosing_mode should be autobolus"
    print("PASS: correct dosing_mode per segment")

    # 5. bg_target values converted to mg/dL (8.3 * 18.018 ≈ 149.5)
    ex_row = exercise.iloc[0]
    btl = float(ex_row["bg_target_low"])
    assert abs(btl - 8.3 * 18.018) < 1.0, f"bg_target_low: expected ~149.5, got {btl}"
    print(f"PASS: bg_target_low converted to mg/dL ({btl:.1f})")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
