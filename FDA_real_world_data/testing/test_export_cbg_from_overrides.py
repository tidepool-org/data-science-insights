"""
Unit test for export_cbg_from_overrides.py.

Tests: CBG readings are matched to all overrides within the time window
(override duration + 2-hour buffer). Both valid and invalid overrides
appear with is_valid_name_only/is_valid_full columns for downstream filtering.

Run on Databricks.
"""

import sys
from datetime import datetime

from pyspark.sql import SparkSession

sys.path.insert(0, "../data_staging")

from export_cbg_from_overrides import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
OVERRIDES_TABLE = f"{TEST_SCHEMA}._test_or_cbg_overrides"
CBG_TABLE = f"{TEST_SCHEMA}._test_or_cbg_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_or_cbg_output"

ALL_TABLES = [OVERRIDES_TABLE, CBG_TABLE, OUTPUT_TABLE]

# --- Test data ---
# Override 1: valid, Exercise, 1-hour duration starting 10:00 Jan 5
#   Window: 10:00 to 13:00 (10:00 + 1hr + 2hr buffer)
# Override 2: invalid (is_valid_name_only=False), Sleep, 8-hour duration starting 10:00 Jan 6
overrides_rows = [
    {
        "_userId": "user_a",
        "override_time": datetime(2025, 1, 5, 10, 0, 0),
        "overridePreset": "Exercise",
        "basalRateScaleFactor": 0.5,
        "bg_target_low": 150.0,
        "bg_target_high": 160.0,
        "carbRatioScaleFactor": 1.0,
        "insulinSensitivityScaleFactor": 1.5,
        "duration": 3600,
        "dosing_mode": "autobolus",
        "segment": "tb_to_ab_seg2",
        "is_valid_name_only": True,
        "is_valid_full": True,
    },
    {
        "_userId": "user_a",
        "override_time": datetime(2025, 1, 6, 10, 0, 0),
        "overridePreset": "Sleep",
        "basalRateScaleFactor": 0.8,
        "bg_target_low": 100.0,
        "bg_target_high": 110.0,
        "carbRatioScaleFactor": 1.0,
        "insulinSensitivityScaleFactor": 1.0,
        "duration": 28800,
        "dosing_mode": "temp_basal",
        "segment": "tb_to_ab_seg1",
        "is_valid_name_only": False,
        "is_valid_full": False,
    },
]

cbg_rows = [
    {  # Row 1: within override window (11:00, between 10:00–13:00) — included
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 11, 0),
        "cbg_mg_dl": 140.0,
    },
    {  # Row 2: within 2h buffer (12:30, after 1hr override but before 13:00) — included
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 12, 30),
        "cbg_mg_dl": 135.0,
    },
    {  # Row 3: before override start — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 9, 0),
        "cbg_mg_dl": 130.0,
    },
    {  # Row 4: after override + buffer (13:05 > 13:00) — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 13, 5),
        "cbg_mg_dl": 145.0,
    },
    {  # Row 5: matches invalid override time window — included with is_valid_name_only=False
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 6, 12, 0),
        "cbg_mg_dl": 150.0,
    },
    {  # Row 6: different user — excluded
        "_userId": "user_b",
        "cbg_timestamp": datetime(2025, 1, 5, 11, 0),
        "cbg_mg_dl": 120.0,
    },
]

# --- Run test ---
try:
    setup_test_table(spark, OVERRIDES_TABLE, overrides_rows)
    setup_test_table(spark, CBG_TABLE, cbg_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        overrides_table=OVERRIDES_TABLE,
        loop_cbg_table=CBG_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 3 rows: 2 from valid Exercise override + 1 from invalid Sleep override
    assert_row_count(result, 3, "CBG readings within all override windows")

    # 2. Both valid and invalid overrides present
    presets = sorted(result["overridePreset"].unique().tolist())
    assert presets == ["Exercise", "Sleep"], f"expected [Exercise, Sleep], got {presets}"
    print("PASS: both valid and invalid overrides included")

    # 3. is_valid_name_only flag distinguishes them
    valid_rows = result[result["is_valid_name_only"] == True]  # noqa: E712
    invalid_rows = result[result["is_valid_name_only"] == False]  # noqa: E712
    assert len(valid_rows) == 2, f"expected 2 valid rows, got {len(valid_rows)}"
    assert len(invalid_rows) == 1, f"expected 1 invalid row, got {len(invalid_rows)}"
    assert invalid_rows.iloc[0]["overridePreset"] == "Sleep", "invalid row should be Sleep"
    print("PASS: is_valid_name_only correctly flags valid/invalid overrides")

    # 4. Correct CBG values
    cbg_values = sorted(result["cbg_mg_dl"].astype(float).tolist())
    assert cbg_values == [135.0, 140.0, 150.0], f"expected [135.0, 140.0, 150.0], got {cbg_values}"
    print("PASS: correct CBG values")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
