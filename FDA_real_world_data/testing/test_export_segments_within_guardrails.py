"""
Unit test for export_segments_within_guardrails.py (transition mode).

Tests: pump settings extraction via segment join, guardrail validation
(valid settings pass, violations detected), and output schema.

Run on Databricks.
"""

import json
import sys
from datetime import date

from pyspark.sql import SparkSession

sys.path.insert(0, "../data_staging")

from export_segments_within_guardrails import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
INPUT_TABLE = f"{TEST_SCHEMA}._test_gr_input"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_gr_segments"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_gr_output"

ALL_TABLES = [INPUT_TABLE, SEGMENTS_TABLE, OUTPUT_TABLE]

# --- Transition segments ---
segments_rows = [
    {
        "_userId": "user_a",
        "tb_to_ab_seg1_start": date(2025, 1, 1),
        "tb_to_ab_seg1_end": date(2025, 1, 14),
        "tb_to_ab_seg2_start": date(2025, 1, 15),
        "tb_to_ab_seg2_end": date(2025, 1, 28),
    },
]

# --- Pump settings (bddp_sample_all_2 schema) ---
VALID_SETTINGS = {
    "basal": json.dumps({"rateMaximum": {"value": 5.0}}),
    "basalSchedules": json.dumps({"default": [{"rate": 1.0, "start": 0}]}),
    "bgTargets": json.dumps({"default": [{"low": 5.0, "high": 6.1, "start": 0}]}),
    "bgTargetsPreprandial": "{}",
    "bgTargetsWorkout": "{}",
    "bgSafetyLimit": 4.2,  # mmol/L → ~75.6 mg/dL, within 67-110
    "insulinSensitivities": json.dumps({"default": [{"amount": 2.8, "start": 0}]}),
    "bolus": json.dumps({"amountMaximum": {"value": 10.0}}),
    "carbRatios": json.dumps({"default": [{"amount": 15.0, "start": 0}]}),
}

input_rows = [
    {  # Row 1: valid pump settings within segment window
        "_userId": "user_a",
        "created_timestamp": "2025-01-10 08:00:00",
        "type": "pumpSettings",
        **VALID_SETTINGS,
    },
    {  # Row 2: settings with basal rate violation (35 > 30 max)
        "_userId": "user_a",
        "created_timestamp": "2025-01-20 08:00:00",
        "type": "pumpSettings",
        **{**VALID_SETTINGS, "basal": json.dumps({"rateMaximum": {"value": 35.0}})},
    },
    {  # Row 3: settings outside segment window — excluded by join
        "_userId": "user_a",
        "created_timestamp": "2025-02-15 08:00:00",
        "type": "pumpSettings",
        **VALID_SETTINGS,
    },
    {  # Row 4: non-pumpSettings type — excluded by WHERE
        "_userId": "user_a",
        "created_timestamp": "2025-01-10 08:00:00",
        "type": "cbg",
        **VALID_SETTINGS,
    },
]


# --- Run test ---
try:
    setup_test_table(spark, INPUT_TABLE, input_rows)
    setup_test_table(spark, SEGMENTS_TABLE, segments_rows)

    run(
        spark,
        mode="transition",
        input_table=INPUT_TABLE,
        segments_table=SEGMENTS_TABLE,
        output_table=OUTPUT_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only 2 rows: rows 3 and 4 excluded (outside window / wrong type)
    assert_row_count(result, 2, "pump settings within segment window")

    # 2. Row with valid settings has no violations
    valid_row = result[result["violation_count"] == 0]
    assert len(valid_row) == 1, f"expected 1 valid row, got {len(valid_row)}"
    assert valid_row.iloc[0]["all_valid"] == True, "valid row should have all_valid=True"  # noqa: E712
    print("PASS: valid settings have zero violations")

    # 3. Row with violation detected
    violation_row = result[result["violation_count"] > 0]
    assert len(violation_row) == 1, f"expected 1 violation row, got {len(violation_row)}"
    assert violation_row.iloc[0]["basal_valid"] == False, "basal should be invalid"  # noqa: E712
    assert violation_row.iloc[0]["basal_rate_max"] == 35.0, "basal_rate_max should be 35.0"
    print("PASS: basal rate violation detected")

    # 4. Output has expected columns
    expected_cols = ["_userId", "all_valid", "violation_count", "basal_rate_max", "basal_valid"]
    for col in expected_cols:
        assert col in result.columns, f"missing column: {col}"
    print("PASS: output schema correct")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
