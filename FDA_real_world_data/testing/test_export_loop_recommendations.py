"""
Unit test for export_loop_recommendations.py.

Tests: reason filtering, timestamp parsing, is_autobolus classification,
day extraction, and loop_version column. Version is no longer filtered —
all versions appear with loop_version populated.

Run on Databricks.
"""

import json
import sys
from datetime import date

from pyspark.sql import SparkSession

# Allow imports from sibling directories
sys.path.insert(0, "../data_staging")

from export_loop_recommendations import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_column_values,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
INPUT_TABLE = f"{TEST_SCHEMA}._test_input_loop_recs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_loop_recs"

# --- Test data: 6 rows covering 5 behaviors ---
TEST_ROWS = [
    {  # Row 1: temp basal — included, is_autobolus=0
        "_userId": "user_a",
        "time_string": "2025-01-15 12:00:00",
        "reason": "loop",
        "origin": json.dumps({"version": "3.2.0"}),
        "recommendedBasal": 1.0,
        "recommendedBolus": None,
    },
    {  # Row 2: autobolus — included, is_autobolus=1
        "_userId": "user_a",
        "time_string": "2025-01-15 12:05:00",
        "reason": "loop",
        "origin": json.dumps({"version": "3.2.0"}),
        "recommendedBasal": None,
        "recommendedBolus": 0.5,
    },
    {  # Row 3: both NULL — included, is_autobolus=NULL
        "_userId": "user_a",
        "time_string": "2025-01-15 12:10:00",
        "reason": "loop",
        "origin": json.dumps({"version": "3.2.0"}),
        "recommendedBasal": None,
        "recommendedBolus": None,
    },
    {  # Row 4: bad timestamp — excluded
        "_userId": "user_a",
        "time_string": "not-a-timestamp",
        "reason": "loop",
        "origin": json.dumps({"version": "3.2.0"}),
        "recommendedBasal": 1.0,
        "recommendedBolus": None,
    },
    {  # Row 5: wrong reason — excluded
        "_userId": "user_a",
        "time_string": "2025-01-15 12:15:00",
        "reason": "other",
        "origin": json.dumps({"version": "3.2.0"}),
        "recommendedBasal": 1.0,
        "recommendedBolus": None,
    },
    {  # Row 6: version >= 3.4 — included (version no longer filtered)
        "_userId": "user_a",
        "time_string": "2025-01-15 12:20:00",
        "reason": "loop",
        "origin": json.dumps({"version": "3.4.0"}),
        "recommendedBasal": 1.0,
        "recommendedBolus": None,
    },
]


# --- Run test ---
try:
    setup_test_table(spark, INPUT_TABLE, TEST_ROWS)
    run(spark, input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 4 rows survive (rows 4 and 5 excluded: bad timestamp, wrong reason)
    assert_row_count(result, 4, "total rows after filtering")

    # 2. is_autobolus classification: {0, 1, None, 0} (row 6 is temp basal)
    assert set(result["is_autobolus"].dropna().tolist()) == {0, 1}, (
        f"is_autobolus values: expected {{0, 1}}, got {set(result['is_autobolus'].dropna().tolist())}"
    )
    print("PASS: is_autobolus — values are {0, 1, None}")

    # 3. All rows have day = 2025-01-15
    assert_column_values(
        result, "day", [date(2025, 1, 15)] * 4, "day extraction"
    )

    # 4. settings_time values are non-null timestamps
    assert result["settings_time"].notna().all(), "settings_time has unexpected NULLs"
    print("PASS: settings_time — all non-null")

    # 5. loop_version column populated
    assert "loop_version" in result.columns, "missing loop_version column"
    versions = set(result["loop_version"].tolist())
    assert versions == {"3.2.0", "3.4.0"}, f"expected versions {{3.2.0, 3.4.0}}, got {versions}"
    print("PASS: loop_version — both 3.2.0 and 3.4.0 present")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, INPUT_TABLE, OUTPUT_TABLE)
