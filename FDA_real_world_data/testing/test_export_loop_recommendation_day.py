"""
Unit test for export_loop_recommendation_day.py.

Tests: bolus→dosingDecision temporal matching (±5s window),
basal→dosingDecision matching, autobolus priority over temp_basal,
non-loop reason exclusion, unmatched records exclusion, bad timestamp exclusion.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

sys.path.insert(0, "../data_staging")

from export_loop_recommendation_day import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_column_values,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

INPUT_TABLE = f"{TEST_SCHEMA}._test_rec_day_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_rec_day_output"

# --- Test data: 13 rows covering 7 behaviors ---
TEST_ROWS = [
    {  # Row 1: loop decision — matches bolus in row 2
        "_userId": "user_a",
        "time_string": "2025-01-15 12:00:00",
        "type": "dosingDecision",
        "reason": "loop",
    },
    {  # Row 2: bolus 5s after row 1 → autobolus day
        "_userId": "user_a",
        "time_string": "2025-01-15 12:00:05",
        "type": "bolus",
        "reason": None,
    },
    {  # Row 3: loop decision — matches basal only (no bolus this day)
        "_userId": "user_a",
        "time_string": "2025-01-16 08:00:00",
        "type": "dosingDecision",
        "reason": "loop",
    },
    {  # Row 4: basal 3s after row 3 → temp_basal day
        "_userId": "user_a",
        "time_string": "2025-01-16 08:00:03",
        "type": "basal",
        "reason": None,
    },
    {  # Row 5: loop decision — both bolus and basal match this day
        "_userId": "user_a",
        "time_string": "2025-01-17 10:00:00",
        "type": "dosingDecision",
        "reason": "loop",
    },
    {  # Row 6: bolus 3s after row 5 → autobolus wins
        "_userId": "user_a",
        "time_string": "2025-01-17 10:00:03",
        "type": "bolus",
        "reason": None,
    },
    {  # Row 7: basal 4s after row 5 → also matches, but autobolus takes priority
        "_userId": "user_a",
        "time_string": "2025-01-17 10:00:04",
        "type": "basal",
        "reason": None,
    },
    {  # Row 8: bolus with no matching decision → excluded
        "_userId": "user_b",
        "time_string": "2025-01-15 12:00:00",
        "type": "bolus",
        "reason": None,
    },
    {  # Row 9: non-loop decision → excluded
        "_userId": "user_a",
        "time_string": "2025-01-18 09:00:00",
        "type": "dosingDecision",
        "reason": "other",
    },
    {  # Row 10: bolus near non-loop decision → excluded
        "_userId": "user_a",
        "time_string": "2025-01-18 09:00:05",
        "type": "bolus",
        "reason": None,
    },
    {  # Row 11: bolus 10s after row 1 decision → outside ±5s window
        "_userId": "user_a",
        "time_string": "2025-01-15 12:00:10",
        "type": "bolus",
        "reason": None,
    },
    {  # Row 12: loop decision with bad-timestamp basal → basal excluded
        "_userId": "user_a",
        "time_string": "2025-01-19 14:00:00",
        "type": "dosingDecision",
        "reason": "loop",
    },
    {  # Row 13: bad timestamp → excluded
        "_userId": "user_a",
        "time_string": "not-a-timestamp",
        "type": "basal",
        "reason": None,
    },
]


# --- Run test ---
try:
    setup_test_table(spark, INPUT_TABLE, TEST_ROWS)
    run(spark, input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Three user-days survive (Jan 15, Jan 16, Jan 17)
    assert_row_count(result, 3, "total user-days")

    # 2. Two autobolus days (Jan 15 and Jan 17)
    ab_days = result[result["day_type"] == "autobolus"]
    assert_row_count(ab_days, 2, "autobolus days")
    assert_column_values(
        ab_days, "day",
        [date(2025, 1, 15), date(2025, 1, 17)],
        "autobolus dates",
    )

    # 3. One temp_basal day (Jan 16)
    tb_days = result[result["day_type"] == "temp_basal"]
    assert_row_count(tb_days, 1, "temp_basal days")
    assert_column_values(
        tb_days, "day",
        [date(2025, 1, 16)],
        "temp_basal date",
    )

    # 4. Only user_a appears
    assert result["_userId"].unique().tolist() == ["user_a"], (
        f"expected only user_a, got {result['_userId'].unique().tolist()}"
    )
    print("PASS: only user_a in output")

    # 5. day_type values are exactly the expected set
    assert set(result["day_type"].tolist()) == {"autobolus", "temp_basal"}, (
        f"unexpected day_type values: {set(result['day_type'].tolist())}"
    )
    print("PASS: day_type values are {'autobolus', 'temp_basal'}")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, INPUT_TABLE, OUTPUT_TABLE)
