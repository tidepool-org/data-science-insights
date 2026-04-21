"""
Unit test for export_carbohydrates_from_transitions.py.

Tests: carbohydrate extraction from nutrition JSON, segment assignment,
and filtering by type, date range, and user.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

import os
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_carbohydrates_from_transitions import run  # type: ignore # noqa: E402
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
BDDP_TABLE = f"{TEST_SCHEMA}._test_carbs_bddp"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_carbs_segments"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_carbs_output"

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

bddp_rows = [
    {  # Row 1: food in seg1 — included
        "_userId": "user_a",
        "time_string": "2025-01-05 12:00:00",
        "type": "food",
        "nutrition": '{"carbohydrate": {"net": 45.0}}',
    },
    {  # Row 2: food in seg2 — included
        "_userId": "user_a",
        "time_string": "2025-01-20 12:00:00",
        "type": "food",
        "nutrition": '{"carbohydrate": {"net": 30.0}}',
    },
    {  # Row 3: food before seg1 — excluded
        "_userId": "user_a",
        "time_string": "2024-12-31 12:00:00",
        "type": "food",
        "nutrition": '{"carbohydrate": {"net": 20.0}}',
    },
    {  # Row 4: food after seg2 — excluded
        "_userId": "user_a",
        "time_string": "2025-01-29 12:00:00",
        "type": "food",
        "nutrition": '{"carbohydrate": {"net": 25.0}}',
    },
    {  # Row 5: non-food type — excluded
        "_userId": "user_a",
        "time_string": "2025-01-05 13:00:00",
        "type": "cbg",
        "nutrition": '{"carbohydrate": {"net": 10.0}}',
    },
    {  # Row 6: food with null nutrition — excluded
        "_userId": "user_a",
        "time_string": "2025-01-05 14:00:00",
        "type": "food",
        "nutrition": None,
    },
    {  # Row 7: wrong user — excluded
        "_userId": "user_b",
        "time_string": "2025-01-05 12:00:00",
        "type": "food",
        "nutrition": '{"carbohydrate": {"net": 50.0}}',
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

    # 1. Only 2 rows: seg1 and seg2 food entries for user_a
    assert_row_count(result, 2, "carb entries within transition segments")

    # 2. Correct segment assignment
    assert_column_values(result, "segment", ["tb_to_ab_seg1", "tb_to_ab_seg2"], "segment labels")

    # 3. Correct carb values
    seg1 = result[result["segment"] == "tb_to_ab_seg1"].iloc[0]
    seg2 = result[result["segment"] == "tb_to_ab_seg2"].iloc[0]
    assert float(seg1["carb_grams"]) == 45.0, f"seg1 carbs: expected 45.0, got {seg1['carb_grams']}"
    assert float(seg2["carb_grams"]) == 30.0, f"seg2 carbs: expected 30.0, got {seg2['carb_grams']}"
    print("PASS: correct carb_grams values per segment")

    # 4. Only user_a
    assert all(result["_userId"] == "user_a"), "expected only user_a"
    print("PASS: only user_a in results")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
