"""
Unit test for export_cbg_from_transitions.py.

Tests: CBG assignment to seg1/seg2, exclusion of CBG outside segment windows,
exclusion of CBG in the gap between segments, and non-matching users.

Run on Databricks.
"""

import sys
from datetime import date, datetime

from pyspark.sql import SparkSession  # type: ignore

sys.path.insert(0, "../data_staging")

from export_cbg_from_transitions import run  # type: ignore # noqa: E402
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
CBG_TABLE = f"{TEST_SCHEMA}._test_cft_cbg"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_cft_segments"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_cft_output"

ALL_TABLES = [CBG_TABLE, SEGMENTS_TABLE, OUTPUT_TABLE]

# --- Transition segments: seg1 = Jan 1-14, seg2 = Jan 15-28 ---
segments_rows = [
    {
        "_userId": "user_a",
        "tb_to_ab_seg1_start": date(2025, 1, 1),
        "tb_to_ab_seg1_end": date(2025, 1, 14),
        "tb_to_ab_seg2_start": date(2025, 1, 15),
        "tb_to_ab_seg2_end": date(2025, 1, 28),
    },
]

# --- CBG data ---
cbg_rows = [
    {  # Row 1: in seg1 — included as tb_to_ab_seg1
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 12, 0),
        "cbg_mg_dl": 120.0,
    },
    {  # Row 2: in seg2 — included as tb_to_ab_seg2
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 20, 12, 0),
        "cbg_mg_dl": 140.0,
    },
    {  # Row 3: before seg1 — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2024, 12, 31, 12, 0),
        "cbg_mg_dl": 110.0,
    },
    {  # Row 4: after seg2 — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 29, 12, 0),
        "cbg_mg_dl": 130.0,
    },
    {  # Row 5: non-matching user — excluded
        "_userId": "user_b",
        "cbg_timestamp": datetime(2025, 1, 5, 12, 0),
        "cbg_mg_dl": 115.0,
    },
]


# --- Run test ---
try:
    setup_test_table(spark, CBG_TABLE, cbg_rows)
    setup_test_table(spark, SEGMENTS_TABLE, segments_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_cbg_table=CBG_TABLE,
        transition_segments_table=SEGMENTS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only 2 rows: seg1 and seg2 CBG readings for user_a
    assert_row_count(result, 2, "CBG rows within transition segments")

    # 2. Correct segment labels
    assert_column_values(
        result, "segment", ["tb_to_ab_seg1", "tb_to_ab_seg2"], "segment labels"
    )

    # 3. Only user_a
    assert_column_values(result, "_userId", ["user_a", "user_a"], "user filtering")

    # 4. Correct glucose values mapped to correct segments
    seg1 = result[result["segment"] == "tb_to_ab_seg1"]
    assert seg1.iloc[0]["cbg_mg_dl"] == 120.0, "seg1 should have 120 mg/dL"
    seg2 = result[result["segment"] == "tb_to_ab_seg2"]
    assert seg2.iloc[0]["cbg_mg_dl"] == 140.0, "seg2 should have 140 mg/dL"
    print("PASS: glucose values correctly assigned to segments")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
