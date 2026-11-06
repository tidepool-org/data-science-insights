"""
Unit test for export_cbg_from_transitions.py.

Tests: CBG assignment to seg1/seg2/seg3, exclusion of CBG outside segment windows,
exclusion of CBG in the gap between segments, and non-matching users.

Run on Databricks.
"""

import sys
from datetime import date, datetime

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
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

# --- Transition segments: seg1 = Jan 1-14, seg2 = Jan 15-28, seg3 = Jan 29 - Feb 11 ---
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
    {  # Row 4: in seg3 — included as tb_to_ab_seg3 (2026-11-06; IR-6 reads it)
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 29, 12, 0),
        "cbg_mg_dl": 130.0,
    },
    {  # Row 6: after seg3 — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 2, 12, 12, 0),
        "cbg_mg_dl": 135.0,
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

    # 1. Only 3 rows: the seg1, seg2, and seg3 readings of user_a
    assert_row_count(result, 3, "CBG rows within the three periods")

    # 2. Correct segment labels
    assert_column_values(
        result, "segment", ["tb_to_ab_seg1", "tb_to_ab_seg2", "tb_to_ab_seg3"], "segment labels"
    )

    # 3. Only user_a
    assert_column_values(result, "_userId", ["user_a", "user_a", "user_a"], "user filtering")

    # 4. Correct glucose values mapped to correct segments
    seg1 = result[result["segment"] == "tb_to_ab_seg1"]
    assert seg1.iloc[0]["cbg_mg_dl"] == 120.0, "seg1 should have 120 mg/dL"
    seg2 = result[result["segment"] == "tb_to_ab_seg2"]
    assert seg2.iloc[0]["cbg_mg_dl"] == 140.0, "seg2 should have 140 mg/dL"
    seg3 = result[result["segment"] == "tb_to_ab_seg3"]
    assert seg3.iloc[0]["cbg_mg_dl"] == 130.0, "seg3 should have 130 mg/dL"
    print("PASS: glucose values correctly assigned to segments")

    # 5. Segment identifier and rank carried through
    assert_column_values(
        result, "tb_to_ab_seg1_start",
        [date(2025, 1, 1)] * 3,
        "tb_to_ab_seg1_start propagated",
    )
    assert_column_values(
        result, "segment_rank", [1, 1, 1], "segment_rank propagated",
    )

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
