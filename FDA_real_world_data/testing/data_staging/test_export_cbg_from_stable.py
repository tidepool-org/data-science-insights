"""
Unit test for export_cbg_from_stable.py.

Tests: CBG readings are filtered to the stable autobolus segment date range,
tagged as 'stable_ab', and non-matching users/dates are excluded.

Run on Databricks.
"""

import sys
from datetime import date, datetime

from pyspark.sql import SparkSession  # type: ignore

import os
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_cbg_from_stable import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
CBG_TABLE = f"{TEST_SCHEMA}._test_stable_cbg_input"
SEGMENTS_TABLE = f"{TEST_SCHEMA}._test_stable_segments"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_stable_cbg_output"

ALL_TABLES = [CBG_TABLE, SEGMENTS_TABLE, OUTPUT_TABLE]

# --- Test data ---
segments_rows = [
    {
        "_userId": "user_a",
        "segment_start": date(2025, 1, 1),
        "segment_end": date(2025, 1, 14),
    },
]

cbg_rows = [
    {  # Row 1: within segment — included
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 5, 12, 0),
        "cbg_mg_dl": 120.0,
    },
    {  # Row 2: before segment — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2024, 12, 31, 12, 0),
        "cbg_mg_dl": 110.0,
    },
    {  # Row 3: after segment — excluded
        "_userId": "user_a",
        "cbg_timestamp": datetime(2025, 1, 15, 12, 0),
        "cbg_mg_dl": 130.0,
    },
    {  # Row 4: different user (no segment) — excluded
        "_userId": "user_b",
        "cbg_timestamp": datetime(2025, 1, 5, 12, 0),
        "cbg_mg_dl": 115.0,
    },
]

# --- Run test ---
try:
    setup_test_table(spark, SEGMENTS_TABLE, segments_rows)
    setup_test_table(spark, CBG_TABLE, cbg_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_cbg_table=CBG_TABLE,
        stable_segments_table=SEGMENTS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Only 1 row: within-segment CBG for user_a
    assert_row_count(result, 1, "CBG readings within stable segment")

    # 2. Correct values
    row = result.iloc[0]
    assert row["_userId"] == "user_a", f"expected user_a, got {row['_userId']}"
    assert row["segment"] == "stable_ab", f"expected stable_ab, got {row['segment']}"
    assert float(row["cbg_mg_dl"]) == 120.0, f"expected 120.0, got {row['cbg_mg_dl']}"
    print("PASS: correct user, segment label, and glucose value")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
