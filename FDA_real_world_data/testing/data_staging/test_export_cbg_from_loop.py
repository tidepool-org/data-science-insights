"""
Unit test for export_cbg_from_loop.py.

Tests: Loop user filtering, cbg type filtering, mmol→mg/dL conversion,
is_plausible flag (38-500 mg/dL), 5-min bucket dedup, null/bad timestamp exclusion.

Run on Databricks.
"""

import sys

from pyspark.sql import SparkSession  # type: ignore

import os
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_cbg_from_loop import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
INPUT_TABLE = f"{TEST_SCHEMA}._test_cbg_input"
LOOP_RECS_TABLE = f"{TEST_SCHEMA}._test_cbg_loop_recs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_cbg_output"

ALL_TABLES = [INPUT_TABLE, LOOP_RECS_TABLE, OUTPUT_TABLE]

# --- Constants ---
MMOL_PER_MGDL = 1 / 18.018

# --- Test data ---
# Input mimics bddp_sample_all_2 schema: _userId, time_string, type, reason, value
TEST_ROWS = [
    {  # Row 1: valid CBG for a Loop user — included
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:00:00",
        "type": "cbg",
        "reason": None,
        "value": 120.0 * MMOL_PER_MGDL,
    },
    {  # Row 2: same user, same 5-min bucket, earlier timestamp — deduped out
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:02:00",
        "type": "cbg",
        "reason": None,
        "value": 130.0 * MMOL_PER_MGDL,
    },
    {  # Row 3: non-cbg row on loop_user — excluded by type filter
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:00:00",
        "type": None,
        "reason": "loop",
        "value": None,
    },
    {  # Row 4: CBG from non-Loop user — excluded
        "_userId": "non_loop_user",
        "time_string": "2025-01-15 12:00:00",
        "type": "cbg",
        "reason": None,
        "value": 100.0 * MMOL_PER_MGDL,
    },
    {  # Row 5: CBG below range (37 mg/dL < 38) — included, is_plausible=FALSE
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:20:00",
        "type": "cbg",
        "reason": None,
        "value": 37.0 * MMOL_PER_MGDL,
    },
    {  # Row 6: CBG above range (501 mg/dL > 500) — included, is_plausible=FALSE
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:10:00",
        "type": "cbg",
        "reason": None,
        "value": 501.0 * MMOL_PER_MGDL,
    },
    {  # Row 7: null value — excluded
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:15:00",
        "type": "cbg",
        "reason": None,
        "value": None,
    },
    {  # Row 8: bad timestamp — excluded
        "_userId": "loop_user",
        "time_string": "not-a-timestamp",
        "type": "cbg",
        "reason": None,
        "value": 100.0 * MMOL_PER_MGDL,
    },
    {  # Row 9: different 5-min bucket — included
        "_userId": "loop_user",
        "time_string": "2025-01-15 12:05:30",
        "type": "cbg",
        "reason": None,
        "value": 150.0 * MMOL_PER_MGDL,
    },
]


# Cohort source: loop_cbg now derives its Loop-user gate from loop_recommendations.
# Fixture lists loop_user only; non_loop_user is absent and so gets filtered out.
LOOP_RECS_ROWS = [{"_userId": "loop_user"}]


# --- Run test ---
try:
    setup_test_table(spark, INPUT_TABLE, TEST_ROWS)
    setup_test_table(spark, LOOP_RECS_TABLE, LOOP_RECS_ROWS)
    run(
        spark,
        input_table=INPUT_TABLE,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=LOOP_RECS_TABLE,
    )
    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 4 rows survive: row 2 wins dedup over row 1 in 12:00 bucket,
    #    row 5 (< 38), row 6 (> 500), row 9 in different buckets.
    #    Excluded: row 3 (not cbg), row 4 (non-Loop user), row 7 (null value), row 8 (bad timestamp)
    assert_row_count(result, 4, "rows after filtering and dedup")

    # 2. Only loop_user appears
    users = result["_userId"].unique().tolist()
    assert users == ["loop_user"], f"expected only loop_user, got {users}"
    print("PASS: only Loop users included")

    # 3. mg/dL conversion correct — dedup kept 130 (row 2, later timestamp) not 120 (row 1)
    mg_values = sorted(result["cbg_mg_dl"].round(0).tolist())
    assert mg_values == [37.0, 130.0, 150.0, 501.0], f"expected [37, 130, 150, 501], got {mg_values}"
    print("PASS: mmol/L → mg/dL conversion correct, out-of-range values retained")

    # 4. Dedup kept later timestamp (12:02 not 12:00 in the 12:00 bucket)
    bucket_12_00 = result[result["cbg_mg_dl"].round(0) == 130.0]
    assert len(bucket_12_00) == 1, "expected 1 row in 12:00 bucket (dedup winner)"
    print("PASS: 5-min bucket dedup kept correct row")

    # 5. is_plausible flag correct
    assert "is_plausible" in result.columns, "missing is_plausible column"
    plausible = result.set_index(result["cbg_mg_dl"].round(0))["is_plausible"]
    assert plausible[130.0] == True, "130 mg/dL should be plausible"  # noqa: E712
    assert plausible[150.0] == True, "150 mg/dL should be plausible"  # noqa: E712
    assert plausible[37.0] == False, "37 mg/dL should be implausible"  # noqa: E712
    assert plausible[501.0] == False, "501 mg/dL should be implausible"  # noqa: E712
    print("PASS: is_plausible flag correct for in-range and out-of-range values")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
