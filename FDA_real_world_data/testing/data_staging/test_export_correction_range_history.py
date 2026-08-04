"""
Unit test for export_correction_range_history.py.

Tests: pumpSettings filtering, (user, settings_time) dedup by latest
created_timestamp, schedule selection (activeSchedule-named schedule first;
'Default' when the active name is absent or empty; first non-empty by sorted
name when 'Default' is absent too; singular bgTarget fallback), slot parsing
(ms -> s, mmol -> mg/dL, drop-on-missing start/low, nullable high), and
validity-interval chaining — including that schedule-less records (NULL fields
or unparseable JSON) emit nothing and do NOT terminate the prior schedule.

Run on Databricks.
"""

import sys

import pandas as pd
from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_correction_range_history import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
BDDP_TABLE = f"{TEST_SCHEMA}._test_crh_bddp"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_crh_output"

ALL_TABLES = [BDDP_TABLE, OUTPUT_TABLE]

# --- Test data ---
# mmol values chosen so the mg/dL conversions land on round numbers:
# 5.55 -> ~100.0, 6.105 -> ~110.0, 6.66 -> ~120.0, 7.77 -> ~140.0, 4.995 -> ~90.0.

# R1 (user_a, Jan 1): plural dict WITH 'Default' (two slots) plus a decoy
# schedule that must not be chosen.
R1_BGTARGETS = (
    '{"Default": [{"start": 0, "low": 5.55, "high": 6.66},'
    ' {"start": 28800000, "low": 6.105, "high": 6.66}],'
    ' "Zzz": [{"start": 0, "low": 9.99, "high": 9.99}]}'
)
# Stale duplicate at R1's settings_time (older created_timestamp, different
# schedule) — dedup must keep R1.
R1_STALE_BGTARGETS = '{"Default": [{"start": 0, "low": 7.77, "high": 8.88}]}'
# R2 (user_a, Feb 1): no 'Default'; 'Alt' is empty so the first non-empty by
# sorted name is 'Weekend'. Of its three slots only the first is usable
# (second lacks low, third lacks start).
R2_BGTARGETS = (
    '{"Weekend": [{"start": 0, "low": 4.995},'
    ' {"start": 43200000, "high": 7.77},'
    ' {"low": 5.0, "high": 6.0}],'
    ' "Alt": []}'
)
# R3 (user_a, Mar 1): plural NULL, singular flat array fallback.
R3_BGTARGET = '[{"start": 0, "low": 6.66, "high": 7.77}]'

bddp_rows = [
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-01-01 08:00:00",
     "created_timestamp": "2024-01-01 08:00:01", "bgTargets": R1_BGTARGETS, "bgTarget": None},
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-01-01 08:00:00",
     "created_timestamp": "2023-12-31 07:00:00", "bgTargets": R1_STALE_BGTARGETS, "bgTarget": None},
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-02-01 08:00:00",
     "created_timestamp": "2024-02-01 08:00:01", "bgTargets": R2_BGTARGETS, "bgTarget": None},
    # Two schedule-less records between R2 and R3 — must emit nothing and must
    # NOT terminate R2's validity: one with NULL fields, one with broken JSON.
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-02-10 08:00:00",
     "created_timestamp": "2024-02-10 08:00:01", "bgTargets": None, "bgTarget": None},
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-02-15 08:00:00",
     "created_timestamp": "2024-02-15 08:00:01", "bgTargets": "not-json", "bgTarget": None},
    {"_userId": "user_a", "type": "pumpSettings", "time_string": "2024-03-01 08:00:00",
     "created_timestamp": "2024-03-01 08:00:01", "bgTargets": None, "bgTarget": R3_BGTARGET},
    # Non-pumpSettings row — excluded by the type filter even though it carries
    # a parseable bgTarget.
    {"_userId": "user_a", "type": "cbg", "time_string": "2024-01-15 08:00:00",
     "created_timestamp": "2024-01-15 08:00:01", "bgTargets": None, "bgTarget": R3_BGTARGET},
    # user_b: only a schedule-less pumpSettings record -> zero output rows.
    {"_userId": "user_b", "type": "pumpSettings", "time_string": "2024-01-01 08:00:00",
     "created_timestamp": "2024-01-01 08:00:01", "bgTargets": None, "bgTarget": None},
    # user_d R1: activeSchedule = 'Night' must win over the 'Default' the
    # legacy heuristic would pick.
    {"_userId": "user_d", "type": "pumpSettings", "time_string": "2024-01-01 08:00:00",
     "created_timestamp": "2024-01-01 08:00:01", "activeSchedule": "Night",
     "bgTargets": '{"Default": [{"start": 0, "low": 5.55, "high": 6.66}],'
                  ' "Night": [{"start": 0, "low": 6.66, "high": 7.77}]}',
     "bgTarget": None},
    # user_d R2: activeSchedule names a schedule absent from bgTargets ->
    # falls back to 'Default'.
    {"_userId": "user_d", "type": "pumpSettings", "time_string": "2024-02-01 08:00:00",
     "created_timestamp": "2024-02-01 08:00:01", "activeSchedule": "Ghost",
     "bgTargets": '{"Default": [{"start": 0, "low": 5.55, "high": 6.66}]}',
     "bgTarget": None},
]

# --- Run test ---
try:
    setup_test_table(spark, BDDP_TABLE, bddp_rows)

    run(spark, output_table=OUTPUT_TABLE, bddp_table=BDDP_TABLE)

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 6 rows: user_a R1 two slots + R2 one usable slot + R3 one slot, plus
    #    user_d's two single-slot records; user_b and the cbg-type row
    #    contribute nothing.
    assert_row_count(result, 6, "correction-range slot rows")
    assert set(result["_userId"]) == {"user_a", "user_d"}, f"unexpected users: {set(result['_userId'])}"
    print("PASS: type filter, schedule-less records, and empty user excluded")

    is_a = result["_userId"] == "user_a"
    r1 = result[is_a & (result["valid_from"] == pd.Timestamp(2024, 1, 1, 8, 0))].sort_values("slot_start_seconds")
    r2 = result[is_a & (result["valid_from"] == pd.Timestamp(2024, 2, 1, 8, 0))]
    r3 = result[is_a & (result["valid_from"] == pd.Timestamp(2024, 3, 1, 8, 0))]

    # 2. R1: 'Default' chosen over the decoy 'Zzz'; dedup kept the latest
    #    record (low ~100, not the stale ~140 or decoy ~180); ms -> s on the
    #    second slot's start.
    assert len(r1) == 2, f"expected 2 R1 slots, got {len(r1)}"
    lows = [float(v) for v in r1["target_low_mgdl"]]
    assert abs(lows[0] - 5.55 * 18.018) < 0.01, f"R1 slot 0 low: {lows[0]}"
    assert abs(lows[1] - 6.105 * 18.018) < 0.01, f"R1 slot 1 low: {lows[1]}"
    assert list(r1["slot_start_seconds"]) == [0, 28800], (
        f"R1 slot starts: {list(r1['slot_start_seconds'])}"
    )
    print("PASS: 'Default' preferred, dedup keeps latest, ms -> s conversion")

    # 3. R2: first non-empty schedule by sorted name ('Alt' empty -> 'Weekend');
    #    slots missing low or start dropped; high stays NULL on the kept slot.
    assert len(r2) == 1, f"expected 1 R2 slot, got {len(r2)}"
    r2_row = r2.iloc[0]
    assert abs(float(r2_row["target_low_mgdl"]) - 4.995 * 18.018) < 0.01, (
        f"R2 low: {r2_row['target_low_mgdl']}"
    )
    assert pd.isna(r2_row["target_high_mgdl"]), f"R2 high should be NULL: {r2_row['target_high_mgdl']}"
    print("PASS: first-non-empty fallback; unusable slots dropped; nullable high")

    # 4. R3: singular flat bgTarget fallback.
    assert len(r3) == 1, f"expected 1 R3 slot, got {len(r3)}"
    assert abs(float(r3.iloc[0]["target_low_mgdl"]) - 6.66 * 18.018) < 0.01, (
        f"R3 low: {r3.iloc[0]['target_low_mgdl']}"
    )
    print("PASS: singular bgTarget fallback")

    # 5. Validity chain: R1 -> R2 -> R3, with the two schedule-less records
    #    NOT terminating R2 (its valid_to is R3's time, not Feb 10/15); the
    #    last record is open-ended.
    assert all(r1["valid_to"] == pd.Timestamp(2024, 2, 1, 8, 0)), f"R1 valid_to: {list(r1['valid_to'])}"
    assert r2_row["valid_to"] == pd.Timestamp(2024, 3, 1, 8, 0), (
        f"R2 valid_to should skip schedule-less records: {r2_row['valid_to']}"
    )
    assert pd.isna(r3.iloc[0]["valid_to"]), f"R3 valid_to should be NULL: {r3.iloc[0]['valid_to']}"
    print("PASS: validity intervals chain over schedule-bearing records only")

    # 6. activeSchedule selection: user_d R1 emits the 'Night' schedule (not
    #    'Default'); R2's active name is absent from bgTargets -> 'Default'
    #    fallback.
    is_d = result["_userId"] == "user_d"
    d1 = result[is_d & (result["valid_from"] == pd.Timestamp(2024, 1, 1, 8, 0))]
    d2 = result[is_d & (result["valid_from"] == pd.Timestamp(2024, 2, 1, 8, 0))]
    assert len(d1) == 1 and abs(float(d1.iloc[0]["target_low_mgdl"]) - 6.66 * 18.018) < 0.01, (
        f"user_d R1 should emit the active 'Night' schedule: {d1.to_dict('records')}"
    )
    assert len(d2) == 1 and abs(float(d2.iloc[0]["target_low_mgdl"]) - 5.55 * 18.018) < 0.01, (
        f"user_d R2 should fall back to 'Default': {d2.to_dict('records')}"
    )
    print("PASS: activeSchedule preferred; absent active name falls back")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
