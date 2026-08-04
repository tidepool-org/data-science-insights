"""
Unit test for export_overrides_all.py.

Tests: dataset-wide override extraction + dedup, numeric parameter casting and
mmol->mg/dL target conversion, has_own_target, stated-vs-effective duration
(gap truncation, NULL-stated fallback, end-of-data clip, after-end clamp to 0),
end_time / end_day boundaries (midnight-crossing span; exact-midnight end stays
on the prior day), the version-first / date-fallback eligibility flag including
the version_int = 0 trap, and the Loop-user restriction (no loop_recommendations
rows -> overrides dropped).

Run on Databricks.
"""

import sys
from datetime import date

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
from export_overrides_all import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
BDDP_TABLE = f"{TEST_SCHEMA}._test_or_all_bddp"
LOOP_RECS_TABLE = f"{TEST_SCHEMA}._test_or_all_loop_recs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_or_all_output"

ALL_TABLES = [BDDP_TABLE, LOOP_RECS_TABLE, OUTPUT_TABLE]

# --- Test data ---


def loop_rec_row(user, day, loop_version="3.2.0", version_int=3_002_000):
    """One loop_recommendations row; counts are irrelevant here (S1 only uses
    the day's version_int and the user's MAX(day))."""
    return {
        "_userId": user,
        "day": day,
        "dd_autobolus_count": 5,
        "hk_autobolus_count": 0,
        "dd_temp_basal_count": 0,
        "hk_temp_basal_count": 0,
        "loop_version": loop_version,
        "version_int": version_int,
    }


# user_a: 10 observed days on 3.2.0 (eligible) -> last_day = Jan 10, so the
# end-of-data boundary is Jan 11 00:00.
# user_b: three observed days exercising the version rule; last_day = Aug 1.
# user_c: NO loop_recommendations rows at all -> overrides dropped.
loop_recs_rows = (
    [loop_rec_row("user_a", date(2024, 1, d)) for d in range(1, 11)]
    + [
        # Stated 3.4.0 -> version_int at the cutoff, NOT eligible even though
        # the day is before the release date (version wins when parseable).
        loop_rec_row("user_b", date(2024, 1, 2), loop_version="3.4.0", version_int=3_004_000),
        # Unparseable version (version_int = 0) -> date fallback; Jan 3 < cutoff.
        loop_rec_row("user_b", date(2024, 1, 3), loop_version=None, version_int=0),
        # Unparseable version on a post-cutoff day -> not eligible.
        loop_rec_row("user_b", date(2024, 8, 1), loop_version=None, version_int=0),
    ]
)

WORKOUT = {
    "overridePreset": "Workout",
    "basalRateScaleFactor": "0.5",
    "bgTarget": '{"low": 6.0, "high": 8.0}',
    "carbRatioScaleFactor": "2.0",
    "insulinSensitivityScaleFactor": "2.0",
    "duration": "3600",
}

bddp_rows = [
    # --- user_a: duration semantics (all days version-eligible) ---
    # 1. Jan 2 10:00, stated 3600; next at 11:30 (gap 5400) -> effective 3600.
    {"_userId": "user_a", "time_string": "2024-01-02 10:00:00", "created_timestamp": "2024-01-02 10:00:01", **WORKOUT},
    # Stale duplicate of the same activation (older created_timestamp, different
    # name) -> dedup must keep the "Workout" row above.
    {"_userId": "user_a", "time_string": "2024-01-02 10:00:00", "created_timestamp": "2024-01-01 09:00:00", **{**WORKOUT, "overridePreset": "Stale"}},
    # 2. Jan 2 11:30, stated 7200; next at 12:30 (gap 3600) -> gap-truncated to 3600.
    {"_userId": "user_a", "time_string": "2024-01-02 11:30:00", "created_timestamp": "2024-01-02 11:30:01", **{**WORKOUT, "duration": "7200"}},
    # 3. Jan 2 12:30, stated NULL (indefinite); next at Jan 3 12:30 (gap 86400)
    #    -> effective 86400; spans midnight, so end_day = Jan 3.
    {"_userId": "user_a", "time_string": "2024-01-02 12:30:00", "created_timestamp": "2024-01-02 12:30:01", **{**WORKOUT, "duration": None}},
    # 4. Jan 3 12:30, stated 1800; next at Jan 4 10:00 (gap 77400) -> 1800.
    {"_userId": "user_a", "time_string": "2024-01-03 12:30:00", "created_timestamp": "2024-01-03 12:30:01", **{**WORKOUT, "duration": "1800"}},
    # 5. Jan 4 10:00, NO bgTarget, stated 50400 (14 h) -> ends exactly at
    #    Jan 5 00:00: end_day must stay Jan 4 (date of the last second).
    {"_userId": "user_a", "time_string": "2024-01-04 10:00:00", "created_timestamp": "2024-01-04 10:00:01", **{**WORKOUT, "bgTarget": None, "duration": "50400"}},
    # 6. Jan 9 20:00, stated NULL, no next override -> end-of-data clip:
    #    Jan 11 00:00 - Jan 9 20:00 = 100800 s; end_day = Jan 10 (last observed day).
    {"_userId": "user_a", "time_string": "2024-01-09 20:00:00", "created_timestamp": "2024-01-09 20:00:01", **{**WORKOUT, "duration": None}},
    # Null preset -> excluded by WHERE overridePreset IS NOT NULL.
    {
        "_userId": "user_a",
        "time_string": "2024-01-05 14:00:00",
        "created_timestamp": "2024-01-05 14:00:01",
        "overridePreset": None,
        "basalRateScaleFactor": None,
        "bgTarget": None,
        "carbRatioScaleFactor": None,
        "insulinSensitivityScaleFactor": None,
        "duration": None,
    },

    # --- user_b: version-first / date-fallback eligibility ---
    # b1. Jan 2: stated version 3.4.0 on that day -> NOT eligible.
    {"_userId": "user_b", "time_string": "2024-01-02 10:00:00", "created_timestamp": "2024-01-02 10:00:01", **WORKOUT},
    # b2. Jan 3: version_int = 0 that day, pre-cutoff -> eligible via date.
    {"_userId": "user_b", "time_string": "2024-01-03 10:00:00", "created_timestamp": "2024-01-03 10:00:01", **WORKOUT},
    # b3. Jan 5: no loop_recommendations row that day, pre-cutoff -> eligible via date.
    {"_userId": "user_b", "time_string": "2024-01-05 10:00:00", "created_timestamp": "2024-01-05 10:00:01", **WORKOUT},
    # b4. Aug 1: version_int = 0, post-cutoff day -> NOT eligible.
    {"_userId": "user_b", "time_string": "2024-08-01 10:00:00", "created_timestamp": "2024-08-01 10:00:01", **WORKOUT},
    # b5. Aug 2 10:00: after the end-of-data boundary (Aug 2 00:00, last_day
    #     Aug 1) -> effective duration clamps to 0, end_day = override_day;
    #     no loop_recommendations row + post-cutoff -> NOT eligible.
    {"_userId": "user_b", "time_string": "2024-08-02 10:00:00", "created_timestamp": "2024-08-02 10:00:01", **WORKOUT},

    # --- user_c: not a Loop user (no loop_recommendations rows) -> dropped ---
    {"_userId": "user_c", "time_string": "2024-01-02 10:00:00", "created_timestamp": "2024-01-02 10:00:01", **WORKOUT},
]

# --- Run test ---
try:
    setup_test_table(spark, BDDP_TABLE, bddp_rows)
    setup_test_table(spark, LOOP_RECS_TABLE, loop_recs_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        bddp_table=BDDP_TABLE,
        loop_recommendations_table=LOOP_RECS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)
    by_key = {(r["_userId"], r["override_time"]): r for _, r in result.iterrows()}

    # 1. 11 rows: user_a 6 + user_b 5. Stale duplicate collapsed, null preset
    #    excluded, user_c (not a Loop user) dropped entirely.
    assert_row_count(result, 11, "dataset-wide overrides")
    assert "user_c" not in set(result["_userId"]), "user_c should be dropped (no loop_recommendations)"
    print("PASS: null-preset excluded, non-Loop user dropped")

    # 2. Dedup keeps the latest created_timestamp row for a (user, time) pair.
    jan2_10 = by_key[("user_a", pd.Timestamp(2024, 1, 2, 10, 0))]
    assert jan2_10["overridePreset"] == "Workout", f"dedup kept: {jan2_10['overridePreset']}"
    print("PASS: dedup keeps latest created_timestamp")

    # 3. Numeric params + mmol->mg/dL conversion: brsf 0.5 (DOUBLE),
    #    bg_target_low 6.0 * 18.018 = 108.108.
    assert abs(float(jan2_10["basalRateScaleFactor"]) - 0.5) < 1e-9, (
        f"basalRateScaleFactor: {jan2_10['basalRateScaleFactor']}"
    )
    expected_btl = 6.0 * 18.018
    assert abs(float(jan2_10["bg_target_low"]) - expected_btl) < 0.001, (
        f"bg_target_low: expected {expected_btl}, got {jan2_10['bg_target_low']}"
    )
    print(f"PASS: numeric params; bg_target_low converted to mg/dL ({float(jan2_10['bg_target_low']):.4f})")

    # 4. has_own_target: False + NULL targets for the Jan 4 no-bgTarget row;
    #    True elsewhere for user_a.
    jan4 = by_key[("user_a", pd.Timestamp(2024, 1, 4, 10, 0))]
    assert not bool(jan4["has_own_target"]), "Jan 4 should have has_own_target = False"
    assert pd.isna(jan4["bg_target_low"]), f"Jan 4 bg_target_low should be NULL, got {jan4['bg_target_low']}"
    assert bool(jan2_10["has_own_target"]), "Jan 2 10:00 should have has_own_target = True"
    print("PASS: has_own_target flag")

    # 5. Gap truncation: Jan 2 11:30 states 7200 but the 12:30 activation cuts
    #    it to 3600; the untruncated Jan 2 10:00 row keeps 3600.
    jan2_1130 = by_key[("user_a", pd.Timestamp(2024, 1, 2, 11, 30))]
    assert float(jan2_1130["stated_duration"]) == 7200.0, f"stated: {jan2_1130['stated_duration']}"
    assert float(jan2_1130["duration"]) == 3600.0, f"effective: {jan2_1130['duration']}"
    assert float(jan2_10["duration"]) == 3600.0, f"Jan 2 10:00 duration: {jan2_10['duration']}"
    print("PASS: stated_duration preserved; effective duration gap-truncated")

    # 6. NULL stated -> gap fallback, midnight-crossing span: Jan 2 12:30
    #    (stated NULL) runs to the Jan 3 12:30 activation = 86400 s; end_day Jan 3.
    jan2_1230 = by_key[("user_a", pd.Timestamp(2024, 1, 2, 12, 30))]
    assert pd.isna(jan2_1230["stated_duration"]), f"stated should be NULL: {jan2_1230['stated_duration']}"
    assert float(jan2_1230["duration"]) == 86400.0, f"duration: {jan2_1230['duration']}"
    assert jan2_1230["end_day"] == date(2024, 1, 3), f"end_day: {jan2_1230['end_day']}"
    print("PASS: NULL stated falls back to gap; midnight-crossing end_day")

    # 7. End-of-data clip: Jan 9 20:00 (stated NULL, no next) is bounded by
    #    Jan 11 00:00 -> 100800 s; end_day = Jan 10, the last observed day.
    jan9 = by_key[("user_a", pd.Timestamp(2024, 1, 9, 20, 0))]
    assert pd.isna(jan9["stated_duration"]), f"stated should be NULL: {jan9['stated_duration']}"
    assert float(jan9["duration"]) == 100800.0, f"duration: {jan9['duration']}"
    assert jan9["end_day"] == date(2024, 1, 10), f"end_day: {jan9['end_day']}"
    print("PASS: end-of-data clip bounds the indefinite final override")

    # 8. Exact-midnight end: Jan 4 10:00 + 50400 s ends at Jan 5 00:00 sharp;
    #    end_time is the midnight instant but end_day stays Jan 4.
    assert float(jan4["duration"]) == 50400.0, f"Jan 4 duration: {jan4['duration']}"
    assert jan4["end_time"] == pd.Timestamp(2024, 1, 5, 0, 0), f"end_time: {jan4['end_time']}"
    assert jan4["end_day"] == date(2024, 1, 4), f"end_day should stay Jan 4: {jan4['end_day']}"
    print("PASS: exact-midnight end does not claim the next day")

    # 9. Version-first / date-fallback eligibility across the five user_b cases.
    expected_eligibility = {
        pd.Timestamp(2024, 1, 2, 10, 0): False,  # stated 3.4.0 wins over pre-cutoff date
        pd.Timestamp(2024, 1, 3, 10, 0): True,   # version_int 0 -> date rule, pre-cutoff
        pd.Timestamp(2024, 1, 5, 10, 0): True,   # no day row -> date rule, pre-cutoff
        pd.Timestamp(2024, 8, 1, 10, 0): False,  # version_int 0, post-cutoff
        pd.Timestamp(2024, 8, 2, 10, 0): False,  # no day row, post-cutoff
    }
    for ts, expected in expected_eligibility.items():
        row = by_key[("user_b", ts)]
        assert bool(row["is_version_eligible"]) is expected, (
            f"user_b {ts}: is_version_eligible expected {expected}, got {row['is_version_eligible']}"
        )
    assert all(bool(r["is_version_eligible"]) for _, r in result[result["_userId"] == "user_a"].iterrows()), (
        "all user_a activations should be version-eligible"
    )
    print("PASS: version-first / date-fallback eligibility incl. version_int = 0 trap")

    # 10. After-end clamp: the Aug 2 10:00 activation starts after the
    #     end-of-data boundary (Aug 2 00:00) -> duration 0, end_day = override_day.
    aug2 = by_key[("user_b", pd.Timestamp(2024, 8, 2, 10, 0))]
    assert float(aug2["duration"]) == 0.0, f"Aug 2 duration: {aug2['duration']}"
    assert aug2["end_day"] == date(2024, 8, 2), f"Aug 2 end_day: {aug2['end_day']}"
    print("PASS: post-data activation clamps to 0 duration")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
