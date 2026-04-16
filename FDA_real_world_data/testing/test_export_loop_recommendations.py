"""
Unit test for export_loop_recommendations.py.

Tests both detection methods and their per-day count output:
- dosingDecision-based: bolus/basal matched to a prior loop DD within 5s;
  boluses excluded when a normalBolus DD is within +/-15s;
  only subType != 'normal' counts as autobolus.
- HealthKit-based: Loop-sourced records with MetadataKeyAutomaticallyIssued=1.
- Output is one row per (user, day) with four counts — a day with both
  autobolus and temp_basal signals keeps both sets populated.
- loop_version: numeric max (3.10.1 > 3.2.0), not lexicographic.

Run on Databricks.
"""

import json
import math
import sys
from datetime import date

from pyspark.sql import SparkSession


def _isnull(v):
    """True for None or NaN (pandas nulls from integer columns come back as NaN)."""
    return v is None or (isinstance(v, float) and math.isnan(v))

sys.path.insert(0, "../data_staging")

from export_loop_recommendations import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

INPUT_TABLE = f"{TEST_SCHEMA}._test_input_loop_recs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_loop_recs"


def dd_origin(version):
    return json.dumps({"version": version})


def hk_origin(version):
    return json.dumps({
        "version": version,
        "payload": {"sourceRevision": {"source": {"name": "Loop"}}},
    })


AUTO_PAYLOAD = json.dumps({
    "com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 1
})


def row(_userId, time_string, type_, reason=None, subType=None, origin=None, payload=None):
    return {
        "_userId": _userId,
        "time_string": time_string,
        "type": type_,
        "subType": subType,
        "reason": reason,
        "origin": origin,
        "payload": payload,
    }


TEST_ROWS = [
    # --- Day 1 (2025-01-15, user_a): autobolus via dosingDecision ---
    row("user_a", "2025-01-15 12:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-15 12:00:03", "bolus", subType="smb", origin=dd_origin("3.2.0")),

    # --- Day 2 (2025-01-16, user_a): temp_basal via dosingDecision ---
    row("user_a", "2025-01-16 08:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-16 08:00:02", "basal", origin=dd_origin("3.2.0")),

    # --- Day 3 (2025-01-17, user_a): autobolus via HealthKit only (no DD) ---
    row("user_a", "2025-01-17 09:00:00", "bolus", subType="smb",
        origin=hk_origin("3.4.0"), payload=AUTO_PAYLOAD),

    # --- Day 4 (2025-01-18, user_a): both autobolus and temp_basal signals -> autobolus wins ---
    row("user_a", "2025-01-18 10:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-18 10:00:02", "bolus", subType="smb", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-18 10:00:04", "basal", origin=dd_origin("3.2.0")),

    # --- Day 5 (2025-01-19, user_a): normalBolus within +/-15s -> bolus EXCLUDED ---
    # Would otherwise be autobolus via the loop DD, but the nearby normalBolus DD
    # marks it as a user-initiated correction bolus.
    row("user_a", "2025-01-19 11:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-19 11:00:03", "bolus", subType="smb", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-19 11:00:10", "dosingDecision", reason="normalBolus", origin=dd_origin("3.2.0")),

    # --- Day 6 (2025-01-20, user_a): bolus subType='normal' -> NOT autobolus via DD ---
    row("user_a", "2025-01-20 13:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-20 13:00:02", "bolus", subType="normal", origin=dd_origin("3.2.0")),

    # --- Day 7 (2025-01-21, user_a): non-loop DD reason -> no match, bolus dropped ---
    row("user_a", "2025-01-21 14:00:00", "dosingDecision", reason="other", origin=dd_origin("3.2.0")),
    row("user_a", "2025-01-21 14:00:03", "bolus", subType="smb", origin=dd_origin("3.2.0")),

    # --- Day 8 (2025-01-22, user_a): bad bolus timestamp -> excluded ---
    row("user_a", "2025-01-22 15:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_a", "not-a-timestamp", "bolus", subType="smb", origin=dd_origin("3.2.0")),

    # --- Day 9 (2025-01-23, user_b): numeric version selection (3.10.1 > 3.2.0) ---
    row("user_b", "2025-01-23 07:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.2.0")),
    row("user_b", "2025-01-23 07:00:02", "basal", origin=dd_origin("3.2.0")),
    row("user_b", "2025-01-23 20:00:00", "dosingDecision", reason="loop", origin=dd_origin("3.10.1")),
    row("user_b", "2025-01-23 20:00:02", "basal", origin=dd_origin("3.10.1")),
]


try:
    setup_test_table(spark, INPUT_TABLE, TEST_ROWS)
    run(spark, input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. 5 user-days survive: days 1, 2, 3, 4, 9. Days 5, 6, 7, 8 drop out entirely.
    assert_row_count(result, 5, "total user-days")

    # 2. Per-day counts — NaN means NULL for that side
    by_day = {
        (r["_userId"], r["day"]): (
            r["dd_autobolus_count"],
            r["hk_autobolus_count"],
            r["dd_temp_basal_count"],
            r["hk_temp_basal_count"],
        )
        for _, r in result.iterrows()
    }

    # Day 1: DD autobolus only (1 bolus)
    assert by_day[("user_a", date(2025, 1, 15))][0] == 1
    assert _isnull(by_day[("user_a", date(2025, 1, 15))][1])  # no HK autobolus
    assert _isnull(by_day[("user_a", date(2025, 1, 15))][2])  # no temp_basal
    assert _isnull(by_day[("user_a", date(2025, 1, 15))][3])

    # Day 2: DD temp_basal only (1 basal)
    assert _isnull(by_day[("user_a", date(2025, 1, 16))][0])
    assert _isnull(by_day[("user_a", date(2025, 1, 16))][1])
    assert by_day[("user_a", date(2025, 1, 16))][2] == 1
    assert _isnull(by_day[("user_a", date(2025, 1, 16))][3])

    # Day 3: HK autobolus only (1 bolus, no DD match)
    assert _isnull(by_day[("user_a", date(2025, 1, 17))][0])
    assert by_day[("user_a", date(2025, 1, 17))][1] == 1
    assert _isnull(by_day[("user_a", date(2025, 1, 17))][2])
    assert _isnull(by_day[("user_a", date(2025, 1, 17))][3])

    # Day 4: both autobolus (DD) and temp_basal (DD) populated — no more priority rule
    day4 = by_day[("user_a", date(2025, 1, 18))]
    assert day4[0] == 1, f"day 4 dd_autobolus_count: {day4[0]}"
    assert day4[2] == 1, f"day 4 dd_temp_basal_count: {day4[2]}"

    # Day 9: user_b temp_basal only (2 basals across two DD matches)
    assert by_day[("user_b", date(2025, 1, 23))][2] == 2
    print("PASS: per-day counts")

    # 3. Excluded days are absent: 19 (normalBolus), 20 (subType=normal),
    #    21 (non-loop DD), 22 (bad timestamp)
    excluded = {date(2025, 1, d) for d in (19, 20, 21, 22)}
    present = set(result["day"].tolist())
    assert present.isdisjoint(excluded), (
        f"rows for excluded days leaked through: {present & excluded}"
    )
    print("PASS: excluded days absent (normalBolus, subType=normal, non-loop, bad ts)")

    # 4. loop_version uses numeric comparison (3.10.1 > 3.2.0)
    user_b_row = result[result["_userId"] == "user_b"].iloc[0]
    assert user_b_row["loop_version"] == "3.10.1", (
        f"expected loop_version=3.10.1, got {user_b_row['loop_version']}"
    )
    print("PASS: loop_version uses numeric max (3.10.1 > 3.2.0)")

    # 5. All surviving rows have a non-null loop_version
    assert result["loop_version"].notna().all(), "loop_version has unexpected NULLs"
    print("PASS: loop_version populated on every row")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, INPUT_TABLE, OUTPUT_TABLE)
