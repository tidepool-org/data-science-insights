"""
Unit test for compute_glycemic_endpoints.py mode=ab_days (the hypo_group_cols
mechanism added for PLN IR-1002 §7.4).

Tests: one pooled endpoint row per user; range metrics computed over ALL pooled
readings; and — the load-bearing pin — hypo events detected WITHIN days: a
below-54 run split across two non-adjacent days (2 readings ending day one +
1 reading starting day three) must count ZERO events, where detection at the
pooled user grain would have chained them into one. A genuine within-day event
still counts. (The three pre-existing modes keep detection at the output grain;
their behavior is pinned by the existing paired test.)

Run on Databricks.
"""

import sys
from datetime import datetime, timedelta, date

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from compute_glycemic_endpoints import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
INPUT_TABLE = f"{TEST_SCHEMA}._test_ge_abd_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_ge_abd_output"

ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]

# --- Test data ---


def readings(user, day, values, start_hour=10, start_minute=0):
    base = datetime(day.year, day.month, day.day, start_hour, start_minute)
    return [
        {"_userId": user, "day": day, "cbg_timestamp": base + timedelta(minutes=5 * i),
         "cbg_mg_dl": float(v)}
        for i, v in enumerate(values)
    ]


input_rows = (
    # user_x: a below-54 run SPLIT across two non-adjacent days — day 1 ends
    # with two 50s (23:50, 23:55), day 3 begins with one 50 (00:00). Pooled
    # detection would chain them into a 3-run event; within-day detection
    # must count 0. 20 readings, 3 below 54 -> tbr(_very_low) 15%, tir 85%.
    readings("user_x", date(2024, 1, 1), [100] * 8 + [50, 50], start_hour=23, start_minute=10)
    + readings("user_x", date(2024, 1, 3), [50] + [100] * 9, start_hour=0, start_minute=0)
    # user_y: one clean within-day event (3 x 50 bracketed by 3 x 100 each
    # side). 9 readings: tir 66.67, tbr 33.33, mean 83.33, cv 30.0.
    + readings("user_y", date(2024, 1, 1), [100, 100, 100, 50, 50, 50, 100, 100, 100])
)

# --- Run test ---
try:
    setup_test_table(spark, INPUT_TABLE, input_rows)

    run(spark, mode="ab_days", input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)

    result = read_test_output(spark, OUTPUT_TABLE)
    by_user = {r["_userId"]: r for _, r in result.iterrows()}

    # 1. One pooled row per user; no day column at the output grain.
    assert_row_count(result, 2, "pooled per-user endpoint rows")
    assert "day" not in result.columns, f"day should not survive pooling: {list(result.columns)}"
    print("PASS: one pooled row per user")

    # 2. THE PIN: user_x's cross-day below-54 run counts zero events under
    #    within-day detection (pooled detection would count 1).
    assert int(by_user["user_x"]["hypo_events"]) == 0, (
        f"cross-day run must not form an event: {by_user['user_x']['hypo_events']}"
    )
    print("PASS: within-day hypo detection (cross-day run -> 0 events)")

    # 3. A genuine within-day event still counts.
    assert int(by_user["user_y"]["hypo_events"]) == 1, (
        f"user_y should have exactly 1 event: {by_user['user_y']['hypo_events']}"
    )
    print("PASS: within-day event counted")

    # 4. Range metrics pool across ALL of a user's readings: user_x 20
    #    readings with 3 below 54; user_y 9 readings with 3 below 54,
    #    mean 83.33, cv 30.0.
    ux, uy = by_user["user_x"], by_user["user_y"]
    assert int(ux["cbg_count"]) == 20 and int(uy["cbg_count"]) == 9
    assert abs(float(ux["tbr_very_low"]) - 15.0) < 0.01, f"user_x tbr_very_low: {ux['tbr_very_low']}"
    assert abs(float(ux["tir"]) - 85.0) < 0.01, f"user_x tir: {ux['tir']}"
    assert abs(float(uy["tir"]) - 200.0 / 3.0) < 0.01, f"user_y tir: {uy['tir']}"
    assert abs(float(uy["mean_glucose"]) - 250.0 / 3.0) < 0.01, f"user_y mean: {uy['mean_glucose']}"
    assert abs(float(uy["cv"]) - 30.0) < 0.01, f"user_y cv: {uy['cv']}"
    print("PASS: pooled range metrics")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
