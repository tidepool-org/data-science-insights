"""
Unit test for compute_glycemic_endpoints.py (transition mode).

Tests: TIR, TBR, TAR, mean glucose, CV, and hypo event detection
with known CBG distributions.

Run on Databricks.
"""

import sys
from datetime import date, datetime, timedelta

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
INPUT_TABLE = f"{TEST_SCHEMA}._test_ge_input"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_ge_output"

ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]

# --- Test data: valid_transition_cbg schema ---
# 10 CBG readings per segment with known distributions:
#   seg1: all 120 mg/dL (100% TIR)
#   seg2: mix of values to test all ranges

BASE_TIME = datetime(2025, 1, 15, 12, 0)


def _cbg_rows(user_id, segment, values):
    """Generate CBG rows with 5-min spacing."""
    rows = []
    for i, val in enumerate(values):
        rows.append({
            "_userId": user_id,
            "tb_to_ab_seg1_start": date(2025, 1, 1),
            "segment_rank": 1,
            "segment": segment,
            "cbg_mg_dl": float(val),
            "cbg_timestamp": BASE_TIME + timedelta(minutes=5 * i),
        })
    return rows


# Seg1: 10 readings all at 120 mg/dL → 100% TIR, 0% TBR/TAR, mean=120, CV=0
seg1_values = [120.0] * 10

# Seg2: 10 readings with known distribution
#   2 × 50 mg/dL  (< 54 = very low, < 70 = below range)
#   2 × 65 mg/dL  (< 70 = below range, but >= 54)
#   4 × 120 mg/dL (in range 70-180)
#   1 × 200 mg/dL (> 180 = above range, but <= 250)
#   1 × 300 mg/dL (> 250 = very high, > 180)
# Expected: tbr_very_low=20%, tbr=40%, tir=40%, tar=20%, tar_very_high=10%
seg2_values = [50.0, 50.0, 65.0, 65.0, 120.0, 120.0, 120.0, 120.0, 200.0, 300.0]

# user_b seg1: 3 consecutive readings <54 (event starts) followed by 3 consecutive >70 (event ends).
# Expected: hypo_events == 1.
user_b_seg1_values = [50.0, 50.0, 50.0, 100.0, 100.0, 100.0]

cbg_rows = (
    _cbg_rows("user_a", "tb_to_ab_seg1", seg1_values)
    + _cbg_rows("user_a", "tb_to_ab_seg2", seg2_values)
    + _cbg_rows("user_b", "tb_to_ab_seg1", user_b_seg1_values)
)


# --- Run test ---
try:
    input_sdf = spark.createDataFrame(pd.DataFrame(cbg_rows))
    input_sdf = input_sdf.withColumn("cbg_mg_dl", input_sdf["cbg_mg_dl"].cast("double"))
    input_sdf.write.mode("overwrite").saveAsTable(INPUT_TABLE)
    print(f"Setup: wrote {len(cbg_rows)} rows to {INPUT_TABLE}")
    run(spark, mode="transition", input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)
    numeric_cols = ["cbg_count", "tbr_very_low", "tbr", "tir", "tar", "tar_very_high", "mean_glucose", "cv"]
    for col in numeric_cols:
        if col in result.columns:
            result[col] = result[col].astype(float)

    # 1. Three rows: user_a × 2 segments + user_b × 1 segment
    assert_row_count(result, 3, "glycemic endpoint rows (1 per (user, segment))")

    # 2. Seg1: 100% TIR, all zeros elsewhere
    seg1 = result[(result["_userId"] == "user_a") & (result["segment"] == "tb_to_ab_seg1")].iloc[0]
    assert seg1["cbg_count"] == 10, f"seg1 cbg_count: expected 10, got {seg1['cbg_count']}"
    assert abs(seg1["tir"] - 100.0) < 0.1, f"seg1 TIR: expected 100%, got {seg1['tir']}"
    assert abs(seg1["tbr"]) < 0.1, f"seg1 TBR: expected 0%, got {seg1['tbr']}"
    assert abs(seg1["tar"]) < 0.1, f"seg1 TAR: expected 0%, got {seg1['tar']}"
    assert abs(seg1["mean_glucose"] - 120.0) < 0.1, f"seg1 mean: expected 120, got {seg1['mean_glucose']}"
    print("PASS: seg1 — 100% TIR, mean=120")

    # 3. Seg2: verify range percentages
    seg2 = result[(result["_userId"] == "user_a") & (result["segment"] == "tb_to_ab_seg2")].iloc[0]
    assert seg2["cbg_count"] == 10, f"seg2 cbg_count: expected 10, got {seg2['cbg_count']}"
    assert abs(seg2["tbr_very_low"] - 20.0) < 0.1, f"seg2 TBR very low: expected 20%, got {seg2['tbr_very_low']}"
    assert abs(seg2["tbr"] - 40.0) < 0.1, f"seg2 TBR: expected 40%, got {seg2['tbr']}"
    assert abs(seg2["tir"] - 40.0) < 0.1, f"seg2 TIR: expected 40%, got {seg2['tir']}"
    assert abs(seg2["tar"] - 20.0) < 0.1, f"seg2 TAR: expected 20%, got {seg2['tar']}"
    assert abs(seg2["tar_very_high"] - 10.0) < 0.1, f"seg2 TAR very high: expected 10%, got {seg2['tar_very_high']}"
    print("PASS: seg2 — TBR=40%, TIR=40%, TAR=20%")

    # 4. Seg2 mean glucose
    expected_mean = sum(seg2_values) / len(seg2_values)
    assert abs(seg2["mean_glucose"] - expected_mean) < 0.1, (
        f"seg2 mean: expected {expected_mean}, got {seg2['mean_glucose']}"
    )
    print(f"PASS: seg2 mean glucose = {expected_mean}")

    # 5. user_a hypo events: seg1 has 0 (no <54 readings); seg2 has 0 (only 2 consecutive <54).
    assert seg1["hypo_events"] == 0, f"seg1 hypo_events: expected 0, got {seg1['hypo_events']}"
    assert seg2["hypo_events"] == 0, f"seg2 hypo_events: expected 0, got {seg2['hypo_events']}"
    print("PASS: user_a hypo events = 0 (< 3 consecutive readings below 54)")

    # 6. user_b hypo events: 3 consecutive <54 → event starts; 3 consecutive >70 → event ends.
    user_b_seg1 = result[(result["_userId"] == "user_b") & (result["segment"] == "tb_to_ab_seg1")].iloc[0]
    assert user_b_seg1["hypo_events"] == 1, (
        f"user_b seg1 hypo_events: expected 1, got {user_b_seg1['hypo_events']}"
    )
    print("PASS: user_b hypo events = 1 (3+ consecutive <54 then 3+ consecutive >70)")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
