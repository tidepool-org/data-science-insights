"""
Unit test for export_ab_day_cohort.py.

Tests: the type-1 restriction, the >= 3 AB-day threshold via GREATEST across
methods (not the sum; NULL counts coalesce to 0), the version-first /
date-fallback rule including the version_int = 0 trap, the age gate (>= 6 on
the day, DOB-unknown passes, young child fails), the daily coverage gate at the
200/201 boundary with the plausibility filter, the eligible-vs-outcome day
split, and per-user first_eligible_ab_day (NULL when the user has none).

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
from export_ab_day_cohort import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
LOOP_RECS_TABLE = f"{TEST_SCHEMA}._test_abdc_loop_recs"
LOOP_CBG_TABLE = f"{TEST_SCHEMA}._test_abdc_cbg"
USER_DATES_TABLE = f"{TEST_SCHEMA}._test_abdc_user_dates"
DIAGNOSIS_TABLE = f"{TEST_SCHEMA}._test_abdc_diagnosis"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_abdc_output"

ALL_TABLES = [LOOP_RECS_TABLE, LOOP_CBG_TABLE, USER_DATES_TABLE, DIAGNOSIS_TABLE, OUTPUT_TABLE]

# --- Test data ---


def rec(user, day, dd_ab=0, hk_ab=0, loop_version="3.2.0", version_int=3_002_000):
    return {
        "_userId": user,
        "day": day,
        "dd_autobolus_count": dd_ab,
        "hk_autobolus_count": hk_ab,
        "dd_temp_basal_count": 0,
        "hk_temp_basal_count": 0,
        "loop_version": loop_version,
        "version_int": version_int,
    }


def cbg_day(user, day, n_plausible, n_implausible=0):
    """n readings at 5-min cadence from local midnight; implausible ones after."""
    base = datetime(day.year, day.month, day.day)
    rows = [
        {"_userId": user, "cbg_timestamp": base + timedelta(minutes=5 * i),
         "cbg_mg_dl": 100.0, "is_plausible": True}
        for i in range(n_plausible)
    ]
    rows += [
        {"_userId": user, "cbg_timestamp": base + timedelta(minutes=5 * (n_plausible + i)),
         "cbg_mg_dl": 600.0, "is_plausible": False}
        for i in range(n_implausible)
    ]
    return rows


diagnosis_rows = [
    {"_userId": "user_a", "diagnosis_type": "type1"},
    {"_userId": "user_b", "diagnosis_type": "type1"},
    {"_userId": "user_kid", "diagnosis_type": "type1"},
    {"_userId": "user_t2", "diagnosis_type": "type2"},
]

user_dates_rows = [
    {"userid": "user_a", "dob": date(2010, 1, 1)},     # ~14 y in 2024 -> eligible
    {"userid": "user_kid", "dob": date(2020, 6, 1)},   # ~3.6 y in 2024 -> not eligible
    # user_b intentionally absent -> DOB unknown -> age-eligible
]

loop_recs_rows = [
    # user_a: the AB-threshold and coverage matrix (all days version-eligible).
    rec("user_a", date(2024, 1, 1), dd_ab=5),               # AB day; 250 CBG -> outcome day
    rec("user_a", date(2024, 1, 2), dd_ab=2, hk_ab=2),      # GREATEST = 2 < 3 -> NOT an AB day
    rec("user_a", date(2024, 1, 3), hk_ab=4),               # AB via the HK method; no CBG -> not outcome
    rec("user_a", date(2024, 1, 4), dd_ab=5),               # AB day; 200 CBG -> coverage fails (boundary)
    rec("user_a", date(2024, 1, 5), dd_ab=5),               # AB day; 201 plausible -> coverage passes (boundary)
    {**rec("user_a", date(2024, 1, 6)), "dd_autobolus_count": None, "hk_autobolus_count": None},  # NULL counts -> 0
    # user_b: version scenarios (all AB days; DOB unknown).
    rec("user_b", date(2024, 1, 2), dd_ab=5, loop_version="3.4.0", version_int=3_004_000),  # version blocks
    rec("user_b", date(2024, 1, 3), dd_ab=5, loop_version=None, version_int=0),             # 0 -> date rule, pre-cutoff
    rec("user_b", date(2024, 8, 1), dd_ab=5, loop_version=None, version_int=0),             # 0 -> date rule, post-cutoff
    # user_kid: AB day but age ~3.6 -> never eligible.
    rec("user_kid", date(2024, 1, 1), dd_ab=5),
    # user_t2: excluded entirely by the diagnosis gate.
    rec("user_t2", date(2024, 1, 1), dd_ab=5),
]

loop_cbg_rows = (
    cbg_day("user_a", date(2024, 1, 1), 250)
    + cbg_day("user_a", date(2024, 1, 4), 200)
    + cbg_day("user_a", date(2024, 1, 5), 201, n_implausible=10)  # implausible must not count
)

# --- Run test ---
try:
    setup_test_table(spark, LOOP_RECS_TABLE, loop_recs_rows)
    setup_test_table(spark, LOOP_CBG_TABLE, loop_cbg_rows)
    setup_test_table(spark, USER_DATES_TABLE, user_dates_rows)
    setup_test_table(spark, DIAGNOSIS_TABLE, diagnosis_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=LOOP_RECS_TABLE,
        loop_cbg_table=LOOP_CBG_TABLE,
        user_dates_table=USER_DATES_TABLE,
        diagnosis_table=DIAGNOSIS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)
    by_key = {(r["_userId"], r["day"]): r for _, r in result.iterrows()}

    # 1. 10 rows: user_a 6 + user_b 3 + user_kid 1; user_t2 excluded by the
    #    type-1 gate.
    assert_row_count(result, 10, "ab_day_cohort rows")
    assert "user_t2" not in set(result["_userId"]), "type-2 user should be excluded"
    print("PASS: type-1 restriction")

    # 2. AB-day threshold: >= 3 via GREATEST per method — 2+2 across methods is
    #    NOT an AB day; 4 via HK alone is; NULL counts coalesce to 0.
    assert bool(by_key[("user_a", date(2024, 1, 1))]["is_ab_day"])
    assert not bool(by_key[("user_a", date(2024, 1, 2))]["is_ab_day"]), "GREATEST, not sum"
    assert bool(by_key[("user_a", date(2024, 1, 3))]["is_ab_day"])
    assert not bool(by_key[("user_a", date(2024, 1, 6))]["is_ab_day"]), "NULL counts -> 0"
    print("PASS: >= 3 AB-day threshold via GREATEST")

    # 3. Version-first / date-fallback incl. the version_int = 0 trap.
    assert not bool(by_key[("user_b", date(2024, 1, 2))]["is_version_eligible"])
    assert bool(by_key[("user_b", date(2024, 1, 3))]["is_version_eligible"])
    assert not bool(by_key[("user_b", date(2024, 8, 1))]["is_version_eligible"])
    print("PASS: version-first / date-fallback eligibility")

    # 4. Age gate: user_a (~14) eligible, user_b (DOB unknown) eligible,
    #    user_kid (~3.6) not.
    assert bool(by_key[("user_a", date(2024, 1, 1))]["is_age_eligible"])
    assert bool(by_key[("user_b", date(2024, 1, 3))]["is_age_eligible"]), "unknown DOB passes"
    assert not bool(by_key[("user_kid", date(2024, 1, 1))]["is_age_eligible"])
    assert not bool(by_key[("user_kid", date(2024, 1, 1))]["is_eligible_ab_day"])
    print("PASS: age gate (>= 6 on day, DOB-unknown passes)")

    # 5. Coverage boundary and plausibility: 250 passes, 200 fails, 201
    #    plausible passes even with 10 implausible extras; eligible-vs-outcome
    #    split (Jan 3 is an eligible AB day but not an outcome day).
    jan1 = by_key[("user_a", date(2024, 1, 1))]
    jan3 = by_key[("user_a", date(2024, 1, 3))]
    jan4 = by_key[("user_a", date(2024, 1, 4))]
    jan5 = by_key[("user_a", date(2024, 1, 5))]
    assert int(jan5["cbg_day_count"]) == 201, f"implausible readings counted: {jan5['cbg_day_count']}"
    assert bool(jan1["is_outcome_day"])
    assert bool(jan3["is_eligible_ab_day"]) and not bool(jan3["is_outcome_day"])
    assert bool(jan4["is_eligible_ab_day"]) and not bool(jan4["is_coverage_ok"])
    assert bool(jan5["is_outcome_day"])
    print("PASS: 201-reading coverage boundary, plausibility filter, outcome split")

    # 6. first_eligible_ab_day: user_a Jan 1 on every row; user_b Jan 3 (its
    #    first version-eligible AB day); user_kid NULL (never eligible).
    assert all(
        r["first_eligible_ab_day"] == date(2024, 1, 1)
        for _, r in result[result["_userId"] == "user_a"].iterrows()
    ), "user_a first_eligible_ab_day"
    assert by_key[("user_b", date(2024, 8, 1))]["first_eligible_ab_day"] == date(2024, 1, 3)
    assert pd.isna(by_key[("user_kid", date(2024, 1, 1))]["first_eligible_ab_day"])
    print("PASS: per-user first_eligible_ab_day")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
