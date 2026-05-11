"""
Unit test for export_autobolus_durability.py.

Tests: All users appear with auditable flags — adoption detection via 3-day
rolling window, 56-day min followup, final 28-day period classification,
age eligibility, and demographics join.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_autobolus_durability import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
RECS_TABLE = f"{TEST_SCHEMA}._test_dur_recs"
DATES_TABLE = f"{TEST_SCHEMA}._test_dur_dates"
GENDER_TABLE = f"{TEST_SCHEMA}._test_dur_gender"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_dur_output"

ALL_TABLES = [RECS_TABLE, DATES_TABLE, GENDER_TABLE, OUTPUT_TABLE]

# --- Test data ---
# user_continues (age 30): 60 days all-AB.
#   Adoption triggers on day 2 (3 consecutive days >=80% AB), adoption_date = day 0.
#   last_day = day 59, days_post_adoption = 59 >= 56. Final 28 days all AB -> not discontinued.
# user_discontinues (age 30): 3 days AB + 57 days TB.
#   Adoption triggers, then all temp basal. Final 28-day period AB% ~ 0 -> discontinued.
# user_short_followup (age 30): 30 days all-AB. days_post_adoption < 56.
# user_child (age 5): same data as user_continues, but dob makes age < 6.
# user_age_six (age exactly 6.0): boundary check — should be age-eligible (>= 6).
# user_dropoff_sustained (age 30): 60 dense AB days, then one sparse data
#   point ~40 days later. Pre-dropoff window = all AB → user vanished while
#   AB-heavy; censored as sustained (had_terminal_dropoff=True but
#   is_discontinued=0).
# user_dropoff_disc (age 30): 3 AB days then 57 TB days (still adopted via the
#   3-day rolling window), then one sparse data point ~40 days later. Pre-
#   dropoff window = all TB → had already deactivated AB before going dark.
#   had_terminal_dropoff=True AND is_discontinued=1.
recs_rows = (
    make_loop_recs("user_continues", date(2025, 1, 1), n_days=60, dosing_mode="autobolus")
    + make_loop_recs("user_discontinues", date(2025, 1, 1), n_days=3, dosing_mode="autobolus")
    + make_loop_recs("user_discontinues", date(2025, 1, 4), n_days=57, dosing_mode="temp_basal")
    + make_loop_recs("user_short_followup", date(2025, 1, 1), n_days=30, dosing_mode="autobolus")
    + make_loop_recs("user_child", date(2025, 1, 1), n_days=60, dosing_mode="autobolus")
    + make_loop_recs("user_age_six", date(2025, 1, 1), n_days=60, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_sustained", date(2025, 1, 1), n_days=60, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_sustained", date(2025, 4, 10), n_days=1, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_disc", date(2025, 1, 1), n_days=3, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_disc", date(2025, 1, 4), n_days=57, dosing_mode="temp_basal")
    + make_loop_recs("user_dropoff_disc", date(2025, 4, 10), n_days=1, dosing_mode="temp_basal")
)

dates_rows = [
    {"userid": "user_continues", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_discontinues", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_short_followup", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_child", "dob": date(2020, 1, 1), "diagnosis_date": date(2023, 1, 1)},
    {"userid": "user_age_six", "dob": date(2019, 1, 1), "diagnosis_date": date(2023, 1, 1)},
    {"userid": "user_dropoff_sustained", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_dropoff_disc", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
]

gender_rows = [
    {"userid": "user_continues", "gender": "F"},
    {"userid": "user_discontinues", "gender": "M"},
    {"userid": "user_short_followup", "gender": "F"},
    {"userid": "user_child", "gender": "M"},
    {"userid": "user_age_six", "gender": "F"},
    {"userid": "user_dropoff_sustained", "gender": "M"},
    {"userid": "user_dropoff_disc", "gender": "F"},
]

# --- Run test ---
try:
    setup_test_table(spark, RECS_TABLE, recs_rows)
    setup_test_table(spark, DATES_TABLE, dates_rows)
    setup_test_table(spark, GENDER_TABLE, gender_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=RECS_TABLE,
        user_dates_table=DATES_TABLE,
        user_gender_table=GENDER_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. All 7 users appear
    assert_row_count(result, 7, "all users in output")
    user_ids = set(result["_userId"].tolist())
    assert user_ids == {
        "user_continues",
        "user_discontinues",
        "user_short_followup",
        "user_child",
        "user_age_six",
        "user_dropoff_sustained",
        "user_dropoff_disc",
    }, f"expected all 7 users, got {user_ids}"
    print("PASS: all 7 users in output")

    # 2. user_continues: adopted, sufficient followup, not discontinued
    cont = result[result["_userId"] == "user_continues"].iloc[0]
    assert cont["is_adopted"], "user_continues should be adopted"
    assert cont["has_min_followup"], "user_continues should have min followup"
    assert cont["has_final_coverage"], "user_continues should have final coverage"
    assert cont["is_age_eligible"], "user_continues should be age eligible"
    assert int(cont["is_discontinued"]) == 0, "user_continues should not be discontinued"
    assert float(cont["final_autobolus_pct"]) > 0.9, "final_autobolus_pct should be ~1.0"
    print("PASS: user_continues — all flags TRUE, not discontinued")

    # 3. user_discontinues: adopted, sufficient followup, discontinued
    disc = result[result["_userId"] == "user_discontinues"].iloc[0]
    assert disc["is_adopted"]
    assert disc["has_min_followup"]
    assert int(disc["is_discontinued"]) == 1, "user_discontinues should be discontinued"
    assert float(disc["final_autobolus_pct"]) < 0.1, "final_autobolus_pct should be ~0.0"
    print("PASS: user_discontinues — adopted, discontinued, low final AB%")

    # 4. user_short_followup: adopted but insufficient followup
    short = result[result["_userId"] == "user_short_followup"].iloc[0]
    assert short["is_adopted"]
    assert not short["has_min_followup"], "user_short_followup should not have min followup"
    print(f"PASS: user_short_followup — adopted, has_min_followup=False (days={short['days_post_adoption']})")

    # 5. user_child: adopted but not age eligible (age 5, < 6)
    child = result[result["_userId"] == "user_child"].iloc[0]
    assert child["is_adopted"]
    assert not child["is_age_eligible"], "user_child should not be age eligible"
    assert float(child["age_at_adoption"]) < 6, f"user_child age should be < 6, got {child['age_at_adoption']}"
    print("PASS: user_child — adopted, is_age_eligible=False")

    # 6. user_age_six: age exactly 6.0 — boundary, should be eligible (>= 6)
    six = result[result["_userId"] == "user_age_six"].iloc[0]
    assert six["is_adopted"]
    assert six["is_age_eligible"], (
        f"user_age_six should be age eligible at age={six['age_at_adoption']}"
    )
    assert float(six["age_at_adoption"]) == 6.0, (
        f"user_age_six age should be 6.0, got {six['age_at_adoption']}"
    )
    print("PASS: user_age_six — boundary age 6.0, is_age_eligible=True")

    # 7. user_dropoff_sustained: terminal dropoff but pre-dropoff window
    #    was all AB → censored as sustained, NOT discontinued.
    drop_s = result[result["_userId"] == "user_dropoff_sustained"].iloc[0]
    assert drop_s["is_adopted"]
    assert drop_s["has_min_followup"]
    assert drop_s["had_terminal_dropoff"], "should have terminal dropoff"
    assert drop_s["has_final_coverage"], "should pass has_final_coverage via dropoff"
    assert int(drop_s["is_discontinued"]) == 0, (
        "user_dropoff_sustained should NOT be discontinued (pre-dropoff was all AB)"
    )
    assert float(drop_s["pre_dropoff_ab_pct"]) > 0.99, (
        f"pre_dropoff_ab_pct should be ~1.0, got {drop_s['pre_dropoff_ab_pct']}"
    )
    print("PASS: user_dropoff_sustained — dropoff with high pre-dropoff AB% censored as sustained")

    # 8. user_dropoff_disc: terminal dropoff with low pre-dropoff AB% →
    #    discontinued (had deactivated AB before vanishing).
    drop_d = result[result["_userId"] == "user_dropoff_disc"].iloc[0]
    assert drop_d["is_adopted"]
    assert drop_d["has_min_followup"]
    assert drop_d["had_terminal_dropoff"], "should have terminal dropoff"
    assert drop_d["has_final_coverage"], "should pass has_final_coverage via dropoff"
    assert int(drop_d["is_discontinued"]) == 1, (
        "user_dropoff_disc should be discontinued (pre-dropoff was all TB)"
    )
    assert float(drop_d["pre_dropoff_ab_pct"]) < 0.01, (
        f"pre_dropoff_ab_pct should be ~0.0, got {drop_d['pre_dropoff_ab_pct']}"
    )
    print("PASS: user_dropoff_disc — dropoff with low pre-dropoff AB% classified as discontinued")

    # 9. Demographics joined for all
    assert cont["gender"] == "F", f"user_continues gender should be F"
    assert disc["gender"] == "M", f"user_discontinues gender should be M"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
