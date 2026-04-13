"""
Unit test for export_autobolus_durability.py.

Tests: All users appear with auditable flags — adoption detection via 3-day
rolling window, 56-day min followup, final 28-day period classification,
age eligibility, and demographics join.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession

sys.path.insert(0, "../data_staging")

from export_autobolus_durability import run  # noqa: E402
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
# user_continues (age 30): 60 days all-AB, 288/day.
#   Adoption triggers on day 2 (3 consecutive days >=80% AB), adoption_date = day 0.
#   last_day = day 59, days_post_adoption = 59 >= 56. Final 28 days all AB -> not discontinued.
# user_discontinues (age 30): 3 days AB + 57 days TB, 288/day.
#   Adoption triggers, then all temp basal. Final 28-day period AB% ~ 0 -> discontinued.
# user_short_followup (age 30): 30 days all-AB. days_post_adoption < 56.
# user_child (age 5): same data as user_continues, but dob makes age <= 6.
recs_rows = (
    make_loop_recs("user_continues", date(2025, 1, 1), 60, 288, 1)
    + make_loop_recs("user_discontinues", date(2025, 1, 1), 3, 288, 1)
    + make_loop_recs("user_discontinues", date(2025, 1, 4), 57, 288, 0)
    + make_loop_recs("user_short_followup", date(2025, 1, 1), 30, 288, 1)
    + make_loop_recs("user_child", date(2025, 1, 1), 60, 288, 1)
)

dates_rows = [
    {"userid": "user_continues", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_discontinues", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_short_followup", "dob": date(1995, 1, 1), "diagnosis_date": date(2010, 1, 1)},
    {"userid": "user_child", "dob": date(2020, 1, 1), "diagnosis_date": date(2023, 1, 1)},
]

gender_rows = [
    {"userid": "user_continues", "gender": "F"},
    {"userid": "user_discontinues", "gender": "M"},
    {"userid": "user_short_followup", "gender": "F"},
    {"userid": "user_child", "gender": "M"},
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

    # 1. All 4 users appear
    assert_row_count(result, 4, "all users in output")
    user_ids = set(result["_userId"].tolist())
    assert user_ids == {"user_continues", "user_discontinues", "user_short_followup", "user_child"}, (
        f"expected all 4 users, got {user_ids}"
    )
    print("PASS: all 4 users in output")

    # 2. user_continues: adopted, sufficient followup, not discontinued
    cont = result[result["_userId"] == "user_continues"].iloc[0]
    assert cont["is_adopted"] == True, f"user_continues should be adopted"  # noqa: E712
    assert cont["has_min_followup"] == True, f"user_continues should have min followup"  # noqa: E712
    assert cont["has_final_coverage"] == True, f"user_continues should have final coverage"  # noqa: E712
    assert cont["is_age_eligible"] == True, f"user_continues should be age eligible"  # noqa: E712
    assert int(cont["is_discontinued"]) == 0, f"user_continues should not be discontinued"
    assert float(cont["final_autobolus_pct"]) > 0.9, f"final_autobolus_pct should be ~1.0"
    print("PASS: user_continues — all flags TRUE, not discontinued")

    # 3. user_discontinues: adopted, sufficient followup, discontinued
    disc = result[result["_userId"] == "user_discontinues"].iloc[0]
    assert disc["is_adopted"] == True  # noqa: E712
    assert disc["has_min_followup"] == True  # noqa: E712
    assert int(disc["is_discontinued"]) == 1, f"user_discontinues should be discontinued"
    assert float(disc["final_autobolus_pct"]) < 0.1, f"final_autobolus_pct should be ~0.0"
    print("PASS: user_discontinues — adopted, discontinued, low final AB%")

    # 4. user_short_followup: adopted but insufficient followup
    short = result[result["_userId"] == "user_short_followup"].iloc[0]
    assert short["is_adopted"] == True  # noqa: E712
    assert short["has_min_followup"] == False, f"user_short_followup should not have min followup"  # noqa: E712
    print(f"PASS: user_short_followup — adopted, has_min_followup=False (days={short['days_post_adoption']})")

    # 5. user_child: adopted but not age eligible
    child = result[result["_userId"] == "user_child"].iloc[0]
    assert child["is_adopted"] == True  # noqa: E712
    assert child["is_age_eligible"] == False, f"user_child should not be age eligible"  # noqa: E712
    assert float(child["age_at_adoption"]) <= 6, f"user_child age should be <= 6, got {child['age_at_adoption']}"
    print("PASS: user_child — adopted, is_age_eligible=False")

    # 6. Demographics joined for all
    assert cont["gender"] == "F", f"user_continues gender should be F"
    assert disc["gender"] == "M", f"user_discontinues gender should be M"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
