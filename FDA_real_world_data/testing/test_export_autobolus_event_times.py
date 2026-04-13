"""
Unit test for export_autobolus_event_times.py.

Tests: weekly autobolus %, 4-week trailing average, is_window_complete flag,
permanent discontinuation detection. All weeks output for all users.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession

sys.path.insert(0, "../data_staging")

from export_autobolus_event_times import run  # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
DURABILITY_TABLE = f"{TEST_SCHEMA}._test_evt_durability"
RECS_TABLE = f"{TEST_SCHEMA}._test_evt_recs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_evt_output"

ALL_TABLES = [DURABILITY_TABLE, RECS_TABLE, OUTPUT_TABLE]

# --- Test data ---
# Stub durability table directly (not depending on export_autobolus_durability output).
# Only need _userId and adoption_date for the join.
durability_rows = [
    {"_userId": "user_event", "adoption_date": date(2025, 1, 1)},
    {"_userId": "user_censored", "adoption_date": date(2025, 1, 1)},
]

# user_event: 6 weeks AB (42 days) then 7 weeks TB (49 days) = 91 days total.
#   Weeks 0-5: ~100% AB. Weeks 6-12: 0% AB.
#   4-week trailing avg drops permanently -> event detected
recs_rows = (
    make_loop_recs("user_event", date(2025, 1, 1), 42, 288, 1)
    + make_loop_recs("user_event", date(2025, 2, 12), 49, 288, 0)
    # user_censored: 91 days all-AB. Trailing avg stays ~100% -> no event
    + make_loop_recs("user_censored", date(2025, 1, 1), 91, 288, 1)
)

# --- Run test ---
try:
    setup_test_table(spark, DURABILITY_TABLE, durability_rows)
    setup_test_table(spark, RECS_TABLE, recs_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        durability_table=DURABILITY_TABLE,
        loop_recommendations_table=RECS_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. Multiple rows per user (one per week)
    assert len(result) > 2, f"expected multiple weekly rows, got {len(result)}"
    users = set(result["_userId"].tolist())
    assert users == {"user_event", "user_censored"}, f"expected both users, got {users}"
    print(f"PASS: {len(result)} weekly rows for {len(users)} users")

    # 2. Early weeks have is_window_complete=False (< 4 weeks of data)
    event_weeks = result[result["_userId"] == "user_event"].sort_values("week_post_adoption")
    first_week = event_weeks.iloc[0]
    assert first_week["is_window_complete"] == False, (  # noqa: E712
        f"first week should have is_window_complete=False"
    )
    print("PASS: early weeks have is_window_complete=False")

    # 3. Later weeks have is_window_complete=True
    week_3 = event_weeks[event_weeks["week_post_adoption"] == 3].iloc[0]
    assert week_3["is_window_complete"] == True, (  # noqa: E712
        f"week 3 should have is_window_complete=True"
    )
    print("PASS: week 3+ has is_window_complete=True")

    # 4. user_event: has_discontinuation=True, is_event_week marks the event
    assert all(event_weeks["has_discontinuation"] == True), (  # noqa: E712
        "all user_event rows should have has_discontinuation=True"
    )
    event_week_rows = event_weeks[event_weeks["is_event_week"] == True]  # noqa: E712
    assert len(event_week_rows) == 1, f"expected 1 event week, got {len(event_week_rows)}"
    print(f"PASS: user_event — has_discontinuation=True, event at week {event_week_rows.iloc[0]['week_post_adoption']}")

    # 5. user_censored: has_discontinuation=False, no event weeks
    censored_weeks = result[result["_userId"] == "user_censored"]
    assert all(censored_weeks["has_discontinuation"] == False), (  # noqa: E712
        "all user_censored rows should have has_discontinuation=False"
    )
    assert all(censored_weeks["is_event_week"] == False), (  # noqa: E712
        "user_censored should have no event weeks"
    )
    print(f"PASS: user_censored — has_discontinuation=False, {len(censored_weeks)} weeks")

    # 6. autobolus_pct and trailing_ab_pct columns present
    assert "autobolus_pct" in result.columns, "missing autobolus_pct"
    assert "trailing_ab_pct" in result.columns, "missing trailing_ab_pct"
    print("PASS: autobolus_pct and trailing_ab_pct columns present")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
