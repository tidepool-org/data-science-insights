"""
Unit test for export_autobolus_event_times.py.

Tests: weekly autobolus %, 4-week trailing average, is_window_complete flag,
permanent discontinuation detection. All weeks output for all users.

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
from export_autobolus_event_times import run  # type: ignore # noqa: E402
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
# Includes the dropoff-related columns the event-times SQL now reads.
# `is_discontinued` is INT in production (1/0/NULL from a CASE expression),
# not BOOLEAN — keep the same type here so the event-times SQL's `= 1`
# comparison resolves.
durability_rows = [
    {
        "_userId": "user_event",
        "adoption_date": date(2025, 1, 1),
        "had_terminal_dropoff": False,
        "effective_last_day": None,
        "is_discontinued": 1,
    },
    {
        "_userId": "user_censored",
        "adoption_date": date(2025, 1, 1),
        "had_terminal_dropoff": False,
        "effective_last_day": None,
        "is_discontinued": 0,
    },
    # user_dropoff_disc_evt: terminal dropoff AND pre-dropoff AB% was low →
    # is_discontinued=1. The dropoff path emits an event at
    # week FLOOR((effective_last_day + 1 - adoption_date) / 7) = 8.
    {
        "_userId": "user_dropoff_disc_evt",
        "adoption_date": date(2025, 1, 1),
        "had_terminal_dropoff": True,
        "effective_last_day": date(2025, 3, 1),
        "is_discontinued": 1,
    },
    # user_dropoff_sustained_evt: terminal dropoff but pre-dropoff was AB-heavy
    # so durability classifies as is_discontinued=0. The dropoff path
    # must NOT emit an event for this user; KM censors at last observed week.
    {
        "_userId": "user_dropoff_sustained_evt",
        "adoption_date": date(2025, 1, 1),
        "had_terminal_dropoff": True,
        "effective_last_day": date(2025, 3, 1),
        "is_discontinued": 0,
    },
]

# user_event: 6 weeks AB (42 days) then 7 weeks TB (49 days) = 91 days total.
#   Weeks 0-5: ~100% AB. Weeks 6-12: 0% AB.
#   4-week trailing avg drops permanently -> event detected
recs_rows = (
    make_loop_recs("user_event", date(2025, 1, 1), n_days=42, dosing_mode="autobolus")
    + make_loop_recs("user_event", date(2025, 2, 12), n_days=49, dosing_mode="temp_basal")
    # user_censored: 91 days all-AB. Trailing avg stays ~100% -> no event
    + make_loop_recs("user_censored", date(2025, 1, 1), n_days=91, dosing_mode="autobolus")
    # user_dropoff_disc_evt: 3 AB days + 57 TB days, then one sparse AB day
    # on Apr 10. Pre-dropoff window (Feb 2–Mar 1) is all TB → durability stub
    # has is_discontinued=True. The sparse AB at week 14 pulls the trailing
    # 4-week avg back above the 0.20 threshold, so the trailing-AB% path
    # never fires permanently — only the dropoff path emits the event.
    + make_loop_recs("user_dropoff_disc_evt", date(2025, 1, 1), n_days=3, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_disc_evt", date(2025, 1, 4), n_days=57, dosing_mode="temp_basal")
    + make_loop_recs("user_dropoff_disc_evt", date(2025, 4, 10), n_days=1, dosing_mode="autobolus")
    # user_dropoff_sustained_evt: 60 dense AB days then one sparse AB day on
    # Apr 10. Pre-dropoff window all AB → durability stub says
    # is_discontinued=False, so the dropoff path must NOT emit an event.
    + make_loop_recs("user_dropoff_sustained_evt", date(2025, 1, 1), n_days=60, dosing_mode="autobolus")
    + make_loop_recs("user_dropoff_sustained_evt", date(2025, 4, 10), n_days=1, dosing_mode="autobolus")
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

    # 1. Row counts: user_event + user_censored = 13 weeks each (91 / 7).
    #    user_dropoff_disc_evt + user_dropoff_sustained_evt each have data
    #    in weeks 0-8 and week 14 = 10 weeks. Total: 13 + 13 + 10 + 10 = 46.
    assert len(result) == 46, f"expected 46 weekly rows, got {len(result)}"
    users = set(result["_userId"].tolist())
    assert users == {
        "user_event",
        "user_censored",
        "user_dropoff_disc_evt",
        "user_dropoff_sustained_evt",
    }, f"expected all 4 users, got {users}"
    print(f"PASS: {len(result)} weekly rows for {len(users)} users")

    # 2. Pin both sides of the 4-week-trailing-avg threshold:
    #    week 2 incomplete (only 3 weeks of data); week 3 first complete window.
    event_weeks = result[result["_userId"] == "user_event"].sort_values("week_post_adoption")
    week_2 = event_weeks[event_weeks["week_post_adoption"] == 2].iloc[0]
    assert not week_2["is_window_complete"], (
        f"week 2 should have is_window_complete=False (only 3 weeks of data)"
    )
    week_3 = event_weeks[event_weeks["week_post_adoption"] == 3].iloc[0]
    assert week_3["is_window_complete"], (
        f"week 3 should have is_window_complete=True (4 weeks of data)"
    )
    print("PASS: is_window_complete flips False→True between weeks 2 and 3")

    # 4. user_event: has_discontinuation=True, is_event_week marks the event
    assert event_weeks["has_discontinuation"].all(), (
        "all user_event rows should have has_discontinuation=True"
    )
    event_week_rows = event_weeks[event_weeks["is_event_week"].astype(bool)]
    assert len(event_week_rows) == 1, f"expected 1 event week, got {len(event_week_rows)}"
    print(f"PASS: user_event — has_discontinuation=True, event at week {event_week_rows.iloc[0]['week_post_adoption']}")

    # 5. user_censored: has_discontinuation=False, no event weeks
    censored_weeks = result[result["_userId"] == "user_censored"]
    assert not censored_weeks["has_discontinuation"].any(), (
        "all user_censored rows should have has_discontinuation=False"
    )
    assert not censored_weeks["is_event_week"].any(), (
        "user_censored should have no event weeks"
    )
    print(f"PASS: user_censored — has_discontinuation=False, {len(censored_weeks)} weeks")

    # 6. user_dropoff_disc_evt: terminal dropoff with low pre-dropoff AB%
    #    (durability stub says is_discontinued=True). The trailing-AB% path
    #    is suppressed by the sparse week-14 AB day pulling trailing avg
    #    above 0.20, so dropoff_events is the sole event source.
    disc_weeks = result[result["_userId"] == "user_dropoff_disc_evt"]
    assert disc_weeks["has_discontinuation"].all(), (
        "all user_dropoff_disc_evt rows should have has_discontinuation=True"
    )
    expected_event_week = 8  # FLOOR((Mar 2 - Jan 1) / 7) = FLOOR(60/7) = 8
    assert (disc_weeks["event_week"] == expected_event_week).all(), (
        f"user_dropoff_disc_evt event_week should be {expected_event_week}, "
        f"got {disc_weeks['event_week'].unique()}"
    )
    print(
        f"PASS: user_dropoff_disc_evt — dropoff path emits event at week "
        f"{expected_event_week} (trailing-AB% path shadowed)"
    )

    # 7. user_dropoff_sustained_evt: terminal dropoff but pre-dropoff AB% was
    #    high (durability stub says is_discontinued=False). dropoff_events
    #    must NOT fire; trailing-AB% stays high so detected_events doesn't
    #    fire either. has_discontinuation=False, KM censors at last week.
    sus_weeks = result[result["_userId"] == "user_dropoff_sustained_evt"]
    assert not sus_weeks["has_discontinuation"].any(), (
        "user_dropoff_sustained_evt should have no events (censored at last obs)"
    )
    assert not sus_weeks["is_event_week"].any(), (
        "user_dropoff_sustained_evt should have no event weeks"
    )
    print(
        f"PASS: user_dropoff_sustained_evt — high pre-dropoff AB% censored as sustained, "
        f"{len(sus_weeks)} weekly rows"
    )

    # 8. autobolus_pct and trailing_ab_pct columns present
    assert "autobolus_pct" in result.columns, "missing autobolus_pct"
    assert "trailing_ab_pct" in result.columns, "missing trailing_ab_pct"
    print("PASS: autobolus_pct and trailing_ab_pct columns present")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
