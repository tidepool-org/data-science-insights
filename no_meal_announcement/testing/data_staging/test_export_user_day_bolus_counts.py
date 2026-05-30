"""
Unit test for `export_user_day_bolus_counts.py` — Unit 7.

Sub-units exercised:
- 7.1 Meal-bolus identification via food record within ±15 min.
- 7.2 Autobolus exclusion: `subType='automated'`, plus the disguised case
      (subType='normal' + reason='loop' DD within 5s + no normalBolus DD ±15s).
- 7.3 Per-day aggregation row count = distinct (user, day) pairs with ≥1
      surviving normal bolus.

Run on Databricks (or local pyspark): authored module-level so the
existing FDA `runpy.run_path()` discovery in `testing/run_all_tests.py`
will pick it up once the NMA runner is wired up.
"""

import json
import os
import sys
from datetime import date

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook view of a .py file doesn't define __file__.
    _here = (
        "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
        "no_meal_announcement/testing/data_staging"
    )
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))

# Acquire a SparkSession; locally (no Databricks Connect) defer to Databricks
# via pytest.skip so collection of the rest of the NMA suite still works.
try:
    from pyspark.sql import SparkSession  # type: ignore
    spark = SparkSession.builder.getOrCreate()
except (ImportError, RuntimeError) as _spark_err:
    import pytest
    pytest.skip(
        f"Spark unavailable locally ({_spark_err}); run on Databricks via runpy.run_path.",
        allow_module_level=True,
    )

from export_user_day_bolus_counts import run  # type: ignore # noqa: E402

# nma_test_helpers re-exports setup/read/assert from FDA; teardown_test_tables
# and TEST_SCHEMA come straight from the FDA module.
from nma_test_helpers import (  # type: ignore # noqa: E402
    assert_row_count,
    read_test_output,
    setup_test_table,
)
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

INPUT_TABLE = f"{TEST_SCHEMA}._test_input_bolus_counts"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_bolus_counts"
ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]


def _row(_userId, time_string, type_, subType=None, reason=None, nutrition=None):
    """Minimal BDDP row for bolus_counts SQL.

    Every column read by the SQL (`_userId`, `time_string`, `type`, `subType`,
    `reason`, `nutrition`) is present so Spark's schema inference doesn't drop
    a column that's all-None across the fixture.
    """
    return {
        "_userId": _userId,
        "time_string": time_string,
        "type": type_,
        "subType": subType,
        "reason": reason,
        "nutrition": nutrition,
    }


def _food(carb_grams):
    return json.dumps({
        "carbohydrate": {"net": carb_grams, "units": "grams"},
    })


# ---------------------------------------------------------------------------
# Fixture — each "day" block targets a specific sub-unit assertion below.
# ---------------------------------------------------------------------------

TEST_ROWS = [
    # ---- 7.1: Day 1 (user_a, 2025-02-01) — 3 normal boluses, 2 paired w/ food ----
    _row("user_a", "2025-02-01 12:00:00", "food",   nutrition=_food(40.0)),
    _row("user_a", "2025-02-01 12:00:00", "bolus",  subType="normal"),     # meal (at food time)
    _row("user_a", "2025-02-01 12:14:30", "bolus",  subType="normal"),     # meal (within +15min)
    _row("user_a", "2025-02-01 18:00:00", "bolus",  subType="normal"),     # non-meal (5h44m later)

    # ---- 7.1: Day 2 (user_a, 2025-02-02) — positive-side boundary at ±15min ----
    _row("user_a", "2025-02-02 09:00:00", "food",   nutrition=_food(30.0)),
    _row("user_a", "2025-02-02 09:15:00", "bolus",  subType="normal"),     # meal (exactly +15min)
    _row("user_a", "2025-02-02 09:16:00", "bolus",  subType="normal"),     # non-meal (+16min)

    # ---- 7.2: Day 3 (user_a, 2025-02-03) — automated boluses excluded ----
    _row("user_a", "2025-02-03 06:00:00", "bolus",  subType="automated"),
    _row("user_a", "2025-02-03 06:30:00", "bolus",  subType="automated"),
    _row("user_a", "2025-02-03 07:00:00", "bolus",  subType="normal"),     # only this counts

    # ---- 7.2: Day 4 (user_a, 2025-02-04) — all-automated day → DAY ABSENT ----
    _row("user_a", "2025-02-04 06:00:00", "bolus",  subType="automated"),
    _row("user_a", "2025-02-04 06:30:00", "bolus",  subType="automated"),
    _row("user_a", "2025-02-04 07:00:00", "bolus",  subType="automated"),

    # ---- 7.2: Day 5 (user_a, 2025-02-05) — disguised autobolus → EXCLUDED ----
    # subType='normal' + loop DD 3s prior + NO normalBolus DD nearby.
    _row("user_a", "2025-02-05 10:00:00", "dosingDecision", reason="loop"),
    _row("user_a", "2025-02-05 10:00:03", "bolus",          subType="normal"),

    # ---- 7.2: Day 6 (user_a, 2025-02-06) — genuine manual bolus KEPT ----
    # loop DD 3s prior, but normalBolus DD within ±15s rescues the bolus.
    _row("user_a", "2025-02-06 11:00:00", "dosingDecision", reason="loop"),
    _row("user_a", "2025-02-06 11:00:03", "bolus",          subType="normal"),
    _row("user_a", "2025-02-06 11:00:10", "dosingDecision", reason="normalBolus"),

    # ---- 7.3: Day 7 (user_b, 2025-02-07) — second user; meal-only day ----
    _row("user_b", "2025-02-07 12:00:00", "food",   nutrition=_food(50.0)),
    _row("user_b", "2025-02-07 12:00:00", "bolus",  subType="normal"),     # meal

    # ---- 7.3: Day 8 (user_b, 2025-02-08) — bad timestamp on one bolus → dropped ----
    _row("user_b", "2025-02-08 12:00:00", "bolus",         subType="normal"),
    _row("user_b", "not-a-timestamp",     "bolus",         subType="normal"),

    # ---- 7.3: Day 9 (user_c, 2025-02-09) — food with NULL carb → bolus is non-meal ----
    _row("user_c", "2025-02-09 12:00:00", "food",   nutrition=json.dumps({"carbohydrate": {"net": None}})),
    _row("user_c", "2025-02-09 12:00:00", "bolus",  subType="normal"),

    # ---- 7.1: Day 10 (user_a, 2025-02-10) — negative-side boundary at ±15min ----
    _row("user_a", "2025-02-10 12:45:00", "bolus",  subType="normal"),     # meal (-15min)
    _row("user_a", "2025-02-10 12:44:00", "bolus",  subType="normal"),     # non-meal (-16min)
    _row("user_a", "2025-02-10 13:00:00", "food",   nutrition=_food(20.0)),
]


try:
    setup_test_table(spark, INPUT_TABLE, TEST_ROWS)
    run(spark, input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)

    by_day = {
        (r["_userId"], r["day"]): (
            int(r["meal_bolus_count"]),
            int(r["non_meal_bolus_count"]),
        )
        for _, r in result.iterrows()
    }

    # ---------------------------- 7.1 ---------------------------------------
    assert by_day[("user_a", date(2025, 2, 1))] == (2, 1), (
        f"day 1: expected (2, 1) — 2 boluses inside food's ±15min window, 1 outside; "
        f"got {by_day[('user_a', date(2025, 2, 1))]}"
    )
    assert by_day[("user_a", date(2025, 2, 2))] == (1, 1), (
        f"day 2 (±15min boundary): expected (1, 1), got {by_day[('user_a', date(2025, 2, 2))]}"
    )
    assert by_day[("user_a", date(2025, 2, 10))] == (1, 1), (
        f"day 10 (negative boundary): expected (1, 1), got {by_day[('user_a', date(2025, 2, 10))]}"
    )
    print("PASS: 7.1 meal-bolus identification within ±15min (incl. boundaries)")

    # ---------------------------- 7.2 ---------------------------------------
    assert by_day[("user_a", date(2025, 2, 3))] == (0, 1), (
        f"day 3 (automated excluded): expected (0, 1), got {by_day[('user_a', date(2025, 2, 3))]}"
    )
    assert ("user_a", date(2025, 2, 4)) not in by_day, (
        "day 4 (all-automated day): should be absent from output"
    )
    assert ("user_a", date(2025, 2, 5)) not in by_day, (
        "day 5 (disguised autobolus): should be absent from output"
    )
    assert by_day[("user_a", date(2025, 2, 6))] == (0, 1), (
        f"day 6 (genuine manual via normalBolus DD): expected (0, 1), got {by_day[('user_a', date(2025, 2, 6))]}"
    )
    print("PASS: 7.2 autobolus exclusion (subType='automated' + disguised)")

    # ---------------------------- 7.3 ---------------------------------------
    # Per-day row count: 8 surviving (days 1, 2, 3, 6, 7, 8, 9, 10).
    # Days 4 (all automated) and 5 (disguised) are absent.
    assert_row_count(result, 8, "surviving (user, day) pairs")

    # Multi-user spread.
    user_b_days = sorted(d for u, d in by_day if u == "user_b")
    assert user_b_days == [date(2025, 2, 7), date(2025, 2, 8)], (
        f"user_b should appear on days 7 and 8 only, got {user_b_days}"
    )
    assert by_day[("user_b", date(2025, 2, 7))] == (1, 0), (
        f"day 7 (user_b meal): expected (1, 0), got {by_day[('user_b', date(2025, 2, 7))]}"
    )
    assert by_day[("user_b", date(2025, 2, 8))] == (0, 1), (
        f"day 8 (user_b bad-ts dropped): expected (0, 1), got {by_day[('user_b', date(2025, 2, 8))]}"
    )

    assert by_day[("user_c", date(2025, 2, 9))] == (0, 1), (
        f"day 9 (user_c null-carb food): expected (0, 1) — food with null carb does NOT mark bolus as meal, "
        f"got {by_day[('user_c', date(2025, 2, 9))]}"
    )
    print("PASS: 7.3 per-day aggregation (multi-user, bad-ts excluded, null-carb food rejected)")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
