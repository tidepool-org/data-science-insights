"""Unit test for export_loop_users.py.

Covers the minimum assertions from ROADMAP.md for the registry:
  1. DD-only user included, has_dd_signal=true, has_hk_signal=false
  2. HK-only user included, has_hk_signal=true, has_dd_signal=false
  3. User with both signals has both flags set
  4. Non-Loop user (CBG only, no DD, no Loop-sourced bolus/basal) excluded
  5. HK-only user with no MetadataKeyAutomaticallyIssued still in cohort
     (membership uses source.name='Loop', not the automation flag)
  6. max_loop_version uses numeric comparison (3.10.1 > 3.2.0), not lexicographic
  7. Demographics join normalizes across case conventions:
     bddp_user_dates.userid (lower) / user_gender.userId (camel) / BDDP._userId (underscored)
  8. first_loop_day / last_loop_day / loop_day_count span both DD and HK signals

Run on Databricks.
"""

import os
import sys
from datetime import date

import pandas as pd
from pyspark.sql import SparkSession  # type: ignore


_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))

from export_loop_users import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    assert_row_count,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)
from create_test_bddp_data import (  # noqa: E402
    make_basal,
    make_bolus,
    make_cbg,
    make_dosing_decision,
    make_user_dates,
    make_user_gender,
)


spark = SparkSession.builder.getOrCreate()

INPUT_TABLE         = f"{TEST_SCHEMA}._test_input_loop_users_bddp"
USER_DATES_TABLE    = f"{TEST_SCHEMA}._test_input_loop_users_dates"
USER_GENDER_TABLE   = f"{TEST_SCHEMA}._test_input_loop_users_gender"
OUTPUT_TABLE        = f"{TEST_SCHEMA}._test_output_loop_users"


# --- Fixture ---
# Five users exercise the eight assertions above:
#   user_dd      : DD-only (covers 1, 8)
#   user_hk      : HK-only, no automated deliveries (covers 2, 5)
#   user_both    : both signals (covers 3)
#   user_nonloop : CBG + manual bolus, no Loop signal anywhere (covers 4)
#   user_versions: DD with 3.2.0 early and 3.10.1 late (covers 6)
# Demographics across all three case conventions (covers 7).

BDDP_ROWS = [
    # --- user_dd: DD only, days 2025-01-10 and 2025-01-15 (span check) ---
    make_dosing_decision("user_dd", "2025-01-10 08:00:00", reason="loop", version="3.2.0"),
    make_dosing_decision("user_dd", "2025-01-15 09:00:00", reason="loop", version="3.2.0"),
    make_bolus("user_dd",            "2025-01-10 08:00:03", subType="automated"),
    make_cbg("user_dd",              "2025-01-10 08:05:00", value="5.5"),
    make_cbg("user_dd",              "2025-01-15 09:05:00", value="5.6"),

    # --- user_hk: HK source=Loop but NO MetadataKeyAutomaticallyIssued (case 5) ---
    make_bolus("user_hk", "2025-02-01 12:00:00", subType="normal",
               loop_sourced=True, hk_automated=False, version="3.4.0"),
    make_basal("user_hk", "2025-02-20 15:00:00", deliveryType="scheduled",
               loop_sourced=True, hk_automated=False, version="3.4.0"),

    # --- user_both: DD + HK signals ---
    make_dosing_decision("user_both", "2025-03-01 10:00:00", reason="loop", version="3.4.0"),
    make_bolus("user_both", "2025-03-01 10:00:03", subType="automated",
               loop_sourced=True, hk_automated=True, version="3.4.0"),

    # --- user_nonloop: no DD reason='loop', no Loop-sourced deliveries ---
    make_cbg("user_nonloop",   "2025-04-01 10:00:00", value="5.5"),
    make_bolus("user_nonloop", "2025-04-01 10:00:03", subType="normal", loop_sourced=False),
    # A DD with reason='override' — not 'loop' — must not put them in the cohort.
    make_dosing_decision("user_nonloop", "2025-04-01 10:00:05", reason="override"),

    # --- user_versions: DD with 3.2.0 early, 3.10.1 late ---
    make_dosing_decision("user_versions", "2025-05-01 09:00:00", reason="loop", version="3.2.0"),
    make_dosing_decision("user_versions", "2025-05-02 10:00:00", reason="loop", version="3.10.1"),
]

USER_DATES_ROWS = [
    make_user_dates("user_dd",       dob="1990-01-01", diagnosis_date="2005-06-15"),
    make_user_dates("user_hk",       dob="1985-03-20", diagnosis_date="2000-08-01"),
    make_user_dates("user_both",     dob="1995-07-04", diagnosis_date="2010-12-31"),
    make_user_dates("user_nonloop",  dob="1980-05-05", diagnosis_date="1999-01-01"),
    make_user_dates("user_versions", dob="1988-11-11", diagnosis_date="2001-02-14"),
]

USER_GENDER_ROWS = [
    make_user_gender("user_dd",       "F"),
    make_user_gender("user_hk",       "M"),
    make_user_gender("user_both",     "F"),
    # user_nonloop + user_versions intentionally absent to verify LEFT JOIN yields NULL gender.
]


try:
    setup_test_table(spark, INPUT_TABLE,       BDDP_ROWS)
    setup_test_table(spark, USER_DATES_TABLE,  USER_DATES_ROWS)
    setup_test_table(spark, USER_GENDER_TABLE, USER_GENDER_ROWS)

    run(
        spark,
        input_table=INPUT_TABLE,
        user_dates_table=USER_DATES_TABLE,
        user_gender_table=USER_GENDER_TABLE,
        output_table=OUTPUT_TABLE,
    )
    result = read_test_output(spark, OUTPUT_TABLE)

    # 1-4. Cohort composition: 4 users in, 1 user (user_nonloop) out.
    assert_row_count(result, 4, "Loop-user cohort size")
    by_user = {r["_userId"]: r for _, r in result.iterrows()}
    assert "user_nonloop" not in by_user, "non-Loop user leaked into cohort"
    for uid in ("user_dd", "user_hk", "user_both", "user_versions"):
        assert uid in by_user, f"expected {uid} in cohort"
    print("PASS: Loop-user cohort membership (DD OR HK source='Loop')")

    # 1. DD-only user flags
    u = by_user["user_dd"]
    assert bool(u["has_dd_signal"]) and not bool(u["has_hk_signal"]), (
        f"user_dd flags: dd={u['has_dd_signal']}, hk={u['has_hk_signal']}"
    )
    print("PASS: DD-only user — has_dd_signal=true, has_hk_signal=false")

    # 2 & 5. HK-only user (no MetadataKeyAutomaticallyIssued) still in cohort
    u = by_user["user_hk"]
    assert bool(u["has_hk_signal"]) and not bool(u["has_dd_signal"]), (
        f"user_hk flags: dd={u['has_dd_signal']}, hk={u['has_hk_signal']}"
    )
    print("PASS: HK-only user with NO automated deliveries still in cohort "
          "(membership uses source.name='Loop', not the automation flag)")

    # 3. Both signals
    u = by_user["user_both"]
    assert bool(u["has_dd_signal"]) and bool(u["has_hk_signal"])
    print("PASS: user with both DD and HK signals has both flags set")

    # 6. Numeric version sort: 3.10.1 > 3.2.0
    u = by_user["user_versions"]
    assert u["max_loop_version"] == "3.10.1", (
        f"expected max_loop_version='3.10.1', got {u['max_loop_version']}"
    )
    assert u["first_loop_version"] == "3.2.0"
    assert u["last_loop_version"] == "3.10.1"
    print("PASS: max_loop_version uses numeric sort (3.10.1 > 3.2.0)")

    # 7. Demographics join across three case conventions
    u = by_user["user_dd"]
    assert u["dob"] == date(1990, 1, 1), f"user_dd dob: {u['dob']}"
    assert u["diagnosis_date"] == date(2005, 6, 15)
    assert u["gender"] == "F"
    print("PASS: demographics join across userid / userId / _userId case conventions")

    # LEFT JOIN: user_versions has no gender row, should be NULL
    u = by_user["user_versions"]
    assert pd.isna(u["gender"]), f"user_versions gender should be NULL, got {u['gender']!r}"
    assert u["dob"] == date(1988, 11, 11)
    print("PASS: missing user_gender row yields NULL gender (LEFT JOIN)")

    # 8. Date spans across both signals (user_dd: 2025-01-10 → 2025-01-15, 2 distinct days)
    u = by_user["user_dd"]
    assert u["first_loop_day"] == date(2025, 1, 10)
    assert u["last_loop_day"] == date(2025, 1, 15)
    assert u["loop_day_count"] == 2
    assert u["dd_day_count"] == 2
    assert u["hk_day_count"] == 0
    print("PASS: first/last/count loop days computed correctly for DD-only user")

    # HK-only user spans 2025-02-01 → 2025-02-20 (2 distinct HK days)
    u = by_user["user_hk"]
    assert u["first_loop_day"] == date(2025, 2, 1)
    assert u["last_loop_day"] == date(2025, 2, 20)
    assert u["loop_day_count"] == 2
    assert u["hk_day_count"] == 2
    assert u["dd_day_count"] == 0
    print("PASS: first/last/count loop days computed correctly for HK-only user")

    # 9. Activity stats populated (COALESCE'd to 0 where absent)
    u = by_user["user_dd"]
    assert u["cbg_day_count"] == 2, f"user_dd cbg_day_count: {u['cbg_day_count']}"
    assert u["bolus_count"] == 1
    assert u["basal_count"] == 0
    assert u["food_count"] == 0
    print("PASS: per-record-type activity counts populated, zero when absent")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(
        spark, INPUT_TABLE, USER_DATES_TABLE, USER_GENDER_TABLE, OUTPUT_TABLE,
    )
