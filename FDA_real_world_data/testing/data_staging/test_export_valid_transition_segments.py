"""
Unit test for export_valid_transition_segments.py.

Tests the window rule (MC, 2026-10-30): a period passes when at least 12 of
its 14 calendar days are of the right kind, so a day without dosing data counts
against it; windows rank by the calendar-day score with a tie going to the
earliest; the age filter; a missing Loop version stays NULL; and the variant
threshold (10 of 14, the _box070 build).

Run on Databricks.
"""

import math
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession  # type: ignore

import os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/data_staging"
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))
from export_loop_recommendations import BASAL_TAGS, BOLUS_TAGS  # type: ignore # noqa: E402
from export_valid_transition_segments import DEFAULT_MIN_PERIOD_DAYS, run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
LOOP_RECS_TABLE = f"{TEST_SCHEMA}._test_vts_loop_recs"
USER_DATES_TABLE = f"{TEST_SCHEMA}._test_vts_user_dates"
USER_GENDER_TABLE = f"{TEST_SCHEMA}._test_vts_user_gender"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_vts_output"
OUTPUT_TABLE_10 = f"{TEST_SCHEMA}._test_vts_output_10"

ALL_TABLES = [LOOP_RECS_TABLE, USER_DATES_TABLE, USER_GENDER_TABLE, OUTPUT_TABLE, OUTPUT_TABLE_10]

# The day table's columns, written with an explicit schema so a missing version
# is a NULL integer (through pandas it would become NaN).
DAY_SCHEMA = (
    "_userId STRING, day DATE, automated_boluses INT, temp_basals INT, "
    + ", ".join(f"boluses_{tag} INT" for tag in BOLUS_TAGS) + ", "
    + ", ".join(f"basals_{tag} INT" for tag in BASAL_TAGS) + ", "
    + "loop_version STRING, version_int INT, is_autobolus_day INT, is_temp_basal_day INT"
)
DAY_COLUMNS = [c.split()[0] for c in DAY_SCHEMA.split(", ")]

START = date(2025, 1, 1)


def d(i):
    return START + timedelta(days=i)


def tb(user, first, last):
    return make_loop_recs(user, d(first), n_days=last - first + 1, dosing_mode="temp_basal")


def ab(user, first, last):
    return make_loop_recs(user, d(first), n_days=last - first + 1, dosing_mode="autobolus")


# --- Test data (day indexes from START) ---
# user_transition:    14 temp-basal days, then 14 autobolus days: one window, score 1.
# user_no_transition: 28 temp-basal days: no window.
# user_child:         a clean transition, under 6 at the window start: dropped.
# user_gaps_12:       temp-basal days 0-13 except 3 and 8: 12 of 14 passes.
# user_gaps_11:       temp-basal days 0-13 except 3, 8 and 11: 11 of 14 fails at 12
#                     (the former box passed it: 11 of 11 classified days), passes at 10.
# user_tie:           two clean switches (days 0-27 and 40-67): two windows at score 1,
#                     the earliest ranked first.
# user_no_version:    a clean transition with no parsable Loop version: NULL, not 0.
loop_recs_rows = (
    tb("user_transition", 0, 13) + ab("user_transition", 14, 27)
    + tb("user_no_transition", 0, 27)
    + tb("user_child", 0, 13) + ab("user_child", 14, 27)
    + tb("user_gaps_12", 0, 2) + tb("user_gaps_12", 4, 7) + tb("user_gaps_12", 9, 13) + ab("user_gaps_12", 14, 27)
    + tb("user_gaps_11", 0, 2) + tb("user_gaps_11", 4, 7) + tb("user_gaps_11", 9, 10) + tb("user_gaps_11", 12, 13)
    + ab("user_gaps_11", 14, 27)
    + tb("user_tie", 0, 13) + ab("user_tie", 14, 27) + tb("user_tie", 40, 53) + ab("user_tie", 54, 67)
    + tb("user_no_version", 0, 13) + ab("user_no_version", 14, 27)
)
for row in loop_recs_rows:
    if row["_userId"] == "user_no_version":
        row["loop_version"], row["version_int"] = None, None

adults = ["user_transition", "user_no_transition", "user_gaps_12", "user_gaps_11", "user_tie", "user_no_version"]
user_dates_rows = [{"userid": u, "dob": "1990-01-01", "diagnosis_date": "2000-01-01"} for u in adults]
user_dates_rows.append({"userid": "user_child", "dob": "2020-01-01", "diagnosis_date": "2023-01-01"})
user_gender_rows = [{"userid": u, "gender": "female"} for u in adults + ["user_child"]]


def _isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


# --- Run test ---
try:
    assert DEFAULT_MIN_PERIOD_DAYS == 12, DEFAULT_MIN_PERIOD_DAYS
    try:
        run(spark, output_table=OUTPUT_TABLE, min_period_days=15)
    except ValueError:
        print("PASS: a threshold above 14 days is refused")
    else:
        raise AssertionError("min_period_days=15 must raise")

    spark.createDataFrame([tuple(r[c] for c in DAY_COLUMNS) for r in loop_recs_rows], schema=DAY_SCHEMA) \
        .write.mode("overwrite").saveAsTable(LOOP_RECS_TABLE)
    setup_test_table(spark, USER_DATES_TABLE, user_dates_rows)
    setup_test_table(spark, USER_GENDER_TABLE, user_gender_rows)

    kwargs = dict(loop_recommendations_table=LOOP_RECS_TABLE, user_dates_table=USER_DATES_TABLE,
                  user_gender_table=USER_GENDER_TABLE)
    run(spark, output_table=OUTPUT_TABLE, **kwargs)
    run(spark, output_table=OUTPUT_TABLE_10, min_period_days=10, **kwargs)

    result = read_test_output(spark, OUTPUT_TABLE)
    users = set(result["_userId"])
    assert users == {"user_transition", "user_gaps_12", "user_tie", "user_no_version"}, sorted(users)
    print("PASS: the window rule keeps the right users (no transition, the child, and 11 of 14 dropped)")

    def best(user):
        rows = result[result["_userId"] == user].sort_values("segment_rank")
        return rows.iloc[0], rows

    row, rows = best("user_transition")
    assert len(rows) == 1 and int(row["segment_rank"]) == 1, rows[["tb_to_ab_seg1_start", "segment_rank"]]
    assert int(row["tb_to_ab_temp_basal_days_seg1"]) == 14 and int(row["tb_to_ab_autobolus_days_seg2"]) == 14
    assert float(row["tb_to_ab_segment_score"]) == 1.0 and float(row["tb_to_ab_coverage_seg1"]) == 1.0
    assert row["gender"] == "female" and row["tb_to_ab_age_years"] > 6
    print("PASS: a clean switch is one window at score 1, demographics joined")

    row, _ = best("user_gaps_12")
    assert int(row["tb_to_ab_temp_basal_days_seg1"]) == 12, row["tb_to_ab_temp_basal_days_seg1"]
    assert abs(float(row["tb_to_ab_segment_score"]) - 12 / 14) < 1e-9, row["tb_to_ab_segment_score"]
    print("PASS: 12 temporary-basal days of 14 pass; the score counts the 14 calendar days")

    row, rows = best("user_tie")
    scores = rows["tb_to_ab_segment_score"].astype(float).tolist()
    assert scores[:2] == [1.0, 1.0], scores
    assert str(rows.iloc[0]["tb_to_ab_seg1_start"]) == str(d(0)) and str(rows.iloc[1]["tb_to_ab_seg1_start"]) == str(d(40)), \
        rows[["tb_to_ab_seg1_start", "segment_rank"]]
    print("PASS: two windows at score 1, the earliest ranked first")

    row, _ = best("user_no_version")
    assert _isnull(row["tb_to_ab_max_loop_version_int"]), row["tb_to_ab_max_loop_version_int"]
    print("PASS: a window with no parsable version carries NULL, for the loaders' date rule")

    users_10 = set(read_test_output(spark, OUTPUT_TABLE_10)["_userId"])
    assert "user_gaps_11" in users_10 and users <= users_10, sorted(users_10)
    print("PASS: at 10 of 14 (the _box070 build) 11 temporary-basal days pass")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
