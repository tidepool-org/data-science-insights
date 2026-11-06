"""
Spark test for `data_staging/export_user_day_age.py` — per-user-day age + pediatric flag.

Covers: DOB → age (DATEDIFF/365.25), the §7.6 pediatric cutoff (18), and the corrupt-DOB nulling
(unknown DOB, future-dated → negative, or > MAX_PLAUSIBLE_AGE → NULL age + NULL is_pediatric).

Runs on Databricks (or local pyspark); SKIPS cleanly elsewhere. Silent on success.
"""

import os
import sys
from datetime import date

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "no_meal_announcement/testing/data_staging")
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))

try:
    from pyspark.sql import SparkSession  # type: ignore
    spark = SparkSession.builder.getOrCreate()
except (ImportError, RuntimeError) as _spark_err:
    import pytest
    pytest.skip(f"Spark unavailable locally ({_spark_err}); run on Databricks.",
                allow_module_level=True)

import pandas as pd  # noqa: E402

from export_user_day_age import run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

RECS_TABLE = f"{TEST_SCHEMA}._test_input_age_recs"
DATES_TABLE = f"{TEST_SCHEMA}._test_input_age_dates"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_age"
ALL_TABLES = [RECS_TABLE, DATES_TABLE, OUTPUT_TABLE]

DAY = date(2024, 1, 1)
RECS = (
    make_loop_recs("u_adult", DAY, 1, "autobolus")
    + make_loop_recs("u_ped", DAY, 1, "autobolus")
    + make_loop_recs("u_unknown", DAY, 1, "autobolus")
    + make_loop_recs("u_future", DAY, 1, "autobolus")
    + make_loop_recs("u_ancient", DAY, 1, "autobolus")
)
DATES = [
    {"userid": "u_adult", "dob": date(1985, 6, 15)},    # ~38.5 → adult
    {"userid": "u_ped", "dob": date(2014, 1, 1)},        # 10.0 → pediatric
    # u_unknown omitted → no DOB row → NULL.
    {"userid": "u_future", "dob": date(2030, 1, 1)},     # future-dated → negative age → NULL
    {"userid": "u_ancient", "dob": date(1850, 1, 1)},    # >120 yr → corrupt → NULL
]

try:
    setup_test_table(spark, RECS_TABLE, RECS)
    setup_test_table(spark, DATES_TABLE, DATES)
    run(spark, loop_recommendations_table=RECS_TABLE, user_dates_table=DATES_TABLE,
        output_table=OUTPUT_TABLE)
    by_user = {r["_userId"]: (r["age_years"], r["is_pediatric"])
               for _, r in read_test_output(spark, OUTPUT_TABLE).iterrows()}

    age, ped = by_user["u_adult"]
    assert 38.0 <= age <= 39.0 and ped == False, by_user["u_adult"]  # noqa: E712
    age, ped = by_user["u_ped"]
    assert 9.0 <= age <= 11.0 and ped == True, by_user["u_ped"]      # noqa: E712
    for u in ("u_unknown", "u_future", "u_ancient"):
        age, ped = by_user[u]
        assert pd.isna(age) and pd.isna(ped), f"{u}: expected NULL age + is_pediatric, got {(age, ped)}"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
