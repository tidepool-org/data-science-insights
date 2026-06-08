"""
Spark test for `data_staging/export_user_day_analysis_ready.py` — the denormalized join.

Focused on the STAGING-specific logic this step owns (the full join is already covered end-to-end by
the integration harness): the PLN-1001 Loop-version cohort filter (known version_int < 3.4.0 kept,
>= dropped), the `_userId` salted-SHA-256 pseudonymization (raw id never leaves), the §7.5 per-user
TDD reference (mean_tdd_user + tdd_ratio over eligible days), and the §7.3 delivery_strategy threshold.

Runs on Databricks (or local pyspark); SKIPS cleanly elsewhere. Silent on success.
"""

import hashlib
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

from export_user_day_analysis_ready import USERID_SALT, run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

P = f"{TEST_SCHEMA}._test_ar_"
RECS, CLS, BC, EP, TDD, AGE, GENDER, OUT = (
    P + "recs", P + "cls", P + "bc", P + "ep", P + "tdd", P + "age", P + "gender", P + "out")
ALL_TABLES = [RECS, CLS, BC, EP, TDD, AGE, GENDER, OUT]

D1, D2 = date(2024, 1, 1), date(2024, 1, 2)


def _uid(raw):
    return "u" + hashlib.sha256((raw + USERID_SALT).encode("utf-8")).hexdigest()[:16]


def _cls(uid, day, be=0, ce=0):
    return {"_userId": uid, "local_day": day, "bolus_entry_count": be, "carb_entry_count": ce,
            "carb_grams_total": 0.0, "day_eligible": True, "user_eligible": True,
            "ce_eq_0": ce == 0, "be_eq_0": be == 0, "be_le_1": be <= 1,
            "in_ce0_be0": ce == 0 and be == 0, "in_ce0_be_le1": ce == 0 and be <= 1,
            "in_ce0_be_inf": ce == 0, "in_ce_gt0": ce > 0, "in_ce_ge3_be_ge3": ce >= 3 and be >= 3}


def _ep(uid, day):
    return {"_userId": uid, "local_day": day, "cbg_count": 288, "tbr_very_low": 0.5, "tbr": 2.0,
            "tir": 70.0, "tar": 25.0, "tar_very_high": 5.0, "mean_glucose": 150.0, "cv": 35.0,
            "hypo_events": 0.1}


def _tdd(uid, day, units):
    return {"_userId": uid, "local_day": day, "basal_units": units * 0.6,
            "bolus_units": units * 0.4, "tdd_units": float(units), "basal_source": "healthkit"}


def _bc(uid, day, auto):
    return {"_userId": uid, "local_day": day, "automatic_bolus_count": auto,
            "auto_hk_count": auto, "auto_dd_count": 0}


def _age(uid, day):
    return {"_userId": uid, "local_day": day, "age_years": 40.0, "is_pediatric": False}


# u_keep: 2 eligible days (TDD 20, 40 → mean 30), version 3.2.0 (kept), autobolus_on (auto=5).
# u_dropver: version 3.4.0 → version_int 3_004_000 → DROPPED by the cohort filter.
# u_tb: version 3.2.0 (kept), automatic_bolus_count 0 → temp_basal_only.
recs = (make_loop_recs("u_keep", D1, 2, "autobolus", loop_version="3.2.0")
        + make_loop_recs("u_dropver", D1, 1, "autobolus", loop_version="3.4.0")
        + make_loop_recs("u_tb", D1, 1, "autobolus", loop_version="3.2.0"))
cls = [_cls("u_keep", D1), _cls("u_keep", D2), _cls("u_dropver", D1), _cls("u_tb", D1)]
bc = [_bc("u_keep", D1, 5), _bc("u_keep", D2, 5), _bc("u_dropver", D1, 5), _bc("u_tb", D1, 0)]
ep = [_ep("u_keep", D1), _ep("u_keep", D2), _ep("u_dropver", D1), _ep("u_tb", D1)]
tdd = [_tdd("u_keep", D1, 20), _tdd("u_keep", D2, 40), _tdd("u_dropver", D1, 30), _tdd("u_tb", D1, 30)]
age = [_age("u_keep", D1), _age("u_keep", D2), _age("u_dropver", D1), _age("u_tb", D1)]
gender = [{"userid": "u_keep", "gender": "M"}, {"userid": "u_dropver", "gender": "M"},
          {"userid": "u_tb", "gender": "F"}]

try:
    setup_test_table(spark, RECS, recs)
    setup_test_table(spark, CLS, cls)
    setup_test_table(spark, BC, bc)
    setup_test_table(spark, EP, ep)
    setup_test_table(spark, TDD, tdd)
    setup_test_table(spark, AGE, age)
    setup_test_table(spark, GENDER, gender)
    run(spark, loop_recommendations_table=RECS, classification_table=CLS,
        bolus_classification_table=BC, endpoints_table=EP, tdd_table=TDD, age_table=AGE,
        user_gender_table=GENDER, output_table=OUT, output_csv=False)
    res = read_test_output(spark, OUT)
    ids = set(res["_userId"])

    # Pseudonymization: raw ids never appear; the salted-hash ids do.
    assert "u_keep" not in ids and "u_dropver" not in ids and "u_tb" not in ids, "raw _userId leaked"
    assert _uid("u_keep") in ids and _uid("u_tb") in ids
    # Cohort filter: u_dropver (Loop 3.4.0) is dropped entirely.
    assert _uid("u_dropver") not in ids, "version_int >= 3_004_000 should be filtered out"
    assert len(res) == 3, f"expected 3 kept rows (u_keep×2 + u_tb), got {len(res)}"

    by = {(r["_userId"], pd.Timestamp(r["local_day"]).date()): r for _, r in res.iterrows()}

    # §7.5 TDD reference + ratio: u_keep mean = (20+40)/2 = 30; ratio on the 20-unit day = 0.667.
    keep_d1 = by[(_uid("u_keep"), D1)]
    assert abs(float(keep_d1["mean_tdd_user"]) - 30.0) < 0.01, keep_d1["mean_tdd_user"]
    assert abs(float(keep_d1["tdd_ratio"]) - (20.0 / 30.0)) < 0.01, keep_d1["tdd_ratio"]

    # §7.3 delivery strategy from automatic_bolus_count (>=3 → autobolus_on).
    assert keep_d1["delivery_strategy"] == "autobolus_on"
    assert by[(_uid("u_tb"), D1)]["delivery_strategy"] == "temp_basal_only"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
