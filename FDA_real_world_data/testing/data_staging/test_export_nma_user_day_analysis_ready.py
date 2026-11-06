"""
Spark test for `data_staging/export_nma_user_day_analysis_ready.py` — the cohort table (D23).

Focused on the STAGING-specific logic this step owns (the full join is covered end-to-end by
the integration harness): every row of the universe is kept with the criteria as columns
(the version gate on a known version, a missing version judged on the date, the age gate),
the predicates in_analysis_cohort / in_strategy_contrast, the delivery strategy from the day
table's flags with NULL for a neither day, the diagnosis group as a column (type 2 and an
absent user kept), the `_userId` salted-SHA-256 pseudonymization, and the §7.5 per-user TDD
reference over the version-passing eligible days.

Run on Databricks. Silent on success.
"""

import hashlib
import os
import sys
from datetime import date

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/data_staging")
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))

from pyspark.sql import SparkSession  # type: ignore

spark = SparkSession.builder.getOrCreate()

import pandas as pd  # noqa: E402

from export_loop_recommendations import BASAL_TAGS, BOLUS_TAGS  # type: ignore # noqa: E402
from export_nma_user_day_analysis_ready import USERID_SALT, run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

P = f"{TEST_SCHEMA}._test_ar_"
RECS, CLS, BC, EP, TDD, AGE, GENDER, DX, OUT = (
    P + "recs", P + "cls", P + "bc", P + "ep", P + "tdd", P + "age", P + "gender", P + "dx", P + "out")
ALL_TABLES = [RECS, CLS, BC, EP, TDD, AGE, GENDER, DX, OUT]

D1, D2 = date(2024, 1, 1), date(2024, 1, 2)
# A day after Loop 3.4.0's release, for the missing-version date rule.
LATE = date(2024, 8, 1)

# The day table's columns with an explicit schema, so a missing version is a NULL integer.
DAY_SCHEMA = (
    "_userId STRING, day DATE, automated_boluses INT, temp_basals INT, "
    + ", ".join(f"boluses_{tag} INT" for tag in BOLUS_TAGS) + ", "
    + ", ".join(f"basals_{tag} INT" for tag in BASAL_TAGS) + ", "
    + "loop_version STRING, version_int INT, is_autobolus_day INT, is_temp_basal_day INT"
)
DAY_COLUMNS = [c.split()[0] for c in DAY_SCHEMA.split(", ")]


def _uid(raw):
    return "u" + hashlib.sha256((raw + USERID_SALT).encode("utf-8")).hexdigest()[:16]


def _cls(uid, day, be=0, ce=0, day_eligible=True, user_eligible=True):
    return {"_userId": uid, "local_day": day, "bolus_entry_count": be, "carb_entry_count": ce,
            "carb_grams_total": 0.0, "day_eligible": day_eligible, "user_eligible": user_eligible,
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


def _bc(uid, day, auto, manual=0):
    return {"_userId": uid, "local_day": day, "total_bolus_count": auto + manual,
            "manual_bolus_count": manual, "manual_normal_bolus_count": manual,
            "automatic_bolus_count": auto}


def _age(uid, day, age=40.0):
    return {"_userId": uid, "local_day": day, "age_years": age, "is_pediatric": False if age is None else age < 18}


def _dx(uid, diagnosis, label=None):
    return {"_userId": uid, "diagnosis_patients": label or diagnosis, "diagnosis_seagull": None,
            "is_jaeb": False, "is_lada": False, "diagnosis_type": diagnosis}


# Day-table rows: make_loop_recs for the classed days, then the two special cases.
recs = (make_loop_recs("u_keep", D1, 2, "autobolus", loop_version="3.2.0")        # autobolus_on
        + make_loop_recs("u_old", D1, 1, "autobolus", loop_version="3.4.0")       # fails the version gate
        + make_loop_recs("u_tb", D1, 1, "temp_basal", loop_version="3.2.0")       # temp_basal_only
        + make_loop_recs("u_neither", D1, 1, "autobolus", autobolus_count=1)      # neither class
        + make_loop_recs("u_young", D1, 1, "autobolus")                           # fails the age gate
        + make_loop_recs("u_type2", D1, 1, "autobolus")                           # a type-2 user, kept
        + make_loop_recs("u_absent", D1, 1, "autobolus")                          # absent from the lookup
        + make_loop_recs("u_noversion_early", D1, 1, "autobolus")                 # no version, early day
        + make_loop_recs("u_noversion_late", LATE, 1, "autobolus"))               # no version, late day
for row in recs:
    if row["_userId"].startswith("u_noversion"):
        row["loop_version"], row["version_int"] = None, None
users = sorted({r["_userId"] for r in recs})
days = {r["_userId"]: [] for r in recs}
for r in recs:
    days[r["_userId"]].append(r["day"])

cls = [_cls(u, d) for u in users for d in days[u]]
bc = [_bc(u, d, auto=5) for u in users for d in days[u]]
ep = [_ep(u, d) for u in users for d in days[u]]
tdd = ([_tdd("u_keep", D1, 20), _tdd("u_keep", D2, 40)]
       + [_tdd(u, d, 30) for u in users if u != "u_keep" for d in days[u]])
age = [_age(u, d, 5.0 if u == "u_young" else 40.0) for u in users for d in days[u]]
gender = [{"userid": u, "gender": "F"} for u in users]
dx = ([_dx(u, "type1") for u in users if u not in ("u_type2", "u_absent")]
      + [_dx("u_type2", "type2")])

try:
    spark.createDataFrame([tuple(r[c] for c in DAY_COLUMNS) for r in recs], DAY_SCHEMA) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RECS)
    setup_test_table(spark, CLS, cls)
    setup_test_table(spark, BC, bc)
    setup_test_table(spark, EP, ep)
    setup_test_table(spark, TDD, tdd)
    setup_test_table(spark, AGE, age)
    setup_test_table(spark, GENDER, gender)
    setup_test_table(spark, DX, dx)
    run(spark, loop_recommendations_table=RECS, classification_table=CLS,
        bolus_classification_table=BC, endpoints_table=EP, tdd_table=TDD, age_table=AGE,
        user_gender_table=GENDER, diagnosis_table=DX, output_table=OUT, output_csv=False)
    res = read_test_output(spark, OUT)
    ids = set(res["_userId"])

    # Pseudonymization: raw ids never appear; the salted-hash ids do.
    assert not any(u in ids for u in users), "raw _userId leaked"
    assert all(_uid(u) in ids for u in users), "every user of the universe is kept"
    assert len(res) == len(recs), f"every row of the universe is kept, got {len(res)} of {len(recs)}"

    by = {(r["_userId"], pd.Timestamp(r["local_day"]).date()): r for _, r in res.iterrows()}

    def row(user, day=D1):
        return by[(_uid(user), day)]

    # The version gate as a column: a known 3.4.0 fails and stays; a missing version is judged on the date.
    assert row("u_keep")["passes_loop_version_gate"] and row("u_keep")["in_analysis_cohort"]
    assert not row("u_old")["passes_loop_version_gate"] and not row("u_old")["in_analysis_cohort"]
    assert row("u_noversion_early")["passes_loop_version_gate"]
    assert not row("u_noversion_late", LATE)["passes_loop_version_gate"]
    # The age gate as a column.
    assert not row("u_young")["passes_age_gate"] and not row("u_young")["in_analysis_cohort"]
    # The delivery strategy from the day table's flags, NULL for a neither day.
    assert row("u_keep")["delivery_strategy"] == "autobolus_on" and row("u_keep")["in_strategy_contrast"]
    assert row("u_tb")["delivery_strategy"] == "temp_basal_only" and row("u_tb")["in_strategy_contrast"]
    assert pd.isna(row("u_neither")["delivery_strategy"])
    assert row("u_neither")["in_analysis_cohort"] and not row("u_neither")["in_strategy_contrast"]
    # The diagnosis group as a column, never a filter.
    assert row("u_keep")["diagnosis_group"] == "type1" and row("u_keep")["is_primary_diagnosis"]
    assert row("u_type2")["diagnosis_group"] == "type2" and not row("u_type2")["is_primary_diagnosis"]
    assert row("u_type2")["in_analysis_cohort"], "a type-2 user is in the analysis cohort; the loader selects groups"
    assert row("u_absent")["diagnosis_group"] == "unknown"
    # The per-rule bolus counts and the day table's counts ride through.
    assert int(row("u_keep")["automated_boluses"]) == 10 and int(row("u_keep")["boluses_decision_match"]) == 10
    assert int(row("u_keep")["automatic_bolus_count"]) == 5, "the FDA-verdict count from the bolus table"

    # §7.5 TDD reference over version-passing eligible days: u_keep mean = (20+40)/2 = 30.
    keep_d1 = row("u_keep")
    assert abs(float(keep_d1["mean_tdd_user"]) - 30.0) < 0.01, keep_d1["mean_tdd_user"]
    assert abs(float(keep_d1["tdd_ratio"]) - (20.0 / 30.0)) < 0.01, keep_d1["tdd_ratio"]
    assert int(keep_d1["n_eligible_days_for_tdd"]) == 2
    # u_old's one day fails the version gate, so it has no TDD reference.
    assert pd.isna(row("u_old")["mean_tdd_user"])

finally:
    teardown_test_tables(spark, *ALL_TABLES)
