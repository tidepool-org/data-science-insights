"""
Spark test for `data_staging/export_user_day_classification.py` — §7.2 nested arm flags + §7.1
eligibility.

Covers: the four nested classifications (CE=0/BE=0 ⊆ CE=0/BE<=1 ⊆ CE=0/BE<=inf; CE>0 comparator),
the CE>=3/BE>=3 HMA flag, day_eligible (coverage is_eligible, COALESCE FALSE when no coverage row),
and user_eligible (>=10 eligible user-days). Counts coalesce to 0 (zero-entry days surface).

Runs on Databricks (or local pyspark); SKIPS cleanly elsewhere. Silent on success.
"""

import os
import sys
from datetime import date, timedelta

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

from export_user_day_classification import run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

RECS_TABLE = f"{TEST_SCHEMA}._test_input_cls_recs"
COVERAGE_TABLE = f"{TEST_SCHEMA}._test_input_cls_coverage"
BOLUS_TABLE = f"{TEST_SCHEMA}._test_input_cls_bolus"
CARBS_TABLE = f"{TEST_SCHEMA}._test_input_cls_carbs"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_cls"
ALL_TABLES = [RECS_TABLE, COVERAGE_TABLE, BOLUS_TABLE, CARBS_TABLE, OUTPUT_TABLE]


def _d(n):
    return date(2025, 1, n)


# user_a: 12 days. d1-5 exercise the classifications; d6-10 CE=0/BE=0 filler (→ 10 eligible days →
# user_eligible). d11 coverage-ineligible, d12 has no coverage row (→ day_eligible FALSE via COALESCE).
# user_b: 3 eligible days (< 10 → user_eligible FALSE).
RECS = (
    make_loop_recs("user_a", _d(1), 12, "autobolus")
    + make_loop_recs("user_b", _d(1), 3, "autobolus")
)

# bolus_entry_count rows only where non-zero (else COALESCE 0).
BOLUS = [
    {"_userId": "user_a", "local_day": _d(2), "bolus_entry_count": 1},   # CE=0/BE=1
    {"_userId": "user_a", "local_day": _d(3), "bolus_entry_count": 3},   # CE=0/BE=3
    {"_userId": "user_a", "local_day": _d(4), "bolus_entry_count": 2},   # CE>0
    {"_userId": "user_a", "local_day": _d(5), "bolus_entry_count": 3},   # HMA
]
CARBS = [
    {"_userId": "user_a", "local_day": _d(4), "carb_entry_count": 2, "carb_grams_total": 60.0},  # CE>0
    {"_userId": "user_a", "local_day": _d(5), "carb_entry_count": 3, "carb_grams_total": 90.0},  # HMA
]
# Coverage: user_a d1-10 eligible, d11 explicit ineligible, d12 omitted (→ COALESCE FALSE).
COVERAGE = (
    [{"_userId": "user_a", "local_day": _d(n), "is_eligible": True} for n in range(1, 11)]
    + [{"_userId": "user_a", "local_day": _d(11), "is_eligible": False}]
    + [{"_userId": "user_b", "local_day": _d(n), "is_eligible": True} for n in range(1, 4)]
)

try:
    setup_test_table(spark, RECS_TABLE, RECS)
    setup_test_table(spark, COVERAGE_TABLE, COVERAGE)
    setup_test_table(spark, BOLUS_TABLE, BOLUS)
    setup_test_table(spark, CARBS_TABLE, CARBS)
    run(spark, loop_recommendations_table=RECS_TABLE, coverage_table=COVERAGE_TABLE,
        bolus_counts_table=BOLUS_TABLE, carbs_table=CARBS_TABLE, output_table=OUTPUT_TABLE)
    res = read_test_output(spark, OUTPUT_TABLE)
    by = {(r["_userId"], pd.Timestamp(r["local_day"]).date()): r for _, r in res.iterrows()}

    def flags(u, n):
        r = by[(u, _d(n))]
        return (bool(r["in_ce0_be0"]), bool(r["in_ce0_be_le1"]), bool(r["in_ce0_be_inf"]),
                bool(r["in_ce_gt0"]), bool(r["in_ce_ge3_be_ge3"]))

    assert flags("user_a", 1) == (True, True, True, False, False), "CE=0/BE=0"
    assert flags("user_a", 2) == (False, True, True, False, False), "CE=0/BE=1"
    assert flags("user_a", 3) == (False, False, True, False, False), "CE=0/BE=3"
    assert flags("user_a", 4) == (False, False, False, True, False), "CE>0"
    assert flags("user_a", 5) == (False, False, False, True, True), "HMA (CE>=3/BE>=3)"

    # day_eligible: explicit-ineligible (d11) + missing-coverage (d12) both FALSE; d1 TRUE.
    assert bool(by[("user_a", _d(1))]["day_eligible"]) is True
    assert bool(by[("user_a", _d(11))]["day_eligible"]) is False
    assert bool(by[("user_a", _d(12))]["day_eligible"]) is False

    # user_eligible: user_a has 10 eligible days (>=10 → TRUE); user_b has 3 (< 10 → FALSE).
    assert bool(by[("user_a", _d(1))]["user_eligible"]) is True
    assert bool(by[("user_b", _d(1))]["user_eligible"]) is False

finally:
    teardown_test_tables(spark, *ALL_TABLES)
