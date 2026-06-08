"""
Spark test for `data_staging/export_user_day_bolus_classification.py` — the manual-vs-automatic
bolus classifier that is the SINGLE source of truth for BE (manual_normal_bolus_count) and the §7.3
delivery strategy (automatic_bolus_count). No test existed before.

Covers the HK-first / dd-fallback rule + dedup signal aggregation:
  - HK AutomaticallyIssued flag = 1 → automatic_hk
  - HK flag = 0 (explicit manual) → manual (trusted over dd)
  - HK-silent + loop DD in prior 5s + no normalBolus DD ±15s → automatic_dd
  - HK-silent, no dd → manual
  - one logical bolus written twice (dual-sync, same minute + units) → counted once, auto signal
    MAX-aggregated across the duplicate group
  - day with no bolus → counts coalesce to 0 (anchored on loop_recommendations).

Runs on Databricks (or local pyspark); SKIPS cleanly elsewhere. Silent on success.
"""

import json
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

from export_user_day_bolus_classification import run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

INPUT_TABLE = f"{TEST_SCHEMA}._test_input_bolus_classification"
ANCHOR_TABLE = f"{TEST_SCHEMA}._test_anchor_bolus_classification"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_bolus_classification"
ALL_TABLES = [INPUT_TABLE, ANCHOR_TABLE, OUTPUT_TABLE]

HK_AUTO = json.dumps({"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 1})
HK_MANUAL = json.dumps({"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 0})


def _row(time_string, type_, subType=None, normal=None, payload=None, reason=None):
    """Fixed-schema BDDP row covering every column the classifier reads."""
    return {
        "_userId": "user_x", "time_string": time_string, "created_timestamp": time_string,
        "type": type_, "subType": subType, "normal": normal, "payload": payload, "reason": reason,
    }


def _d(n):
    return date(2025, 3, n)


ROWS = [
    # d1 — HK auto (flag=1).
    _row("2025-03-01 08:00:00", "bolus", subType="normal", normal="1.0", payload=HK_AUTO),
    # d2 — HK explicit manual (flag=0).
    _row("2025-03-02 08:00:00", "bolus", subType="normal", normal="1.0", payload=HK_MANUAL),
    # d3 — HK-silent + dd-auto (loop DD 3s prior, no normalBolus DD).
    _row("2025-03-03 10:00:00", "dosingDecision", reason="loop"),
    _row("2025-03-03 10:00:03", "bolus", subType="normal", normal="1.0"),
    # d4 — HK-silent, no dd → manual.
    _row("2025-03-04 10:00:00", "bolus", subType="normal", normal="1.0"),
    # d5 — dedup: one logical 2.0 bolus written twice (exact + dual-sync +15s), 2nd carries HK flag.
    _row("2025-03-05 06:00:00", "bolus", subType="normal", normal="2.0"),
    _row("2025-03-05 06:00:15", "bolus", subType="normal", normal="2.0", payload=HK_AUTO),
    # d6 — no bolus (anchored day → counts coalesce to 0).
]
ANCHOR = make_loop_recs("user_x", _d(1), 6, "autobolus")

try:
    setup_test_table(spark, INPUT_TABLE, ROWS)
    setup_test_table(spark, ANCHOR_TABLE, ANCHOR)
    run(spark, input_table=INPUT_TABLE, anchor_table=ANCHOR_TABLE, output_table=OUTPUT_TABLE)
    res = read_test_output(spark, OUTPUT_TABLE)
    by = {pd.Timestamp(r["local_day"]).date(): r for _, r in res.iterrows()}

    r = by[_d(1)]
    assert (r["automatic_bolus_count"], r["auto_hk_count"], r["manual_bolus_count"], r["total_bolus_count"]) \
        == (1, 1, 0, 1), f"d1 HK-auto: {dict(r)}"

    r = by[_d(2)]
    assert (r["manual_bolus_count"], r["manual_normal_bolus_count"], r["automatic_bolus_count"]) \
        == (1, 1, 0), f"d2 HK-manual: {dict(r)}"

    r = by[_d(3)]
    assert (r["automatic_bolus_count"], r["auto_dd_count"]) == (1, 1), f"d3 dd-auto: {dict(r)}"

    r = by[_d(4)]
    assert (r["manual_normal_bolus_count"], r["automatic_bolus_count"]) == (1, 0), f"d4 silent-manual: {dict(r)}"

    r = by[_d(5)]
    assert (r["total_bolus_count"], r["automatic_bolus_count"], r["auto_hk_count"]) == (1, 1, 1), \
        f"d5 dedup → one automatic_hk bolus: {dict(r)}"

    assert by[_d(6)]["total_bolus_count"] == 0, "d6 no-bolus day → 0"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
