"""
Spark test for `data_staging/export_nma_user_day_bolus_classification.py` — the per-user-day bolus
counts projected from the FDA dose table (decision D23, 2026-11-05).

The script classifies nothing: it counts the dose table's `is_automated` verdict and
`has_normal_subtype` per user-day on the day universe. Covered:
  - an automated bolus counts as automatic, not manual
  - a manual bolus with the normal subtype counts in manual_normal_bolus_count
  - a manual extended bolus counts as manual but not manual-normal
  - a basal dose is ignored
  - a day with no bolus → counts coalesce to 0 (anchored on the day table)
  - a bolus on a day outside the universe is not reported

Run on Databricks. Silent on success.
"""

import os
import sys
from datetime import date, datetime

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

from export_nma_user_day_bolus_classification import run  # type: ignore # noqa: E402
from nma_test_helpers import make_loop_recs, read_test_output, setup_test_table  # type: ignore # noqa: E402
from staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

DOSE_TABLE = f"{TEST_SCHEMA}._test_input_bolus_classification"
ANCHOR_TABLE = f"{TEST_SCHEMA}._test_anchor_bolus_classification"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_bolus_classification"
ALL_TABLES = [DOSE_TABLE, ANCHOR_TABLE, OUTPUT_TABLE]

# The columns of loop_dose_classification the projection reads, plus the dose time
# for realism; the real table carries more (routes, records, the per-rule tags).
DOSE_SCHEMA = ("_userId STRING, type STRING, day DATE, dose_time TIMESTAMP, units DOUBLE, "
               "has_normal_subtype INT, is_automated INT")


def _d(n):
    return date(2025, 3, n)


def dose(day, hour, type_="bolus", units=1.0, normal=1, automated=0):
    return ("user_x", type_, day, datetime(day.year, day.month, day.day, hour), units, normal, automated)


DOSES = [
    # d1 — two automated boluses, one manual normal bolus
    dose(_d(1), 8, automated=1, normal=1, units=0.3),
    dose(_d(1), 9, automated=1, normal=1, units=0.3),
    dose(_d(1), 12, automated=0, normal=1, units=2.0),
    # d2 — a manual extended bolus (not normal) and a manual normal one
    dose(_d(2), 8, automated=0, normal=0, units=3.0),
    dose(_d(2), 9, automated=0, normal=1, units=1.0),
    # d3 — a basal only: ignored
    dose(_d(3), 8, type_="basal", units=0.5, normal=0, automated=1),
    # d4 — no dose at all (an anchored day)
    # d5 — one automated bolus
    dose(_d(5), 8, automated=1, normal=1, units=0.4),
    # d9 — outside the six-day universe: not reported
    dose(_d(9), 8, automated=0, normal=1, units=1.0),
]
ANCHOR = make_loop_recs("user_x", _d(1), 6, "autobolus")

try:
    spark.createDataFrame(DOSES, DOSE_SCHEMA).write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(DOSE_TABLE)
    setup_test_table(spark, ANCHOR_TABLE, ANCHOR)
    run(spark, dose_table=DOSE_TABLE, anchor_table=ANCHOR_TABLE, output_table=OUTPUT_TABLE)
    res = read_test_output(spark, OUTPUT_TABLE)
    assert len(res) == 6, f"one row per day of the universe, got {len(res)}"
    by = {pd.Timestamp(r["local_day"]).date(): r for _, r in res.iterrows()}

    def counts(day):
        r = by[day]
        return (int(r["total_bolus_count"]), int(r["automatic_bolus_count"]),
                int(r["manual_bolus_count"]), int(r["manual_normal_bolus_count"]))

    assert counts(_d(1)) == (3, 2, 1, 1), f"d1: {counts(_d(1))}"
    assert counts(_d(2)) == (2, 0, 2, 1), f"d2 extended manual bolus is manual, not manual-normal: {counts(_d(2))}"
    assert counts(_d(3)) == (0, 0, 0, 0), f"d3 a basal is ignored: {counts(_d(3))}"
    assert counts(_d(4)) == (0, 0, 0, 0), f"d4 no dose: {counts(_d(4))}"
    assert counts(_d(5)) == (1, 1, 0, 0), f"d5: {counts(_d(5))}"
    assert _d(9) not in by, "a day outside the universe is not reported"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
