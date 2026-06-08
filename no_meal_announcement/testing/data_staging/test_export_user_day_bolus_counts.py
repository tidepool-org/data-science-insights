"""
Spark test for `data_staging/export_user_day_bolus_counts.py`.

BE is now a THIN PROJECTION of the central bolus classifier: this module just surfaces
`manual_normal_bolus_count` from `nma_user_day_bolus_classification` as `bolus_entry_count`. The
manual/automatic split itself (HK flag + dd fallback + dedup) lives in
`export_user_day_bolus_classification` and is tested in `test_export_user_day_bolus_classification.py`.
So this test only verifies the projection (column rename, row-for-row, one row per valid Loop day).

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

from export_user_day_bolus_counts import run  # type: ignore # noqa: E402
from nma_test_helpers import read_test_output, setup_test_table  # type: ignore # noqa: E402
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

INPUT_TABLE = f"{TEST_SCHEMA}._test_input_bolus_counts"          # bolus-classification-shaped
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_bolus_counts"
ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]


def _d(n):
    return date(2025, 1, n)


# nma_user_day_bolus_classification-shaped rows; the module reads only _userId/local_day/
# manual_normal_bolus_count, but include the other classifier columns so the fixture matches the
# real table shape.
ROWS = [
    {"_userId": "u1", "local_day": _d(1), "total_bolus_count": 5, "manual_bolus_count": 0,
     "manual_normal_bolus_count": 0, "automatic_bolus_count": 5, "auto_hk_count": 5, "auto_dd_count": 0},
    {"_userId": "u1", "local_day": _d(2), "total_bolus_count": 6, "manual_bolus_count": 1,
     "manual_normal_bolus_count": 1, "automatic_bolus_count": 5, "auto_hk_count": 5, "auto_dd_count": 0},
    {"_userId": "u1", "local_day": _d(3), "total_bolus_count": 9, "manual_bolus_count": 4,
     "manual_normal_bolus_count": 4, "automatic_bolus_count": 5, "auto_hk_count": 5, "auto_dd_count": 0},
    {"_userId": "u2", "local_day": _d(1), "total_bolus_count": 2, "manual_bolus_count": 2,
     "manual_normal_bolus_count": 2, "automatic_bolus_count": 0, "auto_hk_count": 0, "auto_dd_count": 0},
]

try:
    setup_test_table(spark, INPUT_TABLE, ROWS)
    run(spark, bolus_classification_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    res = read_test_output(spark, OUTPUT_TABLE)

    be = {(r["_userId"], pd.Timestamp(r["local_day"]).date()): int(r["bolus_entry_count"])
          for _, r in res.iterrows()}

    # bolus_entry_count is manual_normal_bolus_count projected row-for-row.
    assert be[("u1", _d(1))] == 0
    assert be[("u1", _d(2))] == 1
    assert be[("u1", _d(3))] == 4
    assert be[("u2", _d(1))] == 2
    assert len(res) == 4, f"expected one row per input day (4), got {len(res)}"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
