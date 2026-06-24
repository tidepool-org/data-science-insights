"""Integration test: the type-1 diagnosis gate.

load_type1_user_ids() (utils/data_loading.py) must keep only users whose
user_diagnosis_type.diagnosis_type = 'type1', and exclude type2, other, NULL,
and users absent from the lookup. The pipeline's all-type1 fixture would pass
even if the gate were a no-op, so this test pins the exclusion behavior on a
tiny purpose-built diagnosis table.

Run on Databricks.
"""

import os
import sys

import pandas as pd

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

from testing.integration import run_pipeline  # noqa: E402
import utils.data_loading as data_loading  # noqa: E402


spark = run_pipeline.get_spark()

DX_TABLE = "dev.fda_510k_rwd.test_type1_gate_dx"

rows = [
    {"_userId": "t1_keep_a",  "diagnosis_type": "type1"},
    {"_userId": "t1_keep_b",  "diagnosis_type": "type1"},
    {"_userId": "t2_drop",    "diagnosis_type": "type2"},
    {"_userId": "other_drop", "diagnosis_type": "other"},
    {"_userId": "null_drop",  "diagnosis_type": None},
]
spark.createDataFrame(
    pd.DataFrame(rows, columns=["_userId", "diagnosis_type"]),
    schema="`_userId` string, `diagnosis_type` string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DX_TABLE)

try:
    # Redirect the gate's hard-coded prod table to the tiny fixture above, so
    # load_type1_user_ids reads it without touching production.
    redirected = run_pipeline.RedirectingSpark(
        spark, redirects={data_loading.DIAGNOSIS_TABLE: DX_TABLE}
    )
    got = data_loading.load_type1_user_ids(redirected)

    assert got == {"t1_keep_a", "t1_keep_b"}, (
        f"expected only the two type1 users, got {sorted(got)}"
    )
    # Explicit exclusions: type2, other, NULL, and a never-listed user.
    for excluded in ("t2_drop", "other_drop", "null_drop", "ghost_never_listed"):
        assert excluded not in got, f"{excluded} should have been excluded by the gate"

    print("\nPASS: type-1 gate keeps only diagnosis_type = 'type1' "
          "(type2 / other / NULL / absent excluded).")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {DX_TABLE}")
