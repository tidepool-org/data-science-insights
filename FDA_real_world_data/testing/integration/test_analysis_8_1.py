"""Integration test for analysis 8-1 (TB vs AB performance).

Builds the synthetic BDDP fixture, runs the full staging pipeline against
it, then runs `analysis_8-1.run_analysis()` over a `RedirectingSpark`
wrapper so the analysis reads the `test_*`-prefixed tables instead of prod.

Predicted output (per archetypes.md):
- 3 users survive cohort filters: int_user_01, int_user_02, int_user_03
- int_user_04 dropped: Loop version 3.5.0 >= 3.4.0 cohort cutoff
- int_user_05 dropped: cbg_count_seg1 < 2,822 (only 7 days of seg1 cbg)
- int_user_06 dropped: pumpSettings bg_target_high = 200 > 180 guardrail
- int_user_01 paired diff: tir_seg1 == 50.0, tir_seg2 == 75.0
- int_user_02 paired diff: tir_seg1 == 75.0, tir_seg2 == 60.0 (decliner)
- int_user_03: hypo_events_seg1 == 1, hypo_events_seg2 == 0

Run on Databricks.
"""

import os
import shutil
import sys
import tempfile

import pandas as pd
from pyspark.sql import SparkSession  # type: ignore

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))  # for `analysis.utils`
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))  # for `from utils...`

from testing.integration import run_pipeline  # noqa: E402

# Analysis filenames have a hyphen in them; can't be imported by name.
# Use importlib for the dashed module.
import importlib.util  # noqa: E402

_analysis_path = os.path.join(
    _here, "..", "..", "analysis",
    "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_1", _analysis_path)
analysis_8_1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_1)


spark = SparkSession.builder.getOrCreate()


def _row_for(df, user_id):
    rows = df[df["_userId"] == user_id]
    assert len(rows) == 1, f"expected 1 row for {user_id}, got {len(rows)}"
    return rows.iloc[0]


# ── 1. Build fixture and run pipeline ──────────────────────────────────────
run_pipeline.run(spark)

# ── 2. Run analysis 8-1 against test tables via the redirecting wrapper ────
redirected = run_pipeline.RedirectingSpark(spark)
output_dir = tempfile.mkdtemp(prefix="int_test_8_1_")
try:
    result = analysis_8_1.run_analysis(redirected, output_dir=output_dir)
    df = result["df"]

    # ── 3. Cohort assertions ───────────────────────────────────────────────
    user_ids = set(df["_userId"].tolist())
    assert user_ids == {"int_user_01", "int_user_02", "int_user_03"}, (
        f"expected exactly int_user_01/02/03 in paired-diff frame; got {sorted(user_ids)}"
    )
    print("PASS: 3 users survive cohort filter (int_user_01, _02, _03)")

    for dropped in ("int_user_04", "int_user_05", "int_user_06"):
        assert dropped not in user_ids, f"{dropped} should be dropped; it's still in df"
    print("PASS: int_user_04 (version), _05 (cbg coverage), _06 (guardrail) dropped")

    # ── 4. TIR assertions ──────────────────────────────────────────────────
    u1 = _row_for(df, "int_user_01")
    assert u1["tir_seg1"] == 50.0, f"int_user_01 tir_seg1 expected 50.0, got {u1['tir_seg1']}"
    assert u1["tir_seg2"] == 75.0, f"int_user_01 tir_seg2 expected 75.0, got {u1['tir_seg2']}"
    print("PASS: int_user_01 tir_seg1 = 50.0, tir_seg2 = 75.0")

    u2 = _row_for(df, "int_user_02")
    assert u2["tir_seg1"] == 75.0, f"int_user_02 tir_seg1 expected 75.0, got {u2['tir_seg1']}"
    assert u2["tir_seg2"] == 60.0, f"int_user_02 tir_seg2 expected 60.0, got {u2['tir_seg2']}"
    print("PASS: int_user_02 tir_seg1 = 75.0, tir_seg2 = 60.0 (decliner)")

    u3 = _row_for(df, "int_user_03")
    assert u3["hypo_events_seg1"] == 1, (
        f"int_user_03 hypo_events_seg1 expected 1, got {u3['hypo_events_seg1']}"
    )
    assert u3["hypo_events_seg2"] == 0, (
        f"int_user_03 hypo_events_seg2 expected 0, got {u3['hypo_events_seg2']}"
    )
    print("PASS: int_user_03 hypo_events_seg1 = 1, hypo_events_seg2 = 0")

    # ── 5. Output artifact assertions ──────────────────────────────────────
    expected_outputs = (
        "table_8_1a_parametric.csv",
        "table_8_1a_nonparametric.csv",
        "figure_8_1c_stacked_bars.png",
    )
    for f in expected_outputs:
        path = os.path.join(output_dir, f)
        assert os.path.exists(path), f"missing expected output: {f}"
    print("PASS: expected output artifacts written")

    # ── 6. Parametric table sanity ─────────────────────────────────────────
    table_p = pd.read_csv(os.path.join(output_dir, "table_8_1a_parametric.csv"))
    tir_row = table_p[table_p["Endpoint"] == "Time 70–180 mg/dL (%)"].iloc[0]
    assert tir_row["N"] == 3, f"expected N=3 in TIR row, got {tir_row['N']}"
    print("PASS: table_8_1a_parametric Time 70-180 row has N=3")

    print("\nAll integration assertions for analysis 8-1 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
