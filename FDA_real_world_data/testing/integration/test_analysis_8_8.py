"""Integration test for analysis 8-8 (carbohydrate consumption consistency).

Uses 8-1's transition cohort + `valid_transition_carbs`. Synthetic users
with food rows:
- int_user_01: 50 g/day in both seg1 and seg2 → 0% change, Consistent (≤25%).
- int_user_12: 150 g/day stable both → 0% change, Consistent.
- int_user_13: 120 → 180 g/day (+50%) → Inconsistent / Increased (>25%).
- int_user_14: 180 → 120 g/day (-33%) → Inconsistent / Decreased (>25%).
- int_user_02/03/15/16: no food rows → INNER JOIN drops them.

Run on Databricks.
"""

import os
import shutil
import sys
import tempfile


try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"
sys.path.insert(0, os.path.join(_here, "..", ".."))
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))

from testing.integration import run_pipeline  # noqa: E402

import importlib.util  # noqa: E402

_analysis_path = os.path.join(
    _here, "..", "..", "analysis",
    "analysis_8-8_carbohydrate_consumption_consistency.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_8", _analysis_path)
analysis_8_8 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_8)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_8_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_8.run_analysis(redirected, output_dir=output_dir)
        df = result["df"]

        # ── Output artifact assertions ────────────────────────────────────
        expected_outputs = (
            "table_8_8a_carbohydrate_consumption.csv",
            "table_8_8b_consistent_parametric.csv",
            "table_8_8b_consistent_nonparametric.csv",
            "table_8_8c_inconsistent_parametric.csv",
            "table_8_8c_inconsistent_nonparametric.csv",
            "table_8_8d_treatment_effect_comparison.csv",
            "figure_8_8a_cho_change_distribution.png",
            "figure_8_8b_tir_forest_plot.png",
            "figure_8_8c_paired_tir_by_consistency.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        # ── Cohort contains the carb archetypes; users without food are dropped ──
        user_ids = set(df["_userId"].tolist())
        for required in ("int_user_01", "int_user_12", "int_user_13", "int_user_14"):
            assert required in user_ids, (
                f"{required} missing from 8-8 cohort; got {sorted(user_ids)}"
            )
        print(f"PASS: 8-8 cohort includes int_user_01/12/13/14 (size = {len(user_ids)})")

        # ── cho_consistent classification per archetype ───────────────────
        rows_by_uid = {row["_userId"]: row for _, row in df.iterrows()}
        u12 = rows_by_uid["int_user_12"]
        assert bool(u12["cho_consistent"]) is True, (
            f"int_user_12 (~150 g/day stable) expected cho_consistent=True; "
            f"got change_pct={u12.get('cho_pct_change')}"
        )
        u13 = rows_by_uid["int_user_13"]
        assert bool(u13["cho_consistent"]) is False, (
            f"int_user_13 (+50% change) expected cho_consistent=False; "
            f"got change_pct={u13.get('cho_pct_change')}"
        )
        assert u13["cho_pct_change"] > 25.0, (
            f"int_user_13 expected cho_pct_change > 25; got {u13['cho_pct_change']}"
        )
        u14 = rows_by_uid["int_user_14"]
        assert bool(u14["cho_consistent"]) is False, (
            f"int_user_14 (-33% change) expected cho_consistent=False; "
            f"got change_pct={u14.get('cho_pct_change')}"
        )
        assert u14["cho_pct_change"] < -25.0, (
            f"int_user_14 expected cho_pct_change < -25; got {u14['cho_pct_change']}"
        )
        print("PASS: int_user_12 consistent; _13 inconsistent (+>25%); _14 inconsistent (-<25%)")

        # ── Users without food rows are dropped by the INNER JOIN ─────────
        for excluded in ("int_user_02", "int_user_03", "int_user_15", "int_user_16"):
            assert excluded not in user_ids, (
                f"{excluded} has no food rows; should be dropped from 8-8 cohort"
            )
        print("PASS: int_user_02/03/15/16 (no food) excluded from 8-8")

        # ── int_user_24 has 200 g food entries — should be filtered by the
        # per-entry carb_grams <= 150 outlier gate, leaving them with no
        # surviving carb data, so the INNER JOIN drops them. ──────────────
        assert "int_user_24" not in user_ids, (
            "int_user_24 has 200 g/entry food rows that exceed 8-8's "
            "carb_grams <= 150 outlier filter; should be excluded"
        )
        print("PASS: int_user_24 (200 g/entry outlier) filtered out")

        print("\nAll integration assertions for analysis 8-8 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
