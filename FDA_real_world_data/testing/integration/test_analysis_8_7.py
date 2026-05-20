"""Integration test for analysis 8-7 (autobolus adoption durability).

`autobolus_durability` is filtered to users with adoption + min_followup
(≥56 days post-adoption) + final-coverage gates. Synthetic users:

- int_user_21: 60 days of 100% AB → adopts day 2, follow-up = 58 days, final
  28-day AB% = 100% → SUSTAINED.
- int_user_22: 30 days 100% AB then 30 days TB-only → adopts day 2, follow-up
  = 58 days, final 28-day AB% = 0% → DISCONTINUED (registers as KM event).
- int_user_23: 35 days 100% AB → adopts day 2 but follow-up = 33 days < 56
  → DROPPED by min_followup gate.

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
    "analysis_8-7_autobolus_adoption_durability.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_7", _analysis_path)
analysis_8_7 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_7)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_7_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_7.run_analysis(redirected, output_dir=output_dir)

        # ── Output artifacts ─────────────────────────────────────────────
        expected_outputs = (
            "table_8_7a_autobolus_adoption_durability_overall.csv",
            "figure_8_7a_autobolus_adoption_durability.png",
            "figure_8_7b_autobolus_retention_curve.png",
            "figure_8_7c_per_user_trajectories.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        # ── Durability cohort: 21, 22 present; 23 dropped ───────────────
        durability = result["durability"]
        d_users = set(durability["_userId"].tolist())
        assert "int_user_21" in d_users, (
            f"int_user_21 (adopt+sustain) missing from durability table; got {sorted(d_users)}"
        )
        assert "int_user_22" in d_users, (
            f"int_user_22 (adopt+discontinue) missing from durability table; got {sorted(d_users)}"
        )
        assert "int_user_23" not in d_users, (
            f"int_user_23 (insufficient followup) should be dropped; got {sorted(d_users)}"
        )
        print("PASS: durability cohort = {21, 22}; 23 dropped by min_followup gate")

        # ── 21 sustained; 22 discontinued ────────────────────────────────
        rows_by_uid = {row["_userId"]: row for _, row in durability.iterrows()}
        u21 = rows_by_uid["int_user_21"]
        u22 = rows_by_uid["int_user_22"]
        assert bool(u21["is_discontinued"]) is False, (
            f"int_user_21 expected is_discontinued=False (sustained); "
            f"got {u21['is_discontinued']} (final_autobolus_pct={u21.get('final_autobolus_pct')})"
        )
        assert bool(u22["is_discontinued"]) is True, (
            f"int_user_22 expected is_discontinued=True (discontinued); "
            f"got {u22['is_discontinued']} (final_autobolus_pct={u22.get('final_autobolus_pct')})"
        )
        print("PASS: int_user_21 sustained; int_user_22 discontinued")

        # ── Table 8.7a sanity ────────────────────────────────────────────
        table_a = result["table_8_7a"]
        # Three rows expected: Sustained, Discontinued, Total
        assert len(table_a) >= 3, f"table_8_7a expected ≥3 rows; got {len(table_a)}"
        total_row = table_a[table_a["Outcome"] == "Total eligible users"]
        assert not total_row.empty, "table_8_7a missing `Total eligible users` row"
        n_total = int(total_row["N"].iloc[0])
        assert n_total == 2, (
            f"table_8_7a Total N expected 2 (int_user_21, _22); got {n_total}"
        )
        print(f"PASS: table_8_7a total eligible = {n_total}")

        print("\nAll integration assertions for analysis 8-7 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
