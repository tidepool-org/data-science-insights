"""Integration test for analysis 8-2 (glycemic outcomes during preset activation).

Builds the synthetic BDDP fixture, runs the full staging pipeline against
it, then runs `analysis_8-2.run_analysis()` over a `RedirectingSpark`
wrapper so the analysis reads the `test_*`-prefixed tables instead of prod.

Predicted output (per archetypes.md):
- int_user_08 contributes 3 paired groups (Workout, Sleep, Pre-meal) to
  `datasets_8_2b_primary["_all"]` and `datasets_8_2c_primary["_all"]`
  (≥2 activations per preset in seg1 AND seg{2,3} → is_valid_name_only_* TRUE).
- int_user_09 has one Workout activation in seg2 only — fails both
  `is_valid_name_only_seg2` and `is_valid_name_only_seg3`, so excluded
  from 8.2b and 8.2c primary tables.
- All Workout/Sleep/Pre-meal windows have CBG = 100 mg/dL throughout, so
  every activation's `tir = 100.0` and `hypo_events = 0`.

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
sys.path.insert(0, os.path.join(_here, "..", ".."))  # for `analysis.utils`
sys.path.insert(0, os.path.join(_here, "..", "..", "analysis"))  # for `from utils...`

from testing.integration import run_pipeline  # noqa: E402

import importlib.util  # noqa: E402

_analysis_path = os.path.join(
    _here, "..", "..", "analysis",
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_2", _analysis_path)
analysis_8_2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_2)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_2_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_2.run_analysis(redirected, output_dir=output_dir)

        # ── Output artifact assertions ────────────────────────────────────
        expected_outputs = (
            "table_8_2a.csv",
            "table_8_2b_parametric.csv",
            "table_8_2b_nonparametric.csv",
            "table_8_2c_parametric.csv",
            "table_8_2c_nonparametric.csv",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        # ── 8.2b primary cohort: int_user_08 in, int_user_09 out ──────────
        ds_b = result["datasets_8_2b_primary"]
        assert "_all" in ds_b, f"datasets_8_2b_primary missing `_all` key; got {list(ds_b)}"
        df_b_all = ds_b["_all"]
        b_users = set(df_b_all["_userId"].tolist())
        assert "int_user_08" in b_users, (
            f"int_user_08 should appear in 8.2b primary `_all`; got users {sorted(b_users)}"
        )
        assert "int_user_09" not in b_users, (
            f"int_user_09 should be excluded from 8.2b primary "
            f"(is_valid_name_only_seg2=FALSE); got users {sorted(b_users)}"
        )
        print("PASS: 8.2b primary contains int_user_08, excludes int_user_09")

        # ── 8.2c primary cohort ────────────────────────────────────────────
        ds_c = result["datasets_8_2c_primary"]
        assert "_all" in ds_c, f"datasets_8_2c_primary missing `_all` key; got {list(ds_c)}"
        df_c_all = ds_c["_all"]
        c_users = set(df_c_all["_userId"].tolist())
        assert "int_user_08" in c_users, (
            f"int_user_08 should appear in 8.2c primary `_all`; got users {sorted(c_users)}"
        )
        assert "int_user_09" not in c_users, (
            f"int_user_09 should be excluded from 8.2c primary "
            f"(is_valid_name_only_seg3=FALSE); got users {sorted(c_users)}"
        )
        print("PASS: 8.2c primary contains int_user_08, excludes int_user_09")

        # ── Hypo-rate column naming (P2-commit regression catch) ──────────
        for col in ("hypo_rate_seg1", "hypo_rate_seg2"):
            assert col in df_b_all.columns, (
                f"8.2b primary frame missing `{col}` column; got {list(df_b_all.columns)}"
            )
        print("PASS: hypo_rate_seg1 / hypo_rate_seg2 columns present")

        # ── Three paired groups for int_user_08 (one per preset name) ─────
        u8_b = df_b_all[df_b_all["_userId"] == "int_user_08"]
        assert len(u8_b) == 3, (
            f"expected 3 paired groups for int_user_08 in 8.2b; got {len(u8_b)}"
        )
        presets_b = set(u8_b["overridePreset"])
        assert presets_b == {"Workout", "Sleep", "Pre-meal"}, (
            f"int_user_08's 8.2b presets expected {{Workout, Sleep, Pre-meal}}; got {presets_b}"
        )
        print(f"PASS: int_user_08 contributes 3 paired groups (presets: {sorted(presets_b)})")

        # ── Exact TIR: constant-100 CBG → TIR = 100% in every window ──────
        for _, row in u8_b.iterrows():
            assert row["tir_seg1"] == 100.0, (
                f"int_user_08 / {row['overridePreset']}: "
                f"tir_seg1 expected 100.0, got {row['tir_seg1']}"
            )
            assert row["tir_seg2"] == 100.0, (
                f"int_user_08 / {row['overridePreset']}: "
                f"tir_seg2 expected 100.0, got {row['tir_seg2']}"
            )
        print("PASS: int_user_08 tir_seg1 = tir_seg2 = 100.0 for all three presets")

        # ── Activations frame: 18 rows for int_user_08, 1 for int_user_09 ──
        activations = result["activations"]
        n_08 = (activations["_userId"] == "int_user_08").sum()
        n_09 = (activations["_userId"] == "int_user_09").sum()
        assert n_08 == 18, f"expected 18 activations for int_user_08 (6 / segment × 3); got {n_08}"
        assert n_09 == 1, f"expected 1 activation for int_user_09 (seg2 only); got {n_09}"
        print(f"PASS: activations frame has int_user_08={n_08}, int_user_09={n_09}")

        print("\nAll integration assertions for analysis 8-2 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
