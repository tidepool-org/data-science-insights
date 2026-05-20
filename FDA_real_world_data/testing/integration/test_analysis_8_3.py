"""Integration test for analysis 8-3 (preset parameter changes).

Builds the synthetic BDDP fixture, runs the full staging pipeline against
it, then runs `analysis_8-3.run_analysis()` over a `RedirectingSpark`
wrapper so the analysis reads the `test_*`-prefixed tables instead of prod.

Predicted output:
- int_user_08 contributes 3 paired rows (Workout, Sleep, Pre-meal) to `df`
  via `is_valid_name_only_seg2 == TRUE`. All three presets use identical
  scale factors in seg1 and seg2 (br_sf=0.7, cr_isf_sf=0.7), so each
  paired difference is 0.
- int_user_09 is excluded (one Workout in seg2 only → is_valid_name_only_seg2=FALSE).
- Output CSVs and figures are written unconditionally.

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
    "analysis_8-3_preset_parameter_changes.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_3", _analysis_path)
analysis_8_3 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_3)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_3_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_3.run_analysis(redirected, output_dir=output_dir)
        df = result["df"]

        expected_outputs = (
            "table_8_3a_parametric.csv",
            "table_8_3a_nonparametric.csv",
            "figure_8_3a_paired_parameters.png",
            "figure_8_3b_diff_histograms.png",
            "figure_8_3c_parameter_correlations.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        u8 = df[df["_userId"] == "int_user_08"]
        assert len(u8) == 3, (
            f"expected 3 paired rows for int_user_08 (Workout, Sleep, Pre-meal); got {len(u8)}"
        )
        presets = set(u8["overridePreset"])
        assert presets == {"Workout", "Sleep", "Pre-meal"}, (
            f"int_user_08 presets expected {{Workout, Sleep, Pre-meal}}; got {presets}"
        )
        print(f"PASS: int_user_08 contributes 3 paired rows ({sorted(presets)})")

        assert "int_user_09" not in df["_userId"].values, (
            "int_user_09 should be excluded by is_valid_name_only_seg2=FALSE"
        )
        print("PASS: int_user_09 excluded by validity gate")

        print("\nAll integration assertions for analysis 8-3 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
