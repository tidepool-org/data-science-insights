"""Integration test for analysis 8-4 (preset activation duration).

Builds the synthetic BDDP fixture, runs the full staging pipeline against
it, then runs `analysis_8-4.run_analysis()` over a `RedirectingSpark`
wrapper so the analysis reads the `test_*`-prefixed tables instead of prod.

8-4 includes ALL users in `valid_transition_segments` passing COHORT_WHERE
(no validity filter, no cbg gate). Users with no override activations
appear with `total_duration_hr = 0`.

Predicted output:
- int_user_08: 6 activations per segment × 1 h = 6 h total_duration_hr in seg1, seg2.
- int_user_09: 0 activations in seg1, 1 in seg2 (1 h).
- int_user_01/02/03/05/06/12/13/14/15/16 (transition-cohort archetypes without
  overrides): 0 activations both segments.
- int_user_04 dropped by COHORT_WHERE (Loop 3.5.0).

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
    "analysis_8-4_preset_activation_duration.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_4", _analysis_path)
analysis_8_4 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_4)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_4_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_4.run_analysis(redirected, output_dir=output_dir)
        df = result["df"]

        expected_outputs = (
            "table_8_4a_parametric.csv",
            "table_8_4a_nonparametric.csv",
            "figure_8_4a_paired_duration.png",
            "figure_8_4b_distribution.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        user_ids = set(df["_userId"].tolist())
        assert "int_user_04" not in user_ids, (
            f"int_user_04 should be dropped by Loop version 3.5.0; got {sorted(user_ids)}"
        )
        for required in ("int_user_01", "int_user_08", "int_user_09"):
            assert required in user_ids, (
                f"{required} missing from 8-4 cohort; got {sorted(user_ids)}"
            )
        print(f"PASS: 8-4 cohort includes int_user_01/08/09; excludes int_user_04 "
              f"(size = {len(user_ids)})")

        u8 = df[df["_userId"] == "int_user_08"].iloc[0]
        assert u8["n_activations_seg1"] == 6, (
            f"int_user_08 seg1 expected 6 activations; got {u8['n_activations_seg1']}"
        )
        assert u8["n_activations_seg2"] == 6, (
            f"int_user_08 seg2 expected 6 activations; got {u8['n_activations_seg2']}"
        )
        assert u8["total_duration_hr_seg1"] == 6.0, (
            f"int_user_08 seg1 expected 6.0 h; got {u8['total_duration_hr_seg1']}"
        )
        assert u8["total_duration_hr_seg2"] == 6.0, (
            f"int_user_08 seg2 expected 6.0 h; got {u8['total_duration_hr_seg2']}"
        )
        print("PASS: int_user_08 n_activations = 6, total_duration_hr = 6.0 (both segments)")

        u9 = df[df["_userId"] == "int_user_09"].iloc[0]
        assert u9["n_activations_seg1"] == 0, (
            f"int_user_09 seg1 expected 0 activations; got {u9['n_activations_seg1']}"
        )
        assert u9["n_activations_seg2"] == 1, (
            f"int_user_09 seg2 expected 1 activation; got {u9['n_activations_seg2']}"
        )
        print("PASS: int_user_09 n_activations seg1=0, seg2=1")

        u1 = df[df["_userId"] == "int_user_01"].iloc[0]
        assert u1["n_activations_seg1"] == 0 and u1["n_activations_seg2"] == 0, (
            f"int_user_01 expected 0/0 activations; "
            f"got {u1['n_activations_seg1']}/{u1['n_activations_seg2']}"
        )
        print("PASS: int_user_01 zero-baseline (0 activations both segments)")

        print("\nAll integration assertions for analysis 8-4 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
