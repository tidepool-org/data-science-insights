"""Integration test for analysis 8-5 (demographic subgroup analysis).

Uses the same `load_transition_endpoints` cohort as 8-1, plus the
demographic bins from `_DEMOGRAPHICS` in build_synthetic_bddp.py.

Bins exercised by the synthetic cohort:
- Age: Children (6–<12): int_user_15 (8y); Adolescents (12–<18): int_user_03;
       Adults (18–64): int_user_01/02/12/13/14; Older Adults (≥65): int_user_16.
- Gender: M (02, 03, 12, 14, 15); F (01, 13, 16).
- YLD: Early (<5y): 03, 14, 15; Established (5-<15y): 01, 12, 13;
       Long-duration (≥15y): 02, 16.

Run on Databricks.
"""

import os
import shutil
import sys
import tempfile

import pandas as pd

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
    "analysis_8-5_demographic_subgroup_analysis.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_5", _analysis_path)
analysis_8_5 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_5)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_5_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_8_5.run_analysis(redirected, output_dir=output_dir)
        df = result["df"]

        # ── Output artifact assertions ────────────────────────────────────
        expected_outputs = (
            "table_8_5a_data_availability.csv",
            "table_8_5b_within_subgroup_parametric.csv",
            "table_8_5b_within_subgroup_nonparametric.csv",
            "table_8_5c_between_subgroup.csv",
            "sensitivity_gender_missing.csv",
            "figure_8_5a_delta_tir_by_subgroup.png",
            "figure_8_5b_safety_by_subgroup.png",
            "figure_8_5c_demographic_distributions.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        # ── Cohort contains the demographic reps + named TIR archetypes ───
        user_ids = set(df["_userId"].tolist())
        required = {
            "int_user_01", "int_user_02", "int_user_03",
            "int_user_15", "int_user_16",
        }
        assert required.issubset(user_ids), (
            f"8-5 cohort missing {required - user_ids}; got {sorted(user_ids)}"
        )
        print(f"PASS: 8-5 cohort includes 01/02/03/15/16 (cohort size = {len(user_ids)})")

        # ── Every demographic bin in the wide df has ≥1 user ─────────────
        # The wide df should carry binned columns (age_group, gender_group,
        # yld_group) from analysis_8-5's _bin_age / _bin_yld helpers.
        for col in ("age_group", "gender_group", "yld_group"):
            assert col in df.columns, (
                f"expected `{col}` column in 8-5's wide df; got {list(df.columns)}"
            )
        print("PASS: subgroup columns present (age, gender, yld)")

        # Check the age bins our archetypes cover.
        age_bins = set(df["age_group"].dropna())
        for bin_label, who in (
            ("Children (6–<12)", "int_user_15"),
            ("Adolescents (12–<18)", "int_user_03"),
            ("Adults (18–64)", "int_user_01"),
            ("Older Adults (≥65)", "int_user_16"),
        ):
            assert any(bin_label in str(b) for b in age_bins) or bin_label in age_bins, (
                f"age bin `{bin_label}` (via {who}) not in 8-5 df; got {age_bins}"
            )
        print(f"PASS: all 4 age subgroups populated ({sorted(age_bins)})")

        # YLD bins
        yld_bins = set(df["yld_group"].dropna())
        assert len(yld_bins) >= 3, (
            f"expected 3 YLD subgroup bins; got {len(yld_bins)} ({yld_bins})"
        )
        print(f"PASS: YLD subgroup count = {len(yld_bins)} ({sorted(yld_bins)})")

        # Gender bins
        gender_bins = set(df["gender_group"].dropna())
        assert len(gender_bins) >= 2, (
            f"expected ≥2 gender subgroup bins; got {gender_bins}"
        )
        print(f"PASS: gender subgroup count = {len(gender_bins)} ({sorted(gender_bins)})")

        # ── Sample-size sanity in table_8_5a ──────────────────────────────
        t_avail = pd.read_csv(os.path.join(output_dir, "table_8_5a_data_availability.csv"))
        assert not t_avail.empty, "table_8_5a_data_availability.csv is empty"
        print(f"PASS: table_8_5a_data_availability has {len(t_avail)} rows")

        print("\nAll integration assertions for analysis 8-5 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
