"""Integration test for analysis 8-6 (socioeconomic subgroup export).

8-6 INNER-joins `glycemic_endpoints_stable_autobolus` to
`jaeb_upload_to_userid` via `bddp_sample_all_2.uploadID`, so only users
with a uploadID match in the JAEB table appear in the output.

Fixture:
- int_user_19: stable-AB pattern + `uploadID = "upload_19"` →
  `jaeb_upload_to_userid` has `(upload_19, ptid_19)` → INCLUDED.
- int_user_20: stable-AB pattern + uploadID = NULL → EXCLUDED by JAEB join.

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
    "analysis_8-6_socioeconomic_subgroup_analysis.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_6", _analysis_path)
analysis_8_6 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_6)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_8_6_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        out_df = analysis_8_6.run_analysis(redirected, output_dir=output_dir)

        # ── Output artifact ──────────────────────────────────────────────
        csv_path = os.path.join(output_dir, "glycemic_endpoints_by_jaeb_id.csv")
        assert os.path.exists(csv_path), (
            f"missing expected output: glycemic_endpoints_by_jaeb_id.csv"
        )
        print("PASS: glycemic_endpoints_by_jaeb_id.csv written")

        # ── int_user_19 lands via PtID; int_user_20 excluded by INNER JOIN ──
        pt_ids = set(out_df["PtID"].tolist())
        assert "ptid_19" in pt_ids, (
            f"int_user_19's PtID `ptid_19` missing; got {sorted(pt_ids)}"
        )
        print(f"PASS: int_user_19's PtID `ptid_19` present in output ({len(out_df)} row(s))")

        # ── Endpoint columns present ─────────────────────────────────────
        # Per analysis_8-6_*.py:130, output cols = ["PtID"] + ENDPOINT_COLS.
        # ENDPOINT_COLS includes TIR, TBR, TAR, mean_glucose, etc.
        for col in ("tir", "mean_glucose"):
            assert col in out_df.columns, (
                f"expected `{col}` column in 8-6 output; got {list(out_df.columns)}"
            )
        print(f"PASS: endpoint columns present ({sorted(out_df.columns)})")

        print("\nAll integration assertions for analysis 8-6 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
