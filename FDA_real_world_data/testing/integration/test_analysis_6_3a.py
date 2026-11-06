"""Integration test for analysis 6-3a (Table 6.3a and the cohort flows).

Over the synthetic BDDP fixture: the upstream SQL stages are pinned to the
fixture tables (int_user_25 anchors a candidate window but fails the window
rule); the window stages are counted from transition_cohort and must match
counts recomputed here from that table's columns; the final stage must equal
an independently loaded transition cohort (the one the §8 analyses see);
the by-group table and the other populations' flows are written with the
expected shapes; and the issued columns stay blank when no issued cohort
table exists.

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
    _here, "..", "..", "analysis", "analysis_6-3a_cohort_flow.py",
)
_spec = importlib.util.spec_from_file_location("analysis_6_3a", _analysis_path)
analysis_6_3a = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_6_3a)


spark = run_pipeline.get_spark()


REPORT_STAGES = [
    "BDDP sample", "Loop automated dosing observed", "Candidate 28-day window",
    "Valid TB→AB transition segment", "Glycemic endpoints computed", "Analysis cohort gate",
    "Type 1 diabetes", "CGM coverage filter", "Guardrail exclusion", "Paired TB/AB halves",
    "Final transition cohort",
]

output_dir = tempfile.mkdtemp(prefix="int_test_6_3a_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_6_3a.run_analysis(redirected, output_dir=output_dir,
                                            issued_table=run_pipeline.TABLES["transition_cohort"] + "_no_such_issued")
        table = result["table_6_3a"]
        df = result["df"]

        # ── Output artifacts ──────────────────────────────────────────────
        expected_files = {
            "table_6_3a_cohort_flow.csv", "table_6_3a_cohort_flow_by_diagnosis_group.csv",
            "table_flow_transition_long.csv", "table_flow_carbohydrates.csv",
            "table_flow_preset_activations_seg2.csv", "table_flow_preset_activations_seg3.csv",
            "table_flow_stable_autobolus.csv", "table_flow_stable_autobolus_with_guardrail.csv",
            "table_flow_durability.csv",
        }
        written = set(os.listdir(output_dir))
        assert expected_files <= written, f"missing {expected_files - written}"
        on_disk = pd.read_csv(os.path.join(output_dir, "table_6_3a_cohort_flow.csv"))
        assert list(on_disk.columns) == ["stage", "description", "n_users", "n_segments",
                                         "n_users_issued", "difference_users"], list(on_disk.columns)
        assert on_disk["n_users_issued"].isna().all(), "no issued cohort table: the issued column stays blank"
        print("PASS: the flow files are written with the expected columns")

        # ── Funnel shape ──────────────────────────────────────────────────
        assert table["stage"].tolist() == REPORT_STAGES, table["stage"].tolist()
        users = table["n_users"].tolist()
        assert all(b <= a for a, b in zip(users, users[1:])), f"n_users not non-increasing: {users}"
        print(f"PASS: {len(REPORT_STAGES)}-stage funnel, n_users non-increasing ({users})")

        # ── Upstream SQL stages pinned to the fixture ─────────────────────
        by_stage = table.set_index("stage")["n_users"]
        n_bddp = spark.table(run_pipeline.TABLES["bddp"]).select("_userId").distinct().count()
        n_loop = spark.table(run_pipeline.TABLES["loop_recommendations"]).select("_userId").distinct().count()
        assert by_stage["BDDP sample"] == n_bddp
        assert by_stage["Loop automated dosing observed"] == n_loop
        assert by_stage["Candidate 28-day window"] == n_loop, "every archetype spans 28 days"
        seg_ids = {r["_userId"] for r in spark.table(run_pipeline.TABLES["valid_transition_segments"])
                   .select("_userId").distinct().collect()}
        assert "int_user_25" not in seg_ids, "int_user_25 must fail 12 of 14 days of the right kind"
        print(f"PASS: upstream stages pinned ({n_bddp} BDDP / {n_loop} loop); int_user_25 has no segment")

        # ── Window stages recomputed from the cohort table's columns ─────
        cohort = spark.table(run_pipeline.TABLES["transition_cohort"]).toPandas()
        assert by_stage["Valid TB→AB transition segment"] == cohort["_userId"].nunique()
        gated = cohort[cohort["passes_loop_version_gate"].eq(True) & cohort["passes_age_gate"].eq(True)
                       & (cohort["has_endpoints_seg1"].eq(True) | cohort["has_endpoints_seg2"].eq(True))]
        assert by_stage["Analysis cohort gate"] == gated["_userId"].nunique()
        assert by_stage["Type 1 diabetes"] == gated[gated["is_primary_diagnosis"].eq(True)]["_userId"].nunique()
        final = cohort[cohort["in_transition_cohort"].eq(True) & cohort["is_primary_diagnosis"].eq(True)]
        assert by_stage["Final transition cohort"] == final["_userId"].nunique()
        assert int(table.set_index("stage").loc["Final transition cohort", "n_segments"]) == len(final)
        print("PASS: window stages equal counts recomputed from the cohort table")

        # ── Final stage = the §8 transition cohort ────────────────────────
        import utils.data_loading as data_loading  # analysis/ is on sys.path

        independent = data_loading.load_transition_endpoints(redirected)
        assert by_stage["Final transition cohort"] == independent["_userId"].nunique()
        assert len(df) == df["_userId"].nunique(), "cohort df not one row per user"
        user_ids = set(df["_userId"].tolist())
        required = {"int_user_01", "int_user_02", "int_user_03"}
        assert required.issubset(user_ids), f"final cohort missing {required - user_ids}; got {sorted(user_ids)}"
        print(f"PASS: final stage = transition cohort (N = {len(df)}), contains 01/02/03")

        # ── The by-group table carries every group and the two totals ────
        by_group = pd.read_csv(os.path.join(output_dir, "table_6_3a_cohort_flow_by_diagnosis_group.csv"))
        assert list(by_group.columns[:2]) == ["stage", "description"]
        assert {"all", "primary", *data_loading.DIAGNOSIS_GROUPS} <= set(by_group.columns)
        assert "Type 1 diabetes" not in by_group["stage"].tolist()
        last = by_group.set_index("stage").loc["Final transition cohort"]
        assert last["all"] == last["primary"] == last["type1"], "the fixture is all type 1"
        print("PASS: by-group flow")

        # ── Stable and durability flows ───────────────────────────────────
        stable = pd.read_csv(os.path.join(output_dir, "table_flow_stable_autobolus.csv")).set_index("stage")
        assert stable.loc["JAEB study linkage", "all"] == 1, "int_user_19 is the one JAEB-linked stable user"
        durability = pd.read_csv(os.path.join(output_dir, "table_flow_durability.csv")).set_index("stage")
        assert durability.loc["Age gate", "all"] == 2, "int_user_21 and 22 qualify; 23 lacks follow-up"
        print("PASS: stable and durability flows")

        print("\nAll integration assertions for analysis 6-3a passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
