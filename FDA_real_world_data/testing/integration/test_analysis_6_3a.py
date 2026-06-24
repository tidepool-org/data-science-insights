"""Integration test for analysis 6-3a (Table 6.3a cohort flow).

The funnel over the synthetic BDDP fixture is pinned stage by stage: the
upstream SQL-derived stages must match counts derived from the fixture
tables (int_user_25 — alternating dosing days — is the discriminating case
that separates "Candidate 28-day window" from "Day-coverage gate"), and the
final stage must equal an independently loaded transition cohort (the same
one the §8 analyses see, incl. the 8-1 containment archetypes).

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


output_dir = tempfile.mkdtemp(prefix="int_test_6_3a_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)
        result = analysis_6_3a.run_analysis(redirected, output_dir=output_dir)
        table = result["table_6_3a"]
        df = result["df"]

        # ── Output artifact ───────────────────────────────────────────────
        out_path = os.path.join(output_dir, "table_6_3a_cohort_flow.csv")
        assert os.path.exists(out_path), "missing table_6_3a_cohort_flow.csv"
        on_disk = pd.read_csv(out_path)
        assert list(on_disk.columns) == ["stage", "description", "n_users", "n_segments"], (
            f"unexpected columns: {list(on_disk.columns)}"
        )
        print("PASS: table_6_3a_cohort_flow.csv written with expected columns")

        # ── Funnel shape ──────────────────────────────────────────────────
        # 5 upstream stages + 7 loader stages (incl. the type-1 diagnosis gate).
        assert len(table) == 12, f"expected 12 funnel stages, got {len(table)}"
        users = table["n_users"].tolist()
        assert all(b <= a for a, b in zip(users, users[1:])), (
            f"n_users not non-increasing down the funnel: {users}"
        )
        assert users[0] > 0, "BDDP sample stage is empty"
        print(f"PASS: 12-stage funnel, n_users non-increasing ({users})")

        # ── Upstream SQL stages pinned to the fixture ─────────────────────
        # Exact pins (not just bounds) so an over-admitting drift from
        # export_valid_transition_segments.py's window/coverage SQL fails too.
        by_stage = table.set_index("stage")["n_users"]
        n_bddp = (
            spark.table(run_pipeline.TABLES["bddp"])
            .select("_userId").distinct().count()
        )
        n_loop = (
            spark.table(run_pipeline.TABLES["loop_recommendations"])
            .select("_userId").distinct().count()
        )
        assert by_stage["BDDP sample"] == n_bddp, (
            f"BDDP stage {by_stage['BDDP sample']} != {n_bddp} fixture users"
        )
        assert by_stage["Loop automated dosing observed"] == n_loop, (
            f"loop stage {by_stage['Loop automated dosing observed']} != "
            f"{n_loop} loop_recommendations users"
        )
        # Every archetype emits loop dosing across a ≥28-day span, so all of
        # them anchor a candidate window...
        assert by_stage["Candidate 28-day window"] == n_loop, (
            f"candidate stage {by_stage['Candidate 28-day window']} != {n_loop}"
        )
        # ...and int_user_25 (alternating dosing days → 7/14 per half) is the
        # one archetype built to fail the 70% day-coverage gate.
        assert by_stage["Day-coverage gate"] == n_loop - 1, (
            f"day-coverage stage {by_stage['Day-coverage gate']} != "
            f"{n_loop - 1} (all loop users minus int_user_25)"
        )
        print(f"PASS: upstream stages pinned ({n_bddp} BDDP / {n_loop} loop / "
              f"{n_loop - 1} day-covered)")

        # ── Staged-table stage matches the fixture table exactly ──────────
        seg_users = (
            spark.table(run_pipeline.TABLES["valid_transition_segments"])
            .select("_userId").distinct().count()
        )
        assert by_stage["Valid TB→AB transition segment"] == seg_users, (
            f"segments stage {by_stage['Valid TB→AB transition segment']} != "
            f"{seg_users} distinct users in the fixture segments table"
        )
        print(f"PASS: segments stage matches fixture table ({seg_users} users)")

        # ── Final stage = the §8 transition cohort ────────────────────────
        # Independent loader call (not the object the funnel was recorded
        # from), so the check isn't satisfied by construction.
        import utils.data_loading as data_loading  # analysis/ is on sys.path

        independent = data_loading.load_transition_endpoints(redirected)
        assert by_stage["Final transition cohort"] == independent["_userId"].nunique(), (
            f"final funnel stage {by_stage['Final transition cohort']} != "
            f"independently loaded §8 cohort ({independent['_userId'].nunique()})"
        )
        assert len(df) == df["_userId"].nunique(), "cohort df not one row per user"
        user_ids = set(df["_userId"].tolist())
        required = {"int_user_01", "int_user_02", "int_user_03"}
        assert required.issubset(user_ids), (
            f"6-3a final cohort missing {required - user_ids}; got {sorted(user_ids)}"
        )
        print(f"PASS: final stage = transition cohort (N = {len(df)}), "
              f"contains 01/02/03")

        print("\nAll integration assertions for analysis 6-3a passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
