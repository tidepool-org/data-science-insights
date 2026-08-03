"""Integration test for analysis IR-1 (preset characterization).

Builds the synthetic BDDP fixture, runs the full staging pipeline against
it, then runs `analysis_ir-1.run_analysis()` over a `RedirectingSpark`
wrapper so the analysis reads the `test_*`-prefixed tables instead of prod.

IR-1 is descriptive: every override activation by an eligible transition
user (cohort + guardrail + type-1 gates; no validity or starting-glucose
filter) is characterized. Assertions are structural — table shapes, cohort
membership, and internal consistency against the fixture — rather than
pinned to specific archetype values, so fixture growth doesn't break them.

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
    "analysis_ir-1_preset_characterization.py",
)
_spec = importlib.util.spec_from_file_location("analysis_ir_1", _analysis_path)
analysis_ir_1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_ir_1)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_ir_1_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)

        # Stale-catalog check, before any analysis time is spent: the staged
        # overrides table must carry the stated_duration column (2026-07-30).
        # run_pipeline's idempotency guard is existence-only, so a catalog
        # built before the schema change is reused as-is — fix by re-running
        # export_overrides_from_transitions against the test tables, or
        # run_pipeline.run(spark, force=True) / teardown for a full rebuild.
        staged_cols = redirected.table("dev.fda_510k_rwd.overrides_by_segment").columns
        assert "stated_duration" in staged_cols, (
            "overrides_by_segment lacks stated_duration — stale test catalog; "
            "re-run export_overrides_from_transitions (or run_pipeline.run "
            "force=True) and retry"
        )
        print("PASS: staged overrides_by_segment carries stated_duration")

        result = analysis_ir_1.run_analysis(redirected, output_dir=output_dir)

        expected_outputs = (
            "table_ir1a_parameter_distributions.csv",
            "table_ir1b_activation_durations.csv",
            "table_ir1c_per_user_full_cohort.csv",
            "table_ir1d_per_user_preset_users.csv",
            "table_ir1e_preset_name_breakdown.csv",
            "table_ir1f_data_checks.csv",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        overrides = result["overrides"]
        user_ids = set(overrides["_userId"].tolist())
        assert "int_user_08" in user_ids, (
            f"int_user_08 (override archetype) missing; got {sorted(user_ids)}"
        )
        # The cohort gate is asserted on the denominator (int_user_04 has no
        # override rows, so its absence from `overrides` would be vacuous).
        cohort_ids = set(result["cohort_users"]["_userId"].tolist())
        assert "int_user_04" not in cohort_ids, (
            "int_user_04 (Loop 3.5.0) should fail the cohort gate"
        )
        assert "int_user_01" in cohort_ids, "int_user_01 missing from the cohort"
        print(f"PASS: cohort gate excludes int_user_04, keeps int_user_01; "
              f"int_user_08 has activations ({len(user_ids)} users, "
              f"{len(overrides)} activations)")

        # Table shapes: 2 grains × 3 periods × 6 parameters; 3 periods ×
        # 2 duration outcomes; 3 periods × 4 zero-filled per-user outcomes;
        # 3 periods × 3 preset-user outcomes; 12 checks.
        assert len(result["table_ir1a"]) == 36, len(result["table_ir1a"])
        assert len(result["table_ir1b"]) == 6
        assert len(result["table_ir1c"]) == 12
        assert len(result["table_ir1d"]) == 9
        assert len(result["table_ir1f"]) == 12
        print("PASS: table shapes (IR-1a 36, IR-1b 6, IR-1c 12, IR-1d 9, IR-1f 12)")

        # IR-1c internal consistency: one shared cohort denominator, and
        # users-with-any-use never exceeds it.
        per_user = result["table_ir1c"]
        any_use = per_user[per_user["Outcome"] == "Users with any preset use, n (%)"]
        denominators = set(any_use["N"])
        assert len(denominators) == 1, f"denominator differs by period: {denominators}"
        n_cohort = denominators.pop()
        assert n_cohort == len(cohort_ids) >= len(user_ids) > 0, (
            n_cohort, len(cohort_ids), len(user_ids)
        )
        assert (any_use["N users"] == n_cohort).all()
        print(f"PASS: IR-1c consistent (cohort denominator = {n_cohort})")

        # IR-1d: each period's frequency-row N equals that window's
        # preset-user count in the loaded activations.
        ir1d = result["table_ir1d"]
        freq = ir1d[ir1d["Outcome"] == "Preset activations per user (n/14 days)"]
        for seg, period_label in analysis_ir_1.PERIODS:
            expected_users = overrides.loc[
                overrides["segment"] == seg, "_userId"
            ].nunique()
            row = freq[freq["Period"] == period_label].iloc[0]
            assert row["N"] == expected_users, (period_label, row["N"], expected_users)
        print("PASS: IR-1d per-period N matches window preset-user counts")

        # IR-1e covers every (preset name, period) with activations, and its
        # activation total reconciles with the loaded frame.
        ir1e = result["table_ir1e"]
        assert ir1e["N activations"].sum() == len(overrides), (
            f"IR-1e activations {ir1e['N activations'].sum()} != {len(overrides)}"
        )
        print("PASS: IR-1e activation total reconciles with loaded activations")

        print("\nAll integration assertions for analysis IR-1 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
