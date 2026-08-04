"""Integration test for analysis IR-2 (guardrail-group outcomes, PLN IR-1002).

Builds the synthetic BDDP fixture, runs the full staging pipeline against it,
then runs `analysis_ir-2.run_analysis()` over a `RedirectingSpark` wrapper so
the analysis reads the `test_*`-prefixed tables instead of prod.

Assertions are structural plus archetype-keyed: the IR-1002 archetypes
(int_user_26..32) are built to land in specific guardrail groups, so this test
pins the classification end-to-end — including the settings-fallback mitigation
path, the first-AB-day qualifying anchor, and the indeterminate path.

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
    "analysis_ir-2_guardrail_group_outcomes.py",
)
_spec = importlib.util.spec_from_file_location("analysis_ir_2", _analysis_path)
analysis_ir_2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_ir_2)


# Archetype -> expected guardrail group (see build_synthetic_bddp.py).
EXPECTED_GROUPS = {
    "int_user_26": "compliant",     # in-guardrail activations
    "int_user_27": "p_only",        # target low 40 < 67
    "int_user_28": "m_only",        # needs 180%, no own target, schedule low 100
    "int_user_29": "compliant",     # violation precedes the first AB day
    "int_user_30": "compliant",     # multiday span, compliant params
    "int_user_31": "compliant",     # needs 180% but indeterminate (no settings)
    "int_user_32": "both",          # separate P and M violations
}

spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_ir_2_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)

        # Stale-catalog check before spending analysis time: the fixture must
        # carry activeSchedule (added 2026-08-04 for the correction-range
        # history) and the staged flags table must exist. run_pipeline's
        # idempotency guard is existence-only, so a catalog built before these
        # landed is reused as-is — fix with run_pipeline.run(spark, force=True).
        bddp_cols = redirected.table("dev.default.bddp_sample_all_2").columns
        assert "activeSchedule" in bddp_cols, (
            "BDDP fixture lacks activeSchedule — stale test catalog; re-run "
            "run_pipeline.run(spark, force=True) and retry"
        )
        print("PASS: BDDP fixture carries activeSchedule")

        result = analysis_ir_2.run_analysis(redirected, output_dir=output_dir)

        expected_outputs = (
            "table_ir2a_cohort_flow.csv",
            "table_ir2b_outcomes_by_group.csv",
            "table_ir2c_data_checks.csv",
            "figure_ir2a_stacked_ranges.png",
            "figure_ir2b_target_safety.png",
            "figure_ir2c_hyper_overall.png",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        users = result["users"]
        groups_by_user = dict(zip(users["_userId"], users["guardrail_group"]))

        # THE classification pin: every IR-1002 archetype lands in its group.
        for user_id, expected in EXPECTED_GROUPS.items():
            assert user_id in groups_by_user, (
                f"{user_id} missing from the IR-2 cohort "
                f"(has {len(groups_by_user)} users)"
            )
            assert groups_by_user[user_id] == expected, (
                f"{user_id}: expected {expected}, got {groups_by_user[user_id]}"
            )
        print("PASS: all IR-1002 archetypes land in their expected guardrail group")

        # The fallback path specifically: int_user_28's M comes from the
        # scheduled correction range, not from a preset target, so it only
        # resolves when correction_range_history is populated and joined.
        flags = result["flags"]
        u28 = flags[flags["_userId"] == "int_user_28"]
        assert bool(u28["is_m_violation"].any()), (
            "int_user_28 should violate the mitigation via the settings fallback"
        )
        assert not bool(u28["is_m_indeterminate"].any()), (
            "int_user_28 has a settings record, so it must not be indeterminate"
        )
        # …and the indeterminate path: int_user_31 has no pumpSettings at all.
        u31 = flags[flags["_userId"] == "int_user_31"]
        assert bool(u31["is_m_indeterminate"].any()), (
            "int_user_31 (no pumpSettings) should be mitigation-indeterminate"
        )
        assert not bool(u31["is_m_violation"].any()), (
            "an indeterminate activation must not set the M flag"
        )
        print("PASS: settings-fallback M and indeterminate paths distinguished")

        # The first-AB-day anchor: int_user_29's violating activation precedes
        # its first eligible AB day, so it is flagged but not qualifying.
        u29 = flags[flags["_userId"] == "int_user_29"]
        assert bool(u29["is_p_violation"].any()), (
            "int_user_29's day-0 activation should still carry the P flag"
        )
        assert not bool(
            (u29["is_p_violation"] & u29["is_qualifying"]).any()
        ), "int_user_29's P violation precedes the first AB day → not qualifying"
        print("PASS: pre-first-AB-day violation excluded from the exposure set")

        # Table shapes and internal consistency.
        table_ir2a = result["table_ir2a"]
        group_rows = table_ir2a[table_ir2a["Stage / Group"].str.startswith("—")]
        assert len(group_rows) == 5, f"expected 5 group rows, got {len(group_rows)}"
        assert group_rows["Users, n"].sum() == len(users), (
            f"group rows {group_rows['Users, n'].sum()} != cohort {len(users)}"
        )
        never_row = group_rows[group_rows["Stage / Group"].str.contains("Never-preset")]
        assert int(never_row["Activations, n"].iloc[0]) == 0, (
            "the never-preset group must have zero qualifying activations"
        )
        print(f"PASS: IR-2a group rows partition the cohort ({len(users)} users)")

        table_ir2b = result["table_ir2b"]
        assert "Users, n" in set(table_ir2b["Outcome"]), "IR-2b missing the count row"
        for _, label in analysis_ir_2.GROUPS:
            assert label in table_ir2b.columns, f"IR-2b missing column {label}"
        print("PASS: IR-2b carries all five group columns and the count row")

        print("\nAll integration assertions for analysis IR-2 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
