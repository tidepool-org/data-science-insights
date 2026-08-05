"""Integration test for analysis IR-3 (AB-day preset characterization, PLN IR-1002).

Builds the synthetic BDDP fixture, runs the full staging pipeline against it,
then runs `analysis_ir-3.run_analysis()` over a `RedirectingSpark` wrapper so
the analysis reads the `test_*`-prefixed tables instead of prod.

Pins the two things IR-3 does that IR-2 does not: the all-spanned-days-AB
inclusion rule (a multiday activation crossing a non-AB day is dropped) and the
collapsed insulin-needs quantity (basal = f, CR = ISF = 1/f, reported once as a
percentage rather than as three parallel — and for two of them, inverted —
scale-factor rows).

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
    "analysis_ir-3_preset_characterization_ab_days.py",
)
_spec = importlib.util.spec_from_file_location("analysis_ir_3", _analysis_path)
analysis_ir_3 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_ir_3)


spark = run_pipeline.get_spark()


output_dir = tempfile.mkdtemp(prefix="int_test_ir_3_")
try:
    with run_pipeline.session(spark):
        redirected = run_pipeline.RedirectingSpark(spark)

        result = analysis_ir_3.run_analysis(redirected, output_dir=output_dir)

        expected_outputs = (
            "table_ir3a_parameter_distributions.csv",
            "table_ir3b_activation_durations.csv",
            "table_ir3c_per_user_full_cohort.csv",
            "table_ir3d_per_user_preset_users.csv",
            "table_ir3f_data_checks.csv",
        )
        for f in expected_outputs:
            path = os.path.join(output_dir, f)
            assert os.path.exists(path), f"missing expected output: {f}"
        print("PASS: expected output artifacts written")

        activations = result["activations"]

        # The all-spanned-days-AB rule: int_user_30's day-4 20:00 activation
        # runs 14 h into a temp-basal day, so it must be absent; its same-day
        # activation on day 8 must survive.
        u30 = activations[activations["_userId"] == "int_user_30"]
        assert len(u30) == 1, (
            f"int_user_30 should contribute exactly 1 activation (the same-day "
            f"one); got {len(u30)} — the multiday span crossing a non-AB day "
            f"should have been dropped"
        )
        assert u30["overridePreset"].iloc[0] == "Compliant", (
            f"the surviving int_user_30 activation should be the same-day one; "
            f"got {u30['overridePreset'].iloc[0]}"
        )
        print("PASS: multiday activation crossing a non-AB day excluded")

        # The qualifying anchor carries into IR-3 as well: int_user_29's
        # pre-first-AB-day activation is not in the set.
        u29_presets = set(
            activations.loc[activations["_userId"] == "int_user_29", "overridePreset"]
        )
        assert "PreAB" not in u29_presets, (
            f"int_user_29's pre-first-AB activation should be excluded; "
            f"got {u29_presets}"
        )
        print("PASS: pre-first-AB-day activation excluded from the IR-3 set")

        # Insulin needs: collapsed to ONE quantity, in percent, and derived so
        # that a preset with basal 1.8 / CR = ISF = 1/1.8 reads as 180% — not
        # as one row at 1.8 and two at 0.56.
        u28 = activations[activations["_userId"] == "int_user_28"]
        assert len(u28) == 1, f"int_user_28 should contribute 1 activation; got {len(u28)}"
        needs = float(u28["insulin_needs_pct"].iloc[0])
        assert abs(needs - 180.0) < 0.5, (
            f"int_user_28's insulin needs should read ~180%; got {needs}"
        )
        print(f"PASS: insulin needs collapsed to a single percentage ({needs:.1f}%)")

        table_ir3a = result["table_ir3a"]
        params = set(table_ir3a["Parameter"])
        assert "Overall insulin needs (%)" in params, params
        for dropped in ("Basal rate scale factor", "Carb ratio scale factor",
                        "Insulin sensitivity scale factor"):
            assert dropped not in params, (
                f"IR-3a should not report the raw factor row {dropped!r} — the "
                f"CR/ISF factors are reciprocals of insulin needs"
            )
        # 2 grains x 5 strata x 4 parameters.
        assert len(table_ir3a) == 40, len(table_ir3a)
        print("PASS: IR-3a reports the collapsed needs row at 2 grains x 5 strata")

        # Guardrail-status strata: the archetypes put activations in each.
        status_by_user = dict(zip(activations["_userId"], activations["guardrail_status"]))
        assert status_by_user.get("int_user_27") == "p_only", status_by_user.get("int_user_27")
        assert status_by_user.get("int_user_28") == "m_only", status_by_user.get("int_user_28")
        assert status_by_user.get("int_user_26") == "compliant", status_by_user.get("int_user_26")
        print("PASS: activation-level guardrail status assigned per archetype")

        # IR-3b duration rows: 5 strata x 2 duration definitions.
        assert len(result["table_ir3b"]) == 10, len(result["table_ir3b"])
        # IR-3c is zero-filled over every cohort user, so its denominator is at
        # least the number of users contributing activations.
        table_ir3c = result["table_ir3c"]
        # Label deliberately says "contributing activations to this analysis",
        # not "any preset use" — the IR-3 activation set additionally requires
        # every day of the activation to be AB (see create_table_ir3c).
        any_use = table_ir3c[
            table_ir3c["Outcome"]
            == "Users contributing activations to this analysis, n (%)"
        ]
        assert not any_use.empty, "IR-3c missing the users-contributing row"
        n_cohort = int(any_use["N"].iloc[0])
        assert n_cohort >= activations["_userId"].nunique() > 0, (
            n_cohort, activations["_userId"].nunique()
        )
        print(f"PASS: IR-3c zero-filled over the cohort denominator ({n_cohort})")

        print("\nAll integration assertions for analysis IR-3 passed.")
finally:
    shutil.rmtree(output_dir, ignore_errors=True)
