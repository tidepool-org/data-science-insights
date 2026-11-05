"""
Unit test for production_runs/run_production_staging.py (no Spark).

Replaces every staging run() the driver calls with a recorder and checks that
the driver runs the steps in dependency order, that --steps selects a subset,
that the window and coverage builds are passed through with the analyses off,
and that an unknown step is refused.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import run_production_staging as driver  # noqa: E402

# (module attribute on the driver, the run() argument that names what is built)
STAGING_RUNS = [
    "export_loop_dose_classification", "export_loop_recommendations", "export_cbg_from_loop",
    "export_user_diagnosis_type", "export_stable_autobolus_segments", "export_cbg_from_stable",
    "compute_glycemic_endpoints", "export_segments_within_guardrails", "export_overrides_all",
    "export_correction_range_history", "export_ab_day_cohort", "export_override_guardrail_flags",
    "export_cbg_from_ab_days",
]


def _with_recorders(fn):
    calls = []
    originals = {}

    def recorder(name):
        def fake_run(spark, **kwargs):
            calls.append((name, kwargs))
        return fake_run

    for name in STAGING_RUNS:
        module = getattr(driver, name)
        originals[name] = module.run
        module.run = recorder(name)
    originals["boxes"] = driver.run_all_boxes.run
    originals["coverages"] = driver.run_all_coverages.run
    driver.run_all_boxes.run = lambda spark, run_analysis, only: calls.append(("run_all_boxes", {"run_analysis": run_analysis, "only": only}))
    driver.run_all_coverages.run = lambda spark, run_analysis, only: calls.append(("run_all_coverages", {"run_analysis": run_analysis, "only": only}))
    try:
        fn(calls)
    finally:
        for name in STAGING_RUNS:
            getattr(driver, name).run = originals[name]
        driver.run_all_boxes.run = originals["boxes"]
        driver.run_all_coverages.run = originals["coverages"]


def test_every_step_in_dependency_order():
    def check(calls):
        driver.run(spark=object())
        names = [name for name, _ in calls]
        assert names == [
            "export_loop_dose_classification", "export_loop_recommendations", "export_cbg_from_loop",
            "export_user_diagnosis_type",
            "run_all_boxes",
            "export_stable_autobolus_segments", "export_cbg_from_stable", "compute_glycemic_endpoints",
            "export_segments_within_guardrails",
            "run_all_coverages",
            "export_overrides_all", "export_correction_range_history", "export_ab_day_cohort",
            "export_override_guardrail_flags", "export_cbg_from_ab_days", "compute_glycemic_endpoints",
        ], names
        by_name = {}
        for name, kwargs in calls:
            by_name.setdefault(name, []).append(kwargs)
        # the default window build is the primary only (MC, 2026-11-05); the coverage builds stay all three
        assert by_name["run_all_boxes"] == [{"run_analysis": False, "only": [""]}]
        assert by_name["run_all_coverages"] == [{"run_analysis": False, "only": None}]
        assert [k["mode"] for k in by_name["compute_glycemic_endpoints"]] == ["stable", "ab_days"]
        assert by_name["export_segments_within_guardrails"] == [{"mode": "stable"}]
        # every other staging run() takes its production defaults
        for name, kwargs in calls:
            if name not in ("run_all_boxes", "run_all_coverages", "compute_glycemic_endpoints",
                            "export_segments_within_guardrails"):
                assert kwargs == {}, (name, kwargs)
    _with_recorders(check)
    print("PASS: test_every_step_in_dependency_order")


def test_steps_subset_and_build_selection():
    def check(calls):
        driver.run(spark=object(), steps=["transition", "base"], boxes=[""], coverages=["_cov070"])
        names = [name for name, _ in calls]
        assert names == ["export_loop_dose_classification", "export_loop_recommendations", "export_cbg_from_loop",
                         "export_user_diagnosis_type", "run_all_boxes"], names
        assert calls[-1][1] == {"run_analysis": False, "only": [""]}, calls[-1]
        # 'all' hands every build to the family driver (None = all of them)
        calls.clear()
        driver.run(spark=object(), steps=["transition", "durability"], boxes=driver.ALL_TOKEN, coverages=driver.ALL_TOKEN)
        assert [c for c in calls if c[0] == "run_all_boxes"][0][1] == {"run_analysis": False, "only": None}
        assert [c for c in calls if c[0] == "run_all_coverages"][0][1] == {"run_analysis": False, "only": None}
    _with_recorders(check)
    print("PASS: test_steps_subset_and_build_selection")


def test_unknown_step_raises():
    def check(calls):
        try:
            driver.run(spark=object(), steps=["base", "analyses"])
        except ValueError as exc:
            assert "analyses" in str(exc), exc
        else:
            raise AssertionError("an unknown step must raise")
        assert calls == [], "nothing may run before the step list is checked"
    _with_recorders(check)
    print("PASS: test_unknown_step_raises")


def test_cli_suffix_spelling():
    assert driver._suffixes(None) is None
    assert driver._suffixes("prod,_box070") == ["", "_box070"]
    assert driver._suffixes("_cov090") == ["_cov090"]
    assert driver._suffixes("all") == driver.ALL_TOKEN
    assert driver.DEFAULT_BOXES == [""], "the default window build is the primary only"
    print("PASS: test_cli_suffix_spelling")


if __name__ == "__main__":
    test_every_step_in_dependency_order()
    test_steps_subset_and_build_selection()
    test_unknown_step_raises()
    test_cli_suffix_spelling()
    print("test_run_production_staging: all checks passed")
