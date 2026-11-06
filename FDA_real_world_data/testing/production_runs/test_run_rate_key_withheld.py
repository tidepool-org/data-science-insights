"""
Offline test for production_runs/run_rate_key_withheld.py and the classifier-variant
path of exploratory/run_transition_variant.py (no Spark).

Checks that:
- the driver asks the variant builder for the rate-key suffix, 12 of 14, the
  rate-key basal rule withheld, and analysis 8-1 only;
- a variant with a withheld rule builds its own day table first and hands it
  to the window search; one without a withheld rule builds none; a withheld
  rule without a suffix, or an unknown rule, is refused;
- the day table's SQL lists every rule by default and drops the withheld one;
- teardown drops this build's tables and nothing bare;
- a suffixed analysis 8-1 run puts the build on its figure titles, the
  production run nothing.

pytest-free (the run_all_tests.py convention): assertions run from __main__.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/production_runs/test_run_rate_key_withheld.py
"""
import importlib.util
import os
import sys
import tempfile

import matplotlib
matplotlib.use("Agg")
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
for sub in ("production_runs", "exploratory", "data_staging", "analysis"):
    sys.path.insert(0, os.path.join(PROJECT, sub))

import export_loop_recommendations  # noqa: E402
import run_rate_key_withheld  # noqa: E402
import run_transition_variant  # noqa: E402

RATE_KEY = "health_rate_loop_app"


class FakeSpark:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def patched(module, name, replacement):
    """Swap module.name for the test and give back a restore function."""
    original = getattr(module, name)
    setattr(module, name, replacement)
    return lambda: setattr(module, name, original)


def test_driver_asks_for_the_rate_key_variant():
    calls = []
    restore = patched(run_rate_key_withheld.variant, "run", lambda spark, **kw: calls.append(kw))
    spark = FakeSpark()
    spark.version = "0.test"
    cwd = os.getcwd()
    try:
        with tempfile.TemporaryDirectory() as out:
            os.chdir(out)
            run_rate_key_withheld.run(spark=spark)
            manifest = pd.read_csv(os.path.join(out, "outputs", "analysis_8_1_rate_key_withheld", "manifest.csv"))
        with tempfile.TemporaryDirectory() as tables_only:
            os.chdir(tables_only)
            run_rate_key_withheld.run(spark=spark, run_analysis=False)
            assert os.listdir(tables_only) == [], "a tables-only run writes no manifest (no outputs to describe)"
    finally:
        os.chdir(cwd)
        restore()
    assert calls[0] == {"suffix": "_rate_key_withheld", "min_period_days": 12, "run_analysis": True,
                        "withheld_basal_tags": (RATE_KEY,), "analyses": run_rate_key_withheld.ANALYSES}, calls
    row = manifest.iloc[0]
    assert row["suffix"] == "_rate_key_withheld" and row["withheld_basal_tags"] == RATE_KEY
    assert len(row["script_sha256"]) == 64 and len(row["day_table_export_sha256"]) == 64, "both hashes recorded"
    assert run_rate_key_withheld.ANALYSES == [
        "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py"]
    assert os.path.exists(os.path.join(PROJECT, "analysis", run_rate_key_withheld.ANALYSES[0]))
    assert RATE_KEY in export_loop_recommendations.BASAL_TAGS
    print("PASS: the driver asks for the rate-key suffix, 12 of 14, the rule withheld, analysis 8-1 only")


def test_variant_builds_its_day_table_first():
    day_runs, builds = [], []
    restores = [
        patched(run_transition_variant.export_loop_recommendations, "run", lambda spark, **kw: day_runs.append(kw)),
        patched(run_transition_variant, "build_tables", lambda spark, *a, **kw: builds.append((a, kw))),
    ]
    try:
        run_transition_variant.run(object(), suffix="_x", withheld_basal_tags=(RATE_KEY,), run_analysis=False)
        assert day_runs == [{"output_table": f"{run_transition_variant.CATALOG}.loop_recommendations_x",
                             "basal_tags": tuple(t for t in export_loop_recommendations.BASAL_TAGS if t != RATE_KEY)}], day_runs
        assert builds == [(("_x", 12), {"loop_recommendations_table": f"{run_transition_variant.CATALOG}.loop_recommendations_x"})], builds

        day_runs.clear(); builds.clear()
        run_transition_variant.run(object(), suffix="_y", run_analysis=False)
        assert day_runs == [] and builds == [(("_y", 12), {"loop_recommendations_table": None})], (day_runs, builds)

        for bad in ({"suffix": "", "withheld_basal_tags": (RATE_KEY,)},
                    {"suffix": "_z", "withheld_basal_tags": ("not_a_rule",)}):
            try:
                run_transition_variant.run(object(), run_analysis=False, **bad)
            except ValueError:
                pass
            else:
                raise AssertionError(f"must refuse {bad}")
    finally:
        for restore in restores:
            restore()
    print("PASS: a withheld rule builds the variant's day table first; no suffix or an unknown rule is refused")


def test_build_tables_hands_the_day_table_to_the_window_search():
    segments_calls = []
    restores = [patched(run_transition_variant, "build_segments", lambda spark, **kw: segments_calls.append(kw))]
    for module_name in ("export_cbg_from_transitions", "compute_glycemic_endpoints", "export_segments_within_guardrails",
                        "export_overrides_from_transitions", "export_cbg_from_overrides",
                        "export_carbohydrates_from_transitions",
                        "export_transition_cohort", "export_transition_activation_cohort"):
        restores.append(patched(getattr(run_transition_variant, module_name), "run", lambda spark, **kw: None))
    try:
        run_transition_variant.build_tables(object(), "_x", 12, loop_recommendations_table="day_x")
        run_transition_variant.build_tables(object(), "_x", 12)
    finally:
        for restore in restores:
            restore()
    assert segments_calls[0]["loop_recommendations_table"] == "day_x" and segments_calls[0]["min_period_days"] == 12
    assert "loop_recommendations_table" not in segments_calls[1], "the production day table stays the staging default"
    print("PASS: build_tables hands a variant day table to the window search, else the staging default")


def test_day_table_sql_withholds_the_rule():
    full, without = FakeSpark(), FakeSpark()
    export_loop_recommendations.run(full, output_table="t")
    kept = tuple(t for t in export_loop_recommendations.BASAL_TAGS if t != RATE_KEY)
    export_loop_recommendations.run(without, output_table="t", basal_tags=kept)
    sql_full, sql_without = full.statements[0], without.statements[0]
    for tag in export_loop_recommendations.BOLUS_TAGS + export_loop_recommendations.BASAL_TAGS:
        assert tag in sql_full, tag
    assert RATE_KEY not in sql_without and f"basals_{kept[0]}" in sql_without
    assert "WHERE is_automated" not in sql_full, "the day counts read the listed rules, not the classifier's union"
    assert export_loop_recommendations.recognised_sql(kept, export_loop_recommendations.BASAL_TAGS) == \
        f"GREATEST({', '.join(kept)}) = 1"
    for bad in ((("not_a_rule",), export_loop_recommendations.BASAL_TAGS), ((), export_loop_recommendations.BOLUS_TAGS)):
        try:
            export_loop_recommendations.recognised_sql(*bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"must refuse {bad}")
    print("PASS: the day table's SQL lists every rule by default and drops a withheld one")


def test_teardown_drops_this_builds_tables_only():
    spark = FakeSpark()
    run_rate_key_withheld.teardown(spark)
    expected = [f"DROP TABLE IF EXISTS {run_transition_variant.CATALOG}.{name}_rate_key_withheld"
                for name in ("loop_recommendations",) + run_transition_variant.BOX_TABLES]
    assert spark.statements == expected, spark.statements
    assert all(s.endswith("_rate_key_withheld") for s in spark.statements), "nothing bare"
    print("PASS: teardown drops the day table and the subtree of this build only")


def test_suffixed_8_1_run_titles_the_build():
    spec = importlib.util.spec_from_file_location(
        "a81", os.path.join(PROJECT, "analysis", run_rate_key_withheld.ANALYSES[0]))
    a81 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(a81)
    notes = []
    frame = pd.DataFrame({"diagnosis_group": ["type1"]})
    restores = [patched(a81, "load_data", lambda spark, suffix="", diagnosis_groups=None: frame),
                patched(a81, "write_outputs", lambda df, df_all, output_dir, cohort_note="": notes.append(cohort_note))]
    try:
        a81.run_analysis(object(), output_dir="unused", suffix="_rate_key_withheld")
        a81.run_analysis(object(), output_dir="unused", suffix="")
    finally:
        for restore in restores:
            restore()
    assert "_rate_key_withheld" in notes[0] and notes[1] == "", notes
    print("PASS: a suffixed 8-1 run names the build on its figures, the production run nothing")


if __name__ == "__main__":
    test_driver_asks_for_the_rate_key_variant()
    test_variant_builds_its_day_table_first()
    test_build_tables_hands_the_day_table_to_the_window_search()
    test_day_table_sql_withholds_the_rule()
    test_teardown_drops_this_builds_tables_only()
    test_suffixed_8_1_run_titles_the_build()
    print("test_run_rate_key_withheld: all checks passed")
