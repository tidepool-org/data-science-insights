"""
Unit test for production_runs/run_all_coverages.py (no Spark).

Monkeypatches the two staging run()s and the analysis runner with recorders
and verifies the one-click driver builds every coverage threshold in order
into its own tables, runs 8-7 on the same suffix, honors --only subsets and
--skip-analysis, rejects unknown suffixes, and keeps its primary row in
lockstep with the staging default. The analysis runner is then checked on a
stub module, and analysis 8-7's loaders are checked to read the suffixed
tables.
"""

import os
import sys
import tempfile

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))

import run_all_coverages  # noqa: E402

CATALOG = run_all_coverages.CATALOG


def _with_recorder(fn):
    calls = []
    originals = (run_all_coverages.durability.run, run_all_coverages.event_times.run,
                 run_all_coverages._run_analysis)

    def fake_durability(spark, output_table, min_coverage):
        calls.append(("durability", output_table, min_coverage))

    def fake_event_times(spark, output_table, durability_table):
        calls.append(("event_times", output_table, durability_table))

    def fake_analysis(spark, suffix):
        calls.append(("analysis", suffix))

    # build_tables calls through the driver's module aliases — patch those.
    run_all_coverages.durability.run = fake_durability
    run_all_coverages.event_times.run = fake_event_times
    run_all_coverages._run_analysis = fake_analysis
    try:
        fn(calls)
    finally:
        (run_all_coverages.durability.run, run_all_coverages.event_times.run,
         run_all_coverages._run_analysis) = originals


def _expected(suffix, min_coverage, run_analysis=True):
    steps = [
        ("durability", f"{CATALOG}.autobolus_durability{suffix}", min_coverage),
        ("event_times", f"{CATALOG}.autobolus_event_times{suffix}",
         f"{CATALOG}.autobolus_durability{suffix}"),
    ]
    if run_analysis:
        steps.append(("analysis", suffix))
    return steps


def test_runs_every_coverage_in_order():
    def check(calls):
        run_all_coverages.run(spark=object())
        assert calls == (
            _expected("", 0.80)             # report primary, unsuffixed
            + _expected("_cov070", 0.70)
            + _expected("_cov090", 0.90)
        ), calls
    _with_recorder(check)
    print("PASS: test_runs_every_coverage_in_order")


def test_primary_matches_the_staging_default():
    # MC, 2026-10-30: durability's dosing coverage is 0.80, with 0.70 and 0.90
    # as sensitivity builds. The unsuffixed row and the staging default must
    # describe the same build, so a bare pipeline run produces the primary.
    by_suffix = dict(run_all_coverages.COVERAGE_CONFIGS)
    assert run_all_coverages.durability.DEFAULT_MIN_COVERAGE == 0.80
    assert by_suffix == {"": 0.80, "_cov070": 0.70, "_cov090": 0.90}, by_suffix
    assert by_suffix[""] == run_all_coverages.durability.DEFAULT_MIN_COVERAGE
    print("PASS: test_primary_matches_the_staging_default")


def test_only_subset_and_skip_analysis():
    def check(calls):
        run_all_coverages.run(spark=object(), run_analysis=False, only=["_cov090", ""])
        assert calls == (
            _expected("", 0.80, run_analysis=False)
            + _expected("_cov090", 0.90, run_analysis=False)
        ), calls
    _with_recorder(check)
    print("PASS: test_only_subset_and_skip_analysis")


def test_unknown_suffix_raises():
    def check(calls):
        try:
            run_all_coverages.run(spark=object(), only=["_cov075"])
        except ValueError as e:
            assert "_cov075" in str(e)
        else:
            raise AssertionError("expected ValueError for unknown suffix")
        assert calls == [], "nothing should run on an unknown suffix"
    _with_recorder(check)
    print("PASS: test_unknown_suffix_raises")


def test_analysis_runner_passes_the_suffix_as_the_table_suffix():
    # _run_analysis loads the analysis by path. Point it at a stub whose
    # run_in_databricks raises with its arguments, so the wiring is checked
    # without Spark or the plotting stack.
    stub_dir = tempfile.mkdtemp(prefix="coverage_stub_")
    with open(os.path.join(stub_dir, "stub_analysis.py"), "w") as f:
        f.write("def run_in_databricks(spark, suffix='', table_suffix=''):\n"
                "    raise LookupError(suffix + '|' + table_suffix)\n")
    originals = (run_all_coverages._ANALYSIS_DIR, run_all_coverages.ANALYSIS_FILE)
    run_all_coverages._ANALYSIS_DIR, run_all_coverages.ANALYSIS_FILE = stub_dir, "stub_analysis.py"
    try:
        run_all_coverages._run_analysis(object(), "_cov070")
    except LookupError as e:
        assert str(e) == "_cov070|_cov070", str(e)
    else:
        raise AssertionError("the stub analysis was not called")
    finally:
        run_all_coverages._ANALYSIS_DIR, run_all_coverages.ANALYSIS_FILE = originals
    print("PASS: test_analysis_runner_passes_the_suffix_as_the_table_suffix")


def test_analysis_loaders_read_the_suffixed_tables():
    path = os.path.join(_here, "..", "..", "analysis", run_all_coverages.ANALYSIS_FILE)
    with open(path) as f:
        text = f.read()
    # the durability table through the cohort loader (2026-11-05), the event
    # times inline, both with the table suffix and never without it
    assert text.count("load_durability_cohort(spark, table_suffix=table_suffix)") == 1, \
        "8-7 must read autobolus_durability through load_durability_cohort with the table suffix"
    assert text.count('spark.table(f"dev.fda_510k_rwd.autobolus_event_times{table_suffix}")') == 1, \
        "8-7 must read autobolus_event_times with the table suffix"
    for table in ("autobolus_durability", "autobolus_event_times"):
        assert f'spark.table("dev.fda_510k_rwd.{table}")' not in text, (
            f"8-7 still reads {table} without the table suffix"
        )
    print("PASS: test_analysis_loaders_read_the_suffixed_tables")


if __name__ == "__main__":
    test_runs_every_coverage_in_order()
    test_primary_matches_the_staging_default()
    test_only_subset_and_skip_analysis()
    test_unknown_suffix_raises()
    test_analysis_runner_passes_the_suffix_as_the_table_suffix()
    test_analysis_loaders_read_the_suffixed_tables()
    print("\nAll run_all_coverages unit tests passed.")
