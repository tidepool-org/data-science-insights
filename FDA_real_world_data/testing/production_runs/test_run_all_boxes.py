"""
Unit test for production_runs/run_all_boxes.py (no Spark).

Monkeypatches run_transition_variant.run with a recorder and verifies the
one-click driver runs every box with the right thresholds and order, honors
--only subsets and --skip-analysis, and rejects unknown suffixes.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "FDA_real_world_data/testing/production_runs")
sys.path.insert(0, os.path.join(_here, "..", "..", "production_runs"))
sys.path.insert(0, os.path.join(_here, "..", "..", "exploratory"))

import run_all_boxes  # noqa: E402
import run_transition_variant  # noqa: E402


def _record(calls):
    def fake_run(spark, suffix, autobolus_low, autobolus_high, run_analysis):
        calls.append((suffix, autobolus_low, autobolus_high, run_analysis))
    return fake_run


def _with_recorder(fn):
    calls = []
    original = run_transition_variant.run
    # run_all_boxes calls through its own module alias — patch that reference.
    run_all_boxes.variant.run = _record(calls)
    try:
        fn(calls)
    finally:
        run_all_boxes.variant.run = original


def test_runs_every_box_in_order():
    def check(calls):
        run_all_boxes.run(spark=object())
        assert calls == [
            ("_box080", 0.20, 0.80, True),
            ("",        0.30, 0.70, True),
            ("_box090", 0.10, 0.90, True),
        ], calls
    _with_recorder(check)
    print("PASS: test_runs_every_box_in_order")


def test_box_configs_match_variant_defaults():
    # The _box080 row must stay in lockstep with run_transition_variant's
    # defaults, and the production row with the staging-script defaults.
    by_suffix = {s: (lo, hi) for s, lo, hi in run_all_boxes.BOX_CONFIGS}
    assert by_suffix["_box080"] == (
        run_transition_variant.DEFAULT_AUTOBOLUS_LOW,
        run_transition_variant.DEFAULT_AUTOBOLUS_HIGH,
    )
    from export_valid_transition_segments import (  # staging dir is on sys.path
        DEFAULT_AUTOBOLUS_LOW, DEFAULT_AUTOBOLUS_HIGH,
    )
    assert by_suffix[""] == (DEFAULT_AUTOBOLUS_LOW, DEFAULT_AUTOBOLUS_HIGH)
    print("PASS: test_box_configs_match_variant_defaults")


def test_only_subset_and_skip_analysis():
    def check(calls):
        run_all_boxes.run(spark=object(), run_analysis=False, only=["_box090", ""])
        assert calls == [
            ("",        0.30, 0.70, False),
            ("_box090", 0.10, 0.90, False),
        ], calls
    _with_recorder(check)
    print("PASS: test_only_subset_and_skip_analysis")


def test_unknown_suffix_raises():
    def check(calls):
        try:
            run_all_boxes.run(spark=object(), only=["_box075"])
        except ValueError as e:
            assert "_box075" in str(e)
        else:
            raise AssertionError("expected ValueError for unknown suffix")
        assert calls == [], "nothing should run on an unknown suffix"
    _with_recorder(check)
    print("PASS: test_unknown_suffix_raises")


if __name__ == "__main__":
    test_runs_every_box_in_order()
    test_box_configs_match_variant_defaults()
    test_only_subset_and_skip_analysis()
    test_unknown_suffix_raises()
    print("\nAll run_all_boxes unit tests passed.")
