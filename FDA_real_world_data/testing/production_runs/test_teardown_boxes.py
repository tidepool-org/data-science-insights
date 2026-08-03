"""
Unit test for production_runs/teardown_boxes.py (no Spark).

Uses a fake spark recording .sql() calls to verify: variant-only default
scope, the production guard, --include-prod, --dry-run, the test-catalog
flag wiring, and that BOX_TABLES stays in lockstep with
run_transition_variant.build_tables (drift guard).
"""

import inspect
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

import run_transition_variant  # noqa: E402
import teardown_boxes  # noqa: E402


class FakeSpark:
    def __init__(self):
        self.statements = []

    def sql(self, stmt):
        self.statements.append(stmt)


def test_default_drops_variant_namespaces_only():
    spark = FakeSpark()
    teardown_boxes.run(spark)
    expected = [
        f"DROP TABLE IF EXISTS {teardown_boxes.CATALOG}.{name}{suffix}"
        for suffix in ("_box080", "_box090")
        for name in teardown_boxes.BOX_TABLES
    ]
    assert spark.statements == expected, spark.statements
    # No statement may target a bare production table name.
    for name in teardown_boxes.BOX_TABLES:
        prod = f"DROP TABLE IF EXISTS {teardown_boxes.CATALOG}.{name}"
        assert prod not in spark.statements, prod
    print("PASS: test_default_drops_variant_namespaces_only")


def test_production_guard():
    spark = FakeSpark()
    try:
        teardown_boxes.run(spark, suffixes=[""])
    except ValueError as e:
        assert "PRODUCTION" in str(e)
    else:
        raise AssertionError("expected ValueError without include_prod")
    assert spark.statements == [], "nothing may drop before the guard fires"
    print("PASS: test_production_guard")


def test_include_prod_appends_production():
    spark = FakeSpark()
    teardown_boxes.run(spark, include_prod=True)
    n_tables = len(teardown_boxes.BOX_TABLES)
    assert len(spark.statements) == 3 * n_tables
    prod_stmts = spark.statements[-n_tables:]
    assert prod_stmts[0] == (
        f"DROP TABLE IF EXISTS {teardown_boxes.CATALOG}.valid_transition_segments"
    ), prod_stmts[0]
    print("PASS: test_include_prod_appends_production")


def test_dry_run_executes_nothing():
    spark = FakeSpark()
    teardown_boxes.run(spark, dry_run=True, include_prod=True, test_catalog=True)
    assert spark.statements == [], spark.statements
    print("PASS: test_dry_run_executes_nothing")


def test_test_catalog_flag_wiring():
    calls = []
    original = teardown_boxes._test_catalog_teardown
    teardown_boxes._test_catalog_teardown = lambda spark: calls.append(spark)
    try:
        spark = FakeSpark()
        teardown_boxes.run(spark, test_catalog=True)
        assert calls == [spark], calls
        teardown_boxes.run(FakeSpark(), test_catalog=False)
        assert len(calls) == 1, "teardown must not run without the flag"
    finally:
        teardown_boxes._test_catalog_teardown = original
    print("PASS: test_test_catalog_flag_wiring")


def test_box_tables_match_build_tables():
    # Drift guard: every table build_tables materialises must be listed in
    # BOX_TABLES, and nothing more (count the _t("<name>", suffix) calls).
    src = inspect.getsource(run_transition_variant.build_tables)
    for name in teardown_boxes.BOX_TABLES:
        assert f'_t("{name}", suffix)' in src, f"{name} not built by build_tables"
    assert src.count('_t("') == len(teardown_boxes.BOX_TABLES), (
        "build_tables materialises a table missing from BOX_TABLES"
    )
    print("PASS: test_box_tables_match_build_tables")


if __name__ == "__main__":
    test_default_drops_variant_namespaces_only()
    test_production_guard()
    test_include_prod_appends_production()
    test_dry_run_executes_nothing()
    test_test_catalog_flag_wiring()
    test_box_tables_match_build_tables()
    print("\nAll teardown_boxes unit tests passed.")
