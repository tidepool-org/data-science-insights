"""Run every FDA RWD integration test in sequence.

Each test uses `run_pipeline.session()` so tests share the persisted pipeline
output (the `_all_terminal_tables_exist` short-circuit kicks in). Test tables
persist across runs — failures included; there is NO automatic teardown. Call
`run_pipeline.teardown(spark)` (or `production_runs/teardown_boxes.py
--test-catalog`) when you need a clean rebuild, e.g. after a staging schema
change — the existence-only guard cannot detect one (deliberate: the
auto-rebuild schema sentinel was backed out 2026-07-30, see project_history).

Run on Databricks:
    from testing.integration import run_all_tests
    run_all_tests.main()

Or in a notebook cell:
    %run ./run_all_tests.py
"""

import os
import runpy
import sys
import traceback


TESTS = [
    "test_analysis_6_3a.py",
    "test_analysis_8_1.py",
    "test_analysis_8_2.py",
    "test_analysis_8_3.py",
    "test_analysis_8_4.py",
    "test_analysis_8_5.py",
    "test_analysis_8_6.py",
    "test_analysis_8_7.py",
    "test_analysis_8_8.py",
    "test_analysis_ir_1.py",
    "test_analysis_ir_2.py",
    "test_analysis_ir_3.py",
]


def main():
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing/integration"

    results = []
    for test_name in TESTS:
        path = os.path.join(here, test_name)
        print("\n" + "=" * 72)
        print(f"▶ {test_name}")
        print("=" * 72)
        try:
            runpy.run_path(path, run_name="__main__")
            results.append((test_name, "PASS", None))
        except BaseException as e:
            err = f"{type(e).__name__}: {e}"
            traceback.print_exc()
            results.append((test_name, "FAIL", err))

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    for name, status, err in results:
        marker = "✓" if status == "PASS" else "✗"
        line = f"  {marker} {name}: {status}"
        if err:
            line += f" — {err}"
        print(line)

    passed = sum(1 for _, s, _ in results if s == "PASS")
    failed = len(results) - passed
    print()
    print(f"{passed} passed, {failed} failed (of {len(results)} total)")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
