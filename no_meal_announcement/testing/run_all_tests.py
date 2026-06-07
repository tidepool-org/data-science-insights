"""Run all PLN-1008 NMA tests and report pass/fail.

Two layers, run in order:

  1. **pytest unit tests** (always) — the non-Spark `test_*.py`: the row-builder shape tests
     (`integration/test_build_synthetic_nma_bddp.py`), the helper tests (`test_nma_test_helpers.py`),
     and the statistics tests (`analysis/`). Run via `pytest.main(...)` so the assertions actually
     execute (a bare `runpy` import would not run them — these are pytest-style, not `__main__`-style).

  2. **Spark / integration layer** (only when a SparkSession is available — i.e. on Databricks /
     Databricks Connect):
       - the `data_staging/` Spark tests (also `test_*.py`, added to the pytest targets), and
       - the `run_test_analysis_8_{1,2,3,4}.py` end-to-end runners, executed via `runpy.run_path`
         (they are `run_*`, not `test_*`, so pytest never collects them; each has a `__main__` block
         that builds the staging pipeline and self-fetches Spark via `run_pipeline.get_spark()`).

Run **locally** (`python testing/run_all_tests.py`) for layer 1 only — layer 2 is skipped with a note.
Run on **Databricks** (run-button / a notebook cell, or `run_all_tests.main(spark)`) for both.
Exit code is non-zero if any layer reports a failure.
"""

import glob
import os
import runpy
import sys
import time


def _testing_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:  # Databricks notebook: __file__ undefined
        return ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
                "no_meal_announcement/testing")


def _run_pytest(testing_dir, include_spark):
    """Run the pytest-collectable unit tests. `include_spark` adds the Spark `data_staging/` tests
    (only meaningful on a cluster). Returns (label, ok, detail)."""
    try:
        import pytest
    except ImportError:
        return ("pytest unit tests", False, "pytest not installed")
    targets = [
        os.path.join(testing_dir, "test_nma_test_helpers.py"),
        os.path.join(testing_dir, "analysis"),
        os.path.join(testing_dir, "integration", "test_build_synthetic_nma_bddp.py"),
    ]
    if include_spark:
        targets.append(os.path.join(testing_dir, "data_staging"))
    targets = [t for t in targets if os.path.exists(t)]
    code = int(pytest.main(["-q", *targets]))
    # pytest exit codes: 0 = all passed, 5 = no tests collected; both are "nothing failed".
    label = "pytest unit + data_staging" if include_spark else "pytest unit tests"
    return (label, code in (0, 5), f"pytest exit {code}")


def _run_integration(testing_dir, spark):
    """runpy each run_test_analysis_8_*.py (they self-fetch Spark via run_pipeline.get_spark()).
    Returns a list of (name, ok|None, detail); ok=None means skipped (no Spark)."""
    runners = sorted(glob.glob(os.path.join(testing_dir, "integration", "run_test_analysis_8_*.py")))
    results = []
    for path in runners:
        name = os.path.basename(path)
        if spark is None:
            results.append((name, None, "skipped — no SparkSession (run on Databricks)"))
            continue
        start = time.time()
        try:
            # run_name="__main__" triggers each runner's `if __name__ == "__main__" …: main()` block.
            runpy.run_path(path, run_name="__main__")
            results.append((name, True, f"{time.time() - start:.1f}s"))
        except Exception as e:  # noqa: BLE001 — one failing runner shouldn't abort the suite
            results.append((name, False, f"{time.time() - start:.1f}s — {e}"))
    return results


def main(spark=None) -> int:
    """Run the test layers; return a process exit code (0 ok, 1 if anything failed)."""
    testing_dir = _testing_dir()
    if testing_dir not in sys.path:
        sys.path.insert(0, testing_dir)
    on_cluster = spark is not None

    rows = [_run_pytest(testing_dir, include_spark=on_cluster)]
    rows += _run_integration(testing_dir, spark)

    print(f"\n{'=' * 60}\nNMA TEST SUMMARY\n{'=' * 60}")
    failed = 0
    for name, ok, detail in rows:
        tag = "PASS" if ok else ("SKIP" if ok is None else "FAIL")
        failed += 1 if ok is False else 0
        print(f"  {tag}  {name}  ({detail})")
    skipped = sum(1 for _, ok, _ in rows if ok is None)
    print(f"\n{len(rows) - failed - skipped} ok, {failed} failed, {skipped} skipped")
    if not on_cluster:
        print("(local run — the Spark data_staging tests + integration runners were skipped; "
              "run on Databricks for the full suite)")
    return 1 if failed else 0


# Run as a script (`python run_all_tests.py`) OR via the Databricks run-button / a notebook cell
# (those inject a `spark` global → the Spark layer runs); inert on plain import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821 — Databricks-injected
    except NameError:
        _spark = None
    raise SystemExit(main(_spark))
