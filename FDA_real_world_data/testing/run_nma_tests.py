"""Run all PLN-1008 NMA tests and report pass/fail — concisely.

Two layers, run in order:

  1. **pytest unit tests** (always) — the non-Spark `test_*.py`: the row-builder shape tests,
     the helper tests, the statistics/data_loader/strata tests, and the `cross_checks/` layer.
     On a cluster the Spark `data_staging/` tests are added too.

  2. **Spark / integration layer** (only when a SparkSession is available — Databricks / Connect):
     force-rebuild the synthetic fixture once, then `runpy` the four `run_test_analysis_8_*.py`
     end-to-end runners.

Output: the pytest layer runs verbose (`-v`) so EVERY test prints live as it passes/fails, with a
short stack trace (`--tb=short`) on failure. Each integration runner prints one `PASS|FAIL` line
(its chatty analysis stdout is captured and dropped on success); on failure its full Python
traceback is printed. A final tally closes the run.

Run **locally** (`python testing/run_all_tests.py`) for layer 1 only; run on **Databricks**
(`run_all_tests.main(spark)`) for both. Exit code is non-zero if anything failed.
"""

import contextlib
import glob
import io
import os
import runpy
import sys
import time

# On Databricks the repo lives on the /Workspace FUSE mount, which does NOT support creating
# __pycache__ dirs (`OSError: [Errno 95] Operation not supported`). Importing test modules — and
# pytest's assertion rewriter — would try to write bytecode there and abort collection. Disable
# bytecode writing process-wide (covers both the pytest layer and the runpy integration layer);
# pytest's assertion rewriter is disabled separately via `--assert=plain` in _run_pytest.
sys.dont_write_bytecode = True


def _testing_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:  # Databricks notebook: __file__ undefined
        return ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
                "no_meal_announcement/testing")


def _run_pytest(testing_dir, include_spark):
    """Run the pytest-collectable tests VERBOSE (live, uncaptured) so each test prints as it runs.
    Returns ok. `include_spark` adds the Spark `data_staging/` tests."""
    try:
        import pytest
    except ImportError:
        print("  FAIL  pytest not installed")
        return False
    targets = [
        os.path.join(testing_dir, "test_nma_test_helpers.py"),
        os.path.join(testing_dir, "analysis"),
        os.path.join(testing_dir, "integration", "test_build_synthetic_nma_bddp.py"),
        os.path.join(testing_dir, "cross_checks"),
        os.path.join(testing_dir, "negative_controls"),   # snapshot-gated: null recovered on real data
        os.path.join(testing_dir, "traceability"),         # snapshot-gated: frozen panel drift guard
    ]
    if include_spark:
        targets.append(os.path.join(testing_dir, "data_staging"))
    targets = [t for t in targets if os.path.exists(t)]
    # -v → one "file::test PASSED/FAILED" line per test, live (NOT captured). -rs → print the REASON
    # for every skip (e.g. a cross-check whose output table isn't on disk, or a Spark test off-cluster).
    # --tb=short → a readable stack trace on failure. --assert=plain + no:cacheprovider +
    # sys.dont_write_bytecode avoid the /Workspace __pycache__ write that aborts collection on
    # Databricks. no:warnings drops the warnings summary.
    code = int(pytest.main(["-v", "-rs", "--tb=short", "-p", "no:warnings",
                            "-p", "no:cacheprovider", "--assert=plain", *targets]))
    # pytest exit codes: 0 = all passed, 5 = nothing collected; both are "nothing failed".
    return code in (0, 5)


def _force_rebuild(spark):
    """Force-rebuild the synthetic fixture so the runners see the CURRENT archetypes (run_pipeline.run
    is idempotent and would otherwise reuse a stale analysis-ready table). Returns (ok, detail, tb)."""
    import traceback
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            from integration import run_pipeline as _rp  # testing_dir is on sys.path (see main)
            _rp.run(spark, force=True)
        return (True, "ok", "")
    except Exception:  # noqa: BLE001
        return (False, "build failed", traceback.format_exc())


def _run_runner(path, spark):
    """runpy one run_test_analysis_8_*.py with its (chatty) stdout captured. Returns (ok, detail, tb):
    on failure `tb` is the full Python traceback (stack trace); on success it's empty."""
    import traceback
    start = time.time()
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            runpy.run_path(path, run_name="__main__")  # triggers the runner's __main__ block
        return (True, f"{time.time() - start:.0f}s", "")
    except Exception:  # noqa: BLE001 — one failing runner shouldn't abort the suite
        return (False, f"{time.time() - start:.0f}s", traceback.format_exc())


def _tag(ok):
    return "PASS" if ok else ("SKIP" if ok is None else "FAIL")


def main(spark=None) -> int:
    """Run the test layers with terse per-component output; return a process exit code."""
    testing_dir = _testing_dir()
    if testing_dir not in sys.path:
        sys.path.insert(0, testing_dir)
    on_cluster = spark is not None
    passes = fails = skips = 0
    print(f"NMA tests{'' if on_cluster else '  (local — Spark layer skipped)'}")

    # ── Layer 1: pytest, verbose + live (each test prints as it runs) ─────────
    print(f"\n========== pytest (unit{' + data_staging' if on_cluster else ''}) ==========")
    ok = _run_pytest(testing_dir, include_spark=on_cluster)
    passes += int(ok)
    fails += int(not ok)

    # ── Layer 2: Spark integration (force-rebuild fixture, then the runners) ───
    print("\n========== integration runners ==========")
    runners = sorted(glob.glob(os.path.join(testing_dir, "integration", "run_test_analysis_8_*.py")))
    if not on_cluster:
        for path in runners:
            print(f"  SKIP  {os.path.basename(path)}  (no SparkSession — run on Databricks)")
            skips += 1
    else:
        print("  running fixture rebuild (force) …", flush=True)
        rebuilt, detail, tb = _force_rebuild(spark)
        print(f"  {_tag(rebuilt)}  fixture rebuild  ({detail})")
        if tb:
            print(tb)
        passes += int(rebuilt)
        fails += int(not rebuilt)
        if rebuilt:
            for path in runners:
                name = os.path.basename(path)
                print(f"  running {name} …", flush=True)
                rok, detail, tb = _run_runner(path, spark)
                print(f"  {_tag(rok)}  {name}  ({detail})")
                if tb:
                    print(tb)  # full stack trace on failure
                passes += int(rok)
                fails += int(not rok)
        else:
            print("  (skipping the runners — the fixture didn't build)")

    # ── Tally (components: pytest, fixture rebuild, each runner) ──────────────
    print(f"\n{'-' * 52}")
    headline = "ALL PASS" if fails == 0 else f"{fails} FAILED"
    print(f"{headline} — {passes} ok, {fails} failed, {skips} skipped")
    return 1 if fails else 0


# Run as a script (`python run_all_tests.py`) OR via the Databricks run-button / a notebook cell
# (those inject a `spark` global → the Spark layer runs); inert on plain import.
if __name__ == "__main__" or "dbutils" in globals() or "spark" in globals():
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821 — Databricks-injected
    except NameError:
        _spark = None
    _exit_code = main(_spark)
    # Raise SystemExit ONLY on failure (non-zero), so a failing script exits non-zero / a failing
    # Databricks cell errors out. On success, fall through (the process still exits 0) — raising
    # SystemExit(0) would render as an alarming "SystemExit: 0" in a Databricks notebook.
    if _exit_code:
        raise SystemExit(_exit_code)
