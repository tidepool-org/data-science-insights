"""
Run every test_*.py under this directory (recurses into subdirectories,
e.g. data_staging/, integration/, analysis/, simulation/).

Default output is compact: one colored bar per test (green = pass, red = fail),
grouped by subdirectory, streamed live as each test finishes. Each test's own
stdout/stderr is captured and hidden; a failing test shows its error plus the
tail of its captured output, and every failure is listed again at the very end.

Verbose mode streams each test's full output live (the old behavior). Enable it
any of three ways:
  - flip VERBOSE below to True, or
  - set env var  VERBOSE=1, or
  - pass  --verbose  /  -v  on the command line.

To run a subset, filter by substring of the test's relative path (comma-separate
for several); execution stays serial either way:
  - pass  --only loop_recommendations,guardrail_flags  on the command line, or
  - set env var  ONLY=loop_recommendations  (handy for Databricks Run-file,
    which passes no argv: set os.environ["ONLY"] in a driver cell, then
    `_ = runpy.run_path(<this file>, run_name="__main__")` — assign the result,
    or the notebook displays the returned globals after the tally).

To rerun only what failed last time, pass  --failed  (or set env var
RERUN_FAILED=1, or flip RERUN_FAILED below). Every run records its failed
tests' relative paths in the driver's temp dir
(fda_run_all_tests_last_failures.txt), and --failed selects exactly those
files. The record is of the last run, whatever its selection. A cluster
restart clears the temp dir, in which case --failed says so and runs nothing.

Run on Databricks.
"""

import contextlib
import glob
import io
import os
import runpy
import sys
import tempfile
import time

test_dir = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data/testing"
sys.path.insert(0, test_dir)

# Compact bars by default; see module docstring for how to turn on verbose.
VERBOSE = False
VERBOSE = VERBOSE or os.environ.get("VERBOSE", "") not in ("", "0", "false", "False")
VERBOSE = VERBOSE or any(a in ("-v", "--verbose") for a in sys.argv[1:])


def _parse_only():
    """Substring filters from  --only a,b  /  --only=a,b  / env var ONLY=a,b."""
    raw = [os.environ.get("ONLY", "")]
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a == "--only" and i + 1 < len(argv):
            raw.append(argv[i + 1])
        elif a.startswith("--only="):
            raw.append(a[len("--only="):])
    return [s for chunk in raw for s in chunk.split(",") if s]


ONLY = _parse_only()

# Rerun only the tests that failed last time. Every run writes its failures' relative paths to
# LAST_FAILURES_FILE in the driver's temp dir (an empty file when the run was green); --failed
# selects exactly those files. A cluster restart clears the temp dir, and the run then says so.
RERUN_FAILED = False
RERUN_FAILED = RERUN_FAILED or os.environ.get("RERUN_FAILED", "") not in ("", "0", "false", "False")
RERUN_FAILED = RERUN_FAILED or "--failed" in sys.argv[1:]
LAST_FAILURES_FILE = os.path.join(tempfile.gettempdir(), "fda_run_all_tests_last_failures.txt")

# ANSI styling (rendered in Databricks notebook output / terminals).
GREEN, RED, DIM, BOLD, RESET = "\033[92m", "\033[91m", "\033[2m", "\033[1m", "\033[0m"
BAR = "█" * 5

# Make output appear live: line-buffer stdout (best-effort) so bars and verbose
# test output stream as they happen rather than block-buffering until the end.
_reconfigure = getattr(sys.stdout, "reconfigure", None)
if _reconfigure is not None:
    try:
        _reconfigure(line_buffering=True)
    except (ValueError, OSError):
        pass
try:
    IS_TTY = sys.stdout.isatty()
except Exception:
    IS_TTY = False


def emit(s="", end="\n"):
    """print() that always flushes, so each bar shows the instant it's ready."""
    print(s, end=end, flush=True)


test_files = sorted(glob.glob(os.path.join(test_dir, "**", "test_*.py"), recursive=True))

if ONLY:
    test_files = [
        f for f in test_files
        if any(s in os.path.relpath(f, test_dir) for s in ONLY)
    ]
    emit(f"{DIM}--only {','.join(ONLY)}  →  {len(test_files)} test(s){RESET}")
    if not test_files:
        emit(f"{RED}{BOLD}No tests match.{RESET}")
        sys.exit(1)

if RERUN_FAILED:
    if not os.path.exists(LAST_FAILURES_FILE):
        emit(f"{RED}{BOLD}--failed: no record of a previous run at {LAST_FAILURES_FILE} "
             f"(a cluster restart clears it). Run the suite once first.{RESET}")
        sys.exit(1)
    with open(LAST_FAILURES_FILE) as fh:
        recorded = [os.path.normpath(ln.strip()) for ln in fh if ln.strip()]
    if not recorded:
        emit(f"{GREEN}{BOLD}--failed: the last run had no failures; nothing to rerun.{RESET}")
        sys.exit(0)
    by_rel = {os.path.normpath(os.path.relpath(f, test_dir)): f for f in test_files}
    test_files = [by_rel[r] for r in recorded if r in by_rel]
    emit(f"{DIM}--failed  →  {len(test_files)} test(s) recorded by the last run{RESET}")
    for r in recorded:
        if r not in by_rel:
            emit(f"  {RED}recorded but no longer present: {r}{RESET}")
    if not test_files:
        emit(f"{RED}{BOLD}None of the recorded failures exists as a file.{RESET}")
        sys.exit(1)

# Precompute (file, group, name) + per-group totals so the live group header can
# show its size before any of its tests have run.
metas, group_total = [], {}
for f in test_files:
    rel = os.path.relpath(f, test_dir)
    group = os.path.dirname(rel) or "."
    metas.append((f, group, os.path.basename(f)[:-3], rel))  # name strips ".py"
    group_total[group] = group_total.get(group, 0) + 1

# Run each test and render its bar immediately. results: (group, name, ok, elapsed, error)
results = []
current_group = None
for test_file, group, name, rel in metas:
    if group != current_group:
        current_group = group
        emit(f"\n{BOLD}{group}/{RESET}  {DIM}({group_total[group]}){RESET}")

    # On a real terminal, show the in-flight test, then overwrite it in place
    # with the resolved bar. Non-TTY (Databricks Run-file / notebook) skips this
    # and just streams the finished bar — flushed, so it still ticks live.
    if not VERBOSE and IS_TTY:
        emit(f"  {DIM}{BAR} {name}  running…{RESET}", end="")

    start = time.time()
    if VERBOSE:
        emit(f"  {DIM}── {name} ──{RESET}")
        try:
            runpy.run_path(test_file, run_name="__main__")
            ok, error = True, None
        except Exception as e:
            ok, error = False, f"{type(e).__name__}: {e}"
        output = ""  # already streamed live
    else:
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                runpy.run_path(test_file, run_name="__main__")
            ok, error = True, None
        except Exception as e:
            ok, error = False, f"{type(e).__name__}: {e}"
        output = buf.getvalue()
    elapsed = time.time() - start

    color = GREEN if ok else RED
    bar_line = f"  {color}{BAR}{RESET} {name}  {DIM}{elapsed:.1f}s{RESET}"
    if not VERBOSE and IS_TTY:
        emit("\r\033[K" + bar_line)  # carriage-return + clear-to-eol overwrites "running…"
    else:
        emit(bar_line)
    if not ok and not VERBOSE:
        emit(f"      {RED}{error}{RESET}")
        for ln in [l for l in output.splitlines() if l.strip()][-8:]:
            emit(f"      {DIM}{ln[:120]}{RESET}")

    results.append((group, name, ok, elapsed, error, rel))

# --- One-line overall bar + tally ---
n_pass = sum(1 for r in results if r[2])
n_fail = len(results) - n_pass
emit(f"\n{GREEN}{'█' * n_pass}{RED}{'█' * n_fail}{RESET}")
tally_color = GREEN if n_fail == 0 else RED
emit(f"{tally_color}{BOLD}{n_pass} passed, {n_fail} failed, {len(results)} total{RESET}")

# --- Record this run's failures for --failed (an empty file when green) ---
with open(LAST_FAILURES_FILE, "w") as fh:
    fh.write("".join(f"{rel}\n" for group, name, ok, elapsed, error, rel in results if not ok))

# --- Final recap: which tests failed ---
if n_fail:
    emit(f"\n{RED}{BOLD}Failed tests ({n_fail}):{RESET}")
    for group, name, ok, elapsed, error, rel in results:
        if not ok:
            emit(f"  {RED}✗ {group}/{name}{RESET}  {DIM}{error}{RESET}")
    # Signal failure (non-zero exit) without dumping a traceback over the recap.
    sys.exit(1)
