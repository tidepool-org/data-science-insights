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
    which passes no argv: set os.environ["ONLY"] in a driver cell, then runpy
    this file).

Run on Databricks.
"""

import contextlib
import glob
import io
import os
import runpy
import sys
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

# Precompute (file, group, name) + per-group totals so the live group header can
# show its size before any of its tests have run.
metas, group_total = [], {}
for f in test_files:
    rel = os.path.relpath(f, test_dir)
    group = os.path.dirname(rel) or "."
    metas.append((f, group, os.path.basename(f)[:-3]))  # strip ".py"
    group_total[group] = group_total.get(group, 0) + 1

# Run each test and render its bar immediately. results: (group, name, ok, elapsed, error)
results = []
current_group = None
for test_file, group, name in metas:
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

    results.append((group, name, ok, elapsed, error))

# --- One-line overall bar + tally ---
n_pass = sum(1 for r in results if r[2])
n_fail = len(results) - n_pass
emit(f"\n{GREEN}{'█' * n_pass}{RED}{'█' * n_fail}{RESET}")
tally_color = GREEN if n_fail == 0 else RED
emit(f"{tally_color}{BOLD}{n_pass} passed, {n_fail} failed, {len(results)} total{RESET}")

# --- Final recap: which tests failed ---
if n_fail:
    emit(f"\n{RED}{BOLD}Failed tests ({n_fail}):{RESET}")
    for group, name, ok, elapsed, error in results:
        if not ok:
            emit(f"  {RED}✗ {group}/{name}{RESET}  {DIM}{error}{RESET}")
    # Signal failure (non-zero exit) without dumping a traceback over the recap.
    sys.exit(1)
