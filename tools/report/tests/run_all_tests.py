#!/usr/bin/env python3
"""Run every test of the report tools and print a tally.

    python3 tools/report/tests/run_all_tests.py

Each test_*.py in this folder is a `__main__` that calls its test functions (no pytest,
STYLE.md section 5); this runner runs each as its own process under the interpreter that
runs it, the system python3 by default. The report tools run locally, so they have this
runner of their own; FDA_real_world_data/testing/run_all_tests.py runs on Databricks.
Two files need Quarto (test_live_render.py, and the parity test, which also needs the
Drive mount); their tests skip when it is absent. Exit code 1 when any file fails.
"""

import glob
import os
import subprocess
import sys

TESTS_FOLDER = os.path.dirname(os.path.abspath(__file__))


def main():
    failed, tally = [], []
    for path in sorted(glob.glob(os.path.join(TESTS_FOLDER, "test_*.py"))):
        name = os.path.basename(path)
        completed = subprocess.run([sys.executable, path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        summary = [line for line in completed.stdout.splitlines() if " passed, " in line]
        skipped = [line.strip() for line in completed.stdout.splitlines() if line.strip().startswith("skip")]
        if completed.returncode != 0:
            failed.append(name)
            print("FAIL  %s\n%s" % (name, completed.stdout))
        else:
            print("ok    %s" % (summary[-1] if summary else name))
            for line in skipped:
                print("        " + line)
        tally.append(name)
    print("\n%d test file(s) run, %d failed%s" % (len(tally), len(failed), ": " + ", ".join(failed) if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
