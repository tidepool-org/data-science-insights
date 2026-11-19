#!/usr/bin/env python3
"""Run the report tools on the invented example, start to finish, and say what to open.

    python3 tools/report/example/run_example.py [--output <new folder>]

The six steps, each a command a person could type (the script prints it):

    1. Render the example on data vintage A.                        -> a Word draft and its render record
    2. Render it on vintage B, compared with step 1.                -> the same draft with every changed number flagged
    3. Play a reviewer: edit the step-1 draft with tracked changes. -> an edited Word file
    4. Back-port the reviewer's edits and run the round trip.       -> the next source, the edits file, the report
    5. Break one input and render again.                            -> nothing is published
    6. Run the issue checks by hand on the step-2 draft.

Everything is written under one output folder, a new temporary folder unless --output is
given. The folder must lie outside the repository and outside Drive: a render record holds
result values, and Quarto never runs beside a reviewed file. Step 3 uses tools/docx_edit.py
in place of a person editing in Word; open the step-1 draft in Word, edit it, and run step
4 on your own copy to see the same thing with real edits. Every value is invented.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

EXAMPLE_FOLDER = os.path.dirname(os.path.abspath(__file__))
REPORT_FOLDER = os.path.dirname(EXAMPLE_FOLDER)
TOOLS_FOLDER = os.path.dirname(REPORT_FOLDER)
AUTHOR = "Example Author"
REVIEWER = "Example Reviewer"


def run(step, command, expect=0):
    """Print one command, run it, and stop the example when its exit code is not the expected one."""
    print("\n" + "=" * 100 + "\n" + step + "\n" + "-" * 100)
    print("$ " + " ".join('"%s"' % part if " " in part else part for part in command) + "\n")
    sys.stdout.flush()   # keep this header above the command's own output when the output is piped
    code = subprocess.call(command)
    if code != expect:
        sys.exit("\nstep failed: exit code %d, expected %d" % (code, expect))
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", help="a new folder outside the repository and Drive; a temporary folder when omitted")
    arguments = parser.parse_args()
    output = os.path.realpath(arguments.output) if arguments.output else os.path.realpath(tempfile.mkdtemp(prefix="report_tools_example_"))
    if os.path.isdir(output) and os.listdir(output):
        sys.exit("%s is not empty; give a new folder" % output)
    drafts, scratch = os.path.join(output, "drafts"), os.path.join(output, "scratch")
    os.makedirs(drafts)
    python = sys.executable
    driver = os.path.join(REPORT_FOLDER, "render_document.py")
    vintage_a, vintage_b = os.path.join(EXAMPLE_FOLDER, "data", "vintage_a"), os.path.join(EXAMPLE_FOLDER, "data", "vintage_b")
    draft = os.path.join(drafts, "example_report (draft 2).docx")
    record = os.path.join(drafts, "example_report (draft 2).render")
    refreshed = os.path.join(drafts, "example_report (draft 2, refreshed).docx")

    run("1. Render the example on data vintage A",
        [python, driver, "example_report", "--data-root", vintage_a, "--vintage", "example_vintage_a", "--author", AUTHOR,
         "--publish-to", drafts, "--output-suffix", " (draft 2)", "--scratch", os.path.join(scratch, "render_a")])

    run("2. Render it on vintage B, compared with step 1: every changed number is coloured by class, and the ones that need attention are tracked changes",
        [python, driver, "example_report", "--data-root", vintage_b, "--vintage", "example_vintage_b", "--author", AUTHOR,
         "--compare-to", record, "--publish-to", drafts, "--output-suffix", " (draft 2, refreshed)",
         "--scratch", os.path.join(scratch, "render_b")])

    edited = os.path.join(output, "review", "example_report (draft 2, edited by the reviewer).docx")
    os.makedirs(os.path.dirname(edited))
    run("3. Play a reviewer: seven tracked edits of the step-1 draft (a person would make them in Word)",
        [python, os.path.join(TOOLS_FOLDER, "docx_edit.py"), draft, edited, "--edits", os.path.join(EXAMPLE_FOLDER, "review_edits.json"),
         "--author", REVIEWER])

    run("4. Back-port the reviewer's edits into the source and run the round trip (exit code 3: some edits are refused, by design)",
        [python, os.path.join(REPORT_FOLDER, "backport_edits.py"), edited, "--render-record", record,
         "--next-source", "example_report (draft 3).qmd", "--work-folder", os.path.join(output, "backport"), "--round-trip"], expect=3)

    broken = os.path.join(output, "broken_data")
    shutil.copytree(vintage_a, broken)
    flow = os.path.join(broken, "example_flow.csv")
    with open(flow, encoding="utf-8") as handle:
        text = handle.read()
    with open(flow, "w", encoding="utf-8") as handle:
        handle.write(text.replace("Final cohort,40", "Final cohort,41"))
    run("5. Break one input (the final cohort's count) and render again: the render fails and nothing is published (exit code 3)",
        [python, driver, "example_report", "--data-root", broken, "--vintage", "broken", "--author", AUTHOR,
         "--publish-to", drafts, "--output-suffix", " (from broken data)", "--scratch", os.path.join(scratch, "render_broken")], expect=3)

    run("6. Run the issue checks by hand on the step-2 draft",
        [python, os.path.join(REPORT_FOLDER, "verify_docx.py"), refreshed, "--render-record",
         os.path.join(drafts, "example_report (draft 2, refreshed).render")])

    print("\n" + "=" * 100 + "\nDone. What to open, all under\n  %s\n" % output)
    print("  drafts/example_report (draft 2).docx                 the draft: every number printed by a cell reference")
    print("  drafts/example_report (draft 2).render/cells.csv     the sidecar: one row per printed value, with its file, row, and column")
    print("  drafts/example_report (draft 2, refreshed).docx      the refresh: changed numbers in colour; the ones that need attention as tracked changes")
    print("  drafts/example_report (draft 2, refreshed).render/changes.csv   every reference with its old value, new value, and class")
    print("  review/example_report (draft 2, edited by the reviewer).docx    the reviewer's tracked edits")
    print("  backport/backport_report.csv                         each edit, applied or refused with the reason")
    print("  backport/example_report (draft 3).qmd                the next source: the applied edits, and no number changed")
    print("  backport/edits_draft3.json                           the same edits for make_next_draft.py")
    print("\nThe scratch folders under scratch/ are kept for inspection and can be deleted.")


if __name__ == "__main__":
    main()
