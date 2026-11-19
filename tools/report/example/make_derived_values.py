#!/usr/bin/env python3
"""Compute the example's derived values from the cohort flow of one data vintage.

The analysis-side half of the report tools plan, section 4.7: a percentage of two counts
is never computed in the document. This script computes it, writes
`example_derived.csv`, and records the hash of the file it read, so the document fails
to render if the flow changes and this script is not re-run.

    python3 tools/report/example/make_derived_values.py tools/report/example/data/vintage_a

Every value under tools/report/example/data/ is invented.
"""

import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from report.derived_values import write_derived_values  # noqa: E402

FLOW_FILE = "example_flow.csv"
DERIVED_FILE = "example_derived.csv"
FIRST_STAGE = "All users"
FINAL_STAGE = "Final cohort"


def main(vintage_folder):
    flow_path = os.path.join(vintage_folder, FLOW_FILE)
    with open(flow_path, encoding="utf-8", newline="") as handle:
        users_by_stage = {row["stage"]: int(row["n_users"]) for row in csv.DictReader(handle)}
    share_retained = 100.0 * users_by_stage[FINAL_STAGE] / users_by_stage[FIRST_STAGE]
    rows = [{"label": "share_retained", "value": "%.6f" % share_retained,
             "description": "users of the final cohort as a percentage of all users"}]
    write_derived_values(rows, os.path.join(vintage_folder, DERIVED_FILE), inputs=[flow_path])
    print("wrote %s" % os.path.join(vintage_folder, DERIVED_FILE))


if __name__ == "__main__":
    main(sys.argv[1])
