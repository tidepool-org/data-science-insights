"""Does the issued-vintage column of Table 6.3a reproduce the issued table?

Local, no Spark. The corrected run's table_6_3a_cohort_flow.csv carries an
`n_users_issued` column counted from the issued-vintage cohort table
(transition_cohort_issued, the `_box080` copies under the gates of their
time). RPT-1001 Rev 02's Table 6.3a of record is a file on Drive. This
script reads both and asserts, stage by stage where both carry a count,
that the issued column equals the file of record, so the tie-out the
sensitivity run could not make (execution plan Step 3 item 4) is made when
the download is filed.

  python production_runs/compare_flow_to_issued.py \\
      --flow "<download>/cohort_6_3/table_6_3a_cohort_flow.csv" \\
      --issued "<Drive>/outputs_202_08_05/cohort_6_3_box080/table_6_3a_cohort_flow.csv"

Exit status 0 when every shared stage agrees, 1 otherwise. Prints the
stages side by side; the numbers stay in the terminal and on Drive.
"""

import argparse
import sys

import pandas as pd


def compare(flow, issued):
    """The shared stages with both counts and whether they agree."""
    merged = flow[["stage", "n_users_issued"]].merge(
        issued[["stage", "n_users"]].rename(columns={"n_users": "n_users_of_record"}),
        on="stage", how="inner",
    )
    merged = merged[merged["n_users_issued"].notna()].copy()
    merged["agrees"] = merged["n_users_issued"].astype(int) == merged["n_users_of_record"].astype(int)
    return merged


def main(flow_path, issued_path):
    flow = pd.read_csv(flow_path)
    issued = pd.read_csv(issued_path)
    assert "n_users_issued" in flow.columns, f"{flow_path} carries no issued column"
    table = compare(flow, issued)
    assert len(table) > 0, "no stage is shared between the two tables"
    print(table.to_string(index=False))
    disagreeing = table.loc[~table["agrees"], "stage"].tolist()
    if disagreeing:
        print(f"\nDISAGREE at: {', '.join(disagreeing)}")
        return False
    print(f"\nAGREE: the issued-vintage column reproduces the table of record at {len(table)} stage(s)")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", required=True, help="the corrected run's table_6_3a_cohort_flow.csv")
    parser.add_argument("--issued", required=True, help="the issued Table 6.3a of record")
    args = parser.parse_args()
    sys.exit(0 if main(args.flow, args.issued) else 1)
