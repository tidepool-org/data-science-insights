"""Compare PtIDs in glycemic_endpoints_by_jaeb_id.csv (Analysis 8-6) vs.
autobolus_durability_by_jaeb_id.csv (Analysis 8-7).

Reports overlap and the directional differences. Run from the
FDA_real_world_data dir after both analyses have produced their outputs.
"""

from pathlib import Path

import pandas as pd

GLY_PATH = Path("outputs/analysis_8_6/glycemic_endpoints_by_jaeb_id.csv")
DUR_PATH = Path("outputs/analysis_8_7/autobolus_durability_by_jaeb_id.csv")


def main() -> None:
    gly = pd.read_csv(GLY_PATH)
    dur = pd.read_csv(DUR_PATH)

    gly_ids = set(gly["PtID"])
    dur_ids = set(dur["PtID"])
    both = gly_ids & dur_ids
    only_gly = gly_ids - dur_ids
    only_dur = dur_ids - gly_ids

    print(f"Glycemic (8-6): {len(gly_ids)} PtIDs  [{GLY_PATH}]")
    print(f"Durability (8-7): {len(dur_ids)} PtIDs  [{DUR_PATH}]")
    print(f"In both:                  {len(both)}")
    print(f"In glycemic but not dur:  {len(only_gly)}  → {sorted(only_gly)}")
    print(f"In durability but not gly: {len(only_dur)}  → {sorted(only_dur)}")


if __name__ == "__main__":
    main()
