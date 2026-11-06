"""The PLN-1008 cohort flow (§7.1 cohort and day inclusion), counted from the cohort table.

Execution plan 2026-10-27, Step 3 items 2 and 3, applied to the NMA on 2026-11-05
(decision D23). The flow is counted from nma_user_day_analysis_ready's criterion
columns through the FDA flow machinery (FDA_real_world_data/analysis/utils/
cohort_flow.py), so each stage applies exactly the predicate the analyses apply, is
split by diagnosis group, and can be written beside the issued vintage's counts.

Stages, cumulative, in user-days and users:

  Loop user-days (the day universe)        every row
  Loop version gate                        passes_loop_version_gate
  CGM coverage (day eligible)              day_eligible
  User with ≥ 10 eligible days             user_eligible
  Age gate                                 passes_age_gate
  Analysis cohort                          in_analysis_cohort (equals the stage above)
  In a delivery-strategy contrast          in_strategy_contrast (§8.2 and §8.4 only)

Written to outputs/nma/analysis_7_1/:
  table_7_1_cohort_flow.csv                 users by stage and diagnosis group (all, primary, each group)
  table_7_1_cohort_flow_user_days.csv       the same in user-days
  table_7_1_cohort_flow_primary.csv         the primary cohort's users, corrected vintage beside the
                                            issued one with the difference (blank without --issued_csv)
  table_7_1_cohort_flow_long.csv            the long form, both vintages where given
  table_7_1_arms_by_diagnosis_group.csv     the analysis cohort's user-days and users in each arm
                                            (the three nested CE=0 arms, CE>0, CE≥3/BE≥3), by group

The issued vintage's snapshot predates the cohort columns (its version gate was a WHERE
clause and its age gate a loader filter), so when --issued_csv is given the criterion
columns are derived for it: the version gate passed by construction, the age gate from
age_years, the diagnosis group from diagnosis_type without the LADA label. The note is
printed and written into the long table's `vintage` column as 'issued (derived)'.

Run locally off the snapshot (the normal path):
  python analysis/analysis_nma_7-1_cohort_flow.py
  python analysis/analysis_nma_7-1_cohort_flow.py --issued_csv <the vintage snapshot>
"""

import argparse
import os
import shutil
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from utils.nma_loading import (  # noqa: E402
    CLASSIFICATIONS,
    COMPARATOR_FLAG,
    COMPARATOR_LABEL,
    CRITERION_COLUMNS,
    DIAGNOSIS_GROUPS,
    HIGH_MA_FLAG,
    HIGH_MA_LABEL,
    MIN_AGE_YEARS,
    PRIMARY_DIAGNOSIS_GROUPS,
    analysis_dir,
    as_bool,
    assert_cohort_predicates,
    default_analysis_ready_csv,
    load_fda_cohort_flow,
)

ANALYSIS_READY_TABLE = "dev.fda_510k_rwd.nma_user_day_analysis_ready"
OUTPUT_DIR = os.path.join(analysis_dir(), "outputs", "nma", "analysis_7_1")
ARMS = CLASSIFICATIONS + [(COMPARATOR_FLAG, COMPARATOR_LABEL), (HIGH_MA_FLAG, HIGH_MA_LABEL)]


def stages(flow):
    """The §7.1 stages as the FDA flow machinery's Stage records."""
    return [
        flow.Stage("Loop user-days (the day universe)",
                   "Every classified user-day of the day table (loop_recommendations)",
                   flow.every_row),
        flow.Stage("Loop version gate",
                   "A known Loop version below 3.4.0, or no version and a day before 2024-07-13",
                   lambda f: as_bool(f["passes_loop_version_gate"])),
        flow.Stage("CGM coverage (day eligible)",
                   "CGM coverage ≥ 70% on the day",
                   lambda f: as_bool(f["day_eligible"])),
        flow.Stage("User with ≥ 10 eligible days",
                   "The user contributed at least 10 eligible user-days",
                   lambda f: as_bool(f["user_eligible"])),
        flow.Stage("Age gate",
                   f"Age ≥ {MIN_AGE_YEARS} y on the day, or unknown",
                   lambda f: as_bool(f["passes_age_gate"])),
        flow.Stage("Analysis cohort",
                   "in_analysis_cohort: every criterion above (the rows every §8 analysis starts from)",
                   lambda f: as_bool(f["in_analysis_cohort"])),
        flow.Stage("In a delivery-strategy contrast",
                   "A delivery strategy on the day (autobolus or temporary basal); §8.2 and §8.4",
                   lambda f: as_bool(f["in_strategy_contrast"])),
    ]


def derive_issued_columns(pdf):
    """The criterion and group columns an issued-vintage snapshot lacks, derived as that
    vintage applied them: the version gate was a WHERE clause (every row passed), the age
    gate the loader's floor on age_years, the diagnosis group the resolved type without
    the LADA label."""
    out = pdf.copy()
    if "passes_loop_version_gate" not in out.columns:
        out["passes_loop_version_gate"] = True
    if "passes_age_gate" not in out.columns:
        age = pd.to_numeric(out.get("age_years"), errors="coerce")
        out["passes_age_gate"] = age.isna() | (age >= MIN_AGE_YEARS)
    if "diagnosis_group" not in out.columns:
        diagnosis = out.get("diagnosis_type", pd.Series(index=out.index, dtype=object))
        out["diagnosis_group"] = diagnosis.map(
            lambda d: "unknown" if pd.isna(d) else ("type1" if d == "type1" else ("type2" if d == "type2" else "other"))
        )
    if "delivery_strategy" not in out.columns:
        out["delivery_strategy"] = None
    in_cohort = (as_bool(out["passes_loop_version_gate"]) & as_bool(out["day_eligible"])
                 & as_bool(out["user_eligible"]) & as_bool(out["passes_age_gate"]))
    out["in_analysis_cohort"] = in_cohort
    out["in_strategy_contrast"] = in_cohort & out["delivery_strategy"].notna()
    return out


def arms_by_group(pdf, flow):
    """The analysis cohort's user-days and users in each arm, by diagnosis group."""
    cohort = pdf[as_bool(pdf["in_analysis_cohort"])]
    rows = []
    for arm_flag, arm_label in ARMS:
        member = cohort[as_bool(cohort[arm_flag])]
        groups = [(flow.ALL_GROUPS_LABEL, member), (flow.PRIMARY_LABEL, member[member["diagnosis_group"].isin(PRIMARY_DIAGNOSIS_GROUPS)])]
        groups += [(g, member[member["diagnosis_group"] == g]) for g in DIAGNOSIS_GROUPS]
        for group, subset in groups:
            rows.append({"arm": arm_label, "group": group,
                         "n_users": int(subset["_userId"].nunique()), "n_user_days": int(len(subset))})
    return pd.DataFrame(rows)


def write_csv(frame, output_dir, name):
    assert "_userId" not in frame.columns, f"{name}: a per-user column must not be written"
    path = os.path.join(output_dir, name)
    frame.to_csv(path, index=False)
    print(f"  Saved: {path}")


def run(csv_path=None, issued_csv=None, output_dir=OUTPUT_DIR):
    flow = load_fda_cohort_flow()
    if csv_path is None:
        csv_path = default_analysis_ready_csv(ANALYSIS_READY_TABLE)
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 7.1: the PLN-1008 cohort flow")
    print("=" * 60)
    print(f"\n1. Reading the cohort table: {csv_path}")
    pdf = pd.read_csv(csv_path)
    assert_cohort_predicates(pdf)
    current = flow.count_flow(pdf, stages(flow), unit="user_days")
    flow.assert_monotone(current)
    flow.assert_monotone(current, value="n_user_days")
    by_stage = current[current["group"] == flow.ALL_GROUPS_LABEL].set_index("stage")["n_users"]
    assert by_stage["Age gate"] == by_stage["Analysis cohort"], "in_analysis_cohort is the four criteria"

    issued = None
    if issued_csv is not None:
        print(f"\n2. Reading the issued vintage's snapshot: {issued_csv}")
        issued_frame = derive_issued_columns(pd.read_csv(issued_csv))
        issued = flow.count_flow(issued_frame, stages(flow), unit="user_days")
        print("   criterion columns derived for the issued vintage (version gate by construction, "
              "age gate from age_years, diagnosis group without the LADA label)")

    print("\n3. Writing the flow tables...")
    write_csv(flow.widen_flow(current), output_dir, "table_7_1_cohort_flow.csv")
    write_csv(flow.widen_flow(current, value="n_user_days"), output_dir, "table_7_1_cohort_flow_user_days.csv")
    write_csv(flow.side_by_side(current, issued, group=flow.PRIMARY_LABEL), output_dir,
              "table_7_1_cohort_flow_primary.csv")
    long = current.assign(vintage="current")
    if issued is not None:
        long = pd.concat([long, issued.assign(vintage="issued (derived)")], ignore_index=True)
    write_csv(long, output_dir, "table_7_1_cohort_flow_long.csv")
    write_csv(arms_by_group(pdf, flow), output_dir, "table_7_1_arms_by_diagnosis_group.csv")
    print("\n" + flow.widen_flow(current).to_string(index=False))
    return {"current": current, "issued": issued}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_path", default=None, help="the analysis-ready snapshot (default outputs/nma/<table>.csv)")
    parser.add_argument("--issued_csv", default=None, help="the issued vintage's snapshot, for the side-by-side column")
    parser.add_argument("--output_dir", default=OUTPUT_DIR)
    args = parser.parse_args()
    run(csv_path=args.csv_path, issued_csv=args.issued_csv, output_dir=args.output_dir)
