"""Compare two export runs of the LoopEval transition cohort, user by user (local).

Written for the move from LoopEval ETL data version 19 to 32 (2026-10-14): the
trial on the current ETL is checked against the earlier run of the same users.
For every user exported in both runs the script reads the cohort table's
per-export counts (glucose samples, doses, carbs, automatic and manual boluses,
bolus recommendations, loop decisions, the CGM ratio to the pipeline, carb
visibility lags) from each run and reports the change; then the ETL's own
per-person counts from the candidate's manifests, listing the keys the
baseline's manifests do not carry (the rules the newer data version added:
second-app drops, pump-history re-import drops, unrecorded boluses restored,
schedule-shift days, sensors, late-received readings) so a run shows which of
them fired and for whom; then which of the newer file set's sidecars and
therapy keys each candidate export carries.

Writes run_comparison.csv beside the candidate run (one row per shared user,
alias-keyed, de-identified) and prints the per-user table and an aggregate.

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/compare_export_runs.py <baseline_run_folder> <candidate_run_folder>
"""

import argparse
import json
import os

import pandas as pd

# =============================================================================
# What is compared
# =============================================================================
# Per-export counts in cohort_windows.csv (both runs).
COMPARED_COLUMNS = [
    "n_glucose", "n_doses", "n_carbs", "n_bolus_automatic", "n_bolus_manual", "n_bolus_recommendations",
    "n_loop_decisions", "cgm_ratio_vs_pipeline", "carb_visibility_lag_median_min", "carb_visibility_lag_p90_min",
]
# Sidecars and therapy keys the newer ETL writes (data versions 20-32).
NEWER_FILES = ["cgm_quality.csv", "dose_doubt.csv", "carb_revisions.json"]
NEWER_THERAPY_KEYS = ["scheduleShifts", "maxBolusTimeline", "maxBasalRateTimeline", "suspendThresholdTimeline"]
DECISION_COLUMN_ADDED = "bolus_in_progress"


def exported_users(run_folder):
    """The cohort table rows of the users this run exported, indexed by alias."""
    table = pd.read_csv(os.path.join(run_folder, "cohort_windows.csv"))
    return table[table.export_status == "ok"].set_index("user_alias")


def manifest_counts(run_folder, alias):
    """The ETL's own counts dictionary from one user's manifest (empty if absent)."""
    path = os.path.join(run_folder, alias, "_export_manifest.json")
    if not os.path.exists(path):
        return {}
    return json.load(open(path, encoding="utf-8")).get("counts", {}) or {}


def data_version(run_folder):
    manifest = json.load(open(os.path.join(run_folder, "run_manifest.json"), encoding="utf-8"))
    return manifest.get("loopeval_data_version"), manifest.get("loopeval_commit")


def newer_file_flags(run_folder, alias):
    """Which of the newer sidecars, therapy keys, and the decision column this export carries."""
    user_dir = os.path.join(run_folder, alias)
    flags = {f"has_{name}": os.path.exists(os.path.join(user_dir, name)) for name in NEWER_FILES}
    therapy_path = os.path.join(user_dir, "therapy.json")
    therapy = json.load(open(therapy_path, encoding="utf-8")) if os.path.exists(therapy_path) else {}
    flags.update({f"therapy_{key}": len(therapy.get(key) or []) for key in NEWER_THERAPY_KEYS})
    decision_path = os.path.join(user_dir, "decision_times.csv")
    header = open(decision_path, encoding="utf-8").readline().strip().split(",") if os.path.exists(decision_path) else []
    flags[f"decision_times_has_{DECISION_COLUMN_ADDED}"] = DECISION_COLUMN_ADDED in header
    return flags


def compare(baseline_folder, candidate_folder):
    baseline, candidate = exported_users(baseline_folder), exported_users(candidate_folder)
    shared = sorted(set(baseline.index) & set(candidate.index))
    rows = []
    for alias in shared:
        row = {"user_alias": alias, "tier": candidate.loc[alias, "tier"]}
        for column in COMPARED_COLUMNS:
            before, after = baseline.loc[alias, column], candidate.loc[alias, column]
            row[f"{column}_baseline"] = before
            row[f"{column}_candidate"] = after
            row[f"{column}_delta"] = after - before if pd.notna(before) and pd.notna(after) else None
        baseline_keys = set(manifest_counts(baseline_folder, alias))
        for key, value in manifest_counts(candidate_folder, alias).items():
            if key not in baseline_keys:
                row[f"etl_{key}"] = value
        row.update(newer_file_flags(candidate_folder, alias))
        rows.append(row)
    return pd.DataFrame(rows), shared, set(baseline.index) - set(candidate.index), set(candidate.index) - set(baseline.index)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("baseline", help="run folder of the earlier export")
    parser.add_argument("candidate", help="run folder of the export to check")
    parser.add_argument("--out", default=None, help="CSV path (default: run_comparison.csv beside the candidate)")
    arguments = parser.parse_args()

    comparison, shared, only_baseline, only_candidate = compare(arguments.baseline, arguments.candidate)
    out = arguments.out or os.path.join(arguments.candidate, "run_comparison.csv")
    comparison.to_csv(out, index=False)

    pd.set_option("display.width", 220, "display.max_columns", 60)
    print(f"baseline  {os.path.basename(arguments.baseline)}: data version {data_version(arguments.baseline)}")
    print(f"candidate {os.path.basename(arguments.candidate)}: data version {data_version(arguments.candidate)}")
    print(f"users in both: {len(shared)}; only in baseline: {len(only_baseline)}; only in candidate: {len(only_candidate)}")
    if comparison.empty:
        print("nothing to compare")
        return
    print("\nper-user change (candidate minus baseline):")
    print(comparison[["user_alias", "tier"] + [f"{c}_delta" for c in COMPARED_COLUMNS]].to_string(index=False))
    print("\nthe ETL's own counts that only the candidate's data version writes (per user):")
    etl_columns = [c for c in comparison.columns if c.startswith("etl_")]
    print(comparison[["user_alias"] + etl_columns].to_string(index=False))
    print("\nnewer sidecars, therapy keys (entry counts), and the decision column:")
    flag_columns = [c for c in comparison.columns if c.startswith(("has_", "therapy_", "decision_times_has_"))]
    print(comparison[["user_alias"] + flag_columns].to_string(index=False))
    print("\naggregate: users on which each new rule fired (count > 0):")
    fired = {c: int((pd.to_numeric(comparison[c], errors="coerce").fillna(0) > 0).sum()) for c in etl_columns}
    print(pd.Series(fired).to_string())
    print(f"\nwritten: {out}")


if __name__ == "__main__":
    main()
