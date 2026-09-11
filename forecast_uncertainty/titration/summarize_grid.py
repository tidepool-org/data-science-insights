"""Summarize a titration grid (titration/run_titration.py output): per rule, how often the gate bound, the share of the
recommendation it allowed, and the outcomes against the shipped arm paired by scenario; the forced-dose arms; and the
interval's coverage of the true glucose by arm and horizon. Prints only; the numbers that matter go to project_history.md.
  python titration/summarize_grid.py outputs/titration/runs/<name>
"""
import sys

import numpy as np
import pandas as pd

out = sys.argv[1] if len(sys.argv) > 1 else sys.exit(__doc__)
runs = pd.read_csv(f"{out}/runs.csv"); cov = pd.read_csv(f"{out}/coverage.csv")
pd.set_option("display.width", 250)
shipped = runs[runs.arm == "shipped"].set_index("scenario_id")
policy_arms = [a for a in runs.arm.unique() if a != "shipped" and not a.startswith("forced")]
print("runs:", len(runs), "| arms:", len(runs.arm.unique()), "| scenarios:", shipped.shape[0])
rows = []
for arm in policy_arms:
    g = runs[runs.arm == arm].set_index("scenario_id").loc[shipped.index]
    rows.append({"arm": arm, "meal dose / recommended (median)": (g.applied_u / g.recommended_u).clip(upper=1).median(),
                 "autobolus / Loop's (sum ratio)": g.autobolus_applied_u.sum() / max(g.autobolus_loop_u.sum(), 1e-9) if "autobolus_applied_u" in g else np.nan,
                 "hypo share": g.any_hypo.mean(), "hypo share (shipped)": shipped.any_hypo.mean(), "min < 54 share": (g.min_bg < 54).mean(), "min < 54 (shipped)": (shipped.min_bg < 54).mean(),
                 "minutes < 70": g.minutes_below_70.mean(), "minutes < 70 (shipped)": shipped.minutes_below_70.mean(),
                 "minutes > 180": g.minutes_above_180.mean(), "minutes > 180 (shipped)": shipped.minutes_above_180.mean(),
                 "minimum glucose": g.min_bg.mean(), "insulin 6 h (U)": (g.insulin_bolus_u + g.insulin_basal_u).mean(),
                 "insulin 6 h shipped (U)": (shipped.insulin_bolus_u + shipped.insulin_basal_u).mean()})
print("\nper policy arm against shipped (means over scenarios unless noted):")
print(pd.DataFrame(rows).set_index("arm").round(2).to_string())
for factor in [c for c in ("patient", "sensor", "unannounced_grams") if c in runs.columns and runs[c].nunique() > 1]:
    print(f"\nby {factor}: hypo share / minutes in range / minutes > 180, per arm")
    print(runs.groupby(["arm", factor]).agg(hypo=("any_hypo", "mean"), tir=("minutes_in_range", "mean"), tar=("minutes_above_180", "mean")).round(2).unstack(factor).to_string())
print("\nforced-dose arms and shipped, means over scenarios:")
forced = runs[runs.arm.str.startswith("forced") | (runs.arm == "shipped")]
print(forced.groupby("arm")[["applied_u", "min_bg", "minutes_below_70", "minutes_in_range", "minutes_above_180"]].mean().round(1).to_string())
print("\ncoverage of the TRUE glucose by arm and horizon (share inside the interval issued for the applied dose):")
print(cov.groupby(["arm", "horizon_min"]).covered_true.mean().unstack("horizon_min").round(2).to_string())
print("\nnegative or sub-40 true minima (the truth model has no glucose floor):", int((runs.min_bg < 40).sum()), "runs;", runs[runs.min_bg < 40].arm.value_counts().to_dict())
