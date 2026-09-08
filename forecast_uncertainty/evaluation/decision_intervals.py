"""Loop's dosing decisions seen through the prediction interval -- DESCRIPTIVE, on the temporal holdout.

Question (user, 2026-09-06): what is the prediction interval for the insulin doses Loop decides? How often does the
95% interval attached to a decision cross a glucose floor, and does the lower bound say anything about the hypos that
followed? This script does not evaluate a dosing rule causally: every outcome here happened under Loop's subsequent
actions (the intervention confound), so "blocked decisions" are described, not credited with avoided hypos.

Per loop decision (a 5-min series decision in the holdout with a displayed forecast):
  - Loop's decision: `recommended_bolus` from the dosing decision (> 0 = Loop's algorithm judged insulin warranted;
    with automatic bolus enabled Loop delivers a fraction of it), its own forecast's MINIMUM over the horizons (the
    quantity Loop's safety logic looks at), and IOB.
  - The interval: the MINIMUM of the 95% lower bound over the horizons up to DECISION_HORIZON_MIN, and the same for
    the centre.
  - The outcome: the realized minimum CGM over the next DECISION_HORIZON_MIN minutes (from the tick frame).
Outputs (in --out-dir): decision_intervals_summary.csv (shares and rates by decision class; no counts),
decision_intervals_calibration.csv (hypo rate by lower-bound bin), figures/26_decision_intervals.png.

Run from the project root:  python evaluation/decision_intervals.py --out-dir outputs_loop_displayed
Memory: one process; the holdout table (~1 GB) plus ten small tick frames.
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
sys.path.insert(0, PROJECT_ROOT)
import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from evaluation.residual_schema import HOLDOUT_TABLE, load_table  # noqa: E402
from run_residuals import DEFAULT_DATA_DIR, build_user_frame, load_streams, user_streams  # noqa: E402

DECISION_HORIZON_MIN = 180                 # the window the lower bound and the realized minimum are taken over
TICK_MIN = 5
GLUCOSE_FLOORS_MG_DL = (70, 85)            # "the 95% crosses X" thresholds reported
HYPO_MG_DL = 70                            # the outcome: realized minimum below this within the window
SEVERE_HYPO_MG_DL = 54
LOWER_BOUND_BINS = np.arange(30, 200, 10)  # calibration bins for the minimum lower bound
RECOMMENDED_BOLUS_BINS = [0, 1e-9, 0.5, 1.5, np.inf]
RECOMMENDED_BOLUS_LABELS = ["none recommended", "≤ 0.5 U", "0.5–1.5 U", "> 1.5 U"]
HOLDOUT_COLUMNS = ["_userId", "origin_index", "timestamp", "horizon_min", "cgm0", "iob0", "predicted", "centre",
                   "lower", "upper", "realized", "minutes_since_carb_entry"]


def auc(score, outcome):
    """Area under the ROC curve by the rank statistic (Mann-Whitney); higher score = more hypo-like."""
    score = np.asarray(score, dtype=float); outcome = np.asarray(outcome, dtype=bool)
    ok = ~np.isnan(score); score, outcome = score[ok], outcome[ok]
    if outcome.all() or (~outcome).all():
        return np.nan
    ranks = pd.Series(score).rank().to_numpy()
    n_pos, n_neg = outcome.sum(), (~outcome).sum()
    return (ranks[outcome].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def roc_curve(score, outcome):
    score = np.asarray(score, dtype=float); outcome = np.asarray(outcome, dtype=bool)
    ok = ~np.isnan(score); score, outcome = score[ok], outcome[ok]
    order = np.argsort(-score)
    tp = np.cumsum(outcome[order]) / outcome.sum()
    fp = np.cumsum(~outcome[order]) / (~outcome).sum()
    return np.concatenate([[0], fp]), np.concatenate([[0], tp])


def decisions_with_intervals(held, data_dir):
    """One row per holdout decision origin: Loop's recommended bolus, its forecast minimum, the interval's minimum lower
    bound and centre over the window, and the realized minimum CGM from the tick frame."""
    ticks = DECISION_HORIZON_MIN // TICK_MIN
    window = held[held["horizon_min"] <= DECISION_HORIZON_MIN]
    per_origin = (window.groupby(["_userId", "origin_index"], observed=True)
                  .agg(timestamp=("timestamp", "first"), cgm0=("cgm0", "first"), iob0=("iob0", "first"),
                       minutes_since_carb_entry=("minutes_since_carb_entry", "first"),
                       loop_min_forecast=("predicted", "min"), min_lower=("lower", "min"), min_centre=("centre", "min"),
                       n_horizons=("horizon_min", "size"))
                  .reset_index())
    full = per_origin["n_horizons"] == window["horizon_min"].nunique()
    per_origin = per_origin[full].drop(columns="n_horizons")
    streams = load_streams(data_dir)
    pieces = []
    for _, user in streams["users"].iterrows():
        mine = per_origin[per_origin["_userId"].astype(str) == str(user["_userId"])]
        if mine.empty:
            continue
        s = user_streams(streams, user["_userId"], user["tz_offset_min"])
        frame = build_user_frame(s["cgm"], s["carbs"], s["boluses"], s["dosing"])
        cgm = frame["cgm"]
        # realized minimum over the NEXT `ticks` ticks (origin excluded), needing most of the window observed
        future_min = cgm[::-1].rolling(ticks, min_periods=int(ticks * 0.8)).min()[::-1].shift(-1)
        lookup = pd.DataFrame({"timestamp": frame["timestamp"], "recommended_bolus": frame["recommended_bolus"],
                               "realized_min": future_min.to_numpy()})
        pieces.append(mine.merge(lookup, on="timestamp", how="left"))
    table = pd.concat(pieces, ignore_index=True)
    table = table.dropna(subset=["recommended_bolus", "realized_min", "min_lower"])
    table["decision"] = pd.cut(table["recommended_bolus"], RECOMMENDED_BOLUS_BINS, labels=RECOMMENDED_BOLUS_LABELS, right=True, include_lowest=True)
    table["insulin_recommended"] = table["recommended_bolus"] > 0
    table["hypo"] = table["realized_min"] < HYPO_MG_DL
    table["severe_hypo"] = table["realized_min"] < SEVERE_HYPO_MG_DL
    return table


def summarize(table):
    rows = []
    groups = [("all decisions", table), ("insulin recommended", table[table["insulin_recommended"]]),
              ("none recommended", table[~table["insulin_recommended"]])]
    groups += [(f"recommended {label}", table[table["decision"] == label]) for label in RECOMMENDED_BOLUS_LABELS[1:]]
    for name, g in groups:
        if g.empty:
            continue
        row = {"decision_class": name, "share_of_decisions": len(g) / len(table),
               "median_recommended_bolus_u": g["recommended_bolus"].median(),
               "median_cgm0": g["cgm0"].median(), "median_loop_min_forecast": g["loop_min_forecast"].median(),
               "median_min_lower_bound": g["min_lower"].median(), "median_min_centre": g["min_centre"].median(),
               "median_realized_min": g["realized_min"].median(),
               f"hypo_rate_lt{HYPO_MG_DL}": g["hypo"].mean(), f"severe_hypo_rate_lt{SEVERE_HYPO_MG_DL}": g["severe_hypo"].mean()}
        for floor in GLUCOSE_FLOORS_MG_DL:
            below = g["min_lower"] < floor
            row[f"share_lower_bound_below_{floor}"] = below.mean()
            row[f"share_loop_min_forecast_below_{floor}"] = (g["loop_min_forecast"] < floor).mean()
            row[f"hypo_rate_when_lower_bound_below_{floor}"] = g.loc[below, "hypo"].mean() if below.any() else np.nan
            row[f"hypo_rate_when_lower_bound_at_or_above_{floor}"] = g.loc[~below, "hypo"].mean() if (~below).any() else np.nan
            row[f"share_of_hypos_preceded_by_lower_bound_below_{floor}"] = (below & g["hypo"]).sum() / g["hypo"].sum() if g["hypo"].any() else np.nan
        row["auc_hypo_min_lower_bound"] = auc(-g["min_lower"], g["hypo"])
        row["auc_hypo_loop_min_forecast"] = auc(-g["loop_min_forecast"], g["hypo"])
        row["auc_hypo_current_glucose"] = auc(-g["cgm0"], g["hypo"])
        row["auc_hypo_min_centre"] = auc(-g["min_centre"], g["hypo"])
        rows.append(row)
    return pd.DataFrame(rows)


def calibration(table):
    rows = []
    for name, g in [("insulin recommended", table[table["insulin_recommended"]]), ("none recommended", table[~table["insulin_recommended"]])]:
        for score, label in [("min_lower", "interval lower bound"), ("loop_min_forecast", "Loop's forecast minimum")]:
            binned = pd.cut(g[score], LOWER_BOUND_BINS)
            agg = g.groupby(binned, observed=True).agg(hypo_rate=("hypo", "mean"), share=("hypo", lambda v: len(v) / len(g)))
            for interval_, r in agg.iterrows():
                rows.append({"decision_class": name, "score": label, "bin_left": interval_.left, "bin_right": interval_.right,
                             "share_of_class": r["share"], "hypo_rate": r["hypo_rate"]})
    return pd.DataFrame(rows)


def figure(table, summary, calib, fig_dir):
    fig, axes = plt.subplots(2, 2, figsize=(15, 10.5))
    fig.subplots_adjust(hspace=0.35, wspace=0.28)
    rec, none = table[table["insulin_recommended"]], table[~table["insulin_recommended"]]
    # (a) ECDF of the minimum lower bound over the window, by decision class, with Loop's own minimum dashed
    ax = axes[0][0]
    for g, label, colour in [(rec, "insulin recommended", "C3"), (none, "none recommended", "C0")]:
        for score, style, name in [("min_lower", "-", "interval 95% lower bound"), ("loop_min_forecast", "--", "Loop's forecast minimum")]:
            v = np.sort(g[score].to_numpy()); ax.plot(v, np.arange(1, len(v) + 1) / len(v), style, color=colour, label=f"{label}: {name}")
    for floor in GLUCOSE_FLOORS_MG_DL:
        ax.axvline(floor, color="0.4", linestyle=":", linewidth=0.9); ax.text(floor + 1, 0.02, f"{floor}", fontsize=8, color="0.3")
    ax.set_xlim(20, 260); ax.set_xlabel(f"minimum over the next {DECISION_HORIZON_MIN} min (mg/dL)"); ax.set_ylabel("share of decisions at or below")
    ax.set_title("Where the interval's floor sits when Loop decides", fontsize=10); ax.legend(fontsize=7.5, frameon=False, loc="lower right")
    # (b) calibration: realized hypo rate by bin of the score
    ax = axes[0][1]
    for name, colour in [("insulin recommended", "C3"), ("none recommended", "C0")]:
        for label, style in [("interval lower bound", "o-"), ("Loop's forecast minimum", "s--")]:
            c = calib[(calib["decision_class"] == name) & (calib["score"] == label) & (calib["share_of_class"] >= 0.005)]
            ax.plot((c["bin_left"] + c["bin_right"]) / 2, c["hypo_rate"], style, color=colour, markersize=4, label=f"{name}: {label}")
    ax.axvline(HYPO_MG_DL, color="0.4", linestyle=":", linewidth=0.9)
    ax.set_xlabel(f"score bin (mg/dL): minimum over the next {DECISION_HORIZON_MIN} min"); ax.set_ylabel(f"share with realized minimum < {HYPO_MG_DL}")
    ax.set_title("Does the floor predict the hypos that followed? (bins holding ≥ 0.5% of the class)", fontsize=10); ax.legend(fontsize=7.5, frameon=False)
    # (c) ROC on decisions with insulin recommended
    ax = axes[1][0]
    for score, label, style in [("min_lower", "interval lower bound", "-"), ("loop_min_forecast", "Loop's forecast minimum", "--"),
                                ("cgm0", "current glucose", ":"), ("min_centre", "interval centre minimum", "-.")]:
        fp, tp = roc_curve(-rec[score], rec["hypo"]); ax.plot(fp, tp, style, label=f"{label} (AUC {auc(-rec[score], rec['hypo']):.3f})")
    ax.plot([0, 1], [0, 1], color="0.7", linewidth=0.8)
    ax.set_xlabel("false positive rate"); ax.set_ylabel("true positive rate")
    ax.set_title(f"Ranking hypo risk (< {HYPO_MG_DL} within {DECISION_HORIZON_MIN} min), insulin-recommended decisions", fontsize=10)
    ax.legend(fontsize=8, frameon=False, loc="lower right")
    # (d) the floor rule described: share of insulin-recommended decisions below each floor and hypo rates either side
    ax = axes[1][1]
    x = np.arange(len(GLUCOSE_FLOORS_MG_DL)); width = 0.25
    s = summary[summary["decision_class"] == "insulin recommended"].iloc[0]
    ax.bar(x - width, [s[f"share_lower_bound_below_{f}"] for f in GLUCOSE_FLOORS_MG_DL], width, color="0.6", label="share of decisions with the lower bound below the floor")
    ax.bar(x, [s[f"hypo_rate_when_lower_bound_below_{f}"] for f in GLUCOSE_FLOORS_MG_DL], width, color="C3", label=f"hypo rate (< {HYPO_MG_DL}) when below")
    ax.bar(x + width, [s[f"hypo_rate_when_lower_bound_at_or_above_{f}"] for f in GLUCOSE_FLOORS_MG_DL], width, color="C2", label=f"hypo rate when at or above")
    ax.set_xticks(x); ax.set_xticklabels([f"floor {f} mg/dL" for f in GLUCOSE_FLOORS_MG_DL]); ax.set_ylabel("share")
    ax.set_title("The floor as a gate, insulin-recommended decisions (descriptive)", fontsize=10)
    ax.legend(fontsize=8, frameon=False)
    fig.suptitle(f"Loop's decisions through the prediction interval (holdout; Loop's displayed forecast + full interval model; "
                 f"window {DECISION_HORIZON_MIN} min)", y=0.98, fontsize=11)
    os.makedirs(fig_dir, exist_ok=True)
    path = os.path.join(fig_dir, "26_decision_intervals.png"); fig.savefig(path, dpi=130, bbox_inches="tight"); plt.close(fig)
    print(f"  wrote {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs_loop_displayed"))
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--fig-dir", default=None)
    args = parser.parse_args()
    held = load_table(args.out_dir, HOLDOUT_TABLE, columns=HOLDOUT_COLUMNS)
    table = decisions_with_intervals(held, args.data_dir)
    del held
    summary = summarize(table)
    calib = calibration(table)
    summary.to_csv(os.path.join(args.out_dir, "decision_intervals_summary.csv"), index=False)
    calib.to_csv(os.path.join(args.out_dir, "decision_intervals_calibration.csv"), index=False)
    figure(table, summary, calib, args.fig_dir or os.path.join(args.out_dir, "figures"))
    print(f"decisions: {len(table):,} holdout origins with a decision, {table['_userId'].nunique()} users; "
          f"insulin recommended at {table['insulin_recommended'].mean():.1%}")
    show = ["decision_class", "share_of_decisions", "median_recommended_bolus_u", "median_cgm0", "median_loop_min_forecast",
            "median_min_lower_bound", "median_realized_min", f"hypo_rate_lt{HYPO_MG_DL}"]
    show += [c for c in summary.columns if c.startswith("share_lower_bound_below") or c.startswith("hypo_rate_when") or c.startswith("share_of_hypos")]
    show += [c for c in summary.columns if c.startswith("auc")]
    pd.set_option("display.width", 250)
    print(summary[show].round(3).T.to_string())


if __name__ == "__main__":
    main()
