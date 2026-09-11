"""Can the interval replace Loop's partial application factor? Descriptive test on the REAL series decisions of the holdout.
At every loop decision where Loop recommended a correction, Loop's automatic bolus as shipped is factor × the recommendation
(SHIPPED_PARTIAL_APPLICATION_FACTOR). A floor rule instead allows the largest dose whose lower bound at the rule's level stays
at or above the floor at every horizon of the window; the dose enters the centre only (Loop's forecast at the decision does
not contain the new dose), so d_max is closed-form from the holdout intervals: min over horizons with u_h > 0 of
(centre_h + scale_h × q_L(h) − F) / u_h, u_h = ISF × Loop's curve at h − 5 min. Each decision is then one of: the rule allows
MORE than Loop gave (>= the full recommendation, or between), or LESS ("catch"). Reported with the hypoglycaemia that
followed (< 70 and < 54 within the window) and the insulin the rule would have delivered against Loop's. Outcomes happened
under Loop's own dose: this says where the rule would move insulin and whether it moves it away from the lows.
  python titration/paf_replacement_real_decisions.py [--out-dir outputs/runs/loop_displayed]
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from project_paths import PRIMARY_RUN  # noqa: E402
from evaluation.decision_intervals import DECISION_HORIZON_MIN, TICK_MIN  # noqa: E402
from evaluation.residual_schema import HOLDOUT_TABLE, load_table  # noqa: E402
from model.forecasters import DEFAULT_INSULIN_PRESET, LOOP_INSULIN_PRESETS, LoopExponentialInsulinCurve  # noqa: E402
from run_residuals import DEFAULT_DATA_DIR, build_user_frame, load_streams, user_streams  # noqa: E402
from titration.floor_gate import GATE_RULES, SHIPPED_PARTIAL_APPLICATION_FACTOR  # noqa: E402

COLUMNS = ["_userId", "origin_index", "timestamp", "horizon_min", "cgm0", "isf0", "centre", "scale", "realized"]
RULES = ("P0.5>=70", "P0.8>=70", "P0.9>=54", "P0.95>=54", "P0.5>=70&P0.95>=54", "P0.95>=70")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=PRIMARY_RUN)
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    args = parser.parse_args()
    held = load_table(args.out_dir, HOLDOUT_TABLE, columns=COLUMNS)
    held = held[held["horizon_min"] <= DECISION_HORIZON_MIN].copy()
    duration, peak = LOOP_INSULIN_PRESETS[DEFAULT_INSULIN_PRESET]
    curve = LoopExponentialInsulinCurve(duration, peak)
    held["unit_effect"] = held["isf0"] * curve.cumulative_fraction(held["horizon_min"].to_numpy() - TICK_MIN)
    sample = pd.read_csv(os.path.join(args.out_dir, "standardized_train.csv")).groupby("horizon_min")["standardized"]
    levels = sorted({level for label in RULES for level, _ in GATE_RULES[label]})
    quantiles = {level: sample.quantile(1 - level) for level in levels}
    key = ["_userId", "origin_index"]
    per = held.groupby(key, observed=True).agg(timestamp=("timestamp", "first"), cgm0=("cgm0", "first"), n_h=("horizon_min", "size")).reset_index()
    per = per[per["n_h"] == held["horizon_min"].nunique()]
    # Loop's recommended correction and the realized minimum from the tick frames
    streams = load_streams(args.data_dir)
    pieces = []
    ticks = DECISION_HORIZON_MIN // TICK_MIN
    for _, user in streams["users"].iterrows():
        mine = per[per["_userId"].astype(str) == str(user["_userId"])]
        if mine.empty:
            continue
        s = user_streams(streams, user["_userId"], user["tz_offset_min"])
        frame = build_user_frame(s["cgm"], s["carbs"], s["boluses"], s["dosing"])
        future_min = frame["cgm"][::-1].rolling(ticks, min_periods=int(ticks * 0.8)).min()[::-1].shift(-1)
        pieces.append(mine.merge(pd.DataFrame({"timestamp": frame["timestamp"], "recommended_u": frame["recommended_bolus"],
                                               "realized_min": future_min.to_numpy()}), on="timestamp", how="left"))
    per = pd.concat(pieces, ignore_index=True).dropna(subset=["recommended_u", "realized_min"])
    per = per[per["recommended_u"] > 0].set_index(key)
    per["loop_autobolus_u"] = SHIPPED_PARTIAL_APPLICATION_FACTOR * per["recommended_u"]
    per["hypo70"] = per["realized_min"] < 70
    per["hypo54"] = per["realized_min"] < 54
    acting = held["unit_effect"] > 0
    rows = []
    for label in RULES:
        d_max = pd.Series(np.inf, index=per.index)
        for level, floor in GATE_RULES[label]:
            q = held["horizon_min"].map(quantiles[level]).to_numpy()
            lower0 = held["centre"] + held["scale"] * q
            cap = np.where(acting, np.maximum(lower0 - floor, 0.0) / held["unit_effect"].where(acting, 1.0), np.inf)
            frame = pd.DataFrame({"cap": cap, "fails": lower0 < floor}, index=held.index).join(held[key])
            per_rule = frame.groupby(key, observed=True).agg(cap=("cap", "min"), fails=("fails", "any")).reindex(per.index)
            d_max = np.minimum(d_max, np.where(per_rule["fails"], 0.0, per_rule["cap"]))
        allowed = np.minimum(d_max, per["recommended_u"])
        more = allowed > per["loop_autobolus_u"] + 1e-9
        less = allowed < per["loop_autobolus_u"] - 1e-9
        rows.append({"rule": label, "share allowing more than Loop gave": more.mean(), "share allowing less (catch)": less.mean(),
                     "share allowing the full correction": (allowed >= per["recommended_u"] - 1e-9).mean(),
                     "insulin at decisions vs Loop's": allowed.sum() / per["loop_autobolus_u"].sum(),
                     "hypo<70 rate | more": per.loc[more, "hypo70"].mean(), "hypo<70 rate | less": per.loc[less, "hypo70"].mean(),
                     "hypo<54 rate | more": per.loc[more, "hypo54"].mean(), "hypo<54 rate | less": per.loc[less, "hypo54"].mean(),
                     "share of <70 events at 'less' decisions": (less & per["hypo70"]).sum() / max(per["hypo70"].sum(), 1),
                     "share of <54 events at 'less' decisions": (less & per["hypo54"]).sum() / max(per["hypo54"].sum(), 1)})
        per[f"allowed[{label}]"] = allowed
    per.reset_index().to_csv(os.path.join(args.out_dir, "paf_replacement_real_decisions.csv"), index=False)
    pd.set_option("display.width", 250)
    print(f"holdout loop decisions with a positive correction; hypo < 70 within {DECISION_HORIZON_MIN} min followed {per['hypo70'].mean():.3f}, "
          f"< 54 {per['hypo54'].mean():.3f}; Loop's factor {SHIPPED_PARTIAL_APPLICATION_FACTOR}\n")
    print(pd.DataFrame(rows).set_index("rule").round(3).T.to_string())
    print(f"\nwritten to {os.path.join(args.out_dir, 'paf_replacement_real_decisions.csv')}")


if __name__ == "__main__":
    main()
