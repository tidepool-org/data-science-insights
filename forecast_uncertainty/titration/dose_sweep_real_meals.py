"""Program step 3 of interval_in_the_loop.md: the descriptive dose sweep at REAL holdout meals.

For every holdout bolus-time decision with a meal (the meal channel's meal: carbs entered within ±10 min, a bolus within
15 min), the interval was fit with the delivered dose in the forecast. Because the dose enters the centre only, and the
location and scale read the pre-meal state, the 95% lower bound at any candidate dose d is
    lower_h(d) = lower_h(delivered) + delivered_effect_h − d × u_h,    u_h = ISF × F(h − 5) (Loop's curve, bolus at the next tick),
so for a rule (level L, floor F) the lower bound at level L is centre_h(0) − d × u_h + scale_h × q_L(h) and the largest
    dose satisfying it is closed-form: d_max = min over horizons with u_h > 0 of (lower_h(0) − F) / u_h, 0 if the floor fails at
    zero dose anywhere; a multi-rule gate takes the minimum over its rules. Every rule in floor_gate.GATE_RULES is reported.
Reported against the dose actually delivered and the realized minimum over the window (on the table's horizon grid).
No causal reading: it says how often the rule would have bound on real meals, in which direction, and by how much.

  python titration/dose_sweep_real_meals.py [--out-dir outputs/runs/loop_displayed_bolus_time_meal_channel]
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from project_paths import MEAL_CHANNEL_RUN  # noqa: E402

from evaluation.residual_schema import HOLDOUT_TABLE, load_table  # noqa: E402
from model.forecasters import DEFAULT_INSULIN_PRESET, LOOP_INSULIN_PRESETS, TICK_MINUTES, LoopExponentialInsulinCurve  # noqa: E402
from model.origin_states import level_state  # noqa: E402
from titration.floor_gate import GATE_RULES, WINDOW_MIN  # noqa: E402

COLUMNS = ["_userId", "origin_index", "horizon_min", "cgm0", "isf0", "bolus_window_u", "carb_window_g", "lower", "centre", "scale", "realized",
           "delivered_dose_effect_pred", "carb_effect_pred", "displayed_effect_pred", "hour_local"]
GRAM_BINS = [0, 20, 40, 60, 1000]
GRAM_LABELS = ["<= 20 g", "20-40 g", "40-60 g", "> 60 g"]


def floor_quantiles(out_dir, levels):
    """One-sided floor quantiles per horizon from the run's standardized training sample (the same fit as the table's intervals)."""
    sample = pd.read_csv(os.path.join(out_dir, "standardized_train.csv"))
    grouped = sample.groupby("horizon_min")["standardized"]
    return {level: grouped.quantile(1 - level) for level in levels}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=MEAL_CHANNEL_RUN)
    parser.add_argument("--window-min", type=int, default=WINDOW_MIN)
    args = parser.parse_args()
    held = load_table(args.out_dir, HOLDOUT_TABLE, columns=COLUMNS)
    held = held[(held["carb_window_g"] > 0) & (held["bolus_window_u"] > 0) & (held["horizon_min"] <= args.window_min)].copy()
    duration, peak = LOOP_INSULIN_PRESETS[DEFAULT_INSULIN_PRESET]
    curve = LoopExponentialInsulinCurve(duration, peak)
    held["unit_effect"] = held["isf0"] * curve.cumulative_fraction(held["horizon_min"].to_numpy() - TICK_MINUTES)
    held["centre_at_zero"] = held["centre"] + held["delivered_dose_effect_pred"]
    levels = sorted({level for rules in GATE_RULES.values() for level, _ in rules})
    quantiles = floor_quantiles(args.out_dir, levels)
    acting = held["unit_effect"] > 0
    key = ["_userId", "origin_index"]
    per = (held.groupby(key, observed=True)
               .agg(delivered_u=("bolus_window_u", "first"), grams=("carb_window_g", "first"), cgm0=("cgm0", "first"),
                    hour_local=("hour_local", "first"), realized_min=("realized", "min"), n_h=("horizon_min", "size"))
               .reset_index())
    per = per[per["n_h"] == held["horizon_min"].nunique()].set_index(key)     # every horizon of the window present
    per["hypo"] = per["realized_min"] < 70
    per["level_state"] = level_state(per["cgm0"])
    per["gram_bin"] = pd.cut(per["grams"], GRAM_BINS, labels=GRAM_LABELS)
    summary = []
    for label, rules in GATE_RULES.items():
        d_max = pd.Series(np.inf, index=per.index)
        for level, floor in rules:
            q = held["horizon_min"].map(quantiles[level]).to_numpy()
            lower_at_zero = held["centre_at_zero"] + held["scale"] * q
            cap = np.where(acting, np.maximum(lower_at_zero - floor, 0.0) / held["unit_effect"].where(acting, 1.0), np.inf)
            frame = pd.DataFrame({"cap": cap, "fails": lower_at_zero < floor}, index=held.index).join(held[key])
            per_rule = frame.groupby(key, observed=True).agg(cap=("cap", "min"), fails=("fails", "any")).reindex(per.index)
            d_max = np.minimum(d_max, np.where(per_rule["fails"], 0.0, per_rule["cap"]))
        binds = d_max < per["delivered_u"] - 1e-9
        allowed = np.minimum(d_max / per["delivered_u"], 1.0)
        per[f"d_max[{label}]"] = d_max
        summary.append({"rule": label, "binds": binds.mean(), "fails at zero dose": (d_max <= 0).mean(), "allowed / delivered (median)": pd.Series(allowed).median(),
                        "hypo when bound": per.loc[binds, "hypo"].mean() if binds.any() else np.nan, "hypo when not": per.loc[~binds, "hypo"].mean() if (~binds).any() else np.nan})
    per.reset_index().to_csv(os.path.join(args.out_dir, "dose_sweep_real_meals.csv"), index=False)
    pd.set_option("display.width", 220)
    print(f"holdout meal decisions with every horizon to {args.window_min} min; realized minimum < 70 within the window at {per['hypo'].mean():.3f} of meals\n")
    print(pd.DataFrame(summary).set_index("rule").round(3).to_string())
    print(f"\nwritten to {os.path.join(args.out_dir, 'dose_sweep_real_meals.csv')}")


if __name__ == "__main__":
    main()
