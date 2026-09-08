"""Step 2: online adaptation of the interval level on the holdout stream, lumped vs per-horizon.

    python run_residuals.py   --out-dir outputs
    python evaluation/adaptive_level.py --out-dir outputs [--gammas 0.002,0.005,0.01]

The location and scale models stay fixed (fit on the training part of each user's window). What adapts
is the miscoverage level alpha used to read the standardized quantiles, following adaptive conformal
inference (Gibbs & Candès, 2021): after each outcome arrives,
    alpha_next = alpha + gamma × (alpha_target − miss)
where miss is 1 if the interval issued for that outcome failed to cover it. A run of misses lowers alpha
and widens the intervals; a run of hits raises it and narrows them. Feedback is lagged by the horizon: the
outcome of the interval issued at tick t for horizon h arrives at tick t + h/5.

Three variants are run per user, chronologically over the holdout ticks:
    static        alpha fixed at ALPHA_TARGET (the existing intervals; the baseline)
    lumped        ONE alpha per user, updated each tick with the mean miss over every horizon whose
                  outcome arrived at that tick
    per_horizon   one alpha per horizon, each updated only with its own lagged misses
The quantile function q(alpha) per horizon is the TRAIN standardized sample's empirical quantiles on a
fine alpha grid; alpha is clamped to [ALPHA_MIN, ALPHA_MAX] so the interval can widen far past nominal but
never past the sample's tails.

Writes to --out-dir: aci_summary.csv (per user × variant × gamma: coverage, median width, alpha spread),
aci_by_horizon.csv, aci_by_state.csv (post-meal vs other), aci_alpha_trace.csv (alpha per tick for one user).
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python evaluation/<script>.py` from anywhere
import argparse

import numpy as np
import pandas as pd

from model.forecasters import HORIZONS_MIN, TICK_MINUTES
from model.scale_model import GLUCOSE_FLOOR_MG_DL
from evaluation.residual_schema import HOLDOUT_TABLE, load_table

ALPHA_TARGET = 0.05
ALPHA_MIN, ALPHA_MAX, ALPHA_STEP = 0.002, 0.5, 0.001
GAMMAS = (0.002, 0.005, 0.01)
POST_MEAL_MINUTES = 180
VARIANTS = ("static", "lumped", "per_horizon")


class QuantileTable:
    """q_lo(alpha), q_hi(alpha) per horizon from the train standardized sample, on a fixed alpha grid."""

    def __init__(self, standardized_train):
        self.alphas = np.round(np.arange(ALPHA_MIN, ALPHA_MAX + ALPHA_STEP / 2, ALPHA_STEP), 6)
        self.lo, self.hi = {}, {}
        for horizon, grp in standardized_train.groupby("horizon_min"):
            values = np.sort(grp["standardized"].dropna().to_numpy())
            self.lo[horizon] = np.quantile(values, self.alphas / 2)
            self.hi[horizon] = np.quantile(values, 1 - self.alphas / 2)

    def index(self, alpha):
        return int(np.clip(round((alpha - ALPHA_MIN) / ALPHA_STEP), 0, len(self.alphas) - 1))

    def bounds(self, horizon, alpha):
        i = self.index(alpha)
        return self.lo[horizon][i], self.hi[horizon][i]


def dense_user_arrays(user_rows, horizons):
    """Per horizon, arrays indexed by tick (origin_index) for centre, scale, realized, post_meal; NaN where absent."""
    n = int(user_rows["origin_index"].max()) + 1
    arrays = {}
    for horizon in horizons:
        rows = user_rows[user_rows["horizon_min"] == horizon]
        idx = rows["origin_index"].to_numpy()
        centre = np.full(n, np.nan); scale = np.full(n, np.nan); realized = np.full(n, np.nan)
        post_meal = np.zeros(n, dtype=bool)
        centre[idx] = rows["centre"].to_numpy(); scale[idx] = rows["scale"].to_numpy()
        realized[idx] = rows["realized"].to_numpy()
        post_meal[idx] = (rows["minutes_since_carb_entry"] < POST_MEAL_MINUTES).to_numpy()
        arrays[horizon] = {"centre": centre, "scale": scale, "realized": realized, "post_meal": post_meal}
    return n, arrays


def run_variant(arrays, n_ticks, horizons, quantiles, variant, gamma):
    """Chronological pass over one user's ticks. Returns per-(horizon, tick) covered/width and the alpha trace."""
    lag_ticks = {h: h // TICK_MINUTES for h in horizons}
    alpha = {h: ALPHA_TARGET for h in horizons}          # per_horizon uses all entries; lumped keeps them equal
    miss_issued = {h: np.full(n_ticks, np.nan) for h in horizons}   # miss of the interval issued at tick t
    covered = {h: np.full(n_ticks, np.nan) for h in horizons}
    width = {h: np.full(n_ticks, np.nan) for h in horizons}
    alpha_trace = np.full(n_ticks, np.nan)

    for t in range(n_ticks):
        # 1. Feedback that arrives now: outcomes of intervals issued lag_ticks ago.
        if variant != "static":
            arrived = []
            for h in horizons:
                issued_at = t - lag_ticks[h]
                if issued_at >= 0 and not np.isnan(miss_issued[h][issued_at]):
                    arrived.append((h, miss_issued[h][issued_at]))
            if variant == "lumped" and arrived:
                mean_miss = float(np.mean([m for _, m in arrived]))
                new_alpha = float(np.clip(alpha[horizons[0]] + gamma * (ALPHA_TARGET - mean_miss), ALPHA_MIN, ALPHA_MAX))
                for h in horizons:
                    alpha[h] = new_alpha
            elif variant == "per_horizon":
                for h, miss in arrived:
                    alpha[h] = float(np.clip(alpha[h] + gamma * (ALPHA_TARGET - miss), ALPHA_MIN, ALPHA_MAX))
        alpha_trace[t] = alpha[horizons[0]] if variant != "per_horizon" else np.mean([alpha[h] for h in horizons])

        # 2. Issue this tick's intervals at the current level and record their (future) outcome.
        for h in horizons:
            centre, scale, realized = arrays[h]["centre"][t], arrays[h]["scale"][t], arrays[h]["realized"][t]
            if np.isnan(centre) or np.isnan(realized):
                continue
            q_lo, q_hi = quantiles.bounds(h, alpha[h])
            lower, upper = max(centre + scale * q_lo, GLUCOSE_FLOOR_MG_DL), centre + scale * q_hi
            hit = lower <= realized <= upper
            covered[h][t] = hit
            width[h][t] = upper - lower
            miss_issued[h][t] = 0.0 if hit else 1.0
    return covered, width, alpha_trace


def summarize(user_id, variant, gamma, horizons, arrays, covered, width, alpha_trace):
    per_horizon, per_state = [], []
    all_cov, all_width = [], []
    for h in horizons:
        valid = ~np.isnan(covered[h])
        cov, wid, post = covered[h][valid], width[h][valid], arrays[h]["post_meal"][valid]
        all_cov.append(cov); all_width.append(wid)
        per_horizon.append({"_userId": user_id, "variant": variant, "gamma": gamma, "horizon_min": h,
                            "coverage": cov.mean(), "median_width": np.median(wid)})
        for state, mask in (("post_meal", post), ("other", ~post)):
            if mask.any():
                per_state.append({"_userId": user_id, "variant": variant, "gamma": gamma, "horizon_min": h,
                                  "state": state, "coverage": cov[mask].mean(), "median_width": np.median(wid[mask])})
    trace = alpha_trace[~np.isnan(alpha_trace)]
    summary = {"_userId": user_id, "variant": variant, "gamma": gamma,
               "coverage": np.concatenate(all_cov).mean(), "median_width": np.median(np.concatenate(all_width)),
               "alpha_median": np.median(trace), "alpha_min": trace.min(), "alpha_max": trace.max(),
               "alpha_sd": trace.std()}
    return summary, per_horizon, per_state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--gammas", default=",".join(str(g) for g in GAMMAS))
    parser.add_argument("--trace-user", default=None, help="_userId whose alpha trace is written (default: first)")
    args = parser.parse_args()
    gammas = [float(g) for g in args.gammas.split(",")]

    held = load_table(args.out_dir, HOLDOUT_TABLE)
    standardized_train = pd.read_csv(os.path.join(args.out_dir, "standardized_train.csv"))
    quantiles = QuantileTable(standardized_train)
    horizons = [h for h in HORIZONS_MIN if h in set(held["horizon_min"].unique())]

    summaries, by_horizon, by_state, traces = [], [], [], []
    users = sorted(held["_userId"].unique())
    trace_user = args.trace_user or users[0]
    for user_id in users:
        n_ticks, arrays = dense_user_arrays(held[held["_userId"] == user_id], horizons)
        for variant in VARIANTS:
            for gamma in (gammas if variant != "static" else [0.0]):
                covered, width, alpha_trace = run_variant(arrays, n_ticks, horizons, quantiles, variant, gamma)
                summary, per_h, per_s = summarize(user_id, variant, gamma, horizons, arrays, covered, width, alpha_trace)
                summaries.append(summary); by_horizon += per_h; by_state += per_s
                if user_id == trace_user:
                    traces.append(pd.DataFrame({"variant": variant, "gamma": gamma, "tick": np.arange(n_ticks),
                                                "alpha": alpha_trace}).dropna())
        print(f"user done ({variant} x {len(gammas)} gammas)")

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(os.path.join(args.out_dir, "aci_summary.csv"), index=False)
    pd.DataFrame(by_horizon).to_csv(os.path.join(args.out_dir, "aci_by_horizon.csv"), index=False)
    pd.DataFrame(by_state).to_csv(os.path.join(args.out_dir, "aci_by_state.csv"), index=False)
    pd.concat(traces, ignore_index=True).to_csv(os.path.join(args.out_dir, "aci_alpha_trace.csv"), index=False)

    print("\nper-user coverage and width, median across users (nominal 0.95):")
    print(summary_df.groupby(["variant", "gamma"]).agg(
        coverage_median=("coverage", "median"), coverage_min=("coverage", "min"), coverage_max=("coverage", "max"),
        width_median=("median_width", "median"), alpha_sd=("alpha_sd", "median")).round(3).to_string())
    print("\ncoverage by horizon, median across users:")
    print(pd.DataFrame(by_horizon).groupby(["variant", "gamma", "horizon_min"])["coverage"].median()
          .unstack("horizon_min").round(3).to_string())
    print("\ncoverage by state, median across users:")
    print(pd.DataFrame(by_state).groupby(["variant", "gamma", "state"])["coverage"].median()
          .unstack("state").round(3).to_string())


if __name__ == "__main__":
    main()
