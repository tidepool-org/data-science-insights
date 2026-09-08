"""Per-user shrinkage, cold-start replay under leave-one-user-out.

    python run_residuals.py    --out-dir outputs
    python evaluation/shrinkage_replay.py --out-dir outputs [--max-train-rows 400000] [--half-life-days 21]

Question: starting a NEW user on the population interval, how fast and how far does learning a per-user
offset and scale multiplier from their own residuals improve calibration?

For each held-out user the location / scale models and the standardized quantiles are fit on the OTHER
users (the population). The user's stream is then replayed chronologically from the start of their
window. Each residual, once its outcome arrives (lagged by the horizon), updates two exponentially
forgotten sums of the user's standardized deviations z = (realized − centre) / scale:
    offset      b = w_b × mean(z)                       shrunk toward 0
    multiplier  log k = w_k × (mean(log|z − b|) − population mean log|z|)   shrunk toward 0
with weights w = n_eff / (n_eff + n_0). n_eff is the forgotten count of residuals, each counted as
1 / (horizon / 5 min) of an independent observation because origins overlap; n_0 = within-user variance /
between-user variance of the corresponding per-user statistic, estimated on the population users, so a
population in which users differ a lot individualizes fast. The interval issued at each tick is
    centre + scale × (b + k × [q_lo, q_hi]_h).
Variants: population (b = 0, k = 1) and shrinkage. Metrics per user per replay week: coverage, PIT mean and
variance (PIT under the shrunk distribution), median width. Also the b / k trajectories for one user.

Writes to --out-dir: shrinkage_by_week.csv, shrinkage_summary.csv, shrinkage_priors.csv, shrinkage_trace.csv.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python evaluation/<script>.py` from anywhere
import argparse

import numpy as np
import pandas as pd

from evaluation.evaluate_distribution import folds
from model.forecasters import HORIZONS_MIN, TICK_MINUTES
from evaluation.residual_schema import RESIDUAL_TABLE, load_table
from model.scale_model import centre_and_scale, fit_scale_model, model_columns, standardized_quantiles

ALPHA = 0.05
HALF_LIFE_DAYS = 21                  # forgetting of the per-user sums
TICKS_PER_DAY = 24 * 60 // TICK_MINUTES
MULTIPLIER_BOUNDS = (0.5, 2.0)       # clip on k: no run of quiet days may collapse the interval
LOG_ABS_FLOOR = 1e-3
WEEKS_REPORTED = 12
VARIANTS = ("population", "shrinkage")


def population_priors(model, train_rows):
    """n_0 for offset and multiplier, plus the population mean of log|z|, from the training users."""
    rows = train_rows.dropna(subset=model_columns(model["location_features"], model["scale_features"]))
    centre, scale = centre_and_scale(model, rows)
    z = pd.Series((rows["realized"].to_numpy() - centre) / scale, index=rows.index)
    log_abs = np.log(np.maximum(np.abs(z), LOG_ABS_FLOOR))
    users = rows["_userId"]
    per_user_mean_z = z.groupby(users).mean()
    per_user_mean_log = log_abs.groupby(users).mean()
    within_var_z = z.groupby(users).var().mean()
    within_var_log = log_abs.groupby(users).var().mean()
    between_var_z = per_user_mean_z.var()
    between_var_log = per_user_mean_log.var()
    return {"n0_offset": float(within_var_z / between_var_z), "n0_multiplier": float(within_var_log / between_var_log),
            "population_mean_log_abs_z": float(log_abs.mean()),
            "between_user_sd_offset": float(np.sqrt(between_var_z)), "between_user_sd_log_multiplier": float(np.sqrt(between_var_log))}


def dense_arrays(user_rows, model, horizons):
    """Per horizon, tick-indexed centre, scale, realized for one user (NaN where absent)."""
    rows = user_rows.dropna(subset=model_columns(model["location_features"], model["scale_features"]))
    centre_all, scale_all = centre_and_scale(model, rows)
    n = int(rows["origin_index"].max()) + 1
    arrays = {}
    for h in horizons:
        mask = (rows["horizon_min"] == h).to_numpy()
        idx = rows["origin_index"].to_numpy()[mask]
        centre = np.full(n, np.nan); scale = np.full(n, np.nan); realized = np.full(n, np.nan)
        centre[idx] = centre_all[mask]; scale[idx] = scale_all[mask]; realized[idx] = rows["realized"].to_numpy()[mask]
        arrays[h] = {"centre": centre, "scale": scale, "realized": realized}
    return n, arrays


def replay(arrays, n_ticks, horizons, quantiles, reference, priors, variant, decay):
    """Chronological pass. Returns per-horizon covered / width / pit arrays and the (b, k) trace."""
    lag = {h: h // TICK_MINUTES for h in horizons}
    sum_z = sum_log = count = 0.0
    b = 0.0; k = 1.0
    covered = {h: np.full(n_ticks, np.nan) for h in horizons}
    width = {h: np.full(n_ticks, np.nan) for h in horizons}
    pit = {h: np.full(n_ticks, np.nan) for h in horizons}
    trace_b = np.full(n_ticks, np.nan); trace_k = np.full(n_ticks, np.nan)
    for t in range(n_ticks):
        if variant == "shrinkage":
            sum_z *= decay; sum_log *= decay; count *= decay
            for h in horizons:
                issued = t - lag[h]
                if issued < 0:
                    continue
                c, s, r = arrays[h]["centre"][issued], arrays[h]["scale"][issued], arrays[h]["realized"][issued]
                if np.isnan(c) or np.isnan(r):
                    continue
                z = (r - c) / s
                weight = 1.0 / lag[h]
                sum_z += weight * z
                sum_log += weight * np.log(max(abs(z - b), LOG_ABS_FLOOR))
                count += weight
            if count > 0:
                b = count / (count + priors["n0_offset"]) * (sum_z / count)
                log_k = count / (count + priors["n0_multiplier"]) * (sum_log / count - priors["population_mean_log_abs_z"])
                k = float(np.clip(np.exp(log_k), *MULTIPLIER_BOUNDS))
        trace_b[t], trace_k[t] = b, k
        for h in horizons:
            c, s, r = arrays[h]["centre"][t], arrays[h]["scale"][t], arrays[h]["realized"][t]
            if np.isnan(c) or np.isnan(r):
                continue
            lower = c + s * (b + k * quantiles.loc[h, "q_lo"])
            upper = c + s * (b + k * quantiles.loc[h, "q_hi"])
            covered[h][t] = lower <= r <= upper
            width[h][t] = upper - lower
            pit[h][t] = np.searchsorted(reference[h], ((r - c) / s - b) / k, side="right") / reference[h].size
    return covered, width, pit, trace_b, trace_k


def weekly_metrics(user_id, variant, horizons, n_ticks, covered, width, pit):
    week = np.arange(n_ticks) // (7 * TICKS_PER_DAY)
    frames = []
    for h in horizons:
        frames.append(pd.DataFrame({"week": week, "covered": covered[h], "width": width[h], "pit": pit[h]}))
    table = pd.concat(frames).dropna()
    by_week = table.groupby("week").agg(coverage=("covered", "mean"), median_width=("width", "median"),
                                        pit_mean=("pit", "mean"), pit_var=("pit", "var")).reset_index()
    by_week["_userId"], by_week["variant"] = user_id, variant
    overall = {"_userId": user_id, "variant": variant, "coverage": table["covered"].mean(),
               "median_width": table["width"].median(), "pit_mean": table["pit"].mean(), "pit_var": table["pit"].var(),
               "pit_ks": float(np.max(np.abs(np.sort(table["pit"].to_numpy()) - np.arange(1, len(table) + 1) / len(table))))}
    return by_week, overall


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--max-train-rows", type=int, default=400_000)
    parser.add_argument("--half-life-days", type=float, default=HALF_LIFE_DAYS)
    parser.add_argument("--trace-user", default=None)
    args = parser.parse_args()
    decay = 0.5 ** (1.0 / (args.half_life_days * TICKS_PER_DAY))

    residuals = load_table(args.out_dir, RESIDUAL_TABLE)
    horizons = [h for h in HORIZONS_MIN if h in set(residuals["horizon_min"].unique())]
    weekly, summaries, prior_rows, traces = [], [], [], []
    for name, train, test in folds(residuals, "louo"):
        if args.max_train_rows and len(train) > args.max_train_rows:
            train = train.sample(args.max_train_rows, random_state=0)
        model = fit_scale_model(train)
        quantiles = standardized_quantiles(model, ALPHA)
        reference = {h: np.sort(g["standardized"].dropna().to_numpy()) for h, g in model["standardized"].groupby("horizon_min")}
        priors = population_priors(model, train)
        prior_rows.append({"fold": name, **priors})
        n_ticks, arrays = dense_arrays(test, model, horizons)
        for variant in VARIANTS:
            covered, width, pit, trace_b, trace_k = replay(arrays, n_ticks, horizons, quantiles, reference, priors, variant, decay)
            by_week, overall = weekly_metrics(name, variant, horizons, n_ticks, covered, width, pit)
            weekly.append(by_week); summaries.append(overall)
            if variant == "shrinkage" and (args.trace_user is None or name.startswith(args.trace_user[:8])):
                if not traces:
                    traces.append(pd.DataFrame({"fold": name, "tick": np.arange(n_ticks), "offset_b": trace_b, "multiplier_k": trace_k}))
        print(f"fold {name}: n0_offset={priors['n0_offset']:.1f} n0_multiplier={priors['n0_multiplier']:.1f}  "
              + "  ".join(f"{s['variant']}: cov={s['coverage']:.3f} pit_mean={s['pit_mean']:.3f} ks={s['pit_ks']:.3f}"
                          for s in summaries[-2:]))

    weekly_df = pd.concat(weekly, ignore_index=True)
    weekly_df.to_csv(os.path.join(args.out_dir, "shrinkage_by_week.csv"), index=False)
    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(os.path.join(args.out_dir, "shrinkage_summary.csv"), index=False)
    pd.DataFrame(prior_rows).to_csv(os.path.join(args.out_dir, "shrinkage_priors.csv"), index=False)
    if traces:
        pd.concat(traces).to_csv(os.path.join(args.out_dir, "shrinkage_trace.csv"), index=False)

    print("\nper-user overall (leave-one-user-out; nominal coverage 0.95, PIT mean 0.5, PIT var 0.0833):")
    print(summary_df.groupby("variant").agg(coverage_median=("coverage", "median"), coverage_min=("coverage", "min"),
                                            coverage_max=("coverage", "max"), pit_mean_min=("pit_mean", "min"),
                                            pit_mean_max=("pit_mean", "max"), pit_ks_median=("pit_ks", "median"),
                                            width_median=("median_width", "median")).round(3).to_string())
    early = weekly_df[weekly_df["week"] < WEEKS_REPORTED]
    print(f"\ncoverage by replay week (median across users), first {WEEKS_REPORTED} weeks:")
    print(early.groupby(["variant", "week"])["coverage"].median().unstack("week").round(3).to_string())
    print("\n|PIT mean − 0.5| by replay week (median across users):")
    tilt = early.assign(tilt=(early["pit_mean"] - 0.5).abs()).groupby(["variant", "week"])["tilt"].median()
    print(tilt.unstack("week").round(3).to_string())


if __name__ == "__main__":
    main()
