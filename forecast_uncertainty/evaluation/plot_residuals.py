"""Figures for the residual / coverage backtest. Reads what run_residuals.py wrote.

    python run_residuals.py         --out-dir outputs
    python evaluation/evaluate_distribution.py --out-dir outputs      # figures 12-14
    python evaluation/compare_models.py        --out-dir outputs      # figure 15
    python evaluation/plot_residuals.py        --out-dir outputs [--nominal 0.95] [--only 05,16]

Required: residuals.parquet, holdout_intervals.parquet (figures 03-07, 11, 16).
Optional: stream_hours.csv, example_frame.csv, run_meta.json (figures 01-02),
          standardized_quantiles.csv, standardized_train.csv (the reference lines in figure 04),
          dist_eval_folds.csv, dist_eval_pit.csv, dist_eval_coverage.csv, dist_eval_features.csv (figures 12-14),
          model_comparison.csv (figure 15).
Anything missing is skipped with a message; the rest still run. --only picks figures by number.

Columns are the real names from scale_model.interval() and residual_schema.py (residual, location, scale,
lower, upper, covered, width, horizon_min, _userId, holdout, origin_index, timestamp, predicted_change,
minutes_since_carb_entry); the example frame has timestamp and cgm. Nothing in this script recomputes the
model, so iterating on a plot costs seconds.

Uncertainty bars on coverage are user-day block bootstraps (residual_schema.block_bootstrap_coverage), never
Wilson on rows: origins overlap heavily within a user, so a row-count interval would be badly optimistic.

Disclosure rule: no figure states how many users, rows or events it was built from. Counts stay in the
console output of run_residuals.py.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python evaluation/<script>.py` from anywhere
import argparse
import json

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")  # no display in the dev env
import matplotlib.pyplot as plt  # noqa: E402

from model.scale_model import GLUCOSE_FLOOR_MG_DL  # noqa: E402
from evaluation.residual_schema import (DAY_LADDER_TABLE, HOLDOUT_TABLE, LADDER, RESIDUAL_TABLE,  # noqa: E402
                                        block_bootstrap_coverage, choose_day,
                             covered_series, derive_z, load_model_aux, load_table)


RNG_PLOT = np.random.default_rng(0)
POST_MEAL_MINUTES = 180              # same cut as run_residuals.py and compare_models.py
COMPARISON_NOMINAL = 0.95            # compare_models.py scores coverage_at_95 at a fixed alpha = 0.05
PREDICTED_CHANGE_BINS = [-200, -40, -20, -10, 10, 20, 40, 200]   # mg/dL over the horizon, figure 16
RIBBON_HORIZONS = (60, 180)                    # figure 17: one ribbon panel per horizon
LADDER_HORIZON = 180                           # figure 19 default: one ribbon per ladder rung at this horizon
EVENT_WINDOW_MIN = 60                          # figure 21: origins this soon after a carb entry / bolus
FAN_ORIGIN_LOCAL_HOURS = (6.5, 9.0, 13.5, 19.0)   # figure 18: origins on the ribbon day
# model_comparison.csv coverage columns shown in figure 15's third panel: (column, legend label, marker).
CONDITIONAL_COVERAGE_COLUMNS = [("coverage_at_95", "pooled", "o"),
                                ("cov95_post_meal", "post-meal", "s"),
                                ("cov95_other", "other", "D"),
                                ("cov95_predicted_rise", "predicted rise", "^"),
                                ("cov95_predicted_fall", "predicted fall", "v")]


def _save(fig, ctx, name):
    fig.tight_layout()
    fig.savefig(os.path.join(ctx["fig_dir"], name), dpi=ctx["dpi"], bbox_inches="tight")  # keeps suptitles
    plt.close(fig)
    print(f"  wrote {name}")


def _skip(name, missing):
    print(f"  skipped {name} (missing {missing})")


def post_meal_flag(df):
    """Origins within POST_MEAL_MINUTES of a carb entry. No entry yet compares as NaN, i.e. not post-meal."""
    return df["minutes_since_carb_entry"] < POST_MEAL_MINUTES


def user_day_groups(df):
    """Block key for the coverage bootstrap: one user's calendar day (from the origin timestamp).

    Origins five minutes apart share most of their future, so rows are not independent; whole days come
    closer, and resampling them is what block_bootstrap_coverage does.
    """
    day = pd.to_datetime(df["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df["_userId"].astype(str) + "|" + day


def coverage_by(df, key):
    """Coverage per level of `key` (a column name or an aligned Series) with a user-day bootstrap band.

    Reads the _cov column plus _userId / timestamp; returns (levels, coverage, lower, upper) as arrays.
    """
    groups = user_day_groups(df)
    levels, coverage, lower, upper = [], [], [], []
    for level, sub in df.groupby(key):
        lo, hi = block_bootstrap_coverage(sub["_cov"].to_numpy(), groups.loc[sub.index].to_numpy())
        levels.append(level)
        coverage.append(sub["_cov"].mean())
        lower.append(lo)
        upper.append(hi)
    return np.array(levels), np.array(coverage), np.array(lower), np.array(upper)


def _errorbar(ax, x, p, lo, hi, **kwargs):
    """Coverage points with the bootstrap band as asymmetric error bars (NaN band draws no bar)."""
    yerr = [np.nan_to_num(p - lo), np.nan_to_num(hi - p)]
    ax.errorbar(x, p, yerr=yerr, marker="o", markersize=4, capsize=3, linewidth=1.2, **kwargs)


# --- run diagnostics ---------------------------------------------------------------
def fig_stream_qc(ctx):
    """Hour-of-day distributions per stream -- the cheap check on the APPLY_TZ_OFFSET question.

    If the offset is applied twice, or not at all, meals and boluses stop clustering at
    plausible local mealtimes and the panels drift relative to each other.
    """
    hours = ctx.get("stream_hours")
    if hours is None:
        return _skip("01_stream_qc", "stream_hours.csv")
    streams = [s for s in ["cgm", "carbs", "boluses"] if (hours["stream"] == s).any()]
    fig, axes = plt.subplots(1, len(streams), figsize=(4.2 * len(streams), 3.4), sharex=True)
    axes = np.atleast_1d(axes)
    for ax, stream in zip(axes, streams):
        sub = hours[hours["stream"] == stream].set_index("hour")["count"].reindex(range(24),
                                                                                 fill_value=0)
        ax.bar(sub.index, sub.values / max(sub.sum(), 1), width=0.9, edgecolor="white", linewidth=0.4)
        ax.set_title(stream)
        ax.set_xlabel("local hour of day")
        ax.set_xticks(range(0, 24, 4))
    axes[0].set_ylabel("share of events")
    fig.suptitle(f"Stream timing QC -- APPLY_TZ_OFFSET={ctx['meta'].get('apply_tz_offset', '?')}",
                 y=1.02)
    _save(fig, ctx, "01_stream_qc.png")


def fig_example_trace(ctx):
    """One representative user's CGM trace with the holdout boundary marked."""
    frame = ctx.get("example_frame")
    if frame is None:
        return _skip("02_example_user_trace", "example_frame.csv")
    if not {"timestamp", "cgm"}.issubset(frame.columns):
        return _skip("02_example_user_trace", f"timestamp/cgm columns; frame has {list(frame.columns)[:8]}")
    x = frame["timestamp"]

    # The holdout boundary is read off this user's residual rows: the first held-out origin index.
    user_id = ctx["meta"].get("example_user_id")
    residuals = ctx["residuals"]
    table = residuals[residuals["_userId"].astype(str) == str(user_id)] if user_id else residuals.iloc[0:0]

    fig, ax = plt.subplots(figsize=(11, 3.6))
    ax.plot(x, frame["cgm"], linewidth=0.8)
    if len(table) and table["holdout"].any() and not table["holdout"].all():
        pos = int(min(table.loc[table["holdout"], "origin_index"].min(), len(x) - 1))
        ax.axvline(x.iloc[pos], linestyle="--", linewidth=1.2, color="k")
        ax.text(0.99, 0.94, "holdout ->", transform=ax.transAxes, ha="right", va="top", fontsize=9)
    ax.set_ylabel("cgm (mg/dL)")
    ax.set_xlabel("time")
    label = str(user_id)[:8] + "..." if user_id else "example user"
    ax.set_title(f"Representative user {label}")
    _save(fig, ctx, "02_example_user_trace.png")


def fig_residual_fan(ctx):
    """Residual quantiles vs horizon on train -- the spread the scale model must track."""
    residuals = ctx["residuals"]
    if "residual" not in residuals.columns:
        return _skip("03_residuals_by_horizon", "residual column")
    train = residuals[~residuals["holdout"]]
    grouped = train.groupby("horizon_min")["residual"]
    q = grouped.quantile([0.05, 0.25, 0.5, 0.75, 0.95]).unstack()
    h = q.index.values

    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    axes[0].fill_between(h, q[0.05], q[0.95], alpha=0.22, label="5-95%")
    axes[0].fill_between(h, q[0.25], q[0.75], alpha=0.40, label="25-75%")
    axes[0].plot(h, q[0.5], linewidth=1.4, label="median")
    axes[0].axhline(0, color="k", linewidth=0.7)
    axes[0].set_xlabel("horizon (min)")
    axes[0].set_ylabel("residual")
    axes[0].set_title("Residual quantiles by horizon (train)")
    axes[0].legend(fontsize=8, frameon=False)

    axes[1].plot(h, grouped.std().values, marker="o", markersize=3, label="sd")
    axes[1].plot(h, grouped.apply(lambda s: s.abs().median()).values, marker="s", markersize=3,
                 label="median |residual|")
    axes[1].set_xlabel("horizon (min)")
    axes[1].set_title("Residual scale by horizon (train)")
    axes[1].legend(fontsize=8, frameon=False)
    _save(fig, ctx, "03_residuals_by_horizon.png")


def fig_scale_diagnostics(ctx):
    """Does the train standardized distribution transfer to holdout?

    The reference here is the TRAIN standardized sample, not a normal. The scale is
    exp(E[log|deviation|]), a mean-absolute-deviation quantity, so sd(z) sits above 1 even
    under perfect calibration -- checking sd(z) == 1 or overlaying N(0,1) would both be
    measuring the wrong thing.

    Panel 3 is the one that decides coverage: q_lo/q_hi come from the train sample per
    horizon, so if the holdout's own alpha/2 and 1-alpha/2 quantiles sit outside them, the
    intervals miss nominal at that horizon and the other panels can still look fine.
    """
    held = ctx["held"]
    z_held = derive_z(held)
    if z_held is None:
        return _skip("04_scale_diagnostics", "residual/location/scale columns (rerun run_residuals.py)")
    z_clean = z_held.dropna()
    reference = ctx.get("standardized_train")
    alpha = 1.0 - ctx["nominal"]

    fig, axes = plt.subplots(1, 3, figsize=(13, 3.9))
    sd_h = z_held.groupby(held["horizon_min"]).std()
    axes[0].plot(sd_h.index.values, sd_h.values, marker="o", markersize=3, label="holdout")
    if reference is not None:
        sd_t = reference.groupby("horizon_min")["standardized"].std()
        axes[0].plot(sd_t.index.values, sd_t.values, marker="s", markersize=3, label="train")
    axes[0].set_xlabel("horizon (min)")
    axes[0].set_ylabel("sd of standardized deviation")
    axes[0].set_title("Spread by horizon\n(train vs holdout, no fixed target)")
    axes[0].legend(fontsize=8, frameon=False)

    bins = np.linspace(np.nanquantile(z_clean, 0.005), np.nanquantile(z_clean, 0.995), 60)
    axes[1].hist(z_clean, bins=bins, density=True, alpha=0.55, label="holdout")
    if reference is not None:
        axes[1].hist(reference["standardized"].dropna(), bins=bins, density=True,
                     histtype="step", linewidth=1.4, label="train")
    axes[1].set_xlabel("(residual - location) / scale")
    axes[1].set_title("Standardized deviation")
    axes[1].legend(fontsize=8, frameon=False)

    if ctx["quantiles"] is None:
        axes[2].text(0.5, 0.5, "no standardized_quantiles.csv", ha="center", va="center", fontsize=8)
        axes[2].set_axis_off()
    else:
        emp = z_held.groupby(held["horizon_min"]).quantile([alpha / 2, 1 - alpha / 2]).unstack()
        h = emp.index.values
        axes[2].plot(h, ctx["quantiles"]["q_lo"].reindex(h).values, "s--", markersize=3,
                     color="tab:blue", label="train q_lo (used)")
        axes[2].plot(h, ctx["quantiles"]["q_hi"].reindex(h).values, "s--", markersize=3,
                     color="tab:red", label="train q_hi (used)")
        axes[2].plot(h, emp[alpha / 2].values, "o-", markersize=3, color="tab:blue",
                     label="holdout q_lo")
        axes[2].plot(h, emp[1 - alpha / 2].values, "o-", markersize=3, color="tab:red",
                     label="holdout q_hi")
        axes[2].set_xlabel("horizon (min)")
        axes[2].set_title(f"Quantiles that set the interval\n(alpha={alpha:g})")
        axes[2].legend(fontsize=7, frameon=False)
    _save(fig, ctx, "04_scale_diagnostics.png")


def fig_coverage(ctx):
    """Empirical coverage vs horizon, overall and split by post-meal state, with user-day bootstrap bands."""
    held, nominal = ctx["held"], ctx["nominal"]
    covered = covered_series(held)
    if covered is None or not {"timestamp", "minutes_since_carb_entry"}.issubset(held.columns):
        return _skip("05_coverage", "covered (or lower/upper/realized), timestamp, minutes_since_carb_entry")
    df = held.assign(_cov=covered, _post_meal=post_meal_flag(held))

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.0), sharey=True)
    h, p, lo, hi = coverage_by(df, "horizon_min")
    _errorbar(axes[0], h, p, lo, hi)
    axes[0].axhline(nominal, color="k", linestyle="--", linewidth=0.9)
    axes[0].set_title("Coverage by horizon")
    axes[0].set_ylabel("empirical coverage")

    for flag, label in [(True, f"post-meal (<{POST_MEAL_MINUTES} min)"), (False, "other")]:
        sub = df[df["_post_meal"] == flag]
        if sub.empty:
            continue
        h2, p2, lo2, hi2 = coverage_by(sub, "horizon_min")
        _errorbar(axes[1], h2, p2, lo2, hi2, label=label)
    axes[1].legend(fontsize=8, frameon=False)
    axes[1].axhline(nominal, color="k", linestyle="--", linewidth=0.9)
    axes[1].set_title("Coverage by horizon x post-meal")
    for ax in axes:
        ax.set_xlabel("horizon (min)")
    fig.suptitle(f"Holdout coverage vs nominal {nominal:.0%} (bars = user-day block bootstrap)", y=1.02)
    _save(fig, ctx, "05_coverage.png")


def fig_interval_width(ctx):
    """Where the width goes: median interval width by horizon, split post-meal."""
    held = ctx["held"]
    if not {"width", "minutes_since_carb_entry"}.issubset(held.columns):
        return _skip("06_interval_width", "width / minutes_since_carb_entry")
    df = held.assign(_post_meal=post_meal_flag(held))

    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    g = df.groupby("horizon_min")["width"]
    axes[0].fill_between(g.quantile(0.25).index.values, g.quantile(0.25).values,
                         g.quantile(0.75).values, alpha=0.3, label="25-75%")
    axes[0].plot(g.median().index.values, g.median().values, marker="o", markersize=3,
                 label="median")
    axes[0].set_xlabel("horizon (min)")
    axes[0].set_ylabel("interval width")
    axes[0].set_title("Interval width by horizon")
    axes[0].legend(fontsize=8, frameon=False)

    for flag, label in [(True, "post-meal"), (False, "other")]:
        sub = df[df["_post_meal"] == flag]
        if sub.empty:
            continue
        m = sub.groupby("horizon_min")["width"].median()
        axes[1].plot(m.index.values, m.values, marker="o", markersize=3, label=label)
    axes[1].legend(fontsize=8, frameon=False)
    axes[1].set_xlabel("horizon (min)")
    axes[1].set_title("Median width by post-meal state")
    _save(fig, ctx, "06_interval_width.png")


def fig_per_user_coverage(ctx):
    """Per-user coverage spread -- catches a pooled number that a few users are carrying."""
    held, nominal = ctx["held"], ctx["nominal"]
    covered = covered_series(held)
    if covered is None:
        return _skip("07_per_user_coverage", "covered, or lower/upper plus realized")
    df = held.assign(_cov=covered)
    horizons = sorted(df["horizon_min"].unique())
    picks = [horizons[0], horizons[len(horizons) // 2], horizons[-1]] if len(horizons) >= 3 else horizons

    fig, ax = plt.subplots(figsize=(7.5, 4.0))
    for i, h in enumerate(picks):
        per_user = df[df["horizon_min"] == h].groupby("_userId")["_cov"].mean()
        jitter = (np.random.default_rng(i).random(len(per_user)) - 0.5) * 0.22
        ax.plot(np.full(len(per_user), i) + jitter, per_user.values, "o", markersize=4, alpha=0.55)
        ax.plot([i - 0.25, i + 0.25], [per_user.median()] * 2, "k-", linewidth=1.6)
    ax.axhline(nominal, color="k", linestyle="--", linewidth=0.9)
    ax.set_xticks(range(len(picks)))
    ax.set_xticklabels([f"{h:g} min" for h in picks])
    ax.set_ylabel("per-user coverage")
    ax.set_title("Per-user coverage (one point per user, bar = median)")
    _save(fig, ctx, "07_per_user_coverage.png")


def fig_holdout_drift(ctx):
    """Coverage against position in each user's holdout window.

    Nothing in scale_model widens with elapsed time -- the scale depends on horizon,
    features and hour-of-day (cyclical), so drift across a user's window is unmodelled by
    construction. If coverage slopes down across these deciles, the intervals are stale
    rather than wrong, and that is an argument for the unused scale_multiplier / ACI hooks.

    Position is normalized per user, so users with different window lengths are comparable.
    """
    held, nominal = ctx["held"], ctx["nominal"]
    covered = covered_series(held)
    if covered is None or not {"origin_index", "timestamp"}.issubset(held.columns):
        return _skip("11_holdout_drift", "covered and/or origin_index / timestamp")
    df = held.assign(_cov=covered)
    rank = df.groupby("_userId")["origin_index"].rank(pct=True)
    df["_decile"] = np.clip((rank * 10).astype(int), 0, 9)

    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    decile, p, lo, hi = coverage_by(df, "_decile")
    _errorbar(axes[0], decile, p, lo, hi)
    axes[0].axhline(nominal, color="k", linestyle="--", linewidth=0.9)
    axes[0].set_xlabel("decile of user's holdout window (0 = earliest)")
    axes[0].set_ylabel("coverage")
    axes[0].set_title("Coverage drift across the holdout window\n(bars = user-day block bootstrap)")

    z = derive_z(df)
    if z is not None:
        by_dec = z.abs().groupby(df["_decile"]).median()
        axes[1].plot(by_dec.index.values, by_dec.values, marker="o", markersize=4)
        axes[1].set_ylabel("median |standardized deviation|")
    axes[1].set_xlabel("decile of user's holdout window")
    axes[1].set_title("Standardized error drift")
    _save(fig, ctx, "11_holdout_drift.png")


# --- distributional transfer (needs evaluate_distribution.py) -----------------------
def fig_pit(ctx):
    """PIT histogram per fold, drawn directly from dist_eval_pit.csv.

    Uniform (flat at density 1.0) = the predicted distribution is right at every alpha at once.
    Shape is diagnostic: a U (piled at both edges) means intervals too narrow, a hump in the
    middle means too wide, a tilt means the location term is off for that fold -- which is why the
    histogram is plotted rather than anything derived from the coverage curve (that curve is symmetric
    in alpha by construction and cannot show a tilt). Read the louo folds against the temporal one:
    if the temporal PIT is flat and the louo PITs are not, the model has learned user-specific structure.
    """
    histogram = ctx.get("dist_pit")
    folds = ctx.get("dist_folds")
    if folds is None or folds.empty or histogram is None or histogram.empty:
        return _skip("12_pit", "dist_eval_folds.csv / dist_eval_pit.csv (run evaluate_distribution.py)")
    sub = folds[folds["variant"] == "full"] if "variant" in folds else folds
    n = len(sub)
    ncol = min(4, n)
    nrow = int(np.ceil(n / ncol))
    fig, axes = plt.subplots(nrow, ncol, figsize=(3.3 * ncol, 2.8 * nrow), squeeze=False,
                             sharey=True)
    for ax, (_, row) in zip(axes.ravel(), sub.iterrows()):
        bins = histogram[(histogram["split"] == row["split"])
                         & (histogram["fold"] == row["fold"])].sort_values("bin_left")
        if not bins.empty:
            ax.bar(bins["bin_left"], bins["density"], width=bins["bin_right"] - bins["bin_left"],
                   align="edge", alpha=0.7, edgecolor="white", linewidth=0.4)
        ax.axhline(1.0, color="k", linestyle="--", linewidth=0.8)
        ax.set_xlim(0, 1)
        ax.set_title(f"{row['split']}/{row['fold']}\nKS={row['pit_ks']:.3f} "
                     f"mean={row['pit_mean']:.3f}", fontsize=8)
        ax.set_xlabel("PIT", fontsize=7)
        ax.set_ylabel("density", fontsize=7)
        ax.tick_params(labelsize=6)
    if ctx.get("pit_ylim"):
        axes.ravel()[0].set_ylim(*ctx["pit_ylim"])
    for ax in axes.ravel()[n:]:
        ax.set_axis_off()
    fig.suptitle("PIT histogram by fold (flat at 1.0 = calibrated, shared y axis)", y=1.01)
    _save(fig, ctx, "12_pit.png")


def fig_coverage_curve(ctx):
    """Empirical vs nominal coverage across all alphas, one line per fold.

    This is coverage as a curve rather than a single point, so a model that is right at 95%
    by luck and wrong at 50% has nowhere to hide. Distance from the diagonal at the alpha
    you actually ship is the number that matters.
    """
    curves = ctx.get("dist_coverage")
    if curves is None or curves.empty:
        return _skip("13_coverage_curve", "dist_eval_coverage.csv (run evaluate_distribution.py)")
    splits = sorted(curves["split"].unique())
    fig, axes = plt.subplots(1, len(splits), figsize=(4.8 * len(splits), 4.2), squeeze=False)
    for ax, split in zip(axes.ravel(), splits):
        sub = curves[curves["split"] == split]
        for fold, grp in sub.groupby("fold"):
            grp = grp.sort_values("nominal")
            ax.plot(grp["nominal"], grp["empirical"], marker="o", markersize=3,
                    linewidth=1.0, alpha=0.8, label=str(fold))
        ax.plot([0, 1], [0, 1], "k--", linewidth=0.9)
        ax.set_xlabel("nominal coverage")
        ax.set_ylabel("empirical coverage")
        ax.set_title(split)
        if sub["fold"].nunique() <= 8:
            ax.legend(fontsize=6.5, frameon=False)
    fig.suptitle("Coverage curve -- above the diagonal is conservative, below is overconfident",
                 y=1.02, fontsize=10)
    _save(fig, ctx, "13_coverage_curve.png")


def fig_feature_ablation(ctx):
    """Change in CRPS when each feature is removed, one point per held-out fold.

    The fold spread IS the uncertainty -- a feature whose points sit consistently above zero
    is carrying real signal, one with a big median driven by a single fold is not.
    """
    feats = ctx.get("dist_features")
    if feats is None or feats.empty:
        return _skip("14_feature_ablation", "dist_eval_features.csv (run evaluate_distribution.py)")
    splits = sorted(feats["split"].unique())
    fig, axes = plt.subplots(1, len(splits), figsize=(5.2 * len(splits), 4.2),
                             squeeze=False, sharey=True)
    for ax, split in zip(axes.ravel(), splits):
        sub = feats[feats["split"] == split]
        order = sub.groupby("feature")["crps_delta"].median().sort_values().index.tolist()
        for i, feature in enumerate(order):
            vals = sub[sub["feature"] == feature]["crps_delta"].to_numpy()
            jitter = (RNG_PLOT.random(len(vals)) - 0.5) * 0.2
            ax.plot(vals, np.full(len(vals), i) + jitter, "o", markersize=5, alpha=0.65)
            ax.plot([np.median(vals)] * 2, [i - 0.25, i + 0.25], "k-", linewidth=1.8)
        ax.axvline(0, color="k", linestyle="--", linewidth=0.9)
        ax.set_yticks(range(len(order)))
        ax.set_yticklabels(order, fontsize=8)
        ax.set_xlabel("CRPS increase when removed")
        ax.set_title(split)
    fig.suptitle("Feature ablation (right of zero = the feature helps)", y=1.02, fontsize=10)
    _save(fig, ctx, "14_feature_ablation.png")


def fig_model_comparison(ctx):
    """CRPS, calibration and conditional coverage by location/scale specification, one point per fold.

    The parameter count on the left axis is the whole argument: if a spec with a tenth of
    the parameters matches full on both scores, the features are not carrying the predicted
    distribution, and the smaller fit is the one that will transfer to an unseen user.

    The third column is where a missing feature shows: pooled coverage_at_95 can sit on nominal
    while post-meal or predicted-rise coverage does not, so each origin state gets its own row of
    points (one per fold, larger marker = median across folds) against the nominal line.
    """
    table = ctx.get("model_comparison")
    if table is None or table.empty:
        return _skip("15_model_comparison", "model_comparison.csv (run compare_models.py)")
    splits = sorted(table["split"].unique())
    conditional = [(c, label, marker) for c, label, marker in CONDITIONAL_COVERAGE_COLUMNS if c in table]
    offsets = np.linspace(-0.3, 0.3, len(conditional)) if conditional else []

    fig, axes = plt.subplots(len(splits), 3, figsize=(15.5, 3.6 * len(splits)), squeeze=False)
    for row, split in enumerate(splits):
        sub = table[table["split"] == split]
        order = sub.groupby("spec")["n_params"].median().sort_values(ascending=False).index.tolist()
        labels = [f"{s}\n({int(sub[sub['spec'] == s]['n_params'].median())} params)" for s in order]

        # Columns 0-1: proper score and PIT KS, per fold, dashed line at the full model's median.
        for column, (metric, title) in enumerate([("crps", "CRPS (mg/dL)"), ("pit_ks", "PIT KS")]):
            ax = axes[row][column]
            for i, spec in enumerate(order):
                vals = sub[sub["spec"] == spec][metric].to_numpy()
                jitter = (RNG_PLOT.random(len(vals)) - 0.5) * 0.2
                ax.plot(vals, np.full(len(vals), i) + jitter, "o", markersize=5, alpha=0.65)
                ax.plot([np.median(vals)] * 2, [i - 0.25, i + 0.25], "k-", linewidth=1.8)
            if "full" in order:
                ref = sub[sub["spec"] == "full"][metric].median()
                ax.axvline(ref, color="k", linestyle="--", linewidth=0.9)
            ax.set_yticks(range(len(order)))
            ax.set_yticklabels(labels if column == 0 else [""] * len(order), fontsize=7)
            ax.set_xlabel(title)
            ax.set_title(f"{split} -- {title}", fontsize=9)

        # Column 2: coverage at 95% pooled and by origin state, one sub-row per state within each spec.
        ax = axes[row][2]
        labelled = set()   # each origin state enters the legend once, on its first spec with any rows
        for i, spec in enumerate(order):
            spec_rows = sub[sub["spec"] == spec]
            for k, (column_name, label, marker) in enumerate(conditional):
                vals = spec_rows[column_name].dropna().to_numpy()
                if vals.size == 0:
                    continue   # compare_models.py writes NaN when no origin met the state's threshold
                y = i + offsets[k]
                ax.plot(vals, np.full(vals.size, y), marker, markersize=3.5, alpha=0.45, color=f"C{k}",
                        linestyle="none", label=None if label in labelled else label)
                labelled.add(label)
                ax.plot([np.median(vals)], [y], marker, markersize=6.5, color=f"C{k}",
                        markeredgecolor="k", markeredgewidth=0.6, linestyle="none")
        ax.axvline(COMPARISON_NOMINAL, color="k", linestyle="--", linewidth=0.9)
        ax.set_yticks(range(len(order)))
        ax.set_yticklabels([""] * len(order), fontsize=7)
        ax.set_ylim(-0.6, len(order) - 0.4)
        ax.set_xlabel(f"coverage at {COMPARISON_NOMINAL:.0%}")
        ax.set_title(f"{split} -- coverage by origin state", fontsize=9)
        if conditional:
            ax.legend(fontsize=6.5, frameon=False, loc="best")
    fig.suptitle("Scale model specifications (dashed = current model / nominal; lower CRPS and KS is better)",
                 y=1.01, fontsize=10)
    _save(fig, ctx, "15_model_comparison.png")


# --- location model ----------------------------------------------------------------
def fig_location_diagnostics(ctx):
    """Did the location term remove the residual's dependence on the forecast itself?

    Panel (a): median deviation (residual - location) by predicted-change bin, one line per horizon,
    with the RAW residual median in the same bins drawn faintly. Before the location term the raw
    residual falls steeply with predicted change (the forecast overshoots in both directions, slope
    around -0.85); after it the solid lines should sit near zero across bins. Panel (b): median
    location by horizon and post-meal state -- how much the correction moves the centre, and where.
    """
    held = ctx["held"]
    needed = {"residual", "location", "predicted_change", "horizon_min", "minutes_since_carb_entry"}
    if not needed.issubset(held.columns):
        return _skip("16_location_diagnostics", f"{sorted(needed - set(held.columns))} (rerun run_residuals.py)")
    df = held.assign(_deviation=held["residual"] - held["location"],
                     _bin=pd.cut(held["predicted_change"], PREDICTED_CHANGE_BINS),
                     _post_meal=post_meal_flag(held))
    bins = df["_bin"].cat.categories
    x = np.arange(len(bins))
    horizons = sorted(df["horizon_min"].unique())
    colors = plt.cm.viridis(np.linspace(0.05, 0.9, len(horizons)))

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.0))
    for color, h in zip(colors, horizons):
        sub = df[df["horizon_min"] == h]
        raw = sub.groupby("_bin", observed=False)["residual"].median().reindex(bins)
        corrected = sub.groupby("_bin", observed=False)["_deviation"].median().reindex(bins)
        axes[0].plot(x, raw.values, color=color, linewidth=1.0, linestyle=":", alpha=0.4)
        axes[0].plot(x, corrected.values, color=color, marker="o", markersize=3, linewidth=1.3,
                     label=f"{h:g} min")
    axes[0].axhline(0, color="k", linewidth=0.7)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{b.left:g} to {b.right:g}" for b in bins], rotation=30, ha="right", fontsize=7)
    axes[0].set_xlabel("predicted change over the horizon (mg/dL)")
    axes[0].set_ylabel("median residual (mg/dL)")
    axes[0].set_title("Residual vs predicted change\n(solid = after location term, dotted = raw)")
    axes[0].legend(fontsize=7, frameon=False, title="horizon", title_fontsize=7)

    for flag, label in [(True, "post-meal"), (False, "other")]:
        sub = df[df["_post_meal"] == flag]
        if sub.empty:
            continue
        m = sub.groupby("horizon_min")["location"].median()
        axes[1].plot(m.index.values, m.values, marker="o", markersize=3, label=label)
    axes[1].axhline(0, color="k", linewidth=0.7)
    axes[1].set_xlabel("horizon (min)")
    axes[1].set_ylabel("median location m(x, h) (mg/dL)")
    axes[1].set_title("Location term by horizon x post-meal")
    axes[1].legend(fontsize=8, frameon=False)
    _save(fig, ctx, "16_location_diagnostics.png")


# --- one real day (figures 17-18) -----------------------------------------------------
def pick_ribbon_day(ctx):
    """The user and local date the day figures show (residual_schema.choose_day; pinnable via CLI)."""
    return choose_day(ctx["held"], ctx["meta"], ctx.get("ribbon_user"), ctx.get("ribbon_date"))


def train_median_residual_by_horizon(ctx):
    """The old constant location: per-horizon median residual on the training rows."""
    train = ctx["residuals"][~ctx["residuals"]["holdout"]]
    return train.groupby("horizon_min")["residual"].median()


def fig_day_ribbon(ctx):
    """One real day at each ribbon horizon, x = the time the forecast is FOR.

    Realized CGM (black), the raw static forecast made h minutes earlier (dotted), the corrected centre =
    forecast + location (solid), the interval band around it (shaded), and, as thin dashed lines, where the
    band would sit with the old constant location and the same width. Red points are misses. Triangles mark
    carb entries. The strip below shows location and width over the day: the state dependence as curves.
    """
    held = ctx["held"]
    if not {"centre", "location", "lower", "upper"}.issubset(held.columns):
        return _skip("17_day_ribbon", "centre/location/lower/upper (rerun run_residuals.py)")
    user, day = pick_ribbon_day(ctx)
    rows = held[(held["_userId"].astype(str) == user) & (held["timestamp"].dt.date == day)].copy()
    if rows.empty:
        return _skip("17_day_ribbon", f"no holdout rows for the chosen day {day}")
    old_location = train_median_residual_by_horizon(ctx)
    rows["target_time"] = rows["timestamp"] + pd.to_timedelta(rows["horizon_min"], unit="m")
    carb_entries = rows.loc[rows["minutes_since_carb_entry"] == 0, "timestamp"].drop_duplicates()

    fig, axes = plt.subplots(len(RIBBON_HORIZONS) + 1, 1, figsize=(13, 3.6 * len(RIBBON_HORIZONS) + 2.6),
                             sharex=True, gridspec_kw={"height_ratios": [3] * len(RIBBON_HORIZONS) + [1.6]})
    for ax, h in zip(axes, RIBBON_HORIZONS):
        r = rows[rows["horizon_min"] == h].sort_values("target_time")
        x = r["target_time"]
        half_lo, half_hi = r["lower"] - r["centre"], r["upper"] - r["centre"]
        old_centre = r["predicted"] + old_location.get(h, 0.0)
        ax.fill_between(x, r["lower"], r["upper"], color="tab:blue", alpha=0.18, label="interval (location + scale)")
        ax.plot(x, np.maximum(old_centre + half_lo, GLUCOSE_FLOOR_MG_DL), color="tab:gray", linewidth=0.8, linestyle="--", label="band with constant location")
        ax.plot(x, old_centre + half_hi, color="tab:gray", linewidth=0.8, linestyle="--")
        ax.plot(x, r["predicted"], color="tab:purple", linewidth=1.0, linestyle=":", label="static forecast")
        ax.plot(x, r["centre"], color="tab:blue", linewidth=1.4, label="forecast + location")
        ax.plot(x, r["realized"], color="k", linewidth=1.2, label="realized CGM")
        miss = ~r["covered"].astype(bool)
        ax.plot(x[miss], r.loc[miss, "realized"], "o", color="tab:red", markersize=3.5, label="outside band")
        y_floor = ax.get_ylim()[0]
        ax.plot(carb_entries, np.full(len(carb_entries), y_floor + 4), "^", color="tab:orange", markersize=6,
                label="carb entry", clip_on=False)
        ax.set_ylabel("mg/dL")
        ax.set_title(f"{h}-min horizon", fontsize=10, loc="left")
        if h == RIBBON_HORIZONS[0]:
            ax.legend(fontsize=7.5, frameon=False, ncol=4, loc="upper left")
    strip = axes[-1]
    twin = strip.twinx()
    for h, colour in zip(RIBBON_HORIZONS, ("tab:blue", "tab:green")):
        r = rows[rows["horizon_min"] == h].sort_values("target_time")
        strip.plot(r["target_time"], r["location"], color=colour, linewidth=1.2, label=f"location, {h} min")
        twin.plot(r["target_time"], r["width"], color=colour, linewidth=1.0, linestyle="--", label=f"width, {h} min")
    strip.axhline(0, color="k", linewidth=0.6)
    strip.set_ylabel("location (mg/dL)")
    twin.set_ylabel("width (mg/dL)")
    strip.set_xlabel("local time the forecast is for")
    lines = strip.get_legend_handles_labels()[0] + twin.get_legend_handles_labels()[0]
    strip.legend(lines, [l.get_label() for l in lines], fontsize=7.5, frameon=False, ncol=4, loc="upper left")
    fig.suptitle(f"One holdout day, user {user[:8]}..., {day}: static forecast, corrected centre, band", y=1.0)
    _save(fig, ctx, "17_day_ribbon.png")


def fig_origin_fans(ctx):
    """Four origins on the ribbon day, each with the next three hours: the static forecast at every horizon,
    the corrected centre, the band (linear between horizons), the band the constant location would give at the
    same width (dashed), the CGM trace, and what happened."""
    held = ctx["held"]
    if not {"centre", "lower", "upper", "cgm0"}.issubset(held.columns):
        return _skip("18_origin_fans", "centre/lower/upper/cgm0 (rerun run_residuals.py)")
    user, day = pick_ribbon_day(ctx)
    rows = held[(held["_userId"].astype(str) == user) & (held["timestamp"].dt.date == day)]
    if rows.empty:
        return _skip("18_origin_fans", f"no holdout rows for the chosen day {day}")
    horizons = sorted(rows["horizon_min"].unique())
    old_location = train_median_residual_by_horizon(ctx)
    trace = rows[rows["horizon_min"] == horizons[0]].sort_values("timestamp")   # cgm0 by origin = the CGM trace
    complete = rows.groupby("timestamp")["horizon_min"].nunique()
    origins_available = complete[complete == len(horizons)].index

    fig, axes = plt.subplots(1, len(FAN_ORIGIN_LOCAL_HOURS), figsize=(4.4 * len(FAN_ORIGIN_LOCAL_HOURS), 3.9), sharey=True)
    for ax, hour in zip(np.atleast_1d(axes), FAN_ORIGIN_LOCAL_HOURS):
        wanted = pd.Timestamp(day) + pd.Timedelta(hours=hour)
        if len(origins_available) == 0:
            ax.set_axis_off(); continue
        origin = origins_available[np.argmin(np.abs((origins_available - wanted).total_seconds()))]
        fan = rows[rows["timestamp"] == origin].sort_values("horizon_min")
        times = origin + pd.to_timedelta(fan["horizon_min"], unit="m")
        window = trace[(trace["timestamp"] >= origin - pd.Timedelta(minutes=60))
                       & (trace["timestamp"] <= origin + pd.Timedelta(minutes=max(horizons) + 10))]
        cgm_at_origin = float(fan["cgm0"].iloc[0])
        x_fan = pd.DatetimeIndex([origin]).append(pd.DatetimeIndex(times))
        ax.fill_between(x_fan, np.r_[cgm_at_origin, fan["lower"]], np.r_[cgm_at_origin, fan["upper"]],
                        color="tab:blue", alpha=0.18, label="interval (location + scale)")
        old_centre = fan["predicted"].to_numpy() + fan["horizon_min"].map(old_location).fillna(0.0).to_numpy()
        half_lo, half_hi = (fan["lower"] - fan["centre"]).to_numpy(), (fan["upper"] - fan["centre"]).to_numpy()
        ax.plot(x_fan, np.r_[cgm_at_origin, np.maximum(old_centre + half_lo, GLUCOSE_FLOOR_MG_DL)], "--", color="tab:gray", linewidth=0.9,
                label="band with constant location")
        ax.plot(x_fan, np.r_[cgm_at_origin, old_centre + half_hi], "--", color="tab:gray", linewidth=0.9)
        ax.plot(x_fan, np.r_[cgm_at_origin, fan["predicted"]], ":", color="tab:purple", marker="o", markersize=3, label="static forecast")
        ax.plot(x_fan, np.r_[cgm_at_origin, fan["centre"]], "-", color="tab:blue", marker="o", markersize=3, label="forecast + location")
        ax.plot(window["timestamp"], window["cgm0"], color="k", linewidth=1.2, label="realized CGM")
        ax.plot(times, fan["realized"], "o", color="k", markersize=3)
        ax.axvline(origin, color="k", linewidth=0.7, linestyle="--")
        ax.set_title(f"origin {origin.strftime('%H:%M')}", fontsize=10)
        ax.tick_params(axis="x", labelsize=7, rotation=30)
    np.atleast_1d(axes)[0].set_ylabel("mg/dL")
    np.atleast_1d(axes)[0].legend(fontsize=7.5, frameon=False, loc="upper left")
    fig.suptitle(f"Forecast fans on the ribbon day, user {user[:8]}..., {day}", y=1.02)
    _save(fig, ctx, "18_origin_fans.png")


# --- the model ladder on one real day (figures 19-20; needs evaluation/day_ladder.py) ---------
def fig_ladder_ribbons(ctx):
    """One ribbon per ladder rung, simplest at the top, all at one horizon on the same real day: realized CGM,
    the static forecast (identical in every panel), each rung's corrected centre and band, and its misses.
    Panel titles carry that day's coverage and median width for the rung."""
    ladder = ctx.get("day_ladder")
    if ladder is None or ladder.empty:
        return _skip("19_ladder_ribbons", f"{DAY_LADDER_TABLE} (run evaluation/day_ladder.py)")
    h = ctx["ladder_horizon"]
    rows = ladder[ladder["horizon_min"] == h].copy()
    if rows.empty:
        return _skip("19_ladder_ribbons", f"no ladder rows at {h} min")
    rows["target_time"] = rows["timestamp"] + pd.to_timedelta(rows["horizon_min"], unit="m")
    rungs = [(spec, label) for spec, label in LADDER if (rows["spec"] == spec).any()]
    fig, axes = plt.subplots(len(rungs), 1, figsize=(13, 2.9 * len(rungs)), sharex=True, sharey=True)
    for ax, (spec, label) in zip(np.atleast_1d(axes), rungs):
        r = rows[rows["spec"] == spec].sort_values("target_time")
        x = r["target_time"]
        ax.fill_between(x, r["lower"], r["upper"], color="tab:blue", alpha=0.18)
        ax.plot(x, r["predicted"], ":", color="tab:purple", linewidth=1.0, label="point forecast (persistence or Loop static)")
        ax.plot(x, r["centre"], "-", color="tab:blue", linewidth=1.3, label="centre")
        ax.plot(x, r["realized"], "-", color="k", linewidth=1.1, label="realized CGM")
        miss = ~r["covered"].astype(bool)
        ax.plot(x[miss], r.loc[miss, "realized"], "o", color="tab:red", markersize=3.2, label="outside band")
        ax.set_title(f"{label}   (this day: coverage {r['covered'].mean():.2f}, median width {r['width'].median():.0f} mg/dL)",
                     fontsize=9.5, loc="left")
        ax.set_ylabel("mg/dL")
    np.atleast_1d(axes)[0].legend(fontsize=7.5, frameon=False, ncol=4, loc="upper left")
    np.atleast_1d(axes)[-1].set_xlabel("local time the forecast is for")
    user, day = str(rows["_userId"].iloc[0]), pd.Timestamp(rows["timestamp"].iloc[0]).date()
    fig.suptitle(f"The model ladder on one holdout day, {h}-min horizon, user {user[:8]}..., {day}", y=1.0)
    _save(fig, ctx, "19_ladder_ribbons.png")


def fig_ladder_summary(ctx):
    """Leave-one-user-out scores of the ladder rungs in order, one colour per forecaster whose comparison table was
    given (--compare-dirs): CRPS and PIT KS per held-out user with medians, and the median conditional coverage per
    origin state. The justification for each step of the ladder, and the test of whether a forecast's own predicted
    change adds interval information once every forecaster gets the same state model."""
    tables = [(label, table) for label, table in ctx.get("comparison_tables", []) if table is not None and not table.empty]
    if not tables:
        return _skip("20_ladder_summary", "model_comparison.csv (run evaluation/compare_models.py)")
    ladder_specs = [spec for spec, _ in LADDER]
    tables = [(label, t[(t["split"] == "louo") & t["spec"].isin(ladder_specs)]) for label, t in tables]
    tables = [(label, t) for label, t in tables if not t.empty]
    if not tables:
        return _skip("20_ladder_summary", "no leave-one-user-out rows for the ladder specs")
    rungs = [(spec, label) for spec, label in LADDER if any((t["spec"] == spec).any() for _, t in tables)]
    y = np.arange(len(rungs))[::-1]
    colours = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    offsets = np.linspace(-0.2, 0.2, len(tables)) if len(tables) > 1 else [0.0]
    fig, axes = plt.subplots(1, 3, figsize=(16, 0.75 * len(rungs) + 2.6), sharey=True)
    for ax, (metric, title) in zip(axes[:2], [("crps", "CRPS (mg/dL), one point per held-out user"),
                                             ("pit_ks", "PIT KS, one point per held-out user")]):
        for (label, t), offset, colour in zip(tables, offsets, colours):
            for yi, (spec, _) in zip(y, rungs):
                vals = t.loc[t["spec"] == spec, metric].to_numpy()
                if len(vals) == 0:
                    continue
                ax.plot(vals, np.full(len(vals), yi + offset) + (RNG_PLOT.random(len(vals)) - 0.5) * 0.12, "o",
                        markersize=4.5, alpha=0.55, color=colour, label=label if yi == y[0] else None)
                ax.plot([np.median(vals)] * 2, [yi + offset - 0.14, yi + offset + 0.14], "-", color=colour, linewidth=2.5)
        ax.set_title(title, fontsize=10)
    axes[0].legend(fontsize=8, frameon=False, loc="lower right", title="forecaster", title_fontsize=8)
    ax = axes[2]
    for (label, t), offset, colour in zip(tables, offsets, colours):
        for column, state_label, marker in CONDITIONAL_COVERAGE_COLUMNS:
            if column not in t.columns:
                continue
            med = [t.loc[t["spec"] == spec, column].median() for spec, _ in rungs]
            ax.plot(med, y + offset, marker, markersize=7, color=colour, alpha=0.85,
                    label=state_label if offset == offsets[0] else None)
    ax.axvline(COMPARISON_NOMINAL, color="k", linestyle="--", linewidth=0.9)
    ax.set_title("median coverage at 95% by origin state (marker = state, colour = forecaster)", fontsize=10)
    ax.legend(fontsize=7.5, frameon=False, loc="lower left")
    axes[0].set_yticks(y)
    axes[0].set_yticklabels([label for _, label in rungs], fontsize=8.5)
    fig.suptitle("The model ladder, simplest (top) to final (bottom): leave-one-user-out, "
                 + " vs ".join(label for label, _ in tables), y=1.02, fontsize=11)
    _save(fig, ctx, "20_ladder_summary.png")


# --- point-forecast error: basic Loop vs persistence (figure 21) ------------------------
def fig_point_forecast_error(ctx):
    """Mean absolute error of the two POINT forecasts by horizon, no interval model involved: persistence (glucose
    stays where it is) against Loop's DISPLAYED forecast (dosingDecision.bgForecast, as the app computed it), on
    origin sets: all origins, and within EVENT_WINDOW_MIN after a carb entry, after a bolus, after either. The bias panels show predicted − realized: positive means the forecast overshot. Needs outputs_persistence/ and outputs_loop_displayed/."""
    pers = ctx.get("persistence_residuals")
    displayed = ctx.get("loop_displayed_residuals")
    if pers is None or displayed is None:
        return _skip("21_point_forecast_error", "outputs_persistence/ and outputs_loop_displayed/ residual tables")
    keys = ["_userId", "origin_index", "horizon_min"]
    state_columns = [c for c in ("minutes_since_carb_entry", "minutes_since_bolus") if c in pers.columns]
    state = pers[keys + ["residual"] + state_columns].rename(columns={"residual": "residual_pers"})
    both = state.merge(displayed[keys + ["residual"]].rename(columns={"residual": "residual_disp"}), on=keys)
    since_carb = both["minutes_since_carb_entry"]
    since_bolus = both["minutes_since_bolus"] if "minutes_since_bolus" in both else pd.Series(np.nan, index=both.index)
    after_carb = (since_carb <= EVENT_WINDOW_MIN).to_numpy()
    after_bolus = (since_bolus <= EVENT_WINDOW_MIN).to_numpy()
    sets = [("all origins", np.ones(len(both), dtype=bool)),
            (f"within {EVENT_WINDOW_MIN} min after a carb entry", after_carb),
            (f"within {EVENT_WINDOW_MIN} min after a bolus", after_bolus),
            (f"within {EVENT_WINDOW_MIN} min after either", after_carb | after_bolus)]
    colours = ["0.5", "tab:red", "tab:blue", "tab:purple"]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.4))

    def draw(ax_mae, ax_bias, horizon_max, anchor_at_origin):
        """One row: MAE and bias against horizon for every set, optionally with the origin's zero error drawn."""
        for (label, mask), colour in zip(sets, colours):
            g = both[mask].groupby("horizon_min")
            h = g.size().index.values
            keep = h <= horizon_max
            h_plot = h[keep]
            mae_pers = g["residual_pers"].apply(lambda s: s.abs().mean()).values[keep]
            mae_disp = g["residual_disp"].apply(lambda s: s.abs().mean()).values[keep]
            bias_pers = -g["residual_pers"].mean().values[keep]      # predicted − realized: positive = the forecast overshot
            bias_disp = -g["residual_disp"].mean().values[keep]
            if anchor_at_origin:                       # at horizon 0 both forecasts equal the origin reading
                h_plot = np.r_[0, h_plot]
                mae_pers, mae_disp = np.r_[0.0, mae_pers], np.r_[0.0, mae_disp]
                bias_pers, bias_disp = np.r_[0.0, bias_pers], np.r_[0.0, bias_disp]
            ax_mae.plot(h_plot, mae_pers, "o--", color=colour, markersize=4, label=f"persistence, {label}")
            ax_mae.plot(h_plot, mae_disp, "D-", color=colour, markersize=4, label=f"Loop forecast, {label}")
            ax_bias.plot(h_plot, bias_pers, "o--", color=colour, markersize=4, alpha=0.7, label=f"persistence, {label}")
            ax_bias.plot(h_plot, bias_disp, "D-", color=colour, markersize=4, label=f"Loop forecast, {label}")
        ax_bias.axhline(0, color="k", linewidth=0.7)
        ax_mae.set_ylabel("mean absolute error (mg/dL)")
        ax_bias.set_ylabel("mean signed error, predicted − realized (mg/dL)")

    draw(axes[0, 0], axes[0, 1], horizon_max=10_000, anchor_at_origin=False)
    # Bottom left: the DIFFERENCE, Loop's MAE relative to persistence's in percent, so the crossover is legible.
    for (label, mask), colour in zip(sets, colours):
        g = both[mask].groupby("horizon_min")
        h = g.size().index.values
        keep = h <= 30
        relative = (g["residual_disp"].apply(lambda s: s.abs().mean()).values / g["residual_pers"].apply(lambda s: s.abs().mean()).values - 1.0) * 100.0
        axes[1, 0].plot(h[keep], relative[keep], "D-", color=colour, markersize=5, label=label)
    axes[1, 0].axhline(0, color="k", linewidth=0.8)
    axes[1, 0].set_ylabel("Loop MAE relative to persistence (%)")
    axes[1, 0].legend(fontsize=8, frameon=False, title="below the line: Loop is more accurate", title_fontsize=8)
    # Bottom right: bias over the first 30 minutes, anchored at the origin.
    _mae_dummy = plt.figure()                              # draw() needs an MAE axis; use a throwaway
    draw(_mae_dummy.add_subplot(111), axes[1, 1], horizon_max=30, anchor_at_origin=True)
    plt.close(_mae_dummy)
    axes[0, 0].set_title("Point-forecast error: persistence (dashed) vs Loop's displayed forecast (solid)", fontsize=10)
    axes[0, 1].set_title("Bias (positive = forecast overshot realized)", fontsize=10)
    axes[1, 0].set_title("The first 30 minutes: Loop's error relative to persistence", fontsize=10)
    axes[1, 1].set_title("Bias, the first 30 minutes", fontsize=10)
    for ax in axes[1]:
        ax.set_xlim(-1, 31); ax.set_xticks(range(0, 31, 5))
    axes[0, 0].legend(fontsize=7.5, frameon=False)
    for ax in axes.ravel():
        ax.set_xlabel("horizon (min)")
    _save(fig, ctx, "21_point_forecast_error.png")


# --- the forecast at the bolus decision (figure 22; needs outputs_loop_bolus_time/) ---------
def fig_bolus_time_forecast(ctx):
    """Loop's BOLUS-TIME forecast (the normalBolus / watchBolus decisions: computed for the recommendation, with the
    carbs entered) against persistence from the same origins, by horizon: MAE, Loop's error relative to persistence
    in percent, and bias (predicted − realized; positive = the forecast overshot). Sets: all bolus decisions; meal boluses (a carb entry within 10 min); correction boluses
    (no carb entry in the previous three hours)."""
    bolus = ctx.get("loop_bolus_time_residuals")
    pers = ctx.get("persistence_residuals")
    if bolus is None or pers is None:
        return _skip("22_bolus_time_forecast", "outputs_loop_bolus_time/ and outputs_persistence/ residual tables")
    keys = ["_userId", "origin_index", "horizon_min"]
    both = (bolus[keys + ["residual", "minutes_since_carb_entry", "carbs_entered_recent_g"]].rename(columns={"residual": "residual_disp"})
                 .merge(pers[keys + ["residual"]].rename(columns={"residual": "residual_pers"}), on=keys))
    sets = [("all bolus decisions", np.ones(len(both), dtype=bool)),
            ("meal bolus (carb entry within 10 min)", (both["minutes_since_carb_entry"] <= 10).to_numpy()),
            ("correction bolus (no carbs in 3 h)", (both["carbs_entered_recent_g"] == 0).to_numpy())]
    colours = ["0.5", "tab:red", "tab:blue"]
    fig, axes = plt.subplots(3, 1, figsize=(9, 13), sharex=True)
    for (label, mask), colour in zip(sets, colours):
        g = both[mask].groupby("horizon_min")
        h = g.size().index.values
        mae_pers = g["residual_pers"].apply(lambda s: s.abs().mean()).values
        mae_disp = g["residual_disp"].apply(lambda s: s.abs().mean()).values
        axes[0].plot(h, mae_pers, "o--", color=colour, markersize=4, label=f"persistence, {label}")
        axes[0].plot(h, mae_disp, "D-", color=colour, markersize=4, label=f"Loop at the bolus, {label}")
        axes[1].plot(h, (mae_disp / mae_pers - 1.0) * 100.0, "D-", color=colour, markersize=5, label=label)
        axes[2].plot(h, -g["residual_pers"].mean().values, "o--", color=colour, markersize=4, alpha=0.7, label=f"persistence, {label}")
        axes[2].plot(h, -g["residual_disp"].mean().values, "D-", color=colour, markersize=4, label=f"Loop at the bolus, {label}")
    axes[0].set_ylabel("mean absolute error (mg/dL)"); axes[0].set_title("Error at the bolus decision", fontsize=10)
    axes[0].legend(fontsize=7, frameon=False)
    axes[1].axhline(0, color="k", linewidth=0.8); axes[1].set_ylabel("Loop MAE relative to persistence (%)")
    axes[1].set_title("Below the line: Loop is more accurate", fontsize=10); axes[1].legend(fontsize=8, frameon=False)
    axes[2].axhline(0, color="k", linewidth=0.7); axes[2].set_ylabel("mean signed error, predicted − realized (mg/dL)")
    axes[2].set_title("Bias (positive = forecast overshot realized)", fontsize=10); axes[2].legend(fontsize=7, frameon=False)
    axes[-1].set_xlabel("horizon (min)")
    _save(fig, ctx, "22_bolus_time_forecast.png")


# --- where the correlation between Loop's predicted change and the realized change comes from (figure 23) ------
CORRELATION_HORIZONS = (30, 60, 180, 360)
CORRELATION_BINS = 10                          # predicted-change quantile bins for the binned medians


def _origin_sets(table):
    """Three DISJOINT origin sets from the meal clock: within EVENT_WINDOW_MIN after a carb entry; within it after a
    bolus but with no carb entry that recent (correction-like); neither (quiet)."""
    since_carb = table["minutes_since_carb_entry"].to_numpy()
    since_bolus = table["minutes_since_bolus"].to_numpy() if "minutes_since_bolus" in table else np.full(len(table), np.nan)
    after_carb = since_carb <= EVENT_WINDOW_MIN
    after_bolus_only = (since_bolus <= EVENT_WINDOW_MIN) & ~after_carb
    quiet = ~after_carb & ~after_bolus_only
    return [(f"≤{EVENT_WINDOW_MIN} min after a carb entry", after_carb, "C1"),
            (f"≤{EVENT_WINDOW_MIN} min after a bolus, no carb entry", after_bolus_only, "C2"),
            ("neither (quiet)", quiet, "C0")]


def _covariance_decomposition(x, y, sets):
    """Law of total covariance over disjoint origin sets: cov(x, y) = Σ_s p_s cov_s(x, y) [within]
    + Σ_s p_s (mean_s x − mean x)(mean_s y − mean y) [between]. Returned as shares of the total covariance."""
    total = np.cov(x, y)[0, 1]
    within, between = {}, 0.0
    for label, mask, _ in sets:
        if mask.sum() < 2:
            within[label] = 0.0
            continue
        p = mask.mean()
        within[label] = p * np.cov(x[mask], y[mask])[0, 1] / total
        between += p * (x[mask].mean() - x.mean()) * (y[mask].mean() - y.mean()) / total
    return within, between


def _partial_correlation(x, y, controls):
    """Correlation of x and y after regressing both on the control columns (with intercept)."""
    design = np.column_stack([np.ones(len(x))] + list(controls))
    x_res = x - design @ np.linalg.lstsq(design, x, rcond=None)[0]
    y_res = y - design @ np.linalg.lstsq(design, y, rcond=None)[0]
    return np.corrcoef(x_res, y_res)[0, 1]


def fig_correlation_source(ctx):
    """Where does the correlation between Loop's predicted change and the realized change come from, given that
    Loop's point forecast is less accurate than persistence beyond ~25 min? Top: binned medians of the realized
    change against Loop's predicted change per horizon, all origins (black) and the three disjoint origin sets, with
    the 1:1 line and the fitted recalibration line. Bottom: Pearson r and the recalibration slope by horizon per set,
    and the covariance decomposition into within-set and between-set shares. The r panel also carries the PARTIAL
    correlation given the origin glucose level (and given level + 30-min momentum): what is left of the correlation
    once mean reversion of the level, which Loop encodes through IOB, is taken out."""
    displayed = ctx.get("loop_displayed_residuals")
    if displayed is None:
        return _skip("23_correlation_source", "outputs_loop_displayed/ residual table")
    needed = ["predicted_change", "residual", "horizon_min", "minutes_since_carb_entry"]
    table = displayed.dropna(subset=needed)
    horizons_all = sorted(table["horizon_min"].unique())
    fig, axes = plt.subplots(2, 4, figsize=(18, 9))
    fig.subplots_adjust(hspace=0.4, wspace=0.3)
    # top row: binned medians per horizon
    for ax, h in zip(axes[0], CORRELATION_HORIZONS):
        sub = table[table["horizon_min"] == h]
        x = sub["predicted_change"].to_numpy()
        y = (sub["predicted_change"] + sub["residual"]).to_numpy()   # realized change
        lim = np.nanpercentile(np.abs(np.concatenate([x, y])), 99)
        ax.hexbin(x, y, gridsize=60, cmap="Greys", bins="log", extent=(-lim, lim, -lim, lim), mincnt=1, alpha=0.9)
        edges = np.quantile(x, np.linspace(0, 1, CORRELATION_BINS + 1))
        for label, mask, colour in [("all origins", np.ones(len(sub), dtype=bool), "k")] + _origin_sets(sub):
            xs, ys = [], []
            for lo, hi in zip(edges[:-1], edges[1:]):
                inb = mask & (x >= lo) & (x < hi if hi < edges[-1] else x <= hi)
                if inb.sum() >= 30:
                    xs.append(np.median(x[inb])); ys.append(np.median(y[inb]))
            ax.plot(xs, ys, "o-", color=colour, markersize=4.5, linewidth=1.6 if colour == "k" else 1.2,
                    label=f"{label} (r={np.corrcoef(x[mask], y[mask])[0, 1]:.2f}, n={mask.sum():,})")
        b, a = np.polyfit(x, y, 1)
        grid = np.array([-lim, lim])
        ax.plot(grid, grid, "--", color="0.4", linewidth=0.9, label="1:1 (forecast right on average)")
        ax.plot(grid, a + b * grid, ":", color="C3", linewidth=1.4, label=f"fit: {a:.0f} + {b:.2f} × predicted")
        ax.axhline(0, color="0.7", linewidth=0.6); ax.axvline(0, color="0.7", linewidth=0.6)
        ax.set_xlim(-lim, lim); ax.set_ylim(-lim, lim)
        ax.set_title(f"{h}-min horizon", fontsize=10)
        ax.set_xlabel("Loop predicted change (mg/dL)"); ax.legend(fontsize=6.5, frameon=False, loc="upper left")
    axes[0][0].set_ylabel("realized change (mg/dL)")
    # bottom row: r by horizon per set, slope by horizon per set, covariance shares
    rows = []
    for h in horizons_all:
        sub = table[table["horizon_min"] == h]
        x = sub["predicted_change"].to_numpy(); y = (sub["predicted_change"] + sub["residual"]).to_numpy()
        sets = _origin_sets(sub)
        within, between = _covariance_decomposition(x, y, sets)
        level = sub["cgm0"].to_numpy()
        momentum = sub["prior_change_30"].fillna(0).to_numpy()
        row = {"horizon_min": h, "r_all": np.corrcoef(x, y)[0, 1], "b_all": np.polyfit(x, y, 1)[0], "between": between,
               "r_given_level": _partial_correlation(x, y, [level]),
               "r_given_level_momentum": _partial_correlation(x, y, [level, momentum]),
               "r_level_realized": np.corrcoef(level, y)[0, 1], "r_level_predicted": np.corrcoef(level, x)[0, 1]}
        for label, mask, _ in sets:
            row[f"r_{label}"] = np.corrcoef(x[mask], y[mask])[0, 1] if mask.sum() > 2 else np.nan
            row[f"b_{label}"] = np.polyfit(x[mask], y[mask], 1)[0] if mask.sum() > 2 else np.nan
            row[f"within_{label}"] = within[label]
            row[f"share_{label}"] = mask.mean()
        rows.append(row)
    summary = pd.DataFrame(rows)
    sets = _origin_sets(table[table["horizon_min"] == horizons_all[0]])
    ax = axes[1][0]
    ax.plot(summary["horizon_min"], summary["r_all"], "ko-", label="all origins")
    for label, _, colour in sets:
        ax.plot(summary["horizon_min"], summary[f"r_{label}"], "o-", color=colour, label=label)
    ax.plot(summary["horizon_min"], summary["r_given_level"], "k--", linewidth=1.4,
            label="all origins, partial r given glucose level")
    ax.plot(summary["horizon_min"], summary["r_given_level_momentum"], "k:", linewidth=1.4,
            label="… given level and 30-min momentum")
    ax.plot(summary["horizon_min"], -summary["r_level_realized"], "-", color="0.55", linewidth=1.2,
            label="level alone: −r(level, realized change)")
    ax.set_title("Pearson r: predicted vs realized change", fontsize=10); ax.set_xlabel("horizon (min)"); ax.set_ylim(0, 1)
    ax.legend(fontsize=6.5, frameon=False)
    ax = axes[1][1]
    ax.plot(summary["horizon_min"], summary["b_all"], "ko-", label="all origins")
    for label, _, colour in sets:
        ax.plot(summary["horizon_min"], summary[f"b_{label}"], "o-", color=colour, label=label)
    ax.axhline(1, color="0.4", linestyle="--", linewidth=0.9)
    ax.set_title("recalibration slope b (realized ≈ a + b × predicted)", fontsize=10); ax.set_xlabel("horizon (min)")
    ax.legend(fontsize=7, frameon=False)
    ax = axes[1][2]
    bottom = np.zeros(len(summary))
    for label, _, colour in sets:
        ax.bar(summary["horizon_min"].astype(str), summary[f"within_{label}"], bottom=bottom, color=colour, alpha=0.85,
               label=f"within: {label}")
        bottom += summary[f"within_{label}"].to_numpy()
    ax.bar(summary["horizon_min"].astype(str), summary["between"], bottom=bottom, color="0.5", alpha=0.85,
           label="between sets (set means differ in both)")
    ax.axhline(1, color="k", linewidth=0.6)
    ax.set_title("share of cov(predicted, realized) by origin set", fontsize=10); ax.set_xlabel("horizon (min)")
    ax.tick_params(axis="x", labelsize=7); ax.legend(fontsize=6.5, frameon=False, loc="lower left")
    ax = axes[1][3]
    for label, _, colour in sets:
        ax.plot(summary["horizon_min"], 100 * summary[f"share_{label}"], "o-", color=colour, label=label)
    ax.set_title("share of origins in each set (%)", fontsize=10); ax.set_xlabel("horizon (min)"); ax.legend(fontsize=7, frameon=False)
    fig.suptitle("Where the predicted-vs-realized correlation comes from: Loop's displayed forecast, series decisions, "
                 "origin sets by the meal clock", y=0.995, fontsize=11)
    _save(fig, ctx, "23_correlation_source.png")
    summary.to_csv(os.path.join(ctx["fig_dir"], "23_correlation_source.csv"), index=False)
    print(summary.round(3).to_string(index=False))


# --- the interval right after a carb entry / bolus (figures 24-25) --------------------------------------------
EVENT_GRAMS_RANGE = (25, 80)                   # a representative meal entry
EVENT_BOLUS_WITHIN_MIN = 10                    # a bolus this close to the entry (before, or at the next tick after)
EVENT_CLEAN_GAP_MIN = 180                      # no other carb entry this long before or after
EVENT_TRACE_MIN = (-90, 270)                   # trace extent around the entry
EVENT_FAN_OFFSETS_MIN = (-20, -5, 5, 20, 60)   # origins whose fans are drawn
EVENT_FAN_HORIZON_MAX = 180
EVENT_PATH_MIN = (-60, 120)                    # origin range for the location / width paths
EVENT_PATH_HORIZONS = (30, 60, 180)
EVENT_DAYTIME_HOURS = (7, 20)
EVENT_GALLERY_OFFSETS_MIN = (-5, 15)
TICK_MIN = 5
FAN_COLOURS_BEFORE = ["#1f4e9c", "#6fa0e0"]                    # origins before the entry, dark to light blue
FAN_COLOURS_AFTER = ["#f4a261", "#e76f51", "#9d0208"]          # origins after the entry, light to dark red


def _fan_colours(offsets):
    """Distinct colours for fan origins: blues before the entry, reds after -- no pale mid-map colour that vanishes."""
    before = [o for o in offsets if o < 0]
    after = [o for o in offsets if o >= 0]
    colours = {}
    for o, c in zip(before, FAN_COLOURS_BEFORE[-len(before):] if before else []):
        colours[o] = c
    for o, c in zip(after, FAN_COLOURS_AFTER[-len(after):] if after else []):
        colours[o] = c
    return [colours[o] for o in offsets]


def _origins(held):
    """One row per origin (the shortest horizon's row), indexed by (_userId, origin_index)."""
    h0 = held["horizon_min"].min()
    return held[held["horizon_min"] == h0].set_index(["_userId", "origin_index"]).sort_index()


def select_events(held):
    """Clean carb-plus-bolus events in the holdout, ranked by how typical they are: entry size in EVENT_GRAMS_RANGE,
    a bolus within EVENT_BOLUS_WITHIN_MIN, no other entry within EVENT_CLEAN_GAP_MIN either side, every origin of the
    trace window present, every horizon up to EVENT_FAN_HORIZON_MAX present for the fan origins, daytime. Rank =
    standardized distance to the candidates' medians of grams, origin glucose and realized 60- and 180-min change."""
    origins = _origins(held)
    horizons_per_origin = held[held["horizon_min"] <= EVENT_FAN_HORIZON_MAX].groupby(["_userId", "origin_index"], observed=True).size()
    fan_horizons = int((held["horizon_min"].drop_duplicates() <= EVENT_FAN_HORIZON_MAX).sum())
    since_carb = origins["minutes_since_carb_entry"]
    grams = origins["carbs_entered_recent_g"]
    entries = origins[(since_carb == 0) & grams.between(*EVENT_GRAMS_RANGE)]
    gap_ticks = EVENT_CLEAN_GAP_MIN // TICK_MIN
    trace_ticks = range(EVENT_TRACE_MIN[0] // TICK_MIN, EVENT_TRACE_MIN[1] // TICK_MIN + 1)
    fan_ticks = [o // TICK_MIN for o in EVENT_FAN_OFFSETS_MIN]
    rows = []
    for (user, index), entry in entries.iterrows():
        keys = [(user, index + k) for k in trace_ticks]
        if not all(k in origins.index for k in keys):
            continue
        before, after = origins.loc[(user, index - 1)], origins.loc[(user, index + 1)]
        far_after = origins.loc[(user, index + gap_ticks)] if (user, index + gap_ticks) in origins.index else None
        clean_before = pd.isna(before["minutes_since_carb_entry"]) or before["minutes_since_carb_entry"] >= EVENT_CLEAN_GAP_MIN
        clean_after = far_after is not None and far_after["minutes_since_carb_entry"] >= EVENT_CLEAN_GAP_MIN
        if not (clean_before and clean_after):
            continue
        bolus_before = entry["minutes_since_bolus"] <= EVENT_BOLUS_WITHIN_MIN
        bolus_after = after["minutes_since_bolus"] <= TICK_MIN
        if not (bolus_before or bolus_after):
            continue
        if not all(horizons_per_origin.get((user, index + k), 0) >= fan_horizons for k in fan_ticks):
            continue
        if not (EVENT_DAYTIME_HOURS[0] <= entry["hour_local"] <= EVENT_DAYTIME_HOURS[1]):
            continue
        at_entry = held[(held["_userId"] == user) & (held["origin_index"] == index)].set_index("horizon_min")
        if not {60, 180} <= set(at_entry.index):
            continue
        rows.append({"user": user, "origin_index": index, "timestamp": entry["timestamp"], "hour_local": entry["hour_local"],
                     "grams": grams.loc[(user, index)], "bolus_units": max(entry["bolus_recent_u"], after["bolus_recent_u"]),
                     "bolus_offset_min": -entry["minutes_since_bolus"] if bolus_before else TICK_MIN,
                     "cgm0": entry["cgm0"], "realized_change_60": at_entry.loc[60, "realized"] - entry["cgm0"],
                     "realized_change_180": at_entry.loc[180, "realized"] - entry["cgm0"]})
    events = pd.DataFrame(rows)
    if events.empty:
        return events
    typical = ["grams", "cgm0", "realized_change_60", "realized_change_180"]
    z = (events[typical] - events[typical].median()) / events[typical].std().replace(0, 1)
    events["typicality_distance"] = np.sqrt((z ** 2).sum(axis=1))
    return events.sort_values("typicality_distance").reset_index(drop=True)


def _pick_event(ctx, events):
    """The event to draw: the user's --event-user / --event-time if given, else the most typical candidate."""
    if ctx.get("event_user"):
        sub = events[events["user"].astype(str).str.startswith(ctx["event_user"])]
        if ctx.get("event_time"):
            sub = sub[pd.to_datetime(sub["timestamp"]) == pd.Timestamp(ctx["event_time"])]
        if sub.empty:
            raise SystemExit("figure 24: no clean event matches --event-user / --event-time; see 24_event_candidates.csv")
        return sub.iloc[0]
    return events.iloc[0]


def _fan(held_user, origin_index, horizon_max):
    rows = held_user[(held_user["origin_index"] == origin_index) & (held_user["horizon_min"] <= horizon_max)].sort_values("horizon_min")
    origin = rows.iloc[0]
    h = np.concatenate([[0], rows["horizon_min"].to_numpy()])
    centre = np.concatenate([[origin["cgm0"]], rows["centre"].to_numpy()])
    lower = np.concatenate([[origin["cgm0"]], rows["lower"].to_numpy()])
    upper = np.concatenate([[origin["cgm0"]], rows["upper"].to_numpy()])
    predicted = np.concatenate([[origin["cgm0"]], rows["predicted"].to_numpy()])
    return h, centre, lower, upper, predicted


def _draw_trace_and_fans(ax, held_user, event, offsets, horizon_max, trace_min, label_fans=True):
    index = int(event["origin_index"])
    origins = held_user[held_user["horizon_min"] == held_user["horizon_min"].min()].set_index("origin_index")
    ticks = np.arange(trace_min[0] // TICK_MIN, trace_min[1] // TICK_MIN + 1)
    trace = origins.reindex(index + ticks)["cgm0"].to_numpy()
    ax.plot(ticks * TICK_MIN, trace, "k-", linewidth=1.6, label="realized CGM", zorder=5)
    for offset, colour in zip(offsets, _fan_colours(offsets)):
        h, centre, lower, upper, predicted = _fan(held_user, index + offset // TICK_MIN, horizon_max)
        x = offset + h
        ax.fill_between(x, lower, upper, color=colour, alpha=0.13, linewidth=0)
        ax.plot(x, lower, "-", color=colour, linewidth=0.5, alpha=0.6); ax.plot(x, upper, "-", color=colour, linewidth=0.5, alpha=0.6)
        ax.plot(x, centre, "-", color=colour, linewidth=1.5,
                label=f"forecast issued at {offset:+d} min: interval centre and 95% band" if label_fans else None)
        ax.plot(x, predicted, ":", color=colour, linewidth=1.1,
                label="Loop's own forecast (dotted, same colour)" if (label_fans and offset == offsets[0]) else None)
        ax.plot([offset], [centre[0]], "o", color=colour, markersize=4.5, zorder=6,
                label="● the origin: when the forecast was issued and the glucose it starts from" if (label_fans and offset == offsets[0]) else None)
    ax.axvline(0, color="0.3", linestyle="--", linewidth=1.0)
    ax.axvline(event["bolus_offset_min"], color="0.3", linestyle=":", linewidth=1.0)
    ax.axhline(GLUCOSE_FLOOR_MG_DL, color="0.85", linewidth=0.6)
    ax.set_xlim(trace_min)
    ax.set_xlabel("minutes from the carb entry")
    ax.set_ylabel("mg/dL")


EVENT_STRIP_HEIGHT = 0.4                       # each location / scale strip, as a fraction of the trace panel's height


def _event_paths(held, events, horizon, ticks):
    """Location and scale by origin tick around the entry for every clean event: arrays (events × ticks)."""
    location, scale = [], []
    for _, event in events.iterrows():
        rows = (held[(held["_userId"] == event["user"]) & (held["horizon_min"] == horizon)]
                .set_index("origin_index").reindex(int(event["origin_index"]) + ticks))
        location.append(rows["location"].to_numpy(dtype=float)); scale.append(rows["scale"].to_numpy(dtype=float))
    return np.array(location), np.array(scale)


def fig_event_fans(ctx):
    """One representative carb-plus-bolus event in the holdout, Loop's displayed forecast with the full interval model.
    Top: the realized trace with the interval fan issued from origins before and after the entry (band = 95% interval,
    solid = centre, dotted = Loop's own forecast). Below, on the same time axis at EVENT_STRIP_HEIGHT of the trace's
    height: the location and the scale of the interval issued from each origin, at three horizons, with the median
    path over every clean candidate event dashed so the reader can judge whether this event's response is typical."""
    held = ctx.get("loop_displayed_holdout")
    if held is None:
        return _skip("24_event_interval_fans", "outputs_loop_displayed/holdout_intervals.parquet")
    events = select_events(held)
    if events.empty:
        return _skip("24_event_interval_fans", "no clean carb-plus-bolus event in the holdout")
    events.assign(user=events["user"].astype(str).str[:8]).to_csv(os.path.join(ctx["fig_dir"], "24_event_candidates.csv"), index=False)
    event = _pick_event(ctx, events)
    held_user = held[held["_userId"] == event["user"]]
    index = int(event["origin_index"])
    fig = plt.figure(figsize=(16, 11))
    grid = fig.add_gridspec(3, 1, height_ratios=[1, EVENT_STRIP_HEIGHT, EVENT_STRIP_HEIGHT], hspace=0.07)
    ax = fig.add_subplot(grid[0])
    _draw_trace_and_fans(ax, held_user, event, EVENT_FAN_OFFSETS_MIN, EVENT_FAN_HORIZON_MAX, EVENT_TRACE_MIN)
    ax.set_title(f"carb entry {event['grams']:.0f} g (dashed), bolus {event['bolus_units']:.1f} U at {int(round(event['bolus_offset_min'])):+d} min (dotted); "
                 f"origin glucose {event['cgm0']:.0f} mg/dL; realized change {event['realized_change_60']:+.0f} at 60 min, "
                 f"{event['realized_change_180']:+.0f} at 180", fontsize=10, loc="left")
    ax.legend(fontsize=8, frameon=False, ncol=2, loc="upper left")
    ax.text(0.995, 0.03, "each fan = one forecast, read from its dot forward; the value predicted for a given clock time is where a fan crosses it",
            transform=ax.transAxes, fontsize=8.5, ha="right", va="bottom", color="0.3")
    ax.set_xlabel("")
    ax.tick_params(labelbottom=False)
    # strips: location and scale of the interval issued from each origin, on the trace's time axis
    ticks = np.arange(EVENT_TRACE_MIN[0] // TICK_MIN, EVENT_TRACE_MIN[1] // TICK_MIN + 1)
    x = ticks * TICK_MIN
    strips = [fig.add_subplot(grid[1], sharex=ax), fig.add_subplot(grid[2], sharex=ax)]
    colours = plt.cm.viridis(np.linspace(0.1, 0.8, len(EVENT_PATH_HORIZONS)))
    for h, colour in zip(EVENT_PATH_HORIZONS, colours):
        rows = held_user[held_user["horizon_min"] == h].set_index("origin_index").reindex(index + ticks)
        median_location, median_scale = (np.nanmedian(a, axis=0) for a in _event_paths(held, events, h, ticks))
        strips[0].plot(x, rows["location"], "-", color=colour, linewidth=1.6, label=f"{h} min")
        strips[0].plot(x, median_location, "--", color=colour, linewidth=1.0)
        strips[1].plot(x, rows["scale"], "-", color=colour, linewidth=1.6, label=f"{h} min")
        strips[1].plot(x, median_scale, "--", color=colour, linewidth=1.0)
    for strip, (title, unit) in zip(strips, [("location of the forecast issued at each time (interval centre − Loop's forecast)", "mg/dL"),
                                            ("scale of the forecast issued at each time (95% width = scale × the standardized quantile span, about 8–10)", "mg/dL")]):
        strip.axvline(0, color="0.3", linestyle="--", linewidth=1.0)
        strip.axvline(event["bolus_offset_min"], color="0.3", linestyle=":", linewidth=1.0)
        strip.axhline(0, color="0.8", linewidth=0.6)
        strip.set_ylabel(unit)
        strip.text(0.005, 0.93, title, transform=strip.transAxes, fontsize=9.5, va="top")
    strips[0].plot([], [], "k--", linewidth=1.0, label="dashed: median over all clean events")
    strips[0].legend(fontsize=8, frameon=False, ncol=4, loc="upper right")
    strips[0].tick_params(labelbottom=False)
    strips[1].set_xlabel("minutes from the carb entry (for the strips: the time each forecast was issued)")
    user = str(event["user"])[:8]
    fig.suptitle(f"The interval around one carb-plus-bolus event: Loop's displayed forecast + full interval model, user {user}..., "
                 f"{pd.Timestamp(event['timestamp']).date()} (holdout)", y=0.965, fontsize=11)
    _save(fig, ctx, "24_event_interval_fans.png")
    print(f"  event drawn: user {user}..., {pd.Timestamp(event['timestamp'])}, {event['grams']:.0f} g, {event['bolus_units']:.1f} U, "
          f"typicality distance {event['typicality_distance']:.2f} (rank 1 of the clean candidates unless overridden)")


def fig_event_gallery(ctx):
    """Small multiples: the six most typical clean carb-plus-bolus events on distinct users, each with the fan issued
    just before and shortly after the entry. Shows figure 24's event is not a lucky pick."""
    held = ctx.get("loop_displayed_holdout")
    if held is None:
        return _skip("25_event_gallery", "outputs_loop_displayed/holdout_intervals.parquet")
    events = select_events(held)
    if events.empty:
        return _skip("25_event_gallery", "no clean carb-plus-bolus event in the holdout")
    picks = events.drop_duplicates("user").head(6)
    fig, axes = plt.subplots(2, 3, figsize=(17, 8.5), sharey=False)
    for ax, (_, event) in zip(axes.ravel(), picks.iterrows()):
        held_user = held[held["_userId"] == event["user"]]
        _draw_trace_and_fans(ax, held_user, event, EVENT_GALLERY_OFFSETS_MIN, EVENT_FAN_HORIZON_MAX, (-60, 240), label_fans=False)
        ax.set_title(f"user {str(event['user'])[:8]}..., {event['grams']:.0f} g, {event['bolus_units']:.1f} U, "
                     f"{int(event['hour_local']):02d}:{int(round((event['hour_local'] % 1) * 60)):02d} local", fontsize=9.5)
    for offset, colour in zip(EVENT_GALLERY_OFFSETS_MIN, _fan_colours(EVENT_GALLERY_OFFSETS_MIN)):
        axes[0][0].plot([], [], "-", color=colour, label=f"forecast issued at {offset:+d} min (dot = its start)")
    axes[0][0].legend(fontsize=8, frameon=False, loc="upper left")
    for ax in axes.ravel()[len(picks):]:
        ax.set_visible(False)
    fig.suptitle("The same view on the six most typical clean events (one per user): band = 95% interval, solid = centre, "
                 "dotted = Loop's forecast, dashed = carb entry, dotted vertical = bolus", y=0.995, fontsize=11)
    fig.tight_layout()
    _save(fig, ctx, "25_event_gallery.png")


FIGURES = {
    "01": fig_stream_qc,
    "02": fig_example_trace,
    "03": fig_residual_fan,
    "04": fig_scale_diagnostics,
    "05": fig_coverage,
    "06": fig_interval_width,
    "07": fig_per_user_coverage,
    "11": fig_holdout_drift,
    "12": fig_pit,
    "13": fig_coverage_curve,
    "14": fig_feature_ablation,
    "15": fig_model_comparison,
    "16": fig_location_diagnostics,
    "17": fig_day_ribbon,
    "18": fig_origin_fans,
    "19": fig_ladder_ribbons,
    "20": fig_ladder_summary,
    "21": fig_point_forecast_error,
    "22": fig_bolus_time_forecast,
    "23": fig_correlation_source,
    "24": fig_event_fans,
    "25": fig_event_gallery,
}


# --- loading -----------------------------------------------------------------------
def _forecaster_label(out_dir):
    """Name a run by the forecaster recorded in its run_meta.json, else by the directory name."""
    meta_path = os.path.join(out_dir, "run_meta.json")
    if os.path.exists(meta_path):
        meta = json.load(open(meta_path))
        if meta.get("forecaster"):
            return str(meta["forecaster"])
    return os.path.basename(os.path.normpath(out_dir))


def _read_optional(path, quiet=False):
    if not os.path.exists(path):
        if not quiet:
            print(f"  note: {os.path.basename(path)} not found")
        return None
    return pd.read_csv(path)


# Columns the point-forecast and event figures (21-25) read from a comparator run's residual table. Comparator tables
# are loaded only for the figures that need them, and only with these columns: four full loads in one process is
# what exhausted memory on 2026-09-06.
COMPARATOR_COLUMNS = ["_userId", "origin_index", "horizon_min", "timestamp", "hour_local", "holdout", "cgm0",
                      "predicted_change", "realized", "residual", "minutes_since_carb_entry", "minutes_since_bolus",
                      "carbs_entered_recent_g", "bolus_recent_u", "prior_change_30"]
COMPARATOR_NEEDS = {"21": ("persistence", "loop_displayed"), "22": ("persistence", "loop_bolus_time"),
                    "23": ("loop_displayed",)}
HOLDOUT_NEEDS = {"24": "loop_displayed", "25": "loop_displayed"}   # figures built from a run's holdout intervals
COMPARATOR_ONLY_FIGURES = {"20", "21", "22", "23", "24", "25"}    # figures that never read --out-dir's residual table


def _comparator_residuals(path, needed):
    if not needed or not os.path.exists(os.path.join(path, RESIDUAL_TABLE)):
        return None
    return load_table(path, RESIDUAL_TABLE, columns=COMPARATOR_COLUMNS)


def load_context(args, wanted):
    need_main = any(k not in COMPARATOR_ONLY_FIGURES for k in wanted)
    # The two parquet tables are required for the run figures; load_table exits with a pointer to run_residuals.py if absent.
    residuals = load_table(args.out_dir, RESIDUAL_TABLE) if need_main else None
    held = load_table(args.out_dir, HOLDOUT_TABLE) if need_main else None
    comparators = {name for k in wanted for name in COMPARATOR_NEEDS.get(k, ())}
    holdout_runs = {HOLDOUT_NEEDS[k] for k in wanted if k in HOLDOUT_NEEDS}
    comparator_dirs = {"persistence": args.persistence_dir, "loop_displayed": args.loop_displayed_dir,
                       "loop_bolus_time": args.loop_bolus_time_dir}

    # The example frame is still CSV (one user's tick frame, written with its index); parse its timestamp.
    example_frame = _read_optional(os.path.join(args.out_dir, "example_frame.csv"))
    if example_frame is not None and "timestamp" in example_frame.columns:
        example_frame["timestamp"] = pd.to_datetime(example_frame["timestamp"], errors="coerce")

    meta_path = os.path.join(args.out_dir, "run_meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    quantiles, standardized_train, alpha, location_params = load_model_aux(args.out_dir) if need_main else (None, None, None, None)
    nominal = args.nominal if args.nominal is not None else (1.0 - alpha if alpha else 0.95)

    return {
        "quantiles": quantiles,
        "standardized_train": standardized_train,
        "location_params": location_params,
        "residuals": residuals,
        "held": held,
        "stream_hours": _read_optional(os.path.join(args.out_dir, "stream_hours.csv")),
        "example_frame": example_frame,
        "dist_folds": _read_optional(os.path.join(args.out_dir, "dist_eval_folds.csv")),
        "dist_coverage": _read_optional(os.path.join(args.out_dir, "dist_eval_coverage.csv")),
        "dist_pit": _read_optional(os.path.join(args.out_dir, "dist_eval_pit.csv")),
        "dist_features": _read_optional(os.path.join(args.out_dir, "dist_eval_features.csv")),
        "comparison_tables": [(_forecaster_label(d), _read_optional(os.path.join(d, "model_comparison.csv"), quiet=True))
                              for d in (args.compare_dirs or [args.out_dir])],
        "meta": meta,
        "nominal": nominal,
        "dpi": args.dpi,
        "pit_ylim": ([float(v) for v in args.pit_ylim.split(",")] if args.pit_ylim else None),
        "fig_dir": args.fig_dir or os.path.join(args.out_dir, "figures"),
        "ribbon_user": args.ribbon_user,
        "ribbon_date": args.ribbon_date,
        "day_ladder": (pd.read_parquet(os.path.join(args.out_dir, DAY_LADDER_TABLE))
                       if os.path.exists(os.path.join(args.out_dir, DAY_LADDER_TABLE)) else None),
        "ladder_horizon": args.ladder_horizon,
        "persistence_residuals": _comparator_residuals(args.persistence_dir, "persistence" in comparators),
        "loop_displayed_residuals": _comparator_residuals(args.loop_displayed_dir, "loop_displayed" in comparators),
        "loop_bolus_time_residuals": _comparator_residuals(args.loop_bolus_time_dir, "loop_bolus_time" in comparators),
        "loop_displayed_holdout": (load_table(comparator_dirs["loop_displayed"], HOLDOUT_TABLE)
                                   if "loop_displayed" in holdout_runs
                                   and os.path.exists(os.path.join(comparator_dirs["loop_displayed"], HOLDOUT_TABLE)) else None),
        "event_user": args.event_user,
        "event_time": args.event_time,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "outputs"))
    parser.add_argument("--fig-dir", default=None, help="default: <out-dir>/figures")
    parser.add_argument("--nominal", type=float, default=None,
                        help="nominal level; default reads alpha from run_meta.json (0.95)")
    parser.add_argument("--dpi", type=int, default=130)
    parser.add_argument("--pit-ylim", default=None,
                        help="y limits for figure 12, e.g. 0.7,1.4 -- clips extreme folds so "
                             "the rest stay legible; the KS in each title is unaffected")
    parser.add_argument("--ribbon-user", default=None, help="figures 17-18: _userId (prefix ok); default = example user")
    parser.add_argument("--ribbon-date", default=None, help="figures 17-18: local date YYYY-MM-DD; default = a typical holdout day")
    parser.add_argument("--ladder-horizon", type=int, default=LADDER_HORIZON, help="figure 19: horizon in minutes")
    parser.add_argument("--compare-dirs", nargs="+", default=None,
                        help="figure 20: output dirs whose model_comparison.csv to overlay (default: --out-dir only)")
    parser.add_argument("--persistence-dir", default=os.path.join(PROJECT_ROOT, "outputs_persistence"),
                        help="figure 21: the persistence forecaster's output directory")
    parser.add_argument("--loop-displayed-dir", default=os.path.join(PROJECT_ROOT, "outputs_loop_displayed"),
                        help="figure 21: the displayed-forecast run's output directory (optional)")
    parser.add_argument("--loop-bolus-time-dir", default=os.path.join(PROJECT_ROOT, "outputs_loop_bolus_time"),
                        help="figure 22: the bolus-time-forecast run's output directory (optional)")
    parser.add_argument("--only", default=None,
                        help="comma-separated figure numbers, e.g. --only 05,16")
    parser.add_argument("--event-user", default=None, help="figure 24: _userId prefix of the event to draw (default: auto)")
    parser.add_argument("--event-time", default=None, help="figure 24: timestamp of the carb entry's origin tick (default: auto)")
    args = parser.parse_args()

    wanted = [k.strip().zfill(2) for k in args.only.split(",")] if args.only else list(FIGURES)
    unknown = [k for k in wanted if k not in FIGURES]
    if unknown:
        raise SystemExit(f"unknown figure(s) {unknown}; choose from {sorted(FIGURES)}")
    ctx = load_context(args, wanted)
    os.makedirs(ctx["fig_dir"], exist_ok=True)

    print(f"figures -> {ctx['fig_dir']}")
    for key in wanted:
        FIGURES[key](ctx)


if __name__ == "__main__":
    main()
