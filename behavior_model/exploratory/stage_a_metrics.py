"""Stage A metric suite + iteration history for the meta-analysis.

The point: every model iteration (split changes, feature redesigns, IOB
handling, new users) is scored on the SAME metric suite and appended to one
history file, so improvements are judged against a tracked baseline instead
of anecdotes. Three tiers:

  1. Holdout fit metrics -- one-step-ahead (teacher-forced: self-excitation
     features come from the user's real history), deterministic given the
     fit. Per hazard: held-out log-loss and its skill vs a train-rate
     constant baseline (out-of-sample McFadden R^2), rank AUC, calibration
     slope, and observed/predicted event-count ratio.
  2. Simulation metrics -- free-running Stage A rollouts (self-excitation
     from SIMULATED history), averaged over `n_sims` seeded replicates so an
     iteration-to-iteration delta can be judged against Monte Carlo spread
     (reported sd is across replicates). Rates, gap structure, diurnal-shape
     distance, overnight share, mark fidelity (KS), and the self-excitation
     ablation effect.
  3. Context counts -- event counts, days, meal_bolus_p; they explain metric
     shifts (e.g. a split change moving events between train and holdout).

Usage:
    compute + record:  build_tick_frame.py --label <iteration-name>
    meta-analysis:     python stage_a_metrics.py --report

History lives at outputs/behavior_traces/metrics_history.csv (git-ignored;
long format: one row per run_label x user x metric, with git commit and a
config JSON). Re-running a label replaces that (label, user) block, so runs
are idempotent. Comparisons are only like-for-like when the split config
matches -- the report annotates each run's split from its config column.
"""

import argparse
import json
import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import ks_2samp, rankdata
from statsmodels.tools.sm_exceptions import ConvergenceWarning

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (
    TICKS_PER_DAY,
    block_gap_minutes,
    block_spans,
    simulate_blocks,
)

N_SIMS_DEFAULT = 20
BASE_SEED_DEFAULT = 1000
OVERNIGHT_HOURS = (0, 6)   # [start, end) local hours; over-produced overnight
                           # corrections are a known Stage A failure mode
EPS_P = 1e-9               # probability clip for log-loss / logit

HISTORY_FILENAME = "metrics_history.csv"
DEFAULT_OUT_ROOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "outputs", "behavior_traces")
DEFAULT_HISTORY = os.path.join(DEFAULT_OUT_ROOT, HISTORY_FILENAME)
DEFAULT_META_DIR = os.path.join(DEFAULT_OUT_ROOT, "meta")

EVENT_PREFIXES = [("is_carb_entry", "carb"), ("is_correction", "corr")]

# headline simulation metrics echoed (running mean ± sd) as replicates land,
# so Monte Carlo convergence is visible during the run
PROGRESS_METRICS = ["corr_rate_ratio", "carb_rate_ratio",
                    "corr_gap_p10_sim_min", "ablation_gap_p10_delta_min"]

# every metric _replicate_metrics emits, in report order -- evaluate asserts
# against this so the list can't drift from the implementation; the dashboard
# uses it to tier metrics (an all-NaN sim metric is still a sim metric)
SIM_METRICS = [
    "corr_per_day_sim", "carb_per_day_sim",
    "corr_rate_ratio", "carb_rate_ratio",
    "corr_gap_median_sim_min", "corr_gap_p10_sim_min",
    "ablation_gap_p10_delta_min",
    "diurnal_tv_corrections", "diurnal_tv_carb_entries",
    "overnight_corr_share_sim",
    "ks_carb_grams", "ks_corr_units", "nan_corr_mark_frac",
]
FIT_METRIC_SUFFIXES = ("_holdout_nll", "_nll_skill", "_auc", "_cal_slope",
                       "_obs_pred_ratio")

# metrics shown in the meta-analysis figure, with reference geometry:
# line = target/na(i)ve level, band = acceptance region, real_ref = the
# per-user real-holdout metric plotted as a dashed reference
KEY_METRICS = [
    ("corr_rate_ratio", "corrections/day, sim / real"),
    ("carb_rate_ratio", "carb entries/day, sim / real"),
    ("corr_nll_skill", "correction NLL skill (holdout)"),
    ("carb_nll_skill", "carb-entry NLL skill (holdout)"),
    ("corr_auc", "correction AUC (holdout)"),
    ("carb_auc", "carb-entry AUC (holdout)"),
    ("corr_cal_slope", "correction calibration slope"),
    ("diurnal_tv_corrections", "diurnal TV distance, corrections"),
    ("diurnal_tv_carb_entries", "diurnal TV distance, carb entries"),
    ("overnight_corr_share_sim", "overnight correction share, sim"),
    ("corr_gap_p10_sim_min", "correction gap p10, sim (min)"),
    ("ablation_gap_p10_delta_min", "ablation Δ gap p10 (min)"),
]
REFERENCE = {
    "corr_rate_ratio": {"line": 1.0, "band": (0.8, 1.2)},
    "carb_rate_ratio": {"line": 1.0, "band": (0.8, 1.2)},
    "corr_nll_skill": {"line": 0.0},
    "carb_nll_skill": {"line": 0.0},
    "corr_auc": {"line": 0.5},
    "carb_auc": {"line": 0.5},
    "corr_cal_slope": {"line": 1.0},
    "carb_cal_slope": {"line": 1.0},
    "diurnal_tv_corrections": {"line": 0.0},
    "diurnal_tv_carb_entries": {"line": 0.0},
    "overnight_corr_share_sim": {"real_ref": "overnight_corr_share_real"},
    "corr_gap_p10_sim_min": {"real_ref": "corr_gap_p10_real_min"},
    "ablation_gap_p10_delta_min": {"line": 0.0},
}


# --------------------------------------------------------------------------
# Holdout fit metrics (one-step-ahead, deterministic)
# --------------------------------------------------------------------------

def _predicted_hazard(model, frame, features):
    X = frame[list(features)].to_numpy(dtype=float)
    z = model.params[0] + X @ model.params[1:]
    return 1.0 / (1.0 + np.exp(-np.clip(z, -35.0, 35.0)))


def _nll(y, p):
    """Mean per-tick negative log-likelihood (log loss)."""
    p = np.clip(p, EPS_P, 1.0 - EPS_P)
    return float(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)).mean())


def _auc(y, p):
    """Rank AUC (Mann-Whitney), tie-aware; NaN if one class is absent."""
    y = np.asarray(y, dtype=float) > 0
    n_pos, n_neg = int(y.sum()), int((~y).sum())
    if n_pos == 0 or n_neg == 0:
        return np.nan
    r = rankdata(p)
    return float((r[y].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def _calibration_slope(y, p):
    """Slope of a holdout logistic recalibration y ~ a + b*logit(p_hat).
    1 = calibrated; <1 = predictions too extreme; >1 = too timid."""
    y = np.asarray(y, dtype=float)
    if y.sum() == 0 or y.sum() == len(y):
        return np.nan
    p = np.clip(np.asarray(p, dtype=float), EPS_P, 1.0 - EPS_P)
    z = np.log(p / (1.0 - p))
    if np.ptp(z) < 1e-12:  # constant predictor, slope unidentified
        return np.nan
    X = np.column_stack([np.ones_like(z), z])
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = sm.Logit(y, X).fit(disp=0, maxiter=200)
        if not res.mle_retvals.get("converged", True):
            return np.nan
        return float(res.params[1])
    except Exception:
        return np.nan


def fit_metrics(train, holdout, hazards):
    """Per-hazard one-step-ahead metrics on the holdout. Teacher-forced:
    features (incl. self-excitation) come from the real record, so this
    isolates fit quality from rollout dynamics."""
    features = hazards["features"]
    out = {}
    for event, prefix in EVENT_PREFIXES:
        y = holdout[event].to_numpy(dtype=float)
        p = _predicted_hazard(hazards["models"][event], holdout, features)
        p0 = float(np.clip(train[event].mean(), EPS_P, 1.0 - EPS_P))
        nll = _nll(y, p)
        nll0 = _nll(y, np.full_like(y, p0))
        out[f"{prefix}_holdout_nll"] = nll
        out[f"{prefix}_nll_skill"] = 1.0 - nll / nll0 if nll0 > 0 else np.nan
        out[f"{prefix}_auc"] = _auc(y, p)
        out[f"{prefix}_cal_slope"] = _calibration_slope(y, p)
        out[f"{prefix}_obs_pred_ratio"] = (
            float(y.sum() / p.sum()) if p.sum() > 0 else np.nan)
    return out


# --------------------------------------------------------------------------
# Simulation metrics (free-running, per replicate)
# --------------------------------------------------------------------------

def _diurnal_tv(real_times, sim_times):
    """Total-variation distance between normalized hour-of-day histograms.
    0 = identical shape, 1 = disjoint. Shape only -- rate is tracked
    separately by the rate ratios."""
    if len(real_times) == 0 or len(sim_times) == 0:
        return np.nan
    hours = pd.Index(range(24))

    def _norm(times):
        counts = pd.to_datetime(times).dt.hour.value_counts()
        counts = counts.reindex(hours, fill_value=0).astype(float)
        return counts / counts.sum()

    return float(0.5 * (_norm(real_times) - _norm(sim_times)).abs().sum())


def _overnight_share(times):
    if len(times) == 0:
        return np.nan
    hour = pd.to_datetime(times).dt.hour
    lo, hi = OVERNIGHT_HOURS
    return float(((hour >= lo) & (hour < hi)).mean())


def _ks(a, b):
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a, b = a[np.isfinite(a)], b[np.isfinite(b)]
    if len(a) == 0 or len(b) == 0:
        return np.nan
    return float(ks_2samp(a, b).statistic)


def _percentile(values, q):
    return float(np.percentile(values, q)) if len(values) else np.nan


def _replicate_metrics(sim, sim_ablated, n_days, real_ref):
    """Metric dict for one simulation replicate (plus its paired ablated
    rollout, for the ablation delta)."""
    sim_corr = sim[sim["event"] == "correction"]
    sim_carb = sim[sim["event"] == "carb_entry"]
    gaps = block_gap_minutes(sim_corr["timestamp"], real_ref["spans"])
    gaps_abl = block_gap_minutes(
        sim_ablated.loc[sim_ablated["event"] == "correction", "timestamp"],
        real_ref["spans"])

    corr_per_day = len(sim_corr) / n_days
    carb_per_day = len(sim_carb) / n_days
    corr_marks = sim_corr["mark"].to_numpy(dtype=float)

    return {
        "corr_per_day_sim": corr_per_day,
        "carb_per_day_sim": carb_per_day,
        "corr_rate_ratio": (corr_per_day / real_ref["corr_per_day_real"]
                            if real_ref["corr_per_day_real"] > 0 else np.nan),
        "carb_rate_ratio": (carb_per_day / real_ref["carb_per_day_real"]
                            if real_ref["carb_per_day_real"] > 0 else np.nan),
        "corr_gap_median_sim_min": _percentile(gaps, 50),
        "corr_gap_p10_sim_min": _percentile(gaps, 10),
        "ablation_gap_p10_delta_min": _percentile(gaps_abl, 10) - _percentile(gaps, 10),
        "diurnal_tv_corrections": _diurnal_tv(
            real_ref["corr_times"], sim_corr["timestamp"]),
        "diurnal_tv_carb_entries": _diurnal_tv(
            real_ref["carb_times"], sim_carb["timestamp"]),
        "overnight_corr_share_sim": _overnight_share(sim_corr["timestamp"]),
        "ks_carb_grams": _ks(real_ref["carb_grams"], sim_carb["mark"]),
        "ks_corr_units": _ks(real_ref["corr_units"], corr_marks),
        "nan_corr_mark_frac": (float(np.isnan(corr_marks).mean())
                               if len(corr_marks) else np.nan),
    }


# --------------------------------------------------------------------------
# The suite
# --------------------------------------------------------------------------

def evaluate(result, n_sims=N_SIMS_DEFAULT, base_seed=BASE_SEED_DEFAULT,
             verbose=False, replicates_path=None):
    """Score one run_mvp result. Returns a tidy frame [metric, value, sd]:
    sd is the across-replicate spread for simulation metrics and NaN for
    deterministic ones. Same result + same base_seed => identical output
    (verbose logging and the replicate dump don't touch the RNG streams).

    `verbose` prints the running mean ± sd of PROGRESS_METRICS after each
    replicate; `replicates_path` saves every replicate's raw metric values
    (one row per replicate) so any metric's convergence can be checked."""
    train, holdout = result["train"], result["holdout"]
    frame, blocks = result["frame"], result["holdout_blocks"]
    spans = block_spans(frame, blocks)
    n_days = len(holdout) / TICKS_PER_DAY

    corr_times = holdout.loc[holdout["is_correction"], "timestamp"]
    carb_times = holdout.loc[holdout["is_carb_entry"], "timestamp"]
    real_gaps = block_gap_minutes(corr_times, spans)
    real_ref = {
        "spans": spans,
        "corr_per_day_real": len(corr_times) / n_days,
        "carb_per_day_real": len(carb_times) / n_days,
        "corr_times": corr_times,
        "carb_times": carb_times,
        "carb_grams": holdout.loc[holdout["is_carb_entry"], "carb_entry_g"],
        "corr_units": holdout.loc[holdout["is_correction"], "bolus_u"],
    }

    deterministic = {
        "train_days": len(train) / TICKS_PER_DAY,
        "holdout_days": n_days,
        "n_train_corrections": float(train["is_correction"].sum()),
        "n_holdout_corrections": float(holdout["is_correction"].sum()),
        "n_train_carb_entries": float(train["is_carb_entry"].sum()),
        "n_holdout_carb_entries": float(holdout["is_carb_entry"].sum()),
        "meal_bolus_p": result["meal_bolus_p"],
        "corr_per_day_real": real_ref["corr_per_day_real"],
        "carb_per_day_real": real_ref["carb_per_day_real"],
        "corr_gap_median_real_min": _percentile(real_gaps, 50),
        "corr_gap_p10_real_min": _percentile(real_gaps, 10),
        "overnight_corr_share_real": _overnight_share(corr_times),
        **fit_metrics(train, holdout, result["hazards"]),
    }

    replicates = []
    for k in range(n_sims):
        # sparse-recommendation NaN marks surface as nan_corr_mark_frac, not
        # as n_sims repeated warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sim = simulate_blocks(
                frame, blocks, result["hazards"], result["marks"],
                result["meal_bolus_p"], np.random.default_rng([base_seed, k, 0]))
            sim_abl = simulate_blocks(
                frame, blocks, result["hazards_ablated"], result["marks"],
                result["meal_bolus_p"], np.random.default_rng([base_seed, k, 1]))
        replicates.append(_replicate_metrics(sim, sim_abl, n_days, real_ref))
        if verbose:
            so_far = pd.DataFrame(replicates)
            parts = []
            for name in PROGRESS_METRICS:
                mean, sd = so_far[name].mean(), so_far[name].std(ddof=1)
                cell = f"{name} {mean:.3g}"
                if np.isfinite(sd):
                    cell += f" ±{sd:.2g}"
                parts.append(cell)
            print(f"    replicate {k + 1:>2}/{n_sims}: " + "  ".join(parts))
    rep = pd.DataFrame(replicates)
    if replicates_path:
        rep.insert(0, "replicate", np.arange(n_sims))
        rep.to_csv(replicates_path, index=False)
        rep = rep.drop(columns="replicate")

    assert set(rep.columns) == set(SIM_METRICS), \
        f"SIM_METRICS drifted from _replicate_metrics: " \
        f"{set(rep.columns) ^ set(SIM_METRICS)}"
    rows = [{"metric": m, "value": v, "sd": np.nan}
            for m, v in deterministic.items()]
    rows += [{"metric": m,
              "value": rep[m].mean(),
              "sd": rep[m].std(ddof=1) if n_sims > 1 else np.nan}
             for m in SIM_METRICS]
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Iteration history
# --------------------------------------------------------------------------

def git_commit():
    cwd = os.path.dirname(os.path.abspath(__file__))
    try:
        rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, cwd=cwd,
                             check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"],
                               capture_output=True, text=True, cwd=cwd,
                               check=True).stdout.strip()
        return rev + ("+dirty" if dirty else "")
    except Exception:
        return "unknown"


def append_history(metrics, run_label, user, config, history_path=DEFAULT_HISTORY,
                   note=""):
    """Append one run's metrics; an existing (run_label, user) block is
    replaced, so re-running an iteration is idempotent. `note` is the
    human-readable description of what this iteration changed -- it rides
    along on every row and surfaces in the report and dashboard."""
    rec = metrics.copy()
    rec.insert(0, "run_label", run_label)
    rec.insert(1, "run_ts",
               datetime.now(timezone.utc).isoformat(timespec="seconds"))
    rec.insert(2, "git_commit", git_commit())
    rec.insert(3, "user", user)
    rec["note"] = note or ""
    rec["config"] = json.dumps(config, sort_keys=True)

    if os.path.exists(history_path):
        hist = pd.read_csv(history_path)
        dup = (hist["run_label"] == run_label) & (hist["user"] == user)
        if dup.any():
            print(f"  replacing existing history rows for ({run_label}, {user})")
            hist = hist[~dup]
        hist = pd.concat([hist, rec], ignore_index=True)
    else:
        os.makedirs(os.path.dirname(history_path), exist_ok=True)
        hist = rec
    hist.to_csv(history_path, index=False)
    return history_path


# --------------------------------------------------------------------------
# Meta-analysis report
# --------------------------------------------------------------------------

def _run_order(hist):
    """Run labels in chronological order of first recording."""
    return list(hist.groupby("run_label")["run_ts"].min().sort_values().index)


def _format_cell(value, sd):
    if pd.isna(value):
        return ""
    cell = f"{value:.4g}"
    if pd.notna(sd):
        cell += f" ±{sd:.2g}"
    return cell


def report(history_path=DEFAULT_HISTORY, out_dir=DEFAULT_META_DIR):
    """Per-user metric x iteration tables + the meta figure."""
    if not os.path.exists(history_path):
        raise FileNotFoundError(
            f"{history_path} not found -- record a run first via "
            "build_tick_frame.py --label <name>")
    hist = pd.read_csv(history_path)
    order = _run_order(hist)
    metric_order = list(dict.fromkeys(hist["metric"]))
    os.makedirs(out_dir, exist_ok=True)

    print(f"runs (chronological): {order}")
    for label in order:
        h = hist[hist["run_label"] == label]
        split = json.loads(h["config"].iloc[0]).get("split", {})
        note = h["note"].iloc[0] if "note" in h.columns else ""
        print(f"  {label}: split={split.get('type')} "
              f"train_frac={split.get('train_frac')} "
              f"commit={h['git_commit'].iloc[0]}"
              + (f" — {note}" if isinstance(note, str) and note else ""))

    for user in sorted(hist["user"].unique()):
        h = hist[hist["user"] == user].copy()
        h["cell"] = [_format_cell(v, s) for v, s in zip(h["value"], h["sd"])]
        table = (h.pivot_table(index="metric", columns="run_label",
                               values="cell", aggfunc="first")
                 .reindex(index=metric_order,
                          columns=[l for l in order if l in set(h["run_label"])]))
        print(f"\n=== {user} ===")
        print(table.to_string())
        path = os.path.join(out_dir, f"meta_table_{user}.csv")
        table.to_csv(path)
        print(f"  -> {path}")

    _plot_meta(hist, order, out_dir)

    # lazy: stage_a_dashboard imports this module's constants at load time
    from stage_a_dashboard import build_dashboard
    build_dashboard(history_path,
                    os.path.join(out_dir, "dashboard.html"))


def _plot_meta(hist, order, out_dir):
    # imported here: plot_traces -> build_tick_frame -> stage_a_metrics would
    # otherwise be a circular import at module load
    from plot_traces import (BASELINE, BLUE, GRID, INK, MUT, ORANGE, SEC,
                             SURFACE, _save, _style)
    import matplotlib.pyplot as plt

    AQUA = "#1baf7a"  # categorical slot 3 (validated with slots 1-2)
    user_colors = [BLUE, ORANGE, AQUA]
    users = sorted(hist["user"].unique())
    if len(users) > len(user_colors):
        raise ValueError(f"meta plot supports {len(user_colors)} users; "
                         f"history has {len(users)} -- extend the palette "
                         "(validate slots together) or facet by user")

    def _series(user, metric, column):
        h = hist[(hist["user"] == user) & (hist["metric"] == metric)]
        by_label = h.set_index("run_label")[column]
        return np.array([by_label.get(label, np.nan) for label in order])

    n_cols = 3
    n_rows = int(np.ceil(len(KEY_METRICS) / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(3.9 * n_cols, 2.6 * n_rows),
                             facecolor=SURFACE,
                             gridspec_kw={"hspace": 0.52, "wspace": 0.3})
    x = np.arange(len(order))

    for ax, (metric, title) in zip(axes.flat, KEY_METRICS):
        _style(ax)
        ref = REFERENCE.get(metric, {})
        if "band" in ref:
            ax.axhspan(*ref["band"], color="#f0efec", zorder=0)
        if "line" in ref:
            ax.axhline(ref["line"], color=BASELINE, linewidth=1.0,
                       linestyle="--", zorder=1)
        for u_idx, user in enumerate(users):
            color = user_colors[u_idx]
            offset = (u_idx - (len(users) - 1) / 2) * 0.06
            values = _series(user, metric, "value")
            sds = _series(user, metric, "sd")
            if "real_ref" in ref:
                real = _series(user, ref["real_ref"], "value")
                # open marker so the reference is visible even with one run,
                # where a one-point line draws nothing
                ax.plot(x + offset, real, color=color, linewidth=1.0,
                        linestyle=":", alpha=0.6, zorder=2, marker="o",
                        markersize=3.5, markerfacecolor="none")
            ax.errorbar(x + offset, values, yerr=np.where(np.isfinite(sds), sds, 0.0),
                        color=color, linewidth=1.8, marker="o", markersize=4.5,
                        capsize=2.5, elinewidth=1.0, zorder=3)
        ax.set_title(title, loc="left", color=SEC, fontsize=9.5)
        ax.set_xticks(x)
        ax.set_xticklabels(order, rotation=30, ha="right", fontsize=8)
        ax.margins(x=0.08)
    for ax in axes.flat[len(KEY_METRICS):]:
        ax.set_visible(False)

    handles = [plt.Line2D([], [], color=user_colors[i], marker="o",
                          markersize=4.5, linewidth=1.8, label=user)
               for i, user in enumerate(users)]
    fig.legend(handles=handles, loc="upper right", frameon=False,
               fontsize=9, labelcolor=SEC, bbox_to_anchor=(0.99, 1.0))
    fig.suptitle("Stage A metrics across iterations "
                 "(bars: ±sd over simulation replicates; "
                 "dotted: real holdout reference)",
                 x=0.01, ha="left", color=INK, fontsize=12)
    _save(fig, out_dir, "meta_metrics.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", action="store_true",
                        help="render the meta-analysis from the history file")
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--out-dir", default=DEFAULT_META_DIR)
    args = parser.parse_args()
    if not args.report:
        parser.error("metrics are computed via build_tick_frame.py --label "
                     "<name>; this entry point only renders --report")
    report(args.history, args.out_dir)
