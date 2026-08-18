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
from concurrent.futures import ProcessPoolExecutor, as_completed
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
SURROGATE_KINDS = ("const", "diurnal")  # constant-rate and hour-of-day-rate
                                        # Bernoulli reference generators
SURROGATE_METRIC_STEMS = [
    "corr_rate_ratio", "carb_rate_ratio",
    "corr_gap_p10_min", "corr_gap_median_min", "carb_gap_p10_min",
    "diurnal_tv_corrections", "diurnal_tv_carb_entries",
    "overnight_corr_share",
]
SIM_METRICS = [
    "corr_per_day_sim", "carb_per_day_sim",
    "corr_rate_ratio", "carb_rate_ratio",
    "corr_gap_median_sim_min", "corr_gap_p10_sim_min",
    "carb_gap_median_sim_min", "carb_gap_p10_sim_min",
    "ablation_gap_p10_delta_min", "ablation_carb_gap_p10_delta_min",
    "diurnal_tv_corrections", "diurnal_tv_carb_entries",
    "overnight_corr_share_sim", "overnight_carb_share_sim",
    "ks_carb_grams", "ks_corr_units", "nan_corr_mark_frac",
] + [f"surr_{kind}_{stem}"
     for kind in SURROGATE_KINDS for stem in SURROGATE_METRIC_STEMS]
FIT_METRIC_SUFFIXES = ("_holdout_nll", "_nll_skill", "_nll_skill_diurnal",
                       "_auc", "_cal_slope", "_obs_pred_ratio")

# meta-view layout: one metric family per row, correction hazard in the LEFT
# column, carb-entry hazard in the RIGHT (every family exists for both).
# Reference geometry per metric lives in REFERENCE below: line = target/naive
# level, band = acceptance region, real_ref = real-holdout dotted reference,
# surr_refs = surrogate floors.
KEY_METRIC_PAIRS = [
    [["corr_rate_ratio", "corrections/day, sim ÷ real"],
     ["carb_rate_ratio", "carb entries/day, sim ÷ real"]],
    [["corr_nll_skill", "NLL skill vs constant baseline (holdout)"],
     ["carb_nll_skill", "NLL skill vs constant baseline (holdout)"]],
    [["corr_nll_skill_diurnal", "NLL skill vs diurnal baseline (holdout)"],
     ["carb_nll_skill_diurnal", "NLL skill vs diurnal baseline (holdout)"]],
    [["corr_auc", "AUC (holdout)"],
     ["carb_auc", "AUC (holdout)"]],
    [["corr_cal_slope", "calibration slope (holdout)"],
     ["carb_cal_slope", "calibration slope (holdout)"]],
    [["diurnal_tv_corrections", "diurnal TV distance, real vs sim"],
     ["diurnal_tv_carb_entries", "diurnal TV distance, real vs sim"]],
    [["overnight_corr_share_sim", "overnight share, sim"],
     ["overnight_carb_share_sim", "overnight share, sim"]],
    [["corr_gap_p10_sim_min", "gap p10, sim (min)"],
     ["carb_gap_p10_sim_min", "gap p10, sim (min)"]],
    [["ablation_gap_p10_delta_min", "ablation Δ gap p10 (min)"],
     ["ablation_carb_gap_p10_delta_min", "ablation Δ gap p10 (min)"]],
]
PAIR_COLUMN_HEADS = ("correction hazard", "carb-entry hazard")
# --------------------------------------------------------------------------
# Metric descriptions (shown as hover explanations in the dashboard; the
# evaluate smoke test asserts every emitted metric resolves to one)
# --------------------------------------------------------------------------

METRIC_INFO = {
    "train_days": "Days of record in the training split.",
    "holdout_days": "Days of record in the holdout split (interleaved weeks "
                    "by default; simulation and all holdout metrics live here).",
    "n_train_corrections": "Correction events in the training split — the "
                           "events-per-parameter budget for the hazard fit.",
    "n_holdout_corrections": "Correction events in the real holdout — sets "
                             "the stability of the holdout metrics.",
    "n_train_carb_entries": "Carb entries in the training split.",
    "n_holdout_carb_entries": "Carb entries in the real holdout.",
    "meal_bolus_p": "P(bolus within the ±15-min association window | carb "
                    "entry), fit on train; the meal-bolus coin flip used in "
                    "simulation.",
    "corr_per_day_real": "Observed corrections per day in the real holdout — "
                         "denominator of the correction rate ratio.",
    "carb_per_day_real": "Observed carb entries per day in the real holdout.",
    "corr_gap_median_real_min": "Median gap (min) between consecutive real "
                                "holdout corrections, pooled within holdout "
                                "blocks (cross-block gaps are split artifacts).",
    "corr_gap_p10_real_min": "10th-percentile real correction gap (min), "
                             "within blocks — short gaps are cascade "
                             "behavior (rapid repeat corrections).",
    "overnight_corr_share_real": "Fraction of real holdout corrections "
                                 "between 00:00 and 06:00.",
    "carb_gap_median_real_min": "Median gap (min) between consecutive real "
                                "holdout carb entries, within blocks — meal "
                                "spacing.",
    "carb_gap_p10_real_min": "10th-percentile real carb-entry gap (min) — "
                             "short gaps are split/staggered meals and "
                             "rescue-carb clusters.",
    "overnight_carb_share_real": "Fraction of real holdout carb entries "
                                 "between 00:00 and 06:00 (night eating).",
    "corr_per_day_sim": "Simulated corrections per day (free-running rollout "
                        "on real CGM; self-excitation from simulated history).",
    "carb_per_day_sim": "Simulated carb entries per day (free-running).",
    "corr_rate_ratio": "Simulated ÷ real corrections per day. Shaded band = "
                       "±20% go/no-go target; 'binomial' floor = what a "
                       "rate-matched memoryless null achieves.",
    "carb_rate_ratio": "Simulated ÷ real carb entries per day (±20% band; "
                       "'binomial' floor as reference).",
    "corr_gap_median_sim_min": "Median gap (min) between simulated "
                               "corrections, within blocks.",
    "corr_gap_p10_sim_min": "10th-percentile simulated correction gap (min). "
                            "Compare to the real dotted reference and the "
                            "memoryless 'binomial' floor — beating the floor "
                            "means the model produces genuine cascade "
                            "structure.",
    "ablation_gap_p10_delta_min": "Correction gap p10 with the "
                                  "correction-history features removed minus "
                                  "with them (paired replicates). >0 = "
                                  "excitation terms shorten short gaps, i.e. "
                                  "they do real work; ~0 = decorative.",
    "carb_gap_median_sim_min": "Median gap (min) between simulated carb "
                               "entries, within blocks.",
    "carb_gap_p10_sim_min": "10th-percentile simulated carb-entry gap (min) "
                            "— meal-spacing / rescue-cluster structure; "
                            "compare to the real reference and the "
                            "memoryless 'binomial' floor.",
    "ablation_carb_gap_p10_delta_min": "Carb-entry gap p10 without the "
                                       "correction-history features minus "
                                       "with them. The features feed BOTH "
                                       "hazards, so this tests "
                                       "correction→carb coupling in "
                                       "simulated timing (e.g. rescue carbs "
                                       "after corrections).",
    "overnight_carb_share_sim": "Fraction of simulated carb entries between "
                                "00:00–06:00 vs the real dotted reference.",
    "diurnal_tv_corrections": "Total-variation distance between real and "
                              "simulated hour-of-day correction histograms "
                              "(0 = same shape, 1 = disjoint; rate-free). "
                              "'clock' floor = habit-clock surrogate, "
                              "'binomial' = flat.",
    "diurnal_tv_carb_entries": "Diurnal-shape TV distance for carb entries "
                               "(see corrections variant).",
    "overnight_corr_share_sim": "Fraction of simulated corrections between "
                                "00:00–06:00 vs the real dotted reference — "
                                "tracks the overnight over-production "
                                "failure mode.",
    "ks_carb_grams": "KS distance between simulated and real holdout "
                     "carb-gram distributions — mark fidelity of the "
                     "empirical resampler.",
    "ks_corr_units": "KS distance between simulated and real correction "
                     "units. NaN when marks are uncomputable (sparse "
                     "recommended_bolus).",
    "nan_corr_mark_frac": "Fraction of simulated correction marks that are "
                          "NaN — the recommended-bolus coverage gap (≈1 for "
                          "HK-path users).",
}
FIT_SUFFIX_INFO = {
    "_holdout_nll": "Mean per-tick log loss of the {event} hazard on the "
                    "holdout, teacher-forced (features from the real "
                    "record). Scale depends on the event rate — compare via "
                    "the skill scores.",
    "_nll_skill_diurnal": "Holdout NLL skill of the {event} hazard vs the "
                          "train hour-of-day-rate baseline — what "
                          "glucose/IOB/excitation add beyond the habit "
                          "clock. Can be negative (a 24-bin lookup can beat "
                          "the model's clock terms).",
    "_nll_skill": "1 − NLL/NLL(train-rate constant baseline) for the {event} "
                  "hazard — out-of-sample McFadden R² vs a fitted binomial. "
                  ">0 = the features add predictive value.",
    "_auc": "Rank AUC of the {event} hazard on holdout ticks: P(random "
            "event tick scores above random non-event tick). Discrimination "
            "only — says nothing about calibration.",
    "_cal_slope": "Slope of a logistic recalibration of the {event} hazard "
                  "on the holdout. 1 = calibrated; <1 = predictions too "
                  "extreme; >1 = too timid.",
    "_obs_pred_ratio": "Observed ÷ predicted {event} count on the holdout "
                       "(calibration-in-the-large). <1 = the model "
                       "over-predicts in the holdout era.",
}
SURROGATE_KIND_INFO = {
    "const": "Constant-rate binomial surrogate (iid per-tick draws at the "
             "train event rate — the structure-free floor):",
    "diurnal": "Hour-of-day-rate surrogate (iid draws at the train hourly "
               "rate — the habit-clock null):",
}
SURROGATE_STEM_INFO = {
    "corr_rate_ratio": "its simulated ÷ real corrections per day (≈1 by "
                       "construction, up to residual drift).",
    "carb_rate_ratio": "its simulated ÷ real carb entries per day.",
    "corr_gap_p10_min": "its correction gap p10 (min) — the memoryless "
                        "reference for cascade structure.",
    "corr_gap_median_min": "its median correction gap (min).",
    "carb_gap_p10_min": "its carb-entry gap p10 (min) — the memoryless "
                        "reference for meal spacing.",
    "diurnal_tv_corrections": "its diurnal TV distance vs the real "
                              "correction profile.",
    "diurnal_tv_carb_entries": "its diurnal TV distance vs the real "
                               "carb-entry profile.",
    "overnight_corr_share": "its share of corrections between 00:00–06:00.",
}


def metric_description(name):
    """Human explanation for a metric name; '' if unknown."""
    if name in METRIC_INFO:
        return METRIC_INFO[name]
    for kind, kind_text in SURROGATE_KIND_INFO.items():
        prefix = f"surr_{kind}_"
        if name.startswith(prefix):
            stem_text = SURROGATE_STEM_INFO.get(name[len(prefix):], "")
            return f"{kind_text} {stem_text}" if stem_text else ""
    for suffix, text in FIT_SUFFIX_INFO.items():
        if name.endswith(suffix):
            event = "correction" if name.startswith("corr") else "carb-entry"
            return text.format(event=event)
    return ""


# surr_refs: surrogate floors drawn as labeled dash-dot references (cross-user
# median) in the panel -- "binomial" = surr_const, "clock" = surr_diurnal
REFERENCE = {
    "corr_rate_ratio": {"line": 1.0, "band": (0.8, 1.2),
                        "surr_refs": [["surr_const_corr_rate_ratio", "binomial"]]},
    "carb_rate_ratio": {"line": 1.0, "band": (0.8, 1.2),
                        "surr_refs": [["surr_const_carb_rate_ratio", "binomial"]]},
    "corr_nll_skill": {"line": 0.0},
    "carb_nll_skill": {"line": 0.0},
    "corr_auc": {"line": 0.5},
    "carb_auc": {"line": 0.5},
    "corr_cal_slope": {"line": 1.0},
    "carb_cal_slope": {"line": 1.0},
    "diurnal_tv_corrections": {
        "line": 0.0,
        "surr_refs": [["surr_const_diurnal_tv_corrections", "binomial"],
                      ["surr_diurnal_diurnal_tv_corrections", "clock"]]},
    "diurnal_tv_carb_entries": {
        "line": 0.0,
        "surr_refs": [["surr_const_diurnal_tv_carb_entries", "binomial"],
                      ["surr_diurnal_diurnal_tv_carb_entries", "clock"]]},
    "overnight_corr_share_sim": {"real_ref": "overnight_corr_share_real"},
    "overnight_carb_share_sim": {"real_ref": "overnight_carb_share_real"},
    "corr_gap_p10_sim_min": {
        "real_ref": "corr_gap_p10_real_min",
        "surr_refs": [["surr_const_corr_gap_p10_min", "binomial"]]},
    "carb_gap_p10_sim_min": {
        "real_ref": "carb_gap_p10_real_min",
        "surr_refs": [["surr_const_carb_gap_p10_min", "binomial"]]},
    "ablation_gap_p10_delta_min": {"line": 0.0},
    "ablation_carb_gap_p10_delta_min": {"line": 0.0},
    "corr_nll_skill_diurnal": {"line": 0.0},
    "carb_nll_skill_diurnal": {"line": 0.0},
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


def _hourly_rates(train, event):
    """Empirical per-tick event rate by hour of day, clipped away from 0/1."""
    hourly = train.groupby(train["timestamp"].dt.hour)[event].mean()
    hourly = hourly.reindex(range(24)).fillna(train[event].mean())
    return np.clip(hourly.to_numpy(dtype=float), EPS_P, 1.0 - EPS_P)


def fit_metrics(train, holdout, hazards):
    """Per-hazard one-step-ahead metrics on the holdout. Teacher-forced:
    features (incl. self-excitation) come from the real record, so this
    isolates fit quality from rollout dynamics. Two skill baselines:
    the train-rate constant (a fitted binomial) and the train hour-of-day
    rate -- skill vs the latter is what glucose/IOB/excitation add beyond
    the user's habit clock."""
    features = hazards["features"]
    hour = holdout["timestamp"].dt.hour.to_numpy()
    out = {}
    for event, prefix in EVENT_PREFIXES:
        y = holdout[event].to_numpy(dtype=float)
        p = _predicted_hazard(hazards["models"][event], holdout, features)
        p0 = float(np.clip(train[event].mean(), EPS_P, 1.0 - EPS_P))
        nll = _nll(y, p)
        nll0 = _nll(y, np.full_like(y, p0))
        nll0_diurnal = _nll(y, _hourly_rates(train, event)[hour])
        out[f"{prefix}_holdout_nll"] = nll
        out[f"{prefix}_nll_skill"] = 1.0 - nll / nll0 if nll0 > 0 else np.nan
        out[f"{prefix}_nll_skill_diurnal"] = (
            1.0 - nll / nll0_diurnal if nll0_diurnal > 0 else np.nan)
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
    spans = real_ref["spans"]
    gaps = block_gap_minutes(sim_corr["timestamp"], spans)
    gaps_abl = block_gap_minutes(
        sim_ablated.loc[sim_ablated["event"] == "correction", "timestamp"], spans)
    carb_gaps = block_gap_minutes(sim_carb["timestamp"], spans)
    carb_gaps_abl = block_gap_minutes(
        sim_ablated.loc[sim_ablated["event"] == "carb_entry", "timestamp"], spans)

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
        "carb_gap_median_sim_min": _percentile(carb_gaps, 50),
        "carb_gap_p10_sim_min": _percentile(carb_gaps, 10),
        "ablation_gap_p10_delta_min": _percentile(gaps_abl, 10) - _percentile(gaps, 10),
        "ablation_carb_gap_p10_delta_min": (
            _percentile(carb_gaps_abl, 10) - _percentile(carb_gaps, 10)),
        "diurnal_tv_corrections": _diurnal_tv(
            real_ref["corr_times"], sim_corr["timestamp"]),
        "diurnal_tv_carb_entries": _diurnal_tv(
            real_ref["carb_times"], sim_carb["timestamp"]),
        "overnight_corr_share_sim": _overnight_share(sim_corr["timestamp"]),
        "overnight_carb_share_sim": _overnight_share(sim_carb["timestamp"]),
        "ks_carb_grams": _ks(real_ref["carb_grams"], sim_carb["mark"]),
        "ks_corr_units": _ks(real_ref["corr_units"], corr_marks),
        "nan_corr_mark_frac": (float(np.isnan(corr_marks).mean())
                               if len(corr_marks) else np.nan),
    }


# --------------------------------------------------------------------------
# The suite
# --------------------------------------------------------------------------

def _replicate_payload(result, n_days, real_ref):
    """The picklable subset of a run_mvp result that replicate workers need."""
    holdout = result["holdout"]
    surr_rates = {}
    for event, prefix in EVENT_PREFIXES:
        surr_rates[f"{prefix}_const"] = float(
            np.clip(result["train"][event].mean(), EPS_P, 1.0 - EPS_P))
        surr_rates[f"{prefix}_hourly"] = _hourly_rates(result["train"], event)
    return {
        "frame": result["frame"],
        "blocks": result["holdout_blocks"],
        "hazards": result["hazards"],
        "hazards_ablated": result["hazards_ablated"],
        "marks": result["marks"],
        "meal_bolus_p": result["meal_bolus_p"],
        "n_days": n_days,
        "real_ref": real_ref,
        "holdout_ts": holdout["timestamp"].to_numpy(),
        "holdout_hour": holdout["timestamp"].dt.hour.to_numpy(),
        "surr_rates": surr_rates,
    }


def _surrogate_replicate(payload, k, base_seed):
    """Reference generators on the same holdout ticks: iid Bernoulli draws at
    the train rate ('const') and at the train hour-of-day rate ('diurnal').
    Rate-matched floors for the simulation metrics -- whatever the model
    beats here is structure beyond base rate / the habit clock. Own seed
    streams ([base_seed, k, 2+]), so adding them never perturbs the model's
    replicate draws."""
    ts, hour = payload["holdout_ts"], payload["holdout_hour"]
    rates, real_ref = payload["surr_rates"], payload["real_ref"]
    n_days = payload["n_days"]
    out = {}
    for s_idx, kind in enumerate(SURROGATE_KINDS):
        rng = np.random.default_rng([base_seed, k, 2 + s_idx])
        times = {}
        for prefix in ("corr", "carb"):
            p = (rates[f"{prefix}_hourly"][hour] if kind == "diurnal"
                 else rates[f"{prefix}_const"])
            times[prefix] = pd.Series(ts[rng.random(len(hour)) < p])
        gaps = block_gap_minutes(times["corr"], real_ref["spans"])
        carb_gaps = block_gap_minutes(times["carb"], real_ref["spans"])
        for prefix in ("corr", "carb"):
            real_rate = real_ref[f"{prefix}_per_day_real"]
            out[f"surr_{kind}_{prefix}_rate_ratio"] = (
                len(times[prefix]) / n_days / real_rate
                if real_rate > 0 else np.nan)
        out[f"surr_{kind}_corr_gap_p10_min"] = _percentile(gaps, 10)
        out[f"surr_{kind}_corr_gap_median_min"] = _percentile(gaps, 50)
        out[f"surr_{kind}_carb_gap_p10_min"] = _percentile(carb_gaps, 10)
        out[f"surr_{kind}_diurnal_tv_corrections"] = _diurnal_tv(
            real_ref["corr_times"], times["corr"])
        out[f"surr_{kind}_diurnal_tv_carb_entries"] = _diurnal_tv(
            real_ref["carb_times"], times["carb"])
        out[f"surr_{kind}_overnight_corr_share"] = _overnight_share(times["corr"])
    return out


def _run_replicate(payload, k, base_seed):
    """One replicate: paired full + ablated rollouts, seeded by k alone --
    results are identical however replicates are distributed over
    processes."""
    with warnings.catch_warnings():
        # sparse-recommendation NaN marks surface as nan_corr_mark_frac,
        # not as n_sims repeated warnings
        warnings.simplefilter("ignore")
        sim = simulate_blocks(
            payload["frame"], payload["blocks"], payload["hazards"],
            payload["marks"], payload["meal_bolus_p"],
            np.random.default_rng([base_seed, k, 0]))
        sim_abl = simulate_blocks(
            payload["frame"], payload["blocks"], payload["hazards_ablated"],
            payload["marks"], payload["meal_bolus_p"],
            np.random.default_rng([base_seed, k, 1]))
    row = _replicate_metrics(sim, sim_abl, payload["n_days"],
                             payload["real_ref"])
    row.update(_surrogate_replicate(payload, k, base_seed))
    return row


def _replicate_chunk(payload, ks, base_seed):
    return [(k, _run_replicate(payload, k, base_seed)) for k in ks]


def _progress_line(done_rows, n_done, n_sims):
    so_far = pd.DataFrame(done_rows)
    parts = []
    for name in PROGRESS_METRICS:
        mean, sd = so_far[name].mean(), so_far[name].std(ddof=1)
        cell = f"{name} {mean:.3g}"
        if np.isfinite(sd):
            cell += f" ±{sd:.2g}"
        parts.append(cell)
    return f"    replicate {n_done:>2}/{n_sims}: " + "  ".join(parts)


def evaluate(result, n_sims=N_SIMS_DEFAULT, base_seed=BASE_SEED_DEFAULT,
             verbose=False, replicates_path=None, n_jobs=1):
    """Score one run_mvp result. Returns a tidy frame [metric, value, sd]:
    sd is the across-replicate spread for simulation metrics and NaN for
    deterministic ones. Same result + same base_seed => identical output
    (verbose logging, the replicate dump, and `n_jobs` don't touch the
    per-replicate RNG streams).

    `verbose` prints the running mean ± sd of PROGRESS_METRICS as
    replicates complete; `replicates_path` saves every replicate's raw
    metric values (one row per replicate, in replicate order) so any
    metric's convergence can be checked. `n_jobs` > 1 distributes
    replicates over processes -- used when the machine has more cores than
    the cohort has users."""
    train, holdout = result["train"], result["holdout"]
    frame, blocks = result["frame"], result["holdout_blocks"]
    spans = block_spans(frame, blocks)
    n_days = len(holdout) / TICKS_PER_DAY

    corr_times = holdout.loc[holdout["is_correction"], "timestamp"]
    carb_times = holdout.loc[holdout["is_carb_entry"], "timestamp"]
    real_gaps = block_gap_minutes(corr_times, spans)
    real_carb_gaps = block_gap_minutes(carb_times, spans)
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
        "carb_gap_median_real_min": _percentile(real_carb_gaps, 50),
        "carb_gap_p10_real_min": _percentile(real_carb_gaps, 10),
        "overnight_corr_share_real": _overnight_share(corr_times),
        "overnight_carb_share_real": _overnight_share(carb_times),
        **fit_metrics(train, holdout, result["hazards"]),
    }

    payload = _replicate_payload(result, n_days, real_ref)
    if n_jobs <= 1 or n_sims <= 1:
        ordered = []
        for k in range(n_sims):
            ordered.append(_run_replicate(payload, k, base_seed))
            if verbose:
                print(_progress_line(ordered, k + 1, n_sims))
    else:
        chunks = [c.tolist() for c in np.array_split(np.arange(n_sims), n_jobs)
                  if len(c)]
        by_k = []
        with ProcessPoolExecutor(max_workers=len(chunks)) as pool:
            futures = [pool.submit(_replicate_chunk, payload, ks, base_seed)
                       for ks in chunks]
            for fut in as_completed(futures):
                by_k.extend(fut.result())
                if verbose:
                    print(_progress_line([r for _, r in by_k], len(by_k),
                                         n_sims))
        ordered = [r for _, r in sorted(by_k, key=lambda t: t[0])]
    rep = pd.DataFrame(ordered)
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
    from matplotlib.lines import Line2D

    AQUA = "#1baf7a"  # categorical slot 3 (validated with slots 1-2)
    user_colors = [BLUE, ORANGE, AQUA]
    users = sorted(hist["user"].unique())
    # per-user hue identity caps at the validated palette; beyond it, users
    # become thin muted lines and the cross-user median carries the story
    # (identity lives in the tables/dashboard hover, hues are never cycled)
    spaghetti = len(users) > len(user_colors)

    def _series(user, metric, column):
        h = hist[(hist["user"] == user) & (hist["metric"] == metric)]
        by_label = h.set_index("run_label")[column]
        return np.array([by_label.get(label, np.nan) for label in order])

    def _median_series(metric, column="value"):
        stacked = np.array([_series(u, metric, column) for u in users])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # all-NaN slices -> NaN
            return np.nanmedian(stacked, axis=0)

    panels = [p for pair in KEY_METRIC_PAIRS for p in pair]
    n_rows = len(KEY_METRIC_PAIRS)
    fig, axes = plt.subplots(n_rows, 2,
                             figsize=(5.9 * 2, 3.0 * n_rows),
                             facecolor=SURFACE,
                             gridspec_kw={"hspace": 0.55, "wspace": 0.18})
    for col, head in enumerate(PAIR_COLUMN_HEADS):
        axes[0, col].annotate(head.upper(), xy=(0.5, 1.28),
                              xycoords="axes fraction", ha="center",
                              color=SEC, fontsize=12, fontweight="bold")
    x = np.arange(len(order))

    for ax, (metric, title) in zip(axes.flat, panels):
        _style(ax)
        ref = REFERENCE.get(metric, {})
        if "band" in ref:
            ax.axhspan(*ref["band"], color="#f0efec", zorder=0)
        if "line" in ref:
            ax.axhline(ref["line"], color=BASELINE, linewidth=1.0,
                       linestyle="--", zorder=1)
        for surr_metric, surr_label in ref.get("surr_refs", []):
            floor = _median_series(surr_metric)
            finite = np.flatnonzero(np.isfinite(floor))
            if not len(finite):
                continue
            ax.plot(x, floor, color=MUT, linewidth=1.1,
                    linestyle=(0, (4, 2, 1, 2)), marker="s", markersize=3,
                    markerfacecolor="none", zorder=2)
            ax.annotate(surr_label, (x[finite[-1]], floor[finite[-1]]),
                        textcoords="offset points", xytext=(4, 3),
                        fontsize=7.5, color=MUT)
        if spaghetti:
            for user in users:
                ax.plot(x, _series(user, metric, "value"), color=MUT,
                        linewidth=0.9, alpha=0.4, zorder=2)
            if "real_ref" in ref:
                ax.plot(x, _median_series(ref["real_ref"]), color=BLUE,
                        linewidth=1.2, linestyle=":", alpha=0.7, zorder=3,
                        marker="o", markersize=3.5, markerfacecolor="none")
            ax.plot(x, _median_series(metric), color=BLUE, linewidth=2.2,
                    marker="o", markersize=4.5, zorder=4)
        else:
            for u_idx, user in enumerate(users):
                color = user_colors[u_idx]
                offset = (u_idx - (len(users) - 1) / 2) * 0.06
                values = _series(user, metric, "value")
                sds = _series(user, metric, "sd")
                if "real_ref" in ref:
                    real = _series(user, ref["real_ref"], "value")
                    # open marker so the reference is visible even with one
                    # run, where a one-point line draws nothing
                    ax.plot(x + offset, real, color=color, linewidth=1.0,
                            linestyle=":", alpha=0.6, zorder=2, marker="o",
                            markersize=3.5, markerfacecolor="none")
                ax.errorbar(x + offset, values,
                            yerr=np.where(np.isfinite(sds), sds, 0.0),
                            color=color, linewidth=1.8, marker="o",
                            markersize=4.5, capsize=2.5, elinewidth=1.0,
                            zorder=3)
        ax.set_title(title, loc="left", color=SEC, fontsize=9.5)
        ax.set_xticks(x)
        ax.set_xticklabels(order, rotation=30, ha="right", fontsize=8)
        # pin the run axis: a metric recorded only in later runs must not
        # collapse the panel onto its own points
        ax.set_xlim(-0.4, len(order) - 0.6)

    if spaghetti:
        handles = [
            Line2D([], [], color=MUT, linewidth=0.9, alpha=0.4,
                       label=f"individual users (n={len(users)})"),
            Line2D([], [], color=BLUE, marker="o", markersize=4.5,
                       linewidth=2.2, label="median across users"),
        ]
        subtitle = ("dotted: median real reference; dash-dot: surrogate floors")
    else:
        handles = [Line2D([], [], color=user_colors[i], marker="o",
                              markersize=4.5, linewidth=1.8, label=user)
                   for i, user in enumerate(users)]
        subtitle = ("bars: ±sd over simulation replicates; dotted: real "
                    "holdout reference; dash-dot: surrogate floors")
    fig.legend(handles=handles, loc="upper right", frameon=False,
               fontsize=9, labelcolor=SEC, bbox_to_anchor=(0.99, 1.0))
    fig.suptitle(f"Stage A metrics across iterations ({subtitle})",
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
