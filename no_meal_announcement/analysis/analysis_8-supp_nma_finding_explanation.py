#!/usr/bin/env python
"""Analysis 8 (supplement): explaining higher TIR on no-meal-announcement (CE=0) days.

§8.1 finds TIR is HIGHER on CE=0 (no-meal-announcement) days than on CE>0 days —
counterintuitive. A structural fact reframes it: in staging CE=0 ⟺ carb_grams_total = 0
exactly, so a CE=0 day is the *zero endpoint of the announced-carb axis*. The leading
explanation is an INTAKE CONFOUND (CE=0 ≈ low/no-intake days → less to chase → less hyper →
higher TIR), not "disengaged dosing is better." This supplement tests that against the
safety-relevant alternative (TIR up because users run LOW) and standard confounders.

Exploratory / explanatory — NOT pre-specified §8.1; it reuses §8.1's contrast + figure
machinery but writes to its own dir so the §8.1 audit trail is untouched.

Sections:
  S1  intake characterization  — CE=0 vs CE>0 on TDD / insulin / boluses / carbs (paired).
  S2  carbohydrate dose-response — TIR & TAR vs announced carb grams across CE>0 days, with
                                   CE=0 as the pinned 0 g anchor + within-user slope LMM.
  S3  glycemic decomposition + safety — split the TIR gain into TAR↓ (benign) vs TBR↑/hypo↑.
  C1–C4 confounder checks — selection bias, CE=0-day clustering, CGM coverage, weighting note.

Outputs (analysis/outputs/analysis_8_supp/<cohort>/):
  table_s1_intake_characterization.csv, figure_s1_intake_violin.png
  table_s2_carb_dose_response.csv,       figure_s2_carb_dose_response.png
  table_s3_decomposition.csv,            figure_s3_stacked_ranges.png, figure_s3_tbr_violin.png
  table_c_confounders.csv

Usage: python analysis_8-supp_nma_finding_explanation.py [--cohort {adult,pediatric,all}]
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Make the sibling `utils` package importable regardless of launch method.
try:
    _ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ANALYSIS_DIR = (
        "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
        "no_meal_announcement/analysis"
    )
if _ANALYSIS_DIR not in sys.path:
    sys.path.insert(0, _ANALYSIS_DIR)

from utils.data_loader import (  # noqa: E402
    CLASSIFICATIONS,
    COMPARATOR_FLAG,
    COMPARATOR_LABEL,
    ENDPOINTS,
    MIN_AGE,
    analysis_dir,
    default_analysis_ready_csv,
    filter_cohort,
    load_day_level,
    load_nma_statistics,
    prepare_day_level,
    restrict_comparator,
)
from utils.plotting import (  # noqa: E402
    SUPTITLE_FS,
    TICK_FS,
    TITLE_FS,
    TIDEPOOL,
    endpoint_color,
    violin_box_panel,
)

# The broadest NMA arm (CE=0, any BE) — the single "no-meal-announcement" arm used where one
# arm is needed (S2 anchor, S3 contrast). S1 loops over all three nested classifications.
NMA_FLAG = "in_ce0_be_inf"
NMA_LABEL = "CE=0 (any BE)"

# Intake proxies for S1 (per-day). All exist in the analysis-ready snapshot; guarded anyway.
INTAKE_PROXIES = [
    ("tdd_units", "Total daily insulin (U/day)"),
    ("bolus_units", "Bolus insulin (U/day)"),
    ("basal_units", "Basal insulin (U/day)"),
    ("carb_grams_total", "Announced carbs (g/day)"),
    ("automatic_bolus_count", "Automatic boluses / day"),
    ("bolus_entry_count", "Manual bolus entries / day"),
]

# S2 carb-load bins for CE>0 days (right-open); CE=0 is added as a pinned 0 g anchor.
CARB_BIN_EDGES = [0, 30, 60, 90, np.inf]
CARB_BIN_LABELS = ["0-30 g", "30-60 g", "60-90 g", "90+ g"]
CE0_ANCHOR = "CE=0 (0 g)"
S2_ORDER = [CE0_ANCHOR] + CARB_BIN_LABELS


def _load_a81():
    """Load §8.1 by file path to reuse its generic per-arm mean + figure helpers (the file
    name has hyphens, so it can't be imported normally — same by-path idiom as statistics)."""
    path = os.path.join(analysis_dir(), "analysis_8-1_glycemic_outcomes_nma_vs_carb_entry.py")
    spec = importlib.util.spec_from_file_location("nma_analysis_8_1", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# S1 — intake characterization: is CE=0 just lighter-intake days?
# ---------------------------------------------------------------------------

def table_s1_intake(pdf, nma_stats, a81):
    """Per classification × intake proxy, within-user paired NMA − CE>0 (per-user arm means).
    If CE=0 days have much lower TDD / bolus / autobolus / carbs, the higher TIR is an intake
    effect, not better disengaged control."""
    rows = []
    for nma_flag, cls_label in CLASSIFICATIONS:
        for col, label in INTAKE_PROXIES:
            if col not in pdf.columns:
                continue
            nma = a81.per_user_arm_mean(pdf, col, nma_flag).dropna()
            cmp = a81.per_user_arm_mean(pdf, col, COMPARATOR_FLAG).dropna()
            res = nma_stats.paired_within_user(nma, cmp)  # diff = NMA − CE>0
            rows.append({
                "classification": cls_label,
                "proxy": label,
                "ce0_mean": nma.mean(), "ce0_sd": nma.std(ddof=1),
                "ce_gt0_mean": cmp.mean(), "ce_gt0_sd": cmp.std(ddof=1),
                "paired_diff": res["mean_diff"],
                "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
                "wilcoxon_p": res["wilcoxon_p"], "n_pairs": res["n_pairs"],
            })
    return pd.DataFrame(rows)


def figure_s1_intake(pdf, a81):
    """Per-user mean TDD and bolus insulin by arm (CE=0 arms vs CE>0) — visual of the intake gap.
    Shared violin convention (utils.plotting); TDD/bolus aren't glycemic ranges, so the NMA arms
    use the Tidepool brand colour (graded by breadth) with the CE>0 comparator grey."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 6.4))
    for ax, (col, lab, unit) in zip(axes, [("tdd_units", "Per-user mean TDD by arm", "TDD (U/day)"),
                                           ("bolus_units", "Per-user mean bolus insulin by arm",
                                            "Bolus (U/day)")]):
        violin_box_panel(ax, a81.arm_violin_groups(pdf, col, base=TIDEPOOL),
                         title=lab, title_color=TIDEPOOL, separators=(3.5,))
        ax.set_ylabel(unit)
    fig.suptitle("S1: intake proxies by arm — CE=0 arms vs CE>0 comparator", fontsize=SUPTITLE_FS)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    return fig


def figure_s3_tbr(pdf, a81):
    """Per-user time below range by arm (<70 and <54), re-presented under the safety question
    (replaces the retired §8.1c single figure; same shared violin convention + range colours)."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 6.4))
    for ax, (col, lab) in zip(axes, [("tbr", "Time <70 (%)"), ("tbr_very_low", "Time <54 (%)")]):
        base = endpoint_color(col)
        violin_box_panel(ax, a81.arm_violin_groups(pdf, col, base=base),
                         title=lab, title_color=base, separators=(3.5,))
        ax.set_ylabel(f"Per-user mean {lab}")
    fig.suptitle("S3: per-user time below range by arm (safety check)", fontsize=SUPTITLE_FS)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    return fig


# ---------------------------------------------------------------------------
# S2 — carbohydrate dose-response: is the benefit the zero endpoint of an intake gradient?
# ---------------------------------------------------------------------------

def table_s2_dose_response(pdf, a81):
    """Across CE>0 days (CE=0 is identically 0 g), per-user-mean TIR/TAR by announced-carb bin,
    summarized across users; CE=0 added as the pinned 0 g anchor. If TIR declines monotonically
    with carbs and CE=0 sits at the top, the 'NMA benefit' is the left end of an intake gradient."""
    ce = pdf[pdf[COMPARATOR_FLAG] == True].copy()  # noqa: E712
    ce["carb_bin"] = pd.cut(ce["carb_grams_total"], bins=CARB_BIN_EDGES,
                            right=False, labels=CARB_BIN_LABELS)
    rows = []
    for outcome in ("tir", "tar"):
        # CE=0 anchor (per-user mean over the NMA arm)
        anchor = a81.per_user_arm_mean(pdf, outcome, NMA_FLAG).dropna()
        rows.append({"outcome": outcome, "carb_bin": CE0_ANCHOR,
                     "mean": anchor.mean(), "sd": anchor.std(ddof=1),
                     "n_users": int(len(anchor)),
                     "n_days": int((pdf[NMA_FLAG] == True).sum())})  # noqa: E712
        for lab in CARB_BIN_LABELS:
            sub = ce[ce["carb_bin"] == lab]
            per_user = sub.groupby("_userId")[outcome].mean().dropna()
            rows.append({"outcome": outcome, "carb_bin": lab,
                         "mean": per_user.mean() if len(per_user) else np.nan,
                         "sd": per_user.std(ddof=1) if len(per_user) > 1 else np.nan,
                         "n_users": int(len(per_user)), "n_days": int(len(sub))})
    return pd.DataFrame(rows)


def carb_slope_lmm(pdf, outcome="tir"):
    """Within-user slope of `outcome` per 10 g announced carbs across CE>0 days:
    outcome ~ carb10 + (1|user). Negative slope = more announced carbs → lower TIR."""
    try:
        from statsmodels.regression.mixed_linear_model import MixedLM
    except ImportError:
        return None
    ce = pdf[pdf[COMPARATOR_FLAG] == True].dropna(subset=[outcome, "carb_grams_total"]).copy()  # noqa: E712
    if ce["_userId"].nunique() < 2 or ce[outcome].nunique() < 2:
        return None
    ce["carb10"] = ce["carb_grams_total"] / 10.0
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = MixedLM.from_formula(f"{outcome} ~ carb10", groups=ce["_userId"], data=ce).fit(
            reml=True, method="lbfgs")
    ci = res.conf_int()
    return {
        "outcome": outcome,
        "slope_per_10g": float(res.fe_params["carb10"]),
        "ci_lo": float(ci.loc["carb10", 0]), "ci_hi": float(ci.loc["carb10", 1]),
        "pvalue": float(res.pvalues["carb10"]),
        "n_users": int(ce["_userId"].nunique()), "n_days": int(len(ce)),
    }


def figure_s2_dose_response(table_s2):
    """TIR & TAR per-user-mean vs carb bin (CE=0 anchor leftmost), ±SE error bars."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, outcome, ylab, color in [
        (axes[0], "tir", "Per-user mean TIR (%)", "#5AC692"),
        (axes[1], "tar", "Per-user mean TAR (%)", "#7046CC"),
    ]:
        d = table_s2[table_s2.outcome == outcome].set_index("carb_bin").reindex(S2_ORDER)
        se = d["sd"] / np.sqrt(d["n_users"].clip(lower=1))
        x = np.arange(len(S2_ORDER))
        ax.errorbar(x, d["mean"], yerr=se, marker="o", color=color, capsize=3, lw=1.8)
        ax.axvline(0.5, color="gray", ls=":", lw=1)  # separate CE=0 anchor from CE>0 bins
        ax.set_xticks(x)
        ax.set_xticklabels(S2_ORDER, rotation=20, fontsize=TICK_FS)
        ax.set_ylabel(ylab)
        ax.set_title(f"{outcome.upper()} vs announced carbohydrate load", fontsize=TITLE_FS)
    fig.suptitle("S2: carbohydrate dose-response (CE=0 is the pinned 0 g anchor)",
                 fontsize=SUPTITLE_FS)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


# ---------------------------------------------------------------------------
# S3 — glycemic decomposition + safety: TAR↓ (benign) vs TBR↑/hypo↑ (running low)?
# ---------------------------------------------------------------------------

def table_s3_decomposition(pdf, nma_stats, a81):
    """Per endpoint, within-user paired NMA(CE=0, any BE) − CE>0. Read alongside the TIR row:
    if ΔTIR ≈ −ΔTAR with ΔTBR ≈ 0, the gain is benign (less hyper); a positive ΔTBR / Δhypo is
    a safety flag (running low)."""
    rows = []
    for col, label in ENDPOINTS:
        nma = a81.per_user_arm_mean(pdf, col, NMA_FLAG).dropna()
        cmp = a81.per_user_arm_mean(pdf, col, COMPARATOR_FLAG).dropna()
        res = nma_stats.paired_within_user(nma, cmp)
        rows.append({
            "endpoint": col, "label": label,
            "ce0_mean": nma.mean(), "ce_gt0_mean": cmp.mean(),
            "paired_diff": res["mean_diff"],
            "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
            "wilcoxon_p": res["wilcoxon_p"], "n_pairs": res["n_pairs"],
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# C1–C4 — confounder quick-checks
# ---------------------------------------------------------------------------

def table_c_confounders(pdf, nma_stats, a81):
    """C1 selection bias (CE=0-contributing users vs others, Welch t), C2 CE=0-day clustering
    (per-user run-length), C3 CGM-coverage artifact (paired cbg_count), C4 weighting note."""
    from scipy import stats

    rows = []

    # C1 — selection bias: who contributes CE=0 days?
    ce0_users = set(pdf.loc[pdf[NMA_FLAG] == True, "_userId"])  # noqa: E712
    srt = pdf.sort_values("local_day")
    per_user = srt.groupby("_userId").agg(
        age=("age_years", "first"),
        tir=("tir", "mean"),
        cbg=("cbg_count", "mean"),
        n_days=("local_day", "size"),
    )
    per_user["contrib"] = per_user.index.isin(ce0_users)
    for col, label in [("age", "Age (yr)"), ("tir", "Mean TIR (%)"),
                       ("cbg", "Mean CGM readings/day"), ("n_days", "Eligible days, n")]:
        a = pd.to_numeric(per_user.loc[per_user.contrib, col], errors="coerce").dropna()
        b = pd.to_numeric(per_user.loc[~per_user.contrib, col], errors="coerce").dropna()
        p = (stats.ttest_ind(a, b, equal_var=False).pvalue
             if len(a) >= 3 and len(b) >= 3 else np.nan)
        rows.append({"check": "C1 selection (CE=0 contributors vs others)", "variable": label,
                     "group_a": f"{a.mean():.2f} (n={len(a)})" if len(a) else "N/A",
                     "group_b": f"{b.mean():.2f} (n={len(b)})" if len(b) else "N/A",
                     "stat_p": f"{p:.3g}" if pd.notna(p) else "N/A"})

    # C2 — CE=0-day clustering (reverse-causation/inflated-n proxy): per-user contiguous runs.
    run_lengths = []
    for _uid, g in pdf.loc[pdf[NMA_FLAG] == True].sort_values("local_day").groupby("_userId"):  # noqa: E712
        days = pd.to_datetime(g["local_day"]).sort_values().to_numpy()
        if len(days) == 0:
            continue
        gaps = np.diff(days).astype("timedelta64[D]").astype(int)
        n_runs = 1 + int((gaps > 1).sum())
        run_lengths.append(len(days) / n_runs)  # mean run length for this user
    if run_lengths:
        rl = np.array(run_lengths)
        rows.append({"check": "C2 CE=0-day clustering", "variable": "Mean CE=0 run length (days), per user",
                     "group_a": f"median {np.median(rl):.2f}", "group_b": f"mean {rl.mean():.2f}",
                     "stat_p": f"% users runs>1: {100.0 * (rl > 1).mean():.1f}%"})

    # C3 — CGM-coverage artifact: paired cbg_count NMA − CE>0.
    if "cbg_count" in pdf.columns:
        nma = a81.per_user_arm_mean(pdf, "cbg_count", NMA_FLAG).dropna()
        cmp = a81.per_user_arm_mean(pdf, "cbg_count", COMPARATOR_FLAG).dropna()
        res = nma_stats.paired_within_user(nma, cmp)
        rows.append({"check": "C3 CGM coverage", "variable": "cbg_count/day (NMA vs CE>0)",
                     "group_a": f"CE=0 {nma.mean():.1f}", "group_b": f"CE>0 {cmp.mean():.1f}",
                     "stat_p": f"Δ {res['mean_diff']:.1f} [{res['mean_diff_ci_lo']:.1f}, "
                               f"{res['mean_diff_ci_hi']:.1f}], p={res['wilcoxon_p']:.3g}"})

    # C4 — weighting robustness (documented note; cluster-bootstrap already used throughout).
    rows.append({"check": "C4 weighting", "variable": "day-clustering / user-weighting",
                 "group_a": "cluster-bootstrap CIs (used in S1/S3) resample users",
                 "group_b": "stringent arms: report no directional claim (see weighting_sensitivity.md)",
                 "stat_p": ""})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort="all",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Run the supplement for one age cohort → outputs/analysis_8_supp/<cohort>/."""
    nma_stats = load_nma_statistics()
    a81 = _load_a81()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_supp", cohort)
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if csv_path is None and spark is None:
        csv_path = default_analysis_ready_csv(analysis_ready_table)
        print(f"no Spark session; reading analysis-ready snapshot: {csv_path}")
    if csv_path is not None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"analysis-ready CSV not found at {csv_path}. Run "
                "data_staging/export_user_day_analysis_ready.py first, or pass --csv_path.")
        pdf = prepare_day_level(pd.read_csv(csv_path))
    else:
        pdf = load_day_level(spark, analysis_ready_table)
    pdf = filter_cohort(pdf, cohort=cohort, min_age=min_age)
    pdf = restrict_comparator(pdf)

    # S1
    table_s1_intake(pdf, nma_stats, a81).to_csv(
        os.path.join(output_dir, "table_s1_intake_characterization.csv"), index=False)
    fig = figure_s1_intake(pdf, a81)
    fig.savefig(os.path.join(output_dir, "figure_s1_intake_violin.png"), dpi=150)
    plt.close(fig)

    # S2 (+ continuous slope LMM appended as extra rows)
    s2 = table_s2_dose_response(pdf, a81)
    slopes = [s for s in (carb_slope_lmm(pdf, o) for o in ("tir", "tar")) if s]
    if slopes:
        slope_df = pd.DataFrame([{
            "outcome": s["outcome"], "carb_bin": "LMM slope / 10 g",
            "mean": s["slope_per_10g"], "sd": np.nan,
            "n_users": s["n_users"], "n_days": s["n_days"],
            "slope_ci_lo": s["ci_lo"], "slope_ci_hi": s["ci_hi"], "slope_p": s["pvalue"],
        } for s in slopes])
        s2 = pd.concat([s2, slope_df], ignore_index=True)
    s2.to_csv(os.path.join(output_dir, "table_s2_carb_dose_response.csv"), index=False)
    fig = figure_s2_dose_response(s2[s2["carb_bin"] != "LMM slope / 10 g"])
    fig.savefig(os.path.join(output_dir, "figure_s2_carb_dose_response.png"), dpi=150)
    plt.close(fig)

    # S3 (reuse §8.1 stacked-range + TBR-violin figures, re-presented under the safety question)
    table_s3_decomposition(pdf, nma_stats, a81).to_csv(
        os.path.join(output_dir, "table_s3_decomposition.csv"), index=False)
    fig = a81.make_stacked_bar(pdf)
    fig.savefig(os.path.join(output_dir, "figure_s3_stacked_ranges.png"), dpi=150)
    plt.close(fig)
    fig = figure_s3_tbr(pdf, a81)
    fig.savefig(os.path.join(output_dir, "figure_s3_tbr_violin.png"), dpi=150)
    plt.close(fig)

    # C1–C4
    table_c_confounders(pdf, nma_stats, a81).to_csv(
        os.path.join(output_dir, "table_c_confounders.csv"), index=False)

    print(f"wrote supplement ({cohort}) outputs to {output_dir}")


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
):
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path)


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--analysis_ready_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--csv_path", default=None)
    _parser.add_argument("--output_dir", default=None)
    _parser.add_argument("--cohort", default=None, choices=["adult", "pediatric", "all"])
    _parser.add_argument("--min_age", type=int, default=MIN_AGE)
    _args, _ = _parser.parse_known_args()

    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        _spark = None

    if _args.cohort is None:
        main(_spark, _args.analysis_ready_table, min_age=_args.min_age, csv_path=_args.csv_path)
    else:
        run(_spark, _args.analysis_ready_table, _args.output_dir,
            cohort=_args.cohort, min_age=_args.min_age, csv_path=_args.csv_path)
