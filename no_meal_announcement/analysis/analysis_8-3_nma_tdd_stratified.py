#!/usr/bin/env python
"""Analysis 8.3: glycemic outcomes on NMA-like (CE=0) days, stratified by within-user TDD.

PLN-1008 §8.3. On CE=0 days, within each user, compare days where total daily insulin (TDD)
is BELOW the user's personal average (Low-TDD, R<1.0) vs AT/ABOVE it (High-TDD, R≥1.0), where
R = tdd_units / mean_tdd_user. The plan's motivating hypothesis: **Low-TDD CE=0 days ≈ genuinely
light/skipped intake (algorithm should do well); High-TDD CE=0 days ≈ unannounced meals the
controller absorbed without a carb signal (stress test).** This is the pre-specified bridge for
the supplement's "intake confound" story — it separates light-intake CE=0 days from
unannounced-meal CE=0 days using TDD as the (dose-based, unvalidated) intake proxy.

Mirrors §8.1/§8.2 per-cohort run()/main() + output-clearing; reuses utils/data_loader (cohort
filter, comparator restriction) and utils/statistics (paired_within_user, lmm_tdd_stratum).

Eligibility: ≥30 eligible days for a reliable personal TDD reference (`n_eligible_days_for_tdd`)
AND ≥1 CE=0 day in EACH stratum (for the within-user Low−High pairing).

Sign convention: contrasts are **Low − High** (lmm_tdd_stratum ref = High).

High meal-announcement (CE>=3/BE>=3) arm — mirrors §8.1's high-engagement treatment: HMA days are
stratified Low/High by within-user TDD just like CE=0 days, shown as a 3rd group/section in the
primary stratum figures + Table 8.3a (bronze), and emitted in the Appendix §12.3 supplement (its
own within-user contrast + day-level LMM). Overlapping reference (CE>=3/BE>=3 ⊂ CE>0).

Appendix §12.3 supplement (§8.3 sensitivities — flat *_12_3* names; cf. §8.1 §12.1 / §8.2 §12.2):
two alternative TDD-reference definitions (median; rolling-30-day mean), each a full mini-analysis
(per-user-by-stratum table + violin figure + within-user contrast), plus the HMA arm (within-user
contrast + LMM). ⚠️ All inherit the §8.3 D12 not-citable caveat. (The empirical-tercile sensitivity
was dropped — pending the D12 two-view rank-tercile rework.)

Outputs (analysis/outputs/analysis_8_3/<cohort>/) — primary (mean TDD reference):
    table_8_3a_per_user_by_stratum.csv     per section × endpoint × stratum: across-user mean±SD —
                                           sections = the 3 nested CE=0 classifications + CE>0 + CE>=3/BE>=3 (HMA)
    table_8_3b_within_user_contrast.csv    per classification × endpoint: within-user Low−High (Wilcoxon + boot CI + paired-t)
    table_8_3c_lmm_sensitivity.csv         day-level LMM outcome ~ tdd_stratum + (1|user)
    figure_8_3a_grid{1,2}_*.png            per-user endpoints by stratum (CE=0 / CE>0 / CE>=3-BE>=3 Low&High),
                                           violin+box+dots, two 2×2 metric grids; colour = glycemic range
    figure_8_3b_grid{1,2}_*.png            within-user Low−High delta histograms (CE=0 / CE>0 / CE>=3-BE>=3), two 2×2 grids
    figure_8_3c_stacked_ranges.png         mean glycemic ranges: CE=0 Low/High vs CE>0 + CE>=3/BE>=3 references
    figure_8_3d_R_distribution.png         within-user R = tdd/mean_tdd distribution on CE=0 days
    figure_8_3e_tir_vs_tdd_percentile.png  scatter: per-day TIR vs within-user TDD percentile, coloured by
                                           CE/BE category (CE=0 BE=0/1/≥2 + CE>0) + 11-dot decile-mean trend
  Appendix §12.3 supplement:
    table_12_3a_median_per_user_by_stratum.csv   median ref — per-user-by-stratum (5 sections)
    figure_12_3a_median_grid{1,2}_*.png          median ref — per-user violin grids (6 groups)
    table_12_3b_median_within_user.csv           median ref — within-user Low−High (CE=0)
    table_12_3c_rolling_per_user_by_stratum.csv  rolling-30d ref — per-user-by-stratum (5 sections)
    figure_12_3c_rolling_grid{1,2}_*.png         rolling-30d ref — per-user violin grids (6 groups)
    table_12_3d_rolling_within_user.csv          rolling-30d ref — within-user Low−High (CE=0)
    table_12_3e_high_engagement_within_user.csv  HMA — within-user Low−High (8.3b parallel)
    table_12_3f_high_engagement_lmm.csv          HMA — day-level LMM (8.3c parallel)

Rolling-30-day reference (§7.5) is computed in-analysis from per-day tdd_units + local_day
(trailing 30-calendar-day mean; no staging column needed).
Caveat: residual high-TDD outliers (300+ U/day) land in the High stratum (architecture Open
Questions) — flagged; winsorize upstream before strong High-stratum claims.

Usage: python analysis_8-3_nma_tdd_stratified.py [--cohort {adult,pediatric,all}]
"""

# %pip install statsmodels
# dbutils.library.restartPython()

from __future__ import annotations

import argparse
import os
import shutil
import sys
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

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
    HIGH_MA_FLAG,
    HIGH_MA_LABEL,
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
    GRAY,
    GRIDS,
    HIGH_MA_COLOR,
    LEGEND_FS,
    RANGE_COLORS,
    SUPTITLE_FS,
    TITLE_FS,
    endpoint_color,
    overlay_hist_panel,
    violin_box_panel,
)

MIN_REF_DAYS = 30        # ≥30 eligible days for a reliable personal TDD reference (§8.3)
R_CUT = 1.0              # Low (R<1.0) vs High (R≥1.0)
ROLL_WINDOW_DAYS = 30    # rolling TDD reference window (§7.5)
ROLL_MIN_DAYS = 7        # min eligible days in the window for a usable rolling reference
BOOTSTRAP_SEED = 20260520
# Colours, the 2×2 metric grids, and the violin/histogram panel helpers are shared across
# §8.1–§8.3 (utils.plotting): CE=0 data carries the endpoint's glycemic-range colour, the CE>0
# comparator is grey, and within each the Low/High TDD stratum is light/dark (alpha).


def _ce0_strata(pdf, nma_flag, ratio_col="tdd_ratio"):
    """CE=0 days of one classification for TDD-reference-eligible users, with a Low/High
    `tdd_stratum` from `ratio_col` (= tdd/reference) cut at R_CUT."""
    df = pdf[(pdf[nma_flag] == True)  # noqa: E712
             & (pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS)
             & (pdf[ratio_col].notna())].copy()
    df["tdd_stratum"] = np.where(df[ratio_col] < R_CUT, "Low", "High")
    return df


def _per_user_stratum_mean(df, col, stratum):
    return df[df["tdd_stratum"] == stratum].groupby("_userId")[col].mean()


def table_8_3a_per_user_by_stratum(strata_by_cls):
    """Across-user mean ± SD of each endpoint's per-user within-stratum mean, by section × Low/High
    stratum, with user/day counts. Reused for the primary mean-reference Table 8.3a and the
    Appendix §12.3 median- / rolling-reference per-user-by-stratum variants."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            for stratum in ("Low", "High"):
                m = _per_user_stratum_mean(df, col, stratum).dropna()
                sub = df[df["tdd_stratum"] == stratum]
                rows.append({
                    "classification": cls_label, "endpoint": col, "label": ep_label,
                    "stratum": stratum,
                    "mean": m.mean() if len(m) else np.nan,
                    "sd": m.std(ddof=1) if len(m) > 1 else np.nan,
                    "n_users": int(len(m)), "n_days": int(len(sub)),
                })
    return pd.DataFrame(rows)


def table_8_3b_within_user(strata_by_cls, nma_stats):
    """Within-user Low−High paired contrast per classification × endpoint (Wilcoxon primary,
    paired-t companion, cluster-bootstrap CI). Only users with ≥1 day in BOTH strata contribute."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            low = _per_user_stratum_mean(df, col, "Low").dropna()
            high = _per_user_stratum_mean(df, col, "High").dropna()
            res = nma_stats.paired_within_user(low, high)  # diff = Low − High
            rows.append({
                "classification": cls_label, "endpoint": col, "label": ep_label,
                "low_mean": low.mean() if len(low) else np.nan,
                "high_mean": high.mean() if len(high) else np.nan,
                "diff_low_minus_high": res["mean_diff"],
                "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
                "median_diff": res["median_diff"],
                "wilcoxon_p": res["wilcoxon_p"], "t_p": res["t_p"], "n_pairs": res["n_pairs"],
            })
    return pd.DataFrame(rows)


def table_8_3c_lmm(strata_by_cls, nma_stats):
    """Day-level LMM sensitivity: outcome ~ tdd_stratum + (1|user) per classification × endpoint
    (ref = High; coef = Low − High). Degenerate slices (a stratum <2 users or constant outcome)
    yield a converged=False NaN row."""
    rows = []
    for cls_label, df in strata_by_cls.items():
        for col, ep_label in ENDPOINTS:
            sl = df[["_userId", "tdd_stratum", col]].dropna(subset=[col])
            n_low = sl.loc[sl["tdd_stratum"] == "Low", "_userId"].nunique()
            n_high = sl.loc[sl["tdd_stratum"] == "High", "_userId"].nunique()
            row = {"classification": cls_label, "endpoint": col, "label": ep_label,
                   "coef_low_minus_high": np.nan, "ci_lo": np.nan, "ci_hi": np.nan,
                   "pvalue": np.nan, "converged": False,
                   "n_users": int(sl["_userId"].nunique()), "n_days": int(len(sl))}
            if n_low >= 2 and n_high >= 2 and sl[col].nunique() >= 2:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = nma_stats.lmm_tdd_stratum(sl, outcome=col)
                    assert "Low" in res["term"], f"sign convention: term={res['term']}"
                    row.update({"coef_low_minus_high": res["coef"], "ci_lo": res["ci_lo"],
                                "ci_hi": res["ci_hi"], "pvalue": res["pvalue"],
                                "converged": True, "n_users": res["n_users"], "n_days": res["n_days"]})
                except Exception as e:  # noqa: BLE001
                    print(f"  §8.3 LMM failed for {cls_label} / {col}: {e}")
            rows.append(row)
    return pd.DataFrame(rows)


def _stratum_delta(df, col):
    """Per-user (Low mean − High mean) array for one endpoint."""
    low = _per_user_stratum_mean(df, col, "Low")
    high = _per_user_stratum_mean(df, col, "High")
    return pd.DataFrame({"L": low, "H": high}).dropna().eval("L - H").to_numpy()


def figure_8_3b_paired_delta(strata_by_cls, cmp_strata, hma_strata):
    """Figure 8.3b (two semantic 2×2 grids): within-user Low−High TDD delta histograms
    (CE=0/BE≤∞ arm), one grid per metric group; overlays CE=0 (endpoint range colour), CE>0
    (grey) and the high meal-announcement CE>=3/BE>=3 arm (bronze). Solid line = each
    distribution's mean, dashed = 0. Returns {filename: figure}. (Per-classification deltas are in
    table_8_3b_*.csv; the CE>=3/BE>=3 contrast in table_12_3a_high_engagement_tdd_strata.csv.)"""
    df = strata_by_cls["CE=0/BE<=inf"]
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(2, 2, figsize=(10, 8))
        for ax, (col, label) in zip(axes.ravel(), eps):
            base = endpoint_color(col)
            overlay_hist_panel(
                ax,
                [(_stratum_delta(df, col), "CE=0", base),
                 (_stratum_delta(cmp_strata, col), "CE>0", GRAY),
                 (_stratum_delta(hma_strata, col), HIGH_MA_LABEL, HIGH_MA_COLOR)],
                xlabel="per-user Low−High Δ", title=label, title_color=base)
        fig.suptitle(f"Figure 8.3b — {gtitle}\nwithin-user Low − High differences (BE≤∞)",
                     fontsize=SUPTITLE_FS)
        fig.tight_layout(rect=[0, 0, 1, 0.92])
        out[f"figure_8_3b_{key}.png"] = fig
    return out


def figure_8_3c_stacked(pdf, strata_inf):
    """Mean % time in glycemic ranges: Low vs High CE=0 days vs CE>0 reference."""
    def ranges(frame):
        f = frame.copy()
        f["<54"] = f["tbr_very_low"]; f["54-70"] = f["tbr"] - f["tbr_very_low"]
        f["70-180"] = f["tir"]; f["180-250"] = f["tar"] - f["tar_very_high"]; f[">250"] = f["tar_very_high"]
        keys = list(RANGE_COLORS)
        um = f.groupby("_userId")[keys].mean()
        return {k: (um[k].mean() if len(um) else 0.0) for k in keys}, int(len(um))

    groups = [("CE=0 Low-TDD", strata_inf[strata_inf["tdd_stratum"] == "Low"]),
              ("CE=0 High-TDD", strata_inf[strata_inf["tdd_stratum"] == "High"]),
              ("CE>0 (ref)", pdf[pdf[COMPARATOR_FLAG] == True]),  # noqa: E712
              (f"{HIGH_MA_LABEL} (ref)", pdf[pdf[HIGH_MA_FLAG] == True])]  # noqa: E712
    fig, ax = plt.subplots(figsize=(9.5, 7))
    x = np.arange(len(groups))
    bottom = np.zeros(len(groups))
    means = {k: [] for k in RANGE_COLORS}
    ns = []
    for _, frame in groups:
        m, nu = ranges(frame)
        ns.append(nu)
        for k in RANGE_COLORS:
            means[k].append(m[k])
    for k, color in RANGE_COLORS.items():
        vals = np.array(means[k])
        ax.bar(x, vals, bottom=bottom, label=k, color=color, edgecolor="white")
        bottom += vals
    ax.set_xticks(x); ax.set_xticklabels([g for g, _ in groups])
    ax.set_ylim(0, 108); ax.set_ylabel("Mean time in range (%)")
    for xi, nu in enumerate(ns):
        ax.text(xi, 101, f"users={nu}", ha="center", va="bottom", fontsize=11)
    ax.legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=LEGEND_FS)
    ax.set_title("Figure 8.3c: glycemic ranges by TDD stratum (CE=0) vs CE>0 & CE>=3/BE>=3 refs",
                 fontsize=TITLE_FS)
    fig.tight_layout()
    return fig


def figure_8_3d_r_dist(strata_inf):
    """Within-user R = tdd/mean_tdd distribution on CE=0 days, with the R=1.0 cut."""
    fig, ax = plt.subplots(figsize=(9, 5))
    r = strata_inf["tdd_ratio"].clip(0, 3).dropna()
    ax.hist(r, bins=60, color="#607cff", edgecolor="white", alpha=0.85)
    ax.axvline(R_CUT, color="#E03830", ls="--", lw=2, label="R = 1.0 (Low | High)")
    ax.set_xlabel("R = day TDD / user mean TDD (clipped at 3)")
    ax.set_ylabel("CE=0 user-days")
    ax.legend()
    ax.set_title("Figure 8.3d: within-user TDD ratio on CE=0 days", fontsize=TITLE_FS)
    fig.tight_layout()
    return fig


# Four mutually-exclusive day categories for figure_8_3e, each its own colour. CE=0 is split by
# bolus-entry count on a diverging green→amber→red ramp (escalating BE: 0 / 1 / ≥2); CE>0 is the
# grey comparator.
CE_BE_COLORS = {
    "CE=0/BE=0": "#1a9850",   # green
    "CE=0/BE=1": "#f1a340",   # amber
    "CE=0/BE≥2": "#d73027",   # red
    "CE>0": GRAY,             # grey comparator
}


def figure_8_3e_tir_vs_tdd_pct(pdf):
    """For-fun scatter: per-day TIR vs the day's within-user TDD percentile, for TDD-reference-
    eligible users, every eligible day coloured by CE/BE category — CE=0 split by bolus count
    (BE=0 / BE=1 / BE≥2) plus the CE>0 comparator. Percentile = each day's rank of tdd_units
    within that user's eligible days (0–100), comparable across users. Each category gets its own
    decile-mean TIR line (11 dots on the x-ticks), plus a dashed black overall-mean line across all
    days; the faint scatter behind shows day-level spread."""
    df = pdf[pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS].dropna(subset=["tdd_units", "tir"]).copy()
    df["tdd_pct"] = df.groupby("_userId")["tdd_units"].rank(pct=True) * 100.0
    ce0 = df["carb_entry_count"] == 0
    be = df["bolus_entry_count"]
    df["cat"] = np.select(
        [~ce0, ce0 & (be == 0), ce0 & (be == 1)],
        ["CE>0", "CE=0/BE=0", "CE=0/BE=1"], default="CE=0/BE≥2")

    fig, ax = plt.subplots(figsize=(9.5, 6))
    marks = np.arange(0, 101, 10)
    edges = np.arange(-5, 106, 10)  # 10-pct-wide bins centred on the marks → dots land on ticks

    # Faint scatter for density: largest category on the bottom, CE>0 drawn faintest.
    plot_order = sorted(CE_BE_COLORS, key=lambda c: int((df["cat"] == c).sum()), reverse=True)
    for cat in plot_order:
        sub = df[df["cat"] == cat]
        ax.scatter(sub["tdd_pct"], sub["tir"], s=6, color=CE_BE_COLORS[cat], linewidths=0,
                   zorder=2, alpha=0.07 if cat == "CE>0" else 0.16)

    # A decile-mean TIR line per category (11 dots on the x-ticks), in logical CE/BE order.
    handles = []
    for cat in CE_BE_COLORS:
        sub = df[df["cat"] == cat]
        binned = (sub.assign(_b=pd.cut(sub["tdd_pct"], edges, labels=marks))
                     .groupby("_b", observed=False)["tir"].mean().reindex(marks))
        h, = ax.plot(marks, binned.to_numpy(dtype=float), "-o", color=CE_BE_COLORS[cat], lw=2,
                     ms=5, zorder=5, label=f"{cat} (n={len(sub):,})")
        handles.append(h)

    # Overall mean TIR per decile across all categories (dashed black, on top).
    overall = (df.assign(_b=pd.cut(df["tdd_pct"], edges, labels=marks))
                 .groupby("_b", observed=False)["tir"].mean().reindex(marks))
    h_all, = ax.plot(marks, overall.to_numpy(dtype=float), "--o", color="#111111", lw=2.5, ms=5,
                     zorder=6, label=f"overall (n={len(df):,})")
    handles.append(h_all)

    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_xticks(marks)
    ax.set_xlabel("within-user TDD percentile (all eligible days, %)")
    ax.set_ylabel("Time 70-180 mg/dL (%)")
    ax.legend(handles=handles, fontsize=LEGEND_FS)
    ax.set_title(f"Figure 8.3e: TIR vs within-user TDD percentile by CE/BE category (n={len(df):,})",
                 fontsize=TITLE_FS)
    fig.tight_layout()
    return fig


def _add_rolling_ref(pdf):
    """Add a trailing rolling-30-calendar-day TDD reference + ratio, computed in-analysis from
    per-day tdd_units + local_day (no staging column needed). tdd_ratio_rolling = day TDD ÷ the
    mean TDD over that user's eligible days in the trailing ROLL_WINDOW_DAYS (inclusive); NaN
    until the window holds ≥ ROLL_MIN_DAYS days, so early-record days don't get an unstable ref."""
    out = pdf.copy()
    out["_d"] = pd.to_datetime(out["local_day"])
    out = out.sort_values(["_userId", "_d"])
    # groupby+time-rolling preserves (_userId, _d) order, so .values aligns positionally with `out`.
    s = out.set_index("_d").groupby("_userId")["tdd_units"]
    out["rolling_tdd_mean"] = s.rolling(f"{ROLL_WINDOW_DAYS}D").mean().values
    out["rolling_tdd_count"] = s.rolling(f"{ROLL_WINDOW_DAYS}D").count().values
    out["tdd_ratio_rolling"] = np.where(
        (out["rolling_tdd_count"] >= ROLL_MIN_DAYS) & (out["rolling_tdd_mean"] > 0),
        out["tdd_units"] / out["rolling_tdd_mean"], np.nan)
    return out.drop(columns="_d")


def _violin_panel(ax, endpoint, label, strata_inf, cmp_strata, hma_strata):
    """One endpoint's violin+box+dots panel, 6 groups: CE=0 Low/High + CE>0 Low/High +
    CE>=3/BE>=3 (HMA) Low/High. CE=0 carries the endpoint's glycemic-range colour, the CE>0
    comparator is grey, the high meal-announcement arm is bronze; Low = lighter, High = darker
    (alpha). Drawing convention is shared via utils.plotting.violin_box_panel."""
    base = endpoint_color(endpoint)
    # 2-line tick labels (arm \n stratum) so 6 groups don't collide — violin_box_panel appends the
    # n-count as a 3rd line.
    gdef = [("CE=0\nLow", strata_inf, "Low", base), ("CE=0\nHigh", strata_inf, "High", base),
            ("CE>0\nLow", cmp_strata, "Low", GRAY), ("CE>0\nHigh", cmp_strata, "High", GRAY),
            (f"{HIGH_MA_LABEL}\nLow", hma_strata, "Low", HIGH_MA_COLOR),
            (f"{HIGH_MA_LABEL}\nHigh", hma_strata, "High", HIGH_MA_COLOR)]
    groups = [
        (gl, df.loc[df["tdd_stratum"] == st].groupby("_userId")[endpoint].mean().dropna().to_numpy(),
         color, 0.35 if st == "Low" else 0.7)
        for gl, df, st, color in gdef
    ]
    violin_box_panel(ax, groups, title=label, title_color=base, separators=(2.5, 4.5))


def figure_8_3a_violin(strata_inf, cmp_strata, hma_strata, *, fig_id="8.3a",
                       fname_stem="figure_8_3a", ref_note=""):
    """Per-user means by TDD stratum (two semantic 2×2 grids), one grid per metric group. Returns
    {filename: figure}. Endpoint colour = glycemic range (Tidepool for the non-range metrics); CE=0
    coloured, CE>0 grey, CE>=3/BE>=3 bronze; Low lighter / High darker. `fig_id` / `fname_stem` /
    `ref_note` parametrize the title + filename so the Appendix §12.3 median- and rolling-reference
    variants reuse this (primary = Figure 8.3a; §12.3a = median ref; §12.3c = rolling ref)."""
    ref = f" ({ref_note})" if ref_note else ""
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(2, 2, figsize=(13, 8.6))
        for ax, (col, label) in zip(axes.ravel(), eps):
            _violin_panel(ax, col, label, strata_inf, cmp_strata, hma_strata)
        fig.suptitle(f"Figure {fig_id} — {gtitle}\nper-user means by TDD stratum (CE=0, CE>0, CE>=3/BE>=3){ref}",
                     fontsize=SUPTITLE_FS)
        fig.tight_layout(rect=[0, 0, 1, 0.91])
        out[f"{fname_stem}_{key}.png"] = fig
    return out


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort="all",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Run §8.3 for one age cohort → outputs/analysis_8_3/<cohort>/."""
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_3", cohort)
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

    # Primary strata (mean-TDD reference via tdd_ratio), per nested classification.
    strata = {cls_label: _ce0_strata(pdf, flag) for flag, cls_label in CLASSIFICATIONS}
    # CE>0 comparator and the high meal-announcement (CE>=3/BE>=3) arm, both stratified Low/High by
    # within-user TDD with the same _ce0_strata machinery (HMA: overlapping reference, CE>=3/BE>=3 ⊂
    # CE>0). Used as the extra Table 8.3a sections + the §12.3 contrast + the 3rd/4th groups in the
    # stratum figures (8.3a/8.3b/8.3c).
    cmp_strata = _ce0_strata(pdf, COMPARATOR_FLAG)
    hma_strata = _ce0_strata(pdf, HIGH_MA_FLAG)

    # Table 8.3a (primary, mean reference): the 3 nested CE=0 classifications plus a CE>0 and a
    # CE>=3/BE>=3 (HMA) section, each split Low/High by TDD (matches figure 8.3a's groups).
    strata_8_3a = {**strata, COMPARATOR_LABEL: cmp_strata, HIGH_MA_LABEL: hma_strata}
    table_8_3a_per_user_by_stratum(strata_8_3a).to_csv(
        os.path.join(output_dir, "table_8_3a_per_user_by_stratum.csv"), index=False)
    table_8_3b_within_user(strata, nma_stats).to_csv(
        os.path.join(output_dir, "table_8_3b_within_user_contrast.csv"), index=False)
    table_8_3c_lmm(strata, nma_stats).to_csv(
        os.path.join(output_dir, "table_8_3c_lmm_sensitivity.csv"), index=False)

    # Appendix §12.3 — §8.3 sensitivities (flat *_12_3* names; cf. §8.1 §12.1 / §8.2 §12.2). Three
    # blocks: two alternative TDD-reference definitions (median; rolling-30-day mean), each a full
    # mini-analysis (per-user-by-stratum table + violin figure + within-user contrast), and the
    # high meal-announcement (CE>=3/BE>=3) arm (within-user contrast + day-level LMM, the 8.3c
    # parallel). All overlapping references; ⚠️ inherit the §8.3 D12 not-citable caveat. (The
    # empirical-tercile sensitivity was dropped — pending the D12 two-view rank-tercile rework.)

    # §12.3a/b — median TDD reference (R = tdd / median_tdd_user). The per-user-by-stratum table +
    # violin carry the same 5 sections / 6 groups as the primary; the within-user contrast is CE=0.
    pdf_med = pdf.copy()
    pdf_med["tdd_ratio_median"] = pdf_med["tdd_units"] / pdf_med["median_tdd_user"]
    med = {cls_label: _ce0_strata(pdf_med, flag, ratio_col="tdd_ratio_median")
           for flag, cls_label in CLASSIFICATIONS}
    med_8_3a = {**med,
                COMPARATOR_LABEL: _ce0_strata(pdf_med, COMPARATOR_FLAG, ratio_col="tdd_ratio_median"),
                HIGH_MA_LABEL: _ce0_strata(pdf_med, HIGH_MA_FLAG, ratio_col="tdd_ratio_median")}
    table_8_3a_per_user_by_stratum(med_8_3a).to_csv(
        os.path.join(output_dir, "table_12_3a_median_per_user_by_stratum.csv"), index=False)
    table_8_3b_within_user(med, nma_stats).to_csv(
        os.path.join(output_dir, "table_12_3b_median_within_user.csv"), index=False)

    # §12.3c/d — rolling-30-day mean TDD reference (computed in-analysis from per-day tdd_units +
    # local_day; §7.5).
    pdf_roll = _add_rolling_ref(pdf)
    roll = {cls_label: _ce0_strata(pdf_roll, flag, ratio_col="tdd_ratio_rolling")
            for flag, cls_label in CLASSIFICATIONS}
    roll_8_3a = {**roll,
                 COMPARATOR_LABEL: _ce0_strata(pdf_roll, COMPARATOR_FLAG, ratio_col="tdd_ratio_rolling"),
                 HIGH_MA_LABEL: _ce0_strata(pdf_roll, HIGH_MA_FLAG, ratio_col="tdd_ratio_rolling")}
    table_8_3a_per_user_by_stratum(roll_8_3a).to_csv(
        os.path.join(output_dir, "table_12_3c_rolling_per_user_by_stratum.csv"), index=False)
    table_8_3b_within_user(roll, nma_stats).to_csv(
        os.path.join(output_dir, "table_12_3d_rolling_within_user.csv"), index=False)

    # §12.3e/f — high meal-announcement (CE>=3/BE>=3) arm, mean reference: within-user Low−High
    # contrast (8.3b parallel) + day-level LMM outcome ~ tdd_stratum + (1|user) (8.3c parallel).
    table_8_3b_within_user({HIGH_MA_LABEL: hma_strata}, nma_stats).to_csv(
        os.path.join(output_dir, "table_12_3e_high_engagement_within_user.csv"), index=False)
    table_8_3c_lmm({HIGH_MA_LABEL: hma_strata}, nma_stats).to_csv(
        os.path.join(output_dir, "table_12_3f_high_engagement_lmm.csv"), index=False)

    # Figures (broadest CE=0 arm for stratum-level figures; CE>0 + HMA also split Low/High,
    # computed above).
    strata_inf = strata["CE=0/BE<=inf"]
    figs = {}
    figs.update(figure_8_3a_violin(strata_inf, cmp_strata, hma_strata))     # primary, figure_8_3a_grid{1,2}
    # Appendix §12.3 median- and rolling-reference violin variants (figs 12.3a / 12.3c).
    figs.update(figure_8_3a_violin(med_8_3a["CE=0/BE<=inf"], med_8_3a[COMPARATOR_LABEL],
                                   med_8_3a[HIGH_MA_LABEL], fig_id="12.3a", ref_note="median TDD ref",
                                   fname_stem="figure_12_3a_median"))
    figs.update(figure_8_3a_violin(roll_8_3a["CE=0/BE<=inf"], roll_8_3a[COMPARATOR_LABEL],
                                   roll_8_3a[HIGH_MA_LABEL], fig_id="12.3c", ref_note="rolling-30-day TDD ref",
                                   fname_stem="figure_12_3c_rolling"))
    figs.update(figure_8_3b_paired_delta(strata, cmp_strata, hma_strata))   # two 2×2 grid figures
    figs["figure_8_3c_stacked_ranges.png"] = figure_8_3c_stacked(pdf, strata_inf)
    figs["figure_8_3d_R_distribution.png"] = figure_8_3d_r_dist(strata_inf)
    figs["figure_8_3e_tir_vs_tdd_percentile.png"] = figure_8_3e_tir_vs_tdd_pct(pdf)
    for fname, fig in figs.items():
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)

    print(f"wrote analysis 8.3 ({cohort}) outputs to {output_dir}")


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
