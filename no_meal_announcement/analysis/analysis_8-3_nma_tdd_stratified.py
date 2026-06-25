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

Same-user-set-gated rank terciles (D12 rework) replace the dropped empirical terciles: each day is
labeled Low/Mid/High (or binary Low/High) by a balanced WITHIN-USER TDD RANK, in two reference views
— OVERALL (rank over the user's all eligible days, à la fig 8.3e) and CE=0 (rank within the arm's own
days) — gated to users present in every stratum so the across-user means are apples-to-apples. The
OVERALL-reference tercile is promoted to the PRIMARY section (Table 8.3d + figs 8.3f/8.3g); the CE=0
view, both binaries, and the within-user bottom−top contrasts live in the Appendix §12.3 supplement.

The rank terciles are robust to residual high-TDD outliers (300+ U/day) by construction — they cut on
the within-user TDD RANK, not the magnitude — so no winsorization is applied. (A 300 U/day cap was
evaluated and found immaterial: it touches ≈6 eligible days / 5 users in the snapshot, none of them
CE=0 days, and leaves the rank strata essentially unchanged.)

Appendix §12.3 supplement (§8.3 sensitivities — flat *_12_3* names; cf. §8.1 §12.1 / §8.2 §12.2):
two alternative TDD-reference definitions (median; rolling-30-day mean), each a full mini-analysis
(per-user-by-stratum table + violin figure + within-user contrast); the HMA arm (within-user contrast
+ LMM); and the rest of the two-view rank-tercile rework (CE=0-ref tercile, both binaries, within-user
bottom−top). The median/rolling/HMA sensitivities still inherit the §8.3 D12 not-citable caveat; see
decisions.md for which rank-tercile outputs the D12 update lifts that caveat for.

Outputs (analysis/outputs/analysis_8_3/<cohort>/) — primary (mean TDD reference):
    table_8_3a_per_user_by_stratum.csv     per section × endpoint × stratum: across-user mean±SD —
                                           sections = the 3 nested CE=0 classifications + CE>0 + CE>=3/BE>=3 (HMA)
    table_8_3b_within_user_contrast.csv    per classification × endpoint: within-user Low−High (Wilcoxon + boot CI + paired-t)
    table_8_3c_lmm_sensitivity.csv         day-level LMM outcome ~ tdd_stratum + (1|user)
    table_8_3d_rank_tercile_strata.csv     OVERALL-ref rank TERCILES (Low/Mid/High), 5 sections,
                                           same-user-set gated — the promoted primary tercile table
    figure_8_3a_grid{1,2}_*.png            per-user endpoints by stratum (CE=0 / CE>0 / CE>=3-BE>=3 Low&High),
                                           violin+box+dots, two 2×2 metric grids; colour = glycemic range
    figure_8_3f_grid{1,2}_*.png            OVERALL-ref rank-tercile per-user violins (9 groups: CE=0/CE>0/HMA × Low/Mid/High)
    figure_8_3g_grid{1,2}_*.png            OVERALL-ref: the 5 day types (3 CE=0 + CE>0 + HMA) by tercile, staggered vertical 95% CI bars, all 8 endpoints
    figure_8_3b_grid{1,2}_*.png            within-user Low−High delta histograms (CE=0 / CE>0 / CE>=3-BE>=3), two 2×2 grids
    figure_8_3c_stacked_ranges.png         mean glycemic ranges — 10 stacked bars (5 day types × Low/High), labeled %s + dashed Low→High segment connectors
    figure_8_3d_R_distribution.png         within-user R = tdd/mean_tdd distribution on CE=0 days
    figure_8_3e_tir_vs_tdd_percentile.png  scatter: per-day TIR vs within-user TDD percentile, coloured by the
                                           SAME 5 day types as fig 8.3g (TIR band ramp); per-day-type decile lines + overall.
                                           Horizontal TIR gridlines every 5% (P1).
    figure_8_3i_tir_vs_tdd_absolute.png    NEW (P2): the same chart on an ABSOLUTE-TDD x-axis (U/day, 10-U bins, x≤120 U/day),
                                           with a stacked per-bin user-day DENSITY PANEL (P3) below. Body-size caveat stamped;
                                           read per-cohort. Descriptive (NOT the within-user contrast).
    figure_8_3j_tir_vs_tdd_percentile_density.png  the percentile chart (8.3e axis) + the density panel (P3) —
                                           the percentile companion of 8.3i (the panel is ~flat: the rank axis is uniform)
  Appendix §12.3 supplement:
    table_12_3a_median_per_user_by_stratum.csv   median ref — per-user-by-stratum (5 sections)
    figure_12_3a_median_grid{1,2}_*.png          median ref — per-user violin grids (6 groups)
    table_12_3b_median_within_user.csv           median ref — within-user Low−High (CE=0)
    table_12_3c_rolling_per_user_by_stratum.csv  rolling-30d ref — per-user-by-stratum (5 sections)
    figure_12_3c_rolling_grid{1,2}_*.png         rolling-30d ref — per-user violin grids (6 groups)
    table_12_3d_rolling_within_user.csv          rolling-30d ref — within-user Low−High (CE=0)
    table_12_3e_high_engagement_within_user.csv  HMA — within-user Low−High (8.3b parallel)
    table_12_3f_high_engagement_lmm.csv          HMA — day-level LMM (8.3c parallel)
    table_12_3g_ce0_rank_tercile_strata.csv      CE=0-ref rank terciles (the other view), gated
    table_12_3h_rank_binary_strata.csv           both refs' balanced binary Low/High, gated
    table_12_3i_rank_within_user_overall.csv     overall-ref within-user bottom−top (binary + tercile)
    table_12_3j_rank_within_user_ce0.csv         CE=0-ref within-user bottom−top (binary + tercile)
    figure_12_3g_ce0_grid{1,2}_*.png             CE=0-ref rank-tercile per-user violins (9 groups)
    figure_12_3h_overall_tercile_scatter.png     fig-8.3e scatter with overall-ref tercile bands shaded
    figure_12_3i_ce0_bars_grid{1,2}_*.png        CE=0-ref companion of fig 8.3g: 5 day types by tercile, staggered 95% CI bars

Rolling-30-day reference (§7.5) is computed in-analysis from per-day tdd_units + local_day
(trailing 30-calendar-day mean; no staging column needed).
Residual high-TDD outliers (300+ U/day) were evaluated and not winsorized: they are ≈6 eligible days
/ 5 users in the snapshot (none CE=0 days), the rank terciles are robust to them by construction, and
the mean/median/rolling references inherit the staged values — so a cap was immaterial.

Usage: python analysis_8-3_nma_tdd_stratified.py [--cohort {adult,pediatric,all}]
Fast figure iteration (skips the slow LMM/bootstrap tables): add --figures-only, or --figs <tag>
to render only matching figures (e.g. `--figs 8_3g --cohort all` ≈ 9 s vs a multi-minute full run).
Tags: 8_3a/b/c/d/e/f/g/i/j, 12_3a/c/g/h/i. Do a full run first so the table CSVs exist.
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
import matplotlib.ticker as mticker  # noqa: E402

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
    STRATEGIES,
    STRATEGY_COL,
    SUPPLEMENT_ARMS,
    analysis_dir,
    default_analysis_ready_csv,
    filter_cohort,
    load_day_level,
    load_nma_statistics,
    prepare_day_level,
    restrict_comparator,
)
from utils.plotting import (  # noqa: E402
    DAY_TYPE_ORDER,
    GRAY,
    GRIDS,
    HIGH_MA_COLOR,
    LEGEND_FS,
    RANGE_COLORS,
    SUPTITLE_FS,
    TITLE_FS,
    day_type_colors,
    day_type_legend,
    endpoint_color,
    overlay_hist_panel,
    render_4x2_grid,
    violin_box_panel,
)
from utils.strata import (  # noqa: E402
    MIN_REF_DAYS,
    R_CUT,
    RANK_TERCILES,
    _ce0_strata,
    _rank_sections,
    _rank_strata,
    _stratum_delta,
    _tercile_trend_stats,
    table_8_3a_per_user_by_stratum,
    table_8_3b_within_user,
    table_8_3c_lmm,
    table_rank_across_user,
    table_rank_within_user,
)

# MIN_REF_DAYS / R_CUT / RANK_TERCILES / RANK_BINARY + the strata and base-table machinery
# (_ce0_strata, _rank_strata, _same_user_set_gate, table_8_3a/b/c, _rank_sections, table_rank_*,
# _stratum_delta, _tercile_trend_stats) now live in utils.strata (shared with §8.4); imported above.
ROLL_WINDOW_DAYS = 30    # rolling TDD reference window (§7.5)
ROLL_MIN_DAYS = 7        # min eligible days in the window for a usable rolling reference
BOOTSTRAP_SEED = 20260520
# fig 8.3e family: y-axis TIR reference gridlines (P1), the absolute-TDD x-axis variant figure_8_3i
# (P2), and the density-panel / CI-ribbon overlays (P3). Hoisted per the "cutoffs at the top" rule.
TIR_GRID_STEP = 5        # horizontal TIR gridline spacing (%) — read line values off the chart (P1)
ABS_TDD_BIN_W = 10       # U/day fixed-width bins for the absolute-TDD decile line (P2)
ABS_TDD_XMAX = 120       # absolute-TDD x-axis clip — ~p98.4 of eligible days; tail to 431 is <2% & sparse
ABS_TDD_EDGES = np.arange(0, ABS_TDD_XMAX + 1, ABS_TDD_BIN_W)      # 0,10,…,120 (12 bins)
ABS_TDD_MARKS = (ABS_TDD_EDGES[:-1] + ABS_TDD_EDGES[1:]) / 2.0     # bin centres 5,15,…,115
ABS_TDD_MIN_BIN_N = 100  # absolute-TDD line dot suppressed below this many days; trims the sparse
                         # absolute-TDD tails (percentile axis keeps every bin) — P2
# Colours, the 2×2 metric grids, and the violin/histogram panel helpers are shared across
# §8.1–§8.3 (utils.plotting): CE=0 data carries the endpoint's glycemic-range colour, the CE>0
# comparator is grey, and within each the Low/High TDD stratum is light/dark (alpha).


# _ce0_strata / _rank_strata / _same_user_set_gate / _per_user_stratum_mean / table_8_3a/b/c /
# _rank_sections / table_rank_across_user / table_rank_within_user / _stratum_delta now live in
# utils.strata (shared with §8.4); imported above. Figure builders below still consume them.


def figure_8_3b_paired_delta(strata_by_cls, cmp_strata, hma_strata):
    """Figure 8.3b (single 4×2 grid): within-user Low−High TDD delta histograms for the headline
    CE=0/BE≤1 arm, all 8 endpoints; overlays CE=0/BE≤1 (endpoint range colour), CE>0 (grey) and the
    high meal-announcement CE>=3/BE>=3 arm (bronze). Solid line = each distribution's mean, dashed = 0.
    Returns {filename: figure}. (All 3 nested arms' deltas are in table_8_3b_*.csv; the CE>=3/BE>=3
    contrast in table_12_3e_high_engagement_within_user.csv.)"""
    df = strata_by_cls["CE=0/BE<=1"]

    def panel(ax, col, label):
        base = endpoint_color(col)
        overlay_hist_panel(
            ax,
            [(_stratum_delta(df, col), "CE=0/BE<=1", base),
             (_stratum_delta(cmp_strata, col), "CE>0", GRAY),
             (_stratum_delta(hma_strata, col), HIGH_MA_LABEL, HIGH_MA_COLOR)],
            xlabel="per-user Low−High Δ", title=label, title_color=base)

    return render_4x2_grid(panel, fig_stem="figure_8_3b",
                           subtitle="within-user Low − High differences — headline arm: CE=0 / BE≤1")


def figure_8_3c_stacked(strata_8_3a):
    """Mean % time in glycemic ranges by TDD stratum, for all 5 day types (the 3 nested CE=0
    classifications + CE>0 + CE>=3/BE>=3 HMA), each split Low/High → **10 stacked bars**. Mean-
    reference (R = tdd/mean) binary strata, matching Table 8.3a / figure 8.3a (which `strata_8_3a`
    carries, in SUPPLEMENT_ARMS order). A light separator divides each day type's Low|High pair."""
    def ranges(frame):
        f = frame.copy()
        f["<54"] = f["tbr_very_low"]; f["54-70"] = f["tbr"] - f["tbr_very_low"]
        f["70-180"] = f["tir"]; f["180-250"] = f["tar"] - f["tar_very_high"]; f[">250"] = f["tar_very_high"]
        keys = list(RANGE_COLORS)
        um = f.groupby("_userId")[keys].mean()
        return {k: (um[k].mean() if len(um) else 0.0) for k in keys}, int(len(um))

    groups = [(f"{label}\n{st}", df[df["tdd_stratum"] == st])
              for label, df in strata_8_3a.items() for st in ("Low", "High")]
    fig, ax = plt.subplots(figsize=(15, 7))
    x = np.arange(len(groups))
    ns = []
    means = {k: [] for k in RANGE_COLORS}
    for _, frame in groups:
        m, nu = ranges(frame)
        ns.append(nu)
        for k in RANGE_COLORS:
            means[k].append(m[k])
    order = list(RANGE_COLORS)
    seg = np.array([means[k] for k in order])               # (n_ranges, n_groups)
    tops = np.cumsum(seg, axis=0)                            # cumulative top of each segment
    bots = np.vstack([np.zeros(len(groups)), tops[:-1]])     # bottom of each segment
    W = 0.8
    for ki, (k, color) in enumerate(RANGE_COLORS.items()):
        ax.bar(x, seg[ki], bottom=bots[ki], width=W, label=k, color=color, edgecolor="white")
    # Labeled percentages — simple black text centred in each segment (matching §8.1): skip the
    # smallest <54 band, offset the thin 54-70 band upward with a thin leader, label the rest centred.
    for gi in range(len(groups)):
        for ki, k in enumerate(order):
            v = seg[ki, gi]
            cen = bots[ki, gi] + v / 2.0
            if k == "<54":
                continue
            if k == "54-70":
                ax.annotate(f"{v:.1f}%", xy=(gi, cen), xytext=(0, 16), textcoords="offset points",
                            ha="center", va="bottom", fontsize=10,
                            arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
            else:
                ax.text(gi, cen, f"{v:.1f}%", ha="center", va="center", fontsize=10)
    # Dashed connectors across each day type's Low|High pair, one per internal segment boundary —
    # show how each range shifts Low→High (the gap between the paired bars).
    for j in range(0, len(groups), 2):
        for b in range(len(order) - 1):
            ax.plot([j + W / 2, j + 1 - W / 2], [tops[b, j], tops[b, j + 1]],
                    ls="--", color="0.4", lw=1.0, zorder=4)
    for sep in range(2, len(groups), 2):  # divide each day type's Low|High pair
        ax.axvline(sep - 0.5, color="0.85", lw=1, zorder=0)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{glabel}\n(n={nu})" for (glabel, _), nu in zip(groups, ns)], fontsize=9)
    ax.set_ylim(0, 105); ax.set_ylabel("Mean time in range (%)")
    ax.set_xlim(-0.6, len(groups) - 0.4)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, title="Glucose (mg/dL)", loc="lower center", ncol=len(labels),
               fontsize=LEGEND_FS, bbox_to_anchor=(0.5, 0.0))
    ax.set_title("Glycemic ranges by TDD stratum — 5 day types × Low/High (mean-ref)",
                 fontsize=TITLE_FS)
    fig.tight_layout(rect=[0, 0.1, 1, 1])  # reserve bottom for the horizontal legend
    return fig


def figure_8_3d_r_dist(ce0_strata):
    """Within-user R = tdd/mean_tdd distribution on the headline CE=0/BE≤1 days, with the R=1.0 cut."""
    fig, ax = plt.subplots(figsize=(9, 5))
    r = ce0_strata["tdd_ratio"].clip(0, 3).dropna()
    ax.hist(r, bins=60, color="#607cff", edgecolor="white", alpha=0.85)
    ax.axvline(R_CUT, color="#E03830", ls="--", lw=2, label="R = 1.0 (Low | High)")
    ax.set_xlabel("R = day TDD / user mean TDD (clipped at 3)")
    ax.set_ylabel("CE=0/BE≤1 user-days")
    ax.legend()
    ax.set_title("Within-user TDD ratio on CE=0/BE≤1 days", fontsize=TITLE_FS)
    fig.tight_layout()
    return fig


# The per-day-type palette now lives in utils.plotting (day_type_colors / DAY_TYPE_ORDER, imported
# above) so §8.1/§8.2/§8.3 share one definition: the 3 nested CE=0 arms are a dark→light ramp of each
# endpoint's glycemic-range band colour, CE>0 grey, CE>=3/BE>=3 bronze.


def figure_8_3e_tir_vs_tdd_pct(pdf, *, tercile_bands=False, day_types=None, delivery_strategy=None,
                               show_age_breakdown=False, x_axis="percentile",
                               show_density_panel=False):
    """Scatter: per-day TIR vs the day's TDD position, for TDD-reference-eligible users, coloured by the
    **same 5 day types as figure 8.3g** (3 nested CE=0 classifications + CE>0 + CE>=3/BE>=3 HMA). TIR-
    only figure → colours come from day_type_colors("tir"): the 3 nested CE=0 arms a dark→light ramp of
    the TIR band colour, CE>0 grey, HMA bronze.

    `x_axis` selects the x position (default reproduces production fig 8.3e / appendix 12.3h):
      "percentile"   — each day's rank of tdd_units within that user's eligible days (0–100); uniform
                       by construction (~10% of every user's days per decile), comparable across users.
      "absolute_tdd" — the raw tdd_units (U/day), fixed ABS_TDD_BIN_W-wide bins, x clipped at
                       ABS_TDD_XMAX (the right tail to ~431 U/day is <2% of days). This is the §8.3i NEW
                       analysis: it shows where on the real dose scale TIR moves, but POSITION CONFLATES
                       between-user insulin need (body size) with within-user variation — it is NOT the
                       within-user contrast (that is the percentile axis + Table 8.3d). Read per-cohort;
                       a factual caveat is stamped on the figure.

    All day types in scope get a decile-mean TIR LINE over that category's (cumulative, flag-defined)
    days, matching figure 8.3g; thin bins (< ABS_TDD_MIN_BIN_N days) drop their dot (a no-op on the
    uniform percentile axis; trims the sparse absolute-TDD tails). Plus a dashed black overall line. The
    scatter behind shows ALL days coloured by MOST-SPECIFIC day type (a clean per-day partition: CE=0
    BE=0 / BE=1 / BE≥2; CE>0 HMA → bronze, the rest grey); the two big categories (HMA/CE>0) are drawn
    fainter so they don't wash out the smaller CE=0 greens.

    Horizontal TIR reference gridlines every TIR_GRID_STEP % are drawn behind the data on every variant
    (P1) so line values read off the y-axis. `tercile_bands` shades the overall-reference tercile
    regions (percentile axis only — the rank terciles have no fixed U/day cut) — the Appendix §12.3h
    variant; the unshaded primary is Figure 8.3e.

    `show_density_panel` (default False) adds a thin shared-x companion panel of per-bin user-day counts
    stacked by day type — the population-density / n signal: on the percentile axis it is ~flat (shows
    the day-type MIX + the rank-axis uniformity); on the absolute axis it shows how the population
    concentrates and disperses into the tails. Default off, so the production figures change only by the
    gridlines.

    `day_types` (default None = all 5) restricts the dots + decile lines to a subset of DAY_TYPE_ORDER
    labels — e.g. ("CE=0/BE=0", "CE>=3/BE>=3") for the clean no-announcement vs high-announcement
    contrast. `delivery_strategy` (default None = all days) restricts to AB/TB days (a STRATEGY_COL
    value, e.g. "autobolus_on"); on the percentile axis it is applied AFTER the within-user rank, so the
    x-axis stays the OVERALL all-eligible-days percentile (only which days are *shown* changes); the
    absolute axis is order-free.

    `show_age_breakdown` (default False) appends, per arm, the adult vs pediatric split of the
    contributing users + user-days to each legend entry (e.g. "879 adults / 24,264 days · 190 peds /
    3,779 days"), widening the figure and moving the legend outside the axes so it fits. Only
    meaningful for cohort=all (where both age groups are present); off leaves the figure unchanged."""
    assert x_axis in ("percentile", "absolute_tdd"), x_axis
    df = pdf[pdf["n_eligible_days_for_tdd"] >= MIN_REF_DAYS].dropna(subset=["tdd_units", "tir"]).copy()
    # x position: within-user TDD percentile (default) OR absolute TDD. The rank is computed BEFORE the
    # delivery_strategy filter so the percentile axis stays the all-eligible-days percentile (only which
    # days are shown changes); the absolute axis is the raw value, so the filter order is immaterial.
    if x_axis == "percentile":
        df["_x"] = df.groupby("_userId")["tdd_units"].rank(pct=True) * 100.0
    else:
        df["_x"] = df["tdd_units"]
    if delivery_strategy is not None:
        df = df[df[STRATEGY_COL] == delivery_strategy].copy()
    # Per-day colour = most-specific day type (the tightest nested CE=0 class, else HMA, else CE>0);
    # first match wins, so dots get one crisp day_type_colors("tir") colour. Days in none of the 5 (CE>0
    # of non-CE=0-contributing users, zeroed by restrict_comparator) fall through to "" and are dropped.
    f0, f1, finf = (c[0] for c in CLASSIFICATIONS)  # in_ce0_be0 / in_ce0_be_le1 / in_ce0_be_inf
    df["dotcat"] = np.select(
        [df[f0] == True, df[f1] == True, df[finf] == True,          # noqa: E712
         df[HIGH_MA_FLAG] == True, df[COMPARATOR_FLAG] == True],     # noqa: E712
        ["CE=0/BE=0", "CE=0/BE<=1", "CE=0/BE<=inf", HIGH_MA_LABEL, COMPARATOR_LABEL], default="")
    df = df[df["dotcat"] != ""]
    order = list(DAY_TYPE_ORDER) if day_types is None else list(day_types)
    if day_types is not None:
        df = df[df["dotcat"].isin(order)].copy()

    def _age_note(sub):  # per-arm adult vs pediatric users + user-days (cohort=all has both)
        if not show_age_breakdown:
            return ""
        ad = sub[sub["is_pediatric"] == False]  # noqa: E712
        pe = sub[sub["is_pediatric"] == True]   # noqa: E712
        return (f"\n  {ad['_userId'].nunique():,} adults / {len(ad):,} days"
                f"\n  {pe['_userId'].nunique():,} peds / {len(pe):,} days")

    # Axis geometry: percentile lines land on round ticks 0..100 (bins centred on the marks); absolute
    # lines land on the fixed-width bin CENTRES (5,15,…) while ticks label the bin EDGES (0,10,…,120).
    if x_axis == "percentile":
        marks = np.arange(0, 101, 10)
        edges = np.arange(-5, 106, 10)  # 10-pct-wide bins centred on the marks → dots land on ticks
        xticks, xlim = marks, (0, 100)
        xlabel = "within-user TDD percentile (all eligible days, %)"
    else:
        marks, edges = ABS_TDD_MARKS, ABS_TDD_EDGES
        xticks, xlim = ABS_TDD_EDGES, (0, ABS_TDD_XMAX)
        xlabel = "total daily insulin (U/day)"
    # Thin-bin dot suppression applies ONLY to the absolute axis (its tails genuinely empty out); the
    # uniform percentile axis keeps every bin → fig 8.3e is unchanged except for the gridlines.
    min_bin_n = ABS_TDD_MIN_BIN_N if x_axis == "absolute_tdd" else 0

    base_w = 14 if show_age_breakdown else 9.5
    if show_density_panel:  # main axes + a short shared-x density panel below
        fig = plt.figure(figsize=(base_w, 7.4))
        gs = fig.add_gridspec(2, 1, height_ratios=[5, 1.15], hspace=0.07)
        ax = fig.add_subplot(gs[0])
        ax_den = fig.add_subplot(gs[1], sharex=ax)
        ax.tick_params(labelbottom=False)
    else:
        fig, ax = plt.subplots(figsize=(base_w, 6))
        ax_den = None

    if tercile_bands and x_axis == "percentile":  # overall-ref tercile regions (percentile axis only)
        b_lo, b_hi = (q * 100.0 for q in RANK_TERCILES)
        ax.axvspan(0, b_lo, color="#000000", alpha=0.04, zorder=0)
        ax.axvspan(b_hi, 100, color="#000000", alpha=0.08, zorder=0)
        for b in (b_lo, b_hi):
            ax.axvline(b, color="#555555", ls=":", lw=1.5, zorder=1)

    # Scatter for day-level spread — ALL days plotted, same colours as the lines (TIR band ramp).
    # The day types are very unequal in size (HMA + CE>0 are ~5-30× the CE=0 greens), so the two big
    # categories are pushed into a faint, small-dot BACKDROP while the CE=0 greens are drawn larger,
    # more opaque, and on top so they stay distinct against it (the LINES below use all days too).
    tir_colors = day_type_colors("tir")
    counts = {c: int((df["dotcat"] == c).sum()) for c in order}
    big = {COMPARATOR_LABEL, HIGH_MA_LABEL}  # the two large categories → faint backdrop
    for cat in sorted(order, key=lambda c: counts[c], reverse=True):
        sub = df[df["dotcat"] == cat]
        if cat in big:
            ax.scatter(sub["_x"], sub["tir"], s=4, color=tir_colors[cat], linewidths=0,
                       zorder=2, alpha=0.05)
        else:
            ax.scatter(sub["_x"], sub["tir"], s=7, color=tir_colors[cat], linewidths=0,
                       zorder=3, alpha=0.18)

    # A decile-mean TIR line per day type — over that category's (cumulative, flag-defined) days, so
    # the lines match figure 8.3g's 5 categories exactly. Thin bins drop their dot (a gap).
    handles = []
    for flag, label in SUPPLEMENT_ARMS:
        if label not in order:  # honour the day_types subset (e.g. the 2-type contrast)
            continue
        sub = df[df[flag] == True]  # noqa: E712
        g = sub.assign(_b=pd.cut(sub["_x"], edges, labels=marks)).groupby("_b", observed=False)["tir"]
        binned = g.mean().reindex(marks)
        binned = binned.where(g.count().reindex(marks).fillna(0) >= min_bin_n)
        h, = ax.plot(marks, binned.to_numpy(dtype=float), "-o", color=tir_colors[label], lw=2,
                     ms=5, zorder=5, label=f"{label} (n={len(sub):,}){_age_note(sub)}")
        handles.append(h)

    # Overall mean TIR per bin across all categories (dashed black, on top).
    g_all = df.assign(_b=pd.cut(df["_x"], edges, labels=marks)).groupby("_b", observed=False)["tir"]
    overall = g_all.mean().reindex(marks).where(g_all.count().reindex(marks).fillna(0) >= min_bin_n)
    h_all, = ax.plot(marks, overall.to_numpy(dtype=float), "--o", color="#111111", lw=2.5, ms=5,
                     zorder=6, label=f"overall (n={len(df):,}){_age_note(df)}")
    handles.append(h_all)

    # Density companion panel: per-bin user-day counts stacked by day type (the dotcat partition →
    # heights sum to total days/bin). Percentile axis ~flat (shows the mix + the uniform rank axis);
    # absolute axis shows how the population concentrates and disperses into the tails.
    if ax_den is not None:
        bar_w = (edges[1] - edges[0]) * 0.85
        bottoms = np.zeros(len(marks), dtype=float)
        for cat in order:
            sub = df[df["dotcat"] == cat]
            cnt = (sub.assign(_b=pd.cut(sub["_x"], edges, labels=marks))
                      .groupby("_b", observed=False).size().reindex(marks).fillna(0).to_numpy(float))
            ax_den.bar(np.asarray(marks, dtype=float), cnt, bottom=bottoms, width=bar_w,
                       color=tir_colors[cat], linewidth=0, align="center")
            bottoms += cnt
        ax_den.set_ylabel("user-days", fontsize=LEGEND_FS)
        ax_den.set_ylim(bottom=0)
        ax_den.margins(x=0)
        ax_den.tick_params(labelsize=LEGEND_FS - 1)
        ax_den.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _pos: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}"))

    ax.set_xlim(*xlim)
    ax.set_ylim(0, 100)
    # P1: horizontal TIR reference gridlines (every TIR_GRID_STEP %), behind the data; labels every 10.
    ax.set_axisbelow(True)
    ax.set_yticks(np.arange(0, 101, 10))
    ax.yaxis.set_minor_locator(mticker.MultipleLocator(TIR_GRID_STEP))
    ax.grid(axis="y", which="major", color="#7d7d7d", lw=0.9, alpha=0.85)
    ax.grid(axis="y", which="minor", color="#a8a8a8", lw=0.7, alpha=0.75)
    ax.set_ylabel("Time 70-180 mg/dL (%)")
    bottom_ax = ax_den if ax_den is not None else ax  # x-label/ticks live on the bottom-most axis
    bottom_ax.set_xticks(xticks)
    bottom_ax.set_xlabel(xlabel)

    if show_age_breakdown:  # longer entries → legend outside the axes (right) so it doesn't cover dots
        ax.legend(handles=handles, fontsize=LEGEND_FS, loc="center left", bbox_to_anchor=(1.01, 0.5))
    else:
        ax.legend(handles=handles, fontsize=LEGEND_FS)
    x_name = "within-user TDD percentile" if x_axis == "percentile" else "absolute TDD (U/day)"
    head = (f"TIR vs {x_name} by day type" if day_types is None
            else f"TIR vs {x_name}: " + " vs ".join(order))
    notes = []
    if delivery_strategy is not None:
        notes.append(f"{dict(STRATEGIES).get(delivery_strategy, delivery_strategy)} days only")
    if tercile_bands and x_axis == "percentile":
        notes.append("overall-ref tercile bands")
    if x_axis == "absolute_tdd":
        notes.append(f"x clipped at {ABS_TDD_XMAX} U/day; dots where bin n ≥ {ABS_TDD_MIN_BIN_N}")
    # 2-line title: metric/contrast on line 1, qualifiers + n on line 2 — keeps the longer variants from
    # overflowing the axes width. With no qualifiers (the plain fig 8.3e) it stays 1 line.
    n_note = f"(n={len(df):,})"
    title = f"{head}\n{'; '.join(notes)} — {n_note}" if notes else f"{head} {n_note}"
    ax.set_title(title, fontsize=TITLE_FS)

    # Body-size conflation caveat for the absolute axis lives in the report caption + decisions.md
    # (per MJC: not stamped on the figure) — absolute TDD conflates between-user insulin need with
    # within-user variation; the within-user contrast is fig 8.3e (percentile) + Table 8.3d.
    if show_density_panel or show_age_breakdown:
        fig.subplots_adjust(left=0.06 if show_age_breakdown else 0.09,
                            right=0.68 if show_age_breakdown else 0.97,
                            top=0.90, bottom=0.10, hspace=0.07)
    else:
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


# Stratum shading (lighter → darker) for the violin panels — shared by the binary (Low/High) and
# rank-tercile (Low/Mid/High) figures. Binary picks Low/High = 0.35/0.70 (unchanged from before).
STRATUM_ALPHA = {"Low": 0.35, "Mid": 0.55, "High": 0.70}


def _violin_panel(ax, endpoint, label, ce0_strata, cmp_strata, hma_strata, *, strata=("Low", "High"),
                  ce0_label="CE=0"):
    """One endpoint's violin+box+dots panel: 3 arms (CE=0 / CE>0 / CE>=3/BE>=3 HMA) × `strata`
    (Low/High for the binary figures, Low/Mid/High for the rank-tercile figures) → 6 or 9 groups.
    The CE=0 arm carries the endpoint's glycemic-range colour, the CE>0 comparator is grey, the high
    meal-announcement arm is bronze; stratum = lighter→darker (alpha). `ce0_label` names the CE=0 arm
    explicitly (e.g. "CE=0/BE<=1" for 8.3f). Drawing convention is shared via plotting.violin_box_panel."""
    base = endpoint_color(endpoint)
    arms = [(ce0_label, ce0_strata, base), ("CE>0", cmp_strata, GRAY),
            (HIGH_MA_LABEL, hma_strata, HIGH_MA_COLOR)]
    # 2-line tick labels (arm \n stratum) so the groups don't collide — violin_box_panel appends the
    # n-count as a 3rd line.
    groups = [
        (f"{arm}\n{st}",
         df.loc[df["tdd_stratum"] == st].groupby("_userId")[endpoint].mean().dropna().to_numpy(),
         color, STRATUM_ALPHA[st])
        for arm, df, color in arms for st in strata
    ]
    n = len(strata)  # separators sit between arms (positions are 1-indexed → n*k + 0.5)
    seps = tuple(n * k + 0.5 for k in range(1, len(arms)))
    violin_box_panel(ax, groups, title=label, title_color=base, separators=seps)


def figure_8_3a_violin(ce0_strata, cmp_strata, hma_strata, *,
                       fname_stem="figure_8_3a", ref_note="", strata=("Low", "High"), ce0_label="CE=0"):
    """Per-user means by TDD stratum (two semantic 2×2 grids), one grid per metric group. Returns
    {filename: figure}. Endpoint colour = glycemic range (Tidepool for the non-range metrics); CE=0
    coloured, CE>0 grey, CE>=3/BE>=3 bronze; stratum lighter→darker. `fname_stem` / `ref_note`
    parametrize the filename + title so the §12.3 median-/rolling-reference variants reuse this
    (§12.3a = median ref; §12.3c = rolling ref); `strata` switches between the binary Low/High figures
    and the rank-tercile Low/Mid/High figures (8.3f / 12.3g). `ce0_label` names the CE=0 arm explicitly
    (8.3f passes the BE≤1 arm as "CE=0/BE<=1")."""
    ref = f" ({ref_note})" if ref_note else ""
    # 9-group tercile panels are wide → stack the 4 endpoints 4×1 (each panel full-width); the
    # 6-group binary panels stay in the 2×2 grid.
    tercile = len(strata) >= 3
    nrows, ncols, figsize, top = (4, 1, (15, 20), 0.96) if tercile else (2, 2, (13, 8.6), 0.94)
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(nrows, ncols, figsize=figsize)
        for ax, (col, label) in zip(axes.ravel(), eps):
            _violin_panel(ax, col, label, ce0_strata, cmp_strata, hma_strata, strata=strata,
                          ce0_label=ce0_label)
        fig.suptitle(f"{gtitle}\nper-user means by TDD stratum ({ce0_label}, CE>0, CE>=3/BE>=3){ref}",
                     fontsize=SUPTITLE_FS)
        fig.tight_layout(rect=[0, 0, 1, top])
        out[f"{fname_stem}_{key}.png"] = fig
    return out


def figure_8_3a_5way_violin(strata, cmp_strata, hma_strata, *, fname_stem="figure_8_3a", ref_note=""):
    """Figure 8.3a (two 4×1 grids → all 8 endpoints): per-user mean endpoints by TDD stratum for ALL
    **5 day types** (the 3 nested CE=0 arms + CE>0 + CE>=3/BE>=3 HMA), each split Low/High → 10
    violins per panel. The 10-cell panels are wide, so the 4 endpoints of each grid stack 4×1
    (full-width, à la §8.2a). Colour = day type per panel (day_type_colors(col) — the 3 nested CE=0 arms
    a dark→light ramp of that endpoint's glycemic-range band colour, CE>0 grey, HMA bronze), alpha =
    stratum (Low lighter / High darker); the panel title carries the endpoint colour. Mean-reference
    (R = tdd/mean) binary strata. A separator divides each day type's Low|High pair. Returns
    {filename: figure}."""
    sections = ([(lab, strata[lab]) for _flag, lab in CLASSIFICATIONS]
                + [(COMPARATOR_LABEL, cmp_strata), (HIGH_MA_LABEL, hma_strata)])
    order = ("Low", "High")
    ref = f" ({ref_note})" if ref_note else ""
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(4, 1, figsize=(16, 20))
        for ax, (col, label) in zip(axes.ravel(), eps):
            groups = [
                (f"{lab}\n{st}",
                 df.loc[df["tdd_stratum"] == st].groupby("_userId")[col].mean().dropna().to_numpy(),
                 day_type_colors(col)[lab], STRATUM_ALPHA[st])
                for lab, df in sections for st in order
            ]
            seps = tuple(len(order) * k + 0.5 for k in range(1, len(sections)))
            violin_box_panel(ax, groups, title=label, title_color=endpoint_color(col), separators=seps)
        fig.suptitle(f"{gtitle}\nper-user means by TDD stratum — 5 day types × Low/High{ref}",
                     fontsize=SUPTITLE_FS)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        out[f"{fname_stem}_{key}.png"] = fig
    return out


def _tercile_bars_panel(ax, endpoint, label, sections, order):
    """One endpoint panel: x = the terciles in `order`; within each, the 5 day types as staggered
    vertical 95% CI bars (marker = across-user mean of the per-user tercile mean, whisker = ±1.96
    SEM), each day type's Low→Mid→High markers joined by a thin connecting line to show its
    trajectory. y auto-scales to the estimates so the (narrow) CIs stay visible. Colour = day type for
    this endpoint's band ramp (day_type_colors(endpoint)); a per-panel arm legend matches the ramp."""
    x = np.arange(len(order))
    labels = DAY_TYPE_ORDER
    colors = day_type_colors(endpoint)
    offs = np.linspace(-0.30, 0.30, len(labels))
    for lbl, off in zip(labels, offs):
        mu, lo, hi = _tercile_trend_stats(sections[lbl], endpoint, order)
        # fmt="-o": connect each day type's Low→Mid→High points (thin line) + marker + CI whisker.
        ax.errorbar(x + off, mu, yerr=np.vstack([mu - lo, hi - mu]), fmt="-o", ms=5, lw=1.6,
                    color=colors[lbl], ecolor=colors[lbl],
                    elinewidth=2.2, capsize=3, zorder=3)
    ax.set_xticks(x); ax.set_xticklabels(order)
    ax.set_xlabel("within-user TDD tercile")
    ax.set_title(label, color=endpoint_color(endpoint), fontsize=TITLE_FS)
    ax.margins(x=0.12)
    day_type_legend(ax, endpoint, DAY_TYPE_ORDER)  # per-panel: hue varies by endpoint


def figure_8_3g_rank_tercile_bars(pdf, *, reference="overall",
                                  fname_stem="figure_8_3g", ref_note="overall TDD-rank ref"):
    """Figure 8.3g (single 4×2 grid → all 8 endpoints): the **5 day types** (3 nested CE=0
    classifications + CE>0 + CE>=3/BE>=3 HMA) across within-user TDD rank terciles (Low/Mid/High),
    same-user-set gated. Each day type at each tercile is a staggered vertical 95% CI bar (marker =
    across-user mean of the per-user tercile mean, whisker = ±1.96 SEM). `reference` switches the
    ranking universe — `overall` (rank over the user's all eligible days, à la 8.3e) is the primary
    Figure 8.3g; `ce0` (rank within the arm's own days) is the Appendix §12.3 companion. Returns
    {filename: figure}."""
    order = ("Low", "Mid", "High")
    sections = _rank_sections(pdf, reference=reference, split="tercile")  # 5 SUPPLEMENT_ARMS, gated
    ref = f" ({ref_note})" if ref_note else ""

    def panel(ax, col, label):
        # Per-panel arm legend (the day-type ramp varies by endpoint) is added inside _tercile_bars_panel.
        _tercile_bars_panel(ax, col, label, sections, order)

    return render_4x2_grid(panel, fig_stem=fname_stem,
                           subtitle=f"day types by within-user TDD rank tercile{ref}; "
                                    f"whiskers = 95% CI, same-user-set gated",
                           figsize=(13, 16))


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort="all",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    """Run §8.3 for one age cohort → outputs/analysis_8_3/<cohort>/.

    figures_only=True skips the SLOW table writes (day-level LMM fits + cluster-bootstrap CIs) and
    re-renders only the figures, in place (existing table CSVs are left untouched, not cleared) — for
    fast figure-tweak iteration. Do a full run first so the tables exist; then iterate with
    --figures-only.

    figs_filter (a substring, e.g. "8_3g") renders ONLY the figure builders whose tag contains it,
    skipping the rest — so iterating on one figure avoids re-rendering the others (notably the
    8.3e/12.3h scatters). Implies figures_only (you're tweaking a figure)."""
    figures_only = figures_only or figs_filter is not None  # filtering figures ⇒ skip the tables
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_3", cohort)
    if os.path.isdir(output_dir) and not figures_only:
        shutil.rmtree(output_dir)  # full run starts clean; figures_only overwrites PNGs in place
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

    # ---- Strata frames (FAST: groupby/where; no LMM or bootstrap). Computed up front because BOTH
    # the tables and the figures consume them; under figures_only the slow table writes below are
    # skipped while these (and the figures) still run. ----
    # Primary strata (mean-TDD reference via tdd_ratio) per nested classification, + the CE>0
    # comparator and the high meal-announcement (CE>=3/BE>=3, ⊂ CE>0) arm, each stratified Low/High
    # with the same _ce0_strata machinery.
    strata = {cls_label: _ce0_strata(pdf, flag) for flag, cls_label in CLASSIFICATIONS}
    cmp_strata = _ce0_strata(pdf, COMPARATOR_FLAG)
    hma_strata = _ce0_strata(pdf, HIGH_MA_FLAG)
    strata_8_3a = {**strata, COMPARATOR_LABEL: cmp_strata, HIGH_MA_LABEL: hma_strata}
    # §12.3a/b median-reference frames (R = tdd / median_tdd_user).
    pdf_med = pdf.copy()
    pdf_med["tdd_ratio_median"] = pdf_med["tdd_units"] / pdf_med["median_tdd_user"]
    med = {cls_label: _ce0_strata(pdf_med, flag, ratio_col="tdd_ratio_median")
           for flag, cls_label in CLASSIFICATIONS}
    med_8_3a = {**med,
                COMPARATOR_LABEL: _ce0_strata(pdf_med, COMPARATOR_FLAG, ratio_col="tdd_ratio_median"),
                HIGH_MA_LABEL: _ce0_strata(pdf_med, HIGH_MA_FLAG, ratio_col="tdd_ratio_median")}
    # §12.3c/d rolling-30-day-reference frames (computed in-analysis from per-day tdd_units + local_day).
    pdf_roll = _add_rolling_ref(pdf)
    roll = {cls_label: _ce0_strata(pdf_roll, flag, ratio_col="tdd_ratio_rolling")
            for flag, cls_label in CLASSIFICATIONS}
    roll_8_3a = {**roll,
                 COMPARATOR_LABEL: _ce0_strata(pdf_roll, COMPARATOR_FLAG, ratio_col="tdd_ratio_rolling"),
                 HIGH_MA_LABEL: _ce0_strata(pdf_roll, HIGH_MA_FLAG, ratio_col="tdd_ratio_rolling")}

    # ---- Tables (SLOW: day-level LMM fits + cluster-bootstrap CIs). Skipped under figures_only so a
    # figure tweak re-renders in seconds without recomputing the statistics. The fast across-user
    # mean tables (8.3a/d, 12.3a/c/g/h) are bundled here too for one clean guard. ----
    if not figures_only:
        # Primary: Table 8.3a (mean-ref binary, 5 sections) + 8.3b/8.3c (within-user + LMM) + Table
        # 8.3d (OVERALL-reference rank TERCILES — the D12 apples-to-apples replacement, 5 sections).
        table_8_3a_per_user_by_stratum(strata_8_3a).to_csv(
            os.path.join(output_dir, "table_8_3a_per_user_by_stratum.csv"), index=False)
        table_8_3b_within_user(strata, nma_stats).to_csv(
            os.path.join(output_dir, "table_8_3b_within_user_contrast.csv"), index=False)
        table_8_3c_lmm(strata, nma_stats).to_csv(
            os.path.join(output_dir, "table_8_3c_lmm_sensitivity.csv"), index=False)
        table_rank_across_user(pdf, "overall", "tercile").to_csv(
            os.path.join(output_dir, "table_8_3d_rank_tercile_strata.csv"), index=False)

        # Appendix §12.3 — sensitivities (median + rolling alt TDD references: per-user table +
        # within-user contrast each), the HMA arm (within-user + LMM), and the rest of the rank
        # rework (CE=0-ref tercile, both-ref binary, within-user bottom−top). ⚠️ the magnitude-based
        # tables inherit the §8.3 D12 not-citable caveat; the rank tables resolve it.
        table_8_3a_per_user_by_stratum(med_8_3a).to_csv(
            os.path.join(output_dir, "table_12_3a_median_per_user_by_stratum.csv"), index=False)
        table_8_3b_within_user(med, nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3b_median_within_user.csv"), index=False)
        table_8_3a_per_user_by_stratum(roll_8_3a).to_csv(
            os.path.join(output_dir, "table_12_3c_rolling_per_user_by_stratum.csv"), index=False)
        table_8_3b_within_user(roll, nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3d_rolling_within_user.csv"), index=False)
        table_8_3b_within_user({HIGH_MA_LABEL: hma_strata}, nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3e_high_engagement_within_user.csv"), index=False)
        table_8_3c_lmm({HIGH_MA_LABEL: hma_strata}, nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3f_high_engagement_lmm.csv"), index=False)
        table_rank_across_user(pdf, "ce0", "tercile").to_csv(
            os.path.join(output_dir, "table_12_3g_ce0_rank_tercile_strata.csv"), index=False)
        pd.concat([table_rank_across_user(pdf, "overall", "binary"),
                   table_rank_across_user(pdf, "ce0", "binary")], ignore_index=True).to_csv(
            os.path.join(output_dir, "table_12_3h_rank_binary_strata.csv"), index=False)
        table_rank_within_user(pdf, "overall", nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3i_rank_within_user_overall.csv"), index=False)
        table_rank_within_user(pdf, "ce0", nma_stats).to_csv(
            os.path.join(output_dir, "table_12_3j_rank_within_user_ce0.csv"), index=False)

    # ---- Figures. Each builder is a thunk → {filename: figure}, keyed by a short tag. `figs_filter`
    # (a substring) renders only the matching builders — so iterating on one figure skips the others
    # (notably the dense fig 8.3e/12.3h scatters). 8.3a/12.3a/12.3c show all 5 day types; the
    # single-CE=0-arm figures (8.3b/8.3d/8.3f, 12.3g) feature the headline CE=0/BE≤1 arm (D20);
    # CE>0 + HMA also split Low/High (computed above). ----
    def _tercile_arms(reference, ce0_flag=CLASSIFICATIONS[-1][0]):
        a = lambda flag: _rank_strata(pdf, flag, reference=reference, split="tercile")  # noqa: E731
        return a(ce0_flag), a(COMPARATOR_FLAG), a(HIGH_MA_FLAG)

    fig_builders = {
        "8_3a": lambda: figure_8_3a_5way_violin(strata, cmp_strata, hma_strata,        # 5-day-type violins (4×1)
                                                ref_note="mean-ref"),
        "12_3a": lambda: figure_8_3a_5way_violin(med, med_8_3a[COMPARATOR_LABEL],       # 5-day-type, median ref
                                                 med_8_3a[HIGH_MA_LABEL], ref_note="median TDD ref",
                                                 fname_stem="figure_12_3a_median"),
        "12_3c": lambda: figure_8_3a_5way_violin(roll, roll_8_3a[COMPARATOR_LABEL],     # 5-day-type, rolling ref
                                                 roll_8_3a[HIGH_MA_LABEL], ref_note="rolling-30-day TDD ref",
                                                 fname_stem="figure_12_3c_rolling"),
        "8_3b": lambda: figure_8_3b_paired_delta(strata, cmp_strata, hma_strata),
        "8_3c": lambda: {"figure_8_3c_stacked_ranges.png": figure_8_3c_stacked(strata_8_3a)},
        "8_3d": lambda: {"figure_8_3d_R_distribution.png": figure_8_3d_r_dist(strata["CE=0/BE<=1"])},
        "8_3e": lambda: {"figure_8_3e_tir_vs_tdd_percentile.png": figure_8_3e_tir_vs_tdd_pct(pdf)},
        "8_3i": lambda: {"figure_8_3i_tir_vs_tdd_absolute.png":               # NEW: absolute-TDD x-axis (P2)
                         figure_8_3e_tir_vs_tdd_pct(pdf, x_axis="absolute_tdd",
                                                    show_density_panel=True)},
        "8_3j": lambda: {"figure_8_3j_tir_vs_tdd_percentile_density.png":      # percentile chart + density panel (P3)
                         figure_8_3e_tir_vs_tdd_pct(pdf, show_density_panel=True)},
        # AB-only variants of fig 8.3e — the source PNGs for the report's §8.4 TDD-percentile scatters
        # (autobolus days only): all day types (report Fig 8.4b) and the CE=0/BE=0 vs CE>=3/BE>=3 contrast
        # (report Fig 8.4c, cited in the §9 discussion). The percentile rank is the all-eligible-days rank
        # (the AB filter is applied AFTER ranking — see figure_8_3e_tir_vs_tdd_pct).
        "8_3e_ab": lambda: {"figure_8_3e_ab_by_day_type.png":
                            figure_8_3e_tir_vs_tdd_pct(pdf, delivery_strategy="autobolus_on")},
        "8_3e_ab_ce0hma": lambda: {"figure_8_3e_ab_ce0be0_vs_hma.png":
                                   figure_8_3e_tir_vs_tdd_pct(
                                       pdf, day_types=("CE=0/BE=0", "CE>=3/BE>=3"),
                                       delivery_strategy="autobolus_on")},
        "8_3f": lambda: figure_8_3a_violin(*_tercile_arms("overall", ce0_flag=CLASSIFICATIONS[1][0]),  # overall-ref tercile violins (CE=0/BE<=1)
                                           ref_note="overall TDD-rank terciles", fname_stem="figure_8_3f",
                                           strata=("Low", "Mid", "High"), ce0_label="CE=0/BE<=1"),
        "8_3g": lambda: figure_8_3g_rank_tercile_bars(pdf, reference="overall"),           # 5-day-type CI bars
        "12_3g": lambda: figure_8_3a_violin(*_tercile_arms("ce0", ce0_flag=CLASSIFICATIONS[1][0]),   # CE=0-ref tercile violins (CE=0/BE<=1)
                                            ref_note="CE=0 TDD-rank terciles", fname_stem="figure_12_3g_ce0",
                                            strata=("Low", "Mid", "High"), ce0_label="CE=0/BE<=1"),
        "12_3i": lambda: figure_8_3g_rank_tercile_bars(pdf, reference="ce0",
                                                       fname_stem="figure_12_3i_ce0_bars",
                                                       ref_note="CE=0 TDD-rank ref"),
        "12_3h": lambda: {"figure_12_3h_overall_tercile_scatter.png":
                          figure_8_3e_tir_vs_tdd_pct(pdf, tercile_bands=True)},
    }
    n = 0
    for key, build in fig_builders.items():
        if figs_filter and figs_filter not in key:
            continue
        for fname, fig in build().items():
            fig.savefig(os.path.join(output_dir, fname), dpi=150)
            plt.close(fig)
            n += 1

    bits = []
    if figures_only:
        bits.append("figures_only — tables skipped")
    if figs_filter:
        bits.append(f"figs~'{figs_filter}'")
    tag = f" ({'; '.join(bits)})" if bits else ""
    print(f"wrote analysis 8.3 ({cohort}) — {n} figure(s){tag} to {output_dir}")


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path, figures_only=figures_only, figs_filter=figs_filter)


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--analysis_ready_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--csv_path", default=None)
    _parser.add_argument("--output_dir", default=None)
    _parser.add_argument("--cohort", default=None, choices=["adult", "pediatric", "all"])
    _parser.add_argument("--min_age", type=int, default=MIN_AGE)
    _parser.add_argument("--figures-only", dest="figures_only", action="store_true",
                         help="re-render figures only, skipping the slow LMM/bootstrap table writes "
                              "(do a full run first so the tables exist)")
    _parser.add_argument("--figs", dest="figs_filter", default=None,
                         help="render only figure builders whose tag contains this substring "
                              "(e.g. 8_3g); implies --figures-only. Tags: 8_3a/b/c/d/e/f/g/i/j, 12_3a/c/g/h/i")
    _args, _ = _parser.parse_known_args()

    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        _spark = None

    if _args.cohort is None:
        main(_spark, _args.analysis_ready_table, min_age=_args.min_age, csv_path=_args.csv_path,
             figures_only=_args.figures_only, figs_filter=_args.figs_filter)
    else:
        run(_spark, _args.analysis_ready_table, _args.output_dir,
            cohort=_args.cohort, min_age=_args.min_age, csv_path=_args.csv_path,
            figures_only=_args.figures_only, figs_filter=_args.figs_filter)
