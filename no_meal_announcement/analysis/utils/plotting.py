"""Shared figure conventions for the NMA analyses (§8.1–§8.3).

One place for the look so 8.1 / 8.2 / 8.3 render identically:
- range-based endpoint colours (AGP/Tidepool bands; the 3 non-range metrics get the brand blue),
- the violin+box+dots panel (subtle dots behind, crisp box + orange median on top),
- the shared-bin overlaid-histogram panel (bars align; solid mean line + dashed zero),
- the two semantic 2×2 metric grids (target+safety vs hyperglycemia+overall) covering all 8 endpoints.

Treatment data (NMA / CE=0) carries the endpoint colour; the CE>0 comparator is rendered grey.
"""
from __future__ import annotations

import numpy as np
import matplotlib as mpl

# House style: larger fonts across every NMA figure. All four analyses import this module (after
# matplotlib.use("Agg")), so this rcParams bump applies everywhere; the explicit sizes below feed
# the panel helpers / are re-exported for suptitles so hand-set sizes scale too.
mpl.rcParams.update({
    "font.size": 13,
    "axes.titlesize": 15,
    "axes.labelsize": 14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 11,
    "figure.titlesize": 16,
})
SUPTITLE_FS = 15   # figure suptitles
TITLE_FS = 14      # panel titles
LABEL_FS = 13      # axis labels
TICK_FS = 11       # x-group tick labels (4 multi-line groups → kept a touch smaller)
LEGEND_FS = 10.5

# Deterministic jitter (same seed the analyses use for bootstrap).
BOOTSTRAP_SEED = 20260520

# Tidepool brand + neutral comparator grey.
TIDEPOOL = "#607cff"
GRAY = "#9e9e9e"

# High meal-announcement ("high engagement", CE>=3/BE>=3) 5th-category styling: a distinct
# bronze/gold (the heavy-announcement end), separate from the range colours and the grey CE>0
# comparator. Shared across §8.1–§8.3 so the HMA arm reads identically everywhere.
HIGH_MA_COLOR = "#9c6b30"
HIGH_MA_ALPHA = 0.70

# Standard AGP/Tidepool glycemic-range band colours (stacked bars).
RANGE_COLORS = {"<54": "#E03830", "54-70": "#FF6D5C", "70-180": "#5AC692",
                "180-250": "#AA85DE", ">250": "#7046CC"}
# Disjoint ranges (cumulative-endpoint derived) for stacked bars: (column, label).
RANGE_COLS = [("r_lt54", "<54"), ("r_54_70", "54-70"), ("r_70_180", "70-180"),
              ("r_180_250", "180-250"), ("r_gt250", ">250")]

# Each endpoint coloured by its glycemic range; the 3 non-range metrics get the Tidepool brand.
ENDPOINT_COLORS = {
    "tir": "#5AC692",            # 70-180 (in range)
    "tbr": "#FF6D5C",            # <70 (54-70 band)
    "tbr_very_low": "#E03830",   # <54
    "tar": "#AA85DE",            # >180 (180-250 band)
    "tar_very_high": "#7046CC",  # >250
    "mean_glucose": TIDEPOOL,
    "cv": TIDEPOOL,
    "hypo_events": TIDEPOOL,
}

# Two semantically-grouped 2×2 grids spanning all 8 endpoints: (key, grid title, [(col, label)]).
GRIDS = [
    ("grid1_target_safety", "Time in range & hypoglycemia",
     [("tir", "Time 70-180 (%)"), ("tbr", "Time <70 (%)"),
      ("tbr_very_low", "Time <54 (%)"), ("hypo_events", "Hypo events / day")]),
    ("grid2_hyper_overall", "Hyperglycemia & overall glycemia",
     [("tar", "Time >180 (%)"), ("tar_very_high", "Time >250 (%)"),
      ("mean_glucose", "Mean glucose (mg/dL)"), ("cv", "CV (%)")]),
]
# Per-endpoint short label (panel titles), pulled from GRIDS.
ENDPOINT_LABELS = {col: lab for _, _, eps in GRIDS for col, lab in eps}


def endpoint_color(endpoint):
    """The glycemic-range colour for `endpoint` (Tidepool brand for the non-range metrics)."""
    return ENDPOINT_COLORS.get(endpoint, TIDEPOOL)


# Fixed per-day-type palette — supersedes D13's per-endpoint arm colouring for the day-type figures
# so the day types read identically across the document: the 3 nested NMA/CE=0 classifications on a
# green ramp (strictest BE=0 darkest → broadest BE≤∞ lightest; the mid step = the TIR/70-180 green),
# CE>0 grey, CE>=3/BE>=3 (HMA) bronze. Shared by §8.1 (8.1b/12.1b violins), §8.2 (8.2a violins, 8.2c
# interaction lines) and §8.3 (8.3e/8.3g). Keys = the data_loader arm labels (COMPARATOR_LABEL /
# HIGH_MA_LABEL / the CLASSIFICATIONS labels).
DAY_TYPE_COLORS = {
    "CE=0/BE=0": "#00441b",                # very dark green (strictest CE=0)
    "CE=0/BE<=1": ENDPOINT_COLORS["tir"],  # = the TIR / 70-180 green
    "CE=0/BE<=inf": "#b7e075",             # light yellow-green (broadest CE=0)
    "CE>0": GRAY,                          # CE>0 comparator
    "CE>=3/BE>=3": HIGH_MA_COLOR,          # CE>=3/BE>=3 high meal-announcement (bronze)
}

# Delivery-strategy fill alpha for the §8.4 strategy figures: colour stays the day type's
# DAY_TYPE_COLORS, alpha cues the strategy — autobolus_on darker, temp_basal_only lighter (TB-first
# display via data_loader.STRATEGIES). Keyed by the delivery_strategy column value. Matches §8.2's
# local encoding so AB/TB read identically across the document. (§8.2 keeps its own local copy.)
STRATEGY_ALPHA = {"autobolus_on": 0.78, "temp_basal_only": 0.40}


def violin_box_panel(ax, groups, *, title=None, title_color=None, separators=(), label_fs=TICK_FS):
    """Draw the standard violin + box + dots panel into `ax`.

    groups: list of (xtick_label, values(1-D array), facecolor, alpha) — one x position each
    (1..N). Dots recede behind (small, translucent, dark-neutral); the box is drawn on top with
    crisp black lines and a bright orange median so it reads over dense cohorts.
    separators: x positions (e.g. 2.5) for a light vertical divider (e.g. treatment | comparator).
    """
    positions = list(range(1, len(groups) + 1))
    violin = [(p, g) for p, g in zip(positions, groups) if len(g[1]) > 1]
    if violin:
        vp = ax.violinplot([g[1] for _, g in violin], positions=[p for p, _ in violin],
                           showextrema=False, widths=0.8)
        for body, (_, g) in zip(vp["bodies"], violin):
            body.set_facecolor(g[2])
            body.set_alpha(g[3])
    # Subtle dots behind, so dense cohorts don't form a black mass.
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    for p, g in zip(positions, groups):
        vals = g[1]
        if len(vals):
            ax.scatter(rng.normal(p, 0.06, size=len(vals)), vals, s=4, alpha=0.22,
                       color="#3a3a3a", linewidths=0, zorder=2)
    # Box on top with crisp black lines + a bright median so it reads over the dots.
    box = [(p, g[1]) for p, g in zip(positions, groups) if len(g[1]) > 0]
    if box:
        bp = ax.boxplot([v for _, v in box], positions=[p for p, _ in box], widths=0.28,
                        showfliers=False,
                        boxprops=dict(color="#111111", linewidth=1.4),
                        whiskerprops=dict(color="#111111", linewidth=1.2),
                        capprops=dict(color="#111111", linewidth=1.2),
                        medianprops=dict(color="#ff7f0e", linewidth=2.2))
        for el in [*bp["boxes"], *bp["medians"], *bp["whiskers"], *bp["caps"]]:
            el.set_zorder(5)
    for sx in separators:
        ax.axvline(sx, color="#dddddd", lw=0.8)
    ax.set_xticks(positions)
    ax.set_xticklabels([f"{g[0]}\n(n={len(g[1])})" for g in groups], fontsize=label_fs)
    if title is not None:
        ax.set_title(title, fontsize=TITLE_FS, color=title_color or "#000000")


def overlay_hist_panel(ax, series, *, xlabel="", title=None, title_color=None, bins=24,
                       zero_line=True):
    """Overlaid histograms that share one set of bin edges (so the bars align).

    series: list of (values(1-D array), legend_label, colour). A solid vertical line marks each
    series' mean; a dashed line marks 0. Edges are derived from the pooled values across series.
    """
    series = [(np.asarray(v, dtype=float), lab, c) for v, lab, c in series]
    nonempty = [v for v, _, _ in series if len(v)]
    pooled = np.concatenate(nonempty) if nonempty else np.array([0.0, 1.0])
    edges = np.histogram_bin_edges(pooled, bins=bins)
    for vals, lab, color in series:
        if len(vals):
            ax.hist(vals, bins=edges, color=color, alpha=0.55, edgecolor="white",
                    label=f"{lab} (n={len(vals)}, μ={vals.mean():.1f})")
            ax.axvline(float(vals.mean()), color=color, ls="-", lw=1.8)
    if zero_line:
        ax.axvline(0, color="#333333", ls="--", lw=1)
    ax.set_xlabel(xlabel, fontsize=LABEL_FS)
    ax.legend(fontsize=LEGEND_FS, loc="upper right")
    if title is not None:
        ax.set_title(title, fontsize=TITLE_FS, color=title_color or "#000000")
