"""Analysis 8.2: NMA-day glycemic outcomes by delivery strategy (PLN-1008 §8.2).

For each of the three nested NMA classifications, fit a day-level interaction LMM:

    outcome ~ day_type * delivery_strategy + (1 | user)

where day_type ∈ {NMA (the classification's CE=0 days), CE>0 (the comparator)} and
delivery_strategy ∈ {autobolus_on, temp_basal_only}. The day_type × delivery_strategy
interaction is the pre-specified test: does the NMA-vs-comparator contrast differ between
autobolus and temp-basal-only days? Days with an ambiguous delivery strategy are excluded
(§7.3); the CE>0 comparator is restricted to users with >=1 CE=0 day, as in §8.1.

Reference levels (alphabetical, patsy treatment coding): day_type=CE>0, strategy=autobolus_on,
so the main day_type term is NMA−CE>0, the main strategy term is temp_basal_only−autobolus_on,
and the interaction is how much the NMA−CE>0 contrast shifts on temp_basal_only vs
autobolus_on days. Primary outcome: TIR (70-180 mg/dL); the other endpoints are reported
alongside.

Mirrors §8.1's per-cohort run()/main() + output-clearing; shares the analysis-ready loader,
§7.6 cohort filter, CE>0 comparator restriction, and endpoint/classification constants via
utils/data_loader, and the interaction LMM via utils/statistics.lmm_day_strategy_interaction.

Note: autobolus_on days are sparse (~2% of user-days), so within the stringent classifications
some NMA × autobolus_on cells are too small to fit; those (classification, endpoint) cells are
guarded and emitted as converged=False NaN rows rather than aborting the table.

High meal-announcement (CE>=3/BE>=3) supplement — mirrors §8.1's high-engagement treatment:
the descriptive figures (8.2a/8.2c/8.2d) show HMA as a 3rd, overlapping day type (HMA ⊂ CE>0,
bronze) beside NMA and CE>0 (4 → 6 cells per strategy pair); and a parallel Appendix §12.2
interaction contrast (day_type ∈ {CE>=3/BE>=3, CE>0} × strategy) is emitted alongside.

Outputs (analysis/outputs/analysis_8_2/<cohort>/):
    table_8_2a_marginal_cells.csv   (per classification × endpoint × day_type × strategy:
                                     observed per-user-mean summary + model-estimated mean; the 3
                                     nested NMA classifications (NMA + CE>0 cells) plus a 5th
                                     descriptive CE>=3/BE>=3 (HMA) section — HMA cells only, from
                                     the §12.2 frame/fit, its CE>0 omitted as already present)
    table_8_2b_interaction.csv      (per classification × endpoint: main day-type, main
                                     strategy, and interaction coef/CI/p + n_users/n_days/converged)
    table_12_2a_high_engagement_interaction.csv  (Appendix §12.2: CE>=3/BE>=3 vs CE>0 × strategy
                                     interaction — same columns as 8.2b; overlapping reference)
    figure_8_2a_violin_grid{1,2}_*.png    (per-user means by strategy × day type {NMA, CE>0, HMA},
                                           broadest arm, all 8 endpoints, two 2×2 grids)
    figure_8_2c_interaction_grid{1,2}_*.png  (model marginal-mean interaction lines {NMA, CE>0, HMA},
                                           broadest arm, all 8)
    figure_8_2d_stacked_bars.png    (mean time in glycemic ranges per cell, all 3 classifications × 6 cells)
"""

# %pip install statsmodels
# dbutils.library.restartPython()

import argparse
import importlib.util
import os
import shutil
import sys
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Make the sibling `utils` package importable regardless of launch method (the script dir is
# auto-added for `python this.py`, but a Databricks notebook needs the hint).
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
    GRIDS,
    LEGEND_FS,
    RANGE_COLORS,
    RANGE_COLS,
    STACKED_BAR_PCT_DECIMALS,
    SUPTITLE_FS,
    TICK_FS,
    TITLE_FS,
    day_type_colors,
    day_type_legend,
    endpoint_color,
    render_4x2_grid,
    violin_box_panel,
)

# §7.3 delivery strategies (STRATEGIES / STRATEGY_COL) now live in utils.data_loader (shared with
# §8.3); imported above. Any other / null strategy is "ambiguous" and excluded.
DAY_TYPE_COL = "day_type"
NMA_LABEL = "NMA"
PRIMARY_ENDPOINT = "tir"

BOOTSTRAP_SEED = 20260520

# Colours: the violins (8.2a) + interaction lines (8.2c) colour each day type by the PANEL ENDPOINT's
# glycemic-range band (day_type_colors(col)) — the 3 nested NMA/CE=0 arms a dark→light ramp of that
# band, CE>0 grey, CE>=3/BE>=3 bronze — so the hue reinforces which metric a panel shows (the arm hue
# now varies per endpoint). The stacked bars (8.2d) keep RANGE_COLORS (by glycemic range). All shared
# via utils.plotting.
VIOLIN_ALPHA = 0.72   # day-type fill alpha (the distinct day-type colours separate the day types)
# In 8.2a the AB/TB pair of a day type sits adjacent (same day-type colour), so the strategy is cued
# by alpha — autobolus_on darker, temp_basal_only lighter.
STRATEGY_ALPHA = {"autobolus_on": 0.78, "temp_basal_only": 0.40}


def build_day_type_frame(pdf, treatment_flag, treatment_label=NMA_LABEL):
    """Long 2-day-type frame for one interaction fit: treatment days (`treatment_flag`) labeled
    `treatment_label`, comparator days (in_ce_gt0) labeled 'CE>0', keeping only the two known
    delivery strategies (ambiguous excluded). The main pass uses the classification's CE=0 flag
    (treatment_label='NMA', disjoint from CE>0); the §12.2 supplement passes HIGH_MA_FLAG /
    HIGH_MA_LABEL (CE>=3/BE>=3 — note this is ⊂ CE>0, an overlapping reference, as in §8.1)."""
    strat_names = [s for s, _ in STRATEGIES]
    treat = pdf.loc[pdf[treatment_flag] == True].copy()  # noqa: E712
    treat[DAY_TYPE_COL] = treatment_label
    cmp = pdf.loc[pdf[COMPARATOR_FLAG] == True].copy()  # noqa: E712
    cmp[DAY_TYPE_COL] = COMPARATOR_LABEL
    frame = pd.concat([treat, cmp], ignore_index=True)
    return frame[frame[STRATEGY_COL].isin(strat_names)].copy()


# Display day types for the descriptive 6-cell figures (8.2a/8.2c): the classification's NMA days,
# the CE>0 comparator, and the high meal-announcement arm (CE>=3/BE>=3). HMA is an overlapping subset
# of CE>0 (HMA ⊂ CE>0) shown as its own day type — descriptive, not a disjoint partition (mirrors
# §8.1's 5th arm). DISPLAY_CELLS (3 day types) drives the per-classification 8.2d stacked bars
# (NMA = the subplot's classification); 8.2a uses DISPLAY_CELLS_5WAY below — the 3 nested NMA/CE=0
# arms each as their own cell + CE>0 + HMA — so it shows all 5 day types like §8.1's 8.1b.
# These lists carry (label, alpha) only: the day-type colour now varies per endpoint, so it is
# resolved inside the per-panel loop as day_type_colors(col)[label].
DISPLAY_CELLS = [
    (NMA_LABEL, VIOLIN_ALPHA),
    (COMPARATOR_LABEL, VIOLIN_ALPHA),
    (HIGH_MA_LABEL, VIOLIN_ALPHA),
]
# 5-day-type cells for the 8.2a violins: (label, alpha) — the 3 nested NMA/CE=0 classifications, CE>0,
# and HMA. Colour resolved per panel via day_type_colors(col) (band ramp / grey / bronze).
DISPLAY_CELLS_5WAY = [(lab, VIOLIN_ALPHA) for _f, lab in CLASSIFICATIONS] + [
    (COMPARATOR_LABEL, VIOLIN_ALPHA),
    (HIGH_MA_LABEL, VIOLIN_ALPHA),
]


def build_display_frame(pdf, nma_flag):
    """Long 3-day-type frame for the descriptive figures: NMA days (`nma_flag`) labeled 'NMA',
    CE>0 comparator days, and HMA (CE>=3/BE>=3) days — each labeled by day_type, two strategies
    only. HMA overlaps CE>0 (its days appear under both labels); intentional, for the descriptive
    6-cell display only (the inferential fits use the disjoint 2-day-type frames)."""
    strat_names = [s for s, _ in STRATEGIES]
    parts = []
    for flag, label in [(nma_flag, NMA_LABEL), (COMPARATOR_FLAG, COMPARATOR_LABEL),
                        (HIGH_MA_FLAG, HIGH_MA_LABEL)]:
        sub = pdf.loc[pdf[flag] == True].copy()  # noqa: E712
        sub[DAY_TYPE_COL] = label
        parts.append(sub)
    frame = pd.concat(parts, ignore_index=True)
    return frame[frame[STRATEGY_COL].isin(strat_names)].copy()


def build_display_frame_5way(pdf):
    """Long 5-day-type frame for the 8.2a violins: the 3 nested NMA/CE=0 classifications (each
    labeled by its classification), the CE>0 comparator, and HMA (CE>=3/BE>=3) — two strategies
    only. The nested NMA arms overlap (CE=0/BE=0 ⊂ BE≤1 ⊂ BE≤∞) and HMA ⊂ CE>0; intentional, for
    the descriptive display only (the inferential fits use the disjoint 2-day-type frames)."""
    strat_names = [s for s, _ in STRATEGIES]
    arms = list(CLASSIFICATIONS) + [(COMPARATOR_FLAG, COMPARATOR_LABEL), (HIGH_MA_FLAG, HIGH_MA_LABEL)]
    parts = []
    for flag, label in arms:
        sub = pdf.loc[pdf[flag] == True].copy()  # noqa: E712
        sub[DAY_TYPE_COL] = label
        parts.append(sub)
    frame = pd.concat(parts, ignore_index=True)
    return frame[frame[STRATEGY_COL].isin(strat_names)].copy()


def fit_interaction_models(frames, nma_stats, treatments=CLASSIFICATIONS, treatment_label=NMA_LABEL):
    """Fit the day_type × delivery_strategy interaction LMM for every (treatment, endpoint).
    Returns one record per cell with the coefficient summary and the model's marginal cell means
    (None when the fit was skipped/failed).

    The main pass fits the 3 nested NMA classifications (treatment day_type = 'NMA' vs CE>0). The
    §12.2 supplement passes treatments=[(HIGH_MA_FLAG, HIGH_MA_LABEL)], treatment_label=HIGH_MA_LABEL
    to fit the high meal-announcement contrast (CE>=3/BE>=3 vs CE>0). The day_type reference is CE>0
    in both (alphabetical), so main_day_coef = treatment − CE>0.

    Guards (mirror §8.1 create_table_8_1b): a fit is attempted only when both day_type levels
    and both delivery strategies are present, all four cells have >=2 contributing users, and
    the outcome is non-constant. Sparse autobolus_on days make some cells degenerate; those
    are flagged converged=False rather than aborting the table."""
    has_statsmodels = importlib.util.find_spec("statsmodels") is not None
    if not has_statsmodels:
        print("  §8.2 LMM skipped: statsmodels not installed — interaction columns will be "
              "NaN. Install with `%pip install statsmodels` (or use an ML runtime).")
    strat_names = [s for s, _ in STRATEGIES]
    day_types = [treatment_label, COMPARATOR_LABEL]
    cells = [(d, s) for d in day_types for s in strat_names]

    records = []
    for treat_flag, cls_label in treatments:
        frame = frames[cls_label]
        for col, ep_label in ENDPOINTS:
            sl = frame[["_userId", DAY_TYPE_COL, STRATEGY_COL, col]].dropna(subset=[col])
            cell_users = sl.groupby([DAY_TYPE_COL, STRATEGY_COL])["_userId"].nunique()
            cells_ok = all(cell_users.get((d, s), 0) >= 2 for d, s in cells)
            fittable = (
                sl[DAY_TYPE_COL].nunique() == 2
                and sl[STRATEGY_COL].nunique() == 2
                and cells_ok
                and sl[col].nunique() >= 2
            )
            rec = {
                "classification": cls_label, "endpoint": col, "label": ep_label,
                "converged": False,
                "main_day_coef": np.nan, "main_day_ci_lo": np.nan, "main_day_ci_hi": np.nan,
                "main_day_p": np.nan,
                "main_strategy_coef": np.nan, "main_strategy_ci_lo": np.nan,
                "main_strategy_ci_hi": np.nan, "main_strategy_p": np.nan,
                "interaction_coef": np.nan, "interaction_ci_lo": np.nan,
                "interaction_ci_hi": np.nan, "interaction_p": np.nan,
                "n_users": int(sl["_userId"].nunique()), "n_days": int(len(sl)),
                "marginal_cells": None,
            }
            if has_statsmodels and fittable:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = nma_stats.lmm_day_strategy_interaction(
                            sl, outcome=col, day_type_col=DAY_TYPE_COL,
                            strategy_col=STRATEGY_COL, user_col="_userId")
                    rec.update({
                        "converged": True,
                        "main_day_coef": res["main_day_coef"],
                        "main_day_ci_lo": res["main_day_ci"][0],
                        "main_day_ci_hi": res["main_day_ci"][1],
                        "main_day_p": res["main_day_p"],
                        "main_strategy_coef": res["main_strategy_coef"],
                        "main_strategy_ci_lo": res["main_strategy_ci"][0],
                        "main_strategy_ci_hi": res["main_strategy_ci"][1],
                        "main_strategy_p": res["main_strategy_p"],
                        "interaction_coef": res["interaction_coef"],
                        "interaction_ci_lo": res["interaction_ci"][0],
                        "interaction_ci_hi": res["interaction_ci"][1],
                        "interaction_p": res["interaction_p"],
                        "n_users": res["n_users"], "n_days": res["n_days"],
                        "marginal_cells": res["marginal_cells"],
                    })
                except Exception as e:  # noqa: BLE001 — degenerate cells shouldn't abort the table
                    print(f"  §8.2 LMM failed for {cls_label} / {col}: {e}")
            elif has_statsmodels and not fittable:
                print(f"  §8.2 LMM skipped (degenerate cell) for {cls_label} / {col}")
            records.append(rec)
    return records


def build_table_8_2b(records):
    """Table 8.2b: per (classification, endpoint), the main day-type effect (NMA−CE>0), main
    strategy effect (TB−AB), and the day-type × strategy interaction, each with 95% CI + p.
    Degenerate cells carry NaN coefs and converged=False."""
    keep = [
        "classification", "endpoint", "label", "converged",
        "main_day_coef", "main_day_ci_lo", "main_day_ci_hi", "main_day_p",
        "main_strategy_coef", "main_strategy_ci_lo", "main_strategy_ci_hi", "main_strategy_p",
        "interaction_coef", "interaction_ci_lo", "interaction_ci_hi", "interaction_p",
        "n_users", "n_days",
    ]
    df = pd.DataFrame([{k: r[k] for k in keep} for r in records])
    df["is_primary"] = df["endpoint"] == PRIMARY_ENDPOINT
    df["interaction_display"] = df.apply(
        lambda x: f"{x['interaction_coef']:.2f} [{x['interaction_ci_lo']:.2f}, "
                  f"{x['interaction_ci_hi']:.2f}]" if pd.notna(x["interaction_coef"]) else "N/A",
        axis=1)
    return df


def build_table_8_2a(frames, records, hma_frames, hma_fits):
    """Table 8.2a: marginal cell means per (classification, endpoint, day_type, strategy).
    Observed = across-user mean ± SD of per-user means (robust, model-independent); model_mean
    = the LMM-estimated marginal mean (NaN where the fit was degenerate).

    Emits the 3 nested NMA classifications (each as its NMA + CE>0 cells) plus the high
    meal-announcement arm (CE>=3/BE>=3) as a 5th descriptive day type, from the §12.2 HMA
    frame/fit — so the report can render all 5 day types × {AB, TB} descriptively (the day ×
    strategy interaction stays in Table 8.2b). The HMA section emits only its CE>=3/BE>=3 cells:
    its CE>0 comparator is the same canonical set already present under each NMA classification
    (CE>0 = pdf[COMPARATOR_FLAG]==True in every frame), so it is omitted rather than re-copied.
    (D18 emit, developer_note 2026-06-09.)"""
    model_means = {}
    for r in list(records) + list(hma_fits):
        mc = r["marginal_cells"]
        if mc:
            for (d, s), info in mc.items():
                model_means[(r["classification"], r["endpoint"], d, s)] = info["mean"]

    strat_names = [s for s, _ in STRATEGIES]
    # (frame, day_types to emit, classification key). The 3 NMA frames emit NMA + CE>0; the HMA
    # frame emits only its CE>=3/BE>=3 cells (CE>0 already present, byte-identical).
    sections = [(frames[cls_label], [NMA_LABEL, COMPARATOR_LABEL], cls_label)
                for _flag, cls_label in CLASSIFICATIONS]
    sections.append((hma_frames[HIGH_MA_LABEL], [HIGH_MA_LABEL], HIGH_MA_LABEL))

    rows = []
    for frame, day_types, cls_label in sections:
        for col, ep_label in ENDPOINTS:
            for d in day_types:
                for s in strat_names:
                    cell = frame[(frame[DAY_TYPE_COL] == d) & (frame[STRATEGY_COL] == s)]
                    per_user = cell.groupby("_userId")[col].mean().dropna()
                    n_u = int(len(per_user))
                    rows.append({
                        "classification": cls_label, "endpoint": col, "label": ep_label,
                        "day_type": d, "delivery_strategy": s,
                        "observed_mean": per_user.mean() if n_u else np.nan,
                        "observed_sd": per_user.std(ddof=1) if n_u > 1 else np.nan,
                        "n_users": n_u, "n_days": int(cell[col].notna().sum()),
                        "model_mean": model_means.get((cls_label, col, d, s), np.nan),
                    })
    df = pd.DataFrame(rows)
    df["observed_display"] = df.apply(
        lambda x: f"{x['observed_mean']:.1f} ± {x['observed_sd']:.1f}" if pd.notna(x["observed_sd"])
        else (f"{x['observed_mean']:.1f}" if pd.notna(x["observed_mean"]) else "N/A"), axis=1)
    return df


# The per-endpoint figures show all 5 day types (8.2a violins → 10 cells/strategy-pair; 8.2c lines →
# one line per nested NMA arm + CE>0 + HMA), each from its own fit/frame. HEADLINE_CLS (the headline
# CE=0/BE≤1 arm, matching §8.1c/§8.4) supplies 8.2c's single CE>0 comparator line. Cells/lines colour
# each day type by the panel endpoint's band (day_type_colors(col)).
HEADLINE_CLS = CLASSIFICATIONS[1][1]   # "CE=0/BE<=1" — the headline NMA arm


def _cell_violin_groups(frame, endpoint):
    """Violin groups for the 10 (day_type × strategy) cells of one endpoint, in the shape
    utils.plotting.violin_box_panel expects: (label, values, colour, alpha). Grouped by day type with
    the TB/AB pair adjacent (so the strategy effect reads within each day type): d1-TB, d1-AB |
    d2-TB, d2-AB | … Colour = the day type's colour for THIS endpoint's band (day_type_colors(endpoint):
    3 nested NMA/CE=0 ramp, CE>0 grey, HMA bronze); within a pair AB is darker, TB lighter
    (STRATEGY_ALPHA)."""
    dt_colors = day_type_colors(endpoint)
    groups = []
    for d, _alpha in DISPLAY_CELLS_5WAY:
        color = dt_colors[d]
        for s_name, s_short in STRATEGIES:  # STRATEGIES is TB-first (data_loader), so each pair is TB|AB
            cell = frame[(frame[STRATEGY_COL] == s_name) & (frame[DAY_TYPE_COL] == d)]
            vals = cell.groupby("_userId")[endpoint].mean().dropna().to_numpy()
            groups.append((f"{s_short}\n{d}", vals, color, STRATEGY_ALPHA[s_name]))
    return groups


def make_violin_grids(display_frame):
    """Figure 8.2a: per-user mean endpoints by day type × delivery strategy, showing all 5 day types
    (3 nested NMA/CE=0 + CE>0 + CE>=3/BE>=3) with each day type's AB/TB pair adjacent → 10 cells per
    endpoint. The 10-cell panels are wide, so the 4 endpoints of each grid stack 4×1 (full-width).
    Returns {filename: figure}. Cells colour each day type by the panel endpoint's band
    (day_type_colors(col); AB darker / TB lighter); a light separator divides each day-type TB|AB pair
    from the next, and a dark line + diamonds connects the two across-user means within each pair (the
    TB→AB strategy shift for that day type)."""
    n_strat = len(STRATEGIES)  # separators between day-type groups (after each TB/AB pair)
    seps = tuple(n_strat * k + 0.5 for k in range(1, len(DISPLAY_CELLS_5WAY)))
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(4, 1, figsize=(16, 20))
        for ax, (col, label) in zip(axes.ravel(), eps):
            groups = _cell_violin_groups(display_frame, col)
            violin_box_panel(ax, groups, title=label, title_color=endpoint_color(col), separators=seps)
            # Connect the across-user means within each day type's TB|AB pair (positions are 1-indexed).
            means = [g[1].mean() if len(g[1]) else np.nan for g in groups]
            for i in range(0, len(groups), n_strat):
                ax.plot([i + 1, i + 2], means[i:i + 2], color="#222222", lw=1.4, marker="D",
                        ms=5, mec="white", mew=0.6, zorder=6)
        fig.suptitle(f"{gtitle}\nper-user means by strategy × day type "
                     f"(3 nested NMA, CE>0, CE>=3/BE>=3)", fontsize=SUPTITLE_FS)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        out[f"figure_8_2a_violin_{key}.png"] = fig
    return out


def make_interaction_grids(records, hma_records):
    """Figure 8.2c: model-estimated marginal-mean interaction (delivery strategy on x) with all 5 day
    types as lines — the 3 nested NMA/CE=0 classifications, CE>0, and the high meal-announcement
    CE>=3/BE>=3 arm — across the two shared 2×2 metric grids (all 8 endpoints). Each NMA line comes
    from that classification's own NMA-vs-CE>0 fit; the single CE>0 line from the broadest fit
    (CE=0/BE≤∞); the HMA line from the §12.2 HMA-vs-CE>0 fit. Lines colour each day type by the panel
    endpoint's band (day_type_colors(col): 3 nested ramp, CE>0 grey, HMA bronze); panel title = endpoint
    colour. Because the hue varies per endpoint, each panel carries its own arm legend
    (day_type_legend) rather than one figure-level legend. Returns {filename: figure} (a single 4×2
    grid over all 8 endpoints); a panel with no fitted line shows a degenerate note."""
    strat_names = [s for s, _ in STRATEGIES]
    strat_short = [sh for _, sh in STRATEGIES]
    by_cls_ep = {(r["classification"], r["endpoint"]): r for r in records}
    by_ep_hma = {r["endpoint"]: r for r in hma_records}

    def _line(ax, rec, day_key, label, color):
        """Plot one day type's marginal-mean line across strategies if the fit produced it; returns
        the arm label when a line was drawn (so the panel can build its legend), else None."""
        mc = rec["marginal_cells"] if rec else None
        if not mc:
            return None
        ys = [mc.get((day_key, s), {}).get("mean", np.nan) for s in strat_names]
        if np.all(np.isnan(ys)):
            return None
        ax.plot(range(len(strat_names)), ys, marker="o", label=label, color=color)
        return label

    def panel(ax, col, label):
        dt_colors = day_type_colors(col)
        plotted_arms = []
        # 3 nested NMA/CE=0 arms — each line from its own fit (treatment day_type = 'NMA').
        for _flag, cls_label in CLASSIFICATIONS:
            plotted_arms.append(_line(ax, by_cls_ep.get((cls_label, col)), NMA_LABEL, cls_label,
                                      dt_colors[cls_label]))
        # CE>0 comparator — one line, from the broadest classification's fit.
        plotted_arms.append(_line(ax, by_cls_ep.get((HEADLINE_CLS, col)), COMPARATOR_LABEL,
                                  COMPARATOR_LABEL, dt_colors[COMPARATOR_LABEL]))
        # CE>=3/BE>=3 high meal-announcement arm — from the §12.2 fit.
        plotted_arms.append(_line(ax, by_ep_hma.get(col), HIGH_MA_LABEL, HIGH_MA_LABEL,
                                  dt_colors[HIGH_MA_LABEL]))
        drawn = {a for a in plotted_arms if a is not None}
        if drawn:
            # Per-panel arm legend (swatches match this endpoint's ramp), arms in DAY_TYPE_ORDER.
            arms = [a for a in DAY_TYPE_ORDER if a in drawn]
            day_type_legend(ax, col, arms)
        else:
            ax.text(0.5, 0.5, "model not fit\n(degenerate)", ha="center", va="center",
                    transform=ax.transAxes, fontsize=11, color="gray")
        ax.set_xticks(range(len(strat_names)))
        ax.set_xticklabels(strat_short)
        ax.set_title(label, fontsize=TITLE_FS, color=endpoint_color(col))

    return render_4x2_grid(panel, fig_stem="figure_8_2c_interaction",
                           subtitle="day-type × strategy interaction (marginal means)")


def make_figure_8_2d(display_frames):
    """Figure 8.2d: stacked bar of mean % time in each glycemic range per cell (strategy ×
    day_type {NMA, CE>0, HMA}), one subplot per classification. Per-user mean within cell, then
    averaged across users (equal weighting, as in §8.1). HMA (CE>=3/BE>=3 ⊂ CE>0) does not vary by
    classification, so its two cells repeat across subplots — shown for parallel comparison."""
    fig, axes = plt.subplots(1, len(CLASSIFICATIONS),
                             figsize=(7.5 * len(CLASSIFICATIONS), 7), squeeze=False, sharey=True)
    range_keys = [rc for rc, _ in RANGE_COLS]
    day_type_labels = [d for d, _ in DISPLAY_CELLS]

    for ax, (nma_flag, cls_label) in zip(axes[0], CLASSIFICATIONS):
        frame = display_frames[cls_label].copy()
        frame["r_lt54"] = frame["tbr_very_low"]
        frame["r_54_70"] = frame["tbr"] - frame["tbr_very_low"]
        frame["r_70_180"] = frame["tir"]
        frame["r_180_250"] = frame["tar"] - frame["tar_very_high"]
        frame["r_gt250"] = frame["tar_very_high"]

        bar_labels, user_ns = [], []
        means = {rc: [] for rc in range_keys}
        for s_name, s_short in STRATEGIES:
            for d in day_type_labels:
                cell = frame[(frame[STRATEGY_COL] == s_name) & (frame[DAY_TYPE_COL] == d)]
                um = cell.groupby("_userId")[range_keys].mean()
                bar_labels.append(f"{s_short}\n{d}")
                user_ns.append(len(um))
                for rc in range_keys:
                    means[rc].append(um[rc].mean() if len(um) else 0.0)

        x = np.arange(len(bar_labels))
        bottom = np.zeros(len(bar_labels))
        for rc, rlabel in RANGE_COLS:
            vals = np.array(means[rc])
            ax.bar(x, vals, bottom=bottom, color=RANGE_COLORS[rlabel], edgecolor="white",
                   label=rlabel)
            # Per-segment % labels, matching the §8.1 / §8.3 stacked bars: skip the
            # smallest (<54) band, offset the thin 54-70 band upward with a leader.
            centers = bottom + vals / 2.0
            for xi, (v, cen) in enumerate(zip(vals, centers)):
                if rlabel == "<54":
                    continue
                if rlabel == "54-70":
                    ax.annotate(f"{v:.{STACKED_BAR_PCT_DECIMALS}f}%", xy=(xi, cen), xytext=(0, 16),
                                textcoords="offset points", ha="center", va="bottom", fontsize=11,
                                arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
                else:
                    ax.text(xi, cen, f"{v:.{STACKED_BAR_PCT_DECIMALS}f}%", ha="center",
                            va="center", fontsize=11)
            bottom += vals
        ax.set_xticks(x)
        ax.set_xticklabels(bar_labels, fontsize=TICK_FS)
        ax.set_ylim(0, 108)
        ax.set_title(cls_label, fontsize=TITLE_FS)
        for xi, u in enumerate(user_ns):
            ax.text(xi, 101, f"users={u}", ha="center", va="bottom", fontsize=11)

    axes[0][0].set_ylabel("Mean time in range (%)")
    axes[0][-1].legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left",
                       fontsize=LEGEND_FS)
    fig.suptitle("Mean time in glycemic ranges by classification × cell",
                 fontsize=SUPTITLE_FS)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort: Literal["adult", "pediatric", "all"] = "all",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    """Run Analysis 8.2 for one age cohort. Outputs land in outputs/analysis_8_2/<cohort>/
    unless an explicit output_dir is given. `min_age` defaults to the §6 floor (MIN_AGE=6);
    pass None to disable.

    Source priority: an explicit `csv_path`, else `spark.table(analysis_ready_table)` when a
    Spark session is given, else (local, no Spark) the CSV snapshot that
    data_staging/export_user_day_analysis_ready.py writes — outputs/<table-name>.csv.

    figures_only=True skips the table writes and re-renders only the figures, in place (existing
    table CSVs untouched). figs_filter (a substring, e.g. "8_2a") renders only matching figure
    builders (tags: 8_2a/8_2c/8_2d) and implies figures_only. NB the slow part is the LMM interaction
    fit, which figure 8.2c needs — so it is computed only when tables run OR 8.2c is being rendered
    (tweaking 8.2a/8.2d alone skips it)."""
    figures_only = figures_only or figs_filter is not None  # filtering figures ⇒ skip the tables
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_2", cohort)
    # Clear this cohort's dir first so it reflects only the current run (full run only; figures_only
    # overwrites PNGs in place, leaving the table CSVs intact).
    if os.path.isdir(output_dir) and not figures_only:
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if csv_path is None and spark is None:
        csv_path = default_analysis_ready_csv(analysis_ready_table)
        print(f"no Spark session; reading analysis-ready snapshot: {csv_path}")
    if csv_path is not None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"analysis-ready CSV not found at {csv_path}. Run "
                "data_staging/export_user_day_analysis_ready.py to create it (it writes "
                "outputs/<table-name>.csv), or pass csv_path/--csv_path."
            )
        pdf = prepare_day_level(pd.read_csv(csv_path))
    else:
        pdf = load_day_level(spark, analysis_ready_table)
    pdf = filter_cohort(pdf, cohort=cohort, min_age=min_age)
    pdf = restrict_comparator(pdf)
    # Drop ambiguous delivery strategy (§7.3); keep only the two known values.
    strat_names = [s for s, _ in STRATEGIES]
    pdf = pdf[pdf[STRATEGY_COL].isin(strat_names)].copy()

    # Build the per-classification NMA/CE>0 frames + display frames once (fast); reuse for tables +
    # figures.
    frames = {cls_label: build_day_type_frame(pdf, nma_flag)
              for nma_flag, cls_label in CLASSIFICATIONS}
    display_frames = {cls_label: build_display_frame(pdf, nma_flag)
                      for nma_flag, cls_label in CLASSIFICATIONS}

    # The LMM interaction fits are the slow step; figure 8.2c needs them, the tables need them, but
    # figures 8.2a/8.2d don't — so fit only when tables run OR 8.2c is among the rendered figures.
    renders_8_2c = (not figs_filter) or (figs_filter in "8_2c")
    fits = hma_fits = hma_frames = None
    if not figures_only or renders_8_2c:
        fits = fit_interaction_models(frames, nma_stats)
        # Appendix §12.2: high meal-announcement (CE>=3/BE>=3) vs CE>0 × strategy interaction — same
        # model/columns as Table 8.2b, treatment day_type = the high-engagement arm (overlapping
        # reference, CE>=3/BE>=3 ⊂ CE>0; mirrors §8.1's §12.1 high-engagement supplement).
        hma_frames = {HIGH_MA_LABEL: build_day_type_frame(pdf, HIGH_MA_FLAG, HIGH_MA_LABEL)}
        hma_fits = fit_interaction_models(hma_frames, nma_stats,
                                          treatments=[(HIGH_MA_FLAG, HIGH_MA_LABEL)],
                                          treatment_label=HIGH_MA_LABEL)

    # ---- Tables. Skipped under figures_only so a figure tweak re-renders quickly. ----
    table_b = None
    if not figures_only:
        table_b = build_table_8_2b(fits)
        print(table_b.to_string(index=False))
        table_b.to_csv(os.path.join(output_dir, "table_8_2b_interaction.csv"), index=False)
        build_table_8_2a(frames, fits, hma_frames, hma_fits).to_csv(
            os.path.join(output_dir, "table_8_2a_marginal_cells.csv"), index=False)
        build_table_8_2b(hma_fits).to_csv(
            os.path.join(output_dir, "table_12_2a_high_engagement_interaction.csv"), index=False)

    # ---- Figures. Each builder is a thunk → {filename: figure}, keyed by a short tag; figs_filter
    # (a substring) renders only matching builders. 8.2a per-user violin grids (strategy × day type
    # {NMA, CE>0, HMA}, broadest arm); 8.2c interaction-marginal-mean grids (+ HMA line); 8.2d stacked
    # glycemic ranges per cell across all three classifications. ----
    fig_builders = {
        "8_2a": lambda: make_violin_grids(build_display_frame_5way(pdf)),
        "8_2c": lambda: make_interaction_grids(fits, hma_fits),
        "8_2d": lambda: {"figure_8_2d_stacked_bars.png": make_figure_8_2d(display_frames)},
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
    print(f"wrote analysis 8.2 ({cohort}) — {n} figure(s){tag} to {output_dir}")
    return table_b


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    """Orchestrate the §7.6 cohort split: adult and pediatric reported separately, plus a
    pooled `all` sanity run. Each lands in its own outputs/analysis_8_2/<cohort>/ dir.
    figures_only / figs_filter forward the fast figure-only path to each cohort run."""
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path, figures_only=figures_only, figs_filter=figs_filter)


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--analysis_ready_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--csv_path", default=None,
                         help="analysis-ready CSV snapshot (run without Spark); defaults to "
                              "outputs/<table-name>.csv when no Spark session is present")
    _parser.add_argument("--output_dir", default=None)
    _parser.add_argument("--cohort", default=None, choices=["adult", "pediatric", "all"])
    _parser.add_argument("--min_age", type=int, default=MIN_AGE,
                         help=f"§6 min-age floor in years (default {MIN_AGE}); known-younger "
                              "users dropped, unknown-age retained")
    _parser.add_argument("--figures-only", dest="figures_only", action="store_true",
                         help="re-render figures only, skipping the slow LMM/table writes "
                              "(do a full run first so the tables exist)")
    _parser.add_argument("--figs", dest="figs_filter", default=None,
                         help="render only figure builders whose tag contains this substring "
                              "(e.g. 8_2a); implies --figures-only. Tags: 8_2a, 8_2c, 8_2d")
    _args, _ = _parser.parse_known_args()

    # Databricks injects a `spark` global; a local run has none.
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
