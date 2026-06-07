#!/usr/bin/env python
"""Analysis 8.4: glycemic outcomes & carb-entry-rate by delivery strategy (AB vs TB).

PLN-1008 §8.4 — SECONDARY / EXPLORATORY (not a primary citable claim). Two delivery-strategy
objectives, bundled into one module (per MJC):

PART 1 — glycemic outcomes × within-user TDD stratum × day type. Does the AB-vs-TB difference on
  glycemic endpoints depend on (a) the day's within-user TDD stratum and (b) the day type? We cross
  delivery_strategy (autobolus_on=AB vs temp_basal_only=TB) with the §8.3 same-user-set-gated
  WITHIN-USER TDD RANK strata (overall reference), per day type, fitting a per-arm 2-way interaction
  LMM `outcome ~ tdd_stratum * delivery_strategy + (1|user)`. The stratum×strategy interaction
  coefficient (does the AB−TB gap differ Low vs High?) is the inferential headline; the equal-user-
  weight descriptive cross-tab (per-cell per-user means + n) is the Method-A anchor. Per D5, report
  NO directional claim where the LMM (Method B, precision-weighted) and the cross-tab (Method A,
  equal-user-weight) diverge in sign — flagged per cell.

PART 2 — carb-entry-rate × strategy. "Are users more likely to log carbs on TB vs AB days?" Within-
  user paired AB-vs-TB: per user, the fraction of days with ≥1 carb entry (primary) and mean carb
  entries/day (secondary), Method A (paired_within_user) primary + a supportive day-level LMM.

⚠️ CAVEATS (frame in the prose):
- SAME-DAY ENTANGLEMENT / endogeneity: a day's delivery_strategy is itself a behavioural/algorithmic
  outcome (automatic_bolus_count ≥ 3 ⇒ AB), not a randomized assignment — AB-vs-TB confounds strategy
  with whatever drove it. Part 2 is the extreme case: carb logging (manual boluses) mechanically pushes
  a day toward TB, so carb-rate-by-strategy is near-tautological — descriptive only.
- D11 intake confound stacks on the High-TDD × strategy cell (High-TDD CE=0 days run much worse, likely
  unannounced intake) — report strategy contrasts WITHIN stratum (TDD rank held fixed), never pooled.
- CITATION STATUS: inherits §8.3's D12 rank-tercile status (D12 RESOLVED — citable).
  Winsorization NOT applied (rank-robust by construction; immaterial per MJC). See decisions.md D19.

Stratification axis (per MJC "sketch out both"): the rank-BINARY Low/High (overall ref) is the
cell-viable candidate for the ONE main summary figure (2 strata × 2 strategies = 4 cells/day type);
the rank-TERCILE Low/Mid/High is built in full alongside in Appendix §12.4 (easily promotable).
TB is the thin arm (~23% of eligible days), so High-TDD × TB cells can be thin — every fit/cell is
guarded (≥2 users/cell + non-constant) and flagged converged=False rather than asserted; n per cell
is surfaced. Across-user means are COMPOSITE same-user-set gated (users present in EVERY
stratum × strategy cell) so the cells are apples-to-apples (the D12 fix extended to the strategy dim).

Day types: the main TABLES (8.4a cross-tab + 8.4b interaction) cover ALL 5 day types (3 nested NMA +
CE>0 + HMA); the main summary FIGURE 8.4a stays the headline CE=0/BE<=1 vs CE>0 (a 5-day-type figure
is too busy — the all-5 figure is appendix fig 12.4b). The Appendix §12.4 cross-tab tables hold the
alternative axes (tercile, CE=0-reference, within-user) on the headline CE=0/BE<=1 vs CE>0 pair.

Reuses utils/strata (the hoisted §8.3 rank machinery), utils/statistics
(lmm_day_strategy_interaction, lmm_arm_contrast, paired_within_user) and utils/plotting
(DAY_TYPE_COLORS + STRATEGY_ALPHA). Mirrors §8.1–§8.3 per-cohort run()/main() + output-clearing.

Outputs (analysis/outputs/analysis_8_4/<cohort>/) — main §8.4:
    table_8_4a_strategy_cross_binary.csv     per (day type / strategy) × endpoint × stratum: across-user
                                             mean±SD + n_users + n_days (overall-ref rank BINARY,
                                             composite same-user-set gated) — the Method-A cross-tab,
                                             ALL 5 day types
    table_8_4b_strategy_interaction.csv      per day type × endpoint: stratum×strategy interaction LMM
                                             (binary), coef/CI/p + converged + n — the Method-B table,
                                             ALL 5 day types
    table_8_4c_carb_entry_by_strategy.csv    Part 2: within-user TB−AB carb-logging (frac CE>0 days +
                                             carb entries/day), paired + supportive LMM
    figure_8_4a_grid{1,2}_*.png              THE summary figure: AB vs TB across Low/High TDD strata,
                                             headline CE=0/BE≤1 + CE>0, all 8 endpoints (95% CI bars)
    figure_8_4b_carb_entry_by_strategy.png   Part 2: per-user TB−AB carb-logging deltas
  Appendix §12.4 breakout:
    table_12_4a_strategy_cross_tercile.csv   rank TERCILE Low/Mid/High × strategy, headline pair (the
                                             "both" sketch)
    table_12_4c_strategy_cross_ce0_binary.csv    CE=0-reference companion (binary), headline pair
    table_12_4d_strategy_within_user.csv     within-user AB−TB contrast within each fixed TDD stratum,
                                             headline pair
    figure_12_4a_tercile_grid{1,2}_*.png     tercile companion of fig 8.4a (headline pair)
    figure_12_4b_all5_grid{1,2}_*.png        all-5-day-type version of fig 8.4a (figure companion to
                                             Table 8.4a; too busy for the main section)
    figure_12_4c_ce0_grid{1,2}_*.png         CE=0-reference companion (headline pair)

Usage: python analysis_8-4_nma_by_delivery_strategy_stratified.py [--cohort {adult,pediatric,all}]
Fast figure iteration: add --figures-only, or --figs <tag> (e.g. `--figs 8_4a`). Tags: 8_4a/b,
12_4a/b/c. Do a full run first so the table CSVs exist.
"""

# %pip install statsmodels
# dbutils.library.restartPython()

from __future__ import annotations

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
from matplotlib.lines import Line2D  # noqa: E402

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
from utils.strata import (  # noqa: E402
    _rank_strata,
    _tercile_trend_stats,
    table_8_3a_per_user_by_stratum,
)
from utils.plotting import (  # noqa: E402
    DAY_TYPE_COLORS,
    GRIDS,
    LEGEND_FS,
    STRATEGY_ALPHA,
    SUPTITLE_FS,
    TIDEPOOL,
    TITLE_FS,
    endpoint_color,
    overlay_hist_panel,
    render_4x2_grid,
)

# Headline day type (per MJC): the CE=0/BE<=1 nested NMA arm, vs the CE>0 comparator. The main figure
# + Tables 8.4a/b use these two day types; the Appendix §12.4 breakout uses all 5 SUPPLEMENT_ARMS.
HEADLINE_ARM = CLASSIFICATIONS[1]                                  # ("in_ce0_be_le1", "CE=0/BE<=1")
MAIN_ARMS = [HEADLINE_ARM, (COMPARATOR_FLAG, COMPARATOR_LABEL)]    # main-figure day types
APPENDIX_ARMS = SUPPLEMENT_ARMS                                    # all 5 (3 nested NMA + CE>0 + HMA)
PRIMARY_ENDPOINT = "tir"
PRIMARY_REFERENCE = "overall"     # promoted §8.3 reference (D12); CE=0-ref companion → Appendix §12.4
CARB_COUNT_COL = "carb_entry_count"
STRAT_NAMES = [s for s, _ in STRATEGIES]   # ["temp_basal_only", "autobolus_on"] (TB-first)


def _order(split):
    """Stratum labels (in display order) for a split."""
    return ("Low", "Mid", "High") if split == "tercile" else ("Low", "High")


# ---------------------------------------------------------------------------------------------------
# Part 1 — glycemic outcomes × TDD stratum × delivery strategy
# ---------------------------------------------------------------------------------------------------

def _composite_gate(df, cells):
    """Keep only users with ≥1 day in EVERY (tdd_stratum, strategy) cell in `cells`, so the across-
    user means compare a single user set across all cells of a day type (the D12 same-user-set gate
    extended to the strategy dimension). Guards thin High-TDD × TB without dropping the run."""
    needed = {f"{st}|{s}" for st, s in cells}
    key = df["tdd_stratum"].astype(str) + "|" + df[STRATEGY_COL].astype(str)
    have = key.groupby(df["_userId"]).apply(lambda s: needed.issubset(set(s)))
    keep = have[have].index
    return df[df["_userId"].isin(keep)].copy()


def _arm_strategy_frame(pdf, arm_flag, *, reference, split):
    """One day type's days, labeled Low/(Mid/)High by within-user TDD rank (`reference` overall|ce0),
    filtered to the two known delivery strategies (ambiguous excluded), then COMPOSITE same-user-set
    gated over the (stratum × strategy) cells. The rank is computed over the user's ALL eligible days
    (overall ref) BEFORE the strategy filter, so the strategy filter does not distort the TDD norm."""
    order = _order(split)
    df = _rank_strata(pdf, arm_flag, reference=reference, split=split)
    df = df[df[STRATEGY_COL].isin(STRAT_NAMES)].copy()
    cells = [(st, s) for st in order for s in STRAT_NAMES]
    return _composite_gate(df, cells)


def table_strategy_cross(pdf, arms, *, reference, split):
    """Across-user mean±SD of the per-user cell mean, by (day type / strategy) section × endpoint ×
    stratum, with n_users + n_days per cell — the Method-A descriptive cross-tab. Composite same-user-
    set gated within each day type (so n_users is equal across that day type's stratum × strategy
    cells). Reuses table_8_3a_per_user_by_stratum with strategy-suffixed section labels."""
    order = _order(split)
    frames = []
    for arm_flag, arm_label in arms:
        gated = _arm_strategy_frame(pdf, arm_flag, reference=reference, split=split)
        sections = {f"{arm_label} / {s_short}": gated[gated[STRATEGY_COL] == s_name].copy()
                    for s_name, s_short in STRATEGIES}
        t = table_8_3a_per_user_by_stratum(sections, strata_order=order)
        frames.append(t)
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"classification": "arm_strategy"})
    out.insert(0, "split", split)
    out.insert(0, "reference", reference)
    return out


def _empty_interaction_rec(arm_label, col, ep_label, n_users, n_days):
    return {
        "arm": arm_label, "endpoint": col, "label": ep_label, "converged": False,
        "stratum_main_coef_low_minus_high": np.nan, "stratum_main_ci_lo": np.nan,
        "stratum_main_ci_hi": np.nan, "stratum_main_p": np.nan,
        "strategy_main_coef_tb_minus_ab": np.nan, "strategy_main_ci_lo": np.nan,
        "strategy_main_ci_hi": np.nan, "strategy_main_p": np.nan,
        "interaction_coef": np.nan, "interaction_ci_lo": np.nan, "interaction_ci_hi": np.nan,
        "interaction_p": np.nan, "n_users": n_users, "n_days": n_days,
    }


def fit_strategy_interaction(pdf, arms, nma_stats, *, reference, split):
    """Per day type × endpoint: the 2-way interaction LMM `outcome ~ tdd_stratum * delivery_strategy
    + (1|user)` (Method B). Reuses utils.statistics.lmm_day_strategy_interaction with
    day_type_col="tdd_stratum". Reference levels (alphabetical): stratum ref = High, strategy ref = AB,
    so stratum_main = Low−High, strategy_main = TB−AB, and the INTERACTION = how the AB−TB gap shifts
    Low vs High (the effect-modification headline). Binary (2×2) gives one clean interaction term;
    use split="binary" for the headline.

    Guards (mirror §8.2 fit_interaction_models / §8.3 table_8_3c_lmm): a fit is attempted only when
    every (stratum × strategy) cell has ≥2 users, both levels of each factor are present, and the
    outcome is non-constant; degenerate slices (esp. High-TDD × TB) are emitted converged=False NaN
    rather than raising. n_users / n_days surfaced regardless."""
    order = _order(split)
    cells = [(st, s) for st in order for s in STRAT_NAMES]
    has_sm = importlib.util.find_spec("statsmodels") is not None
    if not has_sm:
        print("  §8.4 LMM skipped: statsmodels not installed — interaction columns will be NaN.")
    rows = []
    for arm_flag, arm_label in arms:
        gated = _arm_strategy_frame(pdf, arm_flag, reference=reference, split=split)
        for col, ep_label in ENDPOINTS:
            sl = gated[["_userId", "tdd_stratum", STRATEGY_COL, col]].dropna(subset=[col])
            cell_users = sl.groupby(["tdd_stratum", STRATEGY_COL])["_userId"].nunique()
            cells_ok = all(cell_users.get((st, s), 0) >= 2 for st, s in cells)
            fittable = (sl["tdd_stratum"].nunique() == len(order)
                        and sl[STRATEGY_COL].nunique() == 2
                        and cells_ok and sl[col].nunique() >= 2)
            rec = _empty_interaction_rec(arm_label, col, ep_label,
                                         int(sl["_userId"].nunique()), int(len(sl)))
            if has_sm and fittable:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = nma_stats.lmm_day_strategy_interaction(
                            sl, outcome=col, day_type_col="tdd_stratum",
                            strategy_col=STRATEGY_COL, user_col="_userId")
                    rec.update({
                        "converged": True,
                        "stratum_main_coef_low_minus_high": res["main_day_coef"],
                        "stratum_main_ci_lo": res["main_day_ci"][0],
                        "stratum_main_ci_hi": res["main_day_ci"][1],
                        "stratum_main_p": res["main_day_p"],
                        "strategy_main_coef_tb_minus_ab": res["main_strategy_coef"],
                        "strategy_main_ci_lo": res["main_strategy_ci"][0],
                        "strategy_main_ci_hi": res["main_strategy_ci"][1],
                        "strategy_main_p": res["main_strategy_p"],
                        "interaction_coef": res["interaction_coef"],
                        "interaction_ci_lo": res["interaction_ci"][0],
                        "interaction_ci_hi": res["interaction_ci"][1],
                        "interaction_p": res["interaction_p"],
                        "n_users": res["n_users"], "n_days": res["n_days"],
                    })
                except Exception as e:  # noqa: BLE001 — degenerate cells shouldn't abort the table
                    print(f"  §8.4 LMM failed for {arm_label} / {col}: {e}")
            elif has_sm and not fittable:
                print(f"  §8.4 LMM skipped (degenerate cell) for {arm_label} / {col}")
            rows.append(rec)
    out = pd.DataFrame(rows)
    out["is_primary"] = out["endpoint"] == PRIMARY_ENDPOINT
    out.insert(0, "split", split)
    out.insert(0, "reference", reference)
    return out


def table_strategy_within_user(pdf, arms, nma_stats, *, reference, split):
    """Within-user AB−TB paired contrast WITHIN each fixed TDD stratum, per day type × endpoint ×
    stratum (Wilcoxon + boot CI + paired-t; diff = TB − AB). Holds the TDD rank fixed so the strategy
    contrast is not confounded by the (much larger) Low→High TDD gradient (D11). Only users with ≥1 TB
    and ≥1 AB day in that stratum contribute (self-gating via paired_within_user's dropna)."""
    order = _order(split)
    rows = []
    for arm_flag, arm_label in arms:
        gated = _arm_strategy_frame(pdf, arm_flag, reference=reference, split=split)
        for col, ep_label in ENDPOINTS:
            for st in order:
                sub = gated[gated["tdd_stratum"] == st]
                tb = sub[sub[STRATEGY_COL] == "temp_basal_only"].groupby("_userId")[col].mean()
                ab = sub[sub[STRATEGY_COL] == "autobolus_on"].groupby("_userId")[col].mean()
                paired = pd.DataFrame({"tb": tb, "ab": ab}).dropna()
                res = nma_stats.paired_within_user(paired["tb"], paired["ab"])  # diff = TB − AB
                rows.append({
                    "arm": arm_label, "endpoint": col, "label": ep_label, "stratum": st,
                    "tb_mean": paired["tb"].mean() if len(paired) else np.nan,
                    "ab_mean": paired["ab"].mean() if len(paired) else np.nan,
                    "diff_tb_minus_ab": res["mean_diff"],
                    "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
                    "median_diff": res["median_diff"],
                    "wilcoxon_p": res["wilcoxon_p"], "t_p": res["t_p"], "n_pairs": res["n_pairs"],
                })
    out = pd.DataFrame(rows)
    out.insert(0, "split", split)
    out.insert(0, "reference", reference)
    return out


# ---------------------------------------------------------------------------------------------------
# Part 2 — carb-entry-rate by delivery strategy
# ---------------------------------------------------------------------------------------------------

def _carb_per_user(pdf):
    """Per-user, per-strategy carb-logging summaries on eligible days (known strategies only):
    `ce_gt0` = fraction of days with ≥1 carb entry; mean carb entries/day. Returns
    (day_level_gated, per_frac, per_rate) where per_frac/per_rate are user × strategy wide frames
    restricted to users with ≥1 TB and ≥1 AB day (same-user-set gate for the paired contrast)."""
    df = pdf[pdf[STRATEGY_COL].isin(STRAT_NAMES)].dropna(subset=[CARB_COUNT_COL]).copy()
    df["ce_gt0"] = (df[CARB_COUNT_COL] > 0).astype(float)
    g = df.groupby(["_userId", STRATEGY_COL])
    per_frac = g["ce_gt0"].mean().unstack(STRATEGY_COL)
    per_rate = g[CARB_COUNT_COL].mean().unstack(STRATEGY_COL)
    both = (per_frac.dropna(subset=STRAT_NAMES).index
            .intersection(per_rate.dropna(subset=STRAT_NAMES).index))
    return df[df["_userId"].isin(both)].copy(), per_frac.loc[both], per_rate.loc[both]


def table_carb_entry_by_strategy(per_frac, per_rate, df_g, nma_stats):
    """Part 2 table: within-user TB−AB carb-logging contrast for two metrics — fraction of days with
    ≥1 carb entry, and carb entries/day. Method A (paired_within_user, equal-user-weight, diff = TB −
    AB) primary; a supportive day-level LMM (`metric ~ delivery_strategy + (1|user)`; coef = TB − AB)
    alongside. Positive ⇒ more carb logging on TB days. ⚠️ same-day entanglement — descriptive only."""
    has_sm = importlib.util.find_spec("statsmodels") is not None
    rows = []
    for metric, outcome_col, tab in [("frac_days_ce_gt0", "ce_gt0", per_frac),
                                     ("carb_entries_per_day", CARB_COUNT_COL, per_rate)]:
        tb = tab["temp_basal_only"]
        ab = tab["autobolus_on"]
        res = nma_stats.paired_within_user(tb, ab)  # diff = TB − AB
        rec = {
            "metric": metric, "tb_mean": float(tb.mean()), "ab_mean": float(ab.mean()),
            "diff_tb_minus_ab": res["mean_diff"],
            "diff_ci_lo": res["mean_diff_ci_lo"], "diff_ci_hi": res["mean_diff_ci_hi"],
            "median_diff": res["median_diff"],
            "wilcoxon_p": res["wilcoxon_p"], "t_p": res["t_p"], "n_pairs": res["n_pairs"],
            "lmm_coef_tb_minus_ab": np.nan, "lmm_ci_lo": np.nan, "lmm_ci_hi": np.nan,
            "lmm_p": np.nan, "lmm_converged": False,
        }
        if has_sm and df_g[STRATEGY_COL].nunique() == 2 and df_g[outcome_col].nunique() >= 2:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    lr = nma_stats.lmm_arm_contrast(df_g, outcome=outcome_col, arm_col=STRATEGY_COL)
                # term = delivery_strategy[T.temp_basal_only] (ref = AB) → coef = TB − AB
                rec.update({"lmm_coef_tb_minus_ab": lr["coef"], "lmm_ci_lo": lr["ci_lo"],
                            "lmm_ci_hi": lr["ci_hi"], "lmm_p": lr["pvalue"], "lmm_converged": True})
            except Exception as e:  # noqa: BLE001
                print(f"  §8.4 carb LMM failed for {metric}: {e}")
        rows.append(rec)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------------------------------

def _strategy_trend_panel(ax, endpoint, label, cells, order):
    """One endpoint panel: x = the TDD strata in `order`; one staggered errorbar line per (day type,
    strategy) cell — colour = day type (DAY_TYPE_COLORS), alpha = strategy (STRATEGY_ALPHA, AB darker
    / TB lighter). Marker = across-user mean of the per-user cell mean; whisker = ±1.96 SEM (95% CI).
    Each cell's Low→High markers are joined so the AB-vs-TB gap and how it shifts across strata read
    directly. No p-values (D13)."""
    x = np.arange(len(order))
    combos = list(cells.keys())            # (arm_label, s_name)
    offs = np.linspace(-0.28, 0.28, len(combos))
    for (arm_label, s_name), off in zip(combos, offs):
        mu, lo, hi = _tercile_trend_stats(cells[(arm_label, s_name)], endpoint, order)
        ax.errorbar(x + off, mu, yerr=np.vstack([mu - lo, hi - mu]), fmt="-o", ms=5, lw=1.6,
                    color=DAY_TYPE_COLORS[arm_label], alpha=STRATEGY_ALPHA[s_name],
                    ecolor=DAY_TYPE_COLORS[arm_label], elinewidth=2.0, capsize=3, zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(order)
    ax.set_xlabel("within-user TDD stratum")
    ax.set_title(label, color=endpoint_color(endpoint), fontsize=TITLE_FS)
    ax.margins(x=0.16)


def figure_8_4a_strategy_bars(pdf, arms, *, reference="overall", split="binary",
                              fname_stem="figure_8_4a", ref_note="overall TDD-rank ref"):
    """THE §8.4 summary figure family (single 4×2 grid → all 8 endpoints): AB vs TB across within-user
    TDD strata, one staggered 95% CI bar per (day type, strategy) cell, same-user-set gated. Colour =
    day type (DAY_TYPE_COLORS), alpha = strategy (AB darker / TB lighter). Parametrized by `arms`
    (MAIN_ARMS for the headline; APPENDIX_ARMS for the all-5 breakout), `reference` (overall|ce0) and
    `split` (binary|tercile) so the main figure and the Appendix §12.4 companions come from one
    builder. Returns {filename: figure}."""
    order = _order(split)
    cells = {}
    n_by_cell = {}
    for arm_flag, arm_label in arms:
        gated = _arm_strategy_frame(pdf, arm_flag, reference=reference, split=split)
        for s_name, s_short in STRATEGIES:
            sub = gated[gated[STRATEGY_COL] == s_name]
            cells[(arm_label, s_name)] = sub
            n_by_cell[(arm_label, s_short)] = int(sub["_userId"].nunique())
    handles = [Line2D([0], [0], marker="o", color=DAY_TYPE_COLORS[arm_label],
                      alpha=STRATEGY_ALPHA[s_name], lw=2, ms=7,
                      label=f"{arm_label} · {s_short} (n={n_by_cell[(arm_label, s_short)]})")
               for arm_flag, arm_label in arms for s_name, s_short in STRATEGIES]
    ref = f" ({ref_note})" if ref_note else ""

    def panel(ax, col, label):
        _strategy_trend_panel(ax, col, label, cells, order)

    def legend(fig):
        fig.legend(handles=handles, loc="lower center", ncol=min(len(handles), 5), fontsize=LEGEND_FS,
                   bbox_to_anchor=(0.5, 0.0))

    return render_4x2_grid(panel, fig_stem=fname_stem,
                           subtitle=f"AB vs TB across within-user TDD strata{ref}; "
                                    f"whiskers = 95% CI, same-user-set gated",
                           figsize=(13, 16), bottom=0.06, decorate=legend)


def figure_8_4b_carb(per_frac, per_rate):
    """Part 2 figure: per-user within-user TB−AB carb-logging deltas, two panels (fraction of days
    with ≥1 carb entry; carb entries/day). Solid line = mean Δ, dashed = 0. Positive ⇒ more carb
    logging on TB days. ⚠️ same-day entanglement caveat applies (carb logging is the behaviour that
    sets the strategy)."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
    specs = [("Fraction of days with ≥1 carb entry", per_frac, "frac days CE>0"),
             ("Carb entries per day", per_rate, "carb entries/day")]
    for ax, (title, tab, xlab) in zip(axes, specs):
        delta = (tab["temp_basal_only"] - tab["autobolus_on"]).dropna().to_numpy()
        overlay_hist_panel(ax, [(delta, "TB − AB", TIDEPOOL)],
                           xlabel=f"per-user Δ {xlab} (TB − AB)", title=title)
    fig.suptitle("Within-user carb-logging by delivery strategy (TB − AB)\n"
                 "positive ⇒ more carb logging on TB days; ⚠️ same-day entanglement (descriptive only)",
                 fontsize=SUPTITLE_FS)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
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
    """Run §8.4 for one age cohort → outputs/analysis_8_4/<cohort>/.

    figures_only=True skips the SLOW table writes (LMM fits + cluster-bootstrap CIs) and re-renders
    only the figures, in place. figs_filter (a substring, e.g. "8_4a") renders only matching figure
    builders (tags: 8_4a/8_4b/12_4a/12_4b/12_4c) and implies figures_only. Do a full run first so the
    table CSVs exist."""
    figures_only = figures_only or figs_filter is not None  # filtering figures ⇒ skip the tables
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_4", cohort)
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
    # NB: ambiguous delivery strategy is NOT dropped globally here — the overall-reference TDD rank
    # (_rank_strata) must be computed over the user's ALL eligible days; the strategy filter is applied
    # downstream inside _arm_strategy_frame / _carb_per_user.

    # Part 2 per-user carb summaries (cheap; consumed by both the table and the figure).
    df_carb_g, per_frac, per_rate = _carb_per_user(pdf)

    # ---- Tables (SLOW: LMM fits + cluster-bootstrap CIs). Skipped under figures_only. ----
    if not figures_only:
        # Main §8.4 — Method-A cross-tab (binary) + Method-B interaction LMM (binary) + Part 2. Both
        # tables cover ALL 5 day types (3 nested NMA + CE>0 + HMA); the main FIGURE 8.4a stays the
        # headline CE=0/BE≤1 vs CE>0 (a 5-day-type figure is too busy — that is appendix fig 12.4b).
        table_strategy_cross(pdf, APPENDIX_ARMS, reference=PRIMARY_REFERENCE, split="binary").to_csv(
            os.path.join(output_dir, "table_8_4a_strategy_cross_binary.csv"), index=False)
        fit_strategy_interaction(pdf, APPENDIX_ARMS, nma_stats, reference=PRIMARY_REFERENCE,
                                 split="binary").to_csv(
            os.path.join(output_dir, "table_8_4b_strategy_interaction.csv"), index=False)
        table_carb_entry_by_strategy(per_frac, per_rate, df_carb_g, nma_stats).to_csv(
            os.path.join(output_dir, "table_8_4c_carb_entry_by_strategy.csv"), index=False)

        # Appendix §12.4 — alternative-axis sensitivities on the headline CE=0/BE≤1 vs CE>0 pair
        # (matching their figures): the "both" tercile sketch, the CE=0-reference companion, and the
        # within-user AB−TB contrast within each fixed stratum. (The all-5 binary cross-tab is now
        # Table 8.4a; appendix fig 12.4b is its 5-day-type figure companion.)
        table_strategy_cross(pdf, MAIN_ARMS, reference=PRIMARY_REFERENCE, split="tercile").to_csv(
            os.path.join(output_dir, "table_12_4a_strategy_cross_tercile.csv"), index=False)
        table_strategy_cross(pdf, MAIN_ARMS, reference="ce0", split="binary").to_csv(
            os.path.join(output_dir, "table_12_4c_strategy_cross_ce0_binary.csv"), index=False)
        table_strategy_within_user(pdf, MAIN_ARMS, nma_stats, reference=PRIMARY_REFERENCE,
                                   split="binary").to_csv(
            os.path.join(output_dir, "table_12_4d_strategy_within_user.csv"), index=False)

    # ---- Figures (thunk dict keyed by short tag; figs_filter renders only matching builders). ----
    fig_builders = {
        "8_4a": lambda: figure_8_4a_strategy_bars(pdf, MAIN_ARMS, reference=PRIMARY_REFERENCE,
                                                  split="binary"),
        "8_4b": lambda: {"figure_8_4b_carb_entry_by_strategy.png":
                         figure_8_4b_carb(per_frac, per_rate)},
        "12_4a": lambda: figure_8_4a_strategy_bars(
            pdf, MAIN_ARMS, reference=PRIMARY_REFERENCE, split="tercile",
            fname_stem="figure_12_4a_tercile", ref_note="overall TDD-rank terciles"),
        "12_4b": lambda: figure_8_4a_strategy_bars(
            pdf, APPENDIX_ARMS, reference=PRIMARY_REFERENCE, split="binary",
            fname_stem="figure_12_4b_all5", ref_note="all 5 day types, overall ref"),
        "12_4c": lambda: figure_8_4a_strategy_bars(
            pdf, MAIN_ARMS, reference="ce0", split="binary",
            fname_stem="figure_12_4c_ce0", ref_note="CE=0 TDD-rank ref"),
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
    print(f"wrote analysis 8.4 ({cohort}) — {n} figure(s){tag} to {output_dir}")


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
                              "(e.g. 8_4a); implies --figures-only. Tags: 8_4a/8_4b, 12_4a/12_4b/12_4c")
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
