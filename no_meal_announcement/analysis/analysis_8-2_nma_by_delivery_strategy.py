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

Outputs (analysis/outputs/analysis_8_2/<cohort>/):
    table_8_2a_marginal_cells.csv   (per classification × endpoint × day_type × strategy:
                                     observed per-user-mean summary + model-estimated mean)
    table_8_2b_interaction.csv      (per classification × endpoint: main day-type, main
                                     strategy, and interaction coef/CI/p + n_users/n_days/converged)
    figure_8_2a_tir_violin_box.png  (per-user TIR by classification × strategy, NMA vs CE>0)
    figure_8_2b_tbr_violin_box.png  (per-user time <70, same layout)
    figure_8_2c_interaction.png     (model-estimated marginal-mean TIR interaction plot)
    figure_8_2d_stacked_bars.png    (mean time in glycemic ranges per cell)
"""

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
    RANGE_COLORS,
    RANGE_COLS,
    endpoint_color,
    violin_box_panel,
)

# §7.3 delivery strategies: (column value, short label). Any other / null strategy is
# "ambiguous" and excluded (the staging CASE currently only emits these two values).
STRATEGIES = [("autobolus_on", "AB"), ("temp_basal_only", "TB")]
STRATEGY_COL = "delivery_strategy"
DAY_TYPE_COL = "day_type"
NMA_LABEL = "NMA"
PRIMARY_ENDPOINT = "tir"

BOOTSTRAP_SEED = 20260520

# Colours (RANGE_COLORS/RANGE_COLS for the stacked bars, endpoint range colours for the violins/
# interaction lines) and the violin panel helper are shared across §8.1–§8.3 via utils.plotting.
# In the per-endpoint figures the NMA day-type carries the endpoint's range colour and the CE>0
# comparator is rendered grey.


def build_day_type_frame(pdf, nma_flag):
    """Long day-level frame for one classification: NMA days (the flag) labeled
    day_type='NMA', comparator days (in_ce_gt0) labeled 'CE>0', keeping only the two known
    delivery strategies (ambiguous excluded). The arms are disjoint (CE=0 vs CE>0), so a day
    belongs to at most one day_type."""
    strat_names = [s for s, _ in STRATEGIES]
    nma = pdf.loc[pdf[nma_flag] == True].copy()  # noqa: E712
    nma[DAY_TYPE_COL] = NMA_LABEL
    cmp = pdf.loc[pdf[COMPARATOR_FLAG] == True].copy()  # noqa: E712
    cmp[DAY_TYPE_COL] = COMPARATOR_LABEL
    frame = pd.concat([nma, cmp], ignore_index=True)
    return frame[frame[STRATEGY_COL].isin(strat_names)].copy()


def fit_interaction_models(frames, nma_stats):
    """Fit the day_type × delivery_strategy interaction LMM for every (classification,
    endpoint). Returns one record per cell with the coefficient summary and the model's
    marginal cell means (None when the fit was skipped/failed).

    Guards (mirror §8.1 create_table_8_1b): a fit is attempted only when both day_type levels
    and both delivery strategies are present, all four cells have >=2 contributing users, and
    the outcome is non-constant. Sparse autobolus_on days make some cells degenerate; those
    are flagged converged=False rather than aborting the table."""
    has_statsmodels = importlib.util.find_spec("statsmodels") is not None
    if not has_statsmodels:
        print("  §8.2 LMM skipped: statsmodels not installed — interaction columns will be "
              "NaN. Install with `%pip install statsmodels` (or use an ML runtime).")
    strat_names = [s for s, _ in STRATEGIES]
    day_types = [NMA_LABEL, COMPARATOR_LABEL]
    cells = [(d, s) for d in day_types for s in strat_names]

    records = []
    for nma_flag, cls_label in CLASSIFICATIONS:
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


def build_table_8_2a(frames, records):
    """Table 8.2a: marginal cell means per (classification, endpoint, day_type, strategy).
    Observed = across-user mean ± SD of per-user means (robust, model-independent); model_mean
    = the LMM-estimated marginal mean (NaN where the fit was degenerate)."""
    model_means = {}
    for r in records:
        mc = r["marginal_cells"]
        if mc:
            for (d, s), info in mc.items():
                model_means[(r["classification"], r["endpoint"], d, s)] = info["mean"]

    strat_names = [s for s, _ in STRATEGIES]
    rows = []
    for nma_flag, cls_label in CLASSIFICATIONS:
        frame = frames[cls_label]
        for col, ep_label in ENDPOINTS:
            for d in [NMA_LABEL, COMPARATOR_LABEL]:
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


# The per-endpoint figures (8.2a violins, 8.2c interaction) use the broadest classification
# (CE=0/BE≤∞, the most-populated headline arm), consistent with §8.3's broadest-arm choice; the
# stricter arms' interaction coefficients remain in table_8_2b and the stacked bars (8.2d) keep
# all three. NMA cells carry the endpoint range colour, CE>0 cells grey.
HEADLINE_CLS = CLASSIFICATIONS[-1][1]   # "CE=0/BE<=inf"


def _cell_violin_groups(frame, endpoint, base):
    """Violin groups for the 4 (strategy × day_type) cells of one endpoint, in the shape
    utils.plotting.violin_box_panel expects: (label, values, colour, alpha). NMA cells carry
    `base` (the endpoint range colour), CE>0 cells grey; ordered AB-NMA, AB-CE>0 | TB-NMA, TB-CE>0."""
    groups = []
    for s_name, s_short in STRATEGIES:
        for d in [NMA_LABEL, COMPARATOR_LABEL]:
            cell = frame[(frame[STRATEGY_COL] == s_name) & (frame[DAY_TYPE_COL] == d)]
            vals = cell.groupby("_userId")[endpoint].mean().dropna().to_numpy()
            is_nma = d == NMA_LABEL
            groups.append((f"{s_short}\n{d}", vals, base if is_nma else GRAY,
                           0.7 if is_nma else 0.55))
    return groups


def make_violin_grids(frames):
    """Figure 8.2a: per-user mean endpoints by delivery strategy × day type (NMA vs CE>0) on the
    broadest classification (CE=0/BE≤∞), as the two shared 2×2 metric grids (all 8). Returns
    {filename: figure}. NMA cells carry the endpoint range colour, CE>0 grey; a separator divides
    the AB strategy from TB."""
    frame = frames[HEADLINE_CLS]
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(2, 2, figsize=(10, 8))
        for ax, (col, label) in zip(axes.ravel(), eps):
            base = endpoint_color(col)
            violin_box_panel(ax, _cell_violin_groups(frame, col, base),
                             title=label, title_color=base, separators=(2.5,))
        fig.suptitle(f"Figure 8.2a — {gtitle}\nper-user means by delivery strategy × day type, "
                     "CE=0/BE≤∞ (NMA coloured, CE>0 grey)", fontsize=10)
        fig.tight_layout(rect=[0, 0, 1, 0.92])
        out[f"figure_8_2a_violin_{key}.png"] = fig
    return out


def make_interaction_grids(records):
    """Figure 8.2c: model-estimated marginal-mean interaction (strategy on x, NMA vs CE>0 lines)
    on the broadest classification (CE=0/BE≤∞), as the two shared 2×2 metric grids (all 8).
    Returns {filename: figure}. Degenerate fits show a note; NMA = endpoint colour, CE>0 = grey."""
    strat_names = [s for s, _ in STRATEGIES]
    strat_short = [sh for _, sh in STRATEGIES]
    by_ep = {r["endpoint"]: r for r in records if r["classification"] == HEADLINE_CLS}
    out = {}
    for key, gtitle, eps in GRIDS:
        fig, axes = plt.subplots(2, 2, figsize=(10, 8))
        for ax, (col, label) in zip(axes.ravel(), eps):
            base = endpoint_color(col)
            rec = by_ep.get(col)
            mc = rec["marginal_cells"] if rec else None
            if mc:
                for d, color in [(NMA_LABEL, base), (COMPARATOR_LABEL, GRAY)]:
                    ys = [mc.get((d, s), {}).get("mean", np.nan) for s in strat_names]
                    ax.plot(range(len(strat_names)), ys, marker="o", label=d, color=color)
                ax.legend(fontsize=7)
            else:
                ax.text(0.5, 0.5, "model not fit\n(degenerate)", ha="center", va="center",
                        transform=ax.transAxes, fontsize=9, color="gray")
            ax.set_xticks(range(len(strat_names)))
            ax.set_xticklabels(strat_short)
            ax.set_title(label, fontsize=10, color=base)
        fig.suptitle(f"Figure 8.2c — {gtitle}\nday-type × delivery-strategy interaction "
                     "(model marginal means), CE=0/BE≤∞", fontsize=10)
        fig.tight_layout(rect=[0, 0, 1, 0.92])
        out[f"figure_8_2c_interaction_{key}.png"] = fig
    return out


def make_figure_8_2d(frames):
    """Figure 8.2d: stacked bar of mean % time in each glycemic range per cell (strategy ×
    day_type), one subplot per classification. Per-user mean within cell, then averaged across
    users (equal weighting, as in §8.1)."""
    fig, axes = plt.subplots(1, len(CLASSIFICATIONS),
                             figsize=(6 * len(CLASSIFICATIONS), 7), squeeze=False, sharey=True)
    range_keys = [rc for rc, _ in RANGE_COLS]

    for ax, (nma_flag, cls_label) in zip(axes[0], CLASSIFICATIONS):
        frame = frames[cls_label].copy()
        frame["r_lt54"] = frame["tbr_very_low"]
        frame["r_54_70"] = frame["tbr"] - frame["tbr_very_low"]
        frame["r_70_180"] = frame["tir"]
        frame["r_180_250"] = frame["tar"] - frame["tar_very_high"]
        frame["r_gt250"] = frame["tar_very_high"]

        bar_labels, user_ns = [], []
        means = {rc: [] for rc in range_keys}
        for s_name, s_short in STRATEGIES:
            for d in [NMA_LABEL, COMPARATOR_LABEL]:
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
            bottom += vals
        ax.set_xticks(x)
        ax.set_xticklabels(bar_labels, fontsize=8)
        ax.set_ylim(0, 108)
        ax.set_title(cls_label, fontsize=10)
        for xi, u in enumerate(user_ns):
            ax.text(xi, 101, f"users={u}", ha="center", va="bottom", fontsize=7)

    axes[0][0].set_ylabel("Mean time in range (%)")
    axes[0][-1].legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left",
                       fontsize=8)
    fig.suptitle("Figure 8.2d: Mean time in glycemic ranges by classification × cell", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort: Literal["adult", "pediatric", "all"] = "all",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Run Analysis 8.2 for one age cohort. Outputs land in outputs/analysis_8_2/<cohort>/
    unless an explicit output_dir is given. `min_age` defaults to the §6 floor (MIN_AGE=6);
    pass None to disable.

    Source priority: an explicit `csv_path`, else `spark.table(analysis_ready_table)` when a
    Spark session is given, else (local, no Spark) the CSV snapshot that
    data_staging/export_user_day_analysis_ready.py writes — outputs/<table-name>.csv."""
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_2", cohort)
    # Clear this cohort's dir first so it reflects only the current run.
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

    # Build the per-classification NMA/CE>0 frames once; reuse for tables + figures.
    frames = {cls_label: build_day_type_frame(pdf, nma_flag)
              for nma_flag, cls_label in CLASSIFICATIONS}

    fits = fit_interaction_models(frames, nma_stats)
    table_b = build_table_8_2b(fits)
    print(table_b.to_string(index=False))
    table_b.to_csv(os.path.join(output_dir, "table_8_2b_interaction.csv"), index=False)
    build_table_8_2a(frames, fits).to_csv(
        os.path.join(output_dir, "table_8_2a_marginal_cells.csv"), index=False)

    # Figures (shared NMA conventions): 8.2a per-user violin grids (all 8, strategy × day type,
    # broadest arm); 8.2c interaction-marginal-mean grids (all 8, broadest arm); 8.2d stacked
    # glycemic ranges per cell across all three classifications.
    figures = {}
    figures.update(make_violin_grids(frames))       # figure_8_2a_violin_grid{1,2}_*.png
    figures.update(make_interaction_grids(fits))    # figure_8_2c_interaction_grid{1,2}_*.png
    figures["figure_8_2d_stacked_bars.png"] = make_figure_8_2d(frames)
    for fname, fig in figures.items():
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)

    print(f"wrote analysis 8.2 ({cohort}) outputs to {output_dir}")
    return table_b


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Orchestrate the §7.6 cohort split: adult and pediatric reported separately, plus a
    pooled `all` sanity run. Each lands in its own outputs/analysis_8_2/<cohort>/ dir."""
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path)


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
    _args, _ = _parser.parse_known_args()

    # Databricks injects a `spark` global; a local run has none.
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        _spark = None

    if _args.cohort is None:
        main(_spark, _args.analysis_ready_table, min_age=_args.min_age, csv_path=_args.csv_path)
    else:
        run(_spark, _args.analysis_ready_table, _args.output_dir,
            cohort=_args.cohort, min_age=_args.min_age, csv_path=_args.csv_path)
