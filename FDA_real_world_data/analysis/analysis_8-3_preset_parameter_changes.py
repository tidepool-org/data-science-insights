"""
=============================================================================
Analysis 8.3: Preset Parameter Changes
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Assess whether users modify preset parameters (scale factors and
glucose targets) when transitioning from temp basal to autobolus delivery.

Inputs:
- dev.fda_510k_rwd.overrides_by_segment   (one row per override event)
- dev.fda_510k_rwd.valid_transition_cbg   (_userId, cbg_timestamp, cbg_mg_dl)

Inclusion:
- Same preset string name activated in both temp basal and autobolus phases
  (is_valid_name_only = TRUE: preset name appears >= 2 times in each phase)
- Only override activations where starting glucose is 70–180 mg/dL

Methods:
- For each (user, preset), compute time-weighted average parameters in each
  dosing mode phase (weights = override duration)
- Paired statistical tests assess whether mean/median differences differ from zero

Outputs:
- Table 8.3a: Time-Weighted Preset Parameters by Delivery Strategy
- Figure 8.3a: Paired Changes in Preset Parameters
- Figure 8.3b: Correlation Between Parameter Changes
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from scipy.stats import pearsonr
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, Optional, Tuple
import os

# Configuration
OUTPUT_DIR = "outputs/analysis_8_3"
SEG1 = "temp_basal"
SEG2 = "autobolus"
STARTING_GLUCOSE_LOW  = 70    # mg/dL — inclusion criterion
STARTING_GLUCOSE_HIGH = 180   # mg/dL — inclusion criterion
CBG_LOOKBACK_MINUTES  = 30    # minutes before override_time to search for CBG

from utils.constants import FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT
from utils.statistics import test_normality, compute_paired_statistics, format_p

# =============================================================================
# Parameter definitions: (display_name, seg1_col, seg2_col, unit)
# =============================================================================
PARAMETERS = [
    ("Basal Rate Scale Factor",         "brsf_seg1",  "brsf_seg2",  "ratio"),
    ("Carb Ratio Scale Factor",         "crsf_seg1",  "crsf_seg2",  "ratio"),
    ("Insulin Sensitivity SF",          "issf_seg1",  "issf_seg2",  "ratio"),
    ("Glucose Target Midpoint (mg/dL)", "gtm_seg1",   "gtm_seg2",   "mg/dL"),
]



# =============================================================================
# Data Loading
# =============================================================================

def load_data(spark) -> pd.DataFrame:
    """
    Load eligible override events, join to the nearest pre-activation CBG
    for starting-glucose filtering, then compute time-weighted parameter
    averages per (user, preset, dosing_mode).

    Returns a wide DataFrame with one row per (_userId, overridePreset) pair
    that has paired values in both temp_basal and autobolus phases.
    """
    # ── Step 1: eligible override events ─────────────────────────────────────
    overrides_sdf = (
        spark.table("dev.fda_510k_rwd.overrides_by_segment")
        .filter(F.col("is_valid_name_only") == True)
        .select(
            "_userId", "override_time", "dosing_mode", "overridePreset",
            "basalRateScaleFactor", "carbRatioScaleFactor",
            "insulinSensitivityScaleFactor", "bg_target_low", "bg_target_high",
            "duration",
        )
    )

    # ── Step 2: join to nearest CBG within lookback window ────────────────────
    cbg_sdf = spark.table("dev.fda_510k_rwd.valid_transition_cbg").select(
        F.col("_userId").alias("_cbg_userId"),
        "cbg_timestamp",
        "cbg_mg_dl",
    )

    # Left-join: for each override, match CBG readings within the lookback window
    joined = overrides_sdf.join(
        cbg_sdf,
        on=(
            (overrides_sdf["_userId"] == cbg_sdf["_cbg_userId"]) &
            (cbg_sdf["cbg_timestamp"].between(
                overrides_sdf["override_time"] - F.expr(f"INTERVAL {CBG_LOOKBACK_MINUTES} MINUTES"),
                overrides_sdf["override_time"],
            ))
        ),
        how="left",
    ).drop("_cbg_userId")

    # Pick the single closest CBG per override event (most recent in window)
    w_cbg = Window.partitionBy(
        "_userId", "override_time", "overridePreset", "dosing_mode"
    ).orderBy(F.col("cbg_timestamp").desc_nulls_last())

    eligible = (
        joined
        .withColumn("_rn", F.row_number().over(w_cbg))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "cbg_timestamp")
    )

    # ── Step 3: apply starting-glucose inclusion filter ───────────────────────
    eligible = eligible.filter(
        F.col("cbg_mg_dl").between(STARTING_GLUCOSE_LOW, STARTING_GLUCOSE_HIGH)
    )

    df = eligible.toPandas()
    print(f"  Override events passing inclusion criteria: {len(df)}")

    # ── Step 4: cast parameters to numeric; compute derived columns ───────────
    for col in [
        "basalRateScaleFactor", "carbRatioScaleFactor",
        "insulinSensitivityScaleFactor", "bg_target_low", "bg_target_high",
        "duration",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["gtm"]  = (df["bg_target_low"] + df["bg_target_high"]) / 2.0
    df["brsf"] = df["basalRateScaleFactor"]
    df["crsf"] = df["carbRatioScaleFactor"]
    df["issf"] = df["insulinSensitivityScaleFactor"]

    # Duration weight — fall back to 1 if NULL or zero
    df["w"] = df["duration"].fillna(1.0).clip(lower=1.0)

    # ── Step 5: time-weighted aggregation per (user, preset, dosing_mode) ─────
    param_cols = ["brsf", "crsf", "issf", "gtm"]
    GROUP_COLS = ["_userId", "overridePreset", "dosing_mode"]

    agg_rows = []
    for keys, g in df.groupby(GROUP_COLS):
        row = dict(zip(GROUP_COLS, keys))
        for col in param_cols:
            mask = g[col].notna()
            if mask.sum() == 0:
                row[col] = np.nan
            else:
                row[col] = (g.loc[mask, col] * g.loc[mask, "w"]).sum() / g.loc[mask, "w"].sum()
        row["n_events"] = len(g)
        agg_rows.append(row)

    agg = pd.DataFrame(agg_rows)

    # ── Step 6: pivot to wide — one row per (user, preset) ───────────────────
    seg1_df = (
        agg[agg["dosing_mode"] == SEG1]
        .set_index(["_userId", "overridePreset"])
        [param_cols + ["n_events"]]
        .add_suffix("_seg1")
    )
    seg2_df = (
        agg[agg["dosing_mode"] == SEG2]
        .set_index(["_userId", "overridePreset"])
        [param_cols + ["n_events"]]
        .add_suffix("_seg2")
    )

    wide = seg1_df.join(seg2_df, how="inner").reset_index()
    print(f"  Paired (user, preset) combinations: {len(wide)}")

    return wide


# =============================================================================
# Table 8.3a
# =============================================================================

def create_table_8_3a(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    parametric_rows = []
    nonparametric_rows = []

    for name, c1, c2, _ in PARAMETERS:
        if c1 not in df.columns or c2 not in df.columns:
            print(f"  Warning: {c1} or {c2} not found, skipping {name}")
            continue
        s = compute_paired_statistics(df[c1], df[c2])

        p_t = format_p(s["p_ttest"])
        p_w = format_p(s["p_wsrt"])

        ci_str = (
            f"({s['diff_ci_low']:.3f}, {s['diff_ci_hi']:.3f})"
            if not np.isnan(s["diff_ci_low"]) else "N/A"
        )

        parametric_rows.append({
            "Parameter": name,
            "Temp Basal Mean ± SD":          f"{s['seg1_mean']:.3f} ± {s['seg1_sd']:.3f}",
            "Autobolus Mean ± SD":           f"{s['seg2_mean']:.3f} ± {s['seg2_sd']:.3f}",
            "Paired Diff Mean ± SD (95% CI)": f"{s['diff_mean']:.3f} ± {s['diff_sd']:.3f} {ci_str}",
            "p (paired t-test)": p_t,
            "N": s["n_pairs"],
        })

        nonparametric_rows.append({
            "Parameter": name,
            "Temp Basal Median [IQR]":  f"{s['seg1_median']:.3f} [{s['seg1_q1']:.3f}, {s['seg1_q3']:.3f}]",
            "Autobolus Median [IQR]":   f"{s['seg2_median']:.3f} [{s['seg2_q1']:.3f}, {s['seg2_q3']:.3f}]",
            "Paired Diff Median [IQR]": f"{s['diff_median']:.3f} [{s['diff_q1']:.3f}, {s['diff_q3']:.3f}]",
            "p (Wilcoxon signed-rank)": p_w,
            "Normality (Shapiro p)": f"{s['normality_p']:.2e}" if not np.isnan(s["normality_p"]) else "N/A",
            "N": s["n_pairs"],
        })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.3a — Paired dot + violin for each parameter
# =============================================================================

def create_figure_8_3a(df: pd.DataFrame, output_path: str):
    fig, axes = plt.subplots(2, 2, figsize=(12, 12))
    labels = [f"({chr(97 + i)})" for i in range(len(PARAMETERS))]

    for idx, (ax, (title, c1, c2, unit)) in enumerate(zip(axes.flatten(), PARAMETERS)):
        valid = df[c1].notna() & df[c2].notna()
        v1, v2 = df.loc[valid, c1].values, df.loc[valid, c2].values

        if len(v1) == 0:
            ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                    fontsize=FONT["annotation"])
            ax.set_title(f"{labels[idx]} {title}", fontweight="bold",
                         fontsize=FONT["title"])
            continue

        s = compute_paired_statistics(df[c1], df[c2])
        p_str = f"t: {format_p(s['p_ttest'])}  WSRT: {format_p(s['p_wsrt'])}"

        for s1_val, s2_val in zip(v1, v2):
            ax.plot([0, 1], [s1_val, s2_val], "o-", color="gray", alpha=0.3, lw=0.5, ms=3)

        bp = ax.boxplot([v1, v2], positions=[0, 1], widths=0.3,
                        patch_artist=True, showfliers=False)
        bp["boxes"][0].set(facecolor=COLORS_PRIMARY, alpha=0.7)
        bp["boxes"][1].set(facecolor=COLORS_SECONDARY, alpha=0.7)

        if len(v1) > 1 and len(v2) > 1:
            parts = ax.violinplot([v1, v2], positions=[0, 1],
                                  showmeans=False, showmedians=False, widths=0.5)
            for i, pc in enumerate(parts["bodies"]):
                pc.set_facecolor(COLORS_PRIMARY if i == 0 else COLORS_SECONDARY)
                pc.set_alpha(0.3)

        ax.set_xticks([0, 1])
        ax.set_xticklabels(["Temp Basal", "Autobolus"], fontsize=FONT["axis_label"])
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.set_ylabel(unit, fontsize=FONT["axis_label"])
        ax.set_title(f"{labels[idx]} {title}\n{p_str}",
                     fontsize=FONT["title"], fontweight="bold")
        ax.grid(axis="y", alpha=0.3)

    plt.suptitle(
        "Figure 8.3a: Paired Changes in Preset Parameters\n"
        "Individual (user × preset) pairs connected by lines; box + violin overlay",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


# =============================================================================
# Figure 8.3b — Histograms of paired differences
# =============================================================================

def create_figure_8_3b(df: pd.DataFrame, output_path: str):
    labels = [f"({chr(97 + i)})" for i in range(len(PARAMETERS))]

    fig, axes = plt.subplots(2, 2, figsize=(12, 12))

    for idx, (ax, (title, c1, c2, _)) in enumerate(zip(axes.flatten(), PARAMETERS)):
        valid = df[c1].notna() & df[c2].notna()
        diff = df.loc[valid, c2] - df.loc[valid, c1]

        if len(diff) == 0:
            ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                    fontsize=FONT["annotation"])
            ax.set_title(f"{labels[idx]} {title}", fontweight="bold",
                         fontsize=FONT["title"])
            continue

        s = compute_paired_statistics(df[c1], df[c2])
        p_str = f"t: {format_p(s['p_ttest'])}  WSRT: {format_p(s['p_wsrt'])}"

        ax.hist(diff, bins=20, edgecolor="black", alpha=0.7, color=COLORS_PRIMARY)
        ax.axvline(0, color=COLORS_ACCENT, ls="--", lw=2, label="Zero")
        ax.axvline(diff.mean(), color="#8B0000", ls="-", lw=2,
                   label=f"Mean: {diff.mean():.3f}")

        ax.set_xlabel("Δ (Autobolus − Temp Basal)", fontsize=FONT["axis_label"])
        ax.set_ylabel("Number of Pairs", fontsize=FONT["axis_label"])
        ax.tick_params(axis="both", labelsize=FONT["tick"])
        ax.set_title(f"{labels[idx]} {title}\n{p_str}",
                     fontsize=FONT["title"], fontweight="bold")
        ax.legend(fontsize=FONT["legend"])
        ax.grid(axis="y", alpha=0.3)

    plt.suptitle(
        "Figure 8.3b: Distribution of Paired Differences in Preset Parameters",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


# =============================================================================
# Figure 8.3c — Correlation between parameter changes (pairwise scatterplots)
# =============================================================================

def create_figure_8_3c(df: pd.DataFrame, output_path: str):
    """
    6-panel (2×3) grid of pairwise scatterplots of paired Δ values
    (Δ = autobolus − temp_basal). Each panel includes an OLS regression
    line and Pearson r with p-value annotation.
    """
    # Compute deltas
    delta_cols   = []
    delta_labels = []
    for name, c1, c2, _ in PARAMETERS:
        col = f"delta_{c1.replace('_seg1', '')}"
        valid = df[c1].notna() & df[c2].notna()
        df[col] = np.where(valid, df[c2] - df[c1], np.nan)
        delta_cols.append(col)
        delta_labels.append(f"Δ {name}")

    # All 6 unique pairwise combinations
    pairs = [(i, j) for i in range(len(delta_cols)) for j in range(i + 1, len(delta_cols))]

    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()

    for ax, (i, j) in zip(axes, pairs):
        xcol, ycol   = delta_cols[i],   delta_cols[j]
        xlabel, ylabel = delta_labels[i], delta_labels[j]

        valid = df[xcol].notna() & df[ycol].notna()
        x = df.loc[valid, xcol].values
        y = df.loc[valid, ycol].values

        ax.scatter(x, y, color=COLORS_PRIMARY, alpha=0.5, s=20)
        ax.axhline(0, color="lightgray", lw=0.8, ls="--")
        ax.axvline(0, color="lightgray", lw=0.8, ls="--")

        if len(x) >= 3:
            m, b = np.polyfit(x, y, 1)
            x_line = np.linspace(x.min(), x.max(), 100)
            ax.plot(x_line, m * x_line + b, color=COLORS_ACCENT, lw=1.5)
            r, p_r = pearsonr(x, y)
            annot = f"r={r:.2f}, {format_p(p_r)}\nn={len(x)}"
        else:
            annot = f"n={len(x)} (insufficient)"

        ax.text(0.05, 0.95, annot, transform=ax.transAxes, va="top",
                fontsize=FONT["annotation"], color=COLORS_ACCENT)
        ax.set_xlabel(xlabel, fontsize=FONT["axis_label"])
        ax.set_ylabel(ylabel, fontsize=FONT["axis_label"])
        ax.tick_params(axis="both", labelsize=FONT["tick"])
        ax.grid(alpha=0.3)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.suptitle(
        "Figure 8.3c: Correlation Between Preset Parameter Changes\n"
        "Δ = Autobolus − Temp Basal  (per user × preset pair)",
        fontsize=FONT["suptitle"], fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.3: Preset Parameter Changes")
    print("=" * 60)

    print("\n1. Loading data...")
    df = load_data(spark)
    print(f"   {len(df)} paired (user × preset) combinations")

    print("\n2. Creating Table 8.3a...")
    table_parametric, table_nonparametric = create_table_8_3a(df)
    table_parametric.to_csv(f"{output_dir}/table_8_3a_parametric.csv", index=False)
    table_nonparametric.to_csv(f"{output_dir}/table_8_3a_nonparametric.csv", index=False)
    print("\n" + table_parametric.to_string(index=False))

    print("\n3. Creating Figure 8.3a...")
    create_figure_8_3a(df, f"{output_dir}/figure_8_3a_paired_parameters.png")

    print("\n4. Creating Figure 8.3b...")
    create_figure_8_3b(df, f"{output_dir}/figure_8_3b_diff_histograms.png")

    print("\n5. Creating Figure 8.3c...")
    create_figure_8_3c(df, f"{output_dir}/figure_8_3c_parameter_correlations.png")

    print("\n" + "=" * 60)
    print("Analysis 8.3 Complete!")
    print("=" * 60)

    return {
        "table_parametric": table_parametric,
        "table_nonparametric": table_nonparametric,
        "df": df,
    }


def run_in_databricks(spark):
    return run_analysis(spark)

run_in_databricks(spark) # type: ignore[name-defined]
