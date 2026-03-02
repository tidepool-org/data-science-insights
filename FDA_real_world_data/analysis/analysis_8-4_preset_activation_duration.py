"""
=============================================================================
Analysis 8.4: Preset Activation Duration
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Assess whether the amount of time users have a preset activated
changes between temporary basal and autobolus dosing strategies.

Inputs:
- dev.fda_510k_rwd.overrides_by_segment       (one row per override event)
- dev.fda_510k_rwd.valid_transition_segments  (all eligible transition users)

Methods:
- For each user, sum total preset activation duration and count activations
  in each 14-day segment
- Users with no preset activations in a segment contribute 0 (not excluded)
- Paired statistical tests for total duration, frequency, mean duration/activation

Outputs:
- Table 8.4a: Preset Activation Duration by Delivery Strategy
- Figure 8.4a: Paired Comparison of Preset Duration (raincloud + diff histogram)
- Figure 8.4b: Distribution of Preset Usage (side-by-side violin plots)
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from typing import Dict, Optional, Tuple
import os

# Configuration
OUTPUT_DIR = "outputs/analysis_8_4"
SEG1 = "temp_basal"
SEG2 = "autobolus"
S_PER_HOUR = 3_600   # override duration is stored in seconds

from utils.constants import FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT
from utils.statistics import test_normality, compute_paired_statistics, format_p


# =============================================================================
# Data Loading
# =============================================================================

def load_data(spark) -> pd.DataFrame:
    """
    Load all transition users and aggregate their preset activation totals
    per 14-day segment. Users with no activations in a segment receive 0
    for duration and frequency so they are included in paired comparisons.
    """
    # All eligible transition users (provides the complete user list)
    users_df = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .select("_userId")
        .toPandas()
    )

    # All override events — no is_valid filter; we want total preset activity
    overrides_df = (
        spark.table("dev.fda_510k_rwd.overrides_by_segment")
        .select("_userId", "dosing_mode", "duration")
        .toPandas()
    )

    overrides_df["duration"] = pd.to_numeric(overrides_df["duration"], errors="coerce")
    overrides_df["duration_hr"] = overrides_df["duration"] / S_PER_HOUR

    # Aggregate per (user, dosing_mode)
    agg = (
        overrides_df
        .groupby(["_userId", "dosing_mode"])
        .agg(
            total_duration_hr=("duration_hr", "sum"),
            n_activations=("duration_hr", "count"),
        )
        .reset_index()
    )
    agg["mean_duration_hr"] = agg["total_duration_hr"] / agg["n_activations"]

    # Pivot to wide
    seg1_agg = (
        agg[agg["dosing_mode"] == SEG1]
        .set_index("_userId")[["total_duration_hr", "n_activations", "mean_duration_hr"]]
        .rename(columns=lambda c: f"{c}_seg1")
    )
    seg2_agg = (
        agg[agg["dosing_mode"] == SEG2]
        .set_index("_userId")[["total_duration_hr", "n_activations", "mean_duration_hr"]]
        .rename(columns=lambda c: f"{c}_seg2")
    )

    # Left join from complete user list — preserves users with 0 preset use
    wide = (
        users_df.set_index("_userId")
        .join(seg1_agg, how="left")
        .join(seg2_agg, how="left")
        .reset_index()
    )

    # 0-fill duration and frequency; mean_duration_hr stays NaN when n==0
    for col in ["total_duration_hr_seg1", "total_duration_hr_seg2",
                "n_activations_seg1",     "n_activations_seg2"]:
        wide[col] = wide[col].fillna(0)

    n = len(wide)
    print(f"  Transition users: {n}")
    print(f"  Any preset use — Temp Basal: {(wide['n_activations_seg1'] > 0).sum()}")
    print(f"  Any preset use — Autobolus:  {(wide['n_activations_seg2'] > 0).sum()}")
    return wide


# =============================================================================
# Table 8.4a
# =============================================================================

OUTCOMES = [
    ("Total preset duration (hours/14 days)",    "total_duration_hr", 1, "Hours"),
    ("Preset activation frequency (n/14 days)",  "n_activations",     0, "Count"),
    ("Mean duration per activation (hours)",     "mean_duration_hr",  1, "Hours"),
]


def create_table_8_4a(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    n = len(df)
    parametric_rows    = []
    nonparametric_rows = []

    for label, col, dp, _ in OUTCOMES:
        c1, c2 = f"{col}_seg1", f"{col}_seg2"
        if c1 not in df.columns or c2 not in df.columns:
            print(f"  Warning: {c1} or {c2} not found — skipping {label}")
            continue
        s = compute_paired_statistics(df[c1], df[c2])

        p_t = format_p(s["p_ttest"])
        p_w = format_p(s["p_wsrt"])
        ci_str = (
            f"({s['diff_ci_low']:.{dp}f}, {s['diff_ci_hi']:.{dp}f})"
            if not np.isnan(s["diff_ci_low"]) else "N/A"
        )

        parametric_rows.append({
            "Outcome": label,
            "Temp Basal Mean ± SD":           f"{s['seg1_mean']:.{dp}f} ± {s['seg1_sd']:.{dp}f}",
            "Autobolus Mean ± SD":            f"{s['seg2_mean']:.{dp}f} ± {s['seg2_sd']:.{dp}f}",
            "Paired Diff Mean ± SD (95% CI)": f"{s['diff_mean']:.{dp}f} ± {s['diff_sd']:.{dp}f} {ci_str}",
            "p (paired t-test)": p_t,
            "N": s["n_pairs"],
        })
        nonparametric_rows.append({
            "Outcome": label,
            "Temp Basal Median [IQR]":  f"{s['seg1_median']:.{dp}f} [{s['seg1_q1']:.{dp}f}, {s['seg1_q3']:.{dp}f}]",
            "Autobolus Median [IQR]":   f"{s['seg2_median']:.{dp}f} [{s['seg2_q1']:.{dp}f}, {s['seg2_q3']:.{dp}f}]",
            "Paired Diff Median [IQR]": f"{s['diff_median']:.{dp}f} [{s['diff_q1']:.{dp}f}, {s['diff_q3']:.{dp}f}]",
            "p (Wilcoxon signed-rank)": p_w,
            "N": s["n_pairs"],
        })

    # Users with any preset use — count row, no paired test
    n_tb = int((df["n_activations_seg1"] > 0).sum())
    n_ab = int((df["n_activations_seg2"] > 0).sum())
    for rows in (parametric_rows, nonparametric_rows):
        rows.append({
            "Outcome": "Users with any preset use, n (%)",
            list(rows[0].keys())[1]: f"{n_tb} ({100 * n_tb / n:.1f}%)",
            list(rows[0].keys())[2]: f"{n_ab} ({100 * n_ab / n:.1f}%)",
            list(rows[0].keys())[3]: "—",
            list(rows[0].keys())[4]: "—",
            "N": n,
        })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.4a — Paired dot + symmetric violin + difference histogram
# =============================================================================

def create_figure_8_4a(df: pd.DataFrame, output_path: str):
    labels = [f"({chr(97 + i)})" for i in range(len(OUTCOMES))]
    fig, axes = plt.subplots(1, 3, figsize=(15, 6))

    for idx, (ax, (title, col, _, unit)) in enumerate(zip(axes, OUTCOMES)):
        c1, c2 = f"{col}_seg1", f"{col}_seg2"
        valid = df[c1].notna() & df[c2].notna()
        v1 = df.loc[valid, c1].values
        v2 = df.loc[valid, c2].values

        if len(v1) == 0:
            ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                    fontsize=FONT["annotation"])
            ax.set_title(f"{labels[idx]} {title}", fontweight="bold",
                         fontsize=FONT["title"])
            continue

        s     = compute_paired_statistics(df[c1], df[c2])
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
        "Figure 8.4a: Paired Comparison of Preset Activation Duration\n"
        "Individual users connected by lines; box + violin overlay",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


# =============================================================================
# Figure 8.4b — Histograms of paired differences for all three outcomes
# =============================================================================

def create_figure_8_4b(df: pd.DataFrame, output_path: str):
    labels = [f"({chr(97 + i)})" for i in range(len(OUTCOMES))]
    fig, axes = plt.subplots(1, 3, figsize=(15, 6))

    for idx, (ax, (title, col, _, unit)) in enumerate(zip(axes, OUTCOMES)):
        c1, c2 = f"{col}_seg1", f"{col}_seg2"
        valid = df[c1].notna() & df[c2].notna()
        diff  = df.loc[valid, c2] - df.loc[valid, c1]

        if len(diff) == 0:
            ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                    fontsize=FONT["annotation"])
            ax.set_title(f"{labels[idx]} {title}", fontweight="bold",
                         fontsize=FONT["title"])
            continue

        s     = compute_paired_statistics(df[c1], df[c2])
        p_str = f"t: {format_p(s['p_ttest'])}  WSRT: {format_p(s['p_wsrt'])}"

        ax.hist(diff, bins=20, edgecolor="black", alpha=0.7, color=COLORS_PRIMARY)
        ax.axvline(0, color=COLORS_ACCENT, ls="--", lw=2, label="Zero")
        ax.axvline(diff.mean(), color="#8B0000", ls="-", lw=2,
                   label=f"Mean: {diff.mean():.1f}")

        ax.set_xlabel(f"Δ (Autobolus − Temp Basal, {unit})", fontsize=FONT["axis_label"])
        ax.set_ylabel("Number of Users", fontsize=FONT["axis_label"])
        ax.tick_params(axis="both", labelsize=FONT["tick"])
        ax.set_title(f"{labels[idx]} {title}\n{p_str}",
                     fontsize=FONT["title"], fontweight="bold")
        ax.legend(fontsize=FONT["legend"])
        ax.grid(axis="y", alpha=0.3)

    plt.suptitle("Figure 8.4b: Distribution of Paired Differences in Preset Activation Duration",
                 fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
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
    print("Analysis 8.4: Preset Activation Duration")
    print("=" * 60)

    print("\n1. Loading data...")
    df = load_data(spark)
    print(f"   {len(df)} transition users")

    print("\n2. Creating Table 8.4a...")
    table_parametric, table_nonparametric = create_table_8_4a(df)
    table_parametric.to_csv(f"{output_dir}/table_8_4a_parametric.csv", index=False)
    table_nonparametric.to_csv(f"{output_dir}/table_8_4a_nonparametric.csv", index=False)
    print("\n" + table_parametric.to_string(index=False))

    print("\n3. Creating Figure 8.4a...")
    create_figure_8_4a(df, f"{output_dir}/figure_8_4a_paired_duration.png")

    print("\n4. Creating Figure 8.4b...")
    create_figure_8_4b(df, f"{output_dir}/figure_8_4b_distribution.png")

    print("\n" + "=" * 60)
    print("Analysis 8.4 Complete!")
    print("=" * 60)

    return {
        "table_parametric":    table_parametric,
        "table_nonparametric": table_nonparametric,
        "df": df,
    }


def run_in_databricks(spark):
    return run_analysis(spark)

run_in_databricks(spark) # type: ignore[name-defined]
