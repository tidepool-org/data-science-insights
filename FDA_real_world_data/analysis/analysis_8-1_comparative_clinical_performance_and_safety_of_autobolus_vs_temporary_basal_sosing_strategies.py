"""
=============================================================================
Analysis 8.1: Performance and Safety of 40% PAF
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Evaluate the performance and safety of the 40% PAF parameter.

Inputs:
- dev.fda_510k_rwd.glycemic_endpoints_transition  (long: _userId, segment, tir, tbr, ..., hypo_events)
- dev.fda_510k_rwd.valid_transition_guardrails     (_userId, violation_count)

Outputs:
- Table 8.1a: Glycemic Endpoints by Delivery Strategy
- Figure 8.1a: Paired Differences (paired dot + violin)
- Figure 8.1b: Distribution of Paired Differences (histograms)
- Figure 8.1c: Time in Ranges Stacked Bar Comparison
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from typing import Dict, Optional, Tuple
import os

# Configuration
USE_PARAMETRIC = True
NORMALITY_ALPHA = 0.05
OUTPUT_DIR = "outputs/analysis_8_1"

SEG1 = "tb_to_ab_seg1"  # temp basal period
SEG2 = "tb_to_ab_seg2"  # autobolus period

# =============================================================================
# Font sizes — centralized so every figure stays consistent
# =============================================================================
FONT = {
    "suptitle":   18,   # figure-level title
    "title":      15,   # subplot / axis title
    "axis_label": 14,   # xlabel / ylabel
    "tick":       12,   # tick labels
    "legend":     11,   # legend entries
    "annotation": 13,   # in-bar percentages, mean labels, etc.
}

# Color scheme
COLORS_STACKED_BAR = {
    "<54":     ["#8C65D6", "#8C65D6"],
    "54-70":   ["#BB9AE7", "#BB9AE7"],
    "70-180":  ["#76D3A6", "#76D3A6"],
    "180-250": ["#FF8B7C", "#FF8B7C"],
    ">250":    ["#FB5951", "#FB5951"],
}
COLORS_PRIMARY = "#607cff"
COLORS_SECONDARY = "#4f59be"
COLORS_ACCENT = "#241144"


# =============================================================================
# Endpoint definitions: (display_name, seg1_col, seg2_col, unit)
# =============================================================================

ENDPOINTS = [
    ("Time <54 mg/dL (%)",            "tbr_very_low_seg1", "tbr_very_low_seg2", "%"),
    ("Time <70 mg/dL (%)",            "tbr_seg1",          "tbr_seg2",          "%"),
    ("Time 70–180 mg/dL (%)",         "tir_seg1",          "tir_seg2",          "%"),
    ("Time >180 mg/dL (%)",           "tar_seg1",          "tar_seg2",          "%"),
    ("Time >250 mg/dL (%)",           "tar_very_high_seg1","tar_very_high_seg2","%"),
    ("Hypoglycemic events (n/14 d)",  "hypo_events_seg1",  "hypo_events_seg2",  "n"),
    ("Mean glucose (mg/dL)",          "mean_glucose_seg1",  "mean_glucose_seg2", "mg/dL"),
    ("Coefficient of variation (%)",  "cv_seg1",            "cv_seg2",           "%"),
]


# =============================================================================
# Data Loading
# =============================================================================

def load_data(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints (including hypo events) and pivot into a single
    wide DataFrame with one row per user and paired seg1/seg2 columns.

    Excludes users with any pump settings guardrail violations.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_transition").toPandas()
    guardrails = spark.table("dev.fda_510k_rwd.valid_transition_guardrails") \
        .select("_userId", "violation_count") \
        .toPandas()

    # Exclude segments with insufficient CBG coverage
    endpoints = endpoints.loc[endpoints["cbg_count"] >= 14 * 288 * 0.7]

    # Users with any guardrail violation
    guardrails["violation_count"] = pd.to_numeric(guardrails["violation_count"], errors="coerce").fillna(0)
    excluded_users = guardrails.loc[guardrails["violation_count"] > 0, "_userId"].unique()

    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "segment"):
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Exclude guardrail violators
    endpoints = endpoints[~endpoints["_userId"].isin(excluded_users)]

    seg1 = endpoints[endpoints["segment"] == SEG1].set_index("_userId").add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index("_userId").add_suffix("_seg2")

    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(columns=[c for c in wide.columns if c.startswith("segment")], errors="ignore")

    print(f"  Excluded {len(excluded_users)} users with guardrail violations")

    return wide.reset_index()


# =============================================================================
# Statistics
# =============================================================================

def test_normality(data: pd.Series, alpha: float = NORMALITY_ALPHA) -> Tuple[bool, float]:
    if len(data.dropna()) < 3:
        return False, np.nan
    _, p = stats.shapiro(data.dropna())
    return p > alpha, p


def compute_paired_statistics(
    seg1: pd.Series, seg2: pd.Series, use_parametric: Optional[bool] = None
) -> Dict:
    valid = seg1.notna() & seg2.notna()
    s1, s2 = seg1[valid], seg2[valid]
    diff = s2 - s1

    is_normal, norm_p = test_normality(diff)
    if use_parametric is None:
        use_parametric = is_normal

    def _summary(x):
        return x.mean(), x.std(), x.median(), x.quantile(0.25), x.quantile(0.75)

    s1_mean, s1_sd, s1_med, s1_q1, s1_q3 = _summary(s1)
    s2_mean, s2_sd, s2_med, s2_q1, s2_q3 = _summary(s2)
    d_mean, d_sd, d_med, d_q1, d_q3 = _summary(diff)

    # Paired t-test
    if len(diff) >= 3:
        _, p_ttest = stats.ttest_rel(s1, s2)
    else:
        p_ttest = np.nan

    # Wilcoxon signed-rank (paired, non-parametric)
    if len(diff) >= 3:
        _, p_wsrt = stats.wilcoxon(s1, s2)
    else:
        p_wsrt = np.nan

    return {
        "seg1_mean": s1_mean, "seg1_sd": s1_sd,
        "seg1_median": s1_med, "seg1_iqr": f"[{s1_q1:.1f}, {s1_q3:.1f}]",
        "seg2_mean": s2_mean, "seg2_sd": s2_sd,
        "seg2_median": s2_med, "seg2_iqr": f"[{s2_q1:.1f}, {s2_q3:.1f}]",
        "diff_mean": d_mean, "diff_sd": d_sd,
        "diff_median": d_med, "diff_iqr": f"[{d_q1:.1f}, {d_q3:.1f}]",
        "p_ttest": p_ttest, "p_wsrt": p_wsrt,
        "normality_p": norm_p, "is_normal": is_normal,
        "n_pairs": len(diff),
    }


def _format_p(p: float) -> str:
    """Format p-value for display."""
    if np.isnan(p):
        return "N/A"
    if p < 0.001:
        return f"p={p:.2e}"
    return f"p={p:.3f}"


# =============================================================================
# Table 8.1a
# =============================================================================

def create_table_8_1a(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    parametric_rows = []
    nonparametric_rows = []

    for name, c1, c2, _ in ENDPOINTS:
        if c1 not in df.columns or c2 not in df.columns:
            print(f"  Warning: {c1} or {c2} not found, skipping {name}")
            continue
        s = compute_paired_statistics(df[c1], df[c2])
        
        p_t = _format_p(s['p_ttest'])
        p_w = _format_p(s['p_wsrt'])
        
        parametric_rows.append({
            "Endpoint": name,
            "Temp Basal Mean ± SD":  f"{s['seg1_mean']:.2f} ± {s['seg1_sd']:.2f}",
            "Autobolus Mean ± SD":   f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
            "Paired Diff Mean ± SD": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
            "p (paired t-test)": p_t,
            "N": s["n_pairs"],
        })

        nonparametric_rows.append({
            "Endpoint": name,
            "Temp Basal Median [IQR]":  f"{s['seg1_median']:.2f} {s['seg1_iqr']}",
            "Autobolus Median [IQR]":   f"{s['seg2_median']:.2f} {s['seg2_iqr']}",
            "Paired Diff Median [IQR]": f"{s['diff_median']:.2f} {s['diff_iqr']}",
            "p (Wilcoxon signed-rank)": p_w,
            "Normality (Shapiro p)": f"{s['normality_p']:.2e}" if not np.isnan(s['normality_p']) else "N/A",
            "N": s["n_pairs"],
        })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.1a — Paired dot + violin with p-values in titles
# =============================================================================

def create_figure_8_1a(df: pd.DataFrame, output_path: str):
    labels = [f"({chr(97 + i)})" for i in range(len(ENDPOINTS))]

    for part, row_slice in enumerate([slice(0, 4), slice(4, 8)]):
        fig, axes = plt.subplots(2, 2, figsize=(12, 12))
        endpoints_subset = ENDPOINTS[row_slice]
        labels_subset = labels[row_slice]

        for idx, (ax, (title, c1, c2, unit)) in enumerate(zip(axes.flatten(), endpoints_subset)):
            valid = df[c1].notna() & df[c2].notna()
            v1, v2 = df.loc[valid, c1].values, df.loc[valid, c2].values

            if len(v1) == 0:
                ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                        fontsize=FONT["annotation"])
                ax.set_title(f"{labels_subset[idx]} {title}", fontweight="bold",
                             fontsize=FONT["title"])
                continue

            s = compute_paired_statistics(df[c1], df[c2])
            p_str = f"t: {_format_p(s['p_ttest'])}  WSRT: {_format_p(s['p_wsrt'])}"

            for s1, s2 in zip(v1, v2):
                ax.plot([0, 1], [s1, s2], "o-", color="gray", alpha=0.3, lw=0.5, ms=3)

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
            ax.set_title(f"{labels_subset[idx]} {title}\n{p_str}",
                         fontsize=FONT["title"], fontweight="bold")
            ax.grid(axis="y", alpha=0.3)

        part_label = "i" if part == 0 else "ii"
        plt.suptitle(f"Figure 8.1a ({part_label}): Paired Differences in Glycemic Endpoints\n"
                     "Individual users connected by lines; box + violin overlay",
                     fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
        plt.tight_layout()

        path = output_path.replace(".png", f"_{part_label}.png")
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {path}")

# =============================================================================
# Figure 8.1b — Histograms of paired differences with p-values in titles
# =============================================================================

def create_figure_8_1b(df: pd.DataFrame, output_path: str):
    labels = [f"({chr(97 + i)})" for i in range(len(ENDPOINTS))]

    for part, row_slice in enumerate([slice(0, 4), slice(4, 8)]):
        fig, axes = plt.subplots(2, 2, figsize=(12, 12))
        endpoints_subset = ENDPOINTS[row_slice]
        labels_subset = labels[row_slice]

        for idx, (ax, (title, c1, c2, _)) in enumerate(zip(axes.flatten(), endpoints_subset)):
            valid = df[c1].notna() & df[c2].notna()
            diff = df.loc[valid, c2] - df.loc[valid, c1]

            if len(diff) == 0:
                ax.text(0.5, 0.5, "No valid data", ha="center", va="center",
                        fontsize=FONT["annotation"])
                ax.set_title(f"{labels_subset[idx]} {title}", fontweight="bold",
                             fontsize=FONT["title"])
                continue

            s = compute_paired_statistics(df[c1], df[c2])
            p_str = f"t: {_format_p(s['p_ttest'])}  WSRT: {_format_p(s['p_wsrt'])}"

            ax.hist(diff, bins=20, edgecolor="black", alpha=0.7, color=COLORS_PRIMARY)
            ax.axvline(0, color=COLORS_ACCENT, ls="--", lw=2, label="Zero")
            ax.axvline(diff.mean(), color='#8B0000', ls="-", lw=2,
                       label=f"Mean: {diff.mean():.1f}")

            ax.set_xlabel("Δ (Autobolus − Temp Basal)", fontsize=FONT["axis_label"])
            ax.set_ylabel("Number of Users", fontsize=FONT["axis_label"])
            ax.tick_params(axis="both", labelsize=FONT["tick"])
            ax.set_title(f"{labels_subset[idx]} {title}\n{p_str}",
                         fontsize=FONT["title"], fontweight="bold")
            ax.legend(fontsize=FONT["legend"])
            ax.grid(axis="y", alpha=0.3)

        part_label = "i" if part == 0 else "ii"
        plt.suptitle(f"Figure 8.1b ({part_label}): Distribution of Paired Differences in Glycemic Endpoints",
                     fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
        plt.tight_layout()

        path = output_path.replace(".png", f"_{part_label}.png")
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {path}")


# =============================================================================
# Figure 8.1c — Stacked bar with matched colors and flipped legend
# =============================================================================

def create_figure_8_1c(df: pd.DataFrame, output_path: str):
    # Use paired-complete means to match table values
    range_pairs = [
        ("<54",     "tbr_very_low_seg1", "tbr_very_low_seg2"),
        ("<70",     "tbr_seg1",          "tbr_seg2"),
        ("70-180",  "tir_seg1",          "tir_seg2"),
        (">180",     "tar_seg1",          "tar_seg2"),
        (">250",    "tar_very_high_seg1", "tar_very_high_seg2"),
    ]
    paired = {key: compute_paired_statistics(df[c1], df[c2]) for key, c1, c2 in range_pairs}

    seg1 = {
        "<54":     paired["<54"]["seg1_mean"],
        "54-70":   paired["<70"]["seg1_mean"] - paired["<54"]["seg1_mean"],
        "70-180":  paired["70-180"]["seg1_mean"],
        "180-250": paired[">180"]["seg1_mean"] - paired[">250"]["seg1_mean"],
        ">250":    paired[">250"]["seg1_mean"],
    }
    seg2 = {
        "<54":     paired["<54"]["seg2_mean"],
        "54-70":   paired["<70"]["seg2_mean"] - paired["<54"]["seg2_mean"],
        "70-180":  paired["70-180"]["seg2_mean"],
        "180-250": paired[">180"]["seg2_mean"] - paired[">250"]["seg2_mean"],
        ">250":    paired[">250"]["seg2_mean"],
    }

    fig, ax = plt.subplots(figsize=(10, 8))
    categories = ["<54", "54-70", "70-180", "180-250", ">250"]
    range_labels = ["<54 mg/dL\n(Very Low)", "54-70 mg/dL\n(Low)",
                    "70-180 mg/dL\n(In Range)", "180-250 mg/dL\n(High)",
                    ">250 mg/dL\n(Very High)"]
    width = 0.5
    bot1 = bot2 = 0

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    
    for cat, label in zip(categories, range_labels):
        v1, v2 = max(0, seg1[cat]), max(0, seg2[cat])
        c_tb, c_ab = COLORS_STACKED_BAR[cat]
        ax.bar(0, v1, width, bottom=bot1, color=c_tb, label=label, edgecolor="white", lw=0.5)
        ax.bar(1, v2, width, bottom=bot2, color=c_ab, label="_nolegend_", edgecolor="white", lw=0.5)
        if v1 > 1:
            ax.text(0, bot1 + v1 / 2, f"{v1:.1f}%", ha="center", va="center",
                    fontsize=FONT["annotation"])
        if v2 > 1:
            ax.text(1, bot2 + v2 / 2, f"{v2:.1f}%", ha="center", va="center",
                    fontsize=FONT["annotation"])
        bot1 += v1
        bot2 += v2

    # Connect segment boundaries between bars with dashed lines
    bot1 = bot2 = 0
    for cat in categories:
        bot1 += max(0, seg1[cat])
        bot2 += max(0, seg2[cat])
        # Skip the top boundary (100%) to keep it clean
        if bot1 < 99 and bot2 < 99:
            ax.plot([0 + width/2, 1 - width/2], [bot1, bot2],
                    ls="--", color="black", lw=1, alpha=1)
            
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Temp Basal", "Autobolus"], fontsize=FONT["tick"])
    ax.tick_params(axis="y", labelsize=FONT["tick"])
    ax.set_ylabel("Percent Time (%)", fontsize=FONT["axis_label"])
    ax.set_ylim(0, 100)

    # Flip legend to match visual stacking order
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc="upper right", bbox_to_anchor=(1.3, 1),
              fontsize=FONT["legend"])
    
    ax.set_title("Figure 8.1c: Time in Glycemic Ranges\nTemp Basal vs Autobolus",
                 fontsize=FONT["suptitle"], fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")

def create_figure_8_1c_horizontal(df: pd.DataFrame, output_path: str):
    # Use paired-complete means to match table values
    range_pairs = [
        ("<54",     "tbr_very_low_seg1", "tbr_very_low_seg2"),
        ("<70",     "tbr_seg1",          "tbr_seg2"),
        ("70-180",  "tir_seg1",          "tir_seg2"),
        (">180",     "tar_seg1",          "tar_seg2"),
        (">250",    "tar_very_high_seg1", "tar_very_high_seg2"),
    ]
    paired = {key: compute_paired_statistics(df[c1], df[c2]) for key, c1, c2 in range_pairs}

    seg1 = {
        "<54":     paired["<54"]["seg1_mean"],
        "54-70":   paired["<70"]["seg1_mean"] - paired["<54"]["seg1_mean"],
        "70-180":  paired["70-180"]["seg1_mean"],
        "180-250": paired[">180"]["seg1_mean"] - paired[">250"]["seg1_mean"],
        ">250":    paired[">250"]["seg1_mean"],
    }
    seg2 = {
        "<54":     paired["<54"]["seg2_mean"],
        "54-70":   paired["<70"]["seg2_mean"] - paired["<54"]["seg2_mean"],
        "70-180":  paired["70-180"]["seg2_mean"],
        "180-250": paired[">180"]["seg2_mean"] - paired[">250"]["seg2_mean"],
        ">250":    paired[">250"]["seg2_mean"],
    }

    fig, ax = plt.subplots(figsize=(12, 6))
    categories = ["<54", "54-70", "70-180", "180-250", ">250"]
    range_labels = ["<54 mg/dL (Very Low)", "54–70 mg/dL (Low)",
                    "70–180 mg/dL (In Range)", "180–250 mg/dL (High)",
                    ">250 mg/dL (Very High)"]
    height = 0.5
    left1 = left2 = 0

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Find the smallest displayed value per bar for offset annotation
    displayed_seg1 = {cat: max(0, seg1[cat]) for cat in categories if seg1[cat] > 1}
    displayed_seg2 = {cat: max(0, seg2[cat]) for cat in categories if seg2[cat] > 1}
    min_cat_seg1 = min(displayed_seg1, key=displayed_seg1.get) if displayed_seg1 else None
    min_cat_seg2 = min(displayed_seg2, key=displayed_seg2.get) if displayed_seg2 else None

    for cat, label in zip(categories, range_labels):
        v1, v2 = max(0, seg1[cat]), max(0, seg2[cat])
        c_tb, c_ab = COLORS_STACKED_BAR[cat]
        ax.barh(1, v1, height, left=left1, color=c_tb, label=label, edgecolor="white", lw=0.5)
        ax.barh(0, v2, height, left=left2, color=c_ab, label="_nolegend_", edgecolor="white", lw=0.5)
        if v1 > 1:
            if cat == min_cat_seg1:
                ax.text(left1 + v1, 1, f"{v1:.1f}%", ha="left", va="center",
                        fontsize=FONT["annotation"])
            else:
                ax.text(left1 + v1 / 2, 1, f"{v1:.1f}%", ha="center", va="center",
                        fontsize=FONT["annotation"])
        if v2 > 1:
            if cat == min_cat_seg2:
                ax.text(left2 + v2, 0, f"{v2:.1f}%", ha="left", va="center",
                        fontsize=FONT["annotation"])
            else:
                ax.text(left2 + v2 / 2, 0, f"{v2:.1f}%", ha="center", va="center",
                        fontsize=FONT["annotation"])
        left1 += v1
        left2 += v2

    # Connect segment boundaries between bars with dashed lines
    left1 = left2 = 0
    for cat in categories:
        left1 += max(0, seg1[cat])
        left2 += max(0, seg2[cat])
        if left1 < 99 and left2 < 99:
            ax.plot([left1, left2], [1 - height / 2, 0 + height / 2],
                    ls="--", color="black", lw=1, alpha=1)

    ax.set_yticks([0, 1])
    ax.set_yticklabels(["Autobolus", "Temp Basal"], fontsize=FONT["tick"])
    ax.tick_params(axis="x", labelsize=FONT["tick"])
    ax.set_xlabel("Percent Time (%)", fontsize=FONT["axis_label"])
    ax.set_xlim(0, 100)

    # Single-row legend at bottom
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.1),
              ncol=len(categories), fontsize=FONT["legend"], frameon=False)

    ax.set_title("Figure 8.1c: Time in Glycemic Ranges\nTemp Basal vs Autobolus",
                 fontsize=FONT["suptitle"], fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
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
    print("Analysis 8.1: Performance and Safety of 40% PAF")
    print("=" * 60)

    print("\n1. Loading data...")
    df = load_data(spark)
    print(f"   {len(df)} users with paired TB→AB segments")

    print("\n2. Creating Table 8.1a...")
    table_parametric, table_nonparametric = create_table_8_1a(df)
    table_parametric.to_csv(f"{output_dir}/table_8_1a_parametric.csv", index=False)
    table_nonparametric.to_csv(f"{output_dir}/table_8_1a_nonparametric.csv", index=False)
    print("\n" + table_parametric.to_string(index=False))

    print("\n3. Creating Figure 8.1a...")
    create_figure_8_1a(df, f"{output_dir}/figure_8_1a_paired_differences.png")

    print("\n4. Creating Figure 8.1b...")
    create_figure_8_1b(df, f"{output_dir}/figure_8_1b_diff_histograms.png")

    print("\n5. Creating Figure 8.1c...")
    create_figure_8_1c(df, f"{output_dir}/figure_8_1c_stacked_bars.png")

    print("\n6. Creating Figure 8.1c (horizontal)...")
    create_figure_8_1c_horizontal(df, f"{output_dir}/figure_8_1c_stacked_bars_horizontal.png")

    print("\n" + "=" * 60)
    print("Analysis 8.1 Complete!")
    print("=" * 60)

    return {"table_parametric": table_parametric, "table_nonparametric": table_nonparametric, "df": df}


def run_in_databricks(spark):
    return run_analysis(spark)

run_in_databricks(spark)