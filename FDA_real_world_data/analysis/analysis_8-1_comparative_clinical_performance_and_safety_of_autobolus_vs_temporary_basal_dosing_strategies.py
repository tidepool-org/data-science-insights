"""
=============================================================================
Analysis 8.1: Performance and Safety of 40% PAF
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Evaluate the performance and safety of the 40% PAF parameter.

Inputs (through utils.data_loading.load_transition_endpoints, 2026-11-05):
- dev.fda_510k_rwd.transition_cohort{suffix}       (one row per window, a boolean per criterion, the
                                                    diagnosis group; in_transition_cohort selects the cohort)
- dev.fda_510k_rwd.glycemic_endpoints_transition{suffix}  (long: _userId, segment, tir, tbr, ..., hypo_events)

Outputs:
- Table 8.1a (parametric): Glycemic Endpoints by Delivery Strategy — Mean ± SD,
  paired diff with t-based 95% CI on the mean difference
- Table 8.1b (nonparametric): Median [IQR], paired diff with Hodges-Lehmann
  estimate + distribution-free (Wilcoxon signed-rank) 95% CI on the median difference
- Figure 8.1a: Paired Differences (paired dot + violin)
- Figure 8.1b: Distribution of Paired Differences (histograms)
- Figure 8.1c: Time in Ranges Stacked Bar Comparison
- By diagnosis group (MC, 2026-11-03; the appendix): Table 8.1a per series,
  the users per group, Figure 8.1a and Figure 8.1c with the series overlaid
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Optional, Tuple
import os
import shutil

# Configuration
NORMALITY_ALPHA = 0.05
OUTPUT_DIR = "outputs/analysis_8_1"

from utils.constants import (
    FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT, COLORS_STACKED_BAR,
    STACKED_BAR_PCT_DECIMALS,
)
from utils.figures import connect_band_boundaries, stacked_bar_with_callouts
from utils.statistics import test_normality, compute_paired_statistics, hodges_lehmann_ci, format_p, paired_non_inferiority
from utils.data_loading import DIAGNOSIS_GROUPS, LADA_DIAGNOSIS_GROUPS, PRIMARY_DIAGNOSIS_GROUPS, load_transition_endpoints

# Diagnosis series overlaid in the by-group outputs: every group (MC,
# 2026-10-29 and 2026-11-05: track every diagnosis type, never filter to type
# 1 unseen; type 1 and LADA were drawn first on 2026-11-03). The series are
# disjoint and cover every group: a JAEB member whose profile label reads LADA
# is in the LADA series only, although the primary cohort keeps it.
DIAGNOSIS_SERIES = [
    ("Type 1", ("type1",)),
    ("LADA", LADA_DIAGNOSIS_GROUPS),
    ("Type 2", ("type2",)),
    ("Other", ("other",)),
    ("Unknown", ("unknown",)),
]
# The second series set every by-group output is also written for: type 1
# beside LADA alone (MC, 2026-11-05: "In 8.1 also do just t1 vs lada").
TYPE1_LADA_SERIES = DIAGNOSIS_SERIES[:2]
# A series needs this many users to be drawn and tabulated as a result; below
# it, the series is a count only and open markers (STYLE.md §4; MC, 2026-11-03).
MIN_USERS_PER_SERIES = 5


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

def load_data(spark, suffix: str = "", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS) -> pd.DataFrame:
    """Load glycemic endpoints, apply standard filters, pivot to wide.

    suffix='_box070' reads the parallel 10-of-14 cohort tables. The default
    diagnosis groups are the primary cohort; None keeps every group for the
    by-group outputs.
    """
    return load_transition_endpoints(spark, suffix=suffix, diagnosis_groups=diagnosis_groups)



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
        hl = hodges_lehmann_ci(df[c1], df[c2])

        p_t = format_p(s['p_ttest'])
        p_w = format_p(s['p_wsrt'])

        # Parametric: t-based 95% CI on the mean paired difference
        ci_t = (
            f"[{s['diff_ci_low']:.2f}, {s['diff_ci_hi']:.2f}]"
            if not np.isnan(s['diff_ci_low']) else "N/A"
        )
        # Nonparametric: Hodges-Lehmann estimate + Wilcoxon-based 95% CI on the median diff
        hl_ci = (
            f"{hl['hl']:.2f} [{hl['ci_low']:.2f}, {hl['ci_hi']:.2f}]"
            if not np.isnan(hl['ci_low']) else "N/A"
        )

        parametric_rows.append({
            "Endpoint": name,
            "Temp Basal Mean ± SD":  f"{s['seg1_mean']:.2f} ± {s['seg1_sd']:.2f}",
            "Autobolus Mean ± SD":   f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
            "Paired Diff Mean ± SD": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
            "Paired Diff 95% CI": ci_t,
            "p (paired t-test)": p_t,
            "N": s["n_pairs"],
        })

        nonparametric_rows.append({
            "Endpoint": name,
            "Temp Basal Median [IQR]":  f"{s['seg1_median']:.2f} [{s['seg1_q1']:.1f}, {s['seg1_q3']:.1f}]",
            "Autobolus Median [IQR]":   f"{s['seg2_median']:.2f} [{s['seg2_q1']:.1f}, {s['seg2_q3']:.1f}]",
            "Paired Diff Median [IQR]": f"{s['diff_median']:.2f} [{s['diff_q1']:.1f}, {s['diff_q3']:.1f}]",
            "HL Median Diff (95% CI)": hl_ci,
            "p (Wilcoxon signed-rank)": p_w,
            "Normality (Shapiro p)": f"{s['normality_p']:.2e}" if not np.isnan(s['normality_p']) else "N/A",
            "N": s["n_pairs"],
        })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.1a — Paired dot + violin with p-values in titles
# =============================================================================

def titled(title: str, cohort_note: str) -> str:
    """A figure title with the cohort note as its last line when the cohort
    is not the production one, so a figure of another cohort names it."""
    return f"{title}\n{cohort_note}" if cohort_note else title


def create_figure_8_1a(df: pd.DataFrame, output_path: str, cohort_note: str = ""):
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
            p_str = f"t: {format_p(s['p_ttest'])}  WSRT: {format_p(s['p_wsrt'])}"

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
        plt.suptitle(titled(f"Figure 8.1a ({part_label}): Paired Differences in Glycemic Endpoints\n"
                            "Individual users connected by lines; box + violin overlay", cohort_note),
                     fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
        plt.tight_layout()

        path = output_path.replace(".png", f"_{part_label}.png")
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {path}")

# =============================================================================
# Figure 8.1b — Histograms of paired differences with p-values in titles
# =============================================================================

def create_figure_8_1b(df: pd.DataFrame, output_path: str, cohort_note: str = ""):
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
            p_str = f"t: {format_p(s['p_ttest'])}  WSRT: {format_p(s['p_wsrt'])}"

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
        plt.suptitle(titled(f"Figure 8.1b ({part_label}): Distribution of Paired Differences in Glycemic Endpoints",
                            cohort_note),
                     fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
        plt.tight_layout()

        path = output_path.replace(".png", f"_{part_label}.png")
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {path}")


# =============================================================================
# Figure 8.1c — Stacked bar with matched colors and flipped legend
# =============================================================================

def create_figure_8_1c(df: pd.DataFrame, output_path: str, cohort_note: str = ""):
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
    width = 0.5
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    tops1 = stacked_bar_with_callouts(ax, 0, seg1, width, legend=True)
    tops2 = stacked_bar_with_callouts(ax, 1, seg2, width, legend=False)
    connect_band_boundaries(ax, 0, 1, tops1, tops2, width)

    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Temp Basal", "Autobolus"], fontsize=FONT["tick"])
    ax.tick_params(axis="y", labelsize=FONT["tick"])
    ax.set_ylabel("Percent Time (%)", fontsize=FONT["axis_label"])
    ax.set_ylim(0, 100)

    # Flip legend to match visual stacking order
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc="upper right", bbox_to_anchor=(1.3, 1),
              fontsize=FONT["legend"])
    
    ax.set_title(titled("Figure 8.1c: Time in Glycemic Ranges\nTemp Basal vs Autobolus", cohort_note),
                 fontsize=FONT["suptitle"], fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")

def create_figure_8_1c_horizontal(df: pd.DataFrame, output_path: str, cohort_note: str = ""):
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
        color = COLORS_STACKED_BAR[cat]
        ax.barh(1, v1, height, left=left1, color=color, label=label, edgecolor="white", lw=0.5)
        ax.barh(0, v2, height, left=left2, color=color, label="_nolegend_", edgecolor="white", lw=0.5)
        if v1 > 1:
            if cat == min_cat_seg1:
                ax.text(left1 + v1, 1, f"{v1:.{STACKED_BAR_PCT_DECIMALS}f}%", ha="left", va="center",
                        fontsize=FONT["annotation"])
            else:
                ax.text(left1 + v1 / 2, 1, f"{v1:.{STACKED_BAR_PCT_DECIMALS}f}%", ha="center", va="center",
                        fontsize=FONT["annotation"])
        if v2 > 1:
            if cat == min_cat_seg2:
                ax.text(left2 + v2, 0, f"{v2:.{STACKED_BAR_PCT_DECIMALS}f}%", ha="left", va="center",
                        fontsize=FONT["annotation"])
            else:
                ax.text(left2 + v2 / 2, 0, f"{v2:.{STACKED_BAR_PCT_DECIMALS}f}%", ha="center", va="center",
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

    ax.set_title(titled("Figure 8.1c: Time in Glycemic Ranges\nTemp Basal vs Autobolus", cohort_note),
                 fontsize=FONT["suptitle"], fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")

# =============================================================================
# Non-inferiority test on TIR
# =============================================================================

def compute_non_inferiority(df: pd.DataFrame, margin: float = 5.0) -> Dict:
    """
    One-sided paired t-test for non-inferiority of autobolus TIR vs temp basal TIR.

    H₀: mean(TIR_autobolus - TIR_temp_basal) ≤ -margin  (autobolus is inferior)
    H₁: mean(TIR_autobolus - TIR_temp_basal) > -margin   (autobolus is non-inferior)

    The test itself lives in utils.statistics.paired_non_inferiority (shared with
    PLN-RFAI-0001 Analysis 2, which runs it within each insulin group); this
    step applies it to the cohort frame and prints the result at α = 0.05.
    """
    result = paired_non_inferiority(df["tir_seg1"], df["tir_seg2"], margin=margin)
    n, mean_diff, sd_diff, t_stat, p_value = (result[k] for k in ("n", "mean_diff", "sd_diff", "t_stat", "p_value"))

    print(f"\n  Non-Inferiority Test: TIR (Autobolus − Temp Basal)")
    print(f"  Margin:     {margin:.1f}%")
    print(f"  N:          {n}")
    print(f"  Mean Δ:     {mean_diff:+.2f}%")
    print(f"  SD Δ:       {sd_diff:.2f}%")
    print(f"  t-stat:     {t_stat:.3f}")
    print(f"  p-value:    {p_value:.4f}")
    print(f"  Conclusion: {'Non-inferior' if result['non_inferior'] else 'Cannot conclude non-inferiority'} (α=0.05)")

    return result


# =============================================================================
# By diagnosis group (the appendix)
# =============================================================================

def series_frames(df_all: pd.DataFrame, series=None):
    """The cohort frame of each diagnosis series: (label, groups, frame, drawn).

    `series` is the list of (label, groups) to cut, DIAGNOSIS_SERIES by
    default; `drawn` is whether the series has at least MIN_USERS_PER_SERIES
    users.
    """
    out = []
    for label, groups in (series or DIAGNOSIS_SERIES):
        frame = df_all[df_all["diagnosis_group"].isin(groups)].copy()
        out.append((label, groups, frame, len(frame) >= MIN_USERS_PER_SERIES))
    return out


def create_table_8_1_diagnosis_groups(df_all: pd.DataFrame) -> pd.DataFrame:
    """Users of the cohort in each diagnosis group, every group listed."""
    counts = df_all["diagnosis_group"].value_counts()
    rows = [{"Diagnosis group": g, "N": int(counts.get(g, 0))} for g in DIAGNOSIS_GROUPS]
    rows.append({"Diagnosis group": "all", "N": int(len(df_all))})
    return pd.DataFrame(rows)


def create_table_8_1a_by_diagnosis(df_all: pd.DataFrame, series=None):
    """Table 8.1a (parametric and nonparametric) once per diagnosis series.

    A series below MIN_USERS_PER_SERIES gets one row per endpoint with its N
    and no statistic, so the count is on the record and nothing is read as a
    result.
    """
    parametric, nonparametric = [], []
    for label, groups, frame, drawn in series_frames(df_all, series):
        head = {"Series": label, "Groups": " + ".join(groups), "Drawn": int(drawn)}
        if drawn:
            p, n = create_table_8_1a(frame)
            parametric.extend({**head, **row} for row in p.to_dict("records"))
            nonparametric.extend({**head, **row} for row in n.to_dict("records"))
        else:
            for name, c1, c2, _ in ENDPOINTS:
                valid = frame[c1].notna() & frame[c2].notna()
                parametric.append({**head, "Endpoint": name, "N": int(valid.sum())})
                nonparametric.append({**head, "Endpoint": name, "N": int(valid.sum())})
    return pd.DataFrame(parametric), pd.DataFrame(nonparametric)


def create_figure_8_1a_by_diagnosis(df_all: pd.DataFrame, output_path: str, cohort_note: str = "", series=None):
    """Figure 8.1a with the diagnosis series side by side in every panel.

    Each series is a temp-basal and an autobolus position with the users'
    pairs as thin grey connectors; a series with at least MIN_USERS_PER_SERIES
    users gets the box and violin, a smaller one open markers only. No
    p-values on the figure (STYLE.md §4).
    """
    labels = [f"({chr(97 + i)})" for i in range(len(ENDPOINTS))]
    # a series with no user has nothing to draw; the groups table lists it
    series = [s for s in series_frames(df_all, series) if len(s[2]) > 0]
    gap = 2.5
    # two panels across, each wide enough for every series' pair of boxes
    width = max(12, 2 * (1.6 * len(series) + 1))

    for part, row_slice in enumerate([slice(0, 4), slice(4, 8)]):
        fig, axes = plt.subplots(2, 2, figsize=(width, 12))
        for idx, (ax, (title, c1, c2, unit)) in enumerate(zip(axes.flatten(), ENDPOINTS[row_slice])):
            ticks, tick_labels, series_ticks, series_labels = [], [], [], []
            for k, (label, _, frame, drawn) in enumerate(series):
                x1, x2 = k * gap, k * gap + 1
                valid = frame[c1].notna() & frame[c2].notna()
                v1, v2 = frame.loc[valid, c1].values, frame.loc[valid, c2].values
                n = len(v1)
                for a, b in zip(v1, v2):
                    ax.plot([x1, x2], [a, b], "-", color="gray", alpha=0.3, lw=0.5)
                if drawn and n > 1:
                    bp = ax.boxplot([v1, v2], positions=[x1, x2], widths=0.3,
                                    patch_artist=True, showfliers=False)
                    bp["boxes"][0].set(facecolor=COLORS_PRIMARY, alpha=0.7)
                    bp["boxes"][1].set(facecolor=COLORS_SECONDARY, alpha=0.7)
                    parts = ax.violinplot([v1, v2], positions=[x1, x2],
                                          showmeans=False, showmedians=False, widths=0.5)
                    for i, pc in enumerate(parts["bodies"]):
                        pc.set_facecolor(COLORS_PRIMARY if i == 0 else COLORS_SECONDARY)
                        pc.set_alpha(0.3)
                    ax.plot([x1] * n, v1, "o", color=COLORS_PRIMARY, ms=3, alpha=0.6)
                    ax.plot([x2] * n, v2, "o", color=COLORS_SECONDARY, ms=3, alpha=0.6)
                else:
                    ax.plot([x1] * n, v1, "o", mfc="none", mec=COLORS_PRIMARY, ms=5)
                    ax.plot([x2] * n, v2, "o", mfc="none", mec=COLORS_SECONDARY, ms=5)
                ticks += [x1, x2]
                tick_labels += ["Temp\nBasal", "Auto-\nbolus"]
                series_ticks.append((x1 + x2) / 2)
                series_labels.append(f"{label} (n={n})")
            # the strategy under each box on two lines; the series and its
            # users below that, centred under the pair (minor ticks, no mark)
            ax.set_xticks(ticks)
            ax.set_xticklabels(tick_labels, fontsize=FONT["tick"])
            ax.set_xticks(series_ticks, minor=True)
            ax.set_xticklabels(series_labels, minor=True, fontsize=FONT["tick"])
            ax.tick_params(axis="x", which="minor", length=0, pad=3.4 * FONT["tick"])
            ax.tick_params(axis="y", labelsize=FONT["tick"])
            ax.set_ylabel(unit, fontsize=FONT["axis_label"])
            ax.set_title(f"{labels[row_slice][idx]} {title}", fontsize=FONT["title"], fontweight="bold")
            ax.grid(axis="y", alpha=0.3)

        part_label = "i" if part == 0 else "ii"
        plt.suptitle(titled(f"Figure 8.1a ({part_label}) by diagnosis group: paired glycemic endpoints\n"
                            f"Series with fewer than {MIN_USERS_PER_SERIES} users are drawn as open markers",
                            cohort_note),
                     fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
        plt.tight_layout()
        path = output_path.replace(".png", f"_{part_label}.png")
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved: {path}")


def _range_means(frame: pd.DataFrame, which: str) -> Dict[str, float]:
    """Paired-complete mean time in each glycemic band for one period
    (which = seg1_mean or seg2_mean), as Figure 8.1c stacks them."""
    pairs = {
        "<54": ("tbr_very_low_seg1", "tbr_very_low_seg2"),
        "<70": ("tbr_seg1", "tbr_seg2"),
        "70-180": ("tir_seg1", "tir_seg2"),
        ">180": ("tar_seg1", "tar_seg2"),
        ">250": ("tar_very_high_seg1", "tar_very_high_seg2"),
    }
    m = {key: compute_paired_statistics(frame[c1], frame[c2])[which] for key, (c1, c2) in pairs.items()}
    return {
        "<54": m["<54"],
        "54-70": m["<70"] - m["<54"],
        "70-180": m["70-180"],
        "180-250": m[">180"] - m[">250"],
        ">250": m[">250"],
    }


def create_figure_8_1c_by_diagnosis(df_all: pd.DataFrame, output_path: str, cohort_note: str = "", series=None):
    """Figure 8.1c with, for each diagnosis series, a temp-basal bar beside an
    autobolus bar (MC, 2026-11-05: everything compares temp basal to
    autobolus), the strategy under each bar and the series under the pair,
    as the by-group Figure 8.1a lays its pairs out. A series below
    MIN_USERS_PER_SERIES is an empty pair labelled with its count."""
    strategies = [("Temp\nBasal", "seg1_mean"), ("Auto-\nbolus", "seg2_mean")]
    # a series with no user has nothing to draw; the groups table lists it
    series = [s for s in series_frames(df_all, series) if len(s[2]) > 0]
    gap, width = 2.5, 0.7
    fig, ax = plt.subplots(figsize=(max(8, 1.6 * len(series) + 3), 8))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ticks, tick_labels, series_ticks, series_labels = [], [], [], []
    for k, (label, _, frame, drawn) in enumerate(series):
        positions = [k * gap, k * gap + 1]
        ticks += positions
        tick_labels += [name for name, _ in strategies]
        series_ticks.append(sum(positions) / 2)
        series_labels.append(f"{label} (n={len(frame)})")
        if not drawn:
            ax.text(sum(positions) / 2, 50, f"n={len(frame)} < {MIN_USERS_PER_SERIES}", ha="center", va="center",
                    fontsize=FONT["annotation"])
            continue
        tops = [stacked_bar_with_callouts(ax, x, _range_means(frame, which), width,
                                          legend=(k == 0 and which == "seg1_mean"))
                for x, (_, which) in zip(positions, strategies)]
        connect_band_boundaries(ax, positions[0], positions[1], tops[0], tops[1], width)
    # the strategy under each bar on two lines; the series and its users
    # below that, centred under the pair (minor ticks, no mark)
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels, fontsize=FONT["tick"])
    ax.set_xticks(series_ticks, minor=True)
    ax.set_xticklabels(series_labels, minor=True, fontsize=FONT["tick"])
    ax.tick_params(axis="x", which="minor", length=0, pad=3.4 * FONT["tick"])
    ax.tick_params(axis="y", labelsize=FONT["tick"])
    ax.set_ylim(0, 100)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylabel("Percent Time (%)", fontsize=FONT["axis_label"])
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=FONT["legend"])
    plt.suptitle(titled("Figure 8.1c by diagnosis group: time in glycemic ranges, temp basal beside autobolus",
                        cohort_note),
                 fontsize=FONT["suptitle"], fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


WIDE_PARAMETRIC_COLUMNS = ["Temp Basal Mean ± SD", "Autobolus Mean ± SD", "Paired Diff Mean ± SD", "Paired Diff 95% CI"]
WIDE_NONPARAMETRIC_COLUMNS = ["Temp Basal Median [IQR]", "Autobolus Median [IQR]", "HL Median Diff (95% CI)"]


def widen_by_diagnosis(long: pd.DataFrame, columns, series=None) -> pd.DataFrame:
    """The by-series Table 8.1a as one glycemic outcome per row with the
    series side by side (MC, 2026-11-05): for each series its N and the
    listed columns, blank where the series is a count only."""
    rows = []
    for name, *_ in ENDPOINTS:
        row = {"Endpoint": name}
        for label, _ in (series or DIAGNOSIS_SERIES):
            cell = long[(long["Series"] == label) & (long["Endpoint"] == name)].iloc[0]
            row[f"{label} N"] = int(cell["N"])
            for column in columns:
                row[f"{label} {column}"] = cell[column] if cell["Drawn"] else ""
        rows.append(row)
    return pd.DataFrame(rows)


def write_series_set(df_all: pd.DataFrame, output_dir: str, cohort_note: str, series, suffix: str):
    """Table 8.1a per series (long and wide) and Figures 8.1a and 8.1c with
    the given series overlaid, the file names carrying `suffix`."""
    parametric, nonparametric = create_table_8_1a_by_diagnosis(df_all, series)
    parametric.to_csv(f"{output_dir}/table_8_1a_parametric_{suffix}.csv", index=False)
    nonparametric.to_csv(f"{output_dir}/table_8_1a_nonparametric_{suffix}.csv", index=False)
    widen_by_diagnosis(parametric, WIDE_PARAMETRIC_COLUMNS, series).to_csv(
        f"{output_dir}/table_8_1a_parametric_{suffix}_wide.csv", index=False)
    widen_by_diagnosis(nonparametric, WIDE_NONPARAMETRIC_COLUMNS, series).to_csv(
        f"{output_dir}/table_8_1a_nonparametric_{suffix}_wide.csv", index=False)
    create_figure_8_1a_by_diagnosis(df_all, f"{output_dir}/figure_8_1a_paired_differences_{suffix}.png",
                                    cohort_note, series)
    create_figure_8_1c_by_diagnosis(df_all, f"{output_dir}/figure_8_1c_stacked_bars_{suffix}.png",
                                    cohort_note, series)
    return parametric, nonparametric


def write_by_diagnosis_outputs(df_all: pd.DataFrame, output_dir: str, cohort_note: str = ""):
    """Every by-group output into output_dir: the users per group, and two
    series sets, every diagnosis group (`by_diagnosis_group`) and type 1
    beside LADA alone (`type1_lada`; MC, 2026-11-05), each with Table 8.1a
    per series (long, one row per series and endpoint, and wide, one row per
    endpoint) and Figures 8.1a and 8.1c with the series overlaid."""
    groups_table = create_table_8_1_diagnosis_groups(df_all)
    groups_table.to_csv(f"{output_dir}/table_8_1_diagnosis_groups.csv", index=False)
    print("\n" + groups_table.to_string(index=False))
    parametric, nonparametric = write_series_set(df_all, output_dir, cohort_note, DIAGNOSIS_SERIES, "by_diagnosis_group")
    print("\n" + parametric.to_string(index=False))
    write_series_set(df_all, output_dir, cohort_note, TYPE1_LADA_SERIES, "type1_lada")
    return groups_table, parametric, nonparametric


# =============================================================================
# Main
# =============================================================================

def write_outputs(df: pd.DataFrame, df_all: pd.DataFrame, output_dir: str, cohort_note: str = ""):
    """Every §8-1 output into output_dir from a loaded cohort.

    df      the primary cohort (the primary diagnosis groups): Table 8.1a,
            Figures 8.1a to 8.1c, the non-inferiority test
    df_all  the same cohort with every diagnosis group: the by-group appendix
    cohort_note  the last line of every figure title when the cohort is not
            the production one (e.g. a cohort another script cut), empty for
            the production run
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"   {len(df)} users with paired TB→AB segments")

    print("\n2. Creating Table 8.1a...")
    table_parametric, table_nonparametric = create_table_8_1a(df)
    table_parametric.to_csv(f"{output_dir}/table_8_1a_parametric.csv", index=False)
    table_nonparametric.to_csv(f"{output_dir}/table_8_1a_nonparametric.csv", index=False)
    print("\n" + table_parametric.to_string(index=False))

    print("\n3. Creating Figure 8.1a...")
    create_figure_8_1a(df, f"{output_dir}/figure_8_1a_paired_differences.png", cohort_note)

    print("\n4. Creating Figure 8.1b...")
    create_figure_8_1b(df, f"{output_dir}/figure_8_1b_diff_histograms.png", cohort_note)

    print("\n5. Creating Figure 8.1c...")
    create_figure_8_1c(df, f"{output_dir}/figure_8_1c_stacked_bars.png", cohort_note)

    print("\n6. Creating Figure 8.1c (horizontal)...")
    create_figure_8_1c_horizontal(df, f"{output_dir}/figure_8_1c_stacked_bars_horizontal.png", cohort_note)

    print("\n7. Non-inferiority test on TIR (margin = 5%)...")
    ni_result = compute_non_inferiority(df, margin=5.0)

    print("\n8. By diagnosis group (every group; the series of DIAGNOSIS_SERIES drawn)...")
    by_group = write_by_diagnosis_outputs(df_all, output_dir, cohort_note)

    return {"table_parametric": table_parametric, "table_nonparametric": table_nonparametric,
            "non_inferiority": ni_result, "df": df, "df_all": df_all, "by_diagnosis_group": by_group}


def run_analysis(spark, output_dir=None, suffix: str = ""):
    # suffix='_box080' runs on the parallel 0.80-box cohort and writes to a
    # parallel output dir so the production outputs aren't clobbered.
    if output_dir is None:
        output_dir = OUTPUT_DIR + suffix

    print("=" * 60)
    print("Analysis 8.1: Performance and Safety of 40% PAF")
    print("=" * 60)

    print("\n1. Loading data (the primary groups, then every group for the appendix)...")
    df = load_data(spark, suffix=suffix)
    df_all = load_data(spark, suffix=suffix, diagnosis_groups=None)
    # a suffixed build (a window-rule or classifier variant) names itself on
    # every figure; the production build's titles carry nothing
    cohort_note = f"Variant build {suffix}, not the production cohort" if suffix else ""
    # The analysis owns its output folder (STYLE.md §5).
    shutil.rmtree(output_dir, ignore_errors=True)
    results = write_outputs(df, df_all, output_dir, cohort_note)

    print("\n" + "=" * 60)
    print("Analysis 8.1 Complete!")
    print("=" * 60)
    return results


def run_in_databricks(spark, suffix: str = ""):
    return run_analysis(spark, suffix=suffix)

if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default="",
                         help="source-table suffix, e.g. _box080 for the 0.80-box cohort")
    _args, _ = _parser.parse_known_args()
    run_in_databricks(spark, suffix=_args.suffix)  # type: ignore[name-defined]