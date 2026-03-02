"""
=============================================================================
Analysis 8.2: Glycemic Endpoints During Override Periods
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Compare glycemic endpoints during valid override configurations
between temp basal and autobolus dosing modes.

Inputs:
- dev.fda_510k_rwd.glycemic_endpoints_override  (long: _userId, overridePreset, segment, tir, tbr, ..., hypo_events)

Outputs:
- Table 8.2a: Glycemic Endpoints by Override Type and Dosing Mode
- Figure 8.2a–b: Paired differences, histograms (per override type)
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from typing import Dict, Optional, Tuple
import os


NORMALITY_ALPHA = 0.05
OUTPUT_DIR = "outputs/analysis_8_2"

from utils.constants import FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT, COLORS_STACKED_BAR
from utils.statistics import test_normality, compute_paired_statistics, format_p

SEG1 = "temp_basal"
SEG2 = "autobolus"

CONFIG_COLS = ["overridePreset", "brsf", "btl", "bth", "crsf", "issf"]

ENDPOINTS = [
    ("Time <54 mg/dL (%)",           "tbr_very_low_seg1", "tbr_very_low_seg2", "%"),
    ("Time <70 mg/dL (%)",           "tbr_seg1",          "tbr_seg2",          "%"),
    ("Time 70–180 mg/dL (%)",        "tir_seg1",          "tir_seg2",          "%"),
    ("Time >180 mg/dL (%)",          "tar_seg1",          "tar_seg2",          "%"),
    ("Time >250 mg/dL (%)",          "tar_very_high_seg1","tar_very_high_seg2","%"),
    ("Hypoglycemic events (n)",      "hypo_events_seg1",  "hypo_events_seg2",  "n"),
    ("Mean glucose (mg/dL)",         "mean_glucose_seg1",  "mean_glucose_seg2", "mg/dL"),
    ("Coefficient of variation (%)", "cv_seg1",            "cv_seg2",           "%"),
]



# =============================================================================
# Data Loading
# =============================================================================

def load_data(spark) -> Dict[str, pd.DataFrame]:
    """
    Load override glycemic endpoints and pivot to wide format.

    Returns a dict keyed by overridePreset, each value a wide DataFrame
    with one row per (user, override config) and paired seg1/seg2 columns.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_override").toPandas()

    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "segment", "overridePreset"):
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Pivot to wide: one row per (user, config) with _seg1/_seg2 suffixes
    index_cols = ["_userId"] + CONFIG_COLS

    seg1 = (endpoints[endpoints["segment"] == SEG1]
            .set_index(index_cols)
            .drop(columns=["segment"])
            .add_suffix("_seg1"))
    seg2 = (endpoints[endpoints["segment"] == SEG2]
            .set_index(index_cols)
            .drop(columns=["segment"])
            .add_suffix("_seg2"))

    wide = seg1.join(seg2, how="inner").reset_index()

    # Split by overridePreset for per-type analysis
    result = {}
    for otype, group in wide.groupby("overridePreset"):
        if len(group) >= 3:  # need at least 3 pairs for stats
            result[otype] = group.reset_index(drop=True)
            print(f"  {otype}: {len(group)} paired configs")
        else:
            print(f"  {otype}: {len(group)} paired configs (skipping, < 3)")

    # Also include pooled across all types
    if len(wide) >= 3:
        result["_all"] = wide.reset_index(drop=True)
        print(f"  _all (pooled): {len(wide)} paired configs")

    return result


# =============================================================================
# Table 8.2a
# =============================================================================

def create_table_8_2a(datasets: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    parametric_rows = []
    nonparametric_rows = []

    for otype, df in datasets.items():
        for name, c1, c2, _ in ENDPOINTS:
            if c1 not in df.columns or c2 not in df.columns:
                continue
            s = compute_paired_statistics(df[c1], df[c2])

            p_t = format_p(s['p_ttest'])
            p_w = format_p(s['p_wsrt'])

            parametric_rows.append({
                "Override Type": otype,
                "Endpoint": name,
                "Temp Basal Mean ± SD":  f"{s['seg1_mean']:.2f} ± {s['seg1_sd']:.2f}",
                "Autobolus Mean ± SD":   f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
                "Paired Diff Mean ± SD": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
                "p (paired t-test)": p_t,
                "N": s["n_pairs"],
            })

            nonparametric_rows.append({
                "Override Type": otype,
                "Endpoint": name,
                "Temp Basal Median [IQR]":  f"{s['seg1_median']:.2f} [{s['seg1_q1']:.1f}, {s['seg1_q3']:.1f}]",
                "Autobolus Median [IQR]":   f"{s['seg2_median']:.2f} [{s['seg2_q1']:.1f}, {s['seg2_q3']:.1f}]",
                "Paired Diff Median [IQR]": f"{s['diff_median']:.2f} [{s['diff_q1']:.1f}, {s['diff_q3']:.1f}]",
                "p (Wilcoxon signed-rank)": p_w,
                "Normality (Shapiro p)": format_p(s['normality_p']),
                "N": s["n_pairs"],
            })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.2a — Paired dot + violin (per override type)
# =============================================================================

def create_figure_8_2a(datasets: Dict[str, pd.DataFrame], output_dir: str):
    for otype, df in datasets.items():
        labels = [f"({chr(97 + i)})" for i in range(len(ENDPOINTS))]

        for part, row_slice in enumerate([slice(0, 4), slice(4, 8)]):
            endpoints_subset = ENDPOINTS[row_slice]
            if not endpoints_subset:
                continue

            nrows = (len(endpoints_subset) + 1) // 2
            fig, axes = plt.subplots(nrows, 2, figsize=(12, 6 * nrows))
            axes = axes.flatten() if nrows > 1 else [axes] if len(endpoints_subset) == 1 else axes.flatten()
            labels_subset = labels[row_slice]

            for idx, (ax, (title, c1, c2, unit)) in enumerate(zip(axes, endpoints_subset)):
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
                ax.set_xticklabels(["Temp Basal", "Autobolus"], fontsize=FONT["tick"])
                ax.tick_params(axis="y", labelsize=FONT["tick"])
                ax.set_ylabel(unit, fontsize=FONT["axis_label"])
                ax.set_title(f"{labels_subset[idx]} {title}\n{p_str}",
                             fontsize=FONT["title"], fontweight="bold")
                ax.grid(axis="y", alpha=0.3)

            # Hide unused axes
            for ax in axes[len(endpoints_subset):]:
                ax.set_visible(False)

            part_label = "i" if part == 0 else "ii"
            otype_display = "All Overrides (Pooled)" if otype == "_all" else otype.title()
            plt.suptitle(f"Figure 8.2a ({part_label}): Paired Differences — {otype_display}\n"
                         "Individual override configs connected by lines; box + violin overlay",
                         fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
            plt.tight_layout()

            path = f"{output_dir}/figure_8_2a_{otype}_{part_label}.png"
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Saved: {path}")


# =============================================================================
# Figure 8.2b — Histograms of paired differences (per override type)
# =============================================================================

def create_figure_8_2b(datasets: Dict[str, pd.DataFrame], output_dir: str):
    for otype, df in datasets.items():
        labels = [f"({chr(97 + i)})" for i in range(len(ENDPOINTS))]

        for part, row_slice in enumerate([slice(0, 4), slice(4, 8)]):
            endpoints_subset = ENDPOINTS[row_slice]
            if not endpoints_subset:
                continue

            nrows = (len(endpoints_subset) + 1) // 2
            fig, axes = plt.subplots(nrows, 2, figsize=(12, 6 * nrows))
            axes = axes.flatten() if nrows > 1 else [axes] if len(endpoints_subset) == 1 else axes.flatten()
            labels_subset = labels[row_slice]

            for idx, (ax, (title, c1, c2, _)) in enumerate(zip(axes, endpoints_subset)):
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
                ax.set_ylabel("Count", fontsize=FONT["axis_label"])
                ax.tick_params(axis="both", labelsize=FONT["tick"])
                ax.set_title(f"{labels_subset[idx]} {title}\n{p_str}",
                             fontsize=FONT["title"], fontweight="bold")
                ax.legend(fontsize=FONT["legend"])
                ax.grid(axis="y", alpha=0.3)

            for ax in axes[len(endpoints_subset):]:
                ax.set_visible(False)

            part_label = "i" if part == 0 else "ii"
            otype_display = "All Overrides (Pooled)" if otype == "_all" else otype.title()
            plt.suptitle(f"Figure 8.2b ({part_label}): Distribution of Paired Differences — {otype_display}",
                         fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
            plt.tight_layout()

            path = f"{output_dir}/figure_8_2b_{otype}_{part_label}.png"
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Saved: {path}")


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.2: Glycemic Endpoints During Override Periods")
    print("=" * 60)

    print("\n1. Loading data...")
    datasets = load_data(spark)

    print("\n2. Creating Table 8.2a...")
    table_p, table_np = create_table_8_2a(datasets)
    table_p.to_csv(f"{output_dir}/table_8_2a_parametric.csv", index=False)
    table_np.to_csv(f"{output_dir}/table_8_2a_nonparametric.csv", index=False)
    print("\n" + table_p.to_string(index=False))

    print("\n3. Creating Figure 8.2a...")
    create_figure_8_2a(datasets, output_dir)

    print("\n4. Creating Figure 8.2b...")
    create_figure_8_2b(datasets, output_dir)

    print("\n" + "=" * 60)
    print("Analysis 8.2 Complete!")
    print("=" * 60)

    return {"table_parametric": table_p, "table_nonparametric": table_np, "datasets": datasets}


def run_in_databricks(spark):
    return run_analysis(spark)

run_in_databricks(spark) # type: ignore[name-defined]