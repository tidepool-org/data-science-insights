"""
=============================================================================
Analysis 8.2: Glycemic Outcomes During Preset Activation
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Compare glycemic endpoints during eligible preset activation
windows (preset duration + 2-hour tail) between the temp-basal and
autobolus phases of a TB→AB transition.

Inclusion (applied by utils.data_loading.load_override_endpoints +
aggregate_override_endpoints):
- Cohort: Loop version below MAX_LOOP_VERSION_INT (or, if version unknown,
  segment ending before MAX_SEG2_END_DATE)
- No guardrail violations on the rank-1 transition segment
- is_starting_glucose_in_range = TRUE (CBG within 30 min before activation
  is between STARTING_GLUCOSE_LOW and STARTING_GLUCOSE_HIGH)
- is_valid_name_only_seg2 = TRUE for Table 8.2b (TB vs initial AB)
- is_valid_name_only_seg3 = TRUE for Table 8.2c (TB vs second AB,
  days 14–28 post-transition)

Per-activation endpoints are computed in compute_glycemic_endpoints (override
mode); the analysis aggregates them up to the requested grain — primary
(preset name) or sensitivity (preset name + exact parameter set).

Outputs:
- Table 8.2a: Sample characteristics (users, activations/segment, hours/segment)
- Table 8.2b: Glycemic endpoints, TB vs initial AB
  - Primary:     `table_8_2b_parametric.csv` / `table_8_2b_nonparametric.csv`
  - Sensitivity: `table_8_2b_by_config_parametric.csv` / `..._nonparametric.csv`
- Table 8.2c: Glycemic endpoints, TB vs second AB (days 14–28)
  - Primary + sensitivity files mirroring 8.2b
- Figure 8.2a: Paired differences for the primary TB vs initial AB analysis
- Figure 8.2b: Example glucose traces during preset activation, anonymized
=============================================================================
"""

import os
import re
from typing import Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from utils.constants import (
    FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT,
    STARTING_GLUCOSE_LOW, STARTING_GLUCOSE_HIGH,
)
from utils.data_loading import (
    MAX_LOOP_VERSION_INT, MAX_SEG2_END_DATE,
    load_override_endpoints, aggregate_override_endpoints,
)
from utils.statistics import compute_paired_statistics, format_p


OUTPUT_DIR = "outputs/analysis_8_2"

# Internal segment labels (match the staging table values).
TB  = "tb_to_ab_seg1"
AB1 = "tb_to_ab_seg2"  # initial AB, days 0–14
AB2 = "tb_to_ab_seg3"  # second AB, days 14–28

# `dosing_mode` collapses both AB segments to "autobolus" (used by Table 8.2a
# to count temp-basal vs autobolus activations).
DOSING_TB = "temp_basal"
DOSING_AB = "autobolus"

ENDPOINTS = [
    ("Time <54 mg/dL (%)",                "tbr_very_low_seg1",  "tbr_very_low_seg2",  "%"),
    ("Time <70 mg/dL (%)",                "tbr_seg1",           "tbr_seg2",           "%"),
    ("Time 70–180 mg/dL (%)",             "tir_seg1",           "tir_seg2",           "%"),
    ("Time >180 mg/dL (%)",               "tar_seg1",           "tar_seg2",           "%"),
    ("Time >250 mg/dL (%)",               "tar_very_high_seg1", "tar_very_high_seg2", "%"),
    ("Hypoglycemic event rate (n/hour)",  "hypo_rate_seg1",     "hypo_rate_seg2",     "n/hr"),
    ("Mean glucose (mg/dL)",              "mean_glucose_seg1",  "mean_glucose_seg2",  "mg/dL"),
    ("Coefficient of variation (%)",      "cv_seg1",            "cv_seg2",            "%"),
]

_COHORT_WHERE = (
    f"(tb_to_ab_max_loop_version_int IS NOT NULL "
    f" AND tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT}) "
    f"OR (tb_to_ab_max_loop_version_int IS NULL "
    f" AND tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}')"
)


def _safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


# =============================================================================
# Data loading
# =============================================================================

def build_datasets(
    activations_endpoints: pd.DataFrame,
    ab_segment: str,
    grain: str,
) -> Dict[str, pd.DataFrame]:
    """
    Aggregate per-activation endpoints to the requested grain, pair TB vs the
    requested AB segment, and split by overridePreset for per-type analysis.
    The "_all" key pools across preset types.
    """
    wide = aggregate_override_endpoints(activations_endpoints, ab_segment, grain)
    if wide.empty:
        return {}

    result: Dict[str, pd.DataFrame] = {}
    for otype, group in wide.groupby("overridePreset"):
        key = str(otype)
        if len(group) >= 3:
            result[key] = group.reset_index(drop=True)
            print(f"    {key}: {len(group)} paired groups")
        else:
            print(f"    {key}: {len(group)} paired groups (skipping, < 3)")
    if len(wide) >= 3:
        result["_all"] = wide.reset_index(drop=True)
        print(f"    _all (pooled): {len(wide)} paired groups")
    return result


def load_activations(spark) -> pd.DataFrame:
    """
    Per-activation rows from `overrides_by_segment` with cohort + guardrail +
    starting-glucose filters applied. Used by Table 8.2a (counts and hours)
    and Figure 8.2b (glucose traces). The two `is_valid_name_only_seg{2,3}`
    flags are kept on each row so downstream code can filter for the
    appropriate segment-pair.
    """
    cohort = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .where("segment_rank = 1")
        .where(_COHORT_WHERE)
        .select("_userId", "tb_to_ab_seg1_start")
    )

    bad_segments = (
        spark.table("dev.fda_510k_rwd.valid_transition_guardrails")
        .withColumn(
            "violation_count_d",
            F.coalesce(F.col("violation_count").cast("double"), F.lit(0.0)),
        )
        .groupBy("_userId", F.col("segment_start").cast("date").alias("tb_to_ab_seg1_start"))
        .agg(F.sum("violation_count_d").alias("vsum"))
        .where("vsum > 0")
        .select("_userId", "tb_to_ab_seg1_start")
    )

    cohort = cohort.join(bad_segments, on=["_userId", "tb_to_ab_seg1_start"], how="left_anti")

    activations = (
        spark.table("dev.fda_510k_rwd.overrides_by_segment")
        .join(cohort, on=["_userId", "tb_to_ab_seg1_start"], how="inner")
        .filter(F.col("is_starting_glucose_in_range") == True)  # noqa: E712
    )
    return activations.toPandas()


# =============================================================================
# Table 8.2a — Sample characteristics
# =============================================================================

def create_table_8_2a(activations: pd.DataFrame) -> pd.DataFrame:
    """
    Sample characteristics covering all three segments. "Both periods" means
    a user has at least one activation in TB and at least one in any AB
    segment (initial OR second). Per-segment counts are reported separately
    so the reader can see seg2 and seg3 cohort sizes side-by-side.
    """
    by_user_seg = (
        activations.groupby(["_userId", "segment"])
        .size()
        .unstack(fill_value=0)
    )
    for seg in (TB, AB1, AB2):
        if seg not in by_user_seg.columns:
            by_user_seg[seg] = 0
    has_tb  = by_user_seg[TB]  > 0
    has_ab1 = by_user_seg[AB1] > 0
    has_ab2 = by_user_seg[AB2] > 0
    n_both = int(((has_ab1 | has_ab2) & has_tb).sum())

    cohort_users = by_user_seg.index[(has_ab1 | has_ab2) & has_tb]
    cohort = activations[activations["_userId"].isin(cohort_users)].copy()
    cohort["window_hours"] = (
        pd.to_numeric(cohort["duration"], errors="coerce") + 7200
    ) / 3600.0

    def _median_iqr(series: pd.Series) -> str:
        if series.empty:
            return "—"
        return (
            f"{series.median():.1f} "
            f"[{series.quantile(0.25):.1f}, {series.quantile(0.75):.1f}]"
        )

    def _per_user_count(seg: str) -> pd.Series:
        return cohort[cohort["segment"] == seg].groupby("_userId").size()

    def _hours(seg: str) -> float:
        return float(cohort.loc[cohort["segment"] == seg, "window_hours"].sum())

    rows = [
        ("Users with preset use in both periods", str(n_both)),
        ("Preset activations per user, temp basal period",
            _median_iqr(_per_user_count(TB))),
        ("Preset activations per user, initial autobolus period",
            _median_iqr(_per_user_count(AB1))),
        ("Preset activations per user, second autobolus period",
            _median_iqr(_per_user_count(AB2))),
        ("Total preset + tail hours analyzed, temp basal",
            f"{_hours(TB):.1f}"),
        ("Total preset + tail hours analyzed, initial autobolus",
            f"{_hours(AB1):.1f}"),
        ("Total preset + tail hours analyzed, second autobolus",
            f"{_hours(AB2):.1f}"),
    ]
    return pd.DataFrame(rows, columns=["Characteristic", "N or Median [IQR]"])


# =============================================================================
# Tables 8.2b / 8.2c — Glycemic endpoints
# =============================================================================

def create_endpoint_table(
    datasets: Dict[str, pd.DataFrame],
    label: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build parametric and nonparametric endpoint summary tables for one
    grain × segment-pair dataset (e.g. 8.2b primary, 8.2c sensitivity)."""
    parametric_rows = []
    nonparametric_rows = []
    for otype, df in datasets.items():
        for name, c1, c2, _ in ENDPOINTS:
            if c1 not in df.columns or c2 not in df.columns:
                continue
            s = compute_paired_statistics(df[c1], df[c2])
            parametric_rows.append({
                "Override Type": otype,
                "Endpoint": name,
                "Temp Basal Mean ± SD":  f"{s['seg1_mean']:.2f} ± {s['seg1_sd']:.2f}",
                f"{label} Mean ± SD":    f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
                "Paired Diff Mean ± SD": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
                "p (paired t-test)":     format_p(s["p_ttest"]),
                "N":                     s["n_pairs"],
            })
            nonparametric_rows.append({
                "Override Type": otype,
                "Endpoint": name,
                "Temp Basal Median [IQR]":  f"{s['seg1_median']:.2f} [{s['seg1_q1']:.1f}, {s['seg1_q3']:.1f}]",
                f"{label} Median [IQR]":    f"{s['seg2_median']:.2f} [{s['seg2_q1']:.1f}, {s['seg2_q3']:.1f}]",
                "Paired Diff Median [IQR]": f"{s['diff_median']:.2f} [{s['diff_q1']:.1f}, {s['diff_q3']:.1f}]",
                "p (Wilcoxon signed-rank)": format_p(s["p_wsrt"]),
                "Normality (Shapiro p)":    format_p(s["normality_p"]),
                "N":                        s["n_pairs"],
            })
    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.2a — Paired connectors + box + violin
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

            for ax in axes[len(endpoints_subset):]:
                ax.set_visible(False)

            part_label = "i" if part == 0 else "ii"
            otype_display = "All Overrides (Pooled)" if otype == "_all" else otype.title()
            plt.suptitle(f"Figure 8.2a ({part_label}): Paired Differences — {otype_display}\n"
                         "Individual users connected by lines; box + violin overlay",
                         fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
            plt.tight_layout()

            subdir = output_dir if otype == "_all" else f"{output_dir}/by_preset"
            os.makedirs(subdir, exist_ok=True)
            path = f"{subdir}/figure_8_2a_{_safe_filename(otype)}_{part_label}.png"
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Saved: {path}")


# =============================================================================
# Figure 8.2b — Example glucose traces
# =============================================================================

def _select_demo_users(activations: pd.DataFrame, max_users: int = 5) -> Tuple[str, list]:
    """Pick the preset name with the most users having both TB and initial-AB
    paired activations, then the top-N users within it."""
    valid = activations[activations["is_valid_name_only_seg2"] == True]  # noqa: E712
    if valid.empty:
        return "", []
    by_seg = (
        valid.groupby(["overridePreset", "_userId", "segment"])
        .size()
        .unstack(fill_value=0)
    )
    for seg in (TB, AB1):
        if seg not in by_seg.columns:
            by_seg[seg] = 0
    paired = by_seg[(by_seg[TB] > 0) & (by_seg[AB1] > 0)]
    if paired.empty:
        return "", []
    by_preset_user_count = paired.reset_index().groupby("overridePreset").size()
    demo_preset = by_preset_user_count.idxmax()
    in_demo = paired.xs(demo_preset, level="overridePreset")
    in_demo = in_demo.assign(total=in_demo[TB] + in_demo[AB1])
    top_users = in_demo.sort_values("total", ascending=False).index[:max_users].tolist()
    return demo_preset, top_users


def create_figure_8_2b(spark, activations: pd.DataFrame, output_dir: str):
    demo_preset, demo_users = _select_demo_users(activations, max_users=5)
    if not demo_users:
        print("  Figure 8.2b: no users with paired activations of any preset; skipping.")
        return

    cbg = (
        spark.table("dev.fda_510k_rwd.valid_override_cbg")
        .filter(F.col("_userId").isin(demo_users))
        .filter(F.col("overridePreset") == demo_preset)
        .filter(F.col("is_valid_name_only_seg2") == True)  # noqa: E712
        .filter(F.col("is_starting_glucose_in_range") == True)  # noqa: E712
        .select("_userId", "segment", "override_time", "duration",
                "cbg_timestamp", "cbg_mg_dl")
        .toPandas()
    )
    cbg["override_time"] = pd.to_datetime(cbg["override_time"])
    cbg["cbg_timestamp"] = pd.to_datetime(cbg["cbg_timestamp"])
    cbg["cbg_mg_dl"] = pd.to_numeric(cbg["cbg_mg_dl"], errors="coerce")
    cbg["duration"] = pd.to_numeric(cbg["duration"], errors="coerce")

    n = len(demo_users)
    fig, axes = plt.subplots(n, 2, figsize=(12, 3 * n), sharey=True)
    if n == 1:
        axes = np.array([axes])

    for row, user in enumerate(demo_users):
        user_label = f"User {row + 1}"
        for col, seg in enumerate([TB, AB1]):
            ax = axes[row, col]
            user_cbg = cbg[(cbg["_userId"] == user) & (cbg["segment"] == seg)]
            if user_cbg.empty:
                ax.text(0.5, 0.5, "No CBG in window", ha="center", va="center",
                        transform=ax.transAxes, fontsize=FONT["annotation"])
                continue

            # Pick the activation with the most CBG readings (avoids the
            # cherry-picked-first-activation case where CGM was offline).
            act_counts = user_cbg.groupby("override_time").size()
            best_t0 = act_counts.idxmax()
            trace = user_cbg[user_cbg["override_time"] == best_t0].sort_values("cbg_timestamp")
            t0 = trace["override_time"].iloc[0]
            dur_s = float(trace["duration"].iloc[0]) if pd.notna(trace["duration"].iloc[0]) else 0.0

            mins = (trace["cbg_timestamp"] - t0).dt.total_seconds() / 60.0
            color = COLORS_PRIMARY if seg == TB else COLORS_SECONDARY
            ax.plot(mins, trace["cbg_mg_dl"], "-o", color=color, markersize=3, lw=1)
            ax.axhspan(70, 180, color="green", alpha=0.08)
            ax.axvline(0, color="black", lw=0.8, ls=":")
            ax.axvline(dur_s / 60.0, color="black", lw=0.8, ls=":")

            label = "Temp Basal" if seg == TB else "Autobolus (initial)"
            ax.set_title(f"{user_label} — {label}", fontsize=FONT["title"])
            if col == 0:
                ax.set_ylabel("Glucose (mg/dL)", fontsize=FONT["axis_label"])
            if row == n - 1:
                ax.set_xlabel("Minutes from activation", fontsize=FONT["axis_label"])
            ax.tick_params(axis="both", labelsize=FONT["tick"])
            ax.grid(True, alpha=0.3)

    plt.suptitle(
        f"Figure 8.2b: Example Glucose Traces During Preset Activation — {demo_preset}",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.0,
    )
    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    path = f"{output_dir}/figure_8_2b.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# =============================================================================
# Main
# =============================================================================

def _save_table_pair(
    parametric: pd.DataFrame,
    nonparametric: pd.DataFrame,
    output_dir: str,
    stem: str,
):
    parametric.to_csv(f"{output_dir}/{stem}_parametric.csv", index=False)
    nonparametric.to_csv(f"{output_dir}/{stem}_nonparametric.csv", index=False)


def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.2: Glycemic Outcomes During Preset Activation")
    print("=" * 60)

    print("\n1. Loading per-activation endpoints...")
    activations_endpoints = load_override_endpoints(spark)

    print("\n2. Loading per-activation rows for Table 8.2a + Figure 8.2b...")
    activations = load_activations(spark)
    print(f"   {len(activations)} eligible activations across "
          f"{activations['_userId'].nunique()} users")

    print("\n3. Aggregating endpoints to (user, preset_name) primary grain...")
    print("   Table 8.2b primary (TB vs initial AB):")
    ds_b_primary = build_datasets(activations_endpoints, AB1, "name")
    print("   Table 8.2c primary (TB vs second AB):")
    ds_c_primary = build_datasets(activations_endpoints, AB2, "name")

    print("\n4. Aggregating endpoints to (user, preset, params) sensitivity grain...")
    print("   Table 8.2b sensitivity:")
    ds_b_config = build_datasets(activations_endpoints, AB1, "config")
    print("   Table 8.2c sensitivity:")
    ds_c_config = build_datasets(activations_endpoints, AB2, "config")

    print("\n5. Building Table 8.2a (sample characteristics)...")
    table_a = create_table_8_2a(activations)
    table_a.to_csv(f"{output_dir}/table_8_2a.csv", index=False)
    print("\n" + table_a.to_string(index=False))

    print("\n6. Building Tables 8.2b and 8.2c...")
    if ds_b_primary:
        b_p, b_np = create_endpoint_table(ds_b_primary, label="Autobolus")
        _save_table_pair(b_p, b_np, output_dir, "table_8_2b")
        print("\nTable 8.2b (TB vs initial AB, primary grain):")
        print(b_p.to_string(index=False))
    if ds_b_config:
        b_p_c, b_np_c = create_endpoint_table(ds_b_config, label="Autobolus")
        _save_table_pair(b_p_c, b_np_c, output_dir, "table_8_2b_by_config")
    if ds_c_primary:
        c_p, c_np = create_endpoint_table(ds_c_primary, label="Autobolus (2nd)")
        _save_table_pair(c_p, c_np, output_dir, "table_8_2c")
        print("\nTable 8.2c (TB vs second AB, primary grain):")
        print(c_p.to_string(index=False))
    if ds_c_config:
        c_p_c, c_np_c = create_endpoint_table(ds_c_config, label="Autobolus (2nd)")
        _save_table_pair(c_p_c, c_np_c, output_dir, "table_8_2c_by_config")

    print("\n7. Creating Figure 8.2a (TB vs initial AB primary)...")
    if ds_b_primary:
        create_figure_8_2a(ds_b_primary, output_dir)
    else:
        print("  Skipping — no eligible groups for the primary 8.2b dataset.")

    print("\n8. Creating Figure 8.2b (example glucose traces)...")
    create_figure_8_2b(spark, activations, output_dir)

    print("\n" + "=" * 60)
    print("Analysis 8.2 Complete!")
    print("=" * 60)

    return {
        "table_8_2a": table_a,
        "datasets_8_2b_primary":     ds_b_primary,
        "datasets_8_2b_sensitivity": ds_b_config,
        "datasets_8_2c_primary":     ds_c_primary,
        "datasets_8_2c_sensitivity": ds_c_config,
        "activations": activations,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    run_in_databricks(spark)  # type: ignore[name-defined]  # noqa: F821
