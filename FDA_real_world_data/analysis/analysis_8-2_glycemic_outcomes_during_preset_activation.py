"""
=============================================================================
Analysis 8.2: Glycemic Outcomes During Preset Activation
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Compare glycemic endpoints during eligible preset activation
windows (preset duration + 2-hour tail) between the temp-basal and
autobolus phases of a TB→AB transition.

Inclusion (applied by utils.data_loading.load_override_endpoints):
- Cohort: Loop version below MAX_LOOP_VERSION_INT (or, if version unknown,
  segment ending before MAX_SEG2_END_DATE)
- No guardrail violations on the rank-1 transition segment
- is_valid_name_only = TRUE (preset name appears >= 2 times in each phase)
- is_starting_glucose_in_range = TRUE (CBG within 30 min before activation
  is between STARTING_GLUCOSE_LOW and STARTING_GLUCOSE_HIGH)

Outputs:
- Table 8.2a: Sample characteristics (users, activations/segment, hours/segment)
- Table 8.2b: Glycemic endpoints during preset activation (TB vs initial AB)
- Table 8.2c: Glycemic endpoints two weeks after transition (DEFERRED;
  requires a third transition segment that the pipeline does not yet emit)
- Figure 8.2a: Paired differences per endpoint (paired connectors + box + violin)
- Figure 8.2b: Example glucose traces during preset activation, TB vs AB
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
    load_override_endpoints,
)
from utils.statistics import compute_paired_statistics, format_p


OUTPUT_DIR = "outputs/analysis_8_2"

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

# Cohort/guardrail predicate, mirrored from load_transition_endpoints. Used
# by the activation loader (load_activations) to filter overrides_by_segment.
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

def load_data(spark) -> Dict[str, pd.DataFrame]:
    """
    Load filtered glycemic endpoints and pivot to one row per (user, config)
    with paired seg1/seg2 columns. Splits by overridePreset for per-type
    analysis; the "_all" key is pooled across types.
    """
    endpoints = load_override_endpoints(spark)

    # Drop the validity columns now that filtering is done — they're constant
    # within the surviving rows.
    endpoints = endpoints.drop(
        columns=["is_valid_name_only", "is_starting_glucose_in_range"],
        errors="ignore",
    )

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

    result = {}
    for otype, group in wide.groupby("overridePreset"):
        if len(group) >= 3:
            result[otype] = group.reset_index(drop=True)
            print(f"  {otype}: {len(group)} paired configs")
        else:
            print(f"  {otype}: {len(group)} paired configs (skipping, < 3)")

    if len(wide) >= 3:
        result["_all"] = wide.reset_index(drop=True)
        print(f"  _all (pooled): {len(wide)} paired configs")

    return result


def load_activations(spark) -> pd.DataFrame:
    """
    Load per-activation rows from overrides_by_segment with the same cohort
    and inclusion filters as load_override_endpoints. Returns one row per
    eligible activation, used by Table 8.2a and Figure 8.2b.
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
        .filter(F.col("is_valid_name_only") == True)  # noqa: E712
        .filter(F.col("is_starting_glucose_in_range") == True)  # noqa: E712
    )

    return activations.toPandas()


# =============================================================================
# Table 8.2a — Sample characteristics
# =============================================================================

def create_table_8_2a(activations: pd.DataFrame) -> pd.DataFrame:
    """
    Sample characteristics for the preset glycemic analysis.

    Counts and durations come from `activations` (per-activation rows post
    cohort + inclusion filters). The "second autobolus period" rows are
    placeholders pending the seg3 (days 14–28) pipeline change tracked for
    Table 8.2c.
    """
    # Both-period users: have ≥1 activation in TB AND ≥1 in initial AB.
    by_user_seg = (
        activations.groupby(["_userId", "dosing_mode"])
        .size()
        .unstack(fill_value=0)
    )
    both = by_user_seg[(by_user_seg.get(SEG1, 0) > 0) & (by_user_seg.get(SEG2, 0) > 0)]
    n_both = len(both)

    def _median_iqr(series: pd.Series) -> str:
        if series.empty:
            return "—"
        return (
            f"{series.median():.1f} "
            f"[{series.quantile(0.25):.1f}, {series.quantile(0.75):.1f}]"
        )

    # Activations-per-user is computed only over users who appear in both
    # periods, so the per-period summaries describe the analyzed cohort.
    cohort_users = both.index
    cohort_acts = activations[activations["_userId"].isin(cohort_users)]
    tb_per_user = cohort_acts[cohort_acts["dosing_mode"] == SEG1].groupby("_userId").size()
    ab_per_user = cohort_acts[cohort_acts["dosing_mode"] == SEG2].groupby("_userId").size()

    # Total preset+tail hours: (duration + 7200s) per activation, in hours.
    cohort_acts = cohort_acts.assign(
        window_hours=(pd.to_numeric(cohort_acts["duration"], errors="coerce") + 7200) / 3600.0
    )
    tb_hours = cohort_acts.loc[cohort_acts["dosing_mode"] == SEG1, "window_hours"].sum()
    ab_hours = cohort_acts.loc[cohort_acts["dosing_mode"] == SEG2, "window_hours"].sum()

    rows = [
        ("Users with preset use in both periods", str(n_both)),
        ("Preset activations per user, temp basal period", _median_iqr(tb_per_user)),
        ("Preset activations per user, initial autobolus period", _median_iqr(ab_per_user)),
        ("Preset activations per user, second autobolus period", "N/A*"),
        ("Total preset + tail hours analyzed, temp basal", f"{tb_hours:.1f}"),
        ("Total preset + tail hours analyzed, initial autobolus", f"{ab_hours:.1f}"),
        ("Total preset + tail hours analyzed, second autobolus", "N/A*"),
    ]

    table = pd.DataFrame(rows, columns=["Characteristic", "N or Median [IQR]"])
    table.attrs["footnote"] = (
        "*Reserved for Table 8.2c (days 14–28 post-transition); pending "
        "addition of a third transition segment to the staging pipeline."
    )
    return table


# =============================================================================
# Table 8.2b — Glycemic endpoints (TB vs initial AB)
# =============================================================================

def create_table_8_2b(datasets: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
                "Autobolus Mean ± SD":   f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
                "Paired Diff Mean ± SD": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
                "p (paired t-test)": format_p(s['p_ttest']),
                "N": s["n_pairs"],
            })

            nonparametric_rows.append({
                "Override Type": otype,
                "Endpoint": name,
                "Temp Basal Median [IQR]":  f"{s['seg1_median']:.2f} [{s['seg1_q1']:.1f}, {s['seg1_q3']:.1f}]",
                "Autobolus Median [IQR]":   f"{s['seg2_median']:.2f} [{s['seg2_q1']:.1f}, {s['seg2_q3']:.1f}]",
                "Paired Diff Median [IQR]": f"{s['diff_median']:.2f} [{s['diff_q1']:.1f}, {s['diff_q3']:.1f}]",
                "p (Wilcoxon signed-rank)": format_p(s['p_wsrt']),
                "Normality (Shapiro p)": format_p(s['normality_p']),
                "N": s["n_pairs"],
            })

    return pd.DataFrame(parametric_rows), pd.DataFrame(nonparametric_rows)


# =============================================================================
# Figure 8.2a — Paired connectors + box + violin (per override type)
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
                         "Individual override configs connected by lines; box + violin overlay",
                         fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
            plt.tight_layout()

            subdir = output_dir if otype == "_all" else f"{output_dir}/by_preset"
            os.makedirs(subdir, exist_ok=True)
            path = f"{subdir}/figure_8_2a_{_safe_filename(otype)}_{part_label}.png"
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Saved: {path}")


# =============================================================================
# Figure 8.2b — Example glucose traces during preset activation
# =============================================================================

def _select_demo_users(activations: pd.DataFrame, max_users: int = 5) -> Tuple[str, list]:
    """
    Pick a demo preset (most paired users) and the top users within it
    (most paired activations across TB and AB).
    """
    paired = (
        activations.groupby(["overridePreset", "_userId", "dosing_mode"])
        .size()
        .unstack(fill_value=0)
    )
    paired_both = paired[(paired.get(SEG1, 0) > 0) & (paired.get(SEG2, 0) > 0)]
    if paired_both.empty:
        return "", []

    by_preset_user_count = paired_both.reset_index().groupby("overridePreset").size()
    demo_preset = by_preset_user_count.idxmax()

    in_demo = paired_both.xs(demo_preset, level="overridePreset")
    in_demo = in_demo.assign(total=in_demo[SEG1] + in_demo[SEG2])
    top_users = in_demo.sort_values("total", ascending=False).index[:max_users].tolist()
    return demo_preset, top_users


def create_figure_8_2b(spark, activations: pd.DataFrame, output_dir: str):
    """
    Plot CGM traces for 3–5 representative users showing one TB activation
    and one AB activation of the same demo preset, with the activation +
    2-hour-tail window highlighted.
    """
    demo_preset, demo_users = _select_demo_users(activations, max_users=5)
    if not demo_users:
        print("  Figure 8.2b: no users with paired activations of any preset; skipping.")
        return

    # Pull CBG only for the chosen users + demo preset (small slice).
    cbg = (
        spark.table("dev.fda_510k_rwd.valid_override_cbg")
        .filter(F.col("_userId").isin(demo_users))
        .filter(F.col("overridePreset") == demo_preset)
        .filter(F.col("is_valid_name_only") == True)  # noqa: E712
        .filter(F.col("is_starting_glucose_in_range") == True)  # noqa: E712
        .select("_userId", "segment", "cbg_timestamp", "cbg_mg_dl")
        .toPandas()
    )

    # Per-activation lookup: pick the first eligible TB activation and first
    # eligible AB activation for each user. For Figure 8.2b we just need one
    # representative trace per user per phase.
    demo_acts = activations[
        (activations["overridePreset"] == demo_preset)
        & (activations["_userId"].isin(demo_users))
    ].copy()
    demo_acts["override_time"] = pd.to_datetime(demo_acts["override_time"])
    demo_acts["duration"] = pd.to_numeric(demo_acts["duration"], errors="coerce")

    cbg["cbg_timestamp"] = pd.to_datetime(cbg["cbg_timestamp"])
    cbg["cbg_mg_dl"] = pd.to_numeric(cbg["cbg_mg_dl"], errors="coerce")

    n = len(demo_users)
    fig, axes = plt.subplots(n, 2, figsize=(12, 3 * n), sharey=True)
    if n == 1:
        axes = np.array([axes])

    for row, user in enumerate(demo_users):
        user_label = f"User {row + 1}"
        for col, mode in enumerate([SEG1, SEG2]):
            ax = axes[row, col]
            user_acts = demo_acts[
                (demo_acts["_userId"] == user) & (demo_acts["dosing_mode"] == mode)
            ].sort_values("override_time")
            if user_acts.empty:
                ax.text(0.5, 0.5, "No activation", ha="center", va="center",
                        transform=ax.transAxes, fontsize=FONT["annotation"])
                continue
            act = user_acts.iloc[0]
            t0 = act["override_time"]
            dur_s = float(act["duration"]) if pd.notna(act["duration"]) else 0.0
            window_end = t0 + pd.Timedelta(seconds=dur_s + 7200)

            trace = cbg[
                (cbg["_userId"] == user)
                & (cbg["segment"] == mode)
                & (cbg["cbg_timestamp"] >= t0)
                & (cbg["cbg_timestamp"] <= window_end)
            ].sort_values("cbg_timestamp")
            if trace.empty:
                ax.text(0.5, 0.5, "No CBG in window", ha="center", va="center",
                        transform=ax.transAxes, fontsize=FONT["annotation"])
                continue

            mins = (trace["cbg_timestamp"] - t0).dt.total_seconds() / 60.0
            color = COLORS_PRIMARY if mode == SEG1 else COLORS_SECONDARY
            ax.plot(mins, trace["cbg_mg_dl"], "-o", color=color, markersize=3, lw=1)
            ax.axhspan(70, 180, color="green", alpha=0.08)
            ax.axvline(0, color="black", lw=0.8, ls=":")
            ax.axvline(dur_s / 60.0, color="black", lw=0.8, ls=":")

            label = "Temp Basal" if mode == SEG1 else "Autobolus"
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

def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.2: Glycemic Outcomes During Preset Activation")
    print("=" * 60)

    print("\n1. Loading paired endpoints...")
    datasets = load_data(spark)

    print("\n2. Loading per-activation rows (for Table 8.2a + Figure 8.2b)...")
    activations = load_activations(spark)
    print(f"   {len(activations)} eligible activations across "
          f"{activations['_userId'].nunique()} users")

    print("\n3. Creating Table 8.2a (sample characteristics)...")
    table_a = create_table_8_2a(activations)
    table_a.to_csv(f"{output_dir}/table_8_2a.csv", index=False)
    print("\n" + table_a.to_string(index=False))
    if "footnote" in table_a.attrs:
        print(table_a.attrs["footnote"])

    print("\n4. Creating Table 8.2b (TB vs initial AB endpoints)...")
    table_b_p, table_b_np = create_table_8_2b(datasets)
    table_b_p.to_csv(f"{output_dir}/table_8_2b_parametric.csv", index=False)
    table_b_np.to_csv(f"{output_dir}/table_8_2b_nonparametric.csv", index=False)
    print("\n" + table_b_p.to_string(index=False))

    print("\n5. Creating Figure 8.2a...")
    create_figure_8_2a(datasets, output_dir)

    print("\n6. Creating Figure 8.2b (example glucose traces)...")
    create_figure_8_2b(spark, activations, output_dir)

    print("\n" + "=" * 60)
    print("Analysis 8.2 Complete!")
    print("=" * 60)

    return {
        "table_8_2a": table_a,
        "table_8_2b_parametric": table_b_p,
        "table_8_2b_nonparametric": table_b_np,
        "datasets": datasets,
        "activations": activations,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    run_in_databricks(spark)  # type: ignore[name-defined]  # noqa: F821
