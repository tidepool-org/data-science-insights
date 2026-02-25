"""
=============================================================================
Analysis 8.8: Glycemic Outcomes Stratified by Carbohydrate Consumption
               Consistency
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Evaluate glycemic outcomes stratified by carbohydrate consumption
consistency across delivery strategy periods to assess potential confounding
variables from dietary behavior changes.

Rationale: Changes in dietary habits between the temporary basal and autobolus
periods could confound the observed treatment effect.  By stratifying users
based on carbohydrate intake consistency, this analysis examines whether the
treatment effect differs between users who maintained stable dietary patterns
versus those who changed their eating habits.

Population: Users with valid TB→AB transitions with reported carbohydrate data
in both temporary basal and autobolus segments.

Definitions:
- Consistent carbohydrate consumption: ≤25% change in total reported
  carbohydrates between periods
- Inconsistent carbohydrate consumption: >25% change in total reported
  carbohydrates between periods

Inputs:
- dev.fda_510k_rwd.glycemic_endpoints_transition  (long: _userId, segment, ...)
- dev.fda_510k_rwd.valid_transition_guardrails     (_userId, violation_count)
- dev.fda_510k_rwd.valid_transition_carbs           (_userId, carb_grams,
                                                     carb_timestamp, segment)

Outputs:
- Table 8.8a: Carbohydrate Consumption by Delivery Strategy Period
- Table 8.8b: Glycemic Outcomes — Consistent Carbohydrate Consumers (±25%)
- Table 8.8c: Glycemic Outcomes — Inconsistent Carbohydrate Consumers (>25%)
- Table 8.8d: Comparison of Treatment Effect by Carbohydrate Consistency
- Figure 8.8a: Distribution of Carbohydrate Change Between Periods
- Figure 8.8b: Treatment Effect on TIR by Carbohydrate Consistency Group
- Figure 8.8c: Individual Paired Outcomes by Carbohydrate Consistency
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
from typing import Dict, Optional, Tuple
import os

# =============================================================================
# Configuration
# =============================================================================
OUTPUT_DIR = "outputs/analysis_8_8"
SEG1 = "tb_to_ab_seg1"  # temp basal period
SEG2 = "tb_to_ab_seg2"  # autobolus period
SEGMENT_DAYS = 14
CHO_CHANGE_THRESHOLD = 0.25  # 25% change threshold

# Glycemic endpoints: (display_name, col_prefix, unit)
GLYCEMIC_OUTCOMES = [
    ("TIR 70–180 mg/dL (%)",           "tir",           "%"),
    ("TBR <70 mg/dL (%)",              "tbr",           "%"),
    ("TBR <54 mg/dL (%)",              "tbr_very_low",  "%"),
    ("TAR >180 mg/dL (%)",             "tar",           "%"),
    ("Mean glucose (mg/dL)",            "mean_glucose",  "mg/dL"),
    ("CV (%)",                          "cv",            "%"),
]

# =============================================================================
# Font sizes — centralized so every figure stays consistent
# =============================================================================
FONT = {
    "suptitle":   18,
    "title":      15,
    "axis_label": 14,
    "tick":       12,
    "legend":     11,
    "annotation": 13,
}

# Color scheme
COLORS_PRIMARY   = "#607cff"
COLORS_SECONDARY = "#4f59be"
COLORS_ACCENT    = "#241144"
COLOR_CONSISTENT   = "#76D3A6"  # green — stable diet
COLOR_INCONSISTENT = "#FF8B7C"  # orange-red — changed diet
COLOR_IMPROVED   = "#76D3A6"
COLOR_WORSENED   = "#FF8B7C"


# =============================================================================
# Statistics helpers
# =============================================================================

def test_normality(data: pd.Series, alpha: float = 0.05) -> Tuple[bool, float]:
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
    d_mean,  d_sd,  d_med,  d_q1,  d_q3  = _summary(diff)

    p_ttest = np.nan
    if len(diff) >= 3:
        _, p_ttest = stats.ttest_rel(s1, s2)

    p_wsrt = np.nan
    if len(diff) >= 3:
        try:
            _, p_wsrt = stats.wilcoxon(s1, s2)
        except ValueError:
            pass

    d_ci_low = d_ci_hi = np.nan
    if len(diff) >= 2:
        se = diff.std(ddof=1) / np.sqrt(len(diff))
        t_crit = stats.t.ppf(0.975, df=len(diff) - 1)
        d_ci_low = d_mean - t_crit * se
        d_ci_hi  = d_mean + t_crit * se

    return {
        "seg1_mean": s1_mean, "seg1_sd": s1_sd,
        "seg1_median": s1_med, "seg1_q1": s1_q1, "seg1_q3": s1_q3,
        "seg2_mean": s2_mean, "seg2_sd": s2_sd,
        "seg2_median": s2_med, "seg2_q1": s2_q1, "seg2_q3": s2_q3,
        "diff_mean": d_mean, "diff_sd": d_sd,
        "diff_ci_low": d_ci_low, "diff_ci_hi": d_ci_hi,
        "diff_median": d_med, "diff_q1": d_q1, "diff_q3": d_q3,
        "p_ttest": p_ttest, "p_wsrt": p_wsrt,
        "normality_p": norm_p, "is_normal": is_normal,
        "n_pairs": len(diff),
    }


def _format_p(p: float) -> str:
    if np.isnan(p):
        return "N/A"
    if p < 0.001:
        return f"p={p:.2e}"
    return f"p={p:.3f}"


# =============================================================================
# Data Loading
# =============================================================================

def load_data(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints and carbohydrate data, apply filters, compute
    carbohydrate summary metrics per user per segment, classify users as
    consistent or inconsistent, and return a wide DataFrame.
    """
    # --- Glycemic endpoints ---
    endpoints = spark.table(
        "dev.fda_510k_rwd.glycemic_endpoints_transition"
    ).toPandas()

    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "segment"):
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # CBG coverage filter (70% of 14-day period)
    endpoints = endpoints.loc[endpoints["cbg_count"] >= 14 * 288 * 0.7].copy()

    # --- Guardrails exclusion ---
    guardrails = (
        spark.table("dev.fda_510k_rwd.valid_transition_guardrails")
        .select("_userId", "violation_count")
        .toPandas()
    )
    guardrails["violation_count"] = pd.to_numeric(
        guardrails["violation_count"], errors="coerce"
    ).fillna(0)
    excluded_users = guardrails.loc[
        guardrails["violation_count"] > 0, "_userId"
    ].unique()
    endpoints = endpoints[~endpoints["_userId"].isin(excluded_users)]
    print(f"  Excluded {len(excluded_users)} users with guardrail violations")

    # --- Pivot glycemic endpoints to wide ---
    seg1 = (
        endpoints[endpoints["segment"] == SEG1]
        .set_index("_userId")
        .add_suffix("_seg1")
    )
    seg2 = (
        endpoints[endpoints["segment"] == SEG2]
        .set_index("_userId")
        .add_suffix("_seg2")
    )
    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")
    wide = wide.reset_index()

    # --- Carbohydrate data ---
    carbs = spark.table("dev.fda_510k_rwd.valid_transition_carbs").toPandas()
    carbs["carb_grams"] = pd.to_numeric(carbs["carb_grams"], errors="coerce")
    # Drop rows with missing or zero carb grams
    carbs = carbs.loc[carbs["carb_grams"].notna() & (carbs["carb_grams"] > 0)]

    # Summarize carb data per user per segment
    carb_summary = (
        carbs.groupby(["_userId", "segment"])
        .agg(
            total_cho=("carb_grams", "sum"),
            carb_entries=("carb_grams", "count"),
        )
        .reset_index()
    )
    carb_summary["daily_cho"] = carb_summary["total_cho"] / SEGMENT_DAYS
    carb_summary["entries_per_day"] = carb_summary["carb_entries"] / SEGMENT_DAYS

    # Pivot carb summary to wide
    carb_seg1 = (
        carb_summary[carb_summary["segment"] == SEG1]
        .set_index("_userId")
        .add_suffix("_seg1")
    )
    carb_seg2 = (
        carb_summary[carb_summary["segment"] == SEG2]
        .set_index("_userId")
        .add_suffix("_seg2")
    )
    carb_wide = carb_seg1.join(carb_seg2, how="inner").reset_index()
    carb_wide = carb_wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")

    # --- Compute percent change in total CHO ---
    # Percent change from baseline: (seg2 - seg1) / seg1 * 100
    carb_wide["cho_pct_change"] = np.where(
        carb_wide["total_cho_seg1"] > 0,
        (carb_wide["total_cho_seg2"] - carb_wide["total_cho_seg1"]) / carb_wide["total_cho_seg1"] * 100,
        np.nan,
    )
    carb_wide["cho_abs_pct_change"] = carb_wide["cho_pct_change"].abs()

    # --- Classify consistency ---
    carb_wide["cho_consistent"] = (
        carb_wide["cho_abs_pct_change"] <= CHO_CHANGE_THRESHOLD * 100
    )
    carb_wide["cho_group"] = np.where(
        carb_wide["cho_consistent"],
        "Consistent (≤25%)",
        "Inconsistent (>25%)",
    )
    # Direction of change for inconsistent users
    carb_wide["cho_direction"] = np.where(
        carb_wide["cho_pct_change"] > CHO_CHANGE_THRESHOLD * 100,
        "Increased (>25%)",
        np.where(
            carb_wide["cho_pct_change"] < -CHO_CHANGE_THRESHOLD * 100,
            "Decreased (>25%)",
            "Stable",
        ),
    )

    # --- Merge with glycemic endpoints ---
    # Only keep users who have both carb data in both segments AND glycemic data
    merged = wide.merge(carb_wide, on="_userId", how="inner")

    n_glyc = len(wide)
    n_carb = len(carb_wide)
    n_merged = len(merged)
    print(f"  Users with paired glycemic data: {n_glyc}")
    print(f"  Users with carb data in both segments: {n_carb}")
    print(f"  Users in final analysis (glycemic + carb): {n_merged}")
    print(
        f"  Consistent CHO: {merged['cho_consistent'].sum()} "
        f"({100 * merged['cho_consistent'].mean():.1f}%)"
    )
    print(
        f"  Inconsistent CHO: {(~merged['cho_consistent']).sum()} "
        f"({100 * (~merged['cho_consistent']).mean():.1f}%)"
    )

    return merged


# =============================================================================
# Table 8.8a — Carbohydrate Consumption by Delivery Strategy Period
# =============================================================================

def create_table_8_8a(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """Carbohydrate consumption summary by delivery strategy period."""
    n_total = len(df)

    def _median_iqr(series):
        s = series.dropna()
        if len(s) < 3:
            return "N/A"
        return f"{s.median():.1f} ({s.quantile(0.25):.1f}, {s.quantile(0.75):.1f})"

    rows = []

    rows.append({
        "Metric": "Users with carb data, n",
        "Temp Basal": str(n_total),
        "Autobolus": str(n_total),
        "Difference": "—",
    })

    # Total CHO
    diff_total = df["total_cho_seg2"] - df["total_cho_seg1"]
    rows.append({
        "Metric": "Total CHO (g), median (IQR)",
        "Temp Basal": _median_iqr(df["total_cho_seg1"]),
        "Autobolus": _median_iqr(df["total_cho_seg2"]),
        "Difference": _median_iqr(diff_total),
    })

    # Daily CHO
    diff_daily = df["daily_cho_seg2"] - df["daily_cho_seg1"]
    rows.append({
        "Metric": "Daily CHO (g/day), median (IQR)",
        "Temp Basal": _median_iqr(df["daily_cho_seg1"]),
        "Autobolus": _median_iqr(df["daily_cho_seg2"]),
        "Difference": _median_iqr(diff_daily),
    })

    # CHO entries per day
    diff_entries = df["entries_per_day_seg2"] - df["entries_per_day_seg1"]
    rows.append({
        "Metric": "CHO entries per day, median (IQR)",
        "Temp Basal": _median_iqr(df["entries_per_day_seg1"]),
        "Autobolus": _median_iqr(df["entries_per_day_seg2"]),
        "Difference": _median_iqr(diff_entries),
    })

    # Consistency classification
    n_consistent = df["cho_consistent"].sum()
    n_inconsistent = n_total - n_consistent
    rows.append({
        "Metric": "Users with consistent CHO (±25%), n (%)",
        "Temp Basal": "",
        "Autobolus": "",
        "Difference": f"{n_consistent} ({100 * n_consistent / n_total:.1f}%)",
    })
    rows.append({
        "Metric": "Users with inconsistent CHO (>25% change), n (%)",
        "Temp Basal": "",
        "Autobolus": "",
        "Difference": f"{n_inconsistent} ({100 * n_inconsistent / n_total:.1f}%)",
    })

    # Direction among inconsistent users
    n_increased = (df["cho_direction"] == "Increased (>25%)").sum()
    n_decreased = (df["cho_direction"] == "Decreased (>25%)").sum()
    rows.append({
        "Metric": "— Increased CHO (>25%), n (%)",
        "Temp Basal": "",
        "Autobolus": "",
        "Difference": f"{n_increased} ({100 * n_increased / n_total:.1f}%)",
    })
    rows.append({
        "Metric": "— Decreased CHO (>25%), n (%)",
        "Temp Basal": "",
        "Autobolus": "",
        "Difference": f"{n_decreased} ({100 * n_decreased / n_total:.1f}%)",
    })

    table = pd.DataFrame(rows)
    out = f"{output_dir}/table_8_8a_carbohydrate_consumption.csv"
    table.to_csv(out, index=False)
    print(f"  Saved: {out}")
    return table


# =============================================================================
# Table 8.8b — Glycemic Outcomes: Consistent Carbohydrate Consumers
# =============================================================================

def _create_glycemic_outcome_table(
    df: pd.DataFrame,
    group_label: str,
    output_dir: str,
    table_id: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Paired glycemic outcome comparison (AB vs TB) for a filtered subgroup.
    Returns (parametric_table, nonparametric_table).
    """
    parametric_rows = []
    nonparametric_rows = []

    for name, col_prefix, unit in GLYCEMIC_OUTCOMES:
        c1 = f"{col_prefix}_seg1"
        c2 = f"{col_prefix}_seg2"
        if c1 not in df.columns or c2 not in df.columns:
            print(f"  Warning: {c1}/{c2} not found, skipping {name}")
            continue

        s = compute_paired_statistics(df[c1], df[c2])
        ci_str = (
            f"({s['diff_ci_low']:.2f}, {s['diff_ci_hi']:.2f})"
            if not np.isnan(s["diff_ci_low"]) else "N/A"
        )

        parametric_rows.append({
            "Outcome": name,
            "Temp Basal (Mean ± SD)": f"{s['seg1_mean']:.2f} ± {s['seg1_sd']:.2f}",
            "Autobolus (Mean ± SD)": f"{s['seg2_mean']:.2f} ± {s['seg2_sd']:.2f}",
            "Δ (AB − TB)": f"{s['diff_mean']:.2f} ± {s['diff_sd']:.2f}",
            "95% CI": ci_str,
            "p-value": _format_p(s["p_ttest"]),
            "n": s["n_pairs"],
        })

        nonparametric_rows.append({
            "Outcome": name,
            "Temp Basal (Median [IQR])": (
                f"{s['seg1_median']:.2f} [{s['seg1_q1']:.2f}, {s['seg1_q3']:.2f}]"
            ),
            "Autobolus (Median [IQR])": (
                f"{s['seg2_median']:.2f} [{s['seg2_q1']:.2f}, {s['seg2_q3']:.2f}]"
            ),
            "Δ (AB − TB) Median [IQR]": (
                f"{s['diff_median']:.2f} [{s['diff_q1']:.2f}, {s['diff_q3']:.2f}]"
            ),
            "p-value (Wilcoxon)": _format_p(s["p_wsrt"]),
            "n": s["n_pairs"],
        })

    tbl_p = pd.DataFrame(parametric_rows)
    tbl_np = pd.DataFrame(nonparametric_rows)

    out_p = f"{output_dir}/{table_id}_parametric.csv"
    out_np = f"{output_dir}/{table_id}_nonparametric.csv"
    tbl_p.to_csv(out_p, index=False)
    tbl_np.to_csv(out_np, index=False)
    print(f"  Saved: {out_p}")
    print(f"  Saved: {out_np}")

    return tbl_p, tbl_np


def create_table_8_8b(df: pd.DataFrame, output_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Glycemic outcomes for consistent carbohydrate consumers (±25% change)."""
    consistent = df[df["cho_consistent"]].copy()
    print(f"  Consistent CHO users: {len(consistent)}")
    return _create_glycemic_outcome_table(
        consistent, "Consistent (≤25%)", output_dir, "table_8_8b_consistent"
    )


def create_table_8_8c(df: pd.DataFrame, output_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Glycemic outcomes for inconsistent carbohydrate consumers (>25% change)."""
    inconsistent = df[~df["cho_consistent"]].copy()
    print(f"  Inconsistent CHO users: {len(inconsistent)}")
    return _create_glycemic_outcome_table(
        inconsistent, "Inconsistent (>25%)", output_dir, "table_8_8c_inconsistent"
    )


# =============================================================================
# Table 8.8d — Comparison of Treatment Effect by Carbohydrate Consistency
# =============================================================================

def create_table_8_8d(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """
    Compare the treatment effect (Δ = AB − TB) between consistent and
    inconsistent CHO groups.  Uses independent-samples t-test and
    Mann-Whitney U test.
    """
    consistent = df[df["cho_consistent"]].copy()
    inconsistent = df[~df["cho_consistent"]].copy()
    n_con = len(consistent)
    n_inc = len(inconsistent)

    rows = []
    for name, col_prefix, unit in GLYCEMIC_OUTCOMES:
        c1 = f"{col_prefix}_seg1"
        c2 = f"{col_prefix}_seg2"
        if c1 not in df.columns or c2 not in df.columns:
            continue

        # Compute deltas for each group
        delta_con = (
            consistent[c2].astype(float) - consistent[c1].astype(float)
        ).dropna()
        delta_inc = (
            inconsistent[c2].astype(float) - inconsistent[c1].astype(float)
        ).dropna()

        # Summary for consistent group
        if len(delta_con) >= 2:
            con_mean = delta_con.mean()
            con_se = delta_con.std(ddof=1) / np.sqrt(len(delta_con))
            con_t_crit = stats.t.ppf(0.975, df=len(delta_con) - 1)
            con_ci_lo = con_mean - con_t_crit * con_se
            con_ci_hi = con_mean + con_t_crit * con_se
            con_str = f"{con_mean:.2f} ({con_ci_lo:.2f}, {con_ci_hi:.2f})"
        else:
            con_mean = np.nan
            con_str = "N/A"

        # Summary for inconsistent group
        if len(delta_inc) >= 2:
            inc_mean = delta_inc.mean()
            inc_se = delta_inc.std(ddof=1) / np.sqrt(len(delta_inc))
            inc_t_crit = stats.t.ppf(0.975, df=len(delta_inc) - 1)
            inc_ci_lo = inc_mean - inc_t_crit * inc_se
            inc_ci_hi = inc_mean + inc_t_crit * inc_se
            inc_str = f"{inc_mean:.2f} ({inc_ci_lo:.2f}, {inc_ci_hi:.2f})"
        else:
            inc_mean = np.nan
            inc_str = "N/A"

        # Difference in treatment effect
        if not np.isnan(con_mean) and not np.isnan(inc_mean):
            diff_effect = inc_mean - con_mean
            diff_str = f"{diff_effect:.2f}"
        else:
            diff_str = "N/A"

        # Interaction p-value: independent-samples t-test on deltas
        p_interaction = np.nan
        if len(delta_con) >= 3 and len(delta_inc) >= 3:
            _, p_interaction = stats.ttest_ind(
                delta_con, delta_inc, equal_var=False
            )
            # Also compute Mann-Whitney U as non-parametric alternative
            _, p_mwu = stats.mannwhitneyu(
                delta_con, delta_inc, alternative="two-sided"
            )
        else:
            p_mwu = np.nan

        rows.append({
            "Outcome": name,
            f"Consistent CHO (n={n_con}) Δ (95% CI)": con_str,
            f"Inconsistent CHO (n={n_inc}) Δ (95% CI)": inc_str,
            "Difference in Effect": diff_str,
            "p (interaction, t-test)": _format_p(p_interaction),
            "p (interaction, MWU)": _format_p(p_mwu),
        })

    table = pd.DataFrame(rows)
    out = f"{output_dir}/table_8_8d_treatment_effect_comparison.csv"
    table.to_csv(out, index=False)
    print(f"  Saved: {out}")
    return table


# =============================================================================
# Figure 8.8a — Distribution of Carbohydrate Change Between Periods
# =============================================================================

def create_figure_8_8a(df: pd.DataFrame, output_path: str):
    """
    Histogram of percent change in total CHO (seg2 vs seg1).
    Vertical dashed lines at ±25% mark the consistency threshold.
    Shaded region between −25% and +25% = "consistent".
    """
    pct_change = df["cho_pct_change"].dropna()

    fig, ax = plt.subplots(figsize=(12, 7))

    # Histogram
    bins = np.linspace(
        max(pct_change.min(), -150),
        min(pct_change.max(), 150),
        40,
    )
    n_vals, bin_edges, patches = ax.hist(
        pct_change.clip(bins[0], bins[-1]),
        bins=bins,
        edgecolor="black",
        alpha=0.8,
        color=COLORS_PRIMARY,
        zorder=3,
    )

    # Color bars by group
    for patch, left_edge in zip(patches, bin_edges[:-1]):
        right_edge = left_edge + (bin_edges[1] - bin_edges[0])
        mid = (left_edge + right_edge) / 2
        if -CHO_CHANGE_THRESHOLD * 100 <= mid <= CHO_CHANGE_THRESHOLD * 100:
            patch.set_facecolor(COLOR_CONSISTENT)
        else:
            patch.set_facecolor(COLOR_INCONSISTENT)

    # Shaded consistency region
    ax.axvspan(
        -CHO_CHANGE_THRESHOLD * 100,
        CHO_CHANGE_THRESHOLD * 100,
        alpha=0.1,
        color=COLOR_CONSISTENT,
        zorder=1,
    )

    # Threshold lines
    ax.axvline(
        -CHO_CHANGE_THRESHOLD * 100, color=COLORS_ACCENT, ls="--", lw=2,
        label=f"±{int(CHO_CHANGE_THRESHOLD * 100)}% threshold",
    )
    ax.axvline(
        CHO_CHANGE_THRESHOLD * 100, color=COLORS_ACCENT, ls="--", lw=2,
    )

    # Zero reference
    ax.axvline(0, color="black", ls="-", lw=1.5, alpha=0.5)

    # Summary counts
    n_con = (pct_change.abs() <= CHO_CHANGE_THRESHOLD * 100).sum()
    n_inc = (pct_change.abs() > CHO_CHANGE_THRESHOLD * 100).sum()
    ax.text(
        0.02, 0.95,
        f"Consistent (±25%): n={n_con} ({100 * n_con / len(pct_change):.1f}%)\n"
        f"Inconsistent (>25%): n={n_inc} ({100 * n_inc / len(pct_change):.1f}%)",
        transform=ax.transAxes,
        fontsize=FONT["annotation"],
        verticalalignment="top",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
    )

    ax.set_xlabel(
        "Percent Change in Total Carbohydrate Consumption\n(Autobolus vs Temp Basal)",
        fontsize=FONT["axis_label"],
    )
    ax.set_ylabel("Number of Users", fontsize=FONT["axis_label"])
    ax.tick_params(axis="both", labelsize=FONT["tick"])
    ax.legend(fontsize=FONT["legend"], loc="upper right")
    ax.grid(axis="y", alpha=0.3)

    ax.set_title(
        "Figure 8.8a: Distribution of Carbohydrate Change Between Periods\n"
        "Shaded region = consistent consumers (−25% to +25%)",
        fontsize=FONT["suptitle"],
        fontweight="bold",
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Figure 8.8b — Treatment Effect on TIR by Carbohydrate Consistency Group
# =============================================================================

def create_figure_8_8b(df: pd.DataFrame, output_path: str):
    """
    Forest plot of paired treatment effect (AB − TB) on TIR for consistent
    and inconsistent carbohydrate consumers, with 95% CIs.
    """
    consistent = df[df["cho_consistent"]].copy()
    inconsistent = df[~df["cho_consistent"]].copy()

    fig, ax = plt.subplots(figsize=(10, 5))

    groups = [
        ("Consistent CHO (≤25%)", consistent, COLOR_CONSISTENT),
        ("Inconsistent CHO (>25%)", inconsistent, COLOR_INCONSISTENT),
    ]

    y_positions = [1, 0]

    for y_pos, (label, sub, color) in zip(y_positions, groups):
        delta = (
            sub["tir_seg2"].astype(float) - sub["tir_seg1"].astype(float)
        ).dropna()

        if len(delta) < 2:
            ax.text(0, y_pos, f"{label}: insufficient data", va="center",
                    fontsize=FONT["annotation"])
            continue

        mean = delta.mean()
        se = delta.std(ddof=1) / np.sqrt(len(delta))
        t_crit = stats.t.ppf(0.975, df=len(delta) - 1)
        ci_lo = mean - t_crit * se
        ci_hi = mean + t_crit * se

        # Point estimate + CI
        ax.errorbar(
            mean, y_pos,
            xerr=[[mean - ci_lo], [ci_hi - mean]],
            fmt="o",
            color=color,
            markersize=12,
            capsize=8,
            capthick=2,
            elinewidth=2,
            markeredgecolor="black",
            markeredgewidth=1,
            zorder=5,
        )

        # Annotation
        ax.text(
            ci_hi + 0.3, y_pos,
            f"Δ = {mean:.2f} ({ci_lo:.2f}, {ci_hi:.2f})\nn = {len(delta)}",
            va="center",
            fontsize=FONT["annotation"],
        )

    # Reference line
    ax.axvline(0, color=COLORS_ACCENT, ls="--", lw=2, alpha=0.7,
               label="No effect")

    ax.set_yticks(y_positions)
    ax.set_yticklabels(
        [g[0] for g in groups],
        fontsize=FONT["axis_label"],
    )
    ax.set_xlabel(
        "Change in TIR 70–180 mg/dL (%, Autobolus − Temp Basal)",
        fontsize=FONT["axis_label"],
    )
    ax.tick_params(axis="x", labelsize=FONT["tick"])
    ax.grid(axis="x", alpha=0.3)
    ax.legend(fontsize=FONT["legend"], loc="lower right")

    # Interaction p-value
    delta_con = (
        consistent["tir_seg2"].astype(float)
        - consistent["tir_seg1"].astype(float)
    ).dropna()
    delta_inc = (
        inconsistent["tir_seg2"].astype(float)
        - inconsistent["tir_seg1"].astype(float)
    ).dropna()
    if len(delta_con) >= 3 and len(delta_inc) >= 3:
        _, p_int = stats.ttest_ind(delta_con, delta_inc, equal_var=False)
        p_str = _format_p(p_int)
    else:
        p_str = "N/A"

    ax.set_title(
        "Figure 8.8b: Treatment Effect on TIR by Carbohydrate Consistency Group\n"
        f"Interaction test: {p_str}; dashed line = no effect",
        fontsize=FONT["suptitle"],
        fontweight="bold",
    )

    ax.set_ylim(-0.5, 1.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Figure 8.8c — Individual Paired Outcomes by Carbohydrate Consistency
# =============================================================================

def create_figure_8_8c(df: pd.DataFrame, output_path: str):
    """
    Panel of paired dot plots (one per consistency group) showing individual
    user TIR values during temp basal and autobolus.  Lines colored by
    direction of change, with group means ± 95% CI overlaid.
    """
    consistent = df[df["cho_consistent"]].copy()
    inconsistent = df[~df["cho_consistent"]].copy()

    groups = [
        ("Consistent CHO (≤25% change)", consistent),
        ("Inconsistent CHO (>25% change)", inconsistent),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(14, 7), sharey=True)

    for ax, (label, sub) in zip(axes, groups):
        c1, c2 = "tir_seg1", "tir_seg2"
        valid = sub[c1].notna() & sub[c2].notna()
        v1 = sub.loc[valid, c1].astype(float).values
        v2 = sub.loc[valid, c2].astype(float).values

        if len(v1) == 0:
            ax.text(
                0.5, 0.5, "No valid data", ha="center", va="center",
                fontsize=FONT["annotation"], transform=ax.transAxes,
            )
            ax.set_title(label, fontsize=FONT["title"], fontweight="bold")
            continue

        # Individual paired lines, colored by direction
        for s1, s2 in zip(v1, v2):
            color = COLOR_IMPROVED if s2 >= s1 else COLOR_WORSENED
            ax.plot(
                [0, 1], [s1, s2], "o-",
                color=color, alpha=0.35, lw=0.8, ms=4,
                zorder=2,
            )

        # Group mean ± 95% CI
        for x_pos, vals, seg_color in [
            (0, v1, COLORS_PRIMARY),
            (1, v2, COLORS_SECONDARY),
        ]:
            mean = np.mean(vals)
            se = np.std(vals, ddof=1) / np.sqrt(len(vals))
            t_crit = stats.t.ppf(0.975, df=len(vals) - 1)
            ci_lo = mean - t_crit * se
            ci_hi = mean + t_crit * se

            ax.errorbar(
                x_pos, mean,
                yerr=[[mean - ci_lo], [ci_hi - mean]],
                fmt="D",
                color=seg_color,
                markersize=10,
                capsize=8,
                capthick=2.5,
                elinewidth=2.5,
                markeredgecolor="black",
                markeredgewidth=1.5,
                zorder=10,
            )

        # Statistics
        s = compute_paired_statistics(
            sub.loc[valid, c1], sub.loc[valid, c2]
        )
        p_str = (
            f"t: {_format_p(s['p_ttest'])}  "
            f"WSRT: {_format_p(s['p_wsrt'])}"
        )

        ax.set_xticks([0, 1])
        ax.set_xticklabels(
            ["Temp Basal", "Autobolus"], fontsize=FONT["axis_label"]
        )
        ax.set_ylabel(
            "TIR 70–180 mg/dL (%)", fontsize=FONT["axis_label"]
        )
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.set_title(
            f"{label}\nn = {len(v1)}; {p_str}",
            fontsize=FONT["title"],
            fontweight="bold",
        )
        ax.grid(axis="y", alpha=0.3)

    # Shared legend
    legend_elements = [
        mpatches.Patch(color=COLOR_IMPROVED, alpha=0.6, label="TIR improved"),
        mpatches.Patch(color=COLOR_WORSENED, alpha=0.6, label="TIR worsened"),
        plt.Line2D(
            [0], [0], marker="D", color="w", markerfacecolor=COLORS_PRIMARY,
            markersize=8, markeredgecolor="black", label="Group mean ± 95% CI",
        ),
    ]
    axes[1].legend(
        handles=legend_elements, fontsize=FONT["legend"], loc="lower right",
    )

    plt.suptitle(
        "Figure 8.8c: Individual Paired TIR by Carbohydrate Consistency\n"
        "Lines connect paired observations; diamonds = group mean ± 95% CI",
        fontsize=FONT["suptitle"],
        fontweight="bold",
        y=1.03,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.8: Glycemic Outcomes Stratified by")
    print("              Carbohydrate Consumption Consistency")
    print("=" * 60)

    # --- 1. Load data ---
    print("\n1. Loading data...")
    df = load_data(spark)
    print(f"   {len(df)} users in final analysis")

    # --- 2. Table 8.8a ---
    print("\n2. Creating Table 8.8a — Carbohydrate Consumption...")
    table_8_8a = create_table_8_8a(df, output_dir)
    print("\n" + table_8_8a.to_string(index=False))

    # --- 3. Table 8.8b ---
    print("\n3. Creating Table 8.8b — Consistent CHO Glycemic Outcomes...")
    tbl_8_8b_p, tbl_8_8b_np = create_table_8_8b(df, output_dir)
    print("\nParametric:")
    print(tbl_8_8b_p.to_string(index=False))

    # --- 4. Table 8.8c ---
    print("\n4. Creating Table 8.8c — Inconsistent CHO Glycemic Outcomes...")
    tbl_8_8c_p, tbl_8_8c_np = create_table_8_8c(df, output_dir)
    print("\nParametric:")
    print(tbl_8_8c_p.to_string(index=False))

    # --- 5. Table 8.8d ---
    print("\n5. Creating Table 8.8d — Treatment Effect Comparison...")
    table_8_8d = create_table_8_8d(df, output_dir)
    print("\n" + table_8_8d.to_string(index=False))

    # --- 6. Figure 8.8a ---
    print("\n6. Creating Figure 8.8a — CHO Change Distribution...")
    create_figure_8_8a(df, f"{output_dir}/figure_8_8a_cho_change_distribution.png")

    # --- 7. Figure 8.8b ---
    print("\n7. Creating Figure 8.8b — TIR Forest Plot by CHO Consistency...")
    create_figure_8_8b(df, f"{output_dir}/figure_8_8b_tir_forest_plot.png")

    # --- 8. Figure 8.8c ---
    print("\n8. Creating Figure 8.8c — Individual Paired TIR...")
    create_figure_8_8c(df, f"{output_dir}/figure_8_8c_paired_tir_by_consistency.png")

    print("\n" + "=" * 60)
    print("Analysis 8.8 Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)

    return {
        "df": df,
        "table_8_8a": table_8_8a,
        "table_8_8b_p": tbl_8_8b_p,
        "table_8_8b_np": tbl_8_8b_np,
        "table_8_8c_p": tbl_8_8c_p,
        "table_8_8c_np": tbl_8_8c_np,
        "table_8_8d": table_8_8d,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


run_in_databricks(spark)
