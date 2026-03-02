"""
=============================================================================
Analysis 8.5: Demographic Subgroup Analysis
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Assess whether glycemic outcomes (ΔTIR) differ across demographic
subgroups (gender, age, years living with diabetes) when users transition
from temporary basal to autobolus dosing.

Inputs:
- dev.fda_510k_rwd.glycemic_endpoints_transition  (long: _userId, segment, ...)
- dev.fda_510k_rwd.valid_transition_guardrails     (_userId, violation_count)
- dev.fda_510k_rwd.valid_transition_segments       (_userId, gender, age_years,
                                                    years_with_diabetes)

Methods:
- ΔTIR = tir_seg2 − tir_seg1 per user
- Within-subgroup: one-sample t-test + Wilcoxon signed-rank (vs 0)
- Between-subgroup: one-way ANOVA + Kruskal-Wallis
- Post-hoc (p < α_Bonferroni): Tukey's HSD + Dunn's test
- Bonferroni correction: α = 0.05 / 3 = 0.017 (3 demographic factors)
- Sensitivity analysis: missing gender data assessment

Outputs:
- Table 8.5a: Data Availability by Subgroup
- Table 8.5b: Within-subgroup ΔTIR (parametric + nonparametric)
- Table 8.5c: Between-subgroup Tests
- Table 8.5d: Post-hoc Pairwise Comparisons (if applicable)
- Figure 8.5a: ΔTIR Boxplots by Subgroup (1×3 panel)
- Figure 8.5b: Safety Outcomes by Subgroup (3×3 panel)
- Figure 8.5c: Demographic Distributions (1×3 bar charts)
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
from typing import Dict, List, Optional, Tuple
import os

# Configuration
OUTPUT_DIR = "outputs/analysis_8_5"
ALPHA_BONFERRONI = 0.05 / 3  # 0.017 — correction across 3 demographic factors

# Subgroup definitions
SUBGROUPS = {
    "gender_group": {
        "label": "Gender",
        "categories": ["Male", "Female", "Other/Unknown"],
    },
    "age_group": {
        "label": "Age Group",
        "categories": ["Children (6–<12)", "Adolescents (12–<18)", "Adults (18–64)", "Older Adults (≥65)"],
    },
    "yld_group": {
        "label": "Years Living with Diabetes",
        "categories": ["Early (<5 yrs)", "Established (5–15 yrs)", "Long-duration (>15 yrs)"],
    },
}

# Safety outcomes for Figure 8.5b: (display_name, col_prefix, unit)
SAFETY_OUTCOMES = [
    ("Time <70 mg/dL (%)",           "tbr",           "%"),
    ("Time <54 mg/dL (%)",           "tbr_very_low",  "%"),
    ("Hypoglycemic events (n/14 d)", "hypo_events",   "n"),
]

from utils.constants import FONT, COLORS_PRIMARY, COLORS_SECONDARY, COLORS_ACCENT
from utils.data_loading import load_transition_endpoints
from utils.statistics import (
    test_normality, compute_paired_statistics, format_p,
    compute_within_subgroup_stats, compute_between_subgroup_stats, compute_posthoc,
)

# Per-subgroup category palettes (used in grouped plots)
PALETTE_GENDER = ["#607cff", "#ff7c60", "#aaaaaa"]
PALETTE_AGE    = ["#607cff", "#4f59be", "#3a3f8a", "#241144"]
PALETTE_YLD    = ["#76D3A6", "#FF8B7C", "#FB5951"]



# =============================================================================
# Data Loading
# =============================================================================

def _bin_age(age):
    """Map numeric age to age group label."""
    if pd.isna(age):
        return None
    age = float(age)
    if age < 12:
        return "Children (6–<12)"
    elif age < 18:
        return "Adolescents (12–<18)"
    elif age < 65:
        return "Adults (18–64)"
    else:
        return "Older Adults (≥65)"


def _bin_yld(yld):
    """Map years_with_diabetes to YLD group label."""
    if pd.isna(yld):
        return None
    yld = float(yld)
    if yld < 5:
        return "Early (<5 yrs)"
    elif yld <= 15:
        return "Established (5–15 yrs)"
    else:
        return "Long-duration (>15 yrs)"


def _bin_gender(gender):
    """Normalize gender to Male / Female / Other-Unknown."""
    if pd.isna(gender) or str(gender).strip() == "":
        return "Other/Unknown"
    g = str(gender).strip().lower()
    if g in ("male", "m"):
        return "Male"
    elif g in ("female", "f"):
        return "Female"
    else:
        return "Other/Unknown"


def load_data(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints, apply guardrail exclusions and CBG coverage
    filter, join demographics, compute ΔTIR, and bin into subgroups.
    """
    wide = load_transition_endpoints(spark)

    # --- ΔTIR ---
    wide["delta_tir"] = wide["tir_seg2"] - wide["tir_seg1"]

    # --- Demographics ---
    demographics = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .select("_userId", "gender", "tb_to_ab_age_years", "tb_to_ab_years_lwd")
        .toPandas()
    )
    # One row per user (take the first if multiple)
    demographics = demographics.drop_duplicates(subset="_userId")
    demographics["tb_to_ab_age_years"] = pd.to_numeric(
        demographics["tb_to_ab_age_years"], errors="coerce"
    )
    demographics["tb_to_ab_years_lwd"] = pd.to_numeric(
        demographics["tb_to_ab_years_lwd"], errors="coerce"
    )

    wide = wide.merge(demographics, on="_userId", how="left")

    # --- Subgroup bins ---
    wide["gender_group"] = wide["gender"].apply(_bin_gender)
    wide["age_group"]    = wide["tb_to_ab_age_years"].apply(_bin_age)
    wide["yld_group"]    = wide["tb_to_ab_years_lwd"].apply(_bin_yld)

    n = len(wide)
    print(f"  Eligible users after filters: {n}")
    for sg_col, info in SUBGROUPS.items():
        print(f"\n  {info['label']} distribution:")
        vc = wide[sg_col].value_counts(dropna=False)
        for cat in info["categories"]:
            cnt = vc.get(cat, 0)
            pct = 100 * cnt / n if n > 0 else 0.0
            print(f"    {cat}: {cnt} ({pct:.1f}%)")
        null_cnt = wide[sg_col].isna().sum()
        if null_cnt > 0:
            print(f"    Unclassified: {null_cnt} ({100 * null_cnt / n:.1f}%)")

    return wide


# =============================================================================
# Table 8.5a — Data Availability by Subgroup
# =============================================================================

def create_table_8_5a(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    n_total = len(df)
    rows = []

    for sg_col, info in SUBGROUPS.items():
        for cat in info["categories"]:
            mask  = df[sg_col] == cat
            n_cat = mask.sum()
            pct   = 100 * n_cat / n_total if n_total > 0 else 0.0
            sub   = df.loc[mask, "delta_tir"].dropna()
            rows.append({
                "Demographic Factor": info["label"],
                "Category": cat,
                "n": n_cat,
                "%": f"{pct:.1f}",
                "Mean ΔTIR ± SD": (
                    f"{sub.mean():.2f} ± {sub.std():.2f}" if len(sub) >= 3 else "N/A"
                ),
                "Median ΔTIR [IQR]": (
                    f"{sub.median():.2f} [{sub.quantile(0.25):.2f}, {sub.quantile(0.75):.2f}]"
                    if len(sub) >= 3 else "N/A"
                ),
            })

    table = pd.DataFrame(rows)
    out = f"{output_dir}/table_8_5a_data_availability.csv"
    table.to_csv(out, index=False)
    print(f"  Saved: {out}")
    return table


# =============================================================================
# Table 8.5b — Within-subgroup ΔTIR
# =============================================================================

def create_table_8_5b(
    df: pd.DataFrame, output_dir: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    parametric_rows    = []
    nonparametric_rows = []

    for sg_col, info in SUBGROUPS.items():
        for cat in info["categories"]:
            mask  = df[sg_col] == cat
            delta = df.loc[mask, "delta_tir"]
            s     = compute_within_subgroup_stats(delta)

            ci_str = (
                f"({s['ci_low']:.2f}, {s['ci_hi']:.2f})"
                if not np.isnan(s["ci_low"]) else "N/A"
            )

            parametric_rows.append({
                "Demographic Factor": info["label"],
                "Category": cat,
                "N": s["n"],
                "Mean ΔTIR ± SD": (
                    f"{s['mean']:.2f} ± {s['sd']:.2f}" if not np.isnan(s["mean"]) else "N/A"
                ),
                "95% CI": ci_str,
                "p (one-sample t vs 0)": format_p(s["p_ttest"]),
            })
            nonparametric_rows.append({
                "Demographic Factor": info["label"],
                "Category": cat,
                "N": s["n"],
                "Median ΔTIR [IQR]": (
                    f"{s['median']:.2f} [{s['q1']:.2f}, {s['q3']:.2f}]"
                    if not np.isnan(s["median"]) else "N/A"
                ),
                "p (Wilcoxon vs 0)": format_p(s["p_wsrt"]),
            })

    tbl_p  = pd.DataFrame(parametric_rows)
    tbl_np = pd.DataFrame(nonparametric_rows)

    out_p  = f"{output_dir}/table_8_5b_within_subgroup_parametric.csv"
    out_np = f"{output_dir}/table_8_5b_within_subgroup_nonparametric.csv"
    tbl_p.to_csv(out_p, index=False)
    tbl_np.to_csv(out_np, index=False)
    print(f"  Saved: {out_p}")
    print(f"  Saved: {out_np}")
    return tbl_p, tbl_np


# =============================================================================
# Table 8.5c — Between-subgroup Tests
# =============================================================================

def create_table_8_5c(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    rows = []

    for sg_col, info in SUBGROUPS.items():
        groups = [
            df.loc[df[sg_col] == cat, "delta_tir"]
            for cat in info["categories"]
        ]
        s = compute_between_subgroup_stats(groups)
        sig = (
            (not np.isnan(s["p_anova"]) and s["p_anova"] < ALPHA_BONFERRONI)
            or (not np.isnan(s["p_kruskal"]) and s["p_kruskal"] < ALPHA_BONFERRONI)
        )
        rows.append({
            "Demographic Factor": info["label"],
            "ANOVA F": f"{s['f_stat']:.3f}" if not np.isnan(s["f_stat"]) else "N/A",
            "p (ANOVA)": format_p(s["p_anova"]),
            "KW H": f"{s['h_stat']:.3f}" if not np.isnan(s["h_stat"]) else "N/A",
            "p (Kruskal-Wallis)": format_p(s["p_kruskal"]),
            f"Significant (α={ALPHA_BONFERRONI:.3f})": "Yes" if sig else "No",
        })

    table = pd.DataFrame(rows)
    out = f"{output_dir}/table_8_5c_between_subgroup.csv"
    table.to_csv(out, index=False)
    print(f"  Saved: {out}")
    return table


# =============================================================================
# Table 8.5d — Post-hoc Pairwise Comparisons
# =============================================================================

def create_table_8_5d(df: pd.DataFrame, table_8_5c: pd.DataFrame, output_dir: str):
    """Run post-hoc tests for any factor flagged as significant."""
    sig_factors = table_8_5c.loc[
        table_8_5c[f"Significant (α={ALPHA_BONFERRONI:.3f})"] == "Yes",
        "Demographic Factor",
    ].tolist()

    if not sig_factors:
        print("  No between-group differences significant at Bonferroni-corrected α — skipping post-hoc")
        return

    # Map label → column name
    label_to_col = {info["label"]: col for col, info in SUBGROUPS.items()}

    for factor_label in sig_factors:
        sg_col = label_to_col[factor_label]
        cats   = SUBGROUPS[sg_col]["categories"]
        groups_dict = {cat: df.loc[df[sg_col] == cat, "delta_tir"] for cat in cats}

        print(f"  Running post-hoc for: {factor_label}")
        tukey_df, dunn_df = compute_posthoc(groups_dict)

        safe_name = factor_label.lower().replace(" ", "_")
        if tukey_df is not None:
            out = f"{output_dir}/table_8_5d_tukey_{safe_name}.csv"
            tukey_df.to_csv(out, index=False)
            print(f"  Saved: {out}")
        else:
            print(f"  Tukey's HSD not available (statsmodels not installed or insufficient data)")

        if dunn_df is not None:
            out = f"{output_dir}/table_8_5d_dunn_{safe_name}.csv"
            dunn_df.to_csv(out)
            print(f"  Saved: {out}")
        else:
            print(f"  Dunn's test not available (scikit-posthocs not installed or insufficient data)")


# =============================================================================
# Figure 8.5a — ΔTIR Boxplots by Subgroup (1×3 panel)
# =============================================================================

def create_figure_8_5a(df: pd.DataFrame, output_path: str):
    sg_cols  = list(SUBGROUPS.keys())
    palettes = [PALETTE_GENDER, PALETTE_AGE, PALETTE_YLD]

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    for ax, sg_col, palette in zip(axes, sg_cols, palettes):
        info = SUBGROUPS[sg_col]
        cats = info["categories"]

        groups      = [df.loc[df[sg_col] == cat, "delta_tir"].dropna().values for cat in cats]
        between_res = compute_between_subgroup_stats(
            [df.loc[df[sg_col] == cat, "delta_tir"] for cat in cats]
        )
        p_str = (
            f"ANOVA {format_p(between_res['p_anova'])}  "
            f"KW {format_p(between_res['p_kruskal'])}"
        )

        # Boxplots
        bp = ax.boxplot(
            groups,
            positions=range(len(cats)),
            widths=0.5,
            patch_artist=True,
            showfliers=False,
            medianprops=dict(color="black", lw=2),
        )
        for patch, color in zip(bp["boxes"], palette):
            patch.set_facecolor(color)
            patch.set_alpha(0.75)

        # Jittered individual points
        rng = np.random.default_rng(42)
        for i, grp in enumerate(groups):
            jitter = rng.uniform(-0.18, 0.18, size=len(grp))
            ax.scatter(
                i + jitter, grp,
                color=palette[i] if i < len(palette) else COLORS_PRIMARY,
                alpha=0.35, s=15, zorder=3,
            )

        ax.axhline(0, color=COLORS_ACCENT, ls="--", lw=1.5, alpha=0.7)
        ax.set_xticks(range(len(cats)))
        ax.set_xticklabels(cats, fontsize=FONT["tick"], rotation=20, ha="right")
        ax.set_ylabel("ΔTIR (Autobolus − Temp Basal, %)", fontsize=FONT["axis_label"])
        ax.set_title(f"{info['label']}\n{p_str}", fontsize=FONT["title"], fontweight="bold")
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.grid(axis="y", alpha=0.3)

    plt.suptitle(
        "Figure 8.5a: Change in Time-in-Range by Demographic Subgroup\n"
        f"Bonferroni-corrected α = {ALPHA_BONFERRONI:.3f}; dashed line at 0",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.03,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Figure 8.5b — Safety Outcomes by Subgroup (3×3 panel)
# =============================================================================

def create_figure_8_5b(df: pd.DataFrame, output_path: str):
    df = df.copy()  # avoid mutating the caller's DataFrame with delta columns
    sg_cols  = list(SUBGROUPS.keys())
    palettes = [PALETTE_GENDER, PALETTE_AGE, PALETTE_YLD]
    n_outcomes = len(SAFETY_OUTCOMES)
    n_sg       = len(sg_cols)

    fig, axes = plt.subplots(n_outcomes, n_sg, figsize=(6 * n_sg, 5 * n_outcomes))

    for row_idx, (outcome_name, col_prefix, unit) in enumerate(SAFETY_OUTCOMES):
        c1 = f"{col_prefix}_seg1"
        c2 = f"{col_prefix}_seg2"

        if c1 not in df.columns or c2 not in df.columns:
            print(f"  Warning: {c1}/{c2} not in DataFrame — skipping {outcome_name}")
            for col_idx in range(n_sg):
                axes[row_idx, col_idx].set_visible(False)
            continue

        delta_col = f"delta_{col_prefix}"
        df[delta_col] = pd.to_numeric(df[c2], errors="coerce") - pd.to_numeric(df[c1], errors="coerce")

        for col_idx, (sg_col, palette) in enumerate(zip(sg_cols, palettes)):
            ax   = axes[row_idx, col_idx]
            info = SUBGROUPS[sg_col]
            cats = info["categories"]

            groups = [df.loc[df[sg_col] == cat, delta_col].dropna().values for cat in cats]
            between_res = compute_between_subgroup_stats(
                [df.loc[df[sg_col] == cat, delta_col] for cat in cats]
            )
            p_str = (
                f"ANOVA {format_p(between_res['p_anova'])}  "
                f"KW {format_p(between_res['p_kruskal'])}"
            )

            bp = ax.boxplot(
                groups,
                positions=range(len(cats)),
                widths=0.5,
                patch_artist=True,
                showfliers=False,
                medianprops=dict(color="black", lw=2),
            )
            for patch, color in zip(bp["boxes"], palette):
                patch.set_facecolor(color)
                patch.set_alpha(0.75)

            ax.axhline(0, color=COLORS_ACCENT, ls="--", lw=1.5, alpha=0.7)
            ax.set_xticks(range(len(cats)))
            ax.set_xticklabels(cats, fontsize=max(FONT["tick"] - 2, 9),
                               rotation=20, ha="right")
            ax.set_ylabel(
                f"Δ {outcome_name}\n(Autobolus − Temp Basal, {unit})",
                fontsize=FONT["axis_label"] - 1,
            )
            ax.set_title(
                f"{outcome_name} | {info['label']}\n{p_str}",
                fontsize=FONT["title"] - 1, fontweight="bold",
            )
            ax.tick_params(axis="y", labelsize=FONT["tick"])
            ax.grid(axis="y", alpha=0.3)

    plt.suptitle(
        "Figure 8.5b: Safety Outcome Changes by Demographic Subgroup",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Figure 8.5c — Demographic Distributions (1×3 bar charts)
# =============================================================================

def create_figure_8_5c(df: pd.DataFrame, output_path: str):
    sg_cols  = list(SUBGROUPS.keys())
    palettes = [PALETTE_GENDER, PALETTE_AGE, PALETTE_YLD]
    n_total  = len(df)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    for ax, sg_col, palette in zip(axes, sg_cols, palettes):
        info = SUBGROUPS[sg_col]
        cats = info["categories"]
        counts = [int((df[sg_col] == cat).sum()) for cat in cats]

        bars = ax.bar(
            range(len(cats)), counts,
            color=palette[: len(cats)], edgecolor="black", alpha=0.85,
        )

        for bar, cnt in zip(bars, counts):
            pct = 100 * cnt / n_total if n_total > 0 else 0.0
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(counts) * 0.01,
                f"n={cnt}\n({pct:.1f}%)",
                ha="center", va="bottom",
                fontsize=FONT["annotation"] - 1,
            )

        ax.set_xticks(range(len(cats)))
        ax.set_xticklabels(cats, fontsize=FONT["tick"], rotation=20, ha="right")
        ax.set_ylabel("Number of Users", fontsize=FONT["axis_label"])
        ax.set_title(f"{info['label']}", fontsize=FONT["title"], fontweight="bold")
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.grid(axis="y", alpha=0.3)
        ax.set_ylim(0, max(counts) * 1.2 if counts else 1)

    plt.suptitle(
        "Figure 8.5c: Demographic Distribution of Eligible Users",
        fontsize=FONT["suptitle"], fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# =============================================================================
# Sensitivity Analysis: Missing Gender Data
# =============================================================================

def run_sensitivity_gender_missing(df: pd.DataFrame):
    """
    Compare baseline characteristics between users with and without gender data
    to assess whether missingness is associated with outcomes.
    """
    print("\n" + "-" * 60)
    print("Sensitivity Analysis: Missing Gender Data")
    print("-" * 60)

    has_gender    = df["gender"].notna() & (df["gender"].str.strip() != "")
    df_known      = df[has_gender].copy()
    df_missing    = df[~has_gender].copy()

    n_known   = len(df_known)
    n_missing = len(df_missing)
    n_total   = len(df)

    print(f"  Users with gender data:    {n_known} ({100 * n_known / n_total:.1f}%)")
    print(f"  Users without gender data: {n_missing} ({100 * n_missing / n_total:.1f}%)")

    comparisons = [
        ("Baseline TIR (seg1, %)",    "tir_seg1"),
        ("Age (years)",               "tb_to_ab_age_years"),
        ("Years with diabetes",       "tb_to_ab_years_lwd"),
        ("ΔTIR (%)",                  "delta_tir"),
    ]

    rows = []
    for label, col in comparisons:
        if col not in df.columns:
            continue
        g1 = pd.to_numeric(df_known[col],   errors="coerce").dropna()
        g2 = pd.to_numeric(df_missing[col], errors="coerce").dropna()

        row = {
            "Variable": label,
            f"Gender Known (n={n_known})":   (
                f"{g1.mean():.2f} ± {g1.std():.2f}" if len(g1) >= 3 else "N/A"
            ),
            f"Gender Missing (n={n_missing})": (
                f"{g2.mean():.2f} ± {g2.std():.2f}" if len(g2) >= 3 else "N/A"
            ),
        }

        p_str = "N/A"
        if len(g1) >= 3 and len(g2) >= 3:
            _, p = stats.ttest_ind(g1, g2, equal_var=False)
            p_str = format_p(p)

        row["p (independent t-test)"] = p_str
        rows.append(row)
        print(
            f"  {label:35s}  Known: {row[f'Gender Known (n={n_known})']:>20s}  "
            f"Missing: {row[f'Gender Missing (n={n_missing})']:>20s}  {p_str}"
        )

    return pd.DataFrame(rows)


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.5: Demographic Subgroup Analysis")
    print("=" * 60)

    print("\n1. Loading data...")
    df = load_data(spark)
    print(f"   {len(df)} eligible users")

    print("\n2. Creating Table 8.5a — Data Availability...")
    table_8_5a = create_table_8_5a(df, output_dir)
    print("\n" + table_8_5a.to_string(index=False))

    print("\n3. Creating Table 8.5b — Within-subgroup ΔTIR...")
    tbl_8_5b_p, tbl_8_5b_np = create_table_8_5b(df, output_dir)
    print("\nParametric:")
    print(tbl_8_5b_p.to_string(index=False))

    print("\n4. Creating Table 8.5c — Between-subgroup Tests...")
    table_8_5c = create_table_8_5c(df, output_dir)
    print("\n" + table_8_5c.to_string(index=False))

    print("\n5. Creating Table 8.5d — Post-hoc Tests (if any significant)...")
    create_table_8_5d(df, table_8_5c, output_dir)

    print("\n6. Creating Figure 8.5a — ΔTIR by Subgroup...")
    create_figure_8_5a(df, f"{output_dir}/figure_8_5a_delta_tir_by_subgroup.png")

    print("\n7. Creating Figure 8.5b — Safety Outcomes by Subgroup...")
    create_figure_8_5b(df, f"{output_dir}/figure_8_5b_safety_by_subgroup.png")

    print("\n8. Creating Figure 8.5c — Demographic Distributions...")
    create_figure_8_5c(df, f"{output_dir}/figure_8_5c_demographic_distributions.png")

    print("\n9. Sensitivity Analysis — Missing Gender Data...")
    sensitivity_df = run_sensitivity_gender_missing(df)
    sensitivity_df.to_csv(f"{output_dir}/sensitivity_gender_missing.csv", index=False)

    print("\n" + "=" * 60)
    print("Analysis 8.5 Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)

    return {
        "df":                df,
        "table_8_5a":        table_8_5a,
        "table_8_5b_p":      tbl_8_5b_p,
        "table_8_5b_np":     tbl_8_5b_np,
        "table_8_5c":        table_8_5c,
        "sensitivity_gender": sensitivity_df,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    run_in_databricks(spark)  # type: ignore[name-defined]
