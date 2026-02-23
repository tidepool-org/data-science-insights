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

# Optional imports — graceful fallback if not installed
try:
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

try:
    import scikit_posthocs as sp
    _HAS_SCIKIT_POSTHOCS = True
except ImportError:
    _HAS_SCIKIT_POSTHOCS = False

# Configuration
OUTPUT_DIR = "outputs/analysis_8_5"
SEG1 = "tb_to_ab_seg1"  # temp basal period
SEG2 = "tb_to_ab_seg2"  # autobolus period
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
COLORS_PRIMARY  = "#607cff"
COLORS_SECONDARY = "#4f59be"
COLORS_ACCENT   = "#241144"

# Per-subgroup category palettes (used in grouped plots)
PALETTE_GENDER = ["#607cff", "#ff7c60", "#aaaaaa"]
PALETTE_AGE    = ["#607cff", "#4f59be", "#3a3f8a", "#241144"]
PALETTE_YLD    = ["#76D3A6", "#FF8B7C", "#FB5951"]


# =============================================================================
# Statistics helpers (identical to analysis_8-4)
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
# New statistics helpers for subgroup analyses
# =============================================================================

def compute_within_subgroup_stats(delta: pd.Series) -> Dict:
    """One-sample tests of ΔTIR vs 0 for a subgroup."""
    d = delta.dropna()
    n = len(d)
    if n < 3:
        return {
            "n": n, "mean": np.nan, "sd": np.nan,
            "median": np.nan, "q1": np.nan, "q3": np.nan,
            "ci_low": np.nan, "ci_hi": np.nan,
            "p_ttest": np.nan, "p_wsrt": np.nan,
        }

    mean   = d.mean()
    sd     = d.std(ddof=1)
    median = d.median()
    q1     = d.quantile(0.25)
    q3     = d.quantile(0.75)

    se = sd / np.sqrt(n)
    t_crit = stats.t.ppf(0.975, df=n - 1)
    ci_low = mean - t_crit * se
    ci_hi  = mean + t_crit * se

    _, p_ttest = stats.ttest_1samp(d, 0)
    p_wsrt = np.nan
    try:
        _, p_wsrt = stats.wilcoxon(d)
    except ValueError:
        pass

    return {
        "n": n, "mean": mean, "sd": sd,
        "median": median, "q1": q1, "q3": q3,
        "ci_low": ci_low, "ci_hi": ci_hi,
        "p_ttest": p_ttest, "p_wsrt": p_wsrt,
    }


def compute_between_subgroup_stats(groups: List[pd.Series]) -> Dict:
    """One-way ANOVA + Kruskal-Wallis across subgroup categories."""
    clean = [g.dropna() for g in groups if len(g.dropna()) >= 2]
    if len(clean) < 2:
        return {"p_anova": np.nan, "f_stat": np.nan, "p_kruskal": np.nan, "h_stat": np.nan}

    f_stat, p_anova = stats.f_oneway(*clean)
    h_stat, p_kruskal = stats.kruskal(*clean)
    return {"p_anova": p_anova, "f_stat": f_stat, "p_kruskal": p_kruskal, "h_stat": h_stat}


def compute_posthoc(
    groups_dict: Dict[str, pd.Series]
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Tukey's HSD + Dunn's test. Returns (tukey_df, dunn_df) or (None, None)."""
    labels = []
    values = []
    for grp_label, series in groups_dict.items():
        clean = series.dropna()
        labels.extend([grp_label] * len(clean))
        values.extend(clean.tolist())

    values_arr = np.array(values)
    labels_arr = np.array(labels)

    tukey_df = None
    if _HAS_STATSMODELS and len(values_arr) >= 4:
        try:
            result = pairwise_tukeyhsd(values_arr, labels_arr, alpha=ALPHA_BONFERRONI)
            tukey_df = pd.DataFrame(
                data=result.summary().data[1:],
                columns=result.summary().data[0],
            )
        except Exception as e:
            print(f"  Tukey's HSD failed: {e}")

    dunn_df = None
    if _HAS_SCIKIT_POSTHOCS and len(values_arr) >= 4:
        try:
            long_df = pd.DataFrame({"value": values, "group": labels})
            dunn_df = sp.posthoc_dunn(long_df, val_col="value", group_col="group", p_adjust="bonferroni")
        except Exception as e:
            print(f"  Dunn's test failed: {e}")

    return tukey_df, dunn_df


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
    # --- Glycemic endpoints ---
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_transition").toPandas()

    # Convert numeric columns
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
    excluded_users = guardrails.loc[guardrails["violation_count"] > 0, "_userId"].unique()
    endpoints = endpoints[~endpoints["_userId"].isin(excluded_users)]
    print(f"  Excluded {len(excluded_users)} users with guardrail violations")

    # --- Pivot to wide ---
    seg1 = endpoints[endpoints["segment"] == SEG1].set_index("_userId").add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index("_userId").add_suffix("_seg2")
    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(
        columns=[c for c in wide.columns if c.startswith("segment")], errors="ignore"
    )
    wide = wide.reset_index()

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
                "p (one-sample t vs 0)": _format_p(s["p_ttest"]),
            })
            nonparametric_rows.append({
                "Demographic Factor": info["label"],
                "Category": cat,
                "N": s["n"],
                "Median ΔTIR [IQR]": (
                    f"{s['median']:.2f} [{s['q1']:.2f}, {s['q3']:.2f}]"
                    if not np.isnan(s["median"]) else "N/A"
                ),
                "p (Wilcoxon vs 0)": _format_p(s["p_wsrt"]),
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
            "p (ANOVA)": _format_p(s["p_anova"]),
            "KW H": f"{s['h_stat']:.3f}" if not np.isnan(s["h_stat"]) else "N/A",
            "p (Kruskal-Wallis)": _format_p(s["p_kruskal"]),
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
            f"ANOVA {_format_p(between_res['p_anova'])}  "
            f"KW {_format_p(between_res['p_kruskal'])}"
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
                f"ANOVA {_format_p(between_res['p_anova'])}  "
                f"KW {_format_p(between_res['p_kruskal'])}"
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
            p_str = _format_p(p)

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


run_in_databricks(spark)
