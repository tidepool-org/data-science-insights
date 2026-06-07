"""Analysis 8.1: glycemic outcomes on NMA-like days vs CE>0 days (PLN-1008 §8.1).

Method A — within-user paired analysis. For each of the three nested NMA-like
classifications and each endpoint:
  - per-user within-arm mean for the NMA arm and the CE>0 comparator arm,
  - one paired difference per user (NMA mean - CE>0 mean); equal weighting; only users
    with eligible days in BOTH arms contribute,
  - Shapiro-Wilk normality screen; paired t-test (mean diff + 95% t-CI);
    Wilcoxon signed-rank; median diff with cluster-bootstrap 95% CI (1,000 user-resamples).

Method B — linear mixed-effects model `outcome ~ arm + (1|user)` per classification x
endpoint (contrast estimate + Wald 95% CI + Wald p), with a per-user-median paired
non-parametric companion. Uses the shared `analysis/utils/statistics.py` (lmm_arm_contrast,
cluster_bootstrap_ci).

The three NMA arms are nested (CE=0/BE=0 ⊆ CE=0/BE<=1 ⊆ CE=0/BE<=inf) and are each
contrasted against the SAME CE>0 comparator. Arm membership comes from the flags in
nma_user_day_classification; only eligible days of eligible users are used (§7.1). The
CE>0 comparator is restricted to users who contribute at least one CE=0 day (§8.1
Table 8.1a). Adult and pediatric cohorts are reported separately (§7.6).

Reuses FDA `FDA_real_world_data/analysis/utils/statistics.py` UNMODIFIED and the local
`analysis/utils/statistics.py`, both loaded by file path under unique module names (our
analysis/ has its own `utils` package, so a bare `import` would collide).

Inputs:
    nma_user_day_analysis_ready        (arm flags + day_eligible + user_eligible + per-day endpoints,
                                        behavioral counts, TDD, age; PLN-1001 cohort filter already
                                        applied — version<3.4.0 when known, else local_day<2024-07-13)

Outputs (analysis/outputs/analysis_8_1/<cohort>/):
  §8.1 (main — matches the plan; arms = 3 nested NMA + CE>0 + the CE>=3/BE>=3 high-engagement column):
    sample_information.csv              (Table 1: per-cohort age + sex demographics, user/day counts)
    sex_missingness_sensitivity.csv     (recorded-vs-missing-sex baseline comparison; FDA §8.5 analog)
    nma_day_frequency.csv               (§4 secondary obj. bullet 3: per-classification NMA-day-type frequency)
    table_8_1a_per_user_means.csv       (Table 8.1a: per-user means ± SD by arm, with user/day counts)
    table_8_1a_expanded.csv             (Table 8.1a expanded: per classification x endpoint Method A paired stats)
    table_8_1b_lmm_contrasts.csv        (Table 8.1b: Method B LMM contrast NMA-CE>0 + np median sensitivity)
    table_8_1c_behavioral_summary.csv   (Table 8.1c: CE>0-day behavioral metrics: mean±SD, median[IQR])
    figure_8_1a_stacked_bars.png        (mean time in 5 glycemic ranges across the arms)
    figure_8_1b_violin_grid{1,2}_*.png  (per-user means by arm, all 8 endpoints, two 2x2 grids)
    figure_8_1c_paired_delta_grid{1,2}_*.png  (within-user NMA-CE>0 paired Δ + NMA-CE>=3/BE>=3 overlay)
  Appendix §12.1 (windowed-comparator sensitivity, per-NMA-day ±45d match — mirrors §8.1 layout):
    table_12_1a_windowed_sensitivity.csv (windowed NMA vs CE>0 per classification x endpoint)
    figure_12_1a_windowed_stacked_bars.png / figure_12_1b_windowed_violin_grid{1,2}_*.png / figure_12_1c_windowed_delta_grid{1,2}_*.png
  Appendix §12.1 (cont.) — high-engagement CE>=3/BE>=3 arm vs CE>0:
    table_12_1b_high_engagement_lmm.csv (full-record LMM) / table_12_1c_high_engagement_windowed.csv (windowed ±45d match)

main() also writes the combined Sample Information across cohorts (run via the no-`--cohort`
entry point):
    analysis/outputs/analysis_8_1/table_8_1_sample_information.csv  (adult/pediatric/all columns)
"""

# %pip install statsmodels
# dbutils.library.restartPython()

import argparse
import importlib.util
import os
import shutil
import sys
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Make the sibling `utils` package importable regardless of launch method (the script dir is
# auto-added for `python this.py`, but a Databricks notebook needs the hint).
try:
    _ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ANALYSIS_DIR = (
        "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
        "no_meal_announcement/analysis"
    )
if _ANALYSIS_DIR not in sys.path:
    sys.path.insert(0, _ANALYSIS_DIR)

from utils.data_loader import (  # noqa: E402
    CLASSIFICATIONS,
    COMPARATOR_FLAG,
    COMPARATOR_LABEL,
    ENDPOINTS,
    FIGURE_ARMS,
    HIGH_MA_FLAG,
    HIGH_MA_LABEL,
    MIN_AGE,
    SUPPLEMENT_ARMS,
    WINDOW_DAYS,
    analysis_dir,
    default_analysis_ready_csv,
    filter_cohort,
    load_day_level,
    load_fda_statistics,
    load_nma_statistics,
    prepare_day_level,
    restrict_comparator,
    windowed_matched_means,
)
from utils.plotting import (  # noqa: E402
    DAY_TYPE_COLORS,
    GRAY,
    GRIDS,
    HIGH_MA_COLOR,
    RANGE_COLORS,
    RANGE_COLS,
    SUPTITLE_FS,
    endpoint_color,
    overlay_hist_panel,
    render_4x2_grid,
    violin_box_panel,
)

BOOTSTRAP_N = 1000
BOOTSTRAP_SEED = 20260520

# Per-user violin grids show the 5 arms (3 nested NMA + CE>0 + CE>=3/BE>=3). Each arm carries its
# fixed DAY_TYPE_COLORS colour (supersedes D13's per-endpoint arm colouring): the 3 nested NMA/CE=0
# arms on the green ramp (dark BE=0 → TIR-green BE≤1 → light BE≤∞), CE>0 grey, HMA bronze — the same
# day-type palette shared across §8.1–§8.3 via utils.plotting, so day types read identically.
VIOLIN_ALPHA = 0.72   # uniform fill alpha (the distinct day-type colours, not alpha, separate arms)

# Behavioral metrics for Table 8.1c (CE>0 days). PLN-1008 §7.5 lists meal boluses,
# manual/correction boluses, announced carbs, and TDD. There is no dedicated meal-bolus
# count in the analysis-ready table, so `carb_entry_count` (the count of food/carb
# entries, each of which carries a meal bolus) is used as a footnoted proxy for meal
# boluses; `bolus_entry_count` is the user-initiated (manual/correction) bolus count.
BEHAVIORAL = [
    ("carb_entry_count", "Meal boluses per day (proxy: carb entries)"),
    ("bolus_entry_count", "Manual/correction boluses per day"),
    ("carb_grams_total", "Announced carbohydrates per day (g)"),
    ("tdd_units", "Total daily insulin (U/day)"),
]

# Sample Information (Table 1) sex categories. Raw `gender` from dev.default.user_gender
# is normalized to these by _bin_sex (mirrors FDA analysis_8-5._bin_gender).
SEX_CATEGORIES = ["Male", "Female", "Other/Unknown"]


def cluster_bootstrap_median_ci(diff, n_boot=BOOTSTRAP_N, seed=BOOTSTRAP_SEED):
    """95% CI for the median paired difference, resampling users with replacement."""
    d = np.asarray(pd.Series(diff).dropna())
    n = len(d)
    if n < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    medians = np.median(d[rng.integers(0, n, size=(n_boot, n))], axis=1)
    return float(np.percentile(medians, 2.5)), float(np.percentile(medians, 97.5))


def per_user_arm_mean(pdf, endpoint, arm_flag):
    """Series indexed by user: mean endpoint over that user's days in the arm."""
    return pdf[pdf[arm_flag] == True].groupby("_userId")[endpoint].mean()  # noqa: E712


def contrasts_table(pdf, stats_mod):
    """Method A paired stats for every (classification, endpoint)."""
    rows = []
    comparator = {col: per_user_arm_mean(pdf, col, COMPARATOR_FLAG) for col, _ in ENDPOINTS}
    for nma_flag, cls_label in CLASSIFICATIONS:
        for col, ep_label in ENDPOINTS:
            nma = per_user_arm_mean(pdf, col, nma_flag)
            wide = pd.DataFrame({"NMA": nma, "CMP": comparator[col]}).dropna()
            # diff = seg2 - seg1 -> seg1=comparator, seg2=NMA gives NMA - CE>0.
            s = stats_mod.compute_paired_statistics(wide["CMP"], wide["NMA"])
            boot_lo, boot_hi = cluster_bootstrap_median_ci(wide["NMA"] - wide["CMP"])
            rows.append({
                "classification": cls_label,
                "endpoint": col,
                "label": ep_label,
                "n_pairs": s["n_pairs"],
                "nma_mean": s["seg2_mean"],
                "ce_gt0_mean": s["seg1_mean"],
                "diff_mean": s["diff_mean"],
                "diff_ci_low": s["diff_ci_low"],
                "diff_ci_hi": s["diff_ci_hi"],
                "diff_median": s["diff_median"],
                "boot_ci_low": boot_lo,
                "boot_ci_hi": boot_hi,
                "shapiro_p": s["normality_p"],
                "is_normal": s["is_normal"],
                "p_ttest": s["p_ttest"],
                "p_wsrt": s["p_wsrt"],
            })
    return pd.DataFrame(rows)


def create_table_8_1a(pdf):
    """Table 8.1a: per-arm, across-user mean ± SD of each endpoint's per-user within-arm
    mean, plus user-day and contributing-user counts. Rows = 8 endpoints + 2 count rows;
    columns = the 5 arms (3 nested NMA + CE>0 + the high meal-announcement CE>=3/BE>=3 column;
    note CE>=3/BE>=3 ⊂ CE>0 — a descriptive heavy-announcement column, not a disjoint arm)."""
    arm_labels = [lab for _, lab in SUPPLEMENT_ARMS]
    table = {}  # row label -> {arm label: display cell}
    for col, ep_label in ENDPOINTS:
        table[ep_label] = {}
        for flag, arm_label in SUPPLEMENT_ARMS:
            m = per_user_arm_mean(pdf, col, flag).dropna()
            if len(m) > 1:
                table[ep_label][arm_label] = f"{m.mean():.1f} ± {m.std(ddof=1):.1f}"
            elif len(m) == 1:
                table[ep_label][arm_label] = f"{m.mean():.1f}"
            else:
                table[ep_label][arm_label] = "N/A"
    user_days, users_contrib = {}, {}
    for flag, arm_label in SUPPLEMENT_ARMS:
        sub = pdf[pdf[flag] == True]  # noqa: E712
        user_days[arm_label] = int(len(sub))
        users_contrib[arm_label] = int(sub["_userId"].nunique())
    table["User-days, n"] = user_days
    table["Users contributing, n"] = users_contrib

    out = pd.DataFrame.from_dict(table, orient="index", columns=arm_labels)
    out.index.name = "metric"
    return out.reset_index()


def create_table_8_1b(pdf, nma_stats, classifications=CLASSIFICATIONS):
    """Table 8.1b (§8.1 Method B): mixed-effects contrast NMA-CE>0 per classification x
    endpoint — `outcome ~ arm + (1|user)` — with Wald 95% CI and p, plus the per-user
    median paired non-parametric companion. Degenerate / non-converging fits yield a
    NaN-coef row flagged converged=False rather than raising.

    Method B needs statsmodels (lazy-imported by the LMM helper). If it is not installed
    (e.g. a non-ML Databricks runtime), the LMM columns are left NaN and only the
    non-parametric companion is reported — checked once here so we don't emit one failure
    line per cell. Install with `%pip install statsmodels` to populate the LMM contrast."""
    has_statsmodels = importlib.util.find_spec("statsmodels") is not None
    if not has_statsmodels:
        print("  Method B (LMM) skipped: statsmodels not installed — Table 8.1b LMM "
              "columns will be NaN; non-parametric companion still computed. "
              "Install with `%pip install statsmodels` (or use an ML runtime).")
    rows = []
    for nma_flag, cls_label in classifications:
        for col, ep_label in ENDPOINTS:
            # Day-level 2-arm slice; arm reference = CE>0 so the coef is NMA - CE>0.
            nma = pdf.loc[pdf[nma_flag] == True, ["_userId", col]].assign(arm="NMA")  # noqa: E712
            cmp = pdf.loc[pdf[COMPARATOR_FLAG] == True, ["_userId", col]].assign(arm="CE>0")  # noqa: E712
            sl = pd.concat([nma, cmp], ignore_index=True).dropna(subset=[col])
            sl["arm"] = pd.Categorical(sl["arm"], categories=["CE>0", "NMA"])

            # Non-parametric companion: per-user within-arm MEDIAN, paired NMA - CE>0.
            nma_med = pdf[pdf[nma_flag] == True].groupby("_userId")[col].median()  # noqa: E712
            cmp_med = pdf[pdf[COMPARATOR_FLAG] == True].groupby("_userId")[col].median()  # noqa: E712
            np_diff = pd.DataFrame({"NMA": nma_med, "CMP": cmp_med}).dropna()
            np_diff = np_diff["NMA"] - np_diff["CMP"]
            if len(np_diff):
                np_median_diff = float(np_diff.median())
                np_lo, np_hi = nma_stats.cluster_bootstrap_ci(np_diff, statistic=np.median)
            else:
                np_median_diff = np_lo = np_hi = np.nan

            row = {
                "classification": cls_label, "endpoint": col, "label": ep_label,
                "coef": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "wald_p": np.nan,
                "converged": False,
                "n_users": int(sl["_userId"].nunique()), "n_days": int(len(sl)),
                "np_median_diff": np_median_diff, "np_boot_ci_lo": np_lo, "np_boot_ci_hi": np_hi,
            }
            n_users_nma = sl.loc[sl["arm"] == "NMA", "_userId"].nunique()
            n_users_cmp = sl.loc[sl["arm"] == "CE>0", "_userId"].nunique()
            fittable = n_users_nma >= 2 and n_users_cmp >= 2 and sl[col].nunique() >= 2
            if has_statsmodels and fittable:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = nma_stats.lmm_arm_contrast(sl, outcome=col, arm_col="arm")
                    assert "NMA" in res["term"], f"sign convention broken: term={res['term']}"
                    row.update({
                        "coef": res["coef"], "ci_lo": res["ci_lo"], "ci_hi": res["ci_hi"],
                        "wald_p": res["pvalue"], "converged": True,
                        "n_users": res["n_users"], "n_days": res["n_days"],
                    })
                except Exception as e:  # noqa: BLE001 — degenerate slices shouldn't abort the table
                    print(f"  LMM failed for {cls_label} / {col}: {e}")
            elif has_statsmodels and not fittable:
                print(f"  LMM skipped (degenerate) for {cls_label} / {col} "
                      f"(users NMA={n_users_nma}, CE>0={n_users_cmp})")
            rows.append(row)

    df = pd.DataFrame(rows)
    df["display"] = df.apply(
        lambda r: f"{r['coef']:.2f} [{r['ci_lo']:.2f}, {r['ci_hi']:.2f}]"
        if pd.notna(r["coef"]) else "N/A", axis=1)
    return df


def create_table_8_1c(pdf):
    """Table 8.1c: behavioral summary over CE>0 (meal-announcement) days. Per-user mean of
    each metric, then summarized across users as mean ± SD and median [IQR]. Descriptive
    context for the comparator arm; not inferential. See BEHAVIORAL for the meal-bolus proxy."""
    ce_days = pdf[pdf[COMPARATOR_FLAG] == True]  # noqa: E712
    rows = []
    for col, label in BEHAVIORAL:
        per_user = ce_days.groupby("_userId")[col].mean().dropna()
        n = len(per_user)
        rows.append({
            "measure": label,
            "mean": per_user.mean() if n else np.nan,
            "sd": per_user.std(ddof=1) if n > 1 else np.nan,
            "median": per_user.median() if n else np.nan,
            "q1": per_user.quantile(0.25) if n else np.nan,
            "q3": per_user.quantile(0.75) if n else np.nan,
            "n_users": int(n),
        })
    df = pd.DataFrame(rows)
    df["display_mean_sd"] = df.apply(lambda r: f"{r['mean']:.2f} ± {r['sd']:.2f}", axis=1)
    df["display_median_iqr"] = df.apply(
        lambda r: f"{r['median']:.2f} [{r['q1']:.2f}, {r['q3']:.2f}]", axis=1)
    return df


def _bin_sex(gender):
    """Normalize raw gender to Male / Female / Other/Unknown (mirrors FDA
    analysis_8-5._bin_gender). dev.default.user_gender stores single-char codes (M/F);
    missing/blank/unrecognized fall to Other/Unknown."""
    if pd.isna(gender) or str(gender).strip() == "":
        return "Other/Unknown"
    g = str(gender).strip().lower()
    if g in ("male", "m"):
        return "Male"
    if g in ("female", "f"):
        return "Female"
    return "Other/Unknown"


def create_sample_information(pdf):
    """Sample Information (Table 1) for one age cohort: per-user demographics over the
    cohort's eligible days (all arms — demographics describe the cohort, not an arm).
    Collapses to one row per user at their first eligible day for age (age_years can drift
    across a long window; the earliest day is the cohort-entry age) and the per-user-constant
    sex. Returns a 2-column ['metric', 'value'] frame; main() concatenates the three cohorts
    into table_8_1_sample_information.csv.

    Sex degrades gracefully: until `gender` is in the analysis-ready snapshot (see
    data_staging/export_user_day_analysis_ready.py), the sex rows read 'N/A (gender pending)'
    so the run completes before the Databricks regeneration step."""
    n_days = int(len(pdf))
    first = pdf.sort_values("local_day").groupby("_userId", as_index=False).first()
    n_users = int(len(first))
    age = pd.to_numeric(first["age_years"], errors="coerce").dropna()

    def pct(n):
        return f"{n} ({100.0 * n / n_users:.1f}%)" if n_users else "0 (n/a)"

    rows = [
        ("Users, n", str(n_users)),
        ("User-days, n", str(n_days)),
        ("Age, mean ± SD", f"{age.mean():.1f} ± {age.std(ddof=1):.1f}" if len(age) > 1 else "N/A"),
        ("Age, median [IQR]",
         f"{age.median():.1f} [{age.quantile(0.25):.1f}, {age.quantile(0.75):.1f}]"
         if len(age) else "N/A"),
        ("Age, min-max", f"{age.min():.1f}-{age.max():.1f}" if len(age) else "N/A"),
    ]
    # Age composition (uniform across cohorts; a self-check row for the adult/pediatric
    # cohorts, the real split for `all`). is_pediatric is NULL when DOB is unknown or the
    # age was implausible and nulled at extraction.
    ped = first["is_pediatric"]
    rows += [
        ("Pediatric (<18), n (%)", pct(int((ped == True).sum()))),  # noqa: E712
        ("Adult (>=18), n (%)", pct(int((ped == False).sum()))),  # noqa: E712
        ("Unknown age, n (%)", pct(int(ped.isna().sum()))),
    ]
    # Sex composition (guarded — gender may not be in the snapshot yet).
    if "gender" in first.columns:
        sex = first["gender"].apply(_bin_sex)
        for cat in SEX_CATEGORIES:
            rows.append((f"{cat}, n (%)", pct(int((sex == cat).sum()))))
    else:
        for cat in SEX_CATEGORIES:
            rows.append((f"{cat}, n (%)", "N/A (gender pending)"))
    return pd.DataFrame(rows, columns=["metric", "value"])


def _combine_sample_information(here=None):
    """Concatenate the three per-cohort sample_information.csv files into one Table-1 with
    adult/pediatric/all columns at outputs/analysis_8_1/table_8_1_sample_information.csv.
    Reads three tiny CSVs (not the snapshot); all three share the same metric rows in the
    same order, so concat on the metric index preserves layout."""
    if here is None:
        here = analysis_dir()
    root = os.path.join(here, "outputs", "analysis_8_1")
    cols = []
    for cohort in ("adult", "pediatric", "all"):
        path = os.path.join(root, cohort, "sample_information.csv")
        if not os.path.exists(path):
            print(f"  sample_information.csv missing for '{cohort}'; skipping combined Table-1")
            return
        cols.append(pd.read_csv(path).set_index("metric")["value"].rename(cohort))
    combined = pd.concat(cols, axis=1).reset_index()
    out = os.path.join(root, "table_8_1_sample_information.csv")
    combined.to_csv(out, index=False)
    print(f"wrote combined Sample Information (Table 1) to {out}")


def create_sex_missingness_sensitivity(pdf):
    """Sex-missingness sensitivity (mirrors FDA analysis_8-5.run_sensitivity_gender_missing).
    Compares users WITH a recorded sex against those WITHOUT, on per-user baseline
    characteristics (age, mean TIR, mean time <70, eligible-day count), to gauge whether
    gender missingness is associated with the cohort — i.e. whether the observed sex split
    is likely representative. Welch t-test per variable (N/A if either group <3). 'Missing'
    is null/blank raw gender (matches FDA), not the binned Other/Unknown category.

    Degrades gracefully when `gender` is absent from the snapshot (single note row)."""
    if "gender" not in pdf.columns:
        return pd.DataFrame([{"variable": "gender not in snapshot — sensitivity pending Databricks regen"}])
    from scipy import stats

    srt = pdf.sort_values("local_day")
    per_user = srt.groupby("_userId").agg(
        gender=("gender", "first"),       # per-user constant
        age_years=("age_years", "first"),  # age at first eligible day
        tir=("tir", "mean"),
        tbr=("tbr", "mean"),
    )
    per_user["n_days"] = pdf.groupby("_userId").size()
    raw = per_user["gender"]
    has = raw.notna() & (raw.astype(str).str.strip() != "")
    known, missing = per_user[has], per_user[~has]
    n_known, n_missing, n_tot = len(known), len(missing), len(per_user)

    def cell(s):
        s = pd.to_numeric(s, errors="coerce").dropna()
        return f"{s.mean():.1f} ± {s.std(ddof=1):.1f}" if len(s) > 1 else "N/A"

    def pval(col):
        a = pd.to_numeric(known[col], errors="coerce").dropna()
        b = pd.to_numeric(missing[col], errors="coerce").dropna()
        if len(a) < 3 or len(b) < 3:
            return "N/A"
        return f"{stats.ttest_ind(a, b, equal_var=False).pvalue:.3g}"

    kcol, mcol = f"Sex recorded (n={n_known})", f"Sex missing (n={n_missing})"
    rows = [{"variable": "Users, n (% of cohort)",
             kcol: f"{n_known} ({100.0 * n_known / n_tot:.1f}%)" if n_tot else "0",
             mcol: f"{n_missing} ({100.0 * n_missing / n_tot:.1f}%)" if n_tot else "0",
             "p (Welch t)": ""}]
    for col, label in [("age_years", "Age (years)"), ("tir", "Mean TIR 70-180 (%)"),
                       ("tbr", "Mean time <70 (%)"), ("n_days", "Eligible days, n")]:
        rows.append({"variable": label, kcol: cell(known[col]), mcol: cell(missing[col]),
                     "p (Welch t)": pval(col)})
    return pd.DataFrame(rows)


def create_nma_day_frequency(pdf):
    """Secondary objective (PLN-1008 §4, bullet 3): frequency and per-user distribution of
    each NMA-like day type in the cohort. Per nested classification: users contributing >=1
    such day (n, % of the cohort's eligible users), total user-days, and the per-user
    day-count distribution among contributors (mean ± SD, median [IQR], max). Descriptive;
    uses only the three NMA classification flags, which the CE>0 comparator restriction does
    not touch, so it is order-independent within run()."""
    n_cohort = int(pdf["_userId"].nunique())
    rows = []
    for flag, label in CLASSIFICATIONS:
        per_user = pdf[pdf[flag] == True].groupby("_userId").size()  # noqa: E712  days/user, contributors
        n_contrib = int(len(per_user))
        pct = f"{100.0 * n_contrib / n_cohort:.1f}%" if n_cohort else "n/a"
        if n_contrib > 1:
            mean_sd = f"{per_user.mean():.1f} ± {per_user.std(ddof=1):.1f}"
            med_iqr = f"{per_user.median():.1f} [{per_user.quantile(0.25):.1f}, {per_user.quantile(0.75):.1f}]"
        elif n_contrib == 1:
            mean_sd, med_iqr = f"{per_user.mean():.1f}", f"{per_user.median():.1f}"
        else:
            mean_sd = med_iqr = "N/A"
        rows.append({
            "classification": label,
            "Users contributing, n (%)": f"{n_contrib} ({pct})",
            "Total user-days, n": int(per_user.sum()),
            "Days/user (contributors), mean ± SD": mean_sd,
            "Days/user, median [IQR]": med_iqr,
            "Days/user, max": int(per_user.max()) if n_contrib else 0,
        })
    return pd.DataFrame(rows)


def make_stacked_bar(pdf):
    """Figure 8.1a: stacked bar of mean % time in each glycemic range (<54, 54-70, 70-180,
    180-250, >250) across the four arms. Per-user mean within arm, then averaged across
    users (equal weighting, consistent with Method A); per-bar user/day counts annotated."""
    df = pdf.copy()
    df["r_lt54"] = df["tbr_very_low"]
    df["r_54_70"] = df["tbr"] - df["tbr_very_low"]
    df["r_70_180"] = df["tir"]
    df["r_180_250"] = df["tar"] - df["tar_very_high"]
    df["r_gt250"] = df["tar_very_high"]

    range_keys = [rc for rc, _ in RANGE_COLS]
    arm_labels, user_ns, day_ns = [], [], []
    means = {rc: [] for rc in range_keys}
    for flag, arm_label in SUPPLEMENT_ARMS:
        sub = df[df[flag] == True]  # noqa: E712
        arm_labels.append(arm_label)
        day_ns.append(len(sub))
        user_means = sub.groupby("_userId")[range_keys].mean()
        user_ns.append(len(user_means))
        for rc in range_keys:
            means[rc].append(user_means[rc].mean() if len(user_means) else 0.0)

    fig, ax = plt.subplots(figsize=(10.5, 7))
    x = np.arange(len(SUPPLEMENT_ARMS))
    bottom = np.zeros(len(SUPPLEMENT_ARMS))
    for rc, rlabel in RANGE_COLS:
        vals = np.array(means[rc])
        ax.bar(x, vals, bottom=bottom, label=rlabel, color=RANGE_COLORS[rlabel], edgecolor="white")
        centers = bottom + vals / 2.0
        for xi, (v, cen) in enumerate(zip(vals, centers)):
            if rlabel == "<54":
                continue  # smallest range — skip the label
            if rlabel == "54-70":
                # second-lowest range is thin and crowds the bottom: offset the label
                # upward with a thin leader line.
                ax.annotate(f"{v:.1f}%", xy=(xi, cen), xytext=(0, 16),
                            textcoords="offset points", ha="center", va="bottom", fontsize=13,
                            arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
            else:
                ax.text(xi, cen, f"{v:.1f}%", ha="center", va="center", fontsize=13)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(arm_labels)
    ax.set_ylabel("Mean time in range (%)")
    ax.set_ylim(0, 108)
    ax.legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left")
    for xi, (u, dd) in enumerate(zip(user_ns, day_ns)):
        ax.text(xi, 101, f"users={u}\ndays={dd}", ha="center", va="bottom", fontsize=11)

    ax.set_title("Mean time in glycemic ranges by arm", fontsize=SUPTITLE_FS)
    fig.tight_layout()
    return fig


def arm_violin_groups(pdf, endpoint, *, include_high_ma=False):
    """Per-user-mean violin groups for the arms of one column, in the shape
    utils.plotting.violin_box_panel expects: (label, values, colour, alpha). Each arm carries its
    fixed DAY_TYPE_COLORS colour (3 nested NMA/CE=0 arms on the green ramp, CE>0 grey, HMA bronze) at
    a uniform VIOLIN_ALPHA. With `include_high_ma=True`, appends the CE>=3/BE>=3 high meal-announcement
    category as a 5th group (used by §8.1 Figure 8.1b; off by default so other callers stay 4-arm)."""
    groups = [
        (lab, per_user_arm_mean(pdf, endpoint, flag).dropna().to_numpy(), DAY_TYPE_COLORS[lab], VIOLIN_ALPHA)
        for flag, lab in CLASSIFICATIONS
    ]
    cmp_vals = per_user_arm_mean(pdf, endpoint, COMPARATOR_FLAG).dropna().to_numpy()
    groups.append((COMPARATOR_LABEL, cmp_vals, DAY_TYPE_COLORS[COMPARATOR_LABEL], VIOLIN_ALPHA))
    if include_high_ma:
        hi_vals = per_user_arm_mean(pdf, endpoint, HIGH_MA_FLAG).dropna().to_numpy()
        groups.append((HIGH_MA_LABEL, hi_vals, DAY_TYPE_COLORS[HIGH_MA_LABEL], VIOLIN_ALPHA))
    return groups


def make_violin_grids(pdf):
    """Figure 8.1b: per-user mean endpoints across the 5 arms (3 nested NMA + CE>0 + CE>=3/BE>=3), as
    a single 4×2 metric grid (all 8 endpoints). Returns {filename: figure}. Each arm carries its fixed
    DAY_TYPE_COLORS colour (3 nested NMA on the green ramp, CE>0 grey, HMA bronze); the panel title
    carries the endpoint's glycemic-range colour; separators divide NMA | CE>0 | HMA."""
    def panel(ax, col, label):
        violin_box_panel(ax, arm_violin_groups(pdf, col, include_high_ma=True),
                         title=label, title_color=endpoint_color(col), separators=(3.5, 4.5))

    return render_4x2_grid(panel, fig_stem="figure_8_1b_violin",
                           subtitle="per-user means by arm (+ CE>=3/BE>=3)", figsize=(14, 17))


def make_paired_delta_grids(pdf):
    """Figure 8.1c: within-user paired differences (NMA − CE>0) for the headline arm (CE=0/BE≤1),
    as a single 4×2 metric grid (all 8 endpoints). Returns {filename: figure}. CE=0/BE≤1's
    TIR/TAR/mean-glucose/CV contrasts are method-robust — Method A & Method B concur in sign on all
    three nested arms (post-D7-regen snapshot, decisions.md D5 update 2026-06-07); the Method-A-vs-B
    divergence caveat attaches to the below-range endpoints (<54, <70) only. The other arms'
    contrasts are in Tables 8.1a/8.1b. Endpoint range colour; solid line = mean, dashed = 0."""
    headline_flag = CLASSIFICATIONS[1][0]  # in_ce0_be_le1 — CE=0/BE<=1, the headline NMA arm

    def panel(ax, col, label):
        base = endpoint_color(col)
        nma = per_user_arm_mean(pdf, col, headline_flag)
        cmp = per_user_arm_mean(pdf, col, COMPARATOR_FLAG)
        hi = per_user_arm_mean(pdf, col, HIGH_MA_FLAG)
        delta = pd.DataFrame({"NMA": nma, "CMP": cmp}).dropna().eval("NMA - CMP").to_numpy()
        hi_delta = pd.DataFrame({"NMA": nma, "HI": hi}).dropna().eval("NMA - HI").to_numpy()
        overlay_hist_panel(ax, [(delta, "NMA − CE>0", base),
                                (hi_delta, "NMA − CE>=3/BE>=3", HIGH_MA_COLOR)],
                           xlabel="per-user Δ (NMA − comparator)", title=label, title_color=base)

    return render_4x2_grid(panel, fig_stem="figure_8_1c_paired_delta",
                           subtitle="within-user differences (NMA − CE>0, NMA − CE>=3/BE>=3) — "
                                    "headline NMA arm: CE=0 / BE≤1")


def create_windowed_contrast_table(pdf, fda_stats, classifications=CLASSIFICATIONS):
    """Table 12.1 (windowed-comparator sensitivity): the §8.1 NMA-vs-CE>0 contrast recomputed with a
    per-NMA-day ±(WINDOW_DAYS/2)-day temporal match — each NMA day is compared only to the mean of
    that user's CE>0 days within ±(WINDOW_DAYS/2) calendar days (utils.data_loader.windowed_matched_means),
    per nested classification × endpoint. Per-user windowed Δ (NMA − local CE>0 mean) summarized
    across users with equal weight (Method A: paired-t + Wilcoxon + t-CI). The pooled-within-user
    full-record contrast (table_8_1a_expanded / Table 8.1a) stays primary; the full-record Δ on the
    SAME matched users (diff_full) is reported alongside so the window's effect is visible."""
    rows = []
    for nma_flag, cls_label in classifications:
        per_user, cov = windowed_matched_means(pdf, nma_flag, COMPARATOR_FLAG, ENDPOINTS)
        pct = round(100.0 * cov["matched_nma_days"] / max(1, cov["total_nma_days"]), 1)
        # Full-record per-user means on the matched users (one groupby per arm, all endpoints).
        mu = pdf[pdf["_userId"].isin(set(per_user["_userId"]))] if len(per_user) else pdf.iloc[:0]
        nma_full = mu[mu[nma_flag] == True].groupby("_userId")[[c for c, _ in ENDPOINTS]].mean()  # noqa: E712
        cmp_full = mu[mu[COMPARATOR_FLAG] == True].groupby("_userId")[[c for c, _ in ENDPOINTS]].mean()  # noqa: E712
        for col, ep_label in ENDPOINTS:
            base = {"classification": cls_label, "endpoint": col, "label": ep_label,
                    "matched_nma_days": cov["matched_nma_days"],
                    "total_nma_days": cov["total_nma_days"], "pct_matched": pct}
            if len(per_user) > 1 and f"{col}__nma" in per_user.columns:
                wide = per_user[[f"{col}__nma", f"{col}__cmp"]].dropna()
                s = fda_stats.compute_paired_statistics(wide[f"{col}__cmp"], wide[f"{col}__nma"])
                full = pd.DataFrame({"NMA": nma_full[col], "CMP": cmp_full[col]}).dropna()
                base.update({
                    "n_users": s["n_pairs"], "nma_mean": s["seg2_mean"], "ce_gt0_mean": s["seg1_mean"],
                    "diff_win": s["diff_mean"], "diff_ci_low": s["diff_ci_low"],
                    "diff_ci_hi": s["diff_ci_hi"], "diff_median": s["diff_median"],
                    "p_ttest": s["p_ttest"], "p_wsrt": s["p_wsrt"],
                    "diff_full": (full["NMA"] - full["CMP"]).mean() if len(full) else np.nan,
                })
            else:
                base.update({"n_users": int(len(per_user)), "nma_mean": np.nan, "ce_gt0_mean": np.nan,
                             "diff_win": np.nan, "diff_ci_low": np.nan, "diff_ci_hi": np.nan,
                             "diff_median": np.nan, "p_ttest": np.nan, "p_wsrt": np.nan,
                             "diff_full": np.nan})
            rows.append(base)
    return pd.DataFrame(rows)


def make_windowed_delta_grids(pdf):
    """Figure 12.1c (single 4×2 grid; last in the windowed set, mirroring main 8.1c): within-user
    windowed Δ on the headline arm (CE=0/BE≤1), per-NMA-day ±(WINDOW_DAYS/2)-day match — overlays
    NMA − CE>0 and NMA − CE>=3/BE>=3 (the windowed companion to the full-record paired-delta grid
    8.1c). NMA−CE>0 in the endpoint range colour, NMA−CE>=3/BE>=3 in bronze; mean line, dashed 0.
    CE=0/BE≤1's TIR/TAR/mean-glucose/CV contrasts are method-robust (Method A & B concur in sign on
    all three nested arms, post-D7-regen snapshot, decisions.md D5 update 2026-06-07); the
    Method-A-vs-B divergence caveat attaches to the below-range endpoints (<54, <70) only."""
    headline_flag = CLASSIFICATIONS[1][0]  # in_ce0_be_le1 — CE=0/BE<=1, the headline NMA arm
    per_user, _ = windowed_matched_means(pdf, headline_flag, COMPARATOR_FLAG, ENDPOINTS)
    per_user_hi, _ = windowed_matched_means(pdf, headline_flag, HIGH_MA_FLAG, ENDPOINTS)

    def panel(ax, col, label):
        base = endpoint_color(col)
        delta = (per_user[f"{col}__nma"] - per_user[f"{col}__cmp"]).dropna().to_numpy()
        hi_delta = (per_user_hi[f"{col}__nma"] - per_user_hi[f"{col}__cmp"]).dropna().to_numpy()
        overlay_hist_panel(ax, [(delta, "NMA − CE>0 (win)", base),
                                (hi_delta, "NMA − CE>=3/BE>=3 (win)", HIGH_MA_COLOR)],
                           xlabel="per-user Δ (windowed)", title=label, title_color=base)

    return render_4x2_grid(panel, fig_stem="figure_12_1c_windowed_delta",
                           subtitle=f"within-user windowed Δ (NMA − CE>0, NMA − CE>=3/BE>=3; "
                                    f"±{WINDOW_DAYS // 2}d) — headline NMA arm: CE=0 / BE≤1")


def make_windowed_violin_grids(pdf):
    """Figure 12.1b (single 4×2 grid): per-user windowed means by arm — the 3 nested NMA arms, the CE>0
    comparator, and the CE>=3/BE>=3 high-engagement arm, each on the per-NMA-day ±(WINDOW_DAYS/2)-day
    match. The windowed companion to the full-record violin grid (8.1b). Each arm carries its fixed
    DAY_TYPE_COLORS colour (3 nested NMA on the green ramp, CE>0 grey, HMA bronze)."""
    win = {lab: windowed_matched_means(pdf, flag, COMPARATOR_FLAG, ENDPOINTS)[0]
           for flag, lab in CLASSIFICATIONS}
    broadest = CLASSIFICATIONS[-1][1]  # CE=0/BE<=inf — supplies the CE>0 windowed comparator group
    win_hi = windowed_matched_means(pdf, HIGH_MA_FLAG, COMPARATOR_FLAG, ENDPOINTS)[0]

    def panel(ax, col, label):
        groups = [(lab, win[lab][f"{col}__nma"].dropna().to_numpy(), DAY_TYPE_COLORS[lab], VIOLIN_ALPHA)
                  for _flag, lab in CLASSIFICATIONS]
        groups.append((COMPARATOR_LABEL, win[broadest][f"{col}__cmp"].dropna().to_numpy(),
                       DAY_TYPE_COLORS[COMPARATOR_LABEL], VIOLIN_ALPHA))
        groups.append((HIGH_MA_LABEL, win_hi[f"{col}__nma"].dropna().to_numpy(),
                       DAY_TYPE_COLORS[HIGH_MA_LABEL], VIOLIN_ALPHA))
        violin_box_panel(ax, groups, title=label, title_color=endpoint_color(col), separators=(3.5, 4.5))

    return render_4x2_grid(panel, fig_stem="figure_12_1b_windowed_violin",
                           subtitle=f"per-user windowed means by arm (NMA, CE>0, CE>=3/BE>=3; "
                                    f"±{WINDOW_DAYS // 2}d match)", figsize=(14, 17))


def make_windowed_stacked_bar(pdf):
    """Figure 12.1a (windowed companion to 8.1a, first in the windowed set to mirror the main figure
    order — stacked bar, violins, Δ-histograms): mean % time in glycemic ranges by arm
    from per-NMA-day ±(WINDOW_DAYS/2)-day matched per-user means — the 3 NMA arms, CE>0, and the
    CE>=3/BE>=3 high-engagement arm. Equal-user weight (consistent with 8.1a / Method A); each
    range is derived per user from the windowed endpoint means (linear, so == windowed mean of the
    range). Arm matching: NMA arms + CE>=3/BE>=3 vs CE>0; CE>0 from the broadest arm's match."""
    win = {lab: windowed_matched_means(pdf, flag, COMPARATOR_FLAG, ENDPOINTS)[0]
           for flag, lab in CLASSIFICATIONS}
    broadest = CLASSIFICATIONS[-1][1]
    win_hi = windowed_matched_means(pdf, HIGH_MA_FLAG, COMPARATOR_FLAG, ENDPOINTS)[0]

    def ranges_for(arm_label):
        if arm_label == COMPARATOR_LABEL:
            src, sfx = win[broadest], "__cmp"
        elif arm_label == HIGH_MA_LABEL:
            src, sfx = win_hi, "__nma"
        else:
            src, sfx = win[arm_label], "__nma"
        g = lambda ep: src[f"{ep}{sfx}"]  # noqa: E731
        return pd.DataFrame({
            "r_lt54": g("tbr_very_low"),
            "r_54_70": g("tbr") - g("tbr_very_low"),
            "r_70_180": g("tir"),
            "r_180_250": g("tar") - g("tar_very_high"),
            "r_gt250": g("tar_very_high"),
        }).dropna()

    range_keys = [rc for rc, _ in RANGE_COLS]
    arm_labels, user_ns = [], []
    means = {rc: [] for rc in range_keys}
    for _flag, arm_label in SUPPLEMENT_ARMS:
        r = ranges_for(arm_label)
        arm_labels.append(arm_label)
        user_ns.append(len(r))
        for rc in range_keys:
            means[rc].append(r[rc].mean() if len(r) else 0.0)

    fig, ax = plt.subplots(figsize=(10.5, 7))
    x = np.arange(len(SUPPLEMENT_ARMS))
    bottom = np.zeros(len(SUPPLEMENT_ARMS))
    for rc, rlabel in RANGE_COLS:
        vals = np.array(means[rc])
        ax.bar(x, vals, bottom=bottom, label=rlabel, color=RANGE_COLORS[rlabel], edgecolor="white")
        centers = bottom + vals / 2.0
        for xi, (v, cen) in enumerate(zip(vals, centers)):
            if rlabel == "<54":
                continue
            if rlabel == "54-70":
                ax.annotate(f"{v:.1f}%", xy=(xi, cen), xytext=(0, 16), textcoords="offset points",
                            ha="center", va="bottom", fontsize=13,
                            arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
            else:
                ax.text(xi, cen, f"{v:.1f}%", ha="center", va="center", fontsize=13)
        bottom += vals
    ax.set_xticks(x); ax.set_xticklabels(arm_labels)
    ax.set_ylabel("Mean time in range (%)"); ax.set_ylim(0, 108)
    ax.legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left")
    for xi, u in enumerate(user_ns):
        ax.text(xi, 101, f"users={u}", ha="center", va="bottom", fontsize=11)
    ax.set_title(f"Windowed mean time in glycemic ranges by arm (±{WINDOW_DAYS // 2}d match)",
                 fontsize=SUPTITLE_FS)
    fig.tight_layout()
    return fig


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort: Literal["adult", "pediatric", "all"] = "all",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    """Run Analysis 8.1 for one age cohort. Outputs land in
    outputs/analysis_8_1/<cohort>/ unless an explicit output_dir is given.

    `min_age` defaults to the §6 floor (MIN_AGE=6); pass None to disable.

    Source priority: an explicit `csv_path`, else `spark.table(analysis_ready_table)` when
    a Spark session is given, else (running locally, no Spark) the CSV snapshot that
    data_staging/export_user_day_analysis_ready.py writes — outputs/<table-name>.csv.

    figures_only=True skips the SLOW table writes (LMM + cluster-bootstrap contrasts) and re-renders
    only the figures, in place (existing table CSVs untouched) — for fast figure-tweak iteration; do a
    full run first so the tables exist. figs_filter (a substring, e.g. "8_1b") renders only matching
    figure builders (tags: 8_1a/b/c, 12_1a/b/c) and implies figures_only."""
    figures_only = figures_only or figs_filter is not None  # filtering figures ⇒ skip the tables
    fda_stats = load_fda_statistics()
    nma_stats = load_nma_statistics()
    here = analysis_dir()
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_1", cohort)
    # Clear this cohort's dir first so it reflects only the current run (no stale files from
    # renamed/removed outputs). Only the per-cohort dir is wiped — the sibling `supplement/`
    # dir (exploratory weighting-sensitivity artifacts) and the parent-level combined table
    # are left untouched.
    if os.path.isdir(output_dir) and not figures_only:
        shutil.rmtree(output_dir)  # full run starts clean; figures_only overwrites PNGs in place
    os.makedirs(output_dir, exist_ok=True)

    if csv_path is None and spark is None:
        csv_path = default_analysis_ready_csv(analysis_ready_table)
        print(f"no Spark session; reading analysis-ready snapshot: {csv_path}")
    if csv_path is not None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"analysis-ready CSV not found at {csv_path}. Run "
                "data_staging/export_user_day_analysis_ready.py to create it (it writes "
                "outputs/<table-name>.csv), or pass csv_path/--csv_path."
            )
        pdf = prepare_day_level(pd.read_csv(csv_path))
    else:
        pdf = load_day_level(spark, analysis_ready_table)
    pdf = filter_cohort(pdf, cohort=cohort, min_age=min_age)
    pdf = restrict_comparator(pdf)

    # ---- Tables (SLOW: LMM + cluster-bootstrap contrasts). Skipped under figures_only so a figure
    # tweak re-renders in seconds without recomputing the statistics. ----
    contrasts = None
    if not figures_only:
        # Method A paired contrasts.
        contrasts = contrasts_table(pdf, fda_stats)
        print(contrasts.to_string(index=False))
        contrasts.to_csv(os.path.join(output_dir, "table_8_1a_expanded.csv"), index=False)

        # Sample Information (Table 1) for this cohort — age + sex demographics.
        create_sample_information(pdf).to_csv(
            os.path.join(output_dir, "sample_information.csv"), index=False)
        # Sex-missingness sensitivity (FDA §8.5 analog): recorded vs missing sex on baselines.
        create_sex_missingness_sensitivity(pdf).to_csv(
            os.path.join(output_dir, "sex_missingness_sensitivity.csv"), index=False)
        # Secondary objective (§4 bullet 3): NMA-day-type frequency + per-user distribution.
        create_nma_day_frequency(pdf).to_csv(
            os.path.join(output_dir, "nma_day_frequency.csv"), index=False)

        # Tables 8.1a / 8.1b / 8.1c.
        create_table_8_1a(pdf).to_csv(os.path.join(output_dir, "table_8_1a_per_user_means.csv"), index=False)
        create_table_8_1b(pdf, nma_stats).to_csv(os.path.join(output_dir, "table_8_1b_lmm_contrasts.csv"), index=False)
        create_table_8_1c(pdf).to_csv(os.path.join(output_dir, "table_8_1c_behavioral_summary.csv"), index=False)
        # Table 12.1 (windowed-comparator sensitivity): NMA vs CE>0 with a per-NMA-day ±45d match.
        create_windowed_contrast_table(pdf, fda_stats).to_csv(
            os.path.join(output_dir, "table_12_1a_windowed_sensitivity.csv"), index=False)
        # Appendix §12.1: the high meal-announcement arm (CE>=3/BE>=3) contrasted vs CE>0 — full-record
        # LMM (table_12_1b_high_engagement_lmm) + windowed (table_12_1c_high_engagement_windowed). NB CE>=3/BE>=3 ⊂ CE>0 (overlapping
        # reference, "heavy vs typical meal day"); Table 8.1a already carries the descriptive column.
        high_ma = [(HIGH_MA_FLAG, HIGH_MA_LABEL)]
        create_table_8_1b(pdf, nma_stats, classifications=high_ma).to_csv(
            os.path.join(output_dir, "table_12_1b_high_engagement_lmm.csv"), index=False)
        create_windowed_contrast_table(pdf, fda_stats, classifications=high_ma).to_csv(
            os.path.join(output_dir, "table_12_1c_high_engagement_windowed.csv"), index=False)

    # ---- Figures. Each builder is a thunk → {filename: figure}, keyed by a short tag; figs_filter
    # (a substring) renders only matching builders. §8.1 main set: 8.1a stacked ranges by arm; 8.1b
    # per-user violin grids (all 8 endpoints, 5 arms); 8.1c paired-difference grids. Appendix §12.1
    # windowed (±45d) companions mirror that order — 12.1a stacked, 12.1b violins, 12.1c Δ-hist. ----
    fig_builders = {
        "8_1a": lambda: {"figure_8_1a_stacked_bars.png": make_stacked_bar(pdf)},
        "8_1b": lambda: make_violin_grids(pdf),
        "8_1c": lambda: make_paired_delta_grids(pdf),
        "12_1a": lambda: {"figure_12_1a_windowed_stacked_bars.png": make_windowed_stacked_bar(pdf)},
        "12_1b": lambda: make_windowed_violin_grids(pdf),
        "12_1c": lambda: make_windowed_delta_grids(pdf),
    }
    n = 0
    for key, build in fig_builders.items():
        if figs_filter and figs_filter not in key:
            continue
        for fname, fig in build().items():
            fig.savefig(os.path.join(output_dir, fname), dpi=150)
            plt.close(fig)
            n += 1

    bits = []
    if figures_only:
        bits.append("figures_only — tables skipped")
    if figs_filter:
        bits.append(f"figs~'{figs_filter}'")
    tag = f" ({'; '.join(bits)})" if bits else ""
    print(f"wrote analysis 8.1 ({cohort}) — {n} figure(s){tag} to {output_dir}")
    return contrasts


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
    figures_only=False,
    figs_filter=None,
):
    """Orchestrate the §7.6 cohort split: adult and pediatric reported separately, plus a
    pooled `all` sanity run. Each lands in its own outputs/analysis_8_1/<cohort>/ dir. The
    §6 min-age floor (MIN_AGE=6) is applied to every cohort. Pass `csv_path` to run locally
    off the analysis-ready CSV snapshot (no Spark). figures_only / figs_filter forward the
    fast figure-only path to each cohort run."""
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path, figures_only=figures_only, figs_filter=figs_filter)
    # Combined Sample Information (Table 1) across the three cohorts (a table — skip in figures-only).
    if not (figures_only or figs_filter):
        _combine_sample_information()


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--analysis_ready_table", default="dev.fda_510k_rwd.nma_user_day_analysis_ready")
    _parser.add_argument("--csv_path", default=None,
                         help="analysis-ready CSV snapshot (run without Spark); defaults to "
                              "outputs/<table-name>.csv when no Spark session is present")
    _parser.add_argument("--output_dir", default=None)
    _parser.add_argument("--cohort", default=None, choices=["adult", "pediatric", "all"])
    _parser.add_argument("--min_age", type=int, default=MIN_AGE,
                         help=f"§6 min-age floor in years (default {MIN_AGE}); known-younger "
                              "users dropped, unknown-age retained")
    _parser.add_argument("--figures-only", dest="figures_only", action="store_true",
                         help="re-render figures only, skipping the slow LMM/bootstrap table writes "
                              "(do a full run first so the tables exist)")
    _parser.add_argument("--figs", dest="figs_filter", default=None,
                         help="render only figure builders whose tag contains this substring "
                              "(e.g. 8_1b); implies --figures-only. Tags: 8_1a/b/c, 12_1a/b/c")
    _args, _ = _parser.parse_known_args()

    # Databricks injects a `spark` global; a local run has none. Resolve it safely so
    # running this file locally never trips over the undefined name — run() then falls back
    # to the analysis-ready CSV snapshot.
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        _spark = None

    if _args.cohort is None:
        main(_spark, _args.analysis_ready_table, min_age=_args.min_age, csv_path=_args.csv_path,
             figures_only=_args.figures_only, figs_filter=_args.figs_filter)
    else:
        run(_spark, _args.analysis_ready_table, _args.output_dir,
            cohort=_args.cohort, min_age=_args.min_age, csv_path=_args.csv_path,
            figures_only=_args.figures_only, figs_filter=_args.figs_filter)
