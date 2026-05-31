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
    sample_information.csv              (Table 1: per-cohort age + sex demographics, user/day counts)
    sex_missingness_sensitivity.csv     (recorded-vs-missing-sex baseline comparison; FDA §8.5 analog)
    nma_day_frequency.csv               (§4 secondary obj. bullet 3: per-classification NMA-day-type frequency + per-user distribution)
    method_a_contrasts.csv              (per classification x endpoint paired stats)
    table_8_1a_per_user_means.csv       (per-user means ± SD by arm, with user/day counts)
    table_8_1b_lmm_contrasts.csv        (Method B LMM contrast NMA-CE>0 + np median sensitivity)
    table_8_1c_behavioral_summary.csv   (CE>0-day behavioral metrics: mean±SD, median[IQR])
    method_a_panel_a.png                (per endpoint: 4 arms of per-user means, jittered points)
    method_a_panel_b_{central,lows,highs}.png   (paired-delta histograms)
    figure_8_1a_stacked_bars.png        (mean time in 5 glycemic ranges across the 4 arms)
    figure_8_1b_tir_violin_box.png      (per-user TIR violin+box across the 4 arms)
    figure_8_1c_tbr_violin_box.png      (per-user time <70 and <54 violin+box across the 4 arms)

main() also writes the combined Sample Information across cohorts (run via the no-`--cohort`
entry point):
    analysis/outputs/analysis_8_1/table_8_1_sample_information.csv  (adult/pediatric/all columns)
"""

import argparse
import importlib.util
import os
import shutil
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# (column, display label) — PLN-1008 §6 endpoints in the endpoints table.
ENDPOINTS = [
    ("tir", "Time 70-180 mg/dL (%)"),
    ("tbr", "Time <70 mg/dL (%)"),
    ("tbr_very_low", "Time <54 mg/dL (%)"),
    ("tar", "Time >180 mg/dL (%)"),
    ("tar_very_high", "Time >250 mg/dL (%)"),
    ("mean_glucose", "Mean glucose (mg/dL)"),
    ("cv", "CV (%)"),
    ("hypo_events", "Hypo events / day"),
]

# The three nested NMA-like classifications: (arm flag column, label).
CLASSIFICATIONS = [
    ("in_ce0_be0", "CE=0/BE=0"),
    ("in_ce0_be_le1", "CE=0/BE<=1"),
    ("in_ce0_be_inf", "CE=0/BE<=inf"),
]
COMPARATOR_FLAG = "in_ce_gt0"
COMPARATOR_LABEL = "CE>0"

# Arms shown in the box-plot figure (3 nested NMA arms + comparator).
FIGURE_ARMS = CLASSIFICATIONS + [(COMPARATOR_FLAG, COMPARATOR_LABEL)]

BOOTSTRAP_N = 1000
BOOTSTRAP_SEED = 20260520

# Per-classification colors for the delta histograms (panel B).
CLS_COLORS = ["#E03830", "#FF6D5C", "#607cff"]
ZERO_LINE_COLOR = "#241144"

# Panel B is split into three subfigures, grouped by endpoint type.
ENDPOINT_GROUPS = [
    ("central", [("tir", "Time 70-180 mg/dL (%)"), ("mean_glucose", "Mean glucose (mg/dL)")]),
    ("lows", [("tbr", "Time <70 mg/dL (%)"), ("tbr_very_low", "Time <54 mg/dL (%)"),
              ("hypo_events", "Hypo events / day")]),
    ("highs", [("tar", "Time >180 mg/dL (%)"), ("tar_very_high", "Time >250 mg/dL (%)"),
               ("cv", "CV (%)")]),
]

# Disjoint glycemic ranges for the stacked bar (sum to ~100% per day), derived from the
# cumulative endpoints, with their stacked-bar colors.
RANGE_COLS = [
    ("r_lt54", "<54"),
    ("r_54_70", "54-70"),
    ("r_70_180", "70-180"),
    ("r_180_250", "180-250"),
    ("r_gt250", ">250"),
]
RANGE_COLORS = {
    "<54": "#E03830",
    "54-70": "#FF6D5C",
    "70-180": "#5AC692",
    "180-250": "#AA85DE",
    ">250": "#7046CC",
}

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

# §6 / PLN-1001 minimum age (years), applied in analysis via filter_cohort. Users KNOWN to
# be younger are dropped; unknown/nulled-age users are retained (PLN-1001
# `is_age_eligible OR dob IS NULL`). Implausible-high ages are already nulled at extraction
# (export_user_day_age.MAX_PLAUSIBLE_AGE), so this floor + that bound together gate age.
MIN_AGE = 6


def _here():
    """The analysis/ directory, with the Databricks-notebook fallback where __file__ is
    undefined (mirrors the path handling in the data_staging scripts)."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/analysis"
        )


def _load_fda_statistics():
    """Load FDA analysis/utils/statistics.py by path as `fda_statistics` (avoids the
    `utils` package-name collision with our own analysis/utils)."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        here = (
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
            "no_meal_announcement/analysis"
        )
    path = os.path.normpath(
        os.path.join(here, "..", "..", "FDA_real_world_data", "analysis", "utils", "statistics.py")
    )
    spec = importlib.util.spec_from_file_location("fda_statistics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, here


def _load_nma_statistics(here):
    """Load the local `analysis/utils/statistics.py` (LMM + cluster_bootstrap_ci) by
    path as `nma_statistics`, so it works both as a script (Databricks) and on import."""
    path = os.path.join(here, "utils", "statistics.py")
    spec = importlib.util.spec_from_file_location("nma_statistics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def cluster_bootstrap_median_ci(diff, n_boot=BOOTSTRAP_N, seed=BOOTSTRAP_SEED):
    """95% CI for the median paired difference, resampling users with replacement."""
    d = np.asarray(pd.Series(diff).dropna())
    n = len(d)
    if n < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    medians = np.median(d[rng.integers(0, n, size=(n_boot, n))], axis=1)
    return float(np.percentile(medians, 2.5)), float(np.percentile(medians, 97.5))


# Columns coerced to float: endpoints (Spark `* 100.0` returns Decimal/object) plus the
# behavioral / age columns used by Table 8.1c and the cohort split.
NUMERIC_COLS = [col for col, _ in ENDPOINTS] + [
    "carb_entry_count", "bolus_entry_count", "carb_grams_total", "tdd_units", "age_years",
]


def prepare_day_level(pdf):
    """Pure transform: coerce numeric columns and keep eligible days of eligible users
    (§7.1). Split out from the Spark read so it is unit-testable on a plain DataFrame."""
    pdf = pdf.copy()
    for c in NUMERIC_COLS:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    pdf = pdf[(pdf["day_eligible"] == True) & (pdf["user_eligible"] == True)].copy()  # noqa: E712
    return pdf


def load_day_level(spark, analysis_ready_table):
    """Eligible-day rows of eligible users, with arm flags + endpoints."""
    return prepare_day_level(spark.table(analysis_ready_table).toPandas())


def _default_analysis_ready_csv(here, analysis_ready_table):
    """Where data_staging/export_user_day_analysis_ready.py writes its CSV snapshot —
    no_meal_announcement/outputs/<table-name>.csv (here is the analysis/ dir)."""
    fname = analysis_ready_table.split(".")[-1] + ".csv"
    return os.path.normpath(os.path.join(here, "..", "outputs", fname))


def filter_cohort(pdf, cohort: Literal["adult", "pediatric", "all"] = "all", min_age=MIN_AGE):
    """§7.6 age cohort. NULL `is_pediatric` (DOB unknown or implausible) is excluded from
    both adult and pediatric, retained in `all`. `min_age` (§6 floor, default MIN_AGE=6)
    drops users KNOWN to be younger than the floor; unknown/nulled-age users are retained
    (PLN-1001 `is_age_eligible OR dob IS NULL`). Pass `min_age=None` to disable the floor."""
    out = pdf
    if cohort == "adult":
        out = out[out["is_pediatric"] == False]  # noqa: E712
    elif cohort == "pediatric":
        out = out[out["is_pediatric"] == True]  # noqa: E712
    elif cohort != "all":
        raise ValueError(f"unknown cohort: {cohort!r}")
    if min_age is not None:
        out = out[out["age_years"].isna() | (out["age_years"] >= min_age)]
    return out.copy()


def restrict_comparator(pdf):
    """§8.1 Table 8.1a: the CE>0 arm is restricted to users who contributed >=1 CE=0 day
    in at least one of the three NMA classifications. The arms are nested under
    in_ce0_be_inf (CE=0), so that reduces to ">=1 in_ce0_be_inf day". Zero in_ce_gt0 for
    users with no CE=0 day so every table/figure uses the restricted comparator."""
    ce0_users = set(pdf.loc[pdf["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    out = pdf.copy()
    out.loc[~out["_userId"].isin(ce0_users), "in_ce_gt0"] = False
    return out


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
    columns = the 4 arms (3 nested NMA + CE>0)."""
    arm_labels = [lab for _, lab in FIGURE_ARMS]
    table = {}  # row label -> {arm label: display cell}
    for col, ep_label in ENDPOINTS:
        table[ep_label] = {}
        for flag, arm_label in FIGURE_ARMS:
            m = per_user_arm_mean(pdf, col, flag).dropna()
            if len(m) > 1:
                table[ep_label][arm_label] = f"{m.mean():.1f} ± {m.std(ddof=1):.1f}"
            elif len(m) == 1:
                table[ep_label][arm_label] = f"{m.mean():.1f}"
            else:
                table[ep_label][arm_label] = "N/A"
    user_days, users_contrib = {}, {}
    for flag, arm_label in FIGURE_ARMS:
        sub = pdf[pdf[flag] == True]  # noqa: E712
        user_days[arm_label] = int(len(sub))
        users_contrib[arm_label] = int(sub["_userId"].nunique())
    table["User-days, n"] = user_days
    table["Users contributing, n"] = users_contrib

    out = pd.DataFrame.from_dict(table, orient="index", columns=arm_labels)
    out.index.name = "metric"
    return out.reset_index()


def create_table_8_1b(pdf, nma_stats):
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
    for nma_flag, cls_label in CLASSIFICATIONS:
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
        here = _here()
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


def make_panel_a(pdf):
    """Panel A: per endpoint, box plot of per-user means across the 4 arms (3 NMA + CE>0),
    with jittered per-user points."""
    n = len(ENDPOINTS)
    ncols = 4
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4.5 * nrows))
    axes = np.atleast_1d(axes).ravel()
    rng = np.random.default_rng(BOOTSTRAP_SEED)

    for ax, (col, label) in zip(axes, ENDPOINTS):
        data, labels = [], []
        for flag, arm_label in FIGURE_ARMS:
            data.append(per_user_arm_mean(pdf, col, flag).dropna().to_numpy())
            labels.append(arm_label)
        ax.boxplot(data, positions=range(1, len(data) + 1), widths=0.5, showfliers=False)
        for pos, vals in enumerate(data, start=1):
            ax.scatter(rng.normal(pos, 0.05, size=len(vals)), vals, s=5, alpha=0.2, color="#607cff")
        ax.set_xticks(range(1, len(data) + 1))
        ax.set_xticklabels([f"{lab}\n(n={len(d)})" for lab, d in zip(labels, data)], fontsize=8)
        ax.set_title(label, fontsize=11)

    for ax in axes[n:]:
        ax.set_visible(False)

    fig.suptitle("Analysis 8.1 Method A (a): per-user mean endpoints by arm (NMA-like vs CE>0)",
                 fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    return fig


def make_panel_b(pdf, group_label, group_endpoints):
    """Panel B subfigure for one endpoint group: within-user paired delta (NMA - CE>0)
    histograms, the three nested classifications stacked as rows, one endpoint per column;
    x-axis shared down each column so the deltas align."""
    nrows = len(CLASSIFICATIONS)
    ncols = len(group_endpoints)
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.6 * ncols, 3.2 * nrows),
                             sharex="col", squeeze=False)

    for j, (col, label) in enumerate(group_endpoints):
        comparator = per_user_arm_mean(pdf, col, COMPARATOR_FLAG)
        for i, ((nma_flag, cls_label), color) in enumerate(zip(CLASSIFICATIONS, CLS_COLORS)):
            ax = axes[i][j]
            nma = per_user_arm_mean(pdf, col, nma_flag)
            paired = pd.DataFrame({"NMA": nma, "CMP": comparator}).dropna()
            delta = paired["NMA"] - paired["CMP"]
            if len(delta) > 0:
                ax.hist(delta, bins=20, color=color, edgecolor="black", alpha=0.8)
                ax.text(0.03, 0.95, f"n={len(delta)}\nmean={delta.mean():.1f}",
                        transform=ax.transAxes, va="top", fontsize=7)
            ax.axvline(0, color=ZERO_LINE_COLOR, ls="--", lw=1.5)
            if i == 0:
                ax.set_title(label, fontsize=10)
            if j == 0:
                ax.set_ylabel(f"{cls_label}\nUsers", fontsize=9)
            if i == nrows - 1:
                ax.set_xlabel("Δ (NMA − CE>0)", fontsize=8)

    fig.suptitle(f"Analysis 8.1 Method A (b): paired differences (NMA-like − CE>0) — {group_label}",
                 fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


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
    for flag, arm_label in FIGURE_ARMS:
        sub = df[df[flag] == True]  # noqa: E712
        arm_labels.append(arm_label)
        day_ns.append(len(sub))
        user_means = sub.groupby("_userId")[range_keys].mean()
        user_ns.append(len(user_means))
        for rc in range_keys:
            means[rc].append(user_means[rc].mean() if len(user_means) else 0.0)

    fig, ax = plt.subplots(figsize=(9, 7))
    x = np.arange(len(FIGURE_ARMS))
    bottom = np.zeros(len(FIGURE_ARMS))
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
                            textcoords="offset points", ha="center", va="bottom", fontsize=12,
                            arrowprops=dict(arrowstyle="-", lw=0.6, color="gray"))
            else:
                ax.text(xi, cen, f"{v:.1f}%", ha="center", va="center", fontsize=12)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(arm_labels)
    ax.set_ylabel("Mean time in range (%)")
    ax.set_ylim(0, 108)
    ax.legend(title="Glucose (mg/dL)", bbox_to_anchor=(1.01, 1), loc="upper left")
    for xi, (u, dd) in enumerate(zip(user_ns, day_ns)):
        ax.text(xi, 101, f"users={u}\ndays={dd}", ha="center", va="bottom", fontsize=8)

    ax.set_title("Figure 8.1a: Mean time in glycemic ranges by arm", fontsize=13)
    fig.tight_layout()
    return fig


def _violin_box_by_arm(ax, pdf, endpoint, title):
    """Per-user mean `endpoint` for each of the 4 arms as overlaid violin + box with
    jittered per-user scatter. Empty/singleton arms are skipped so sparse cohorts (e.g.
    pediatric) don't raise."""
    data, labels = [], []
    for flag, arm_label in FIGURE_ARMS:
        data.append(per_user_arm_mean(pdf, endpoint, flag).dropna().to_numpy())
        labels.append(arm_label)
    positions = list(range(1, len(data) + 1))

    violin = [(p, d) for p, d in zip(positions, data) if len(d) > 1]
    if violin:
        vp = ax.violinplot([d for _, d in violin], positions=[p for p, _ in violin],
                           showextrema=False, widths=0.8)
        for body in vp["bodies"]:
            body.set_alpha(0.25)
            body.set_facecolor("#607cff")
    box = [(p, d) for p, d in zip(positions, data) if len(d) > 0]
    if box:
        ax.boxplot([d for _, d in box], positions=[p for p, _ in box], widths=0.25,
                   showfliers=False)

    rng = np.random.default_rng(BOOTSTRAP_SEED)
    for pos, vals in zip(positions, data):
        if len(vals):
            ax.scatter(rng.normal(pos, 0.05, size=len(vals)), vals, s=6, alpha=0.25,
                       color="#607cff")
    ax.set_xticks(positions)
    ax.set_xticklabels([f"{lab}\n(n={len(d)})" for lab, d in zip(labels, data)], fontsize=8)
    ax.set_title(title, fontsize=11)


def make_figure_8_1b(pdf):
    """Figure 8.1b: per-user mean TIR (70-180 mg/dL) violin+box across the 4 arms."""
    fig, ax = plt.subplots(figsize=(9, 6))
    _violin_box_by_arm(ax, pdf, "tir", "Figure 8.1b: Per-user mean TIR (70-180 mg/dL) by arm")
    ax.set_ylabel("Per-user mean TIR (%)")
    fig.tight_layout()
    return fig


def make_figure_8_1c(pdf):
    """Figure 8.1c: per-user mean time below range violin+box across the 4 arms — <70 in
    the left subplot, <54 in the right."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    _violin_box_by_arm(axes[0], pdf, "tbr", "Time <70 mg/dL")
    axes[0].set_ylabel("Per-user mean time <70 (%)")
    _violin_box_by_arm(axes[1], pdf, "tbr_very_low", "Time <54 mg/dL")
    axes[1].set_ylabel("Per-user mean time <54 (%)")
    fig.suptitle("Figure 8.1c: Per-user time below range by arm", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def run(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    output_dir=None,
    cohort: Literal["adult", "pediatric", "all"] = "all",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Run Analysis 8.1 for one age cohort. Outputs land in
    outputs/analysis_8_1/<cohort>/ unless an explicit output_dir is given.

    `min_age` defaults to the §6 floor (MIN_AGE=6); pass None to disable.

    Source priority: an explicit `csv_path`, else `spark.table(analysis_ready_table)` when
    a Spark session is given, else (running locally, no Spark) the CSV snapshot that
    data_staging/export_user_day_analysis_ready.py writes — outputs/<table-name>.csv."""
    fda_stats, here = _load_fda_statistics()
    nma_stats = _load_nma_statistics(here)
    if output_dir is None:
        output_dir = os.path.join(here, "outputs", "analysis_8_1", cohort)
    # Clear this cohort's dir first so it reflects only the current run (no stale files from
    # renamed/removed outputs). Only the per-cohort dir is wiped — the sibling `supplement/`
    # dir (exploratory weighting-sensitivity artifacts) and the parent-level combined table
    # are left untouched.
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if csv_path is None and spark is None:
        csv_path = _default_analysis_ready_csv(here, analysis_ready_table)
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

    # Method A paired contrasts.
    contrasts = contrasts_table(pdf, fda_stats)
    print(contrasts.to_string(index=False))
    contrasts.to_csv(os.path.join(output_dir, "method_a_contrasts.csv"), index=False)

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

    # Figures: Method A panels (extras) + spec figures 8.1a/8.1b/8.1c.
    figures = {
        "method_a_panel_a.png": make_panel_a(pdf),
        "figure_8_1a_stacked_bars.png": make_stacked_bar(pdf),
        "figure_8_1b_tir_violin_box.png": make_figure_8_1b(pdf),
        "figure_8_1c_tbr_violin_box.png": make_figure_8_1c(pdf),
    }
    for group_label, group_endpoints in ENDPOINT_GROUPS:
        figures[f"method_a_panel_b_{group_label}.png"] = make_panel_b(pdf, group_label, group_endpoints)
    for fname, fig in figures.items():
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)

    print(f"wrote analysis 8.1 ({cohort}) outputs to {output_dir}")
    return contrasts


def main(
    spark=None,
    analysis_ready_table="dev.fda_510k_rwd.nma_user_day_analysis_ready",
    min_age=MIN_AGE,
    csv_path=None,
):
    """Orchestrate the §7.6 cohort split: adult and pediatric reported separately, plus a
    pooled `all` sanity run. Each lands in its own outputs/analysis_8_1/<cohort>/ dir. The
    §6 min-age floor (MIN_AGE=6) is applied to every cohort. Pass `csv_path` to run locally
    off the analysis-ready CSV snapshot (no Spark)."""
    for cohort in ("adult", "pediatric", "all"):
        run(spark, analysis_ready_table, output_dir=None, cohort=cohort, min_age=min_age,
            csv_path=csv_path)
    # Combined Sample Information (Table 1) across the three cohorts.
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
    _args, _ = _parser.parse_known_args()

    # Databricks injects a `spark` global; a local run has none. Resolve it safely so
    # running this file locally never trips over the undefined name — run() then falls back
    # to the analysis-ready CSV snapshot.
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        _spark = None

    if _args.cohort is None:
        main(_spark, _args.analysis_ready_table, min_age=_args.min_age, csv_path=_args.csv_path)
    else:
        run(_spark, _args.analysis_ready_table, _args.output_dir,
            cohort=_args.cohort, min_age=_args.min_age, csv_path=_args.csv_path)
