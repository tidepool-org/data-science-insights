"""Shared data-loading + cohort/comparator helpers for the PLN-1008 NMA analyses.

Extracted from analysis_8-1 so §8.1, §8.2 (and future §8.3) share one definition of the
analysis-ready snapshot loader, the §7.6 age-cohort filter, the §8.1 CE>0 comparator
restriction, the endpoint / classification constants, and the by-path loaders for the FDA +
local statistics modules.

`statsmodels` / `scipy` are NOT imported here; the statistics modules are loaded lazily by
file path (under unique module names — our analysis/ has its own `utils` package, so a bare
`import statistics` would collide) so this module imports cleanly anywhere.
"""

import importlib.util
import os
from typing import Literal

import numpy as np
import pandas as pd

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

# Arms shown in figures (3 nested NMA arms + comparator).
FIGURE_ARMS = CLASSIFICATIONS + [(COMPARATOR_FLAG, COMPARATOR_LABEL)]

# High meal-announcement ("high engagement") arm: >=3 carb entries AND >=3 manual bolus entries —
# the heavy-announcement end of the spectrum, shown as a 5th category beside the 3 NMA arms + CE>0
# (Table 8.1a column + figures 8.1a/8.1b) and in parallel supplement contrast tables. Staged as
# `in_ce_ge3_be_ge3` in export_user_day_classification.py (carried through analysis-ready).
HIGH_MA_FLAG = "in_ce_ge3_be_ge3"
HIGH_MA_LABEL = "CE>=3/BE>=3"
# The 5-category arm set for the supplement / violins (3 NMA + CE>0 + high meal-announcement).
SUPPLEMENT_ARMS = FIGURE_ARMS + [(HIGH_MA_FLAG, HIGH_MA_LABEL)]

# §7.3 delivery strategies: (column value, short label), in DISPLAY order (TB before AB, per MJC) —
# this drives the §8.2 figure cell/line/x-axis order and the table row order. The LMM reference is
# AB (autobolus_on), set alphabetically by statsmodels regardless of this list order, so coefficients
# are unaffected by the ordering. Any other / null strategy is "ambiguous" and excluded (the staging
# CASE currently emits only these two values). Shared by §8.2 + §8.3 (cross-tab) — single source.
STRATEGIES = [("temp_basal_only", "TB"), ("autobolus_on", "AB")]
STRATEGY_COL = "delivery_strategy"

# §6 / PLN-1001 minimum age (years), applied in analysis via filter_cohort. Users KNOWN to
# be younger are dropped; unknown/nulled-age users are retained (PLN-1001
# `is_age_eligible OR dob IS NULL`). Implausible-high ages are already nulled at extraction
# (export_user_day_age.MAX_PLAUSIBLE_AGE), so this floor + that bound together gate age.
MIN_AGE = 6

# Rolling temporal-match window for the §8.1 windowed-comparator sensitivity: a CE=0 (NMA) day is
# paired only against CE>0 comparator days within ± WINDOW_HALF calendar days of it (same user), to
# control within-user temporal drift (Loop-version era, seasonality). See windowed_matched_means.
WINDOW_DAYS = 90
WINDOW_HALF = 45

# Columns coerced to float: endpoints (Spark `* 100.0` returns Decimal/object) plus the
# behavioral / age columns used downstream (Table 8.1c, cohort split).
NUMERIC_COLS = [col for col, _ in ENDPOINTS] + [
    "carb_entry_count", "bolus_entry_count", "carb_grams_total", "tdd_units", "age_years",
]

# Databricks-notebook fallback for the analysis/ dir (where __file__ is undefined).
_DATABRICKS_ANALYSIS_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
    "no_meal_announcement/analysis"
)


def analysis_dir():
    """The no_meal_announcement/analysis/ directory. This file lives in analysis/utils/, so
    the analysis dir is two levels up; falls back to the Databricks path where __file__ is
    undefined (mirrors the path handling in the data_staging scripts)."""
    try:
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        return _DATABRICKS_ANALYSIS_DIR


def default_analysis_ready_csv(analysis_ready_table):
    """Where data_staging/export_user_day_analysis_ready.py writes its CSV snapshot —
    no_meal_announcement/outputs/<table-name>.csv."""
    fname = analysis_ready_table.split(".")[-1] + ".csv"
    return os.path.normpath(os.path.join(analysis_dir(), "..", "outputs", fname))


def load_fda_statistics():
    """Load FDA analysis/utils/statistics.py by path as `fda_statistics` (avoids the
    `utils` package-name collision with our own analysis/utils)."""
    path = os.path.normpath(os.path.join(
        analysis_dir(), "..", "..", "FDA_real_world_data", "analysis", "utils", "statistics.py"))
    spec = importlib.util.spec_from_file_location("fda_statistics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_nma_statistics():
    """Load the local analysis/utils/statistics.py (LMM helpers + cluster_bootstrap_ci) by
    path as `nma_statistics`, so it works both as a script (Databricks) and on import."""
    path = os.path.join(analysis_dir(), "utils", "statistics.py")
    spec = importlib.util.spec_from_file_location("nma_statistics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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
    """§8.1 Table 8.1a: the comparison arms — CE>0 and the CE>=3/BE>=3 high meal-announcement
    supplement arm — are restricted to users who contributed >=1 CE=0 day in at least one of the
    three NMA classifications. The arms are nested under in_ce0_be_inf (CE=0), so that reduces to
    ">=1 in_ce0_be_inf day". Zero in_ce_gt0 (and in_ce_ge3_be_ge3) for users with no CE=0 day so
    every table/figure compares the same CE=0-contributing cohort."""
    ce0_users = set(pdf.loc[pdf["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    out = pdf.copy()
    non_ce0 = ~out["_userId"].isin(ce0_users)
    out.loc[non_ce0, "in_ce_gt0"] = False
    if HIGH_MA_FLAG in out.columns:
        out.loc[non_ce0, HIGH_MA_FLAG] = False
    return out


def windowed_matched_means(pdf, nma_flag, cmp_flag, endpoints, half=WINDOW_HALF):
    """Per-NMA-day ±`half`-day matched comparator inputs (the §8.1 windowed-comparator sensitivity).

    For the §8.1 NMA-vs-CE>0 contrast, anchor on each NMA (`nma_flag`) day; its comparator value is
    the MEAN of `cmp_flag` comparator days (CE>0 = COMPARATOR_FLAG) within ±half CALENDAR days of
    THAT day (same user). Only NMA days with >=1 in-window comparator are kept; a comparator day
    near several NMA days contributes to each of their local means. Matching contemporaneous days
    removes within-user temporal drift (Loop-version era, seasonality) from the contrast.

    Returns (per_user, coverage):
      per_user — one row per user with >=1 matched NMA day; columns `{ep}__nma` (mean over the
                 user's matched NMA days of the NMA-day value) and `{ep}__cmp` (mean over those
                 days of the local comparator mean) for each ep in `endpoints`. The per-user
                 windowed contrast for `ep` is `{ep}__nma - {ep}__cmp` (equal-user weight, D5).
      coverage — dict(total_nma_days, matched_nma_days, n_users_matched).

    Requires `nma_flag` + `cmp_flag` boolean columns and `local_day` (YYYY-MM-DD). Implementation:
    per user, encode days as calendar-day ordinals and use cumulative-sum + searchsorted so each
    NMA day's in-window comparator mean is O(log n)."""
    ep_cols = [e[0] if isinstance(e, (tuple, list)) else e for e in endpoints]
    ordv_all = (pd.to_datetime(pdf["local_day"]) - pd.Timestamp("2000-01-01")).dt.days
    rows = []
    total_nma = matched_nma = 0
    for _, sub in pdf.groupby("_userId"):
        nm = (sub[nma_flag] == True).to_numpy()   # noqa: E712
        cm = (sub[cmp_flag] == True).to_numpy()    # noqa: E712
        n_nma = int(nm.sum())
        if n_nma == 0:
            continue
        total_nma += n_nma
        if cm.sum() == 0:
            continue
        ordv = ordv_all.loc[sub.index].to_numpy()
        n_pos = np.where(nm)[0]
        c_pos = np.where(cm)[0]
        n_ord = ordv[n_pos]
        c_ord = ordv[c_pos]
        order = np.argsort(c_ord)
        c_ord_s = c_ord[order]
        lo = np.searchsorted(c_ord_s, n_ord - half, side="left")
        hi = np.searchsorted(c_ord_s, n_ord + half, side="right")   # inclusive ±half (91-day window)
        cnt = hi - lo
        keep = cnt > 0
        matched_nma += int(keep.sum())
        if not keep.any():
            continue
        rec = {"_userId": sub["_userId"].iloc[0]}
        for ep in ep_cols:
            cv = sub[ep].to_numpy()[c_pos][order].astype(float)
            prefix = np.concatenate([[0.0], np.cumsum(cv)])         # prefix sums over sorted comparator days
            local = (prefix[hi] - prefix[lo]) / np.where(cnt > 0, cnt, 1)   # local comparator mean per NMA day
            nv = sub[ep].to_numpy()[n_pos].astype(float)
            rec[f"{ep}__nma"] = np.nanmean(nv[keep])
            rec[f"{ep}__cmp"] = np.nanmean(local[keep])
        rows.append(rec)
    per_user = pd.DataFrame(rows)
    if per_user.empty:
        # No user had a matched NMA day (e.g. an arm with no qualifying days — like the HMA arm in a
        # cohort/fixture with no CE>=3/BE>=3 days). Return an EMPTY frame that still carries the
        # {ep}__nma / {ep}__cmp columns, so callers get empty Series rather than a KeyError on a
        # column-less frame (the guarded callers check len()>0; the figure builders index by column).
        per_user = pd.DataFrame(columns=(["_userId"]
                                         + [f"{ep}__nma" for ep in ep_cols]
                                         + [f"{ep}__cmp" for ep in ep_cols]))
    coverage = {"total_nma_days": total_nma, "matched_nma_days": matched_nma,
                "n_users_matched": len(per_user)}
    return per_user, coverage
