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

# §6 / PLN-1001 minimum age (years), applied in analysis via filter_cohort. Users KNOWN to
# be younger are dropped; unknown/nulled-age users are retained (PLN-1001
# `is_age_eligible OR dob IS NULL`). Implausible-high ages are already nulled at extraction
# (export_user_day_age.MAX_PLAUSIBLE_AGE), so this floor + that bound together gate age.
MIN_AGE = 6

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
    """§8.1 Table 8.1a: the CE>0 arm is restricted to users who contributed >=1 CE=0 day in
    at least one of the three NMA classifications. The arms are nested under in_ce0_be_inf
    (CE=0), so that reduces to ">=1 in_ce0_be_inf day". Zero in_ce_gt0 for users with no
    CE=0 day so every table/figure uses the restricted comparator."""
    ce0_users = set(pdf.loc[pdf["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    out = pdf.copy()
    out.loc[~out["_userId"].isin(ce0_users), "in_ce_gt0"] = False
    return out
