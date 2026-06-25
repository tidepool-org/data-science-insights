"""
Unit tests for `analysis/utils/data_loader.py` — the shared cohort / comparator /
day-level helpers. Pure pandas (no Spark, no statsmodels), so they run in the
local non-Spark layer.

Each helper is fed a small designed DataFrame with a hand-computed expectation.
"""

import os
import sys

import numpy as np
import pandas as pd

# data_loader has no relative imports, but strata (and the analysis modules) import it as
# `utils.data_loader`, so add analysis/ to the path and import the same way for consistency.
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis")))

from utils.data_loader import (  # type: ignore # noqa: E402
    filter_cohort,
    prepare_day_level,
    restrict_comparator,
    windowed_matched_means,
)


# ---------------------------------------------------------------------------
# restrict_comparator — zero CE>0 / HMA for users with no CE=0 day
# ---------------------------------------------------------------------------

def test_restrict_comparator_zeros_non_ce0_users():
    df = pd.DataFrame({
        "_userId":          ["u1", "u1", "u2", "u2"],
        "in_ce0_be_inf":    [True, False, False, False],
        "in_ce_gt0":        [False, True, True, True],
        "in_ce_ge3_be_ge3": [False, False, True, True],
    })
    out = restrict_comparator(df)
    u1 = out[out["_userId"] == "u1"]
    u2 = out[out["_userId"] == "u2"]
    # u1 contributed a CE=0 day → its comparator flags are untouched.
    assert u1["in_ce_gt0"].tolist() == [False, True]
    # u2 has no CE=0 day → its CE>0 AND HMA flags are zeroed.
    assert (u2["in_ce_gt0"] == False).all()
    assert (u2["in_ce_ge3_be_ge3"] == False).all()


def test_restrict_comparator_no_hma_column_is_ok():
    """The HMA flag is optional (older snapshots) — restrict must not require it."""
    df = pd.DataFrame({
        "_userId":       ["u1", "u2"],
        "in_ce0_be_inf": [True, False],
        "in_ce_gt0":     [True, True],
    })
    out = restrict_comparator(df)
    assert out.loc[out["_userId"] == "u2", "in_ce_gt0"].tolist() == [False]


# ---------------------------------------------------------------------------
# filter_cohort — §7.6 age cohort + §6 min-age floor
# ---------------------------------------------------------------------------

def _cohort_frame():
    return pd.DataFrame({
        "_userId":        ["a", "p", "u", "y"],
        "is_pediatric":   [False, True, np.nan, False],
        "age_years":      [40.0, 12.0, np.nan, 4.0],   # 'y' is a known <6 user
        "diagnosis_type": ["type1"] * 4,               # all type1 → the §7 gate is a no-op here
    })


def test_filter_cohort_adult_drops_known_young_and_excludes_unknown():
    out = filter_cohort(_cohort_frame(), cohort="adult")
    assert out["_userId"].tolist() == ["a"]


def test_filter_cohort_pediatric():
    out = filter_cohort(_cohort_frame(), cohort="pediatric")
    assert out["_userId"].tolist() == ["p"]


def test_filter_cohort_all_retains_unknown_age_drops_known_young():
    out = filter_cohort(_cohort_frame(), cohort="all")
    assert set(out["_userId"]) == {"a", "p", "u"}


def test_filter_cohort_min_age_none_disables_floor():
    out = filter_cohort(_cohort_frame(), cohort="all", min_age=None)
    assert set(out["_userId"]) == {"a", "p", "u", "y"}


# ---------------------------------------------------------------------------
# filter_cohort — §7 type-1 diagnosis gate (strict == 'type1', default-on)
# ---------------------------------------------------------------------------

def _diagnosis_frame():
    """All adult + age-eligible, so only the type-1 gate decides who survives."""
    return pd.DataFrame({
        "_userId":        ["t1", "t2", "oth", "nul"],
        "is_pediatric":   [False, False, False, False],
        "age_years":      [40.0, 40.0, 40.0, 40.0],
        "diagnosis_type": ["type1", "type2", "other", np.nan],
    })


def test_filter_cohort_type1_gate_keeps_only_type1():
    # Default require_type1=True: type2 / other / NULL all drop (FDA-matching strict).
    out = filter_cohort(_diagnosis_frame(), cohort="all")
    assert out["_userId"].tolist() == ["t1"]


def test_filter_cohort_type1_gate_off_retains_all_diagnoses():
    out = filter_cohort(_diagnosis_frame(), cohort="all", require_type1=False)
    assert set(out["_userId"]) == {"t1", "t2", "oth", "nul"}


def test_filter_cohort_require_type1_raises_without_column():
    """Default-on gate must FAIL LOUD on a pre-merge snapshot — never silently pass an
    ungated cohort when `diagnosis_type` is absent."""
    df = pd.DataFrame({
        "_userId":      ["a"],
        "is_pediatric": [False],
        "age_years":    [40.0],
    })
    raised = False
    try:
        filter_cohort(df, cohort="all")   # require_type1 defaults True; no diagnosis_type column
    except KeyError:
        raised = True
    assert raised


# ---------------------------------------------------------------------------
# prepare_day_level — numeric coercion + eligibility filter
# ---------------------------------------------------------------------------

def test_prepare_day_level_keeps_only_eligible_and_coerces_numeric():
    df = pd.DataFrame({
        "_userId":       ["a", "b", "c"],
        "day_eligible":  [True, True, False],
        "user_eligible": [True, False, True],
        "tir":           ["80.0", "70.0", "60.0"],   # Spark returns Decimal/str → coerced
    })
    out = prepare_day_level(df)
    assert out["_userId"].tolist() == ["a"]            # only the both-eligible row
    assert out["tir"].dtype.kind == "f"
    assert out["tir"].iloc[0] == 80.0


# ---------------------------------------------------------------------------
# windowed_matched_means — ±45-day per-NMA-day comparator match (§8.1 / §12.1)
# ---------------------------------------------------------------------------

def test_windowed_matched_means_uses_only_in_window_comparator_days():
    df = pd.DataFrame({
        "_userId":       ["u1", "u1", "u1"],
        "local_day":     ["2024-02-01", "2024-02-10", "2024-06-01"],
        "in_ce0_be_inf": [True, False, False],
        "in_ce_gt0":     [False, True, True],
        "tir":           [80.0, 60.0, 20.0],
    })
    per_user, cov = windowed_matched_means(df, "in_ce0_be_inf", "in_ce_gt0", ["tir"], half=45)
    assert len(per_user) == 1
    row = per_user.iloc[0]
    assert row["tir__nma"] == 80.0     # the NMA day's own value
    assert row["tir__cmp"] == 60.0     # only the 02-10 comparator (06-01 is >45 days away)
    assert cov["total_nma_days"] == 1
    assert cov["matched_nma_days"] == 1


def test_windowed_matched_means_empty_arm_keeps_columns():
    """The 2026-06-07 robustness guard: an arm with no qualifying days must still return a frame
    carrying {ep}__nma / {ep}__cmp columns (empty), not a column-less frame that KeyErrors."""
    df = pd.DataFrame({
        "_userId":          ["u1", "u1"],
        "local_day":        ["2024-02-01", "2024-02-10"],
        "in_ce_ge3_be_ge3": [False, False],   # no HMA days in this fixture
        "in_ce_gt0":        [False, True],
        "tir":              [80.0, 60.0],
    })
    per_user, cov = windowed_matched_means(df, "in_ce_ge3_be_ge3", "in_ce_gt0", ["tir"])
    assert per_user.empty
    assert "tir__nma" in per_user.columns
    assert "tir__cmp" in per_user.columns
    assert cov["n_users_matched"] == 0
