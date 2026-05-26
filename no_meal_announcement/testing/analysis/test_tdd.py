"""
Tests for `analysis/utils/tdd.py` — Unit 3.

Pure-pandas tests.

Rolling-30d convention: for day N, reference = mean of days [N-30, N-1]
(trailing window, exclusive of current day). Days 1-30 have no history
and return NaN.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis", "utils")))

from tdd import (  # type: ignore # noqa: E402
    compute_personal_tdd,
    compute_r_user_day,
    is_tdd_pair_eligible,
    stratify_low_high,
    stratify_terciles,
)


def _make_per_day(user_id: str, tdd_values: list[float]) -> pd.DataFrame:
    """Helper: build a per-day TDD frame for one user over N days."""
    return pd.DataFrame({
        "_userId": [user_id] * len(tdd_values),
        "day": pd.date_range("2024-01-01", periods=len(tdd_values), freq="D"),
        "tdd_u": tdd_values,
    })


# ---------------------------------------------------------------------------
# 3.1-3.3 compute_personal_tdd
# ---------------------------------------------------------------------------

def test_compute_personal_tdd_mean():
    """40-day user with mean TDD = 50 → method='mean' returns 50."""
    df = _make_per_day("u1", [50.0] * 20 + [30.0] * 10 + [70.0] * 10)
    result = compute_personal_tdd(df, method="mean")
    assert result.loc["u1"] == pytest.approx(50.0)


def test_compute_personal_tdd_mean_multi_user():
    df = pd.concat([
        _make_per_day("u1", [50.0] * 10),
        _make_per_day("u2", [80.0] * 10),
    ], ignore_index=True)
    result = compute_personal_tdd(df, method="mean")
    assert result.loc["u1"] == pytest.approx(50.0)
    assert result.loc["u2"] == pytest.approx(80.0)


def test_compute_personal_tdd_median():
    """Non-symmetric distribution → median differs from mean."""
    df = _make_per_day("u1", [10.0, 20.0, 30.0, 40.0, 200.0])
    result = compute_personal_tdd(df, method="median")
    assert result.loc["u1"] == pytest.approx(30.0)


def test_compute_personal_tdd_rolling_30d_day31_equals_mean_of_first_30():
    """Day 31 rolling-30d reference = mean of days 1-30."""
    values = list(range(1, 41))  # 1..40
    df = _make_per_day("u1", [float(v) for v in values])
    result = compute_personal_tdd(df, method="rolling_30d")
    expected_day31 = float(np.mean(values[:30]))  # mean of days 1-30
    assert result.iloc[30] == pytest.approx(expected_day31)


def test_compute_personal_tdd_rolling_30d_early_days_nan():
    """First 30 days (insufficient trailing history) return NaN."""
    df = _make_per_day("u1", [50.0] * 40)
    result = compute_personal_tdd(df, method="rolling_30d")
    assert result.iloc[:30].isna().all()
    assert not result.iloc[30:].isna().any()


def test_compute_personal_tdd_rolling_30d_indexed_like_input():
    df = _make_per_day("u1", [50.0] * 40)
    result = compute_personal_tdd(df, method="rolling_30d")
    assert len(result) == len(df)


# ---------------------------------------------------------------------------
# 3.4 compute_r_user_day
# ---------------------------------------------------------------------------

def test_compute_r_user_day_per_user_reference():
    """Per-user reference: TDD_day / mean_tdd_user."""
    df = _make_per_day("u1", [30.0, 50.0, 70.0])
    personal = pd.Series({"u1": 50.0}, name="mean_tdd_user")
    r = compute_r_user_day(df, personal)
    assert r.iloc[0] == pytest.approx(0.6)
    assert r.iloc[1] == pytest.approx(1.0)
    assert r.iloc[2] == pytest.approx(1.4)


def test_compute_r_user_day_per_row_reference():
    """Per-row reference (rolling) returns per-row R values."""
    df = _make_per_day("u1", [30.0, 50.0, 70.0])
    personal = pd.Series([50.0, 50.0, 50.0])
    r = compute_r_user_day(df, personal)
    assert r.iloc[0] == pytest.approx(0.6)
    assert r.iloc[2] == pytest.approx(1.4)


# ---------------------------------------------------------------------------
# 3.5 stratify_low_high
# ---------------------------------------------------------------------------

def test_stratify_low_high_below_threshold_is_low():
    r = pd.Series([0.6, 0.9, 0.99])
    out = stratify_low_high(r, threshold=1.0)
    assert (out == "Low").all()


def test_stratify_low_high_at_or_above_threshold_is_high():
    """R == 1.0 boundary is High (R >= threshold)."""
    r = pd.Series([1.0, 1.4, 2.0])
    out = stratify_low_high(r, threshold=1.0)
    assert (out == "High").all()


def test_stratify_low_high_mixed():
    r = pd.Series([0.5, 1.0, 1.5])
    out = stratify_low_high(r, threshold=1.0)
    assert out.tolist() == ["Low", "High", "High"]


# ---------------------------------------------------------------------------
# 3.6 stratify_terciles
# ---------------------------------------------------------------------------

def test_stratify_terciles_nine_values():
    """9 ascending values → first 3 = T1, middle 3 = T2, last 3 = T3."""
    df = pd.DataFrame({
        "_userId": ["u1"] * 9,
        "R_user_day": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0],
    })
    labels = stratify_terciles(df)
    assert labels.tolist() == ["T1", "T1", "T1", "T2", "T2", "T2", "T3", "T3", "T3"]


def test_stratify_terciles_per_user_independent():
    """Each user's terciles are computed independently."""
    df = pd.DataFrame({
        "_userId": ["u1"] * 3 + ["u2"] * 3,
        "R_user_day": [10.0, 50.0, 90.0, 0.1, 0.5, 0.9],
    })
    labels = stratify_terciles(df)
    assert labels.tolist() == ["T1", "T2", "T3", "T1", "T2", "T3"]


# ---------------------------------------------------------------------------
# 3.7 is_tdd_pair_eligible
# ---------------------------------------------------------------------------

def test_is_tdd_pair_eligible_below_min_days_false():
    """28-day user → False (need ≥30 days)."""
    df = pd.DataFrame({
        "_userId": ["u1"] * 28,
        "is_ce0_beinf": [True] * 14 + [False] * 14,
        "tdd_stratum": ["Low"] * 14 + ["High"] * 14,
    })
    eligible = is_tdd_pair_eligible(df, min_days=30)
    assert eligible.loc["u1"] is np.bool_(False) or eligible.loc["u1"] == False  # noqa: E712


def test_is_tdd_pair_eligible_both_strata_present_true():
    """35 days, ≥1 CE=0 day in each TDD stratum → True."""
    df = pd.DataFrame({
        "_userId": ["u1"] * 35,
        "is_ce0_beinf": [True] * 20 + [False] * 15,
        "tdd_stratum": ["Low"] * 10 + ["High"] * 10 + ["Low"] * 8 + ["High"] * 7,
    })
    eligible = is_tdd_pair_eligible(df, min_days=30)
    assert bool(eligible.loc["u1"]) is True


def test_is_tdd_pair_eligible_only_one_stratum_false():
    """35 days but all CE=0 days are Low → False."""
    df = pd.DataFrame({
        "_userId": ["u1"] * 35,
        "is_ce0_beinf": [True] * 20 + [False] * 15,
        "tdd_stratum": ["Low"] * 20 + ["High"] * 15,
    })
    eligible = is_tdd_pair_eligible(df, min_days=30)
    assert bool(eligible.loc["u1"]) is False


def test_is_tdd_pair_eligible_multi_user():
    df = pd.concat([
        # u1: 35 days, both strata in CE=0 → eligible
        pd.DataFrame({
            "_userId": ["u1"] * 35,
            "is_ce0_beinf": [True] * 20 + [False] * 15,
            "tdd_stratum": ["Low"] * 10 + ["High"] * 10 + ["Low"] * 8 + ["High"] * 7,
        }),
        # u2: 25 days → too few days
        pd.DataFrame({
            "_userId": ["u2"] * 25,
            "is_ce0_beinf": [True] * 15 + [False] * 10,
            "tdd_stratum": ["Low"] * 5 + ["High"] * 10 + ["Low"] * 5 + ["High"] * 5,
        }),
    ], ignore_index=True)
    eligible = is_tdd_pair_eligible(df, min_days=30)
    assert bool(eligible.loc["u1"]) is True
    assert bool(eligible.loc["u2"]) is False
