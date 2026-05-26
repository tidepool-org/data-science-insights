"""
Tests for `analysis/utils/statistics.py` — Unit 4.

Pure-Python tests. LMM tests use `pytest.importorskip("statsmodels")` so
they skip cleanly until statsmodels is installed in the env.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis", "utils")))

from statistics import (  # type: ignore # noqa: E402
    cluster_bootstrap_ci,
    lmm_arm_contrast,
    lmm_day_strategy_interaction,
    lmm_tdd_stratum,
    paired_within_user,
)


# ---------------------------------------------------------------------------
# 4.1 cluster_bootstrap_ci
# ---------------------------------------------------------------------------

def test_cluster_bootstrap_ci_reproducible():
    """Same seed → identical CI across two calls."""
    values = pd.Series([-5.0, -3.0, -2.0, 0.0, 1.0, 2.0, 4.0, 5.0, 6.0, 7.0])
    ci1 = cluster_bootstrap_ci(values, n_boot=500, seed=20260520)
    ci2 = cluster_bootstrap_ci(values, n_boot=500, seed=20260520)
    assert ci1 == ci2


def test_cluster_bootstrap_ci_different_seeds_differ():
    values = pd.Series([-5.0, -3.0, -2.0, 0.0, 1.0, 2.0, 4.0, 5.0, 6.0, 7.0])
    ci1 = cluster_bootstrap_ci(values, n_boot=500, seed=1)
    ci2 = cluster_bootstrap_ci(values, n_boot=500, seed=2)
    assert ci1 != ci2


def test_cluster_bootstrap_ci_returns_lo_lt_hi():
    values = pd.Series(np.linspace(-10.0, 10.0, 25))
    lo, hi = cluster_bootstrap_ci(values, n_boot=500, seed=20260520)
    assert lo < hi


def test_cluster_bootstrap_ci_brackets_true_median():
    """Symmetric distribution centered at 0 → CI brackets 0."""
    rng = np.random.default_rng(42)
    values = pd.Series(rng.normal(loc=0.0, scale=1.0, size=200))
    lo, hi = cluster_bootstrap_ci(values, n_boot=1000, seed=20260520)
    assert lo < 0.0 < hi


def test_cluster_bootstrap_ci_drops_na():
    values = pd.Series([1.0, 2.0, np.nan, 3.0, 4.0])
    lo, hi = cluster_bootstrap_ci(values, n_boot=200, seed=20260520)
    assert not np.isnan(lo)
    assert not np.isnan(hi)


# ---------------------------------------------------------------------------
# 4.2 paired_within_user
# ---------------------------------------------------------------------------

def test_paired_within_user_returns_expected_keys():
    arm_a = pd.Series([10.0, 12.0, 8.0, 11.0, 9.0, 13.0])
    arm_b = pd.Series([5.0, 6.0, 4.0, 5.5, 4.5, 6.5])
    result = paired_within_user(arm_a, arm_b)
    expected_keys = {
        "mean_diff", "mean_diff_ci_lo", "mean_diff_ci_hi",
        "median_diff", "median_diff_ci_lo", "median_diff_ci_hi",
        "t_stat", "t_p",
        "wilcoxon_stat", "wilcoxon_p",
        "shapiro_p", "n_pairs",
    }
    assert expected_keys <= set(result.keys())


def test_paired_within_user_diff_is_a_minus_b():
    """diff = arm_a - arm_b (per PLN-1008 §8.1 sign convention)."""
    arm_a = pd.Series([10.0] * 5)
    arm_b = pd.Series([4.0] * 5)
    result = paired_within_user(arm_a, arm_b)
    assert result["mean_diff"] == pytest.approx(6.0)
    assert result["median_diff"] == pytest.approx(6.0)


def test_paired_within_user_n_pairs_excludes_na():
    arm_a = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0])
    arm_b = pd.Series([0.0, 1.0, 2.0, np.nan, 4.0])
    result = paired_within_user(arm_a, arm_b)
    assert result["n_pairs"] == 3


def test_paired_within_user_significant_when_consistent_diff():
    arm_a = pd.Series([10.0, 11.0, 9.0, 12.0, 8.0, 11.0, 10.5, 9.5, 11.5, 10.0])
    arm_b = pd.Series([5.0, 6.0, 4.0, 7.0, 3.0, 6.0, 5.5, 4.5, 6.5, 5.0])
    result = paired_within_user(arm_a, arm_b)
    assert result["t_p"] < 0.001
    assert result["mean_diff_ci_lo"] > 0


# ---------------------------------------------------------------------------
# 4.3 lmm_arm_contrast
# ---------------------------------------------------------------------------

def _make_arm_dataset(n_users: int, n_days_per_user: int, true_arm_effect: float, seed: int = 0):
    """Build a 2-arm day-level dataset with a known fixed arm effect."""
    rng = np.random.default_rng(seed)
    rows = []
    for u in range(n_users):
        user_intercept = rng.normal(0, 2)
        for d in range(n_days_per_user):
            arm = "A" if d < n_days_per_user // 2 else "B"
            arm_effect = 0.0 if arm == "A" else true_arm_effect
            y = user_intercept + arm_effect + rng.normal(0, 1)
            rows.append({"_userId": f"u{u}", "day": d, "arm": arm, "y": y})
    return pd.DataFrame(rows)


def test_lmm_arm_contrast_recovers_truth():
    pytest.importorskip("statsmodels")
    df = _make_arm_dataset(n_users=20, n_days_per_user=40, true_arm_effect=-5.0, seed=20260520)
    result = lmm_arm_contrast(df, outcome="y", arm_col="arm")
    assert abs(result["coef"] - (-5.0)) < 1.0
    assert result["pvalue"] < 0.05
    assert result["n_users"] == 20
    assert result["n_days"] == 20 * 40
    assert result["ci_lo"] < result["coef"] < result["ci_hi"]


# ---------------------------------------------------------------------------
# 4.4 lmm_day_strategy_interaction
# ---------------------------------------------------------------------------

def _make_interaction_dataset(true_interaction: float, seed: int = 0):
    """2 (day_type) × 2 (strategy) cell design with a known interaction.

    References (first categorical level): day_type=CE>0, strategy=TB, so the
    interaction term recovered by statsmodels is `day_type[T.NMA]:strategy[T.AB]`
    with coefficient = cell(NMA,AB) − cell(NMA,TB) − cell(CE>0,AB) + cell(CE>0,TB).
    Cell means are chosen so that coefficient equals `true_interaction`.
    """
    rng = np.random.default_rng(seed)
    rows = []
    day_main = 2.0       # NMA − CE>0 at strategy=TB
    strat_main = 1.0     # AB − TB at day_type=CE>0
    cells = {
        ("NMA",  "AB"): day_main + strat_main + true_interaction,
        ("NMA",  "TB"): day_main,
        ("CE>0", "AB"): strat_main,
        ("CE>0", "TB"): 0.0,
    }
    keys = list(cells.keys())
    for u in range(30):
        user_intercept = rng.normal(0, 1)
        for d in range(20):
            day_type, strategy = keys[d % 4]
            y = user_intercept + cells[(day_type, strategy)] + rng.normal(0, 0.5)
            rows.append({
                "_userId": f"u{u}",
                "day_type": day_type,
                "delivery_strategy": strategy,
                "y": y,
            })
    df = pd.DataFrame(rows)
    df["day_type"] = pd.Categorical(df["day_type"], categories=["CE>0", "NMA"])
    df["delivery_strategy"] = pd.Categorical(df["delivery_strategy"], categories=["TB", "AB"])
    return df


def test_lmm_day_strategy_interaction_recovers_truth():
    pytest.importorskip("statsmodels")
    df = _make_interaction_dataset(true_interaction=3.0, seed=20260520)
    result = lmm_day_strategy_interaction(df, outcome="y")
    assert abs(result["interaction_coef"] - 3.0) < 1.0
    assert "interaction_p" in result
    assert "marginal_cells" in result
    assert len(result["marginal_cells"]) == 4


def test_lmm_day_strategy_interaction_marginal_cells_returned():
    pytest.importorskip("statsmodels")
    df = _make_interaction_dataset(true_interaction=3.0, seed=20260520)
    result = lmm_day_strategy_interaction(df, outcome="y")
    cells = result["marginal_cells"]
    for key in [("NMA", "AB"), ("NMA", "TB"), ("CE>0", "AB"), ("CE>0", "TB")]:
        assert key in cells
        assert "mean" in cells[key]
        assert "n_days" in cells[key]


# ---------------------------------------------------------------------------
# 4.5 lmm_tdd_stratum
# ---------------------------------------------------------------------------

def _make_tdd_dataset(true_low_minus_high: float, seed: int = 0):
    rng = np.random.default_rng(seed)
    rows = []
    for u in range(30):
        user_intercept = rng.normal(0, 1)
        for d in range(20):
            stratum = "Low" if d < 10 else "High"
            offset = true_low_minus_high if stratum == "Low" else 0.0
            y = user_intercept + offset + rng.normal(0, 0.5)
            rows.append({"_userId": f"u{u}", "tdd_stratum": stratum, "y": y})
    return pd.DataFrame(rows)


def test_lmm_tdd_stratum_recovers_low_minus_high_contrast():
    pytest.importorskip("statsmodels")
    df = _make_tdd_dataset(true_low_minus_high=15.0, seed=20260520)
    result = lmm_tdd_stratum(df, outcome="y")
    assert abs(result["coef"] - 15.0) < 1.0
    assert result["pvalue"] < 0.05


def test_lmm_tdd_stratum_reference_is_high():
    """Sign convention: coefficient is Low − High, so positive coef ⇒ Low > High."""
    pytest.importorskip("statsmodels")
    df = _make_tdd_dataset(true_low_minus_high=10.0, seed=20260520)
    result = lmm_tdd_stratum(df, outcome="y")
    assert result["coef"] > 0
