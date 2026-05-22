"""
Tests for `analysis/utils/statistics.py`.

Pure-Python tests.

Phase B test cases (planned):
    - cluster_bootstrap_ci deterministic with seed=20260520 on toy 10-user array
    - cluster_bootstrap_ci brackets the true median in a known distribution
    - paired_within_user reports identical t/Wilcoxon stats as FDA helper
    - lmm_arm_contrast: 20-user × 40-day synthetic, true arm effect = -5
      → coef within ±1, p < 0.05
    - lmm_day_strategy_interaction: 4-cell synthetic with designed
      interaction = +3 → interaction coef within ±1
    - lmm_tdd_stratum: known Low-High contrast = +15% → coef within ±1

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_cluster_bootstrap_ci_deterministic_seed():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_cluster_bootstrap_ci_brackets_truth():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_paired_within_user_matches_fda_helper():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_lmm_arm_contrast_recovers_truth():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_lmm_day_strategy_interaction_recovers_truth():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_lmm_tdd_stratum_recovers_truth():
    raise NotImplementedError("Phase B")
