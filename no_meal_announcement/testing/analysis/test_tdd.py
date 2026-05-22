"""
Tests for `analysis/utils/tdd.py`.

Pure-pandas tests.

Phase B test cases (planned):
    - 40-day user, mean TDD = 50 U → compute_personal_tdd('mean') returns 50
    - Median equals constructed median for a non-symmetric distribution
    - R = 30/50 = 0.6 → Low; R = 70/50 = 1.4 → High
    - Tercile labels: 9 CE=0 days at [10,…,90] → T1 first 3, T3 last 3
    - Rolling-30d ref for day 31 equals mean of days 1-30
    - is_tdd_pair_eligible: user with 28 days → False; 35 days with both strata → True

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_compute_personal_tdd_mean():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_compute_personal_tdd_median():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_stratify_low_high():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_stratify_terciles():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_rolling_30d_reference():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_tdd_pair_eligibility():
    raise NotImplementedError("Phase B")
