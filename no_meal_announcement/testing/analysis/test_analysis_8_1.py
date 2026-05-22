"""
Tests for `analysis/analysis_8-1_nma_vs_ce_outcomes.py`.

Phase B test cases (planned, against a small synthetic master DataFrame):
    - Table 8.1a: per-user means averaged across users match hand-computed values
    - Table 8.1b: LMM coef sign and magnitude match design
    - Table 8.1c: behavioral metrics (mean meal boluses, manual boluses, carbs/day)
    - CE>0 comparator restriction: users with no CE=0 day are excluded from
      the CE>0 arm in all three classification columns
    - Pediatric and adult cohorts are reported separately

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_1a_per_user_means():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_1b_lmm_contrasts():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_1c_behavioral_summary():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ce_pos_comparator_restriction():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_pediatric_adult_split():
    raise NotImplementedError("Phase B")
