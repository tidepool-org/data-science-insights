"""
Tests for `analysis/analysis_8-3_nma_tdd_stratified.py`.

Phase B test cases (planned):
    - Restricted to CE=0 days; CE>0 rows excluded from analysis input
    - Eligibility: user with <30 eligible days OR no day in either stratum is dropped
    - Table 8.3a per-user summaries by stratum equal hand-computed values
    - Table 8.3b: Median/Mean diff recovers the +15% design baked into the
      `nma_user_known_low_high_tdd` archetype
    - Table 8.3c LMM coef sign and magnitude match design
    - Tercile sensitivity reruns Table 8.3b with bottom/top tercile cutpoints
    - Median-reference sensitivity reruns Table 8.3b with median TDD
    - Rolling-30d sensitivity reruns Table 8.3b with rolling reference
    - Pediatric and adult cohorts are reported separately

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_restricted_to_ce0_days():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_eligibility_filters_applied():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_3a_per_user_summary():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_3b_paired_contrast_recovers_design():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_table_8_3c_lmm_sensitivity():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_sensitivity_terciles():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_sensitivity_median_reference():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_sensitivity_rolling_30d():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_pediatric_adult_split():
    raise NotImplementedError("Phase B")
