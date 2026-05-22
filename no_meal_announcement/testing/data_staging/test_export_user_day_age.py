"""
Tests for `data_staging/export_user_day_age.py`.

Phase B test cases (planned):
    - DOB=2010-01-01, day=2024-06-15 → age_years=14, is_pediatric=True
    - DOB=1990-01-01, day=2024-06-15 → age_years=34, is_pediatric=False
    - User crosses 18th birthday mid-window → pediatric on one side, adult on the other
    - Missing DOB → row excluded (or flagged) per cohort policy

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_pediatric_age_calculation():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_adult_age_calculation():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_pediatric_to_adult_transition():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_missing_dob_excluded():
    raise NotImplementedError("Phase B")
