"""
Tests for `data_staging/export_user_day_tdd.py`.

Phase B test cases (planned):
    - Known basal rate over 24h → exact tdd_basal_u
    - Three normal boluses summing to known U → exact tdd_bolus_u
    - Autoboluses included in tdd_bolus_u
    - tdd_u = tdd_basal_u + tdd_bolus_u

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_basal_delivered_24h():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_bolus_sum():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_autoboluses_included_in_tdd_bolus():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_tdd_u_equals_basal_plus_bolus():
    raise NotImplementedError("Phase B")
