"""
Tests for `data_staging/export_user_day_carb_grams.py`.

Phase B test cases (planned):
    - Two food records on same day → sum
    - Same record re-ingested twice → keep latest by created_timestamp
    - User-local day boundary respected via timezoneOffset
    - Day with no food records → row absent or carb_grams=0

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_sum_food_records_same_day():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_dedup_by_latest_created_timestamp():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_user_local_day_boundary():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_day_without_food_yields_zero():
    raise NotImplementedError("Phase B")
