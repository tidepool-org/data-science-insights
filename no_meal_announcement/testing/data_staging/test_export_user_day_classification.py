"""
Tests for `data_staging/export_user_day_classification.py`.

Phase B test cases (planned):
    - (carb=0, meal=0, non_meal=0) → is_ce0_be0 ✓ / bele1 ✓ / beinf ✓ / ce_pos ✗
    - (carb=0, meal=0, non_meal=1) → be0 ✗ / bele1 ✓ / beinf ✓ / ce_pos ✗
    - (carb=0, meal=0, non_meal=5) → be0 ✗ / bele1 ✗ / beinf ✓ / ce_pos ✗
    - (carb=20, meal=1, non_meal=0) → all NMA flags ✗ / ce_pos ✓
    - day_type_strictest column resolves correctly to the most stringent label

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ce0_be0_classification():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ce0_bele1_classification():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ce0_beinf_classification():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ce_pos_classification():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_strictest_label_resolution():
    raise NotImplementedError("Phase B")
