"""
Tests for `analysis/utils/classification.py`.

Pure-pandas tests; no SparkSession required.

Phase B test cases (planned):
    - classify_day (0, 0, 0)  → BE=0 ✓, BE≤1 ✓, BE≤∞ ✓, CE>0 ✗
    - classify_day (0, 0, 1)  → BE=0 ✗, BE≤1 ✓, BE≤∞ ✓, CE>0 ✗
    - classify_day (0, 0, 5)  → BE=0 ✗, BE≤1 ✗, BE≤∞ ✓, CE>0 ✗
    - classify_day (20, 1, 0) → all NMA ✗, CE>0 ✓
    - day_type_label resolves correctly to the strictest matching arm
    - Edge case: carb=0 but meal_bolus_count=1 → resolution per docs/

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_classify_day_pure_be0():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_classify_day_bele1_only():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_classify_day_beinf_only():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_classify_day_ce_pos():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_day_type_label_strictest_resolution():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_classify_day_carb_meal_disagreement_edge_case():
    raise NotImplementedError("Phase B")
