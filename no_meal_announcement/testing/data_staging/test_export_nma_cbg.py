"""
Tests for `data_staging/export_nma_cbg.py`.

Phase B test cases (planned):
    - 290 readings on a day → coverage_pct ≈ 1.0, kept
    - 150 readings on a day → coverage_pct ≈ 0.52, dropped (below 0.70)
    - Dedup CBG by latest created_timestamp per (userId, deviceTime)
    - Timezone shift applied correctly for non-UTC offsets

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_full_coverage_day_kept():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_low_coverage_day_dropped():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_cbg_dedup_by_latest_created():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_timezone_offset_applied():
    raise NotImplementedError("Phase B")
