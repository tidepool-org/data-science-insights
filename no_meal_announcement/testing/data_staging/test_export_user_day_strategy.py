"""
Tests for `data_staging/export_user_day_strategy.py`.

Phase B test cases (planned):
    - dd_autobolus_count=5 → autobolus
    - dd_autobolus_count=2, dd_temp_basal_count=10 → temp_basal (below threshold)
    - hk_autobolus_count=5 (DD=0) → autobolus (greatest-of rule)
    - All counts 0 → ambiguous, is_ambiguous=True
    - Custom min_autobolus_count threshold respected

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_autobolus_via_dd_count():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_temp_basal_when_below_autobolus_threshold():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_autobolus_via_hk_count():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_ambiguous_when_no_signal():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_custom_min_autobolus_threshold():
    raise NotImplementedError("Phase B")
