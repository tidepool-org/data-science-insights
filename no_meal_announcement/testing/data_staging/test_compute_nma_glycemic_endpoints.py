"""
Tests for `data_staging/compute_nma_glycemic_endpoints.py`.

Thin wrapper test — ensures the FDA `compute_glycemic_endpoints` is invoked
with `group_cols=["_userId","day"]` and produces per-user-day rows.

Phase B test cases (planned):
    - Ports the relevant cases from FDA `test_compute_glycemic_endpoints.py`
      with the new group_cols.
    - Hypo-event detection: 3 consecutive <54 readings ending at 3
      consecutive ≥70 → 1 event.

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_per_user_day_grouping():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_hypo_event_definition():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_endpoint_columns_present():
    raise NotImplementedError("Phase B")
