"""
Tests for `data_staging/export_user_day_bolus_counts.py`.

Phase B test cases (planned):
    - 3 boluses on day D with 1 paired to a food record ±15min
      → meal_bolus_count=1, non_meal_bolus_count=2
    - Autobolus-only day (3 'automated' boluses, no normalBolus DD) → both counts = 0
    - Empty day (no boluses, no food) → both counts = 0 (or absent row)
    - User with multi-day mix → row count == day count

Status: Phase A placeholder. Body authored in Phase B.
"""

import pytest


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_three_boluses_one_with_food():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_autobolus_only_day_excluded_from_counts():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_empty_day_yields_zero_counts():
    raise NotImplementedError("Phase B")


@pytest.mark.skip(reason="Phase A placeholder — implement in Phase B")
def test_multi_day_user_row_count():
    raise NotImplementedError("Phase B")
