"""
Tests for `testing/integration/build_synthetic_nma_bddp.py` — Unit 6.

Per-archetype row-shape assertions (Units 6.1 – 6.10) plus the aggregate
build (6.11). Pure-Python: the Spark side of `build_synthetic_nma_bddp`
runs on Databricks and is exercised by the end-to-end test (Unit 22), not
here.
"""

import json
import os
import sys
from datetime import date, timedelta

import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "..")))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..")))

from FDA_real_world_data.testing.integration.build_synthetic_bddp import (  # type: ignore # noqa: E402
    BDDP_COLUMNS,
)

from integration import build_synthetic_nma_bddp as nb  # type: ignore # noqa: E402


# ---------------------------------------------------------------------------
# Row-shape utilities
# ---------------------------------------------------------------------------

def _by_type(rows, type_, subtype=None):
    if subtype is None:
        return [r for r in rows if r["type"] == type_]
    return [r for r in rows if r["type"] == type_ and r.get("subType") == subtype]


def _days_with_cbg(rows):
    return sorted({r["time_string"][:10] for r in rows if r["type"] == "cbg"})


def _carb_sum(rows):
    total = 0.0
    for r in _by_type(rows, "food"):
        payload = json.loads(r["nutrition"])
        total += payload["carbohydrate"]["net"]
    return total


# ---------------------------------------------------------------------------
# 6.1 nma_user_pure_be0
# ---------------------------------------------------------------------------

def test_pure_be0_shape():
    rows = nb._archetype_pure_be0()
    assert len(_by_type(rows, "cbg")) == 14 * 288
    assert len(_by_type(rows, "bolus", subtype="normal")) == 0
    assert len(_by_type(rows, "bolus", subtype="automated")) == 14 * 5
    assert len(_by_type(rows, "food")) == 0
    assert _carb_sum(rows) == 0.0
    assert len(_by_type(rows, "basal")) == 14
    assert _days_with_cbg(rows) == [
        (date(2024, 1, 1) + timedelta(days=d)).isoformat() for d in range(14)
    ]


# ---------------------------------------------------------------------------
# 6.2 nma_user_mixed
# ---------------------------------------------------------------------------

def test_mixed_shape():
    """3 BE=0 + 3 BE=1 + 3 BE=5 + 5 CE>0 across 14 consecutive days."""
    rows = nb._archetype_mixed()
    assert len(_by_type(rows, "cbg")) == 14 * 288
    # Non-meal normal boluses: 3×0 + 3×1 + 3×5 + 5×0 = 18
    # Meal normal boluses:     3×0 + 3×0 + 3×0 + 5×2 = 10
    # Total normal-subType bolus rows = 18 + 10 = 28
    assert len(_by_type(rows, "bolus", subtype="normal")) == 28
    assert len(_by_type(rows, "bolus", subtype="automated")) == 14 * 5
    # Food records: 5 CE>0 days × 2 meals = 10
    assert len(_by_type(rows, "food")) == 10
    # Carbs: 10 meals × 30g = 300
    assert _carb_sum(rows) == 300.0
    assert len(_by_type(rows, "basal")) == 14


# ---------------------------------------------------------------------------
# 6.3 nma_user_low_coverage
# ---------------------------------------------------------------------------

def test_low_coverage_has_half_the_cbg():
    rows = nb._archetype_low_coverage()
    cbg = _by_type(rows, "cbg")
    assert len(cbg) == 14 * 144
    # All other shape is CE=0/BE=0.
    assert _carb_sum(rows) == 0.0
    assert len(_by_type(rows, "bolus", subtype="normal")) == 0
    assert len(_by_type(rows, "basal")) == 14


# ---------------------------------------------------------------------------
# 6.4 nma_user_below_min_days
# ---------------------------------------------------------------------------

def test_below_min_days_spans_only_8_days():
    rows = nb._archetype_below_min_days()
    assert len(_days_with_cbg(rows)) == 8
    assert len(_by_type(rows, "cbg")) == 8 * 288
    assert len(_by_type(rows, "basal")) == 8


# ---------------------------------------------------------------------------
# 6.5 nma_user_pediatric
# ---------------------------------------------------------------------------

def test_pediatric_dob_is_2010():
    assert nb.DEMOGRAPHICS["nma_user_pediatric"]["dob"] == date(2010, 1, 1)


def test_pediatric_has_mixed_day_types():
    """Same day-type composition as nma_user_mixed; pediatric flag comes from DOB."""
    rows = nb._archetype_pediatric()
    assert len(_by_type(rows, "cbg")) == 14 * 288
    assert len(_by_type(rows, "food")) == 10  # 5 CE>0 days × 2 meals
    assert _carb_sum(rows) == 300.0


# ---------------------------------------------------------------------------
# 6.6 nma_user_ambiguous_strategy
# ---------------------------------------------------------------------------

def test_ambiguous_strategy_has_no_autobolus():
    rows = nb._archetype_ambiguous_strategy()
    assert len(_by_type(rows, "bolus", subtype="automated")) == 0
    # Manual normal boluses present for strategy=ambiguous classification.
    assert len(_by_type(rows, "bolus", subtype="normal")) == 14


# ---------------------------------------------------------------------------
# 6.7 nma_user_tdd_drift
# ---------------------------------------------------------------------------

def test_tdd_drift_spans_60_days():
    rows = nb._archetype_tdd_drift()
    assert len(_days_with_cbg(rows)) == 60
    assert len(_by_type(rows, "basal")) == 60


def test_tdd_drift_basal_rate_rises_monotonically():
    """Day 0 basal rate < day 59 basal rate by design."""
    rows = nb._archetype_tdd_drift()
    basals = sorted(_by_type(rows, "basal"), key=lambda r: r["time_string"])
    assert basals[0]["normal"] < basals[-1]["normal"]
    # Sanity: 30 U/day target → basal_u ≈ 18 → rate ≈ 0.75 U/hr.
    # And 80 U/day target → basal_u ≈ 48 → rate ≈ 2.0 U/hr.
    assert 0.7 < basals[0]["normal"] < 0.8
    assert 1.9 < basals[-1]["normal"] < 2.1


# ---------------------------------------------------------------------------
# 6.8 nma_user_known_paired_diff
# ---------------------------------------------------------------------------

def test_known_paired_diff_spans_20_days_with_correct_carb_pattern():
    rows = nb._archetype_known_paired_diff()
    assert len(_days_with_cbg(rows)) == 20
    # 10 CE=0 days (no carbs) + 10 CE>0 days × 2 meals × 30g = 600
    assert _carb_sum(rows) == 10 * 2 * 30.0
    assert len(_by_type(rows, "food")) == 10 * 2


def test_known_paired_diff_tir_designed_by_segment():
    """First 10 days target 80% TIR; last 10 days target 70% TIR.

    Coarse sanity: count CBG values in the 70–180 mg/dL range per segment.
    """
    rows = nb._archetype_known_paired_diff()
    cbg = sorted(_by_type(rows, "cbg"), key=lambda r: r["time_string"])
    seg1 = cbg[:10 * 288]
    seg2 = cbg[10 * 288:]

    def in_range_frac(events):
        in_range = sum(1 for r in events if 70.0 <= r["normal"] / nb.MMOL_PER_MGDL <= 180.0)
        return in_range / len(events)

    assert abs(in_range_frac(seg1) - 0.80) < 0.01
    assert abs(in_range_frac(seg2) - 0.70) < 0.01


# ---------------------------------------------------------------------------
# 6.9 nma_user_known_interaction
# ---------------------------------------------------------------------------

def test_known_interaction_spans_20_days_in_four_cells():
    rows = nb._archetype_known_interaction()
    assert len(_days_with_cbg(rows)) == 20
    # AB cells (10 days): 5 autoboluses/day = 50
    assert len(_by_type(rows, "bolus", subtype="automated")) == 50
    # TB cells (10 days): 0 autoboluses; 1 non-meal each
    # AB CE>0 cell (5 days): 2 meals each = 10 food rows
    # TB CE>0 cell (5 days): 2 meals each = 10 food rows
    assert len(_by_type(rows, "food")) == 20


# ---------------------------------------------------------------------------
# 6.10 nma_user_known_low_high_tdd
# ---------------------------------------------------------------------------

def test_known_low_high_tdd_spans_30_days_all_ce0_be0():
    rows = nb._archetype_known_low_high_tdd()
    assert len(_days_with_cbg(rows)) == 30
    # All days CE=0/BE=0: no food, no normal boluses.
    assert len(_by_type(rows, "food")) == 0
    assert len(_by_type(rows, "bolus", subtype="normal")) == 0
    # 30 days × 5 autoboluses
    assert len(_by_type(rows, "bolus", subtype="automated")) == 30 * 5


def test_known_low_high_tdd_low_segment_has_lower_basal_rate():
    rows = nb._archetype_known_low_high_tdd()
    basals = sorted(_by_type(rows, "basal"), key=lambda r: r["time_string"])
    low_seg = basals[:15]
    high_seg = basals[15:]
    assert all(b["normal"] == 0.625 for b in low_seg)
    assert all(b["normal"] == 1.25 for b in high_seg)


# ---------------------------------------------------------------------------
# 6.11 build_synthetic_nma_bddp aggregate (no Spark — exercise _build_rows
# and _to_bddp_row)
# ---------------------------------------------------------------------------

def test_archetypes_dict_has_10_users():
    assert len(nb.ARCHETYPES) == 10
    assert set(nb.ARCHETYPES) == set(nb.DEMOGRAPHICS)


def test_build_rows_sums_archetype_counts():
    rows = nb._build_rows()
    expected = sum(len(builder()) for builder in nb.ARCHETYPES.values())
    assert len(rows) == expected


def test_build_rows_distinct_user_count_is_10():
    rows = nb._build_rows()
    assert len({r["_userId"] for r in rows if r["_userId"]}) == 10


def test_to_bddp_row_has_all_bddp_columns():
    """Round-trip a minimal row through _to_bddp_row and verify every BDDP
    column is present (Spark's createDataFrame with explicit schema would
    fail otherwise)."""
    minimal = next(iter(nb._archetype_pure_be0()))
    expanded = nb._to_bddp_row(minimal)
    assert set(expanded) == set(BDDP_COLUMNS)


def test_to_bddp_row_defaults_timezone_offset():
    expanded = nb._to_bddp_row({"_userId": "x", "type": "cbg"})
    assert expanded["timezoneOffset"] == nb.TZ_OFFSET_MIN


@pytest.mark.parametrize("uid", sorted(["nma_user_pure_be0", "nma_user_mixed",
                                         "nma_user_low_coverage", "nma_user_below_min_days",
                                         "nma_user_pediatric", "nma_user_ambiguous_strategy",
                                         "nma_user_tdd_drift", "nma_user_known_paired_diff",
                                         "nma_user_known_interaction", "nma_user_known_low_high_tdd"]))
def test_every_archetype_has_demographics(uid):
    assert uid in nb.DEMOGRAPHICS
    assert isinstance(nb.DEMOGRAPHICS[uid]["dob"], date)


@pytest.mark.parametrize("uid", sorted(["nma_user_pure_be0", "nma_user_mixed",
                                         "nma_user_low_coverage", "nma_user_below_min_days",
                                         "nma_user_pediatric", "nma_user_ambiguous_strategy",
                                         "nma_user_tdd_drift", "nma_user_known_paired_diff",
                                         "nma_user_known_interaction", "nma_user_known_low_high_tdd"]))
def test_every_archetype_uses_consistent_userid(uid):
    builder = nb.ARCHETYPES[uid]
    rows = builder()
    ids = {r["_userId"] for r in rows if r["_userId"]}
    assert ids == {uid}
