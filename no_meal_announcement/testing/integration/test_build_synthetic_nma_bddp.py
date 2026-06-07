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
import nma_test_helpers as _nh  # type: ignore # noqa: E402  — for _AUTO_PAYLOAD (autobolus marker)


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


def _autoboluses(rows):
    """Bolus rows carrying the HealthKit AutomaticallyIssued flag. Autoboluses are emitted with
    subType='normal' (like all Loop boluses) + the _AUTO_PAYLOAD flag; the classifier counts them as
    automatic, NOT toward BE — so count by the flag, not by subType."""
    return [r for r in rows if r["type"] == "bolus" and r.get("payload") == _nh._AUTO_PAYLOAD]


def _manual_boluses(rows):
    """Manual normal boluses (meal + non-meal) = BE; excludes the HK-flagged autoboluses."""
    return [r for r in rows if r["type"] == "bolus" and r.get("payload") != _nh._AUTO_PAYLOAD]


# ---------------------------------------------------------------------------
# 6.1 nma_user_pure_be0
# ---------------------------------------------------------------------------

def test_pure_be0_shape():
    rows = nb._archetype_pure_be0()
    assert len(_by_type(rows, "cbg")) == 14 * 288
    assert len(_manual_boluses(rows)) == 0          # CE=0/BE=0 → no manual normal boluses (BE=0)
    assert len(_autoboluses(rows)) == 14 * 5        # 5 autoboluses/day (subType='normal' + HK flag)
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
    # Total manual normal boluses (BE) = 18 + 10 = 28
    assert len(_manual_boluses(rows)) == 28
    assert len(_autoboluses(rows)) == 14 * 5
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
    assert len(_manual_boluses(rows)) == 0
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
    assert basals[0]["rate"] < basals[-1]["rate"]
    # Sanity: 30 U/day target → basal_u ≈ 18 → rate ≈ 0.75 U/hr.
    # And 80 U/day target → basal_u ≈ 48 → rate ≈ 2.0 U/hr.  (basal rate lands in `rate`, not `normal`.)
    assert 0.7 < basals[0]["rate"] < 0.8
    assert 1.9 < basals[-1]["rate"] < 2.1


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
    assert len(_autoboluses(rows)) == 50
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
    # All days CE=0/BE=0: no food, no manual normal boluses.
    assert len(_by_type(rows, "food")) == 0
    assert len(_manual_boluses(rows)) == 0
    # 30 days × 5 autoboluses
    assert len(_autoboluses(rows)) == 30 * 5


def test_known_low_high_tdd_low_segment_has_lower_basal_rate():
    rows = nb._archetype_known_low_high_tdd()
    basals = sorted(_by_type(rows, "basal"), key=lambda r: r["time_string"])
    low_seg = basals[:15]
    high_seg = basals[15:]
    assert all(b["rate"] == 0.625 for b in low_seg)
    assert all(b["rate"] == 1.25 for b in high_seg)


# ---------------------------------------------------------------------------
# 6.12 nma_user_known_hma (HMA — CE>=3/BE>=3 arm)
# ---------------------------------------------------------------------------

def test_known_hma_shape():
    """30 days: 6 CE=0/BE=0 + 24 HMA days (3 meal + 3 non-meal normal boluses each → CE=3 / BE=6),
    split 12 AB / 12 TB. All boluses are subType='normal' (autoboluses carry an HK flag, not a
    distinct subtype)."""
    rows = nb._archetype_known_hma()
    assert len(_days_with_cbg(rows)) == 30
    # 24 HMA days × 3 meals = 72 food records (the 6 CE=0 days have none).
    assert len(_by_type(rows, "food")) == 24 * 3
    assert _carb_sum(rows) == 24 * 3 * 30.0
    # All boluses subType='normal': CE=0 day = 5 (auto); HMA-AB = 3 meal + 3 non-meal + 5 auto = 11;
    # HMA-TB = 3 + 3 + 0 = 6.
    assert len(_by_type(rows, "bolus", subtype="normal")) == 6 * 5 + 12 * 11 + 12 * 6
    assert len(_by_type(rows, "basal")) == 30


def test_known_hma_is_registered_as_a_pair():
    """Both HMA users share the builder; the 2nd carries its own _userId so each HMA × strategy cell
    has >=2 distinct users (the HMA LMMs converge)."""
    assert {"nma_user_known_hma", "nma_user_known_hma_2"} <= set(nb.ARCHETYPES)
    assert {r["_userId"] for r in nb.ARCHETYPES["nma_user_known_hma_2"]()} == {"nma_user_known_hma_2"}


# ---------------------------------------------------------------------------
# 6.11 build_synthetic_nma_bddp aggregate (no Spark — exercise _build_rows
# and _to_bddp_row)
# ---------------------------------------------------------------------------

def test_archetypes_dict_user_count():
    assert len(nb.ARCHETYPES) == 13
    assert set(nb.ARCHETYPES) == set(nb.DEMOGRAPHICS) == set(nb.ARCHETYPE_DAYS) == set(nb.USER_GENDER)


def test_build_rows_sums_archetype_counts():
    rows = nb._build_rows()
    expected = sum(len(builder()) for builder in nb.ARCHETYPES.values())
    assert len(rows) == expected


def test_build_rows_distinct_user_count():
    rows = nb._build_rows()
    assert len({r["_userId"] for r in rows if r["_userId"]}) == len(nb.ARCHETYPES)


def test_to_bddp_row_has_all_bddp_columns():
    """Round-trip a minimal row through _to_bddp_row and verify every BDDP
    column is present (Spark's createDataFrame with explicit schema would
    fail otherwise)."""
    minimal = next(iter(nb._archetype_pure_be0()))
    expanded = nb._to_bddp_row(minimal)
    # _to_bddp_row expands to NMA_BDDP_COLUMNS (= BDDP_COLUMNS + the NMA-only `rate` column).
    assert set(expanded) == set(nb.NMA_BDDP_COLUMNS)


def test_to_bddp_row_defaults_timezone_offset():
    expanded = nb._to_bddp_row({"_userId": "x", "type": "cbg"})
    assert expanded["timezoneOffset"] == nb.TZ_OFFSET_MIN


@pytest.mark.parametrize("uid", sorted(nb.ARCHETYPES))
def test_every_archetype_has_demographics(uid):
    assert uid in nb.DEMOGRAPHICS
    assert isinstance(nb.DEMOGRAPHICS[uid]["dob"], date)


@pytest.mark.parametrize("uid", sorted(nb.ARCHETYPES))
def test_every_archetype_uses_consistent_userid(uid):
    builder = nb.ARCHETYPES[uid]
    rows = builder()
    ids = {r["_userId"] for r in rows if r["_userId"]}
    assert ids == {uid}
