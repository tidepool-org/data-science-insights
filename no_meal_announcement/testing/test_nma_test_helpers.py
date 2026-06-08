"""
Tests for `testing/nma_test_helpers.py` — Unit 5.

Pure-Python tests; no SparkSession required. Verifies the helper functions
behave as Unit 6 (`build_synthetic_nma_bddp`) and the Layer 3 Spark staging
tests rely on.
"""

import os
import sys
from datetime import date

import pandas as pd
import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..")))

from testing import nma_test_helpers as nma  # type: ignore # noqa: E402


# ---------------------------------------------------------------------------
# 5.1 FDA helper re-exports
# ---------------------------------------------------------------------------

def test_reexports_fda_helpers_are_identical():
    """Each re-exported name resolves to the same object as the FDA module."""
    from FDA_real_world_data.testing import staging_test_helpers as fda

    assert nma.setup_test_table is fda.setup_test_table
    assert nma.read_test_output is fda.read_test_output
    assert nma.assert_row_count is fda.assert_row_count
    assert nma.assert_column_values is fda.assert_column_values
    assert nma.make_loop_recs is fda.make_loop_recs


def test_reexports_all_listed_in__all__():
    expected = {
        "setup_test_table", "read_test_output", "assert_row_count",
        "assert_column_values", "make_loop_recs",
    }
    assert expected.issubset(set(nma.__all__))


# ---------------------------------------------------------------------------
# 5.2 make_bolus_events
# ---------------------------------------------------------------------------

DAY = date(2024, 1, 1)
UID = "test_user_01"


def test_make_bolus_events_row_count():
    """2 meal + 3 non-meal + 4 autobolus → 2*2 + 3 + 4 = 11 rows."""
    rows = nma.make_bolus_events(UID, DAY, n_meal=2, n_non_meal=3, n_autobolus=4)
    assert len(rows) == 2 * 2 + 3 + 4


def test_make_bolus_events_subtype_breakdown():
    rows = nma.make_bolus_events(UID, DAY, n_meal=2, n_non_meal=3, n_autobolus=4)
    bolus = [r for r in rows if r["type"] == "bolus"]
    food = [r for r in rows if r["type"] == "food"]

    # All Loop boluses (incl. autoboluses) are subType='normal'; autoboluses are told apart by the
    # HealthKit AutomaticallyIssued flag in the payload (the classifier counts them as automatic).
    manual = [r for r in bolus if r.get("payload") != nma._AUTO_PAYLOAD]
    automated = [r for r in bolus if r.get("payload") == nma._AUTO_PAYLOAD]

    assert len(manual) == 2 + 3, "manual normal boluses (BE) = meal + non_meal"
    assert len(automated) == 4, "autoboluses = n_autobolus (HK-flagged, subType='normal')"
    assert len(food) == 2, "one food record per meal"


def test_make_bolus_events_meal_food_pair_timestamps_match():
    """Each meal bolus must share a timestamp with exactly one food record
    so the ±15min meal-detection window is unambiguous."""
    rows = nma.make_bolus_events(UID, DAY, n_meal=2)
    bolus_ts = sorted(r["time_string"] for r in rows if r["type"] == "bolus")
    food_ts = sorted(r["time_string"] for r in rows if r["type"] == "food")
    assert bolus_ts == food_ts


def test_make_bolus_events_userid_propagates_to_all_event_rows():
    rows = nma.make_bolus_events("alice", DAY, n_meal=1, n_non_meal=1, n_autobolus=1)
    # Event rows have _userId set; food row also has _userId set.
    event_rows = [r for r in rows if r["type"] in ("bolus", "food")]
    assert {r["_userId"] for r in event_rows} == {"alice"}


def test_make_bolus_events_empty_request_returns_empty_list():
    assert nma.make_bolus_events(UID, DAY) == []


def test_make_bolus_events_carbs_per_meal_lands_in_nutrition_json():
    import json

    rows = nma.make_bolus_events(UID, DAY, n_meal=1, carbs_per_meal=42.0)
    food = [r for r in rows if r["type"] == "food"][0]
    payload = json.loads(food["nutrition"])
    assert payload["carbohydrate"]["net"] == 42.0


# ---------------------------------------------------------------------------
# 5.2b make_loop_direct_basal_row (Loop-direct TDD stream)
# ---------------------------------------------------------------------------

def test_make_loop_direct_basal_row_shape():
    import json

    r = nma.make_loop_direct_basal_row(UID, DAY, delivered_units=10.0)
    assert r["type"] == "basal"
    assert json.loads(r["origin"])["name"] == "com.loopkit.Loop"   # the LOOP_DIRECT_PREDICATE key
    assert json.loads(r["payload"])["deliveredUnits"] == 10.0      # the actual delivered amount
    # rate is the COMMANDED temp rate ≈ 1.7× the delivered hourly rate — a decoy the TDD SQL ignores.
    assert r["rate"] == pytest.approx(1.7 * (10.0 / 24))


def test_make_loop_direct_basal_row_explicit_commanded_rate():
    r = nma.make_loop_direct_basal_row(UID, DAY, delivered_units=8.0, commanded_rate=2.5)
    assert r["rate"] == 2.5


def test_make_loop_direct_basal_row_in_all():
    assert "make_loop_direct_basal_row" in nma.__all__


# ---------------------------------------------------------------------------
# 5.3 make_user_day_rows
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("archetype", ["ce0_be0", "ce0_bele1", "ce0_beinf", "ce_pos"])
def test_make_user_day_rows_row_count_per_archetype(archetype):
    """Row count = days × records_per_day(archetype)."""
    days = 3
    rows = nma.make_user_day_rows(UID, days=days, archetype=archetype)
    assert len(rows) == days * nma.records_per_day(archetype)


def test_make_user_day_rows_unknown_archetype_raises():
    with pytest.raises(ValueError, match="unknown archetype"):
        nma.make_user_day_rows(UID, days=1, archetype="not_a_real_archetype")


def test_make_user_day_rows_cbg_can_be_disabled():
    rows = nma.make_user_day_rows(UID, days=2, archetype="ce_pos", include_cbg=False)
    cbg = [r for r in rows if r["type"] == "cbg"]
    assert cbg == []
    # Only the bolus + food events remain.
    expected = 2 * nma.records_per_day("ce_pos", include_cbg=False)
    assert len(rows) == expected


def test_make_user_day_rows_spans_consecutive_days():
    rows = nma.make_user_day_rows(UID, days=3, archetype="ce0_be0", start_day=DAY)
    cbg_dates = {r["time_string"][:10] for r in rows if r["type"] == "cbg"}
    assert cbg_dates == {"2024-01-01", "2024-01-02", "2024-01-03"}


# ---------------------------------------------------------------------------
# 5.4 assert_day_type_flags
# ---------------------------------------------------------------------------

def _flag_df():
    return pd.DataFrame([
        {"_userId": "u1", "day": date(2024, 1, 1),
         "is_ce0_be0": True, "is_ce0_bele1": True, "is_ce_pos": False},
        {"_userId": "u1", "day": date(2024, 1, 2),
         "is_ce0_be0": False, "is_ce0_bele1": False, "is_ce_pos": True},
    ])


def test_assert_day_type_flags_passes_on_match():
    df = _flag_df()
    nma.assert_day_type_flags(df, {
        ("u1", date(2024, 1, 1)): {"is_ce0_be0": True, "is_ce_pos": False},
        ("u1", date(2024, 1, 2)): {"is_ce_pos": True},
    })


def test_assert_day_type_flags_raises_on_value_mismatch():
    df = _flag_df()
    with pytest.raises(AssertionError, match="is_ce0_be0"):
        nma.assert_day_type_flags(df, {
            ("u1", date(2024, 1, 1)): {"is_ce0_be0": False},
        })


def test_assert_day_type_flags_raises_on_missing_user_day():
    df = _flag_df()
    with pytest.raises(AssertionError, match="no row for"):
        nma.assert_day_type_flags(df, {
            ("ghost", date(2024, 1, 1)): {"is_ce0_be0": True},
        })


def test_assert_day_type_flags_raises_on_missing_column():
    df = _flag_df()
    with pytest.raises(AssertionError, match="column 'is_ce0_beinf' missing"):
        nma.assert_day_type_flags(df, {
            ("u1", date(2024, 1, 1)): {"is_ce0_beinf": True},
        })


def test_assert_day_type_flags_raises_on_duplicate_user_day():
    df = pd.concat([_flag_df(), _flag_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(AssertionError, match="2 rows for"):
        nma.assert_day_type_flags(df, {
            ("u1", date(2024, 1, 1)): {"is_ce0_be0": True},
        })
