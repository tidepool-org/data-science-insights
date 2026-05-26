"""
Tests for `analysis/utils/classification.py` — Unit 2.

Pure-pandas tests; no SparkSession required.
"""

import os
import sys

import pandas as pd
import pytest

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis", "utils")))

from classification import (  # type: ignore # noqa: E402
    assign_day_type_columns,
    classify_day,
    day_type_label,
)


# ---------------------------------------------------------------------------
# 2.1 classify_day
# ---------------------------------------------------------------------------

def test_classify_day_pure_be0():
    """(carb=0, meal=0, non_meal=0) → BE=0 ✓, BE≤1 ✓, BE≤∞ ✓, CE>0 ✗."""
    flags = classify_day(carb_grams=0.0, meal_bolus_count=0, non_meal_bolus_count=0)
    assert flags == {
        "is_ce0_be0": True,
        "is_ce0_bele1": True,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    }


def test_classify_day_bele1_only():
    """(carb=0, meal=0, non_meal=1) → BE=0 ✗, BE≤1 ✓, BE≤∞ ✓, CE>0 ✗."""
    flags = classify_day(0.0, 0, 1)
    assert flags == {
        "is_ce0_be0": False,
        "is_ce0_bele1": True,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    }


def test_classify_day_beinf_only():
    """(carb=0, meal=0, non_meal=5) → BE=0 ✗, BE≤1 ✗, BE≤∞ ✓, CE>0 ✗."""
    flags = classify_day(0.0, 0, 5)
    assert flags == {
        "is_ce0_be0": False,
        "is_ce0_bele1": False,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    }


def test_classify_day_ce_pos():
    """(carb=20, meal=1, non_meal=0) → all NMA ✗, CE>0 ✓."""
    flags = classify_day(20.0, 1, 0)
    assert flags == {
        "is_ce0_be0": False,
        "is_ce0_bele1": False,
        "is_ce0_beinf": False,
        "is_ce_pos": True,
    }


def test_classify_day_carb_meal_disagreement_edge_case():
    """(carb=0, meal=1, non_meal=0): meal_bolus_count > 0 → CE>0 per docs."""
    flags = classify_day(0.0, 1, 0)
    assert flags["is_ce_pos"] is True
    assert flags["is_ce0_be0"] is False
    assert flags["is_ce0_bele1"] is False
    assert flags["is_ce0_beinf"] is False


def test_classify_day_carb_only_no_meal_bolus():
    """(carb>0, meal=0, non_meal=0): carb_grams > 0 → CE>0."""
    flags = classify_day(20.0, 0, 0)
    assert flags["is_ce_pos"] is True
    assert flags["is_ce0_beinf"] is False


# ---------------------------------------------------------------------------
# 2.2 day_type_label — strictest matching arm
# ---------------------------------------------------------------------------

def test_day_type_label_be0_wins():
    row = pd.Series({
        "is_ce0_be0": True,
        "is_ce0_bele1": True,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    })
    assert day_type_label(row) == "CE=0/BE=0"


def test_day_type_label_bele1_wins_when_be0_false():
    row = pd.Series({
        "is_ce0_be0": False,
        "is_ce0_bele1": True,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    })
    assert day_type_label(row) == "CE=0/BE≤1"


def test_day_type_label_beinf_wins_when_others_false():
    row = pd.Series({
        "is_ce0_be0": False,
        "is_ce0_bele1": False,
        "is_ce0_beinf": True,
        "is_ce_pos": False,
    })
    assert day_type_label(row) == "CE=0/BE≤∞"


def test_day_type_label_ce_pos_fallback():
    row = pd.Series({
        "is_ce0_be0": False,
        "is_ce0_bele1": False,
        "is_ce0_beinf": False,
        "is_ce_pos": True,
    })
    assert day_type_label(row) == "CE>0"


# ---------------------------------------------------------------------------
# 2.3 assign_day_type_columns
# ---------------------------------------------------------------------------

def test_assign_day_type_columns_adds_five_columns_and_preserves_row_count():
    df = pd.DataFrame({
        "_userId": ["u1", "u1", "u1", "u1"],
        "day": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "carb_grams": [0.0, 0.0, 0.0, 25.0],
        "meal_bolus_count": [0, 0, 0, 1],
        "non_meal_bolus_count": [0, 1, 5, 0],
    })
    out = assign_day_type_columns(df)

    assert len(out) == 4
    for col in (
        "is_ce0_be0",
        "is_ce0_bele1",
        "is_ce0_beinf",
        "is_ce_pos",
        "day_type_strictest",
    ):
        assert col in out.columns

    expected_strictest = ["CE=0/BE=0", "CE=0/BE≤1", "CE=0/BE≤∞", "CE>0"]
    assert out["day_type_strictest"].tolist() == expected_strictest


def test_assign_day_type_columns_does_not_mutate_input():
    df = pd.DataFrame({
        "carb_grams": [0.0],
        "meal_bolus_count": [0],
        "non_meal_bolus_count": [0],
    })
    original_cols = list(df.columns)
    _ = assign_day_type_columns(df)
    assert list(df.columns) == original_cols
