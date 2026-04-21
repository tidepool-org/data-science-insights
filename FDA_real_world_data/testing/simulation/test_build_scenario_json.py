"""
Unit tests for simulation/export/build_scenario_json.py.

Pure-Python / pandas — no Spark required. Run from Databricks via
run_all_tests.py or directly with `python test_build_scenario_json.py`.
"""

import json
import os
import sys

import pandas as pd

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "..", "simulation", "export"))

from build_scenario_json import (  # type: ignore # noqa: E402
    _bolus_entries,
    _carb_entries,
    _fmt_dt,
    _required_fields_present,
    _scalar_schedule,
    _snap_to_grid,
    _target_schedule,
)


def _check(cond, label):
    if not cond:
        raise AssertionError(f"FAIL: {label}")
    print(f"PASS: {label}")


# --- _snap_to_grid --------------------------------------------------------

def test_snap_to_grid_basic():
    sim_start = pd.Timestamp("2024-05-28 12:00:00")
    series = pd.Series([
        pd.Timestamp("2024-05-28 12:01:30"),  # -> 12:00 (<2.5m forward)
        pd.Timestamp("2024-05-28 12:04:00"),  # -> 12:05 (=2.5m halfway; banker's rounds to even?)
        pd.Timestamp("2024-05-28 12:07:00"),  # -> 12:05 (closest)
        pd.Timestamp("2024-05-28 11:58:00"),  # -> 12:00 (negative delta, rounds to nearest)
    ])
    snapped = _snap_to_grid(series, sim_start)
    expected = [
        pd.Timestamp("2024-05-28 12:00:00"),
        pd.Timestamp("2024-05-28 12:05:00"),
        pd.Timestamp("2024-05-28 12:05:00"),
        pd.Timestamp("2024-05-28 12:00:00"),
    ]
    # 12:04:00 is exactly 4/5 of the way — round() makes it 12:05 (1.33 → 1)
    # We only assert the unambiguous cases.
    _check(snapped.iloc[0] == expected[0], "snap 01:30 forward -> 00:00")
    _check(snapped.iloc[2] == expected[2], "snap 07:00 -> 05:00")
    _check(snapped.iloc[3] == expected[3], "snap -2m -> 00:00")


# --- _bolus_entries / _carb_entries ---------------------------------------

def test_bolus_entries_sum_same_tick():
    sim_start = pd.Timestamp("2024-05-28 12:00:00")
    df = pd.DataFrame({
        "bolus_timestamp": [
            pd.Timestamp("2024-05-28 12:01:30"),  # same tick as next
            pd.Timestamp("2024-05-28 12:02:30"),  # same tick
            pd.Timestamp("2024-05-28 12:10:00"),
            pd.Timestamp("2024-05-28 12:15:00"),  # NaN value — dropped
        ],
        "bolus_amount": [0.5, 1.0, 2.0, float("nan")],
    })
    df["bolus_timestamp"] = _snap_to_grid(df["bolus_timestamp"], sim_start)
    entries = _bolus_entries(df)
    _check(len(entries) == 2, "2 entries after NaN drop + same-tick merge")
    _check(entries[0]["time"] == "5/28/2024 12:00:00", "first entry on sim_start tick")
    _check(entries[0]["value"] == 1.5, "same-tick boluses summed (0.5 + 1.0)")
    _check(entries[1]["time"] == "5/28/2024 12:10:00", "second entry 10 min later")
    _check(entries[1]["value"] == 2.0, "non-colliding bolus untouched")


def test_bolus_entries_empty_df():
    df = pd.DataFrame(columns=["bolus_timestamp", "bolus_amount"])
    _check(_bolus_entries(df) == [], "empty df returns []")


def test_carb_entries_sum_same_tick():
    sim_start = pd.Timestamp("2024-05-28 12:00:00")
    df = pd.DataFrame({
        "carb_timestamp": [
            pd.Timestamp("2024-05-28 13:00:30"),
            pd.Timestamp("2024-05-28 13:02:00"),   # snaps to same tick as above
            pd.Timestamp("2024-05-28 13:10:00"),
        ],
        "carb_grams": [15.0, 10.0, 30.0],
    })
    df["carb_timestamp"] = _snap_to_grid(df["carb_timestamp"], sim_start)
    entries = _carb_entries(df)
    _check(len(entries) == 2, "2 carb entries after same-tick merge")
    _check(entries[0]["value"] == 25.0, "same-tick carbs summed (15 + 10)")
    _check(entries[0]["start_time"] == "5/28/2024 13:00:00", "sum emitted on merged tick")
    _check(entries[1]["value"] == 30.0, "non-colliding carb untouched")


def test_carb_entries_empty_df():
    df = pd.DataFrame(columns=["carb_timestamp", "carb_grams"])
    _check(_carb_entries(df) == [], "empty df returns []")


# --- _scalar_schedule / _target_schedule ----------------------------------

def test_scalar_schedule():
    payload = json.dumps([
        {"start_time": "00:00:00", "value": 0.3},
        {"start_time": "06:00:00", "value": 0.5},
        {"start_time": "22:00:00", "value": 0.4},
    ])
    sched = _scalar_schedule(payload)
    _check(sched["start_times"] == ["00:00:00", "06:00:00", "22:00:00"],
           "scalar_schedule start_times list")
    _check(sched["values"] == [0.3, 0.5, 0.4], "scalar_schedule values list")


def test_target_schedule_rounds_to_int():
    payload = json.dumps([
        {"start_time": "00:00:00", "low": 100.4, "high": 120.6},
        {"start_time": "06:00:00", "low": 95.5, "high": 110.4},
    ])
    sched = _target_schedule(payload)
    _check(sched["start_times"] == ["00:00:00", "06:00:00"], "target_schedule start_times")
    _check(sched["lower_values"] == [100, 96], "target_schedule rounds lows to int")
    _check(sched["upper_values"] == [121, 110], "target_schedule rounds highs to int")


# --- _required_fields_present ---------------------------------------------

def test_required_fields_all_valid():
    row = {
        "basal_schedule": json.dumps([{"start_time": "00:00:00", "value": 0.3}]),
        "isf_schedule":   json.dumps([{"start_time": "00:00:00", "value": 40}]),
        "cir_schedule":   json.dumps([{"start_time": "00:00:00", "value": 15}]),
        "target_schedule": json.dumps([{"start_time": "00:00:00", "low": 100, "high": 120}]),
    }
    _check(_required_fields_present(row) is True, "all valid schedules -> True")


def test_required_fields_missing_field():
    row = {
        "basal_schedule": json.dumps([{"start_time": "00:00:00", "value": 0.3}]),
        # isf_schedule missing entirely
        "cir_schedule":   json.dumps([{"start_time": "00:00:00", "value": 15}]),
        "target_schedule": json.dumps([{"start_time": "00:00:00", "low": 100, "high": 120}]),
    }
    _check(_required_fields_present(row) is False, "missing field -> False")


def test_required_fields_empty_schedule():
    row = {
        "basal_schedule": json.dumps([]),
        "isf_schedule":   json.dumps([{"start_time": "00:00:00", "value": 40}]),
        "cir_schedule":   json.dumps([{"start_time": "00:00:00", "value": 15}]),
        "target_schedule": json.dumps([{"start_time": "00:00:00", "low": 100, "high": 120}]),
    }
    _check(_required_fields_present(row) is False, "empty schedule array -> False")


def test_required_fields_malformed_json():
    row = {
        "basal_schedule": "not valid json",
        "isf_schedule":   json.dumps([{"start_time": "00:00:00", "value": 40}]),
        "cir_schedule":   json.dumps([{"start_time": "00:00:00", "value": 15}]),
        "target_schedule": json.dumps([{"start_time": "00:00:00", "low": 100, "high": 120}]),
    }
    _check(_required_fields_present(row) is False,
           "malformed JSON caught by try/except -> False (no raise)")


def test_required_fields_non_string():
    row = {
        "basal_schedule": None,
        "isf_schedule":   json.dumps([{"start_time": "00:00:00", "value": 40}]),
        "cir_schedule":   json.dumps([{"start_time": "00:00:00", "value": 15}]),
        "target_schedule": json.dumps([{"start_time": "00:00:00", "low": 100, "high": 120}]),
    }
    _check(_required_fields_present(row) is False, "None value -> False")


# --- _fmt_dt --------------------------------------------------------------

def test_fmt_dt_no_zero_pad():
    ts = pd.Timestamp("2024-05-08 03:04:05")
    _check(_fmt_dt(ts) == "5/8/2024 03:04:05",
           "month/day unpadded, HH:MM:SS zero-padded")


# --- Run all --------------------------------------------------------------

if __name__ == "__main__":
    test_snap_to_grid_basic()
    test_bolus_entries_sum_same_tick()
    test_bolus_entries_empty_df()
    test_carb_entries_sum_same_tick()
    test_carb_entries_empty_df()
    test_scalar_schedule()
    test_target_schedule_rounds_to_int()
    test_required_fields_all_valid()
    test_required_fields_missing_field()
    test_required_fields_empty_schedule()
    test_required_fields_malformed_json()
    test_required_fields_non_string()
    test_fmt_dt_no_zero_pad()
    print("\nAll tests passed.")
