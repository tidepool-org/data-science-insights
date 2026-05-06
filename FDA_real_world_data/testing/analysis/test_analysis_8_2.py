"""
Unit test for analysis_8-2 pandas helpers.

Covers `create_table_8_2a` (sample characteristics across all three segments)
and `_select_demo_users` (Figure 8.2b user selection). These helpers are
pure-pandas and don't require Spark, so this runs as a plain pytest unit test.

Heavier behavior — `aggregate_override_endpoints` (per-activation aggregation,
hypo rate computation), cohort/guardrail filtering, staging propagation — is
exercised by the staging tests under `data_staging/` and is best covered by a
synthetic-BDDP integration test.
"""

import importlib.util
import os
import sys

import pandas as pd
import pytest


_HERE = os.path.dirname(os.path.abspath(__file__))
_ANALYSIS_DIR = os.path.join(_HERE, "..", "..", "analysis")
sys.path.insert(0, _ANALYSIS_DIR)
sys.path.insert(0, os.path.join(_HERE, "..", ".."))

_driver_path = os.path.join(
    _ANALYSIS_DIR,
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_2", _driver_path)
analysis_8_2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_2)


TB  = analysis_8_2.TB
AB1 = analysis_8_2.AB1
AB2 = analysis_8_2.AB2


def _act(user, segment, override_time, duration_s=3600, preset="Exercise",
         valid_seg2=True, valid_seg3=True):
    return {
        "_userId": user,
        "segment": segment,
        "override_time": override_time,
        "duration": duration_s,
        "overridePreset": preset,
        "is_valid_name_only_seg2": valid_seg2,
        "is_valid_name_only_seg3": valid_seg3,
    }


def test_table_8_2a_counts_users_in_both_periods_across_seg2_or_seg3():
    # user_a: TB + AB1 + AB2 → in both periods (counted)
    # user_b: TB + AB2 only → in both periods (TB ∩ any AB) (counted)
    # user_c: TB only → not in both
    # user_d: AB1 + AB2 only (no TB) → not in both
    rows = [
        _act("user_a", TB,  "2025-01-03 10:00"),
        _act("user_a", AB1, "2025-01-17 10:00"),
        _act("user_a", AB2, "2025-01-25 10:00"),
        _act("user_b", TB,  "2025-02-03 10:00"),
        _act("user_b", AB2, "2025-02-25 10:00"),
        _act("user_c", TB,  "2025-03-03 10:00"),
        _act("user_d", AB1, "2025-04-17 10:00"),
        _act("user_d", AB2, "2025-04-25 10:00"),
    ]
    table = analysis_8_2.create_table_8_2a(pd.DataFrame(rows))
    n_both = table.loc[
        table["Characteristic"] == "Users with preset use in both periods",
        "N or Median [IQR]",
    ].iloc[0]
    assert n_both == "2", f"expected 2 users in both periods, got {n_both}"


def test_table_8_2a_hours_split_by_segment_for_cohort_users():
    # user_a is in both periods. user_b only in TB (excluded from hours).
    rows = [
        _act("user_a", TB,  "2025-01-03 10:00", duration_s=3600),  # 3h
        _act("user_a", AB1, "2025-01-17 10:00", duration_s=7200),  # 4h
        _act("user_a", AB2, "2025-01-25 10:00", duration_s=1800),  # 2.5h
        _act("user_b", TB,  "2025-02-03 10:00", duration_s=3600),  # excluded
    ]
    table = analysis_8_2.create_table_8_2a(pd.DataFrame(rows))

    def _hours(label):
        return table.loc[
            table["Characteristic"] == label, "N or Median [IQR]"
        ].iloc[0]

    assert _hours("Total preset + tail hours analyzed, temp basal") == "3.0"
    assert _hours("Total preset + tail hours analyzed, initial autobolus") == "4.0"
    assert _hours("Total preset + tail hours analyzed, second autobolus") == "2.5"


def test_table_8_2a_handles_missing_segments():
    # Only TB rows — no AB activations at all in fixture. No errors should raise;
    # AB rows show "—" and the both-periods count is 0.
    rows = [_act("user_a", TB, "2025-01-03 10:00")]
    table = analysis_8_2.create_table_8_2a(pd.DataFrame(rows))
    n_both = table.loc[
        table["Characteristic"] == "Users with preset use in both periods",
        "N or Median [IQR]",
    ].iloc[0]
    assert n_both == "0"
    ab1_count = table.loc[
        table["Characteristic"] == "Preset activations per user, initial autobolus period",
        "N or Median [IQR]",
    ].iloc[0]
    assert ab1_count == "—"


def test_select_demo_users_picks_preset_with_most_paired_seg2_users():
    rows = (
        # Exercise: 3 users with TB + AB1 (and is_valid_name_only_seg2=True)
        [_act(f"u{i}", TB,  "2025-01-03 10:00", preset="Exercise") for i in range(3)] +
        [_act(f"u{i}", AB1, "2025-01-17 10:00", preset="Exercise") for i in range(3)] +
        # Sleep: 1 user paired (no, with valid_seg2)
        [_act("u9", TB,  "2025-01-04 22:00", preset="Sleep"),
         _act("u9", AB1, "2025-01-18 22:00", preset="Sleep")]
    )
    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations, max_users=5)
    assert demo == "Exercise", f"demo preset: expected 'Exercise', got {demo!r}"
    assert set(users) == {"u0", "u1", "u2"}


def test_select_demo_users_ignores_unvalidated_pairs():
    # Same shape as above but is_valid_name_only_seg2 is False; should yield
    # nothing.
    rows = [
        _act("u0", TB,  "2025-01-03 10:00", preset="Exercise", valid_seg2=False),
        _act("u0", AB1, "2025-01-17 10:00", preset="Exercise", valid_seg2=False),
    ]
    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations)
    assert demo == ""
    assert users == []


def test_select_demo_users_caps_at_max_users():
    rows = []
    for i in range(6):
        rows.append(_act(f"u{i}", TB,  "2025-01-03 10:00", preset="Exercise"))
        rows.append(_act(f"u{i}", AB1, "2025-01-17 10:00", preset="Exercise"))
        # User i has (i+1) total paired activations.
        for _ in range(i):
            rows.append(_act(f"u{i}", AB1, "2025-01-19 10:00", preset="Exercise"))
    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations, max_users=3)
    assert demo == "Exercise"
    assert users == ["u5", "u4", "u3"]


def test_aggregate_override_endpoints_averages_endpoints_and_pools_hypo_rate():
    """Per-activation endpoints averaged unweighted; hypo rate is total events
    over total exposure hours. Verifies that the staging-layer per-activation
    grain produces the right aggregate when collapsed at the analysis layer."""
    from utils.data_loading import aggregate_override_endpoints  # noqa: E402

    # User A, Exercise preset, identical params, two TB activations and three
    # AB activations:
    # TB:  TIRs 60% and 80% (mean 70%); 1 hypo over 3+5 = 8 hours.
    # AB1: TIRs 70%, 75%, 80% (mean 75%); 0 hypo over 4+4+4 = 12 hours.
    rows = []
    for tir, hypo, hours in [(60.0, 1, 3.0), (80.0, 0, 5.0)]:
        rows.append({
            "_userId": "u0", "segment": TB, "override_time": pd.NaT,
            "overridePreset": "Exercise", "brsf": 0.5, "btl": -1, "bth": -1, "crsf": 1, "issf": 1.5,
            "tbr_very_low": 0, "tbr": 0, "tir": tir, "tar": 100 - tir, "tar_very_high": 0,
            "mean_glucose": 130, "cv": 30,
            "hypo_events": hypo, "window_hours": hours,
            "is_valid_name_only_seg2": True, "is_valid_name_only_seg3": True,
        })
    for tir, hypo, hours in [(70.0, 0, 4.0), (75.0, 0, 4.0), (80.0, 0, 4.0)]:
        rows.append({
            "_userId": "u0", "segment": AB1, "override_time": pd.NaT,
            "overridePreset": "Exercise", "brsf": 0.5, "btl": -1, "bth": -1, "crsf": 1, "issf": 1.5,
            "tbr_very_low": 0, "tbr": 0, "tir": tir, "tar": 100 - tir, "tar_very_high": 0,
            "mean_glucose": 130, "cv": 30,
            "hypo_events": hypo, "window_hours": hours,
            "is_valid_name_only_seg2": True, "is_valid_name_only_seg3": True,
        })

    df = pd.DataFrame(rows)
    wide = aggregate_override_endpoints(df, ab_segment=AB1, grain="name")
    assert len(wide) == 1
    row = wide.iloc[0]
    assert row["tir_seg1"] == pytest.approx(70.0)
    assert row["tir_seg2"] == pytest.approx(75.0)
    assert row["hypo_rate_seg1"] == pytest.approx(1 / 8)
    assert row["hypo_rate_seg2"] == pytest.approx(0)
    assert row["activation_count_seg1"] == 2
    assert row["activation_count_seg2"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
