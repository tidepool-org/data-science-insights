"""
Unit test for analysis_8-2 pandas helpers.

Covers `create_table_8_2a` (sample characteristics) and `_select_demo_users`
(Figure 8.2b user selection). These helpers are pure-pandas and do not
require Spark, so this runs as a plain pytest unit test.

Heavier end-to-end behavior (cohort/guardrail filters, staging propagation,
endpoint pivot) is exercised by the staging tests under data_staging/ and is
deferred to an integration test that will accompany the second-AB-segment
pipeline change (Table 8.2c).
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

# The driver filename has a hyphen, so it can't be imported by name.
_driver_path = os.path.join(
    _ANALYSIS_DIR,
    "analysis_8-2_glycemic_outcomes_during_preset_activation.py",
)
_spec = importlib.util.spec_from_file_location("analysis_8_2", _driver_path)
analysis_8_2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_2)


SEG1 = analysis_8_2.SEG1
SEG2 = analysis_8_2.SEG2


def _act(user, mode, override_time, duration_s=3600, preset="Exercise"):
    return {
        "_userId": user,
        "dosing_mode": mode,
        "override_time": override_time,
        "duration": duration_s,
        "overridePreset": preset,
    }


def test_table_8_2a_counts_users_with_both_periods():
    # user_a: 2 TB + 3 AB activations → counted
    # user_b: 1 TB + 0 AB → not counted (no AB)
    # user_c: 0 TB + 4 AB → not counted (no TB)
    rows = [
        _act("user_a", SEG1, "2025-01-03 10:00", duration_s=3600),
        _act("user_a", SEG1, "2025-01-05 10:00", duration_s=1800),
        _act("user_a", SEG2, "2025-01-17 10:00", duration_s=3600),
        _act("user_a", SEG2, "2025-01-19 10:00", duration_s=3600),
        _act("user_a", SEG2, "2025-01-21 10:00", duration_s=3600),
        _act("user_b", SEG1, "2025-02-03 10:00", duration_s=3600),
        _act("user_c", SEG2, "2025-03-01 10:00", duration_s=3600),
        _act("user_c", SEG2, "2025-03-03 10:00", duration_s=3600),
        _act("user_c", SEG2, "2025-03-05 10:00", duration_s=3600),
        _act("user_c", SEG2, "2025-03-07 10:00", duration_s=3600),
    ]
    activations = pd.DataFrame(rows)

    table = analysis_8_2.create_table_8_2a(activations)

    n_both = table.loc[
        table["Characteristic"] == "Users with preset use in both periods",
        "N or Median [IQR]",
    ].iloc[0]
    assert n_both == "1", f"expected 1 user in both periods, got {n_both}"


def test_table_8_2a_hours_uses_only_both_period_users():
    # Only user_a should contribute hours; user_b's TB-only activation must not.
    rows = [
        _act("user_a", SEG1, "2025-01-03 10:00", duration_s=3600),  # 1h + 2h tail = 3h
        _act("user_a", SEG2, "2025-01-17 10:00", duration_s=7200),  # 2h + 2h tail = 4h
        _act("user_b", SEG1, "2025-02-03 10:00", duration_s=3600),  # excluded
    ]
    activations = pd.DataFrame(rows)
    table = analysis_8_2.create_table_8_2a(activations)

    tb_hours = table.loc[
        table["Characteristic"] == "Total preset + tail hours analyzed, temp basal",
        "N or Median [IQR]",
    ].iloc[0]
    ab_hours = table.loc[
        table["Characteristic"] == "Total preset + tail hours analyzed, initial autobolus",
        "N or Median [IQR]",
    ].iloc[0]
    assert tb_hours == "3.0", f"TB hours: expected 3.0, got {tb_hours}"
    assert ab_hours == "4.0", f"AB hours: expected 4.0, got {ab_hours}"


def test_table_8_2a_marks_second_ab_segment_as_pending():
    activations = pd.DataFrame([
        _act("user_a", SEG1, "2025-01-03 10:00"),
        _act("user_a", SEG2, "2025-01-17 10:00"),
    ])
    table = analysis_8_2.create_table_8_2a(activations)

    second_ab = table.loc[
        table["Characteristic"] == "Total preset + tail hours analyzed, second autobolus",
        "N or Median [IQR]",
    ].iloc[0]
    assert second_ab == "N/A*", f"second AB row should be N/A*, got {second_ab}"
    assert "Table 8.2c" in table.attrs.get("footnote", ""), \
        "footnote should reference Table 8.2c deferral"


def test_select_demo_users_picks_preset_with_most_paired_users():
    rows = (
        # Exercise: 3 users paired
        [_act(f"u{i}", SEG1, "2025-01-03 10:00", preset="Exercise") for i in range(3)] +
        [_act(f"u{i}", SEG2, "2025-01-17 10:00", preset="Exercise") for i in range(3)] +
        # Sleep: 1 user paired
        [_act("u9", SEG1, "2025-01-04 22:00", preset="Sleep"),
         _act("u9", SEG2, "2025-01-18 22:00", preset="Sleep")]
    )
    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations, max_users=5)
    assert demo == "Exercise", f"demo preset: expected 'Exercise', got {demo!r}"
    assert set(users) == {"u0", "u1", "u2"}, f"users: {users}"


def test_select_demo_users_caps_at_max_users():
    # 6 users with paired Exercise activations; max_users=3 should cap.
    rows = []
    for i in range(6):
        rows.append(_act(f"u{i}", SEG1, "2025-01-03 10:00", preset="Exercise"))
        rows.append(_act(f"u{i}", SEG2, "2025-01-17 10:00", preset="Exercise"))
        # User i has (i+1) total paired activations so the ranking is deterministic.
        for _ in range(i):
            rows.append(_act(f"u{i}", SEG2, "2025-01-19 10:00", preset="Exercise"))

    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations, max_users=3)
    assert demo == "Exercise"
    # u5 has the most activations, then u4, u3.
    assert users == ["u5", "u4", "u3"], f"top-3 users: {users}"


def test_select_demo_users_returns_empty_when_no_pairing():
    rows = [
        _act("u0", SEG1, "2025-01-03 10:00", preset="Exercise"),
        _act("u1", SEG2, "2025-01-17 10:00", preset="Exercise"),
    ]
    activations = pd.DataFrame(rows)
    demo, users = analysis_8_2._select_demo_users(activations)
    assert demo == ""
    assert users == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
