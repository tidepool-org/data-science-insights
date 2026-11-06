"""Offline test of the loaders' predicate re-derivations (no Spark).

analysis/utils/data_loading.py re-derives each cohort table's predicate
columns from its criterion columns and refuses a table whose staged
predicates disagree (Gate C's loader half). Hand-built frames pin every
predicate, the best-window rule across a user's windows, and the refusal.

Run locally:
  python testing/analysis/test_data_loading_predicates.py
"""

import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "analysis"))

from utils import data_loading as dl  # noqa: E402


def _window(user, rank, start, **flags):
    row = {
        "_userId": user, "segment_rank": rank, "tb_to_ab_seg1_start": pd.Timestamp(start),
        "passes_loop_version_gate": True, "passes_age_gate": True,
        "has_endpoints_seg1": True, "has_endpoints_seg2": True,
        "passes_cgm_coverage_seg1": True, "passes_cgm_coverage_seg2": True,
        "no_guardrail_violation": True, "has_carb_data_seg1": True, "has_carb_data_seg2": True,
    }
    row.update(flags)
    return row


def test_transition_predicates_and_best_window():
    frame = pd.DataFrame([
        _window("a", 1, "2024-01-01", passes_cgm_coverage_seg2=False),   # rank 1 fails
        _window("a", 2, "2024-02-01"),                                    # rank 2 is the best
        _window("a", 3, "2024-03-01"),                                    # passes but ranks lower
        _window("b", 1, "2024-01-01", has_carb_data_seg2=False),          # in the cohort, no carbs
        _window("c", 1, "2024-01-01", passes_loop_version_gate=False),    # out: version
        _window("d", 1, "2024-01-01", no_guardrail_violation=False),      # out: guardrail
        _window("e", 2, "2024-01-01"),                                    # passes, but rank 2 only
    ])
    got = dl.transition_predicates(frame)
    assert got["passes_window_criteria"].tolist() == [False, True, True, True, False, False, True]
    assert got["is_best_window"].tolist() == [False, True, False, True, False, False, True]
    assert got["in_transition_cohort"].tolist() == [False, True, False, True, False, False, True]
    # the preset cohort needs rank 1, the version and age gates, and no violation
    assert got["in_preset_cohort"].tolist() == [True, False, False, True, False, False, False]
    assert got["in_carb_cohort"].tolist() == [False, True, False, False, False, False, True]
    staged = pd.concat([frame, got], axis=1)
    dl.assert_transition_predicates(staged)
    broken = staged.copy()
    broken.loc[0, "in_transition_cohort"] = True
    try:
        dl.assert_transition_predicates(broken)
    except AssertionError as error:
        assert "in_transition_cohort" in str(error)
        print("PASS: transition predicates, best window, and the refusal")
    else:
        raise AssertionError("a wrong staged predicate was accepted")


def test_activation_predicates():
    frame = pd.DataFrame([
        {"_userId": "a", "window_in_preset_cohort": True, "is_in_period_seg1": True, "is_in_period_seg2": False,
         "is_in_period_seg3": False, "is_starting_glucose_in_range": True, "is_valid_name_only_seg2": True,
         "is_valid_name_only_seg3": False},
        {"_userId": "a", "window_in_preset_cohort": True, "is_in_period_seg1": False, "is_in_period_seg2": False,
         "is_in_period_seg3": True, "is_starting_glucose_in_range": False, "is_valid_name_only_seg2": True,
         "is_valid_name_only_seg3": True},
        {"_userId": "b", "window_in_preset_cohort": False, "is_in_period_seg1": True, "is_in_period_seg2": False,
         "is_in_period_seg3": False, "is_starting_glucose_in_range": True, "is_valid_name_only_seg2": True,
         "is_valid_name_only_seg3": True},
    ])
    got = dl.activation_predicates(frame)
    assert got["in_activation_cohort"].tolist() == [True, True, False]
    assert got["in_duration_cohort"].tolist() == [True, False, False]
    assert got["in_paired_outcome_cohort_seg2"].tolist() == [True, False, False]
    assert got["in_paired_outcome_cohort_seg3"].tolist() == [False, False, False]
    dl.assert_activation_predicates(pd.concat([frame, got], axis=1))
    print("PASS: activation predicates")


def test_stable_and_durability_predicates():
    stable = pd.DataFrame([
        {"_userId": "a", "has_endpoints": True, "passes_cgm_coverage": True, "passes_age_gate": True, "is_jaeb_linked": True},
        {"_userId": "b", "has_endpoints": True, "passes_cgm_coverage": True, "passes_age_gate": False, "is_jaeb_linked": True},
        {"_userId": "c", "has_endpoints": False, "passes_cgm_coverage": False, "passes_age_gate": True, "is_jaeb_linked": False},
    ])
    got = dl.stable_predicates(stable)
    assert got["in_stable_cohort"].tolist() == [True, False, False]
    assert got["in_jaeb_cohort"].tolist() == [True, False, False]
    dl.assert_stable_predicates(pd.concat([stable, got], axis=1))

    durability = pd.DataFrame([
        {"_userId": "a", "is_adopted": True, "has_min_followup": True, "has_final_coverage": True,
         "is_age_eligible": True, "is_jaeb_linked": False},
        {"_userId": "b", "is_adopted": True, "has_min_followup": True, "has_final_coverage": None,
         "is_age_eligible": True, "is_jaeb_linked": True},
    ])
    got = dl.durability_predicates(durability)
    assert got["in_durability_cohort"].tolist() == [True, False], "a missing flag reads as False"
    assert got["in_jaeb_cohort"].tolist() == [False, False]
    dl.assert_durability_predicates(pd.concat([durability, got], axis=1))
    print("PASS: stable and durability predicates")


def test_interactive_review_predicates():
    days = pd.DataFrame([
        {"_userId": "a", "is_ab_day": True, "is_version_eligible": True, "is_age_eligible": True, "is_coverage_ok": True},
        {"_userId": "a", "is_ab_day": True, "is_version_eligible": True, "is_age_eligible": True, "is_coverage_ok": False},
        {"_userId": "b", "is_ab_day": False, "is_version_eligible": True, "is_age_eligible": True, "is_coverage_ok": True},
        {"_userId": "c", "is_ab_day": True, "is_version_eligible": False, "is_age_eligible": None, "is_coverage_ok": True},
    ])
    got = dl.ab_day_predicates(days)
    assert got["is_eligible_ab_day"].tolist() == [True, True, False, False]
    assert got["is_outcome_day"].tolist() == [True, False, False, False]
    dl.assert_ab_day_predicates(pd.concat([days, got], axis=1))

    groups = pd.DataFrame([
        {"_userId": "a", "n_qualifying_activations": 0, "ever_p": False, "ever_m": False},
        {"_userId": "b", "n_qualifying_activations": 2, "ever_p": False, "ever_m": False},
        {"_userId": "c", "n_qualifying_activations": 1, "ever_p": True, "ever_m": False},
        {"_userId": "d", "n_qualifying_activations": 1, "ever_p": False, "ever_m": True},
        {"_userId": "e", "n_qualifying_activations": 3, "ever_p": True, "ever_m": True},
    ])
    got = dl.guardrail_group_predicates(groups)
    assert got["guardrail_group"].tolist() == ["never_preset", "compliant", "p_only", "m_only", "both"]
    dl.assert_guardrail_group_predicates(pd.concat([groups, got], axis=1))
    wrong = pd.concat([groups, got], axis=1)
    wrong.loc[1, "guardrail_group"] = "p_only"
    try:
        dl.assert_guardrail_group_predicates(wrong)
    except AssertionError:
        pass
    else:
        raise AssertionError("a wrong guardrail group was accepted")

    flags = pd.DataFrame([
        {"_userId": "a", "is_version_eligible": True, "is_after_first_ab": True, "is_all_days_ab": True},
        {"_userId": "a", "is_version_eligible": True, "is_after_first_ab": True, "is_all_days_ab": False},
        {"_userId": "a", "is_version_eligible": False, "is_after_first_ab": True, "is_all_days_ab": True},
        {"_userId": "a", "is_version_eligible": True, "is_after_first_ab": None, "is_all_days_ab": True},
    ])
    got = dl.guardrail_flag_predicates(flags)
    assert got["is_qualifying"].tolist() == [True, True, False, False]
    assert got["in_characterization_cohort"].tolist() == [True, False, False, False]
    dl.assert_guardrail_flag_predicates(pd.concat([flags, got], axis=1))

    extreme = pd.DataFrame([
        {"_userId": "a", "any_extreme": True, "has_cgm": True, "passes_cgm_coverage": True},
        {"_userId": "a", "any_extreme": True, "has_cgm": True, "passes_cgm_coverage": False},
        {"_userId": "a", "any_extreme": True, "has_cgm": False, "passes_cgm_coverage": False},
    ])
    got = dl.extreme_preset_predicates(extreme)
    assert got["in_extreme_cohort"].tolist() == [True, False, False]
    dl.assert_extreme_preset_predicates(pd.concat([extreme, got], axis=1))

    candidates = pd.DataFrame([
        {"_userId": "a", "in_c1": True, "in_c2": True, "is_phase_tb": True, "is_phase_ab": False},
        {"_userId": "a", "in_c1": True, "in_c2": False, "is_phase_tb": False, "is_phase_ab": True},
        {"_userId": "a", "in_c1": False, "in_c2": False, "is_phase_tb": None, "is_phase_ab": None},
    ])
    got = dl.dose_response_candidate_predicates(candidates)
    assert got["in_candidate_pool"].tolist() == [True, True, False]
    assert got["in_series_C1a"].tolist() == [True, False, False]
    assert got["in_series_C1b"].tolist() == [False, True, False]
    assert got["in_series_C2"].tolist() == [True, False, False]
    dl.assert_dose_response_candidate_predicates(pd.concat([candidates, got], axis=1))

    episodes = pd.DataFrame([
        {"_userId": "a", "pass_single_day": True, "pass_isolation": True, "pass_duration": True,
         "pass_start_bg": True, "pass_coverage": True},
        {"_userId": "a", "pass_single_day": True, "pass_isolation": True, "pass_duration": True,
         "pass_start_bg": False, "pass_coverage": True},
        {"_userId": "a", "pass_single_day": False, "pass_isolation": True, "pass_duration": True,
         "pass_start_bg": True, "pass_coverage": True},
    ])
    got = dl.dose_response_episode_predicates(episodes)
    assert got["is_survivor"].tolist() == [True, True, False]
    assert got["is_retained"].tolist() == [True, False, False]
    dl.assert_dose_response_episode_predicates(pd.concat([episodes, got], axis=1))
    print("PASS: the interactive-review predicates")


def test_numeric_coercion_keeps_dates_labels_and_timestamps():
    """The Databricks suite of 2026-11-06: Spark dates, timestamps cast to
    strings, and labels reach pandas as object columns and must keep their
    values; only columns that are numbers throughout are converted."""
    import datetime
    from decimal import Decimal
    frame = pd.DataFrame({
        "tb_to_ab_seg1_start": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
        "t0": ["2024-01-03 10:00:00", "2024-01-20 10:00:00"],
        "guardrail_group": ["compliant", "never_preset"],
        "cbg_count": [Decimal("2822"), None],
        "violation_count": ["2", "0"],
        "needs_fraction": [0.7, None],
    }).astype(object)
    out = dl._coerce_numeric(frame.copy())
    assert out["tb_to_ab_seg1_start"].tolist() == frame["tb_to_ab_seg1_start"].tolist(), "dates kept"
    assert out["t0"].tolist() == frame["t0"].tolist(), "timestamp strings kept"
    assert out["guardrail_group"].tolist() == frame["guardrail_group"].tolist(), "labels kept"
    assert out["cbg_count"].tolist()[0] == 2822 and pd.isna(out["cbg_count"].tolist()[1]), "decimals converted, a null stays null"
    assert out["violation_count"].tolist() == [2, 0], "numeric strings converted"
    assert out["needs_fraction"].dtype.kind == "f"
    print("PASS: numeric coercion keeps dates, timestamps, and labels")


def test_constants_are_the_staging_constants():
    sys.path.insert(0, os.path.join(_HERE, "..", "..", "data_staging"))
    import cohort_constants as c
    assert dl.MIN_CBG_COUNT is c.MIN_CBG_COUNT or dl.MIN_CBG_COUNT == c.MIN_CBG_COUNT
    assert dl.DIAGNOSIS_GROUPS == c.DIAGNOSIS_GROUPS
    assert dl.PRIMARY_DIAGNOSIS_GROUPS == c.PRIMARY_DIAGNOSIS_GROUPS
    assert dl.diagnosis_group is c.diagnosis_group
    assert not hasattr(dl, "COHORT_WHERE") and not hasattr(dl, "load_type1_user_ids"), (
        "the loader applies no gate of its own since 2026-11-05"
    )
    print("PASS: the loader re-exports the staging constants")


if __name__ == "__main__":
    test_transition_predicates_and_best_window()
    test_activation_predicates()
    test_stable_and_durability_predicates()
    test_interactive_review_predicates()
    test_numeric_coercion_keeps_dates_labels_and_timestamps()
    test_constants_are_the_staging_constants()
    print("\nAll loader predicate tests passed.")
