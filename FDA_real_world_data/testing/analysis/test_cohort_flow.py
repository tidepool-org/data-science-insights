"""Offline test of analysis/utils/cohort_flow.py (no Spark).

A hand-built transition cohort frame with four users across diagnosis groups
and every criterion exercised; the stage counts are computed by hand and
asserted, as are the wide layout, the side-by-side vintage comparison, the
monotone check, and that each population's stage list runs on a frame with
its columns.

Run locally:
  python testing/analysis/test_cohort_flow.py
"""

import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "analysis"))

from utils import cohort_flow as flow  # noqa: E402


def _window(user, rank, group, version=True, age=True, ep1=True, ep2=True, cov1=True, cov2=True,
            clean=True, carb1=True, carb2=True, best=None):
    passes = version and age and ep1 and ep2 and cov1 and cov2 and clean
    if best is None:
        best = passes
    return {
        "_userId": user, "tb_to_ab_seg1_start": pd.Timestamp("2024-01-01") + pd.Timedelta(days=rank),
        "segment_rank": rank, "diagnosis_group": group,
        "is_primary_diagnosis": group in ("type1", "type1 with LADA label"),
        "passes_loop_version_gate": version, "passes_age_gate": age,
        "has_endpoints_seg1": ep1, "has_endpoints_seg2": ep2,
        "passes_cgm_coverage_seg1": cov1, "passes_cgm_coverage_seg2": cov2,
        "no_guardrail_violation": clean, "has_carb_data_seg1": carb1, "has_carb_data_seg2": carb2,
        "passes_window_criteria": passes, "is_best_window": best,
        "in_transition_cohort": passes and best,
    }


def transition_frame():
    return pd.DataFrame([
        # a type-1 user whose rank-1 window fails coverage in the second
        # period and whose rank-2 window passes: the best is rank 2
        _window("a", 1, "type1", cov2=False, best=False),
        _window("a", 2, "type1"),
        # a LADA user with one clean window
        _window("b", 1, "lada"),
        # a type-2 user failing the version gate (endpoints present)
        _window("c", 1, "type2", version=False),
        # an unknown-group user with no carbohydrate data in the first period
        _window("d", 1, "unknown", carb1=False),
    ])


def test_transition_counts_by_hand():
    long = flow.count_flow(transition_frame(), flow.transition_stages(), unit="windows")
    wide_users = flow.widen_flow(long).set_index("stage")
    wide_windows = flow.widen_flow(long, value="n_windows").set_index("stage")
    # every window the search kept
    assert wide_windows.loc["Valid TB→AB transition segment", "all"] == 5
    assert wide_users.loc["Valid TB→AB transition segment", "all"] == 4
    assert wide_users.loc["Valid TB→AB transition segment", "primary"] == 1
    assert wide_users.loc["Valid TB→AB transition segment", "lada"] == 1
    # the version gate removes user c's window
    assert wide_windows.loc["Analysis cohort gate", "all"] == 4
    assert wide_users.loc["Analysis cohort gate", "type2"] == 0
    # user a's rank-1 window still counts at coverage (one period survives)
    assert wide_windows.loc["CGM coverage filter", "all"] == 4
    # paired periods drop it
    assert wide_windows.loc["Paired TB/AB halves", "all"] == 3
    assert wide_users.loc["Paired TB/AB halves", "all"] == 3
    # the final cohort is one window per user: a (rank 2), b, d
    assert wide_windows.loc["Final transition cohort", "all"] == 3
    assert wide_users.loc["Final transition cohort", "primary"] == 1
    assert wide_users.loc["Final transition cohort", "lada"] == 1
    assert wide_users.loc["Final transition cohort", "unknown"] == 1
    flow.assert_monotone(long)
    print("PASS: transition counts")


def test_primary_gate_narrows_all_to_primary():
    long = flow.count_flow(transition_frame(), flow.transition_stages(primary_gate=True), unit="windows")
    wide = flow.widen_flow(long).set_index("stage")
    assert "Type 1 diabetes" in wide.index
    assert wide.loc["Type 1 diabetes", "all"] == wide.loc["Type 1 diabetes", "primary"] == 1
    assert wide.loc["Final transition cohort", "all"] == 1
    print("PASS: primary gate")


def test_carbohydrate_stage():
    long = flow.count_flow(transition_frame(), flow.carbohydrate_stages(), unit="windows")
    wide = flow.widen_flow(long).set_index("stage")
    assert wide.loc["Final transition cohort", "all"] == 3
    assert wide.loc["Carbohydrate data in both periods", "all"] == 2, "user d has no first-period carbs"
    print("PASS: carbohydrate stage")


def test_side_by_side_difference():
    current = flow.count_flow(transition_frame(), flow.transition_stages(), unit="windows")
    issued_frame = transition_frame().iloc[:3]   # the issued vintage had users a and b only
    issued = flow.count_flow(issued_frame, flow.transition_stages(), unit="windows")
    table = flow.side_by_side(current, issued, group="all")
    final = table.set_index("stage").loc["Final transition cohort"]
    assert final["n_users_current"] == 3 and final["n_users_issued"] == 2 and final["difference"] == 1
    blank = flow.side_by_side(current, None, group="all")
    assert blank["n_users_issued"].isna().all() and blank["difference"].isna().all()
    print("PASS: side by side")


def test_monotone_check_refuses_a_rise():
    # count_flow is cumulative, so its own output always narrows; the check
    # guards a long frame read back from a file or assembled by hand.
    long = flow.count_flow(transition_frame(), flow.transition_stages(), unit="windows")
    flow.assert_monotone(long)
    risen = long.copy()
    last = risen.index[(risen["stage"] == "Final transition cohort") & (risen["group"] == "all")]
    risen.loc[last, "n_users"] = 99
    try:
        flow.assert_monotone(risen)
    except AssertionError:
        print("PASS: a rise down the flow is refused")
    else:
        raise AssertionError("a rising count passed the monotone check")


def test_other_populations_run_on_their_columns():
    activations = pd.DataFrame([
        {"_userId": "a", "diagnosis_group": "type1", "window_in_preset_cohort": True,
         "is_in_period_seg1": True, "is_in_period_seg2": False, "is_in_period_seg3": False,
         "is_starting_glucose_in_range": True, "is_valid_name_only_seg2": True, "is_valid_name_only_seg3": False},
        {"_userId": "a", "diagnosis_group": "type1", "window_in_preset_cohort": True,
         "is_in_period_seg1": False, "is_in_period_seg2": False, "is_in_period_seg3": True,
         "is_starting_glucose_in_range": True, "is_valid_name_only_seg2": True, "is_valid_name_only_seg3": False},
        {"_userId": "b", "diagnosis_group": "lada", "window_in_preset_cohort": False,
         "is_in_period_seg1": True, "is_in_period_seg2": False, "is_in_period_seg3": False,
         "is_starting_glucose_in_range": True, "is_valid_name_only_seg2": True, "is_valid_name_only_seg3": True},
    ])
    long = flow.count_flow(activations, flow.activation_stages("seg2"), unit="activations")
    wide = flow.widen_flow(long, value="n_activations").set_index("stage")
    assert wide.loc["Preset activations in rank-1 windows", "all"] == 3
    assert wide.loc["Window in the preset cohort", "all"] == 2
    assert wide.loc["Activation in the compared periods", "all"] == 1
    assert wide.loc["Preset valid by name in both periods", "all"] == 1
    long3 = flow.count_flow(activations, flow.activation_stages("seg3"), unit="activations")
    assert flow.widen_flow(long3, value="n_activations").set_index("stage").loc["Preset valid by name in both periods", "all"] == 0

    stable = pd.DataFrame([
        {"_userId": "a", "diagnosis_group": "type1", "has_endpoints": True, "passes_cgm_coverage": True,
         "passes_age_gate": True, "no_guardrail_violation": False, "is_jaeb_linked": True},
        {"_userId": "b", "diagnosis_group": "type1", "has_endpoints": True, "passes_cgm_coverage": False,
         "passes_age_gate": True, "no_guardrail_violation": True, "is_jaeb_linked": True},
    ])
    wide = flow.widen_flow(flow.count_flow(stable, flow.stable_stages(), unit="segments")).set_index("stage")
    assert wide.loc["JAEB study linkage", "all"] == 1
    wide_g = flow.widen_flow(flow.count_flow(stable, flow.stable_stages(with_guardrail=True), unit="segments")).set_index("stage")
    assert wide_g.loc["JAEB study linkage", "all"] == 0, "the guardrail check would remove user a"

    durability = pd.DataFrame([
        {"_userId": "a", "diagnosis_group": "type1", "is_adopted": True, "has_min_followup": True,
         "has_final_coverage": True, "is_age_eligible": True, "is_jaeb_linked": False},
        {"_userId": "b", "diagnosis_group": "type2", "is_adopted": True, "has_min_followup": False,
         "has_final_coverage": True, "is_age_eligible": True, "is_jaeb_linked": True},
    ])
    wide = flow.widen_flow(flow.count_flow(durability, flow.durability_stages(), unit="users")).set_index("stage")
    assert wide.loc["Minimum follow-up", "all"] == 1 and wide.loc["JAEB study linkage", "all"] == 0
    print("PASS: activation, stable, durability stages")


if __name__ == "__main__":
    test_transition_counts_by_hand()
    test_primary_gate_narrows_all_to_primary()
    test_carbohydrate_stage()
    test_side_by_side_difference()
    test_monotone_check_refuses_a_rise()
    test_other_populations_run_on_their_columns()
    print("\nAll cohort_flow offline tests passed.")
