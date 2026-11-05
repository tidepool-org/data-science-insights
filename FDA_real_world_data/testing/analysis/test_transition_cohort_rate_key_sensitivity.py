"""
Offline test for exploratory/transition_cohort_rate_key_sensitivity.py (no Spark).

Builds a cohort frame as load_transition_endpoints(diagnosis_groups=None)
returns it, a window composition frame as the script's SQL returns it, and a
Rev 02 cohort frame, and checks that:
- a window is rate-key dependent exactly when its first period falls short
  of 12 of 14 temp-basal days without the rate-key tag;
- Rev 02 membership and the same-window flag follow the vintage frame;
- the staged temp-basal day count is asserted against the re-read;
- the version state reads the Loop 3 version by period;
- every subset is tabulated, a subset under 5 users as a count only, and the
  user counts list every diagnosis group;
- the rule list the script withholds the rate key from is the staging's;
- analysis 8-1's full output set is written for the cohort without the
  rate-key-dependent windows, every group counted and the figures titled
  with the cohort.

pytest-free (the run_all_tests.py convention): assertions run from __main__.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_transition_cohort_rate_key_sensitivity.py
"""
import importlib.util
import os
import sys
import tempfile

import matplotlib
matplotlib.use("Agg")
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(PROJECT, "analysis"))
sys.path.insert(0, os.path.join(PROJECT, "data_staging"))

import export_loop_recommendations  # noqa: E402
from utils import data_loading  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "rate_key_sensitivity", os.path.join(PROJECT, "exploratory", "transition_cohort_rate_key_sensitivity.py"))
sens = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sens)

START = pd.Timestamp("2024-01-01").date()
OTHER_START = pd.Timestamp("2023-06-01").date()


def cohort(users, seed=0):
    """One row per user: (id, diagnosis group, seg1 start) with every endpoint's two periods."""
    rng = np.random.default_rng(seed)
    rows = []
    for user, group, start in users:
        row = {"_userId": user, "tb_to_ab_seg1_start": start, "diagnosis_group": group,
               "segment_rank_seg1": 1, "staged_temp_basal_days_seg1": 13}
        for _, c1, c2, _ in sens.analysis_8_1.ENDPOINTS:
            base = rng.uniform(5, 60)
            row[c1], row[c2] = base, base + rng.normal(3, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def composition(users, without_rate_key, v1=None, v2=None, plugin=0):
    rows = []
    for (user, _, start), days in zip(users, without_rate_key):
        rows.append({"_userId": user, "tb_to_ab_seg1_start": start,
                     "temp_basal_days_seg1": 13, "temp_basal_days_seg1_without_rate_key": days,
                     "autobolus_days_seg2": 14,
                     "automated_boluses_per_day_seg1": 0.0, "user_boluses_per_day_seg1": 4.0,
                     "temp_basals_per_day_seg1": 100.0, "rate_key_only_share_of_temp_basals_seg1": 1.0 - days / 14,
                     "automated_boluses_per_day_seg2": 30.0, "user_boluses_per_day_seg2": 3.0,
                     "plugin_doses_seg1": plugin, "version_int_seg1": v1, "version_int_seg2": v2,
                     "user_bolus_median_units_seg1": 2.5,
                     "rate_key_only_basals_key_1": 0, "rate_key_only_basals_key_0": 14 - days,
                     "rate_key_only_basals_key_other": 0, "rate_key_only_basals_key_absent": 0})
    return pd.DataFrame(rows)


# 8 primary users, 1 LADA, 1 unknown; the first three are in Rev 02 (two on the same window)
USERS = [(f"u{i}", "type1", START) for i in range(8)] + [("u8", "lada", START), ("u9", "unknown", START)]
REV02 = cohort([("u0", "type1", START), ("u1", "type1", START), ("u2", "type1", OTHER_START)])


def flagged(without_rate_key=None, **kw):
    without_rate_key = without_rate_key or [14, 13, 12, 11, 10, 0, 14, 14, 14, 14]
    return sens.flag_windows(cohort(USERS), composition(USERS, without_rate_key, **kw), REV02)


def test_rate_key_dependence_is_the_12_of_14_rule_without_the_tag():
    frame = flagged()
    dependent = frame.set_index("_userId")["rate_key_dependent"]
    assert dependent[["u0", "u1", "u2"]].tolist() == [False, False, False], "14, 13, 12 of 14 pass"
    assert dependent[["u3", "u4", "u5"]].tolist() == [True, True, True], "11, 10, 0 of 14 fail"
    assert sens.DEFAULT_MIN_PERIOD_DAYS == 12
    print("PASS: rate-key dependence is the 12 of 14 rule with the tag withheld")


def test_rev02_membership_and_same_window():
    frame = flagged().set_index("_userId")
    assert frame.loc[["u0", "u1", "u2"], "in_rev02"].all() and not frame.loc["u3":, "in_rev02"].any()
    assert frame.loc[["u0", "u1"], "same_window_as_rev02"].all()
    assert not frame.loc["u2", "same_window_as_rev02"], "u2's Rev 02 window starts elsewhere"
    print("PASS: Rev 02 membership and the same-window flag")


def test_staged_day_count_is_asserted():
    comp = composition(USERS, [14] * 10)
    comp.loc[0, "temp_basal_days_seg1"] = 9
    try:
        sens.flag_windows(cohort(USERS), comp, REV02)
    except AssertionError as exc:
        assert "differs from the staged" in str(exc)
    else:
        raise AssertionError("a re-read that differs from the staged count must raise")
    print("PASS: the re-read temp-basal day count is asserted against the staged one")


def test_version_state_and_route():
    v3 = sens.LOOP3_VERSION_INT + 2_003
    assert sens.version_state(v3, v3) == "Loop 3 in both periods"
    assert sens.version_state(None, v3) == "Loop 3 in the autobolus period only"
    assert sens.version_state(v3, None) == "Loop 3 in the temp-basal period only"
    assert sens.version_state(None, None) == "no version in the window"
    assert sens.version_state(2_002_000, v3) == "Loop 3 in the autobolus period only", "a Loop 2 version is not Loop 3"
    frame = flagged(plugin=0)
    assert set(frame["route_seg1"]) == {"apple_health only"}
    assert set(flagged(plugin=5)["route_seg1"]) == {"plugin"}
    print("PASS: version state by period and the upload route")


def test_every_subset_tabulated_and_thin_subsets_count_only():
    frame = flagged()
    primary = frame[frame["diagnosis_group"].isin(data_loading.PRIMARY_DIAGNOSIS_GROUPS)]
    assert len(primary) == 8
    parametric, nonparametric = sens.table_8_1a_by_subset(primary)
    labels = [label for label, _ in sens.SUBSETS]
    assert list(dict.fromkeys(parametric["Subset"])) == labels
    n_by_subset = parametric.groupby("Subset", sort=False)["N"].first()
    assert n_by_subset["primary cohort"] == 8
    assert n_by_subset["without rate-key-dependent windows"] == 5
    assert n_by_subset["rate-key-dependent windows"] == 3
    assert n_by_subset["in the Rev 02 cohort"] == 3 and n_by_subset["in the Rev 02 cohort, same window"] == 2
    assert n_by_subset["not in the Rev 02 cohort"] == 5
    assert n_by_subset["not in the Rev 02 cohort, without rate-key-dependent windows"] == 2
    drawn = parametric.groupby("Subset", sort=False)["Drawn"].first()
    assert drawn["primary cohort"] == 1 and drawn["rate-key-dependent windows"] == 0
    thin = parametric[parametric["Subset"] == "rate-key-dependent windows"]
    assert thin["Paired Diff 95% CI"].isna().all() and len(thin) == len(sens.analysis_8_1.ENDPOINTS)
    assert len(parametric) == len(nonparametric)
    profile = sens.dose_profile_by_subset(primary)
    assert list(profile["Subset"]) == labels and profile["N"].tolist() == [8, 5, 3, 3, 2, 5, 2]
    keys = sens.rate_key_only_basals_by_automatic_key(primary).set_index("Subset")
    assert keys.loc["primary cohort", "0"] == sum(14 - d for d in [14, 13, 12, 11, 10, 0, 14, 14])
    states = sens.version_state_by_subset(primary).set_index("Subset")
    assert states.loc["primary cohort", "no version in the window"] == 8
    print("PASS: every subset tabulated, thin subsets as counts only")


def test_user_counts_list_every_group_and_the_total():
    counts = sens.users_by_subset(flagged())
    assert set(counts["diagnosis_group"]) == {"type1", "lada", "unknown", "all"}
    total = counts[counts["diagnosis_group"] == "all"]
    assert total["n_users"].sum() == 10
    assert len(total) == 4, "four cells: rate-key dependence by Rev 02 membership"
    print("PASS: user counts by group, dependence, and membership")


def test_withheld_rule_list_is_the_stagings():
    assert sens.RATE_KEY_TAG in export_loop_recommendations.BASAL_TAGS
    assert set(sens.OTHER_BASAL_TAGS) == set(export_loop_recommendations.BASAL_TAGS) - {sens.RATE_KEY_TAG}
    assert sens.AUTOBOLUS_DAY_MIN_BOLUSES == export_loop_recommendations.AUTOBOLUS_DAY_MIN_BOLUSES
    # the re-read applies the day rule's bar on unrecognised boluses with the staging's constants
    assert sens.UNRECOGNISED_AUTOMATED_MIN_BOLUSES == export_loop_recommendations.UNRECOGNISED_AUTOMATED_MIN_BOLUSES
    assert sens.UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS == export_loop_recommendations.UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS
    source = open(os.path.join(PROJECT, "exploratory", "transition_cohort_rate_key_sensitivity.py"), encoding="utf-8").read()
    assert source.count("AND user_boluses_look_automated = 0") == 2, "both temp-basal day counts carry the bar"
    print("PASS: the rules kept are the staging's basal rules minus the rate key; the day rule's bar is applied")


def test_analysis_8_1_outputs_for_the_cohort_without_the_tag():
    # kept: u0, u1, u2, u6, u7 (type1), u8 (lada), u9 (unknown); dropped: u3, u4, u5
    with tempfile.TemporaryDirectory() as out:
        results = sens.write_analysis_8_1_without_rate_key(flagged(), out)
        written = sorted(os.listdir(out))
        assert written == [
            "figure_8_1a_paired_differences_by_diagnosis_group_i.png",
            "figure_8_1a_paired_differences_by_diagnosis_group_ii.png",
            "figure_8_1a_paired_differences_i.png",
            "figure_8_1a_paired_differences_ii.png",
            "figure_8_1a_paired_differences_type1_lada_i.png",
            "figure_8_1a_paired_differences_type1_lada_ii.png",
            "figure_8_1b_diff_histograms_i.png",
            "figure_8_1b_diff_histograms_ii.png",
            "figure_8_1c_stacked_bars.png",
            "figure_8_1c_stacked_bars_by_diagnosis_group.png",
            "figure_8_1c_stacked_bars_horizontal.png",
            "figure_8_1c_stacked_bars_type1_lada.png",
            "table_8_1_diagnosis_groups.csv",
            "table_8_1a_nonparametric.csv",
            "table_8_1a_nonparametric_by_diagnosis_group.csv",
            "table_8_1a_nonparametric_by_diagnosis_group_wide.csv",
            "table_8_1a_nonparametric_type1_lada.csv",
            "table_8_1a_nonparametric_type1_lada_wide.csv",
            "table_8_1a_parametric.csv",
            "table_8_1a_parametric_by_diagnosis_group.csv",
            "table_8_1a_parametric_by_diagnosis_group_wide.csv",
            "table_8_1a_parametric_type1_lada.csv",
            "table_8_1a_parametric_type1_lada_wide.csv",
        ], written
        groups = pd.read_csv(os.path.join(out, "table_8_1_diagnosis_groups.csv")).set_index("Diagnosis group")["N"]
        assert groups["type1"] == 5 and groups["lada"] == 1 and groups["unknown"] == 1 and groups["all"] == 7
        assert set(results["table_parametric"]["N"]) == {5}, "Table 8.1a is the primary groups only"
        by_series = pd.read_csv(os.path.join(out, "table_8_1a_parametric_by_diagnosis_group.csv"))
        lada = by_series[by_series["Series"] == "LADA"]
        assert set(lada["N"]) == {1} and set(lada["Drawn"]) == {0}, "one LADA user: a count, no statistic"
    assert sens.analysis_8_1.titled("Figure", sens.WITHOUT_RATE_KEY_COHORT_NOTE).endswith(sens.WITHOUT_RATE_KEY_COHORT_NOTE)
    assert sens.analysis_8_1.titled("Figure", "") == "Figure", "the production run's titles are unchanged"
    print("PASS: analysis 8-1's output set for the cohort without the tag, LADA counted apart")


if __name__ == "__main__":
    test_rate_key_dependence_is_the_12_of_14_rule_without_the_tag()
    test_rev02_membership_and_same_window()
    test_staged_day_count_is_asserted()
    test_version_state_and_route()
    test_every_subset_tabulated_and_thin_subsets_count_only()
    test_user_counts_list_every_group_and_the_total()
    test_withheld_rule_list_is_the_stagings()
    test_analysis_8_1_outputs_for_the_cohort_without_the_tag()
    print("test_transition_cohort_rate_key_sensitivity: all checks passed")
