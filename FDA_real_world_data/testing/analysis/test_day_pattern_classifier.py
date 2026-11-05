"""
Offline test for exploratory/day_pattern_classifier.py (no Spark, no scikit-learn needed).

Builds synthetic classified days with two dosing populations (hand dosing: a
handful of unit-sized boluses with random gaps; autobolus: dozens of
sub-unit boluses on the five-minute cycle) and checks that:
- only plugin-route Loop 3 days of either class are labelled, neither-class
  and no-version days are not;
- the per-label cap and the by-user split hold (no user in both sets);
- the logistic model separates the populations on held-out users;
- the two-feature boundary puts the undecided count between the two modes;
- the rate-key-alone and hand-bar tables have the expected shape and
  agree with the bar on clear-cut days;
- the features SQL reads the day table's bar flag and the staging's rule
  lists, and computes the bolus gaps with LAG.

pytest-free (the run_all_tests.py convention): assertions run from __main__.

Run locally:
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        FDA_real_world_data/testing/analysis/test_day_pattern_classifier.py
"""
import importlib.util
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(PROJECT, "data_staging"))

import export_loop_recommendations as projection  # noqa: E402

_spec = importlib.util.spec_from_file_location("day_pattern_classifier",
                                               os.path.join(PROJECT, "exploratory", "day_pattern_classifier.py"))
dpc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dpc)


def synthetic_days(n_users=60, days_per_user=20, seed=1) -> pd.DataFrame:
    """Half the users dose by hand, half run the autobolus strategy; every
    day is a plugin-route Loop 3 day unless marked otherwise."""
    rng = np.random.default_rng(seed)
    rows = []
    for u in range(n_users):
        autobolus = u % 2 == 1
        for d in range(days_per_user):
            if autobolus:
                count = int(rng.integers(40, 90)); median = float(rng.uniform(0.1, 0.3))
                gap, on_cycle = float(rng.integers(5, 25)), float(rng.uniform(0.7, 1.0))
            else:
                count = int(rng.integers(2, 9)); median = float(rng.uniform(1.0, 4.0))
                gap, on_cycle = float(rng.integers(90, 400)), float(rng.uniform(0.0, 0.3))
            rows.append({"_userId": f"u{u}", "day": pd.Timestamp("2024-01-01") + pd.Timedelta(days=d),
                         "yr": 2024, "version_int": 3_002_000,
                         "is_autobolus_day": int(autobolus), "is_temp_basal_day": int(not autobolus),
                         "unrecognised_boluses_look_automated": 0, "rate_key_alone": 0,
                         "bolus_count": count, "basal_count": int(rng.integers(60, 280)),
                         "bolus_units_min": median * 0.5, "bolus_units_median": median, "bolus_units_max": median * 2,
                         "bolus_units_total": count * median,
                         "share_boluses_under_half_unit": 0.9 if autobolus else 0.1,
                         "bolus_gap_median_minutes": gap, "share_gaps_on_cycle": on_cycle})
    return pd.DataFrame(rows)


def test_labels_are_loop3_plugin_days_of_either_class():
    frame = synthetic_days()
    frame.loc[frame.index[:5], "version_int"] = np.nan                     # no version: not labelled
    frame.loc[frame.index[5:10], "version_int"] = 2_002_000                # Loop 2: not labelled
    frame.loc[frame.index[10:15], ["is_autobolus_day", "is_temp_basal_day"]] = 0  # neither: not labelled
    labelled = dpc.label_days(frame)
    assert len(labelled) == len(frame) - 15
    assert set(labelled[dpc.LABEL]) == {0, 1}
    assert (labelled[dpc.LABEL] == labelled["is_autobolus_day"]).all()
    capped = dpc.cap_per_label(labelled, cap=100)
    assert capped[dpc.LABEL].value_counts().max() == 100
    train, held = dpc.split_by_user(labelled, holdout_share=0.25)
    assert not set(train["_userId"]) & set(held["_userId"]), "no user in both sets"
    assert len(train) + len(held) == len(labelled)
    print("PASS: labels are Loop 3 plugin days of either class; cap and by-user split hold")


def test_logistic_separates_the_populations_on_held_out_users():
    train, held = dpc.split_by_user(dpc.label_days(synthetic_days()))
    model = dpc.Logistic(dpc.FEATURES).fit(train)
    table = dpc.holdout_table({"logistic": model}, held)
    overall = table[table["yr"] == "all"].iloc[0]
    assert overall["accuracy"] >= 0.95, overall.to_dict()
    assert overall["days"] == len(held) and overall["users"] == held["_userId"].nunique()
    coefficients = model.coefficients()
    assert list(coefficients["feature"]) == ["constant"] + dpc.FEATURES
    print("PASS: the logistic model separates the populations on held-out users")


def test_boundary_sits_between_the_modes():
    train, _ = dpc.split_by_user(dpc.label_days(synthetic_days()))
    boundary = dpc.boundary_table(train, sizes=(0.15, 0.5))
    assert list(boundary["bolus_units_median"]) == [0.15, 0.5]
    at_small = boundary.loc[boundary["bolus_units_median"] == 0.15, "boluses_per_day_undecided"].iloc[0]
    assert 8 < at_small < 40, f"undecided count {at_small} should lie between a handful and dozens"
    assert set(boundary["rule_bar_boluses"]) == {projection.UNRECOGNISED_AUTOMATED_MIN_BOLUSES}
    print("PASS: the two-feature boundary lies between the hand-dosing and autobolus modes")


def test_rate_key_alone_and_hand_bar_tables():
    frame = synthetic_days()
    train, held = dpc.split_by_user(dpc.label_days(frame))
    model = dpc.Logistic(dpc.FEATURES).fit(train)
    # the rate-key-alone days: the same two populations, the bar flagging the autobolus-like ones
    apply = synthetic_days(n_users=20, seed=7)
    apply["rate_key_alone"] = 1
    apply["unrecognised_boluses_look_automated"] = apply["is_autobolus_day"]
    apply["is_autobolus_day"] = 0
    apply["is_temp_basal_day"] = 1
    table = dpc.rate_key_alone_table({"logistic": model}, apply)
    assert list(table["yr"]) == ["all", 2024] and table.iloc[0]["days"] == len(apply)
    assert table.iloc[0]["agreement_logistic_with_bar"] >= 0.95, table.iloc[0].to_dict()
    assert abs(table.iloc[0]["share_logistic_calls_autobolus"] - 0.5) < 0.1
    agreement = dpc.hand_bar_agreement({"logistic": model}, held, apply)
    assert list(agreement["days"]) == ["held-out labelled days", "rate-key-alone days"]
    totals = agreement[["bar_on_model_autobolus", "bar_on_model_temp_basal", "bar_off_model_autobolus",
                        "bar_off_model_temp_basal"]].sum(axis=1)
    assert list(totals) == [len(held), len(apply)]
    print("PASS: the rate-key-alone and hand-bar tables have the expected shape and agree with the bar")


def test_features_sql_reads_the_flag_and_the_rule_lists():
    sql = dpc.features_sql()
    assert "unrecognised_boluses_look_automated" in sql and "LAG(b.minute_key)" in sql
    for tag in projection.BOLUS_TAGS:
        assert f"boluses_{tag}" in sql, tag
    for tag in projection.BASAL_TAGS:
        if tag != "health_rate_loop_app":
            assert f"basals_{tag}" in sql, tag
    assert "basals_health_rate_loop_app > 0" in sql
    assert dpc.LOOP3_RELEASE_VERSION_INT == 3_000_000 and dpc.TRAIN_CAP_PER_LABEL <= 400_000
    print("PASS: the features SQL reads the bar flag and the staging's rule lists; the cap is memory-safe")


if __name__ == "__main__":
    test_labels_are_loop3_plugin_days_of_either_class()
    test_logistic_separates_the_populations_on_held_out_users()
    test_boundary_sits_between_the_modes()
    test_rate_key_alone_and_hand_bar_tables()
    test_features_sql_reads_the_flag_and_the_rule_lists()
    print("test_day_pattern_classifier: all checks passed")
