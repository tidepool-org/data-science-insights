"""Offline unit checks for the pure-pandas machinery of
analysis_rfai_insulin_class_outcomes.py (no Spark).

pytest-free (run_all_tests.py convention). Pins: re-pooling by the count
identities reproduces the endpoints of the union of readings, including the
sample-SD coefficient of variation; hypo rates; the small-subgroup withholding;
the window-label roll-up.
"""
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "analysis")))

import analysis_rfai_insulin_class_outcomes as rfai  # noqa: E402


def endpoints_of(readings):
    """What compute_glycemic_endpoints reports for one cell of readings."""
    r = np.asarray(readings, dtype=float)
    return dict(cbg_count=len(r), tbr_very_low=100 * (r < 54).mean(), tbr=100 * (r < 70).mean(),
                tir=100 * ((r >= 70) & (r <= 180)).mean(), tar=100 * (r > 180).mean(), tar_very_high=100 * (r > 250).mean(),
                mean_glucose=r.mean(), cv=100 * r.std(ddof=1) / r.mean())


def test_repool_reproduces_union_of_readings():
    novolog = [50, 100, 150, 200, 260, 90, 110]
    humalog = [80, 120, 300, 65, 140]
    fine = pd.DataFrame([
        dict(_userId="u1", dosing_mode="autobolus", insulin_class="RAI", insulin_type="Novolog", age_band="18+", hypo_events=2, n_days=3, **endpoints_of(novolog)),
        dict(_userId="u1", dosing_mode="autobolus", insulin_class="RAI", insulin_type="Humalog", age_band="18+", hypo_events=1, n_days=2, **endpoints_of(humalog)),
        dict(_userId="u2", dosing_mode="autobolus", insulin_class="URAI", insulin_type="Fiasp", age_band="6-11", hypo_events=0, n_days=4, **endpoints_of([70, 71, 72, 200])),
    ])
    pooled = rfai.repool(fine, ["_userId", "dosing_mode", "insulin_class"]).set_index("_userId")
    expected = endpoints_of(novolog + humalog)
    for col, val in expected.items():
        assert abs(pooled.loc["u1", col] - val) < 1e-9, (col, pooled.loc["u1", col], val)
    assert pooled.loc["u1", "hypo_events"] == 3 and pooled.loc["u1", "n_days"] == 5
    # a single-subgroup user re-pools to itself
    for col in ["cbg_count", "tir", "cv", "mean_glucose"]:
        assert abs(pooled.loc["u2", col] - fine.loc[2, col]) < 1e-9, col
    # the class-grain check accepts its own re-pooling and rejects a perturbed one
    staged = pooled.reset_index()
    rfai.check_repool_matches_class_grain(fine, staged)
    perturbed = staged.copy(); perturbed.loc[0, "cv"] += 0.01
    try:
        rfai.check_repool_matches_class_grain(fine, perturbed); raise AssertionError("perturbed class grain accepted")
    except AssertionError as exc:
        assert "cv" in str(exc)


def test_hypo_rates_and_small_cell_withholding():
    subgroups = pd.DataFrame(dict(_userId=[f"u{i}" for i in range(6)], dosing_mode=["autobolus"] * 6,
                              insulin_class=["RAI"] * 5 + ["URAI"], cbg_count=[288 * 2] * 6, n_days=[2] * 6, hypo_events=[1, 0, 2, 0, 1, 3],
                              **{c: [50.0] * 6 for c in rfai.BAND_COLS}, mean_glucose=[150.0] * 6, cv=[30.0] * 6))
    subgroups = rfai.add_hypo_rates(subgroups)
    assert abs(subgroups.loc[0, "cgm_hours"] - 48.0) < 1e-12 and abs(subgroups.loc[0, "hypo_rate_per_hour"] - 1 / 48) < 1e-12
    assert abs(subgroups.loc[5, "hypo_per_day"] - 1.5) < 1e-12
    summary = rfai.summarize_subgroups(subgroups, ["dosing_mode", "insulin_class"]).set_index("insulin_class")
    assert summary.loc["RAI", "n_users"] == 5 and not summary.loc["RAI", "withheld"]
    assert abs(summary.loc["RAI", "tir_mean"] - 50.0) < 1e-12
    assert summary.loc["URAI", "n_users"] == 1 and summary.loc["URAI", "withheld"]
    assert np.isnan(summary.loc["URAI", "tir_mean"]) and summary.loc["URAI", "n_days"] == 2   # counts kept, statistics withheld
    # the 95% t-interval of the mean is carried, and the two parts split cleanly
    assert summary.loc["RAI", "tir_ci_low"] <= summary.loc["RAI", "tir_mean"] <= summary.loc["RAI", "tir_ci_high"]
    parametric, nonparametric = rfai.split_parametric_nonparametric(summary.reset_index())
    assert "tir_mean" in parametric.columns and "tir_ci_high" in parametric.columns and "tir_median" not in parametric.columns
    assert "tir_median" in nonparametric.columns and "tir_q3" in nonparametric.columns and "tir_sd" not in nonparametric.columns
    assert set(["n_users", "n_days", "withheld"]) <= set(parametric.columns) & set(nonparametric.columns)
    # Table 8.1e: Welch differences within strategy, withheld below the small-subgroup rule
    per_user_by_brand = subgroups.assign(insulin_type=["Novolog"] * 5 + ["Fiasp"])
    e = rfai.layer1_between_groups(subgroups, per_user_by_brand)
    assert set(e["contrast"]) == {"URAI − RAI", "Fiasp − RAI", "Lyumjev − RAI"} and e["withheld"].all()   # URAI subgroup has 1 user


def test_window_group_roll_up():
    assert rfai.window_insulin_group("Novolog") == "RAI" and rfai.window_insulin_group("Lyumjev") == "URAI"
    for passthrough in ["Apidra", "multiple", "unknown", "Afrezza"]:
        assert rfai.window_insulin_group(passthrough) == passthrough


def test_layer2_between_groups_primary_contrast():
    wide = pd.DataFrame(dict(insulin_group=["RAI"] * 4 + ["URAI"] * 3 + ["multiple"], window_insulin_type=["Novolog"] * 4 + ["Fiasp"] * 2 + ["Lyumjev", "multiple"],
                             tir_seg1=[50, 52, 48, 51, 55, 53, 49, 60.0], tir_seg2=[55, 54, 52, 56, 63, 60, 58, 61.0]))
    out = rfai.layer2_between_groups(wide).set_index("contrast")
    assert out.loc["URAI − RAI", "primary"] and abs(out.loc["URAI − RAI", "diff"] - (np.mean([8, 7, 9]) - np.mean([5, 2, 4, 5]))) < 1e-12
    assert out.loc["Lyumjev − RAI", "n_a"] == 1 and np.isnan(out.loc["Lyumjev − RAI", "diff"])   # too small for an interval


if __name__ == "__main__":
    test_repool_reproduces_union_of_readings()
    test_hypo_rates_and_small_cell_withholding()
    test_window_group_roll_up()
    test_layer2_between_groups_primary_contrast()
    print("test_analysis_rfai_insulin_class_outcomes_units: all checks passed")
