"""Self-checks for the curves, the superposition, and the settings lookup. Run directly:
    python tests/test_forecasters.py
Plain asserts, no pytest (repo convention)."""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)   # run as `python tests/test_forecasters.py` from anywhere

import numpy as np
import pandas as pd

from model.forecasters import (BatemanCurve, PersistenceForecaster, LoopExponentialInsulinCurve, LoopPiecewiseLinearCarbCurve,
                         LOOP_INSULIN_PRESETS, loop_full_forecaster, loop_static_forecaster)
from model.settings import MMOL_L_TO_MG_DL, TherapySettings, parse_raw_settings


def check_insulin_curve():
    duration, peak = LOOP_INSULIN_PRESETS["rapid_acting_adult"]
    curve = LoopExponentialInsulinCurve(duration, peak)
    t = np.arange(0, 400, 0.5)
    f = curve.cumulative_fraction(t)
    assert np.all(f[t <= curve.delay] == 0.0), "nothing acts before the delay"
    assert np.all(f[t >= duration + curve.delay] == 1.0), "everything has acted at the action duration"
    assert np.all(np.diff(f) >= -1e-12), "cumulative fraction is monotone"
    just_before_end = curve.cumulative_fraction(duration + curve.delay - 1e-6)
    assert abs(just_before_end - 1.0) < 1e-6, f"remaining must reach 0 continuously at DIA, got 1-{just_before_end}"
    activity = np.gradient(f, t)
    assert abs(t[np.argmax(activity)] - (peak + curve.delay)) <= 2.0, "peak activity sits at peak + delay"
    print("insulin curve ok")


def check_carb_curve():
    curve = LoopPiecewiseLinearCarbCurve(180)
    t = np.arange(0, 220, 0.5)
    f = curve.cumulative_fraction(t)
    assert np.all(f[t <= 10] == 0.0) and np.all(f[t >= 190] == 1.0)
    assert np.all(np.diff(f) >= -1e-12)
    for breakpoint in (0.15, 0.5):
        minutes = 10 + breakpoint * 180
        left, right = curve.cumulative_fraction(minutes - 1e-6), curve.cumulative_fraction(minutes + 1e-6)
        assert abs(left - right) < 1e-5, f"continuous at {breakpoint}"
    assert abs(curve.cumulative_fraction(10 + 0.5 * 180) - curve.scale * (0.075 + 0.35)) < 1e-9
    print("carb curve ok")


def check_bateman_matches_palerm():
    curve = BatemanCurve(55, 70)
    t = np.arange(0, 600, 0.01)
    f = curve.cumulative_fraction(t)
    palerm_activity = (1 / (70 - 55)) * (np.exp(-t / 70) - np.exp(-t / 55))   # PalermInsulinModel, kcl=1
    assert np.allclose(np.gradient(f, t)[5:-5], palerm_activity[5:-5], atol=1e-5)
    assert abs(f[-1] - 1.0) < 1e-3
    print("bateman curve ok")


def check_superposition():
    n = 120
    frame = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=n, freq="5min"),
                          "cgm": 120.0, "bolus_u": np.nan, "carb_entry_g": np.nan,
                          "carb_meal_time": pd.NaT, "carb_entry_time": pd.NaT})
    frame.loc[10, "bolus_u"] = 2.0
    # carbs announced at tick 40 for a meal at tick 46 (30 min ahead)
    frame.loc[40, "carb_entry_g"] = 30.0
    frame.loc[40, "carb_entry_time"] = frame.loc[40, "timestamp"]
    frame.loc[40, "carb_meal_time"] = frame.loc[46, "timestamp"]
    forecaster = loop_static_forecaster(isf=50.0, carb_ratio=10.0)
    pred = forecaster.predict_all(frame, horizons=(180,))[180]
    insulin_only = 120.0 - 50.0 * 2.0 * forecaster.insulin_curve.cumulative_fraction(180.0)
    assert abs(pred[10] - insulin_only) < 1e-9, "bolus at the origin: full 180-min effect"
    assert pred[9] == 120.0, "an event is invisible to origins before it is known"
    carbs_at_40 = 5.0 * 30.0 * forecaster.carb_curve.cumulative_fraction(180.0 - 30.0)
    insulin_at_40 = 50.0 * 2.0 * (forecaster.insulin_curve.cumulative_fraction(150.0 + 180.0)
                                  - forecaster.insulin_curve.cumulative_fraction(150.0))
    assert abs(pred[40] - (120.0 + carbs_at_40 - insulin_at_40)) < 1e-9, "pre-announced carbs count from entry"
    per_row_isf = np.full(n, 50.0); per_row_isf[10] = 25.0
    halved = loop_static_forecaster(isf=per_row_isf, carb_ratio=10.0).predict_all(frame, horizons=(180,))[180]
    assert abs((120.0 - halved[10]) - 0.5 * (120.0 - pred[10])) < 1e-9, "per-origin ISF applies at the origin"
    print("superposition ok")


def check_persistence():
    frame = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=5, freq="5min"), "cgm": [100.0, 110.0, np.nan, 130.0, 140.0]})
    pred = PersistenceForecaster().predict_all(frame, horizons=(30, 180))
    assert np.array_equal(pred[30], frame["cgm"].values, equal_nan=True) and np.array_equal(pred[180], frame["cgm"].values, equal_nan=True)
    comps = PersistenceForecaster().predict_components(frame, horizons=(30,))
    assert not comps["carb_effect"][30].any() and not comps["insulin_effect"][30].any()
    print("persistence ok")


def check_controller_insulin():
    ticks = pd.date_range("2026-01-01 00:00", periods=48, freq="5min")
    basal = pd.DataFrame({
        "basal_timestamp": pd.to_datetime(["2026-01-01 00:00", "2026-01-01 00:30", "2026-01-01 01:00", "2026-01-01 02:00"]),
        "stream": ["healthkit"] * 3 + ["loop_direct"],
        "delivery_type": ["temp", "suspend", "scheduled", "automated"],
        "rate_u_per_h": [2.0, 0.0, 1.0, 5.0],               # loop_direct rate is the COMMANDED rate: must be ignored
        "duration_ms": [30 * 60_000, 30 * 60_000, 60 * 60_000, 30 * 60_000],
        "delivered_units": [np.nan, np.nan, np.nan, 0.5],
        "scheduled_rate_u_per_h": [1.0, 1.0, np.nan, 1.0]})
    net = net_basal_units_per_tick(basal, ticks)
    assert abs(net[:6].sum() - 0.5) < 1e-9, "temp 2 U/h over 1 U/h schedule for 30 min = +0.5 U net"
    assert abs(net[6:12].sum() - (-0.5)) < 1e-9, "suspension for 30 min = −0.5 U net"
    assert abs(net[12:24].sum()) < 1e-9, "scheduled segment is net zero"
    assert abs(net[24:30].sum() - 0.0) < 1e-9 and abs(net[24:30].sum() - (0.5 - 0.5)) < 1e-9, "loop_direct: delivered 0.5 vs scheduled 0.5 = 0"
    assert abs(net[:6] - net[0]).max() < 1e-9, "a segment's units are spread evenly over the ticks it covers"
    frame = pd.DataFrame({"timestamp": ticks, "cgm": 120.0, "bolus_u": np.nan, "carb_entry_g": np.nan,
                          "carb_meal_time": pd.NaT, "carb_entry_time": pd.NaT})
    frame = attach_controller_insulin(frame, basal, pd.DataFrame({"bolus_timestamp": [ticks[3]], "bolus_units": [1.0]}))
    assert frame["autobolus_u"][3] == 1.0 and abs(frame["net_basal_u"].sum() - 0.0) < 1e-9
    forecaster = loop_static_forecaster(isf=50.0, carb_ratio=10.0)
    pred = forecaster.predict_all(frame, horizons=(180,))[180]
    expected_drop_at_7 = 50.0 * (1.0 * (forecaster.insulin_curve.cumulative_fraction(200.0) - forecaster.insulin_curve.cumulative_fraction(20.0))
                                 + sum(frame["net_basal_u"][t] * (forecaster.insulin_curve.cumulative_fraction((7 - t) * 5.0 + 180.0)
                                                                   - forecaster.insulin_curve.cumulative_fraction((7 - t) * 5.0)) for t in range(0, 8)))
    assert abs((120.0 - pred[7]) - expected_drop_at_7) < 1e-6, "autobolus and net basal (incl. negative) enter the insulin effect"
    below = frame.copy(); below["net_basal_u"] = 0.0; below.loc[10, "net_basal_u"] = -1.0; below["autobolus_u"] = 0.0
    assert loop_static_forecaster(isf=50.0, carb_ratio=10.0).predict_all(below, horizons=(180,))[180][10] > 120.0, "below-schedule basal raises the forecast"
    print("controller insulin ok")


def check_loop_full():
    n = 60
    base = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=n, freq="5min"), "cgm": 120.0,
                         "bolus_u": np.nan, "carb_entry_g": np.nan, "carb_meal_time": pd.NaT, "carb_entry_time": pd.NaT})
    full = loop_full_forecaster(isf=50.0, carb_ratio=10.0)
    static = loop_static_forecaster(isf=50.0, carb_ratio=10.0)
    # flat glucose, no events: momentum and retrospective correction are zero, so full == static == persistence
    flat = full.predict_all(base, horizons=(30, 180))
    assert np.allclose(flat[30][20:], 120.0) and np.allclose(flat[180][20:], 120.0)
    # glucose rising 2 mg/dL per tick, no events: slope 0.4 mg/dL/min; the summed discrepancy spans 35 min (+14) over 30
    rising = base.copy(); rising["cgm"] = 100.0 + 2.0 * np.arange(n)
    comps = full.predict_components(rising, horizons=(15, 30, 60, 180))
    t = 30
    velocity = 14.0 / 30.0
    rc_steps = [velocity * 5 * (1 - j / 11.0) for j in range(11)]
    assert abs(comps["retrospective_effect"][30][t] - sum(rc_steps[:6])) < 1e-9
    assert abs(comps["retrospective_effect"][60][t] - sum(rc_steps)) < 1e-9 and abs(sum(rc_steps) - 14.0) < 1e-9
    assert abs(comps["retrospective_effect"][180][t] - 14.0) < 1e-9, "complete after 55 min: 30 × velocity = the discrepancy"
    momentum_expected = 1.0 * (2.0 - rc_steps[0]) + 0.5 * (2.0 - rc_steps[1])   # blend replaces the summed effect steps
    assert abs(comps["momentum_effect"][15][t] - momentum_expected) < 1e-9 and abs(comps["momentum_effect"][180][t] - momentum_expected) < 1e-9
    pred = full.predict_all(rising, horizons=(30,))[30][t]
    assert abs(pred - (rising["cgm"][t] + momentum_expected + sum(rc_steps[:6]))) < 1e-9
    # a jump larger than the gradual-transition threshold disables momentum; a steep rise is capped at 4 mg/dL/min
    jumpy = rising.copy(); jumpy.loc[t, "cgm"] += 50.0
    assert full.predict_components(jumpy, horizons=(15,))["momentum_effect"][15][t] == 0.0
    steep = base.copy(); steep["cgm"] = 100.0 + 30.0 * np.arange(n)      # 6 mg/dL/min, no single jump ≥ 40
    steep_comps = full.predict_components(steep, horizons=(15,))
    steep_rc = 30.0 * 7 / 30.0 * 5
    assert abs(steep_comps["momentum_effect"][15][t] - (1.0 * (20.0 - steep_rc) + 0.5 * (20.0 - steep_rc * 10 / 11.0))) < 1e-9
    # a bolus at the origin with flat glucose: nothing retrospective, no momentum -> full == static
    bolus = base.copy(); bolus.loc[30, "bolus_u"] = 2.0
    assert abs(full.predict_all(bolus, horizons=(180,))[180][30] - static.predict_all(bolus, horizons=(180,))[180][30]) < 1e-9
    # a bolus 30 min ago with glucose flat: the static effect it predicted for the past window did not happen,
    # so the correction is positive (pushes the forecast back up) and equals the unrealized predicted drop after 60 min
    at_25 = bolus.copy(); at_25.loc[25, "bolus_u"] = 2.0; at_25.loc[30, "bolus_u"] = np.nan
    unrealized = 50.0 * 2.0 * static.insulin_curve.cumulative_fraction(25.0)
    comps_25 = full.predict_components(at_25, horizons=(180,))
    assert abs(comps_25["retrospective_effect"][180][30] - unrealized) < 1e-9, "after 55 min the correction equals the discrepancy"
    # flat glucose with an unrealized drop: momentum (slope 0) replaces the first summed steps, so the forecast
    # starts flatter than the static one and the momentum component is positive
    assert comps_25["momentum_effect"][180][30] > 0
    # too few recent samples: no momentum
    sparse = rising.copy(); sparse.loc[[t - 1, t - 2], "cgm"] = np.nan
    assert full.predict_components(sparse, horizons=(15,))["momentum_effect"][15][t] == 0.0
    print("loop full ok")


def check_displayed_forecast():
    ticks = pd.date_range("2026-01-01 00:00", periods=12, freq="5min")
    frame = pd.DataFrame({"timestamp": ticks, "cgm": 100.0})
    decisions = pd.DataFrame({"glucose_timestamp": pd.to_datetime(["2026-01-01 00:10:12", "2026-01-01 00:13:40", "2026-01-01 00:30:05"]),
                              "forecast_0": [101.0, 102.0, 99.0], "forecast_30": [120.0, 125.0, 90.0], "forecast_180": [150.0, 160.0, 70.0]})
    out = attach_displayed_forecasts(frame, decisions)
    assert out["displayed_forecast_30"][2] == 125.0, "the latest decision inside the 00:10 tick wins"
    assert out["displayed_forecast_30"][6] == 90.0 and np.isnan(out["displayed_forecast_30"][5])
    pred = LoopDisplayedForecaster().predict_all(out, horizons=(30, 180))
    assert pred[30][2] == 125.0 and pred[180][6] == 70.0 and np.isnan(pred[30][0])
    comps = LoopDisplayedForecaster().predict_components(out, horizons=(30,))
    assert comps["displayed_effect"][30][2] == 25.0 and not comps["carb_effect"][30].any()
    print("displayed forecast ok")


def check_settings_lookup():
    schedule = pd.DataFrame({
        "effective_time": pd.to_datetime(["2026-01-01"] * 2 + ["2026-02-01"] * 2),
        "start_minutes": [0, 12 * 60, 0, 6 * 60], "value": [40.0, 50.0, 60.0, 70.0]})
    settings = TherapySettings("test", isf_schedule=schedule, constant_carb_ratio=10.0)
    times = pd.to_datetime(["2025-12-01 03:00", "2026-01-15 03:00", "2026-01-15 15:00", "2026-03-01 07:00"])
    assert settings.isf_at(times).tolist() == [40.0, 40.0, 50.0, 70.0]
    assert settings.carb_ratio_at(times).tolist() == [10.0] * 4
    print("settings lookup ok")


def check_parse_raw_settings_units():
    """ISF converts on the mmol/L label, stays put on a credible mg/dL label, and converts anyway when an
    mg/dL-labelled record is unmistakably mmol/L-scaled (the BDDP legacy-record case). Carb ratio never converts."""
    schedule = '[{"start":0,"amount":%s},{"start":21600000,"amount":%s}]'
    raw = pd.DataFrame({
        "_userId": ["a", "b", "c"], "effective_time": pd.to_datetime(["2026-01-01"] * 3),
        "units_raw": ['{"bg":"mmol/L"}', '{"bg":"mg/dL"}', '{"bg":"mg/dL"}'],
        "insulin_sensitivity_raw": [schedule % (2.5, 3.0), schedule % (45, 54), schedule % (2.5, 3.0)],
        "carb_ratio_raw": [schedule % (10, 12)] * 3, "insulin_model_raw": [None, '{"modelType":"fiasp"}', None],
        "active_schedule": [None] * 3})
    isf, carb_ratio, presets = parse_raw_settings(raw)
    by_user = isf.sort_values("start_minutes").groupby("_userId")["value"].apply(list)
    assert np.allclose(by_user["a"], [2.5 * MMOL_L_TO_MG_DL, 3.0 * MMOL_L_TO_MG_DL]), "mmol/L label converts"
    assert by_user["b"] == [45.0, 54.0], "credible mg/dL values stay"
    assert np.allclose(by_user["c"], [2.5 * MMOL_L_TO_MG_DL, 3.0 * MMOL_L_TO_MG_DL]), "mmol/L-scaled mg/dL label converts"
    assert isf["start_minutes"].max() == 360.0, "start is ms since midnight -> minutes"
    assert sorted(carb_ratio["value"].unique()) == [10.0, 12.0], "carb ratio is never unit-converted"
    assert presets == {"b": "fiasp"}
    print("parse_raw_settings units ok")


def check_residual_table():
    n = 200
    frame = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=n, freq="5min"),
                          "cgm": 100.0 + np.arange(n) * 0.5, "iob": np.nan, "cob": np.nan,
                          "bolus_u": np.nan, "carb_entry_g": np.nan, "carb_meal_time": pd.NaT, "carb_entry_time": pd.NaT})
    frame.loc[[0, 50], "iob"] = [3.0, 1.0]                      # displayed IOB only at dosing decisions
    frame.loc[20, ["bolus_u"]] = 1.0
    frame.loc[60, "carb_entry_g"] = 20.0
    frame.loc[60, ["carb_meal_time", "carb_entry_time"]] = frame.loc[60, "timestamp"]
    isf = np.full(n, 40.0); isf[:100] = 50.0                     # a schedule change mid-frame
    table = build_residual_table(frame, loop_static_forecaster(isf=isf, carb_ratio=10.0), isf, 10.0, horizons=(30, 180))
    row = table[(table["horizon_min"] == 180) & (table["origin_index"] == 70)].iloc[0]
    assert abs(row["predicted_change"] - (row["carb_effect_pred"] - row["insulin_effect_pred"])) < 1e-9
    assert abs(row["prior_change_30"] - 3.0) < 1e-9 and abs(row["prior_change_60"] - 6.0) < 1e-9, "0.5 mg/dL per tick"
    assert row["minutes_since_carb_entry"] == 50 and row["minutes_since_carb_entry_capped"] == 50
    early = table[(table["horizon_min"] == 30) & (table["origin_index"] == 10)].iloc[0]
    assert early["minutes_since_carb_entry_capped"] == 180 and np.isnan(early["minutes_since_carb_entry"])
    at_30 = table[table["horizon_min"] == 30].set_index("origin_index")
    assert at_30["iob0"][3] == 3.0 and np.isnan(at_30["iob0"][4]), "IOB carried forward at most 3 ticks"
    assert at_30["iob_effect"][3] == 3.0 * 50.0 and at_30["isf0"][150] == 40.0, "IOB in mg/dL through the ISF in force"
    assert abs(at_30["carbs_recent_effect"][61] - 20.0 * 40.0 / 10.0) < 1e-9, "recent carbs in mg/dL via ISF / CR"
    assert abs(at_30["bolus_recent_effect"][21] - 1.0 * 40.0) < 1e-9, "recent bolus in mg/dL via ISF"
    assert abs(early["fresh_residual_30"] - (early["cgm0"] - table[(table["horizon_min"] == 30) & (table["origin_index"] == 4)].iloc[0]["predicted"])) < 1e-9
    print("residual table ok")


if __name__ == "__main__":
    check_insulin_curve()
    check_carb_curve()
    check_bateman_matches_palerm()
    check_superposition()
    check_settings_lookup()
    check_parse_raw_settings_units()
    print("ALL OK")
