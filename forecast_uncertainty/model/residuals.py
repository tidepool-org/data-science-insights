"""Residual table: for every origin and horizon, forecast vs realized CGM plus origin-state features.

Every feature is known at the origin. Insulin and carb quantities are expressed in mg/dL through the user's own
therapy settings at the origin, so they mean the same thing across users: iob_effect = displayed IOB × ISF,
bolus_recent_effect = recent bolus units × ISF, carbs_recent_effect = recent grams × ISF / carb ratio.
Columns added per horizon: predicted, the forecast's components (carb_effect_pred, insulin_effect_pred, and for the
full Loop forecaster momentum_effect_pred and retrospective_effect_pred; zero where a forecaster lacks the term;
predicted_change = their signed sum), realized, residual.
"""
import numpy as np
import pandas as pd
from model.forecasters import COMPONENT_SIGNS, HORIZONS_MIN, TICK_MINUTES, combine_components

RECENT_WINDOW_MIN = 180            # window for recent carbs / boluses; also the cap on minutes since carb entry
FRESH_RESIDUAL_HORIZON_MIN = 30    # the shortest-horizon forecast whose outcome is already known at the origin
PRIOR_CHANGE_MINUTES = (30, 60)    # realized glucose change over the minutes BEFORE the origin
IOB_FORWARD_FILL_TICKS = 3         # Loop's displayed IOB is logged only at dosing decisions; carry it ≤ 15 min


def _shift_back(values, steps):
    """values[t + steps] aligned to t (NaN past the end)."""
    out = np.full(len(values), np.nan)
    if steps < len(values):
        out[:len(values) - steps] = values[steps:]
    return out


def _shift_forward(values, steps):
    """values[t - steps] aligned to t (NaN before the start)."""
    out = np.full(len(values), np.nan)
    if steps < len(values):
        out[steps:] = values[:len(values) - steps]
    return out


def build_residual_table(frame, forecaster, isf, carb_ratio, horizons=HORIZONS_MIN):
    """isf (mg/dL per U) and carb_ratio (g per U): scalars or arrays aligned to the frame rows."""
    components = forecaster.predict_components(frame, horizons)
    cgm = frame["cgm"].values.astype(float)
    predicted = combine_components(cgm, components, horizons)
    n = len(frame)
    ticks_recent = RECENT_WINDOW_MIN // TICK_MINUTES
    isf0 = np.broadcast_to(np.asarray(isf, dtype=float), (n,))
    carb_ratio0 = np.broadcast_to(np.asarray(carb_ratio, dtype=float), (n,))

    # Origin-state features.
    iob = frame["iob"].astype(float).ffill(limit=IOB_FORWARD_FILL_TICKS).values if "iob" in frame else np.full(n, np.nan)
    state = pd.DataFrame({
        "origin_index": np.arange(n),
        "timestamp": frame["timestamp"].values,
        "hour_local": frame["timestamp"].dt.hour.values + frame["timestamp"].dt.minute.values / 60.0,
        "cgm0": cgm,
        "iob0": iob,
        "cob0": frame["cob"].values.astype(float) if "cob" in frame else np.full(n, np.nan),
        "carbs_entered_recent_g": pd.Series(frame["carb_entry_g"].fillna(0).values).rolling(ticks_recent, min_periods=1).sum().values,
        "bolus_recent_u": pd.Series(frame["bolus_u"].fillna(0).values).rolling(ticks_recent, min_periods=1).sum().values,
    })
    state["isf0"] = isf0
    state["carb_ratio0"] = carb_ratio0
    state["iob_effect"] = state["iob0"] * isf0                                   # mg/dL the IOB can still lower
    state["bolus_recent_effect"] = state["bolus_recent_u"] * isf0                # mg/dL, recent boluses
    state["carbs_recent_effect"] = state["carbs_entered_recent_g"] * isf0 / carb_ratio0   # mg/dL, recent carbs
    for minutes in PRIOR_CHANGE_MINUTES:
        state[f"prior_change_{minutes}"] = cgm - _shift_forward(cgm, minutes // TICK_MINUTES)
    entry_tick = pd.Series(np.where(frame["carb_entry_g"].fillna(0).values > 0, np.arange(n), np.nan)).ffill().values
    minutes_since_entry = (np.arange(n) - entry_tick) * TICK_MINUTES
    state["minutes_since_carb_entry"] = minutes_since_entry
    bolus_tick = pd.Series(np.where(frame["bolus_u"].fillna(0).values > 0, np.arange(n), np.nan)).ffill().values
    state["minutes_since_bolus"] = (np.arange(n) - bolus_tick) * TICK_MINUTES      # user boluses only; NaN before the first
    # Capped version for modelling: no entry yet, or an entry older than the window, both mean "not recent".
    state["minutes_since_carb_entry_capped"] = np.where(np.isnan(minutes_since_entry), RECENT_WINDOW_MIN,
                                                        np.minimum(minutes_since_entry, RECENT_WINDOW_MIN))
    fresh_steps = FRESH_RESIDUAL_HORIZON_MIN // TICK_MINUTES
    state["fresh_residual_30"] = cgm - _shift_forward(predicted[FRESH_RESIDUAL_HORIZON_MIN], fresh_steps)

    parts = []
    for h in horizons:
        part = state.copy()
        part["horizon_min"] = h
        part["predicted"] = predicted[h]
        for name in COMPONENT_SIGNS:                       # every component column exists for every forecaster
            part[f"{name}_pred"] = components[name][h] if name in components else 0.0
        part["predicted_change"] = predicted[h] - cgm
        part["realized"] = _shift_back(cgm, h // TICK_MINUTES)
        part["residual"] = part["realized"] - part["predicted"]
        parts.append(part)
    table = pd.concat(parts, ignore_index=True)
    table["forecaster"] = forecaster.name
    return table.dropna(subset=["predicted", "realized"]).reset_index(drop=True)
