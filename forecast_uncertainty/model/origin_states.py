"""One set of origin states for every evaluation and figure (project_history.md, 2026-09-08).

An origin state describes the situation a forecast was issued in, from quantities known at the origin only:
never the forecast itself, never the outcome. Three axes, each exclusive and exhaustive, crossable with each other:

  intervention_state   post_carb_early     0 <= minutes since a carb entry < POST_CARB_EARLY_MAX_MIN
                       post_carb_late      POST_CARB_EARLY_MAX_MIN <= minutes since a carb entry < INTERVENTION_WINDOW_MIN
                       post_bolus_no_carb  a user bolus within INTERVENTION_WINDOW_MIN and no carb entry within it
                       quiet               neither within INTERVENTION_WINDOW_MIN
  level_state          below_70 / in_range / above_180 -- the consensus time-in-range bands, at the origin CGM
  trend_state          falling / flat / rising -- the prior 30-min change against TREND_THRESHOLD_MG_DL_PER_MIN x 30
                       (the CGM trend-arrow convention: one mg/dL per minute)

Why these boundaries. 60 min is where Loop's forecast beats persistence and where the interval's spread steps up after
an entry. 180 min is Loop's default carb absorption time, the cap on the model's meal-clock features
(residuals.RECENT_WINDOW_MIN), the clean-event gap of figures 24-25, and where the spread had returned to baseline.

The states are a deterministic function of four columns every residual table already carries (minutes_since_carb_entry,
minutes_since_bolus, cgm0, prior_change_30), so they are computed where used rather than stored: no table has to be
rebuilt when the definition is applied, and no stored column can drift from it.

"Forecast direction" (predicted rise / fall from the forecaster's own predicted change) is NOT a state: it is the
forecaster's opinion, undefined for persistence. It survives as an optional diagnostic, `forecast_direction`.
"""
import numpy as np
import pandas as pd

POST_CARB_EARLY_MAX_MIN = 60          # end of the early post-carb window
INTERVENTION_WINDOW_MIN = 180         # a carb entry or bolus older than this no longer defines the state
TREND_THRESHOLD_MG_DL_PER_MIN = 1.0   # |prior 30-min change| above this rate is a trend (30 mg/dL over 30 min)
TREND_WINDOW_MIN = 30                 # the prior-change window the trend axis reads (residuals.prior_change_30)
LEVEL_LOW_MG_DL = 70.0
LEVEL_HIGH_MG_DL = 180.0
FORECAST_DIRECTION_THRESHOLD_MG_DL = 20.0   # |predicted change| at or above this is a predicted rise / fall (diagnostic only)

INTERVENTION_STATES = ["post_carb_early", "post_carb_late", "post_bolus_no_carb", "quiet"]
LEVEL_STATES = ["below_70", "in_range", "above_180"]
TREND_STATES = ["falling", "flat", "rising"]
FORECAST_DIRECTIONS = ["predicted_fall", "predicted_flat", "predicted_rise"]

STATE_LABELS = {                       # figure and table labels
    "post_carb_early": "post-carb, 0-60 min", "post_carb_late": "post-carb, 60-180 min",
    "post_bolus_no_carb": "post-bolus, no carb", "quiet": "quiet",
    "below_70": "< 70 mg/dL", "in_range": "70-180 mg/dL", "above_180": "> 180 mg/dL",
    "falling": "falling", "flat": "flat", "rising": "rising",
    "predicted_fall": "predicted fall", "predicted_flat": "predicted flat", "predicted_rise": "predicted rise",
}
STATE_COLUMNS = ["intervention_state", "level_state", "trend_state"]
REQUIRED_COLUMNS = ["minutes_since_carb_entry", "minutes_since_bolus", "cgm0", "prior_change_30"]


def intervention_state(minutes_since_carb_entry, minutes_since_bolus):
    """The primary axis. NaN minutes (no event yet) count as 'not within the window'."""
    carb = np.asarray(minutes_since_carb_entry, dtype=float)
    bolus = np.asarray(minutes_since_bolus, dtype=float)
    carb_recent = (carb >= 0) & (carb < INTERVENTION_WINDOW_MIN)
    bolus_recent = (bolus >= 0) & (bolus < INTERVENTION_WINDOW_MIN)
    out = np.where(carb_recent & (carb < POST_CARB_EARLY_MAX_MIN), "post_carb_early",
          np.where(carb_recent, "post_carb_late",
          np.where(bolus_recent, "post_bolus_no_carb", "quiet")))
    return pd.Categorical(out, categories=INTERVENTION_STATES)


def level_state(cgm0):
    cgm = np.asarray(cgm0, dtype=float)
    out = np.where(cgm < LEVEL_LOW_MG_DL, "below_70", np.where(cgm > LEVEL_HIGH_MG_DL, "above_180", "in_range"))
    return pd.Categorical(out, categories=LEVEL_STATES)


def trend_state(prior_change_30):
    change = np.asarray(prior_change_30, dtype=float)
    threshold = TREND_THRESHOLD_MG_DL_PER_MIN * TREND_WINDOW_MIN
    out = np.where(change <= -threshold, "falling", np.where(change >= threshold, "rising", "flat"))
    return pd.Categorical(out, categories=TREND_STATES)


def forecast_direction(predicted_change):
    """Diagnostic, forecaster-dependent: the sign of a large predicted change. Persistence predicts no change,
    so every row is 'predicted_flat' and the rise / fall groups are empty by construction."""
    change = np.asarray(predicted_change, dtype=float)
    out = np.where(change <= -FORECAST_DIRECTION_THRESHOLD_MG_DL, "predicted_fall",
          np.where(change >= FORECAST_DIRECTION_THRESHOLD_MG_DL, "predicted_rise", "predicted_flat"))
    return pd.Categorical(out, categories=FORECAST_DIRECTIONS)


def assign_origin_states(table):
    """The three state columns added to a copy of `table` (a residual, holdout or decision table)."""
    missing = [c for c in REQUIRED_COLUMNS if c not in table.columns]
    if missing:
        raise KeyError(f"origin states need columns {missing}")
    out = table.copy()
    out["intervention_state"] = intervention_state(table["minutes_since_carb_entry"], table["minutes_since_bolus"])
    out["level_state"] = level_state(table["cgm0"])
    out["trend_state"] = trend_state(table["prior_change_30"])
    return out


def _self_check():
    table = pd.DataFrame({
        "minutes_since_carb_entry": [10, 90, np.nan, 200, 300, np.nan],
        "minutes_since_bolus": [np.nan, 5, 60, 30, 400, np.nan],
        "cgm0": [65, 100, 250, 180, 70, 120],
        "prior_change_30": [-31, 0, 45, 29, -30, np.nan],
    })
    out = assign_origin_states(table)
    assert out["intervention_state"].tolist() == ["post_carb_early", "post_carb_late", "post_bolus_no_carb",
                                                  "post_bolus_no_carb", "quiet", "quiet"]
    assert out["level_state"].tolist() == ["below_70", "in_range", "above_180", "in_range", "in_range", "in_range"]
    assert out["trend_state"].tolist() == ["falling", "flat", "rising", "flat", "falling", "flat"]
    assert forecast_direction([0.0, 20.0, -20.0]).tolist() == ["predicted_flat", "predicted_rise", "predicted_fall"]
    print("origin states ok")


if __name__ == "__main__":
    _self_check()
