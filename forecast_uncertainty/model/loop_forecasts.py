"""Loop's displayed forecast (dosingDecision.bgForecast) aligned to the tick frame.

Input: `loop_forecasts.csv` beside the trace CSVs, from data_staging/export_loop_forecasts.py: per dosing decision
(the 5-minute `loop` series by default, or the bolus-time `normalBolus` / `watchBolus` decisions -- the forecast Loop
computed for a bolus recommendation with the carbs entered), the forecast values at the glucose sample Loop started from + 0 / 5 … 30 / 60 … 360 min, in the
record's BG units, keyed by that glucose sample's time. Here: mmol/L → mg/dL, then each decision is filed under the
5-min tick of its STARTING GLUCOSE (the tick the frame files that same reading under), the latest per tick winning,
as `displayed_forecast_<h>` columns (NaN where no decision started from a reading in the tick). The LoopDisplayedForecaster reads
those columns; origins without a forecast drop out of the residual table.
"""
import os

import numpy as np
import pandas as pd

from model.forecasters import HORIZONS_MIN, TICK_MINUTES

LOOP_FORECASTS_FILE = "loop_forecasts.csv"
SERIES_REASONS = ("loop",)                          # the 5-minute forecast series
BOLUS_TIME_REASONS = ("normalBolus", "watchBolus")  # the forecast computed for a bolus recommendation, carbs entered
FORECAST_REASON_SETS = {"series": SERIES_REASONS, "bolus_time": BOLUS_TIME_REASONS}
MMOL_L_TO_MG_DL = 18.01559
MMOL_MAGNITUDE_CEILING = 30.0      # a starting glucose below this is mmol/L (confirmed values ~11 in the export)
FORECAST_HORIZONS = (0,) + tuple(HORIZONS_MIN)


def load_loop_forecasts(data_dir, reasons=SERIES_REASONS):
    """The export restricted to the given decision reasons, times parsed and values in mg/dL; None when absent."""
    path = os.path.join(data_dir, LOOP_FORECASTS_FILE)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if "reason" in df.columns:
        df = df[df["reason"].isin(reasons)]
    time_column = "glucose_timestamp" if "glucose_timestamp" in df.columns else "dd_timestamp"
    df["glucose_timestamp"] = pd.to_datetime(df[time_column])
    # Units: trust the label, but a starting glucose around 5-15 cannot be mg/dL whatever the label says.
    labelled_mmol = df["bg_units"].astype(str).str.lower().str.startswith("mmol")
    looks_mmol = df["forecast_0"] < MMOL_MAGNITUDE_CEILING if "forecast_0" in df else False
    mmol = labelled_mmol | looks_mmol
    for h in FORECAST_HORIZONS:
        column = f"forecast_{h}"
        if column in df:
            df.loc[mmol, column] = df.loc[mmol, column] * MMOL_L_TO_MG_DL
    return df


def attach_displayed_forecasts(frame, user_forecasts):
    """Add displayed_forecast_<h> columns: the latest decision within each tick's 5-min bucket, else NaN."""
    frame = frame.copy()
    for h in FORECAST_HORIZONS:
        frame[f"displayed_forecast_{h}"] = np.nan
    if user_forecasts is None or user_forecasts.empty:
        return frame
    bucketed = user_forecasts.assign(tick=user_forecasts["glucose_timestamp"].dt.floor(f"{TICK_MINUTES}min"))
    latest = bucketed.sort_values("glucose_timestamp").groupby("tick").tail(1).set_index("tick")
    for h in FORECAST_HORIZONS:
        column = f"forecast_{h}"
        if column in latest:
            frame[f"displayed_forecast_{h}"] = frame["timestamp"].map(latest[column])
    return frame
