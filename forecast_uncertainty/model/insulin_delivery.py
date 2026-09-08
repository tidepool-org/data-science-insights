"""The controller's insulin, net of the scheduled basal, as per-tick doses for the forecaster.

Inputs (optional CSVs beside the trace CSVs, from data_staging/export_insulin_delivery.py):
  basal.csv        basal segments from both Loop upload streams (see the export's docstring)
  autoboluses.csv  automated boluses
When either file is absent the corresponding per-tick series is zero and the forecaster falls back to the
user's boluses alone, which is the pre-2026-09-04 behaviour; run_residuals.py prints which case applies.

Net basal per tick: for each user-day, one stream is used (HealthKit when it has rows that day, else Loop-direct).
Delivered units per segment are rate × effective duration for HealthKit (effective = min(duration, gap to the next
segment), as in no_meal_announcement's TDD calculation) and payload.deliveredUnits for Loop-direct. Scheduled units
are the suppressed scheduled rate × the same duration; 'scheduled' segments are net zero by definition, and a
temp segment without a scheduled rate is treated as net zero rather than as a full dose. The net units of each
segment are spread over the 5-min ticks it overlaps in proportion to overlap. Negative values mean below schedule.
"""
import os

import numpy as np
import pandas as pd

from model.forecasters import TICK_MINUTES

BASAL_FILE = "basal.csv"
AUTOBOLUS_FILE = "autoboluses.csv"
MS_PER_HOUR = 3_600_000.0
PREFERRED_STREAM = "healthkit"
FALLBACK_STREAM = "loop_direct"
MAX_SEGMENT_MS = 6 * MS_PER_HOUR      # longer 'durations' are open-ended placeholders; clip to the gap instead


def load_insulin_delivery(data_dir):
    """(basal, autoboluses) DataFrames with parsed times, or None for a missing file."""
    def _read(name, time_column):
        path = os.path.join(data_dir, name)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path)
        df[time_column] = pd.to_datetime(df[time_column])
        return df
    return _read(BASAL_FILE, "basal_timestamp"), _read(AUTOBOLUS_FILE, "bolus_timestamp")


def _pick_stream_per_day(user_basal):
    """Keep one upload stream per local day: the preferred one when it has any rows that day."""
    day = user_basal["basal_timestamp"].dt.date
    preferred_days = set(day[user_basal["stream"] == PREFERRED_STREAM])
    keep_preferred = (user_basal["stream"] == PREFERRED_STREAM)
    keep_fallback = (user_basal["stream"] == FALLBACK_STREAM) & ~day.isin(preferred_days)
    return user_basal[keep_preferred | keep_fallback].sort_values("basal_timestamp")


def net_basal_units_per_tick(user_basal, tick_times):
    """Controller insulin net of schedule, U per tick, aligned to tick_times (a regular 5-min DatetimeIndex)."""
    n = len(tick_times)
    out = np.zeros(n)
    if user_basal is None or user_basal.empty:
        return out
    seg = _pick_stream_per_day(user_basal).copy()
    start = seg["basal_timestamp"].values.astype("datetime64[ns]")
    duration_ms = seg["duration_ms"].to_numpy(dtype=float)
    gap_ms = np.append(np.diff(start).astype("timedelta64[ms]").astype(float), np.nan)
    effective_ms = np.fmin(np.fmin(duration_ms, gap_ms), MAX_SEGMENT_MS)
    effective_ms = np.where(np.isnan(effective_ms), np.fmin(duration_ms, MAX_SEGMENT_MS), effective_ms)
    hours = effective_ms / MS_PER_HOUR

    delivered = np.where(seg["stream"].to_numpy() == FALLBACK_STREAM,
                         seg["delivered_units"].to_numpy(dtype=float),
                         seg["rate_u_per_h"].to_numpy(dtype=float) * hours)
    delivered = np.where(np.isnan(delivered), seg["rate_u_per_h"].to_numpy(dtype=float) * hours, delivered)
    scheduled_rate = seg["scheduled_rate_u_per_h"].to_numpy(dtype=float)
    is_scheduled = seg["delivery_type"].to_numpy() == "scheduled"
    scheduled = np.where(is_scheduled, delivered, scheduled_rate * hours)
    net = np.where(np.isnan(scheduled) | np.isnan(delivered), 0.0, delivered - scheduled)

    tick_ns = TICK_MINUTES * 60 * 1_000_000_000
    t0 = tick_times[0].value
    seg_start_ns = start.astype("int64") - t0
    seg_end_ns = seg_start_ns + (effective_ms * 1e6).astype("int64")
    for s, e, units in zip(seg_start_ns, seg_end_ns, net):
        if units == 0.0 or e <= s:
            continue
        first, last = int(s // tick_ns), int((e - 1) // tick_ns)
        for tick in range(max(first, 0), min(last, n - 1) + 1):
            overlap = min(e, (tick + 1) * tick_ns) - max(s, tick * tick_ns)
            out[tick] += units * overlap / (e - s)
    return out


def autobolus_units_per_tick(user_autoboluses, tick_times):
    """Automated boluses summed onto the nearest tick."""
    n = len(tick_times)
    out = np.zeros(n)
    if user_autoboluses is None or user_autoboluses.empty:
        return out
    ticks = user_autoboluses["bolus_timestamp"].dt.round(f"{TICK_MINUTES}min")
    idx = ((ticks.values.astype("datetime64[ns]").astype("int64") - tick_times[0].value)
           // (TICK_MINUTES * 60 * 1_000_000_000))
    ok = (idx >= 0) & (idx < n)
    np.add.at(out, idx[ok].astype(int), user_autoboluses["bolus_units"].to_numpy(dtype=float)[ok])
    return out


def attach_controller_insulin(frame, user_basal, user_autoboluses):
    """Add net_basal_u and autobolus_u columns to a tick frame (zeros when the streams are absent)."""
    ticks = pd.DatetimeIndex(frame["timestamp"])
    frame = frame.copy()
    frame["net_basal_u"] = net_basal_units_per_tick(user_basal, ticks)
    frame["autobolus_u"] = autobolus_units_per_tick(user_autoboluses, ticks)
    return frame
