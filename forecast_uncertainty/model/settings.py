"""Per-user therapy settings (insulin sensitivity, carb ratio) for the forecasters.

Preferred source: `settings_raw.csv` in the data dir, written by data_staging/export_therapy_settings.py
(pumpSettings records, raw JSON strings, pseudonymized ids). Parsed here into per-user schedules of
insulin sensitivity (mg/dL per U) and carb ratio (g per U) by effective time and minute of day.

Fallback when no export is present: a per-user CARB-RATIO PROXY -- the median grams-per-unit over carb
entries paired with a user bolus within PAIRING_TOLERANCE_MIN -- and a sensitivity set from it by the
1800/500 rule ratio (ISF_TO_CARB_RATIO). Both are derived from behavior, not fit to glucose, but they
are proxies: TherapySettings.source records which path each user took, and results obtained on the
proxy must be labelled as such.
"""
import json
import os

import numpy as np
import pandas as pd

SETTINGS_RAW_FILE = "settings_raw.csv"
PAIRING_TOLERANCE_MIN = 15
MIN_PAIRED_CARBS_G = 10.0
MIN_PAIRED_BOLUS_U = 0.5
ISF_TO_CARB_RATIO = 1800.0 / 500.0   # (mg/dL per U) / (g per U): the 1800-rule over the 500-rule
SETTINGS_MAX_STALENESS_DAYS = 365    # an export record older than this before the user's window start is not used
MMOL_L_TO_MG_DL = 18.01559
LOWEST_PLAUSIBLE_ISF_MG_DL_PER_U = 10.0   # an ISF record entirely below this is mmol/L per U whatever units.bg says
MS_PER_MINUTE = 60_000

# pumpSettings insulinModel.modelType -> forecasters.LOOP_INSULIN_PRESETS key
INSULIN_MODEL_TYPE_TO_PRESET = {
    "rapidAdult": "rapid_acting_adult", "rapidChild": "rapid_acting_child",
    "fiasp": "fiasp", "lyumjev": "lyumjev", "afrezza": "afrezza",
}


class TherapySettings:
    """isf_at / carb_ratio_at return arrays aligned to the given timestamps (user-local)."""

    def __init__(self, source, isf_schedule=None, carb_ratio_schedule=None,
                 constant_isf=None, constant_carb_ratio=None, insulin_preset=None, fallback_reason=None):
        self.source = source
        self.fallback_reason = fallback_reason
        self.isf_schedule = isf_schedule
        self.carb_ratio_schedule = carb_ratio_schedule
        self.constant_isf = constant_isf
        self.constant_carb_ratio = constant_carb_ratio
        self.insulin_preset = insulin_preset

    @staticmethod
    def _lookup(schedule, constant, timestamps):
        """Step function: latest record effective at or before t, then the segment covering t's minute of day."""
        ts = pd.DatetimeIndex(timestamps)
        n = len(ts)
        if schedule is None:
            return np.full(n, float(constant))
        effective_times = np.sort(schedule["effective_time"].unique())
        record_index = np.searchsorted(effective_times, ts.values, side="right") - 1
        record_index = np.clip(record_index, 0, len(effective_times) - 1)   # before the first record: use it
        minutes_of_day = (ts.hour * 60 + ts.minute).to_numpy()
        out = np.empty(n)
        for i, effective in enumerate(effective_times):
            rows = record_index == i
            if not rows.any():
                continue
            segments = schedule[schedule["effective_time"] == effective].sort_values("start_minutes")
            starts = segments["start_minutes"].to_numpy()
            values = segments["value"].to_numpy(dtype=float)
            j = np.clip(np.searchsorted(starts, minutes_of_day[rows], side="right") - 1, 0, len(starts) - 1)
            out[rows] = values[j]
        return out

    def isf_at(self, timestamps):
        return self._lookup(self.isf_schedule, self.constant_isf, timestamps)

    def carb_ratio_at(self, timestamps):
        return self._lookup(self.carb_ratio_schedule, self.constant_carb_ratio, timestamps)


def derive_carb_ratio_proxy(carbs, boluses):
    """Per-user median grams-per-unit over carb entries paired with a bolus within the tolerance.
    Returns a DataFrame indexed by _userId: carb_ratio_g_per_u, n_pairs, paired_share."""
    rows = []
    for user_id, user_carbs in carbs.groupby("_userId"):
        user_boluses = boluses[boluses["_userId"] == user_id].sort_values("bolus_timestamp")
        if user_boluses.empty:
            continue
        paired = pd.merge_asof(user_carbs.sort_values("entry_time"), user_boluses,
                               left_on="entry_time", right_on="bolus_timestamp",
                               tolerance=pd.Timedelta(minutes=PAIRING_TOLERANCE_MIN), direction="nearest")
        paired = paired.dropna(subset=["bolus_units"])
        paired = paired[(paired["carb_grams"] >= MIN_PAIRED_CARBS_G) & (paired["bolus_units"] >= MIN_PAIRED_BOLUS_U)]
        if paired.empty:
            continue
        ratio = paired["carb_grams"] / paired["bolus_units"]
        rows.append({"_userId": user_id, "carb_ratio_g_per_u": float(ratio.median()),
                     "n_pairs": int(len(paired)), "paired_share": float(len(paired) / len(user_carbs))})
    return pd.DataFrame(rows).set_index("_userId")


def _parse_schedule(raw, active_schedule=None):
    """Loop uploads [{start: ms since midnight, amount}]; multi-schedule pumps upload {name: [...]}.
    Returns list of (start_minutes, amount) or None."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)) or raw == "":
        return None
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(parsed, dict):
        if active_schedule in parsed:
            parsed = parsed[active_schedule]
        elif parsed:
            parsed = next(iter(parsed.values()))
        else:
            return None
    segments = [(float(seg["start"]) / MS_PER_MINUTE, float(seg["amount"])) for seg in parsed
                if seg.get("amount") is not None]
    return segments or None


def _isf_conversion_factor(bg_units_label, isf_segments):
    """Factor taking one record's ISF amounts to mg/dL per U.

    units.bg decides, with one override: the BDDP stores BG-valued settings platform-normalized to mmol/L,
    yet legacy records can keep the device's display unit in units.bg. A record labelled mg/dL whose every
    ISF segment is below LOWEST_PLAUSIBLE_ISF_MG_DL_PER_U cannot be in mg/dL per U and is converted anyway
    (in the cohort-B export every such amount x MMOL_L_TO_MG_DL lands on an integer mg/dL)."""
    if str(bg_units_label).lower().startswith("mmol"):
        return MMOL_L_TO_MG_DL
    if isf_segments and all(amount < LOWEST_PLAUSIBLE_ISF_MG_DL_PER_U for _, amount in isf_segments):
        return MMOL_L_TO_MG_DL
    return 1.0


def parse_raw_settings(raw):
    """settings_raw.csv -> (isf_schedules, carb_ratio_schedules, insulin_preset_by_user).
    Schedules are long DataFrames: _userId, effective_time, start_minutes, value (mg/dL per U or g per U)."""
    isf_rows, cr_rows, presets = [], [], {}
    for _, rec in raw.iterrows():
        units = json.loads(rec["units_raw"]) if isinstance(rec.get("units_raw"), str) else {}
        bg_units = (units or {}).get("bg", "mg/dL")
        active = rec.get("active_schedule") if isinstance(rec.get("active_schedule"), str) else None
        isf_segments = _parse_schedule(rec.get("insulin_sensitivity_raw"), active)
        carb_ratio_segments = _parse_schedule(rec.get("carb_ratio_raw"), active)
        isf_factor = _isf_conversion_factor(bg_units, isf_segments)
        for segments, factor, sink in ((isf_segments, isf_factor, isf_rows),
                                       (carb_ratio_segments, 1.0, cr_rows)):
            if segments:
                sink.extend({"_userId": rec["_userId"], "effective_time": rec["effective_time"],
                             "start_minutes": start, "value": amount * factor} for start, amount in segments)
        model_raw = rec.get("insulin_model_raw")
        if isinstance(model_raw, str) and model_raw:
            model_type = json.loads(model_raw).get("modelType")
            if model_type in INSULIN_MODEL_TYPE_TO_PRESET:
                presets[rec["_userId"]] = INSULIN_MODEL_TYPE_TO_PRESET[model_type]
    to_frame = lambda rows: pd.DataFrame(rows) if rows else None
    return to_frame(isf_rows), to_frame(cr_rows), presets


def load_settings_export(data_dir):
    """Parsed settings export, or None when the file is absent."""
    path = os.path.join(data_dir, SETTINGS_RAW_FILE)
    if not os.path.exists(path):
        return None
    raw = pd.read_csv(path)
    raw["effective_time"] = pd.to_datetime(raw["effective_time"])
    return parse_raw_settings(raw)


def _fresh_enough(schedule, window_start):
    """True when the user's newest record is effective no earlier than SETTINGS_MAX_STALENESS_DAYS before
    the trace window starts; older records describe a pump era the traces never saw."""
    newest = schedule["effective_time"].max()
    return newest >= window_start - pd.Timedelta(days=SETTINGS_MAX_STALENESS_DAYS)


def build_therapy_settings(data_dir, streams, default_insulin_preset):
    """One TherapySettings per user: from the export where a user has both schedules and they are recent
    enough for the trace window, else the carb-ratio proxy (with the reason recorded)."""
    export = load_settings_export(data_dir)
    proxy = derive_carb_ratio_proxy(streams["carbs"], streams["boluses"])
    window_start = streams["users"].set_index("_userId")["window_start"]
    settings = {}
    for user_id in streams["users"]["_userId"]:
        fallback_reason = "no settings export file"
        if export is not None:
            isf_schedules, cr_schedules, presets = export
            isf = isf_schedules[isf_schedules["_userId"] == user_id] if isf_schedules is not None else None
            cr = cr_schedules[cr_schedules["_userId"] == user_id] if cr_schedules is not None else None
            has_both = isf is not None and len(isf) and cr is not None and len(cr)
            if has_both and _fresh_enough(isf, window_start[user_id]) and _fresh_enough(cr, window_start[user_id]):
                settings[user_id] = TherapySettings(
                    "settings_export", isf_schedule=isf, carb_ratio_schedule=cr,
                    insulin_preset=presets.get(user_id, default_insulin_preset))
                continue
            fallback_reason = ("export records too stale for the window" if has_both
                               else "no ISF/CR records in the export")
        if user_id in proxy.index:
            carb_ratio = proxy.loc[user_id, "carb_ratio_g_per_u"]
            settings[user_id] = TherapySettings(
                "carb_ratio_proxy", constant_isf=ISF_TO_CARB_RATIO * carb_ratio,
                constant_carb_ratio=carb_ratio, insulin_preset=default_insulin_preset,
                fallback_reason=fallback_reason)
    return settings, proxy
