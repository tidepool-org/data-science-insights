"""Build one simulator scenario JSON per user-day from the CSVs produced by
`export_single_user_day.py`.

Inputs (one CSV each, in --input_dir):
  - cgm.csv                : _userId, target_day, cbg_timestamp, cbg_mg_dl
  - carbs.csv              : _userId, target_day, carb_timestamp, carb_grams
  - correction_boluses.csv : _userId, target_day, bolus_timestamp, bolus_amount
  - pump_settings.csv      : _userId, basal_schedule, isf_schedule,
                             cir_schedule, target_schedule (JSON strings)

Output: one JSON per user at --output_dir/<_userId>.json, shaped for the
Tidepool T1 loop simulator. `patient.patient_model` mirrors `patient.pump`
(ground-truth patient physiology equals what the pump believes).

Sim window: 24 hours starting at the last cbg at/before target_day 12:00 PM.
Glucose history is target_day cbgs at or before noon. Carb and bolus entries
within the sim window are emitted, snapped to the simulator's 5-min tick grid;
events that collide on the same tick are summed (preserves total delivered
insulin / carbs). `actual_cgm` (extra top-level key) carries the full cbg
trace for overlay in the runner.

TZ semantics: the sibling `export_single_user_day.py` shifts all event
timestamps (cbg, carb, bolus) into the user's local time before writing them
to the CSVs. This module therefore treats every timestamp as user-local and
makes no UTC↔local conversion itself. `sim_start = target_day 12:00` means
user-local noon of the target day, aligning with the user-local pump-settings
schedules emitted in `pump_settings.csv`. `target_day` stays the UTC calendar
day of the first autobolus (an upstream convention) — it is used only as a
per-user index, never as a wall-clock anchor.
"""

import argparse
import json
import os

import pandas as pd


DEFAULT_INPUT_DIR = "FDA_real_world_data/simulation/data"
DEFAULT_OUTPUT_DIR = "FDA_real_world_data/simulation/data/scenarios"
SIM_START_HOUR = 12
DURATION_HOURS = 24.0
SIM_ID = "controller-fda-rwd"

CONTROLLER_ID = "swift"
CONTROLLER_SETTINGS = {
    "max_basal_rate": 35,
    "max_bolus": 30,
    "glucose_safety_limit": 70,
    "model": "rapid_acting_adult",
    "momentum_data_interval": 15,
    "dynamic_carb_absorption_enabled": True,
    "retrospective_correction_integration_interval": 30,
    "recency_interval": 15,
    "retrospective_correction_grouping_interval": 30,
    "rate_rounder": 0.05,
    "insulin_delay": 10,
    "carb_delay": 10,
    "default_absorption_times": [120.0, 180.0, 240.0],
    "retrospective_correction_enabled": True,
}


def _fmt_dt(ts):
    """Format a pandas/py timestamp as M/D/YYYY HH:MM:SS (no zero-padding on
    month/day)."""
    return f"{ts.month}/{ts.day}/{ts.year} {ts.strftime('%H:%M:%S')}"


def _scalar_schedule(segments_json):
    """Convert a JSON schedule ([{start_time, value}, ...]) from pump_settings
    into the simulator's {start_times, values} schedule block."""
    segs = json.loads(segments_json)
    return {
        "start_times": [s["start_time"] for s in segs],
        "values": [s["value"] for s in segs],
    }


def _target_schedule(segments_json):
    """Convert a JSON target schedule ([{start_time, low, high}, ...]) into
    the simulator's {start_times, lower_values, upper_values} block."""
    segs = json.loads(segments_json)
    return {
        "start_times": [s["start_time"] for s in segs],
        "lower_values": [int(round(s["low"])) for s in segs],
        "upper_values": [int(round(s["high"])) for s in segs],
    }


def _glucose_history(cgm_user_df, sim_start):
    """Return the simulator's glucose_history block for cbg readings at or
    before sim_start."""
    prior = cgm_user_df[cgm_user_df["cbg_timestamp"] <= sim_start].sort_values(
        "cbg_timestamp"
    )
    datetimes = {str(i): _fmt_dt(t) for i, t in enumerate(prior["cbg_timestamp"])}
    values = {str(i): int(round(v)) for i, v in enumerate(prior["cbg_mg_dl"])}
    units = {str(i): "mg/dL" for i in range(len(prior))}
    return {"datetime": datetimes, "value": values, "units": units}


def _metabolism_settings(settings_row):
    return {
        "insulin_sensitivity_factor": _scalar_schedule(settings_row["isf_schedule"]),
        "carb_insulin_ratio": _scalar_schedule(settings_row["cir_schedule"]),
        "basal_rate": _scalar_schedule(settings_row["basal_schedule"]),
    }


def _target_range(settings_row):
    return _target_schedule(settings_row["target_schedule"])


def _snap_to_grid(ts_series, sim_start, step_seconds=300):
    """Snap timestamps to the simulator's 5-min tick grid anchored at sim_start.
    Event-timeline lookups are exact-match, so off-grid events are silently
    dropped at runtime."""
    delta_s = (ts_series - sim_start).dt.total_seconds()
    snapped_s = (delta_s / step_seconds).round() * step_seconds
    return sim_start + pd.to_timedelta(snapped_s, unit="s")


def _bolus_entries(bolus_user_df):
    # Two real-world boluses < 2.5 min apart snap to the same 5-min tick;
    # the simulator's event timeline is an exact-match dict keyed by time,
    # so emitting duplicates would drop one. Sum same-tick doses to preserve
    # total insulin delivered (split doses, rage-boluses, etc.).
    if bolus_user_df.empty:
        return []
    grouped = (
        bolus_user_df.dropna(subset=["bolus_amount"])
        .groupby("bolus_timestamp", as_index=False)["bolus_amount"]
        .sum()
        .sort_values("bolus_timestamp")
    )
    return [
        {
            "type": "bolus",
            "time": _fmt_dt(r["bolus_timestamp"]),
            "value": float(r["bolus_amount"]),
        }
        for _, r in grouped.iterrows()
    ]


def _carb_entries(carb_user_df):
    # Same-tick-collision rule as _bolus_entries: sum carbs that snap to the
    # same 5-min tick so total grams consumed is preserved.
    if carb_user_df.empty:
        return []
    grouped = (
        carb_user_df.dropna(subset=["carb_grams"])
        .groupby("carb_timestamp", as_index=False)["carb_grams"]
        .sum()
        .sort_values("carb_timestamp")
    )
    return [
        {
            "type": "carb",
            "start_time": _fmt_dt(r["carb_timestamp"]),
            "value": float(r["carb_grams"]),
        }
        for _, r in grouped.iterrows()
    ]


def _controller_settings():
    return {
        "id": CONTROLLER_ID,
        "settings": CONTROLLER_SETTINGS,
        "automation_control_timeline": [],
    }


def _build_scenario(target_day, settings_row, cgm_user_df,
                    carb_user_df, bolus_user_df):
    # Snap sim_start to the last cbg at/before target_day noon so
    # glucose_history ends exactly at sim_start (simulator asserts this).
    noon = pd.Timestamp(target_day).replace(hour=SIM_START_HOUR)
    pre_sim_cbg = cgm_user_df[cgm_user_df["cbg_timestamp"] <= noon].sort_values(
        "cbg_timestamp"
    )
    if pre_sim_cbg.empty:
        return None
    sim_start = pre_sim_cbg["cbg_timestamp"].iloc[-1]
    sim_end = sim_start + pd.Timedelta(hours=DURATION_HOURS)

    # Keep only events inside the 24h sim window and snap to the 5-min tick
    # grid (event-timeline lookups are exact-match; off-grid events are dropped).
    bolus_in_window = bolus_user_df[
        (bolus_user_df["bolus_timestamp"] >= sim_start)
        & (bolus_user_df["bolus_timestamp"] < sim_end)
    ].assign(
        bolus_timestamp=lambda df: _snap_to_grid(df["bolus_timestamp"], sim_start)
    )
    carb_in_window = carb_user_df[
        (carb_user_df["carb_timestamp"] >= sim_start)
        & (carb_user_df["carb_timestamp"] < sim_end)
    ].assign(
        carb_timestamp=lambda df: _snap_to_grid(df["carb_timestamp"], sim_start)
    )
    metabolism = _metabolism_settings(settings_row)
    bolus_entries = _bolus_entries(bolus_in_window)
    carb_entries = _carb_entries(carb_in_window)
    glucose_history = _glucose_history(cgm_user_df, sim_start)

    # Real-world CGM for overlay (pre-sim history + in-sim actuals).
    actual_sorted = cgm_user_df.sort_values("cbg_timestamp")
    actual_cgm = [
        {"time": _fmt_dt(t), "value": int(round(v))}
        for t, v in zip(actual_sorted["cbg_timestamp"], actual_sorted["cbg_mg_dl"])
    ]

    return {
        "sim_id": SIM_ID,
        "time_to_calculate_at": _fmt_dt(sim_start),
        "duration_hours": DURATION_HOURS,
        "offset_applied_to_dates": 0,
        "patient": {
            "sensor": {"glucose_history": glucose_history},
            "pump": {
                "metabolism_settings": metabolism,
                "bolus_entries": bolus_entries,
                "carb_entries": carb_entries,
                "target_range": _target_range(settings_row),
            },
            "patient_model": {
                "metabolism_settings": metabolism,
                "glucose_history": glucose_history,
                "bolus_entries": bolus_entries,
                "carb_entries": carb_entries,
            },
        },
        "controller": _controller_settings(),
        "actual_cgm": actual_cgm,
    }


def _required_fields_present(settings_row):
    for f in ("basal_schedule", "isf_schedule", "cir_schedule", "target_schedule"):
        val = settings_row.get(f)
        if not isinstance(val, str):
            return False
        try:
            parsed = json.loads(val)
        except json.JSONDecodeError:
            return False
        if not parsed:
            return False
    return True


def run(input_dir=DEFAULT_INPUT_DIR, output_dir=DEFAULT_OUTPUT_DIR):
    pump_df = pd.read_csv(os.path.join(input_dir, "pump_settings.csv"))
    carbs_df = pd.read_csv(
        os.path.join(input_dir, "carbs.csv"), parse_dates=["carb_timestamp", "target_day"]
    )
    bolus_df = pd.read_csv(
        os.path.join(input_dir, "correction_boluses.csv"),
        parse_dates=["bolus_timestamp", "target_day"],
    )
    cgm_df = pd.read_csv(
        os.path.join(input_dir, "cgm.csv"), parse_dates=["cbg_timestamp", "target_day"]
    )

    os.makedirs(output_dir, exist_ok=True)
    # Wipe any scenario files from previous runs so anonymized rwd_user_NN
    # names stay in sync with the mapping CSV (and real user-ID filenames
    # from older builds don't linger).
    for stale in os.listdir(output_dir):
        if stale.endswith(".json") or stale == "user_id_mapping.csv":
            os.remove(os.path.join(output_dir, stale))

    skipped = 0
    written = 0
    user_index = 0  # increments only on successful writes -> stable rwd_user_NN
    user_map = []   # rows for user_id_mapping.csv

    for _, settings_row in pump_df.iterrows():
        user_id = settings_row["_userId"]
        if not _required_fields_present(settings_row):
            skipped += 1
            print(f"SKIP {user_id}: missing pump-settings field(s)")
            continue

        user_cgm = cgm_df[cgm_df["_userId"] == user_id]
        if user_cgm.empty:
            skipped += 1
            print(f"SKIP {user_id}: no CGM on target day")
            continue

        target_day = user_cgm["target_day"].iloc[0]
        scenario = _build_scenario(
            target_day,
            settings_row,
            user_cgm,
            carbs_df[carbs_df["_userId"] == user_id],
            bolus_df[bolus_df["_userId"] == user_id],
        )
        if scenario is None:
            skipped += 1
            print(f"SKIP {user_id}: no pre-target-day CGM for sim history")
            continue

        # Anonymized sequential id. "day_01" ties to
        # valid_transition_segments.segment_rank = 1 (the only rank processed
        # today). If rank=2 days are added later, plumb segment_rank through
        # and format as f"day_{rank:02d}".
        user_index += 1
        rwd_user_id = f"rwd_user_{user_index:04d}"
        scenario_id = f"{rwd_user_id}_day_01"
        # Override the per-run sim_id so the simulator's own TSVs / plots /
        # summary rows are keyed by scenario_id instead of the module-level
        # constant controller-fda-rwd (which would collide across users).
        scenario["sim_id"] = scenario_id
        out_path = os.path.join(output_dir, f"{scenario_id}.json")
        with open(out_path, "w") as f:
            json.dump(scenario, f, indent=4)
        user_map.append({"rwd_user_id": rwd_user_id, "_userId": user_id,
                         "target_day": str(target_day)[:10]})
        written += 1

    # Emit the rwd_user_NN <-> _userId mapping so results can be traced back.
    if user_map:
        pd.DataFrame(user_map).to_csv(
            os.path.join(output_dir, "user_id_mapping.csv"), index=False
        )

    print(f"Wrote {written} scenarios to {output_dir}/ (skipped {skipped}).")


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_dir", default=DEFAULT_INPUT_DIR)
    _parser.add_argument("--output_dir", default=DEFAULT_OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()

    run(input_dir=_args.input_dir, output_dir=_args.output_dir)
