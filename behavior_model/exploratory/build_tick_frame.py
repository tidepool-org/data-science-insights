"""Assemble §6-contract tick frames from export_behavior_traces.py CSVs and
run Stage A.

Usage (after downloading the export dir to behavior_model/data/behavior_traces):

    python build_tick_frame.py [--data-dir DIR] [--user u<hash>] [--no-run]

Per user: builds the 5-minute tick frame on user-local time, validates it
against the data contract, prints the weekly drift check FIRST (trap #5 --
look at it before trusting the time split), then runs run_mvp and prints the
go/no-go comparison, ablation, and diurnal profile. Outputs are saved under
outputs/behavior_traces/<user>/ (git-ignored).

Assembly conventions:
  - CGM: floor to the 5-min bucket, latest reading per bucket (the loop_cbg
    staging convention).
  - Carb entries sit on the tick of their ENTRY time (two-clock convention),
    rounded to the nearest tick. Same-tick entries are summed (grams) with a
    gram-weighted mean meal time -- the build_scenario_json collision rule.
    Entries missing entry_time are DROPPED with a printed count; the export's
    user selection makes these rare by construction.
  - Boluses: nearest tick, same-tick units summed.
  - iob: latest reason='loop' dosing decision in each bucket, parsed from the
    raw string; NaN between decisions (add_features forward-fills).
  - recommended_bolus: latest 'loop' decision per bucket; at bolus ticks the
    nearest 'normalBolus' decision within ±1 tick wins -- that is the number
    the user actually saw when bolusing.
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from behavior_model_mvp import (DEFAULT_SPLIT, TICK_MINUTES, run_mvp,
                                validate_tick_frame)
from stage_a_metrics import (BASE_SEED_DEFAULT, HISTORY_FILENAME,
                             N_SIMS_DEFAULT, append_history, evaluate)

DEFAULT_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "behavior_traces")
DEFAULT_OUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "outputs", "behavior_traces")
TICK = f"{TICK_MINUTES}min"


def parse_units(raw):
    """Float from a raw BDDP value string: scalar, or JSON with a units-like
    key. Returns NaN when unparseable."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return np.nan
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return np.nan
    try:
        return float(s)
    except ValueError:
        pass
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return np.nan
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, dict):
        for key in ("amount", "value", "units", "normal"):
            v = obj.get(key)
            if isinstance(v, dict):
                v = v.get("value", v.get("amount"))
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    continue
    return np.nan


def _latest_per_bucket(df, ts_col, value_col):
    """Series indexed by 5-min floor bucket: latest value in each bucket."""
    d = df.dropna(subset=[ts_col]).sort_values(ts_col)
    return d.groupby(d[ts_col].dt.floor(TICK))[value_col].last()


def build_user_frame(cgm, carbs, boluses, dosing):
    """One user's §6-contract tick frame from the four event streams.

    All timestamps must already be user-local datetimes.
    """
    grid = pd.date_range(cgm["cbg_timestamp"].min().floor(TICK),
                         cgm["cbg_timestamp"].max().ceil(TICK), freq=TICK)
    frame = pd.DataFrame({"timestamp": grid})

    frame["cgm"] = frame["timestamp"].map(
        _latest_per_bucket(cgm, "cbg_timestamp", "cbg_mg_dl"))

    loop_dd = dosing[dosing["reason"] == "loop"].copy()
    loop_dd["iob"] = loop_dd["insulin_on_board_raw"].map(parse_units)
    loop_dd["rec"] = loop_dd["recommended_bolus_raw"].map(parse_units)
    frame["iob"] = frame["timestamp"].map(
        _latest_per_bucket(loop_dd, "dd_timestamp", "iob"))
    frame["recommended_bolus"] = frame["timestamp"].map(
        _latest_per_bucket(loop_dd, "dd_timestamp", "rec"))

    b = boluses.dropna(subset=["bolus_timestamp", "bolus_units"]).copy()
    b["tick"] = b["bolus_timestamp"].dt.round(TICK)
    frame["bolus_u"] = frame["timestamp"].map(b.groupby("tick")["bolus_units"].sum())

    # the recommendation the user saw at bolus moments: nearest normalBolus
    # decision within ±1 tick of the bolus tick
    nb = dosing[dosing["reason"] == "normalBolus"].copy()
    if len(nb):
        nb["rec"] = nb["recommended_bolus_raw"].map(parse_units)
        nb_by_tick = _latest_per_bucket(nb, "dd_timestamp", "rec")
        bolus_ticks = frame.index[frame["bolus_u"].notna()]
        for i in bolus_ticks:
            t = frame.loc[i, "timestamp"]
            for cand in (t, t - pd.Timedelta(TICK), t + pd.Timedelta(TICK)):
                v = nb_by_tick.get(cand, np.nan)
                if np.isfinite(v):
                    frame.loc[i, "recommended_bolus"] = v
                    break

    c = carbs.dropna(subset=["meal_time", "carb_grams"]).copy()
    n_missing_entry = int(c["entry_time"].isna().sum())
    if n_missing_entry:
        print(f"  dropped {n_missing_entry}/{len(c)} carb entries missing entry_time")
    c = c.dropna(subset=["entry_time"])
    c["tick"] = c["entry_time"].dt.round(TICK)

    def _collapse(group):
        w = group["carb_grams"].to_numpy()
        w = w / w.sum()

        def weighted_time(col):
            ref = group[col].min()
            offsets = (group[col] - ref).dt.total_seconds().to_numpy()
            return ref + pd.to_timedelta((offsets * w).sum(), unit="s")

        return pd.Series({
            "carb_entry_g": group["carb_grams"].sum(),
            "carb_meal_time": weighted_time("meal_time"),
            "carb_entry_time": weighted_time("entry_time"),
        })

    if len(c):
        collapsed = c.groupby("tick").apply(_collapse, include_groups=False)
        frame["carb_entry_g"] = frame["timestamp"].map(collapsed["carb_entry_g"])
        frame["carb_meal_time"] = frame["timestamp"].map(collapsed["carb_meal_time"])
        frame["carb_entry_time"] = frame["timestamp"].map(collapsed["carb_entry_time"])
    else:
        frame["carb_entry_g"] = np.nan
        frame["carb_meal_time"] = pd.NaT
        frame["carb_entry_time"] = pd.NaT

    frame["carb_meal_time"] = pd.to_datetime(frame["carb_meal_time"])
    frame["carb_entry_time"] = pd.to_datetime(frame["carb_entry_time"])
    return frame


def load_streams(data_dir):
    """Read the export CSVs; returns dict of DataFrames with parsed times."""
    streams = {}
    for name, ts_cols in [
        ("users", ["window_start", "window_end"]),
        ("cgm", ["cbg_timestamp"]),
        ("carbs", ["meal_time", "entry_time"]),
        ("boluses", ["bolus_timestamp"]),
        ("dosing", ["dd_timestamp"]),
    ]:
        df = pd.read_csv(os.path.join(data_dir, f"{name}.csv"))
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col])
        streams[name] = df
    return streams


def run_stage_a(data_dir=DEFAULT_DATA_DIR, out_dir=DEFAULT_OUT_DIR,
                only_user=None, run_model=True, label=None,
                n_sims=N_SIMS_DEFAULT, split=DEFAULT_SPLIT, note=""):
    streams = load_streams(data_dir)
    results = {}
    for uid in streams["users"]["_userId"]:
        if only_user and uid != only_user:
            continue
        print(f"\n=== {uid} ===")
        per_user = {
            name: df[df["_userId"] == uid]
            for name, df in streams.items() if name != "users"
        }
        frame = build_user_frame(
            per_user["cgm"], per_user["carbs"], per_user["boluses"], per_user["dosing"])
        validate_tick_frame(frame)
        n_days = len(frame) / (24 * 60 // TICK_MINUTES)
        print(f"  tick frame: {len(frame)} ticks ({n_days:.0f} days), "
              f"CGM coverage {frame['cgm'].notna().mean():.1%}, "
              f"iob coverage {frame['iob'].notna().mean():.1%}")

        if not run_model:
            results[uid] = {"frame": frame}
            continue

        res = run_mvp(frame, split=split)
        print(f"\n  split: {res['split']}")
        print("\n  weekly drift (inspect BEFORE trusting the time split):")
        print(res["drift"].to_string())
        print("\n  go/no-go comparison:")
        print(res["comparison"].to_string(index=False))

        user_out = os.path.join(out_dir, uid)
        os.makedirs(user_out, exist_ok=True)

        print(f"\n  metric suite ({n_sims} simulation replicates):")
        metrics = evaluate(res, n_sims=n_sims, verbose=True,
                           replicates_path=os.path.join(user_out, "replicates.csv"))
        print(f"\n  sim metrics: mean ± sd over {n_sims} replicates "
              "(per-replicate values -> replicates.csv):")
        print(metrics.to_string(index=False,
                                float_format=lambda v: f"{v:.4g}"))
        res["comparison"].to_csv(os.path.join(user_out, "comparison.csv"), index=False)
        res["diurnal"].to_csv(os.path.join(user_out, "diurnal.csv"))
        res["drift"].to_csv(os.path.join(user_out, "drift.csv"))
        res["simulated"].to_csv(os.path.join(user_out, "simulated_events.csv"), index=False)
        metrics.to_csv(os.path.join(user_out, "metrics.csv"), index=False)
        print(f"  outputs -> {user_out}/")

        if label:
            config = {"split": res["split"],
                      "features": res["hazards"]["features"],
                      "n_sims": n_sims, "base_seed": BASE_SEED_DEFAULT}
            history = append_history(metrics, label, uid, config,
                                     os.path.join(out_dir, HISTORY_FILENAME),
                                     note=note)
            print(f"  recorded as '{label}' in {history}")
        results[uid] = {"frame": frame, "result": res, "metrics": metrics}
    if label:
        print("\nmeta-analysis: python stage_a_metrics.py --report")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--user", default=None, help="only this (hashed) user id")
    parser.add_argument("--no-run", action="store_true",
                        help="build + validate frames only")
    parser.add_argument("--label", default=None,
                        help="record this run in the metrics history under "
                             "this iteration label (omit for throwaway runs)")
    parser.add_argument("--n-sims", type=int, default=N_SIMS_DEFAULT,
                        help="simulation replicates per user for the metric suite")
    parser.add_argument("--split", default=DEFAULT_SPLIT,
                        choices=["interleaved_weeks", "chronological"],
                        help="train/holdout split; changing it starts a new "
                             "comparison regime in the metrics history")
    parser.add_argument("--note", default="",
                        help="one-line description of what this iteration "
                             "changed; recorded with --label and shown in "
                             "the report + dashboard")
    args = parser.parse_args()
    run_stage_a(args.data_dir, args.out_dir, args.user,
                run_model=not args.no_run, label=args.label,
                n_sims=args.n_sims, split=args.split, note=args.note)
