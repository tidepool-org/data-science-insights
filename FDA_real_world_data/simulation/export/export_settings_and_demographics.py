"""Export per-user time-weighted settings and demographics for every exported
simulation user, keyed on the anonymized rwd_user_id.

Inputs:
  - simulation/data/scenarios/user_id_mapping.csv  (rwd_user_id, _userId, target_day)
  - simulation/data/pump_settings.csv              (basal/isf/cir/target schedules)
  - dev.fda_510k_rwd.valid_transition_segments    (demographics)

Output: simulation/data/scenarios/settings_demographics.csv
  rwd_user_id, _userId, target_day,
  gender, age_years, years_lwd,
  avg_basal_rate, avg_isf, avg_cir, avg_target_low, avg_target_high

Each `*_schedule` field in pump_settings.csv is a JSON array of
{start_time: "HH:MM:SS", value: ...} segments covering 24h. The time-weighted
average weights each segment's value by its duration (next_start - start,
with the last segment wrapping to 24:00:00) and divides by 86400s. Target
schedule entries have low/high instead of value, averaged independently.
A user missing a schedule (or with empty entries) gets NaN for that field.
"""

import argparse
import json
import os

import pandas as pd


CATALOG = "dev.fda_510k_rwd"
_SIMULATION_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/FDA_real_world_data/simulation"
)
DEFAULT_SCENARIOS_DIR = os.path.join(_SIMULATION_DIR, "data", "scenarios")
DEFAULT_PUMP_SETTINGS_PATH = os.path.join(_SIMULATION_DIR, "data", "pump_settings.csv")
DEFAULT_SEGMENTS_TABLE = f"{CATALOG}.valid_transition_segments"
SECONDS_PER_DAY = 24 * 3600


def _hms_to_seconds(hms):
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def _time_weighted_average(segments_json, value_keys=("value",)):
    """Time-weighted average of a 24h schedule.

    segments_json: JSON string, list of {start_time: "HH:MM:SS", <value_keys>: ...}
    value_keys: tuple of keys to average; returns one float per key (in order).
    Returns a tuple of NaN if the schedule is empty or unparseable.
    """
    nan_result = tuple(float("nan") for _ in value_keys)
    if not isinstance(segments_json, str):
        return nan_result
    try:
        segs = json.loads(segments_json)
    except json.JSONDecodeError:
        return nan_result
    if not segs:
        return nan_result

    segs = sorted(segs, key=lambda s: _hms_to_seconds(s["start_time"]))
    starts = [_hms_to_seconds(s["start_time"]) for s in segs]
    durations = [
        (starts[i + 1] if i + 1 < len(starts) else SECONDS_PER_DAY) - starts[i]
        for i in range(len(starts))
    ]
    return tuple(
        sum(s[k] * d for s, d in zip(segs, durations)) / SECONDS_PER_DAY
        for k in value_keys
    )


def _summarize_pump_settings(pump_settings_df):
    rows = []
    for _, r in pump_settings_df.iterrows():
        (avg_basal,) = _time_weighted_average(r["basal_schedule"])
        (avg_isf,) = _time_weighted_average(r["isf_schedule"])
        (avg_cir,) = _time_weighted_average(r["cir_schedule"])
        avg_low, avg_high = _time_weighted_average(
            r["target_schedule"], value_keys=("low", "high")
        )
        rows.append({
            "_userId": r["_userId"],
            "avg_basal_rate": round(avg_basal, 3) if pd.notna(avg_basal) else float("nan"),
            "avg_isf": round(avg_isf, 1) if pd.notna(avg_isf) else float("nan"),
            "avg_cir": round(avg_cir, 2) if pd.notna(avg_cir) else float("nan"),
            "avg_target_low": round(avg_low, 1) if pd.notna(avg_low) else float("nan"),
            "avg_target_high": round(avg_high, 1) if pd.notna(avg_high) else float("nan"),
        })
    return pd.DataFrame(rows)


def run(
    spark,
    scenarios_dir=DEFAULT_SCENARIOS_DIR,
    pump_settings_path=DEFAULT_PUMP_SETTINGS_PATH,
    segments_table=DEFAULT_SEGMENTS_TABLE,
):
    mapping_path = os.path.join(scenarios_dir, "user_id_mapping.csv")
    if not os.path.exists(mapping_path):
        raise FileNotFoundError(
            f"{mapping_path} not found — run build_scenario_json.py first."
        )
    if not os.path.exists(pump_settings_path):
        raise FileNotFoundError(
            f"{pump_settings_path} not found — run export_single_user_day.py first."
        )

    mapping = pd.read_csv(mapping_path, dtype={"_userId": str, "rwd_user_id": str})
    mapping["target_day"] = pd.to_datetime(mapping["target_day"]).dt.date

    pump_settings = pd.read_csv(pump_settings_path, dtype={"_userId": str})
    settings_summary = _summarize_pump_settings(pump_settings)

    spark.createDataFrame(
        mapping[["_userId", "target_day"]]
    ).createOrReplaceTempView("_scenario_users")

    demographics_df = spark.sql(f"""
    --begin-sql
    SELECT
      v._userId,
      v.tb_to_ab_seg1_start AS target_day,
      v.gender,
      TRY_CAST(v.tb_to_ab_age_years AS DOUBLE) AS age_years,
      TRY_CAST(v.tb_to_ab_years_lwd AS DOUBLE) AS years_lwd
    FROM {segments_table} v
    INNER JOIN _scenario_users s
      ON v._userId = s._userId
      AND v.tb_to_ab_seg1_start = s.target_day
    WHERE v.segment_rank = 1
    ;
    """).toPandas()
    demographics_df["target_day"] = pd.to_datetime(demographics_df["target_day"]).dt.date

    out = (
        mapping
        .merge(demographics_df, on=["_userId", "target_day"], how="left")
        .merge(settings_summary, on="_userId", how="left")
    )
    out = out[[
        "rwd_user_id", "_userId", "target_day",
        "gender", "age_years", "years_lwd",
        "avg_basal_rate", "avg_isf", "avg_cir",
        "avg_target_low", "avg_target_high",
    ]]
    out = out.sort_values("rwd_user_id").reset_index(drop=True)

    out_path = os.path.join(scenarios_dir, "settings_demographics.csv")
    out.to_csv(out_path, index=False)

    matched_demo = out["gender"].notna().sum()
    matched_settings = out["avg_basal_rate"].notna().sum()
    print(
        f"Wrote {len(out)} rows to {out_path} "
        f"({matched_demo} with demographics, "
        f"{matched_settings} with time-weighted settings)."
    )
    return out


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--scenarios_dir", default=DEFAULT_SCENARIOS_DIR)
    _parser.add_argument("--pump_settings_path", default=DEFAULT_PUMP_SETTINGS_PATH)
    _parser.add_argument("--segments_table", default=DEFAULT_SEGMENTS_TABLE)
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        scenarios_dir=_args.scenarios_dir,
        pump_settings_path=_args.pump_settings_path,
        segments_table=_args.segments_table,
    )
