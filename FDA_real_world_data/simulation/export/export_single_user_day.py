"""Snapshot simulator inputs for one user-day per user.

For every user with a valid TB->AB transition segment, picks the first day of
the rank-1 temp-basal segment (tb_to_ab_seg1_start) and emits four CSVs:

  - carbs.csv              : all food entries on the target day
  - correction_boluses.csv : all normal-subType bolus records on the target day
  - cgm.csv                : all cbg readings on the target day, deduped to
                             5-minute buckets and converted to mg/dL
  - pump_settings.csv      : one row per user with JSON-encoded 24h
                             schedules for basal (U/hr), isf (mg/dL/U),
                             cir (g/U), and target range (mg/dL low/high)

TZ semantics: BDDP `time_string` is ISO-8601 ending in `Z` (UTC). Pump-settings
schedules, by contrast, are keyed in milliseconds-since-midnight of the user's
*local* day. To keep the two frames aligned, all event timestamps written to
the CSVs (cbg, carb, bolus) are shifted into user-local time via BDDP's
`timezoneOffset` column (minutes from UTC). All three queries use a per-user
offset (via a `user_tz` CTE that picks the latest non-NULL `timezoneOffset`
at/before `target_day + 36h`) — BDDP populates `timezoneOffset` inconsistently
on food/bolus rows, so we rely on any record with a non-NULL offset rather
than the per-row value. Sub-day TZ changes across a sim are not modelled —
one offset per user. `target_day` is still the UTC calendar day of the first
autobolus event — kept as-is because it is just a user-level index, not a
time-of-day anchor.
"""

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd


CATALOG = "dev.fda_510k_rwd"
DEFAULT_SEGMENTS_TABLE = f"{CATALOG}.valid_transition_segments"
DEFAULT_CBG_TABLE = f"{CATALOG}.loop_cbg"
DEFAULT_BDDP_TABLE = "dev.default.bddp_sample_all_2"
DEFAULT_OUTPUT_DIR = "FDA_real_world_data/simulation/data"
MMOL_TO_MGDL = 18.016


def _all_segments(schedule_json):
    """Return the full list of schedule segments sorted by start time.

    Accepts either shape BDDP uses for pump-settings schedule fields:
      - dict-of-named-schedules, e.g. {"Default": [{start: 0, ...}, ...]}
        or {"standard": [...], "pattern a": []}; prefers 'Default', else
        the first non-empty schedule.
      - flat segment array, e.g. [{start: 0, ...}, ...] (used by the
        singular columns insulinSensitivity / carbRatio / bgTarget).

    Returns [] if the input is missing, unparseable, or empty.
    """
    if not schedule_json:
        return []
    try:
        parsed = json.loads(schedule_json)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(parsed, list):
        segments = parsed
    elif isinstance(parsed, dict):
        segments = parsed.get("Default") or next(
            (v for v in parsed.values() if v), None
        )
    else:
        return []
    if not segments:
        return []
    return sorted(segments, key=lambda s: s.get("start", 0))


def _ms_to_hms(ms):
    total_s = int(ms // 1000)
    h, rem = divmod(total_s, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _flatten_pump_settings(raw_df):
    """Expand raw pump-settings JSON into the full 24h settings schedules.

    Input columns: _userId, basalSchedules,
                   bgTargets / bgTarget,
                   insulinSensitivities / insulinSensitivity,
                   carbRatios / carbRatio
    Output columns: _userId and four JSON-encoded schedules:
      - basal_schedule   : [{"start_time": "HH:MM:SS", "value": U/hr}, ...]
      - isf_schedule     : [{"start_time": "HH:MM:SS", "value": mg/dL/U}, ...]
      - cir_schedule     : [{"start_time": "HH:MM:SS", "value": g/U}, ...]
      - target_schedule  : [{"start_time": "HH:MM:SS",
                             "low": mg/dL, "high": mg/dL}, ...]

    For ISF / CIR / target, BDDP populates either the plural column
    (dict-of-named-schedules) or the singular column (flat segment array)
    depending on how the pump reported the setting. Plural is preferred;
    singular is the fallback.
    """
    def pick(row, *keys):
        for k in keys:
            segs = _all_segments(row.get(k))
            if segs:
                return segs
        return []

    rows = []
    for _, r in raw_df.iterrows():
        br_segs = pick(r, "basalSchedules")
        tgt_segs = pick(r, "bgTargets", "bgTarget")
        isf_segs = pick(r, "insulinSensitivities", "insulinSensitivity")
        cir_segs = pick(r, "carbRatios", "carbRatio")

        # Drop segments with missing values so downstream build_scenario_json
        # always receives a clean schedule.
        basal = [
            {"start_time": _ms_to_hms(s.get("start", 0)), "value": s["rate"]}
            for s in br_segs if s.get("rate") is not None
        ]
        isf = [
            {
                "start_time": _ms_to_hms(s.get("start", 0)),
                "value": round(s["amount"] * MMOL_TO_MGDL, 1),
            }
            for s in isf_segs if s.get("amount") is not None
        ]
        cir = [
            {"start_time": _ms_to_hms(s.get("start", 0)), "value": s["amount"]}
            for s in cir_segs if s.get("amount") is not None
        ]
        target = [
            {
                "start_time": _ms_to_hms(s.get("start", 0)),
                "low": round(s["low"] * MMOL_TO_MGDL, 1),
                "high": round(s["high"] * MMOL_TO_MGDL, 1),
            }
            for s in tgt_segs
            if s.get("low") is not None and s.get("high") is not None
        ]

        rows.append({
            "_userId": r["_userId"],
            "basal_schedule": json.dumps(basal),
            "isf_schedule": json.dumps(isf),
            "cir_schedule": json.dumps(cir),
            "target_schedule": json.dumps(target),
        })
    return pd.DataFrame(
        rows,
        columns=[
            "_userId",
            "basal_schedule",
            "isf_schedule",
            "cir_schedule",
            "target_schedule",
        ],
    )


def run(
    spark,
    segments_table=DEFAULT_SEGMENTS_TABLE,
    bddp_table=DEFAULT_BDDP_TABLE,
    cbg_table=DEFAULT_CBG_TABLE,
    output_dir=DEFAULT_OUTPUT_DIR,
):
    # Precompute the per-user TZ offset once. toPandas() materializes the
    # BDDP scan in the driver; we then re-register the small (~one row per
    # user) DataFrame as a temp view so the three parallel event queries
    # below each join a trivially-sized table rather than re-scanning BDDP.
    # CACHE TABLE would be simpler but is not supported on serverless compute.
    tz_pdf = spark.sql(f"""
    SELECT
      b._userId,
      MAX_BY(b.timezoneOffset, TRY_CAST(b.time_string AS TIMESTAMP)) AS tz_offset_min
    FROM {bddp_table} b
    INNER JOIN {segments_table} t
      ON b._userId = t._userId AND t.segment_rank = 1
    WHERE b.timezoneOffset IS NOT NULL
      AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(b.time_string AS TIMESTAMP)
            <= CAST(t.tb_to_ab_seg1_start AS TIMESTAMP) + INTERVAL 36 HOURS
    GROUP BY b._userId
    """).toPandas()
    spark.createDataFrame(tz_pdf).createOrReplaceTempView("_sim_user_tz")

    # Sim window: [target_day 12:00, target_day+1 12:00). Carbs and boluses
    # are pulled over that 24h span. timezoneOffset is frequently NULL on
    # BDDP food/bolus rows, so all three event queries use the per-user
    # offset from _sim_user_tz rather than the per-row value.
    carbs_sql = f"""
    WITH target_days AS (
      SELECT _userId, tb_to_ab_seg1_start AS target_day
      FROM {segments_table}
      WHERE segment_rank = 1
    ),
    carb_entries AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS carb_utc_ts,
        TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE) AS carb_grams
      FROM {bddp_table}
      WHERE type = 'food'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
        AND nutrition IS NOT NULL
    )
    SELECT DISTINCT
      c._userId,
      t.target_day,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, c.carb_utc_ts) AS carb_timestamp,
      c.carb_grams
    FROM carb_entries c
    INNER JOIN target_days t ON c._userId = t._userId
    INNER JOIN _sim_user_tz u ON c._userId = u._userId
    WHERE TIMESTAMPADD(MINUTE, u.tz_offset_min, c.carb_utc_ts)
            >= CAST(t.target_day AS TIMESTAMP) + INTERVAL 12 HOURS
      AND TIMESTAMPADD(MINUTE, u.tz_offset_min, c.carb_utc_ts)
            <  CAST(t.target_day AS TIMESTAMP) + INTERVAL 36 HOURS
    ORDER BY c._userId, carb_timestamp
    """

    # Only keep boluses with a nearby (±15s) dosingDecision with
    # reason='normalBolus' — these are user-initiated. Autoboluses (no nearby
    # normalBolus DD) are excluded so the sim can regenerate them itself.
    # Mirrors the directional-match used in export_loop_recommendations.py.
    # TODO: swap this raw BDDP match for a clean "user_boluses" table once
    # one exists in dev.fda_510k_rwd.
    correction_boluses_sql = f"""
    WITH target_days AS (
      SELECT _userId, tb_to_ab_seg1_start AS target_day
      FROM {segments_table}
      WHERE segment_rank = 1
    ),
    normal_bolus_decisions AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS nb_ts
      FROM {bddp_table}
      WHERE type = 'dosingDecision'
        AND reason = 'normalBolus'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    )
    SELECT
      b._userId,
      t.target_day,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, TRY_CAST(b.time_string AS TIMESTAMP))
        AS bolus_timestamp,
      COALESCE(
        TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
        TRY_CAST(b.normal AS DOUBLE)
      ) AS bolus_amount
    FROM {bddp_table} b
    INNER JOIN target_days t ON b._userId = t._userId
    INNER JOIN _sim_user_tz u ON b._userId = u._userId
    WHERE b.type = 'bolus'
      AND b.subType = 'normal'
      AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
      AND TIMESTAMPADD(MINUTE, u.tz_offset_min, TRY_CAST(b.time_string AS TIMESTAMP))
            >= CAST(t.target_day AS TIMESTAMP) + INTERVAL 12 HOURS
      AND TIMESTAMPADD(MINUTE, u.tz_offset_min, TRY_CAST(b.time_string AS TIMESTAMP))
            <  CAST(t.target_day AS TIMESTAMP) + INTERVAL 36 HOURS
      AND EXISTS (
        SELECT 1
        FROM normal_bolus_decisions nb
        WHERE nb._userId = b._userId
          AND ABS(TIMESTAMPDIFF(SECOND, nb.nb_ts, TRY_CAST(b.time_string AS TIMESTAMP))) <= 15
      )
    ORDER BY b._userId, bolus_timestamp
    """

    # CGM spans target_day 00:00 -> target_day+1 12:00 so build_scenario_json
    # has pre-sim history (00:00 -> sim_start) and in-sim actual cbgs for the
    # runner's overlay (sim_start -> sim_end). loop_cbg drops timezoneOffset
    # upstream, so we join _sim_user_tz (precomputed above) to shift cbg UTC
    # timestamps into user-local.
    cgm_sql = f"""
    WITH target_days AS (
      SELECT _userId, tb_to_ab_seg1_start AS target_day
      FROM {segments_table}
      WHERE segment_rank = 1
    )
    SELECT
      c._userId,
      t.target_day,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, c.cbg_timestamp) AS cbg_timestamp,
      c.cbg_mg_dl
    FROM {cbg_table} c
    INNER JOIN target_days t ON c._userId = t._userId
    INNER JOIN _sim_user_tz u ON c._userId = u._userId
    WHERE c.is_plausible
      AND TIMESTAMPADD(MINUTE, u.tz_offset_min, c.cbg_timestamp)
            >= CAST(t.target_day AS TIMESTAMP)
      AND TIMESTAMPADD(MINUTE, u.tz_offset_min, c.cbg_timestamp)
            <  CAST(t.target_day AS TIMESTAMP) + INTERVAL 36 HOURS
    ORDER BY c._userId, cbg_timestamp
    """

    pump_settings_sql = f"""
    WITH target_days AS (
      SELECT _userId, tb_to_ab_seg1_start AS target_day
      FROM {segments_table}
      WHERE segment_rank = 1
    ),
    settings_before_target AS (
      SELECT
        ps._userId,
        TRY_CAST(ps.time_string AS TIMESTAMP) AS settings_time,
        ps.basalSchedules,
        ps.bgTargets,
        ps.bgTarget,
        ps.insulinSensitivities,
        ps.insulinSensitivity,
        ps.carbRatios,
        ps.carbRatio
      FROM {bddp_table} ps
      INNER JOIN target_days t
        ON ps._userId = t._userId
      WHERE ps.type = 'pumpSettings'
        AND TRY_CAST(ps.time_string AS TIMESTAMP) IS NOT NULL
        AND CAST(TRY_CAST(ps.time_string AS TIMESTAMP) AS DATE) <= t.target_day
    )
    SELECT
      _userId,
      MAX_BY(CAST(basalSchedules AS STRING),
             CASE WHEN basalSchedules IS NOT NULL THEN settings_time END) AS basalSchedules,
      MAX_BY(CAST(bgTargets AS STRING),
             CASE WHEN bgTargets IS NOT NULL THEN settings_time END) AS bgTargets,
      MAX_BY(CAST(bgTarget AS STRING),
             CASE WHEN bgTarget IS NOT NULL THEN settings_time END) AS bgTarget,
      MAX_BY(CAST(insulinSensitivities AS STRING),
             CASE WHEN insulinSensitivities IS NOT NULL THEN settings_time END) AS insulinSensitivities,
      MAX_BY(CAST(insulinSensitivity AS STRING),
             CASE WHEN insulinSensitivity IS NOT NULL THEN settings_time END) AS insulinSensitivity,
      MAX_BY(CAST(carbRatios AS STRING),
             CASE WHEN carbRatios IS NOT NULL THEN settings_time END) AS carbRatios,
      MAX_BY(CAST(carbRatio AS STRING),
             CASE WHEN carbRatio IS NOT NULL THEN settings_time END) AS carbRatio
    FROM settings_before_target
    GROUP BY _userId
    ORDER BY _userId
    """

    # Fire all four pulls in parallel — each .toPandas() is its own Spark
    # action, so the cluster runs them concurrently.
    def _fetch(sql):
        return spark.sql(sql).toPandas()

    with ThreadPoolExecutor(max_workers=4) as pool:
        carbs_fut = pool.submit(_fetch, carbs_sql)
        boluses_fut = pool.submit(_fetch, correction_boluses_sql)
        cgm_fut = pool.submit(_fetch, cgm_sql)
        settings_fut = pool.submit(_fetch, pump_settings_sql)
        carbs_df = carbs_fut.result()
        correction_boluses_df = boluses_fut.result()
        cgm_df = cgm_fut.result()
        raw_pump_settings_df = settings_fut.result()
    pump_settings_df = _flatten_pump_settings(raw_pump_settings_df)

    target_user_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM {segments_table} WHERE segment_rank = 1"
    ).collect()[0]["n"]
    users_missing_pump_settings = target_user_count - len(pump_settings_df)
    if users_missing_pump_settings > 0:
        print(
            f"WARNING: {users_missing_pump_settings} of {target_user_count} users "
            f"had no pumpSettings record at or before their target day; "
            f"they are absent from pump_settings.csv."
        )

    os.makedirs(output_dir, exist_ok=True)
    carbs_df.to_csv(os.path.join(output_dir, "carbs.csv"), index=False)
    correction_boluses_df.to_csv(os.path.join(output_dir, "correction_boluses.csv"), index=False)
    cgm_df.to_csv(os.path.join(output_dir, "cgm.csv"), index=False)
    pump_settings_df.to_csv(os.path.join(output_dir, "pump_settings.csv"), index=False)

    print(
        f"Wrote {len(carbs_df)} carb rows, {len(correction_boluses_df)} correction-bolus rows, "
        f"{len(cgm_df)} cgm rows, {len(pump_settings_df)} pump-settings rows to {output_dir}/"
    )


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--segments_table", default=DEFAULT_SEGMENTS_TABLE)
    _parser.add_argument("--bddp_table", default=DEFAULT_BDDP_TABLE)
    _parser.add_argument("--cbg_table", default=DEFAULT_CBG_TABLE)
    _parser.add_argument("--output_dir", default=DEFAULT_OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        segments_table=_args.segments_table,
        bddp_table=_args.bddp_table,
        cbg_table=_args.cbg_table,
        output_dir=_args.output_dir,
    )
