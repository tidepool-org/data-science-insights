"""Extract every preset override activation dataset-wide (PLN IR-1002; IR-2/IR-3).

Dataset-wide analog of export_overrides_from_transitions.py: same raw extraction
(`overridePreset IS NOT NULL`), same (_userId, override_time) dedup by latest
created_timestamp, same stated-vs-effective duration split — but no transition-
segment join. Divergences from overrides_by_segment:

- Effective duration = min(stated, gap to next override, end of the user's
  observed data) — the segment-end clip is replaced by an end-of-data clip at
  the day after the user's last `loop_recommendations` day.
- The five preset parameters are emitted numeric (TRY_CAST to DOUBLE); the
  transition table passes the scale factors through as strings.
- Adds `end_time` / `end_day` (activation window bounds), `has_own_target`,
  and `is_version_eligible` — the PLN IR-1002 §7.1 per-day version/date rule
  evaluated on the activation's start day.
- Restricted to Loop users (INNER JOIN on `loop_recommendations` users): BDDP
  override rows for users with no dosing records are out of scope, and the
  restriction guarantees the end-of-data clip always resolves (effective
  duration is never NULL).
"""

import argparse


CATALOG = "dev.fda_510k_rwd"

# mmol/L -> mg/dL conversion (staging-layer convention).
MMOL_TO_MGDL = 18.018

# Per-day version/date eligibility (PLN IR-1002 §7.1). Mirrors
# MAX_LOOP_VERSION_INT / MAX_SEG2_END_DATE in analysis/utils/data_loading.py.
# version_int = 0 marks an unparseable version and must fall to the date rule,
# not pass a bare `< MAX_LOOP_VERSION_INT`.
MAX_LOOP_VERSION_INT = 3_004_000  # Loop 3.4.0
MAX_DAY = "2024-07-13"            # Loop 3.4.0 GitHub release date


def run(
    spark,
    output_table=f"{CATALOG}.overrides_all",
    bddp_table="dev.default.bddp_sample_all_2",
    loop_recommendations_table=f"{CATALOG}.loop_recommendations",
):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    raw_overrides AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS override_time,
        CAST(TRY_CAST(time_string AS TIMESTAMP) AS DATE) AS override_day,
        overridePreset,
        TRY_CAST(basalRateScaleFactor AS DOUBLE) AS basalRateScaleFactor,
        bgTarget:low * {MMOL_TO_MGDL} AS bg_target_low,
        bgTarget:high * {MMOL_TO_MGDL} AS bg_target_high,
        TRY_CAST(carbRatioScaleFactor AS DOUBLE) AS carbRatioScaleFactor,
        TRY_CAST(insulinSensitivityScaleFactor AS DOUBLE) AS insulinSensitivityScaleFactor,
        TRY_CAST(duration AS BIGINT) AS duration,
        created_timestamp
      FROM {bddp_table}
      WHERE overridePreset IS NOT NULL
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    ),

    ranked_overrides AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY _userId, override_time
          ORDER BY created_timestamp DESC
        ) AS rn
      FROM raw_overrides
    ),

    deduped_overrides AS (
      SELECT
        _userId,
        override_time,
        override_day,
        overridePreset,
        basalRateScaleFactor,
        bg_target_low,
        bg_target_high,
        carbRatioScaleFactor,
        insulinSensitivityScaleFactor,
        duration
      FROM ranked_overrides
      WHERE rn = 1
    ),

    -- One row per Loop user with their last observed dosing day. The INNER
    -- JOIN below restricts overrides_all to this universe.
    user_last_day AS (
      SELECT _userId, MAX(day) AS last_day
      FROM {loop_recommendations_table}
      GROUP BY _userId
    ),

    -- Split the source duration into two columns:
    --   stated_duration — the duration as the user programmed it, untouched.
    --   duration        — the effective duration: starting a new override ends
    --                     the current one, so this is capped at the gap to the
    --                     user's next override (no next override → the stated
    --                     value stands). The next CTE also clips it to the end
    --                     of the user's observed data.
    -- NULL programmed duration (indefinite override, or unparseable) stays NULL
    -- in stated_duration only: LEAST skips NULLs, so `duration` falls back to
    -- the gap here (and to the end-of-data clip downstream) and is never NULL —
    -- an indefinite override reads as "ran until the next override / data end".
    overrides AS (
      SELECT
        _userId,
        override_time,
        override_day,
        overridePreset,
        basalRateScaleFactor,
        bg_target_low,
        bg_target_high,
        carbRatioScaleFactor,
        insulinSensitivityScaleFactor,
        duration AS stated_duration,
        LEAST(
          duration,
          COALESCE(
            UNIX_TIMESTAMP(
              LEAD(override_time) OVER (
                PARTITION BY _userId ORDER BY override_time
              )
            ) - UNIX_TIMESTAMP(override_time),
            duration
          )
        ) AS duration
      FROM deduped_overrides
    ),

    -- End-of-data clip: the dataset-wide replacement for the transition
    -- script's segment-end clip. Bounds every activation at the day after the
    -- user's last observed dosing day; an override recorded after that
    -- boundary (device clock skew, post-departure upload) clamps to 0 rather
    -- than going negative.
    clipped AS (
      SELECT
        o._userId,
        o.override_time,
        o.override_day,
        o.overridePreset,
        o.basalRateScaleFactor,
        o.bg_target_low,
        o.bg_target_high,
        o.carbRatioScaleFactor,
        o.insulinSensitivityScaleFactor,
        o.stated_duration,
        LEAST(
          o.duration,
          GREATEST(
            UNIX_TIMESTAMP(CAST(DATE_ADD(u.last_day, 1) AS TIMESTAMP))
              - UNIX_TIMESTAMP(o.override_time),
            0
          )
        ) AS duration
      FROM overrides o
      INNER JOIN user_last_day u
        ON o._userId = u._userId
    )

    SELECT
      c._userId,
      c.override_time,
      c.override_day,
      TIMESTAMPADD(SECOND, c.duration, c.override_time) AS end_time,
      -- end_day is the date of the activation's LAST second, so an activation
      -- ending exactly at midnight does not claim the next day; a 0-duration
      -- activation gets end_day = override_day.
      CAST(
        TIMESTAMPADD(SECOND, GREATEST(c.duration - 1, 0), c.override_time)
        AS DATE
      ) AS end_day,
      c.overridePreset,
      c.basalRateScaleFactor,
      c.bg_target_low,
      c.bg_target_high,
      c.carbRatioScaleFactor,
      c.insulinSensitivityScaleFactor,
      c.stated_duration,
      c.duration,
      c.bg_target_low IS NOT NULL AS has_own_target,
      -- Version-first / date-fallback eligibility on the activation's start
      -- day (PLN IR-1002 §7.1): a stated version decides when parseable; an
      -- absent or unparseable (version_int = 0) version falls to the date rule.
      CASE
        WHEN r.version_int IS NOT NULL AND r.version_int > 0
             AND r.version_int < {MAX_LOOP_VERSION_INT} THEN TRUE
        WHEN (r.version_int IS NULL OR r.version_int = 0)
             AND c.override_day < DATE '{MAX_DAY}' THEN TRUE
        ELSE FALSE
      END AS is_version_eligible
    FROM clipped c
    LEFT JOIN {loop_recommendations_table} r
      ON c._userId = r._userId
     AND c.override_day = r.day
    ORDER BY c._userId, c.override_time
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--output_table", default=f"{CATALOG}.overrides_all")
    _args, _ = _parser.parse_known_args()

    run(spark, output_table=_args.output_table, bddp_table=_args.bddp_table)
