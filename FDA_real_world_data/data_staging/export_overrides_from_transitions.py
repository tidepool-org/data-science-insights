import argparse


CATALOG = "dev.fda_510k_rwd"

# Window before override_time to search for the activation's starting CBG.
# Mirrors Analysis 8-3's CBG_LOOKBACK_MINUTES so both analyses use the same
# starting-glucose definition.
STARTING_GLUCOSE_LOOKBACK_MINUTES = 30

# Starting-glucose inclusion thresholds (mg/dL). Analysis-side code reads
# these from analysis/utils/constants.py (STARTING_GLUCOSE_LOW/HIGH); the
# staging defaults below match. Override via run() if they ever diverge.
_STARTING_GLUCOSE_LOW = 70
_STARTING_GLUCOSE_HIGH = 180


def run(
    spark,
    output_table=f"{CATALOG}.overrides_by_segment",
    bddp_table="dev.default.bddp_sample_all_2",
    transition_segments_table=f"{CATALOG}.valid_transition_segments",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
    starting_glucose_low=_STARTING_GLUCOSE_LOW,
    starting_glucose_high=_STARTING_GLUCOSE_HIGH,
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
        basalRateScaleFactor,
        bgTarget:low * 18.018 AS bg_target_low,
        bgTarget:high * 18.018 AS bg_target_high,
        carbRatioScaleFactor,
        insulinSensitivityScaleFactor,
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

    -- Truncate stated duration to the gap until the next override for the same user.
    -- If the user starts another override before this one's stated duration elapses,
    -- the effective duration is the gap; otherwise the stated duration stands.
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

    overrides_with_segments AS (
      SELECT
        t._userId,
        o.override_time,
        o.overridePreset,
        o.basalRateScaleFactor,
        o.bg_target_low,
        o.bg_target_high,
        o.carbRatioScaleFactor,
        o.insulinSensitivityScaleFactor,

        -- Clip the override's duration to the end of its tagged segment.
        -- Combined with the gap-to-next truncation upstream, this bounds
        -- per-override duration by min(stated, gap_to_next, time_to_segment_end),
        -- so total preset time per user-segment cannot exceed 14 days × 86400s.
        LEAST(
          o.duration,
          CASE
            WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN
              UNIX_TIMESTAMP(CAST(DATE_ADD(t.tb_to_ab_seg1_end, 1) AS TIMESTAMP))
                - UNIX_TIMESTAMP(o.override_time)
            WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN
              UNIX_TIMESTAMP(CAST(DATE_ADD(t.tb_to_ab_seg2_end, 1) AS TIMESTAMP))
                - UNIX_TIMESTAMP(o.override_time)
            WHEN o.override_day BETWEEN t.tb_to_ab_seg3_start AND t.tb_to_ab_seg3_end THEN
              UNIX_TIMESTAMP(CAST(DATE_ADD(t.tb_to_ab_seg3_end, 1) AS TIMESTAMP))
                - UNIX_TIMESTAMP(o.override_time)
          END
        ) AS duration,

        CASE
          WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg3_start AND t.tb_to_ab_seg3_end THEN 'tb_to_ab_seg3'
        END AS segment,

        -- dosing_mode collapses seg2 and seg3 into a single 'autobolus' label
        -- because both phases are post-transition. Analysis 8-2 uses `segment`
        -- (not `dosing_mode`) when it needs to distinguish the initial vs
        -- second AB period (Tables 8.2b vs 8.2c).
        CASE
          WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'temp_basal'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'autobolus'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg3_start AND t.tb_to_ab_seg3_end THEN 'autobolus'
        END AS dosing_mode,

        t.tb_to_ab_seg1_start,
        t.tb_to_ab_seg1_end,
        t.tb_to_ab_seg2_start,
        t.tb_to_ab_seg2_end,
        t.tb_to_ab_seg3_start,
        t.tb_to_ab_seg3_end

      FROM {transition_segments_table} t
      INNER JOIN overrides o ON t._userId = o._userId
      WHERE t.segment_rank = 1
        AND o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg3_end
    ),

    overrides_clean AS (
      SELECT
        *,
        COALESCE(overridePreset, '__none__') AS preset_key,
        COALESCE(TRY_CAST(basalRateScaleFactor AS DOUBLE), -1) AS brsf,
        COALESCE(TRY_CAST(bg_target_low AS DOUBLE), -1) AS btl,
        COALESCE(TRY_CAST(bg_target_high AS DOUBLE), -1) AS bth,
        COALESCE(TRY_CAST(carbRatioScaleFactor AS DOUBLE), -1) AS crsf,
        COALESCE(TRY_CAST(insulinSensitivityScaleFactor AS DOUBLE), -1) AS issf
      FROM overrides_with_segments
      WHERE segment IS NOT NULL
    ),

    config_counts AS (
      SELECT
        _userId, preset_key, segment,
           brsf, btl, bth, crsf, issf,
        COUNT(*) AS n
      FROM overrides_clean
      GROUP BY _userId, preset_key, segment
       , brsf, btl, bth, crsf, issf
    ),

    config_counts_name_only AS (
      SELECT
        _userId, preset_key, segment,
        COUNT(*) AS n
      FROM overrides_clean
      GROUP BY _userId, preset_key, segment
    ),

    config_validity_name_only AS (
      SELECT
        _userId, preset_key,
        MAX(CASE WHEN segment = 'tb_to_ab_seg1' THEN n ELSE 0 END) AS n_seg1_name,
        MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2_name,
        MAX(CASE WHEN segment = 'tb_to_ab_seg3' THEN n ELSE 0 END) AS n_seg3_name
      FROM config_counts_name_only
      GROUP BY _userId, preset_key
    ),

    config_validity AS (
      SELECT
        _userId, preset_key,
        brsf, btl, bth, crsf, issf,
        MAX(CASE WHEN segment = 'tb_to_ab_seg1' THEN n ELSE 0 END) AS n_seg1,
        MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2,
        MAX(CASE WHEN segment = 'tb_to_ab_seg3' THEN n ELSE 0 END) AS n_seg3
      FROM config_counts
      GROUP BY _userId, preset_key
        , brsf, btl, bth, crsf, issf
    ),

    -- Starting glucose: the CBG reading closest to (but at or before) the
    -- override activation, within a fixed lookback window. Activations with
    -- no CBG in the window get starting_glucose = NULL and fail the
    -- in-range check (treated as failed inclusion).
    starting_cbg_ranked AS (
      SELECT
        o._userId,
        o.override_time,
        c.cbg_mg_dl,
        ROW_NUMBER() OVER (
          PARTITION BY o._userId, o.override_time
          ORDER BY c.cbg_timestamp DESC
        ) AS rn
      FROM overrides_clean o
      INNER JOIN {loop_cbg_table} c
        ON c._userId = o._userId
       AND c.cbg_timestamp BETWEEN
             o.override_time - INTERVAL '{STARTING_GLUCOSE_LOOKBACK_MINUTES}' MINUTE
             AND o.override_time
    ),

    starting_cbg AS (
      SELECT _userId, override_time, cbg_mg_dl AS starting_glucose
      FROM starting_cbg_ranked
      WHERE rn = 1
    )

    SELECT
      o._userId,
      o.override_time,
      o.segment,
      o.dosing_mode,
      o.overridePreset,
      o.basalRateScaleFactor,
      o.bg_target_low,
      o.bg_target_high,
      o.carbRatioScaleFactor,
      o.insulinSensitivityScaleFactor,
      o.duration,
      o.tb_to_ab_seg1_start,
      o.tb_to_ab_seg1_end,
      o.tb_to_ab_seg2_start,
      o.tb_to_ab_seg2_end,
      o.tb_to_ab_seg3_start,
      o.tb_to_ab_seg3_end,
      vn.n_seg1_name,
      vn.n_seg2_name,
      vn.n_seg3_name,
      -- Each AB segment has its own pairing with TB. is_valid_name_only_seg2
      -- gates Analysis 8-2 Table 8.2b (TB vs initial AB); _seg3 gates Table 8.2c
      -- (TB vs second AB). A user can satisfy one and not the other.
      CASE WHEN vn.n_seg1_name >= 2 AND vn.n_seg2_name >= 2 THEN TRUE ELSE FALSE END AS is_valid_name_only_seg2,
      CASE WHEN vn.n_seg1_name >= 2 AND vn.n_seg3_name >= 2 THEN TRUE ELSE FALSE END AS is_valid_name_only_seg3,
      v.n_seg1,
      v.n_seg2,
      v.n_seg3,
      CASE WHEN v.n_seg1 >= 2 AND v.n_seg2 >= 2 THEN TRUE ELSE FALSE END AS is_valid_full_seg2,
      CASE WHEN v.n_seg1 >= 2 AND v.n_seg3 >= 2 THEN TRUE ELSE FALSE END AS is_valid_full_seg3,
      sc.starting_glucose,
      CASE
        WHEN sc.starting_glucose BETWEEN {starting_glucose_low} AND {starting_glucose_high}
          THEN TRUE
        ELSE FALSE
      END AS is_starting_glucose_in_range
    FROM overrides_clean o
    JOIN config_validity_name_only vn
      ON o._userId = vn._userId
      AND o.preset_key = vn.preset_key
    JOIN config_validity v
      ON o._userId = v._userId
      AND o.preset_key = v.preset_key
      AND o.brsf = v.brsf
      AND o.btl = v.btl
      AND o.bth = v.bth
      AND o.crsf = v.crsf
      AND o.issf = v.issf
    LEFT JOIN starting_cbg sc
      ON sc._userId = o._userId
      AND sc.override_time = o.override_time
    ORDER BY o._userId, o.override_time
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(spark, bddp_table=_args.bddp_table)
