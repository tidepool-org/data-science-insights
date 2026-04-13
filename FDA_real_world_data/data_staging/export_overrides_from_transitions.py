import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.overrides_by_segment",
    bddp_table="dev.default.bddp_sample_all_2",
    transition_segments_table=f"{CATALOG}.valid_transition_segments",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

    WITH

    overrides AS (
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
        duration
      FROM {bddp_table}
      WHERE overridePreset IS NOT NULL
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
        o.duration,

        CASE
          WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
        END AS segment,

        CASE
          WHEN o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'temp_basal'
          WHEN o.override_day BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'autobolus'
        END AS dosing_mode,

        t.tb_to_ab_seg1_start,
        t.tb_to_ab_seg1_end,
        t.tb_to_ab_seg2_start,
        t.tb_to_ab_seg2_end

      FROM {transition_segments_table} t
      INNER JOIN overrides o ON t._userId = o._userId
      WHERE o.override_day BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
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
        MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2_name
      FROM config_counts_name_only
      GROUP BY _userId, preset_key
    ),

    config_validity AS (
      SELECT
        _userId, preset_key,
        brsf, btl, bth, crsf, issf,
        MAX(CASE WHEN segment = 'tb_to_ab_seg1' THEN n ELSE 0 END) AS n_seg1,
        MAX(CASE WHEN segment = 'tb_to_ab_seg2' THEN n ELSE 0 END) AS n_seg2
      FROM config_counts
      GROUP BY _userId, preset_key
        , brsf, btl, bth, crsf, issf
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
      vn.n_seg1_name,
      vn.n_seg2_name,
      CASE WHEN vn.n_seg1_name >= 2 AND vn.n_seg2_name >= 2 THEN TRUE ELSE FALSE END AS is_valid_name_only,
      v.n_seg1,
      v.n_seg2,
      CASE WHEN v.n_seg1 >= 2 AND v.n_seg2 >= 2 THEN TRUE ELSE FALSE END AS is_valid_full
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
    ORDER BY o._userId, o.override_time
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--bddp_table", default="dev.default.bddp_sample_all_2")
    _args, _ = _parser.parse_known_args()

    run(spark, bddp_table=_args.bddp_table)
