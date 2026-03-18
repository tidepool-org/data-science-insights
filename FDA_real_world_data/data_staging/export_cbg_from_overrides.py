spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_override_cbg AS

SELECT
    c._userId,
    o.overridePreset,
    COALESCE(TRY_CAST(o.basalRateScaleFactor AS DOUBLE), -1) AS brsf,
    COALESCE(TRY_CAST(o.bg_target_low AS DOUBLE), -1) AS btl,
    COALESCE(TRY_CAST(o.bg_target_high AS DOUBLE), -1) AS bth,
    COALESCE(TRY_CAST(o.carbRatioScaleFactor AS DOUBLE), -1) AS crsf,
    COALESCE(TRY_CAST(o.insulinSensitivityScaleFactor AS DOUBLE), -1) AS issf,
    o.dosing_mode AS segment,
    c.cbg_mg_dl,
    c.cbg_timestamp,
    o.is_valid_name_only,
    o.is_valid_full
FROM dev.fda_510k_rwd.overrides_by_segment o
INNER JOIN dev.fda_510k_rwd.loop_cbg c
    ON o._userId = c._userId
    AND c.cbg_timestamp >= o.override_time
    AND c.cbg_timestamp <= o.override_time + o.duration * INTERVAL '1' SECOND + INTERVAL '2' HOUR
WHERE o.is_valid_name_only = TRUE
""")
