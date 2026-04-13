import argparse


CATALOG = "dev.fda_510k_rwd"


def run(
    spark,
    output_table=f"{CATALOG}.valid_override_cbg",
    overrides_table=f"{CATALOG}.overrides_by_segment",
    loop_cbg_table=f"{CATALOG}.loop_cbg",
):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {output_table} AS

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
    FROM {overrides_table} o
    INNER JOIN {loop_cbg_table} c
        ON o._userId = c._userId
        AND c.cbg_timestamp >= o.override_time
        AND c.cbg_timestamp <= o.override_time + o.duration * INTERVAL '1' SECOND + INTERVAL '2' HOUR
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
