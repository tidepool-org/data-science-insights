import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ---------------------------------------------------------------------------
# Hypo event detection
# ---------------------------------------------------------------------------

def _compute_hypo_events(spark, cbg_df, group_cols, threshold_start=54, threshold_end=70, consec=3):
    """
    Count hypoglycemic events per group using applyInPandas.

    An event begins after `consec` consecutive readings < threshold_start (54 mg/dL).
    An event ends after `consec` consecutive readings > threshold_end (70 mg/dL).
    """

    # Infer schema from the actual DataFrame types
    group_fields = [cbg_df.schema[c] for c in group_cols]
    schema = StructType(
        group_fields
        + [StructField("hypo_events", IntegerType())]
    )

    def count_events(pdf):
        pdf = pdf.sort_values("cbg_timestamp")
        events = 0
        in_event = False
        streak_below = 0
        streak_above = 0

        for val in pdf["cbg_mg_dl"]:
            if not in_event:
                if val < threshold_start:
                    streak_below += 1
                    if streak_below >= consec:
                        in_event = True
                        events += 1
                        streak_above = 0
                else:
                    streak_below = 0
            else:
                if val > threshold_end:
                    streak_above += 1
                    if streak_above >= consec:
                        in_event = False
                        streak_below = 0
                else:
                    streak_above = 0

        row = {c: pdf[c].iloc[0] for c in group_cols}
        row["hypo_events"] = events
        return pd.DataFrame([row])

    return cbg_df.groupby(*group_cols).applyInPandas(count_events, schema=schema)


# ---------------------------------------------------------------------------
# Glycemic endpoints (range metrics + hypo events)
# ---------------------------------------------------------------------------

def compute_glycemic_endpoints(spark, cbg_df, group_cols=None):
    """
    Compute glycemic endpoints (TIR, TBR, TAR, CV, mean glucose) and
    hypoglycemic event counts per group.

    Parameters
    ----------
    spark : SparkSession
    cbg_df : DataFrame
        Must contain `cbg_mg_dl`, `cbg_timestamp`, and all group_cols.
    group_cols : list of str, default ['_userId', 'segment']
        Columns to group by.
    """
    if group_cols is None:
        group_cols = ["_userId", "segment"]

    group_clause = ", ".join(group_cols)

    cbg_df.createOrReplaceTempView("_cbg_input")

    range_endpoints = spark.sql(f"""
        SELECT
            {group_clause},
            COUNT(*) AS cbg_count,
            SUM(CASE WHEN cbg_mg_dl < 54 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tbr_very_low,
            SUM(CASE WHEN cbg_mg_dl < 70 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tbr,
            SUM(CASE WHEN cbg_mg_dl >= 70 AND cbg_mg_dl <= 180 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tir,
            SUM(CASE WHEN cbg_mg_dl > 180 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tar,
            SUM(CASE WHEN cbg_mg_dl > 250 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tar_very_high,
            AVG(cbg_mg_dl) AS mean_glucose,
            STDDEV(cbg_mg_dl) * 100.0 / AVG(cbg_mg_dl) AS cv
        FROM _cbg_input
        GROUP BY {group_clause}
    """)

    hypo_events = _compute_hypo_events(spark, cbg_df, group_cols)

    return range_endpoints.join(hypo_events, on=group_cols, how="left").withColumn(
        "hypo_events", F.coalesce(F.col("hypo_events"), F.lit(0))
    )


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

MODE = dbutils.widgets.get("mode")
# MODE = "transition"

if MODE == "transition":

    transition_cbg = spark.sql("""
        SELECT
            c._userId,
            c.cbg_mg_dl,
            c.cbg_timestamp,
            CASE
                WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
                WHEN CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
            END AS segment
        FROM dev.fda_510k_rwd.valid_transition_cbg c
        INNER JOIN dev.fda_510k_rwd.valid_transition_segments t ON c._userId = t._userId
        WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
    """)

    transition_cbg = transition_cbg.filter("segment IS NOT NULL")

    transition_endpoints = compute_glycemic_endpoints(spark, transition_cbg)
    transition_endpoints.write.mode("overwrite").saveAsTable(
        "dev.fda_510k_rwd.glycemic_endpoints_transition"
    )

elif MODE == "override":

    override_cbg = spark.sql("""
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
            c.cbg_timestamp
        FROM dev.fda_510k_rwd.overrides_by_segment o
        INNER JOIN dev.fda_510k_rwd.valid_transition_cbg c
            ON o._userId = c._userId
            AND c.cbg_timestamp >= o.override_time
            AND c.cbg_timestamp <= o.override_time + o.duration * INTERVAL '1' SECOND + INTERVAL '2' HOUR
        WHERE o.is_valid = TRUE
    """)

    override_endpoints = compute_glycemic_endpoints(
        spark, override_cbg,
        group_cols=["_userId", "overridePreset", "brsf", "btl", "bth", "crsf", "issf", "segment"]
    )
    override_endpoints.write.mode("overwrite").saveAsTable(
        "dev.fda_510k_rwd.glycemic_endpoints_override"
    )

elif MODE == "stable":

    stable_cbg = spark.sql("""
        SELECT
            c._userId,
            c.cbg_mg_dl,
            c.cbg_timestamp,
            'stable_ab' AS segment
        FROM dev.fda_510k_rwd.stable_autobolus_cbg c
        INNER JOIN dev.fda_510k_rwd.stable_autobolus_segments s ON c._userId = s._userId
        WHERE CAST(c.cbg_timestamp AS DATE) BETWEEN s.segment_start AND s.segment_end
    """)

    stable_endpoints = compute_glycemic_endpoints(spark, stable_cbg)
    stable_endpoints.write.mode("overwrite").saveAsTable(
        "dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus"
    )