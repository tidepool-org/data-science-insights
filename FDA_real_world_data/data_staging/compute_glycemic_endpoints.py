import argparse

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

        for val in pdf["cbg_mg_dl"].astype(float):
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
# Mode configuration
# ---------------------------------------------------------------------------

CATALOG = "dev.fda_510k_rwd"

MODE_CONFIG = {
    "transition": {
        "default_input_table": f"{CATALOG}.valid_transition_cbg",
        "default_output_table": f"{CATALOG}.glycemic_endpoints_transition",
        "group_cols": ["_userId", "tb_to_ab_seg1_start", "segment_rank", "segment"],
    },
    "override": {
        "default_input_table": f"{CATALOG}.valid_override_cbg",
        "default_output_table": f"{CATALOG}.glycemic_endpoints_override",
        "group_cols": [
            "_userId", "overridePreset",
            "brsf", "btl", "bth", "crsf", "issf",
            "segment",
        ],
    },
    "stable": {
        "default_input_table": f"{CATALOG}.stable_autobolus_cbg",
        "default_output_table": f"{CATALOG}.glycemic_endpoints_stable_autobolus",
        "group_cols": ["_userId", "segment"],
    },
}


def run(spark, mode="transition", input_table=None, output_table=None):
    if mode not in MODE_CONFIG:
        raise ValueError(f"Unknown mode '{mode}'. Valid modes: {sorted(MODE_CONFIG)}")

    cfg = MODE_CONFIG[mode]
    input_table = input_table or cfg["default_input_table"]
    output_table = output_table or cfg["default_output_table"]

    cbg_df = spark.table(input_table)
    endpoints = compute_glycemic_endpoints(spark, cbg_df, group_cols=cfg["group_cols"])
    endpoints.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--mode", default="transition")
    _args, _ = _parser.parse_known_args()

    run(spark, mode=_args.mode)