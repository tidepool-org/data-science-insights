import argparse

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ---------------------------------------------------------------------------
# Hypo event detection
# ---------------------------------------------------------------------------

def _compute_hypo_events(spark, cbg_df, group_cols, threshold_start=54, threshold_end=70,
                         consec=3, max_gap_minutes=None):
    """
    Count hypoglycemic events per group using applyInPandas.

    An event begins after `consec` consecutive readings < threshold_start (54 mg/dL).
    An event ends after `consec` consecutive readings > threshold_end (70 mg/dL).

    max_gap_minutes: when set, readings separated by more than this are NOT
    treated as consecutive — the run counters reset and any open event is
    closed. "Consecutive" otherwise means adjacent rows in timestamp order
    regardless of elapsed time, so a sensor dropout can join readings hours
    apart into one run. Left as None (the original behavior) for the
    transition / stable / override modes so their published results are
    unchanged; mode=ab_days sets it, because an outcome day only needs 201 of
    288 samples and can therefore carry gaps of over an hour.
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

        gap_limit = (
            pd.Timedelta(minutes=max_gap_minutes)
            if max_gap_minutes is not None else None
        )
        prev_ts = None

        for ts, val in zip(pdf["cbg_timestamp"], pdf["cbg_mg_dl"].astype(float)):
            if gap_limit is not None and prev_ts is not None and (ts - prev_ts) > gap_limit:
                # Dropout: the next reading cannot continue a run, and an open
                # event cannot be assumed to have persisted across the gap.
                in_event = False
                streak_below = 0
                streak_above = 0
            prev_ts = ts
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

def compute_glycemic_endpoints(spark, cbg_df, group_cols=None, hypo_group_cols=None,
                               hypo_max_gap_minutes=None):
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
    hypo_group_cols : list of str, optional
        When given, hypo events are DETECTED within these (finer) groups and
        then summed to group_cols — consecutive-reading runs cannot span the
        boundary between the finer groups. Must be a superset of group_cols.
        Used by mode=ab_days for within-day detection over pooled
        non-contiguous days (PLN IR-1002 §7.4); default = group_cols
        (detection at the output grain, the pre-existing behavior).
    """
    if group_cols is None:
        group_cols = ["_userId", "segment"]
    if hypo_group_cols is None:
        hypo_group_cols = group_cols

    group_clause = ", ".join(group_cols)

    cbg_df.createOrReplaceTempView("_cbg_input")

    range_endpoints = spark.sql(f"""
        --begin-sql
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
    ;
    """)

    hypo_events = _compute_hypo_events(
        spark, cbg_df, hypo_group_cols, max_gap_minutes=hypo_max_gap_minutes
    )
    if hypo_group_cols != group_cols:
        hypo_events = hypo_events.groupBy(*group_cols).agg(
            F.sum("hypo_events").cast("int").alias("hypo_events")
        )

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
        # Per-activation grain. (_userId, override_time) is the activation
        # identifier; the rest are passthrough — constant within an activation
        # so they don't split groups, just ride through to keep the columns
        # available downstream (aggregate_override_endpoints needs duration
        # for window_hours / hypo rate; the validity and starting-glucose
        # flags are per-activation properties the analysis filters on).
        "group_cols": [
            "_userId", "override_time", "duration",
            "overridePreset",
            "brsf", "btl", "bth", "crsf", "issf",
            "segment",
            "is_valid_name_only_seg2",
            "is_valid_name_only_seg3",
            "is_starting_glucose_in_range",
        ],
    },
    "stable": {
        "default_input_table": f"{CATALOG}.stable_autobolus_cbg",
        "default_output_table": f"{CATALOG}.glycemic_endpoints_stable_autobolus",
        "group_cols": ["_userId", "segment"],
    },
    "ab_days": {
        "default_input_table": f"{CATALOG}.ab_day_cbg",
        "default_output_table": f"{CATALOG}.glycemic_endpoints_ab_days",
        # One pooled row per user over all outcome-eligible AB days (PLN
        # IR-1002 §7.4). Hypo events are detected per (user, day) so
        # consecutive-reading runs never span the gap between non-adjacent
        # pooled days, then summed per user.
        "group_cols": ["_userId"],
        "hypo_group_cols": ["_userId", "day"],
        # An outcome day needs only 201 of 288 samples, so a day can carry
        # dropouts of over an hour; readings either side of one are not
        # "consecutive" for event detection.
        "hypo_max_gap_minutes": 15,
    },
}


def run(spark, mode="transition", input_table=None, output_table=None):
    if mode not in MODE_CONFIG:
        raise ValueError(f"Unknown mode '{mode}'. Valid modes: {sorted(MODE_CONFIG)}")

    cfg = MODE_CONFIG[mode]
    input_table = input_table or cfg["default_input_table"]
    output_table = output_table or cfg["default_output_table"]

    cbg_df = spark.table(input_table)
    endpoints = compute_glycemic_endpoints(
        spark,
        cbg_df,
        group_cols=cfg["group_cols"],
        hypo_group_cols=cfg.get("hypo_group_cols"),
        hypo_max_gap_minutes=cfg.get("hypo_max_gap_minutes"),
    )
    endpoints.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--mode", default="transition")
    _args, _ = _parser.parse_known_args()

    run(spark, mode=_args.mode)