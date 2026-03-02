"""
Shared data loading functions for FDA 510(k) RWD analysis scripts.
"""

import pandas as pd
import numpy as np

# Segment labels for the transition analysis tables
SEG1 = "tb_to_ab_seg1"  # temp basal period
SEG2 = "tb_to_ab_seg2"  # autobolus period

# CBG coverage threshold: 70% of a 14-day period at 5-min intervals
SEGMENT_DAYS = 14
SAMPLES_PER_DAY = 288
MIN_COVERAGE = 0.70
MIN_CBG_COUNT = int(SEGMENT_DAYS * SAMPLES_PER_DAY * MIN_COVERAGE)


def load_transition_endpoints(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints for the TB→AB transition analysis, apply
    standard filters, and pivot to a wide DataFrame.

    Steps:
    1. Load glycemic_endpoints_transition
    2. Coerce object columns to numeric
    3. Filter by CBG coverage (>=70% of 14-day period)
    4. Exclude users with any pump settings guardrail violation
    5. Pivot to wide format (one row per user, paired seg1/seg2 columns)

    Returns
    -------
    pd.DataFrame
        Wide DataFrame indexed by _userId with columns like
        tir_seg1, tir_seg2, tbr_seg1, tbr_seg2, etc.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_transition").toPandas()

    # Coerce object columns to numeric
    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "segment"):
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # CBG coverage filter
    endpoints = endpoints.loc[endpoints["cbg_count"] >= MIN_CBG_COUNT].copy()

    # Guardrails exclusion
    guardrails = (
        spark.table("dev.fda_510k_rwd.valid_transition_guardrails")
        .select("_userId", "violation_count")
        .toPandas()
    )
    guardrails["violation_count"] = pd.to_numeric(
        guardrails["violation_count"], errors="coerce"
    ).fillna(0)
    excluded_users = guardrails.loc[guardrails["violation_count"] > 0, "_userId"].unique()
    endpoints = endpoints[~endpoints["_userId"].isin(excluded_users)]
    print(f"  Excluded {len(excluded_users)} users with guardrail violations")

    # Pivot to wide
    seg1 = endpoints[endpoints["segment"] == SEG1].set_index("_userId").add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index("_userId").add_suffix("_seg2")
    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")

    return wide.reset_index()
