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

SEGMENT_KEY = ["_userId", "tb_to_ab_seg1_start"]

# Cohort eligibility:
#   - If Loop version is known, keep segments below this version_int.
#   - If Loop version is unknown, fall back to segments ending before this date.
MAX_LOOP_VERSION_INT = 3_004_000  # Loop 3.4.0
MAX_SEG2_END_DATE = "2024-07-13"


def load_transition_endpoints(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints for the TB→AB transition analysis, apply
    per-segment filters, and pivot to a wide DataFrame with one row per user.

    Steps:
    1. Load glycemic_endpoints_transition
    2. Coerce object columns to numeric
    3. Drop segment-halves below the CBG coverage threshold
    4. Drop segments with any pump-settings guardrail violation
    5. Inner-join seg1 and seg2 halves on (user, seg1_start) so only segments
       with both halves surviving remain
    6. Pick the best surviving segment per user (lowest segment_rank)

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with one row per _userId, paired seg1/seg2 columns.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_transition").toPandas()

    # Coerce object columns to numeric
    non_numeric_cols = {"_userId", "segment", "tb_to_ab_seg1_start"}
    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in non_numeric_cols:
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Cohort filter: version takes precedence. If a Loop version is known, keep
    # the segment iff version_int < MAX_LOOP_VERSION_INT. If unknown, fall back
    # to segments ending before MAX_SEG2_END_DATE.
    allowed = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .where(
            f"(tb_to_ab_max_loop_version_int IS NOT NULL "
            f" AND tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT}) "
            f"OR (tb_to_ab_max_loop_version_int IS NULL "
            f" AND tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}')"
        )
        .select("_userId", "tb_to_ab_seg1_start")
        .toPandas()
    )
    allowed_keys = pd.MultiIndex.from_frame(allowed)
    seg_keys = pd.MultiIndex.from_frame(endpoints[SEGMENT_KEY])
    endpoints = endpoints[seg_keys.isin(allowed_keys)].copy()
    print(f"  Cohort filter kept {len(allowed_keys)} segments")

    # Per-segment-half CBG coverage filter. Both halves must pass; the inner join
    # below enforces that by dropping any (user, seg1_start) with only one half.
    endpoints = endpoints.loc[endpoints["cbg_count"] >= MIN_CBG_COUNT].copy()

    # Per-segment guardrail exclusion.
    guardrails = (
        spark.table("dev.fda_510k_rwd.valid_transition_guardrails")
        .select("_userId", "segment_start", "violation_count")
        .toPandas()
    )
    guardrails["violation_count"] = pd.to_numeric(
        guardrails["violation_count"], errors="coerce"
    ).fillna(0)
    # segment_start is StringType in guardrails; tb_to_ab_seg1_start is DateType in endpoints.
    guardrails["tb_to_ab_seg1_start"] = pd.to_datetime(guardrails["segment_start"]).dt.date
    seg_any_violation = guardrails.groupby(SEGMENT_KEY)["violation_count"].sum() > 0
    bad_segments = seg_any_violation[seg_any_violation].index
    seg_keys = pd.MultiIndex.from_frame(endpoints[SEGMENT_KEY])
    endpoints = endpoints[~seg_keys.isin(bad_segments)].copy()
    print(f"  Excluded {len(bad_segments)} segments with guardrail violations")

    # Pivot per-segment; inner join ensures both halves survived.
    seg1 = endpoints[endpoints["segment"] == SEG1].set_index(SEGMENT_KEY).add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index(SEGMENT_KEY).add_suffix("_seg2")
    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")

    # Best surviving segment per user: lowest segment_rank.
    wide = wide.sort_values("segment_rank_seg1").reset_index()
    wide = wide.drop_duplicates(subset="_userId", keep="first")

    return wide
