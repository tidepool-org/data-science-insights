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


def load_override_endpoints(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints aggregated per (user, preset, params, segment) for
    Analysis 8-2, applying the same cohort and guardrail filters used in the
    transition analyses, plus the override-specific inclusion criteria.

    Filters (in order):
    1. Cohort: Loop version below MAX_LOOP_VERSION_INT (or, if version unknown,
       segment ending before MAX_SEG2_END_DATE). Sourced from
       `valid_transition_segments` (rank-1 row per user).
    2. Guardrail: drop users whose rank-1 segment has any guardrail violation.
    3. Inclusion: `is_valid_name_only = TRUE` (preset name appears >= 2 times
       in each TB and AB phase for the user).
    4. Inclusion: `is_starting_glucose_in_range = TRUE` (activation begins with
       a CBG between STARTING_GLUCOSE_LOW and STARTING_GLUCOSE_HIGH).

    Returns a long DataFrame with one row per surviving (user, preset, params,
    segment) bucket. Numeric columns are coerced; the analysis driver pivots
    to wide form.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_override").toPandas()

    # Coerce object columns to numeric (skip the keys + boolean flags).
    non_numeric_cols = {
        "_userId", "segment", "overridePreset",
        "is_valid_name_only", "is_valid_full",
        "is_starting_glucose_in_range",
    }
    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in non_numeric_cols:
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Cohort filter: same predicate as load_transition_endpoints.
    allowed = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .where("segment_rank = 1")
        .where( 
            f"(tb_to_ab_max_loop_version_int IS NOT NULL "
            f" AND tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT}) "
            f"OR (tb_to_ab_max_loop_version_int IS NULL "
            f" AND tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}')"
        )
        .select("_userId", "tb_to_ab_seg1_start")
        .toPandas()
    )
    pre_cohort = endpoints["_userId"].nunique()
    endpoints = endpoints.merge(allowed, on="_userId", how="inner")
    print(f"  Cohort filter kept {endpoints['_userId'].nunique()}/{pre_cohort} users")

    # Guardrail-violation exclusion: drop users whose rank-1 segment has any
    # violation. Match on (_userId, tb_to_ab_seg1_start), same key as the
    # transition loader.
    guardrails = (
        spark.table("dev.fda_510k_rwd.valid_transition_guardrails")
        .select("_userId", "segment_start", "violation_count")
        .toPandas()
    )
    guardrails["violation_count"] = pd.to_numeric(
        guardrails["violation_count"], errors="coerce"
    ).fillna(0)
    guardrails["tb_to_ab_seg1_start"] = pd.to_datetime(guardrails["segment_start"]).dt.date
    seg_any_violation = guardrails.groupby(SEGMENT_KEY)["violation_count"].sum() > 0
    bad_segments = seg_any_violation[seg_any_violation].index
    pre_gr = endpoints["_userId"].nunique()
    seg_keys = pd.MultiIndex.from_frame(endpoints[SEGMENT_KEY])
    endpoints = endpoints[~seg_keys.isin(bad_segments)].copy()
    print(f"  Guardrail filter kept {endpoints['_userId'].nunique()}/{pre_gr} users")

    # Inclusion criteria.
    endpoints = endpoints[endpoints["is_valid_name_only"] == True].copy()  # noqa: E712
    endpoints = endpoints[endpoints["is_starting_glucose_in_range"] == True].copy()  # noqa: E712
    print(f"  Inclusion filters kept {endpoints['_userId'].nunique()} users, "
          f"{len(endpoints)} (user, config, segment) rows")

    # tb_to_ab_seg1_start is no longer needed downstream (analysis pivots on
    # _userId + config). Drop it to keep the schema tight.
    endpoints = endpoints.drop(columns=["tb_to_ab_seg1_start"])

    return endpoints
