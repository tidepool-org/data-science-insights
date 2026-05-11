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
#   - Loop autobolus indication: age ≥6 at segment start (or DOB unknown).
MAX_LOOP_VERSION_INT = 3_004_000  # Loop 3.4.0
MAX_SEG2_END_DATE = "2024-07-13"
MIN_AGE = 6

# Cohort predicate against `valid_transition_segments`. Imported by
# analysis_8-3 / analysis_8-4 so the cohort definition lives in one place.
COHORT_WHERE = (
    f"((tb_to_ab_max_loop_version_int IS NOT NULL "
    f"  AND tb_to_ab_max_loop_version_int < {MAX_LOOP_VERSION_INT}) "
    f" OR (tb_to_ab_max_loop_version_int IS NULL "
    f"  AND tb_to_ab_seg2_end < DATE '{MAX_SEG2_END_DATE}')) "
    f"AND (tb_to_ab_age_years >= {MIN_AGE} OR tb_to_ab_age_years IS NULL)"
)


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

    # Cohort filter: Loop-version predicate + age ≥6 (per indication). Version
    # takes precedence — if known, keep iff < MAX_LOOP_VERSION_INT; if unknown,
    # fall back to seg2_end < MAX_SEG2_END_DATE. Users with unknown DOB
    # (tb_to_ab_age_years IS NULL) are kept.
    allowed = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .where(COHORT_WHERE)
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
    Load per-activation glycemic endpoints for Analysis 8-2 with cohort,
    guardrail, and starting-glucose filters applied.

    Returns one row per surviving preset activation. The validity flags
    `is_valid_name_only_seg2` and `is_valid_name_only_seg3` are kept on the
    DataFrame so callers can filter for the appropriate AB segment pairing
    (Table 8.2b uses _seg2, Table 8.2c uses _seg3).

    Filters applied here:
    1. Cohort: Loop version below MAX_LOOP_VERSION_INT (or, if version unknown,
       segment ending before MAX_SEG2_END_DATE). Sourced from
       `valid_transition_segments` rank-1 row per user.
    2. Guardrail: drop users whose rank-1 segment has any guardrail violation.
    3. Inclusion: `is_starting_glucose_in_range = TRUE`.

    The name-only validity is applied later by `aggregate_override_endpoints`
    because seg2 and seg3 have separate validity flags.
    """
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_override").toPandas()

    # Coerce object columns to numeric (skip the keys + boolean flags).
    non_numeric_cols = {
        "_userId", "segment", "overridePreset", "override_time",
        "is_valid_name_only_seg2", "is_valid_name_only_seg3",
        "is_starting_glucose_in_range",
    }
    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in non_numeric_cols:
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # Cohort filter: same predicate as load_transition_endpoints.
    allowed = (
        spark.table("dev.fda_510k_rwd.valid_transition_segments")
        .where("segment_rank = 1")
        .where(COHORT_WHERE)
        .select("_userId", "tb_to_ab_seg1_start")
        .toPandas()
    )
    pre_cohort = endpoints["_userId"].nunique()
    endpoints = endpoints.merge(allowed, on="_userId", how="inner")
    print(f"  Cohort filter kept {endpoints['_userId'].nunique()}/{pre_cohort} users")

    # Guardrail-violation exclusion.
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

    # Starting-glucose inclusion (applies to all 8.2b / 8.2c analyses).
    endpoints = endpoints[endpoints["is_starting_glucose_in_range"] == True].copy()  # noqa: E712
    print(f"  Starting-glucose filter kept {endpoints['_userId'].nunique()} users, "
          f"{len(endpoints)} per-activation rows")

    endpoints = endpoints.drop(columns=["tb_to_ab_seg1_start"])

    # Per-activation exposure window (preset duration + 2-hour tail), in hours.
    # Duration is in seconds; +7200s captures the tail.
    endpoints["window_hours"] = (
        pd.to_numeric(endpoints["duration"], errors="coerce") + 7200
    ) / 3600.0

    return endpoints


_ENDPOINT_AVG_COLS = (
    "tbr_very_low", "tbr", "tir", "tar", "tar_very_high",
    "mean_glucose", "cv",
)
_GRAIN_COLS = {
    "name":   ["overridePreset"],
    "config": ["overridePreset", "brsf", "btl", "bth", "crsf", "issf"],
}


def aggregate_override_endpoints(
    activations: pd.DataFrame,
    ab_segment: str,
    grain: str,
) -> pd.DataFrame:
    """
    Average per-activation endpoints up to the requested grain and return
    a wide DataFrame pairing temp_basal with the requested AB segment.

    Parameters
    ----------
    activations : pd.DataFrame
        Per-activation rows from `load_override_endpoints`.
    ab_segment : {"tb_to_ab_seg2", "tb_to_ab_seg3"}
        Which AB segment to pair with TB. Selects the matching
        `is_valid_name_only_*` validity column.
    grain : {"name", "config"}
        "name"   → (user, overridePreset)
        "config" → (user, overridePreset, brsf, btl, bth, crsf, issf)

    Aggregation rules (per the analysis plan):
    - Range and shape endpoints (TIR / TBR / TAR / mean / CV): unweighted
      mean across activations within the group.
    - Hypo events: total events ÷ total exposure hours → events/hour.
    - activation_count: number of activations contributing to the group.

    The returned DataFrame has one row per (user, grain_key) with `_seg1`
    (TB) and `_seg2` (the requested AB segment) suffixed columns; `_seg2`
    is used in the suffix regardless of which AB segment was requested so
    downstream code (paired stats, plotting) doesn't need to know.
    """
    if grain not in _GRAIN_COLS:
        raise ValueError(f"grain must be 'name' or 'config', got {grain!r}")
    if ab_segment not in ("tb_to_ab_seg2", "tb_to_ab_seg3"):
        raise ValueError(f"ab_segment must be 'tb_to_ab_seg2' or 'tb_to_ab_seg3', got {ab_segment!r}")

    valid_col = "is_valid_name_only_seg2" if ab_segment == "tb_to_ab_seg2" else "is_valid_name_only_seg3"
    df = activations[activations[valid_col] == True].copy()  # noqa: E712
    df = df[df["segment"].isin(["tb_to_ab_seg1", ab_segment])].copy()

    grain_cols = _GRAIN_COLS[grain]
    group_cols = ["_userId"] + grain_cols + ["segment"]

    agg_spec = {col: "mean" for col in _ENDPOINT_AVG_COLS}
    agg_spec["hypo_events"] = "sum"
    agg_spec["window_hours"] = "sum"

    aggregated = df.groupby(group_cols, as_index=False).agg(agg_spec)
    sizes = (
        df.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "activation_count"})
    )
    aggregated = aggregated.merge(sizes, on=group_cols, how="left")
    # Hypo rate per hour of preset exposure (window = duration + 2h tail).
    aggregated["hypo_rate"] = (
        aggregated["hypo_events"] / aggregated["window_hours"]
    ).where(aggregated["window_hours"] > 0)

    # Pivot to wide on segment.
    index_cols = ["_userId"] + grain_cols
    seg1 = (aggregated[aggregated["segment"] == "tb_to_ab_seg1"]
            .set_index(index_cols).drop(columns=["segment"]).add_suffix("_seg1"))
    seg2 = (aggregated[aggregated["segment"] == ab_segment]
            .set_index(index_cols).drop(columns=["segment"]).add_suffix("_seg2"))
    wide = seg1.join(seg2, how="inner").reset_index()
    return wide
