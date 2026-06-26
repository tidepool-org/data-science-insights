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

CATALOG = "dev.fda_510k_rwd"

# Cohort eligibility:
#   - If Loop version is known, keep segments below this version_int.
#   - If Loop version is unknown, fall back to segments ending before the Loop
#     3.4.0 release date — before then, an undeclared version can only be <3.4.0.
#   - Loop autobolus indication: age ≥6 at segment start (or DOB unknown).
MAX_LOOP_VERSION_INT = 3_004_000   # Loop 3.4.0
MAX_SEG2_END_DATE = "2024-07-13"   # Loop 3.4.0 release date (GitHub LoopKit/Loop v3.4.0, 2024-07-13)
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

# Human-readable Loop version for funnel-stage descriptions (e.g. "3.4.0").
_MAX_LOOP_VERSION_STR = (
    f"{MAX_LOOP_VERSION_INT // 1_000_000}"
    f".{MAX_LOOP_VERSION_INT // 1_000 % 1_000}"
    f".{MAX_LOOP_VERSION_INT % 1_000}"
)

# Diagnosis-type cohort gate. Every analysis cohort is restricted to confirmed
# type-1 users (the FDA Loop indication). Strict: only diagnosis_type = 'type1'
# qualifies — type2/other, unresolved (NULL), and users absent from the lookup
# are all excluded. The lookup (data_staging/export_user_diagnosis_type.py)
# already resolves JAEB-cohort members to 'type1', so they survive. The table is
# box-independent (no {suffix}).
DIAGNOSIS_TABLE = f"{CATALOG}.user_diagnosis_type"
TYPE1_DX = "type1"
# Self-contained predicate for SQL cohort builders that expose `_userId`
# (analysis_8-3 / analysis_8-4) — AND it onto COHORT_WHERE.
TYPE1_SEGMENT_WHERE = (
    f"_userId IN (SELECT _userId FROM {DIAGNOSIS_TABLE} "
    f"WHERE diagnosis_type = '{TYPE1_DX}')"
)


def _record_funnel(funnel, stage, description, df):
    """Append a cohort-flow snapshot (Table 6.3a) to `funnel`; no-op if None.

    Counts distinct users and distinct (user, seg1_start) segments present
    in `df` at this point in the filter chain.
    """
    if funnel is None:
        return
    funnel.append({
        "stage": stage,
        "description": description,
        "n_users": int(df["_userId"].nunique()),
        "n_segments": int(df[SEGMENT_KEY].drop_duplicates().shape[0]),
    })


def load_type1_user_ids(spark) -> set:
    """Return the set of `_userId`s resolved to type-1 diabetes.

    Strict gate (FDA Loop indication): only `diagnosis_type = 'type1'` in
    `user_diagnosis_type` qualifies. Users who are type2/other, have an
    unresolved diagnosis (NULL), or are absent from the lookup are excluded.
    JAEB-cohort members are resolved to 'type1' upstream, so they survive.
    The lookup is box-independent — same set for every validity-box build.
    """
    ids = (
        spark.table(DIAGNOSIS_TABLE)
        .where(f"diagnosis_type = '{TYPE1_DX}'")
        .select("_userId")
        .toPandas()["_userId"]
    )
    return set(ids)


def load_allowed_transition_segments(spark, suffix: str = ""):
    """Eligible (user, seg1_start) transition segments, as a Spark DataFrame.

    The cohort the override analyses (8-3 / 8-4) build their work from: every
    valid transition segment that (a) passes the analysis cohort gate
    (COHORT_WHERE — Loop version + age), (b) carries no pump-settings guardrail
    violation, and (c) belongs to a confirmed type-1 user (TYPE1_SEGMENT_WHERE).

    These are the same gates `load_transition_endpoints` applies for 8-1/8-5/8-8.
    8-3/8-4 don't use that loader (they need override data, not glycemic
    endpoints), so they share the gate here rather than duplicating the SQL.

    Returns a DataFrame with columns (_userId, tb_to_ab_seg1_start).
    """
    return spark.sql(f"""
        SELECT s._userId, s.tb_to_ab_seg1_start
        FROM {CATALOG}.valid_transition_segments{suffix} s
        LEFT ANTI JOIN (
            SELECT _userId, CAST(segment_start AS DATE) AS tb_to_ab_seg1_start
            FROM {CATALOG}.valid_transition_guardrails{suffix}
            GROUP BY _userId, CAST(segment_start AS DATE)
            HAVING SUM(COALESCE(TRY_CAST(violation_count AS DOUBLE), 0)) > 0
        ) g
          ON s._userId = g._userId
         AND s.tb_to_ab_seg1_start = g.tb_to_ab_seg1_start
        WHERE {COHORT_WHERE}
          AND {TYPE1_SEGMENT_WHERE}
    """)


def load_transition_endpoints(spark, suffix: str = "", funnel=None) -> pd.DataFrame:
    """
    Load glycemic endpoints for the TB→AB transition analysis, apply
    per-segment filters, and pivot to a wide DataFrame with one row per user.

    `suffix` selects the source tables: "" (default) reads the production
    valid_transition_segments / glycemic_endpoints_transition /
    valid_transition_guardrails; "_box080" reads the parallel 0.80-box build
    (see exploratory/run_transition_variant.py).

    `funnel`, if a list, accumulates a user/segment-count snapshot after each
    filter step — the analysis-side stages of the Table 6.3a cohort flow
    (analysis_6-3a_cohort_flow.py). Counting here keeps the funnel on the
    exact code path the §8 analyses use.

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
    segments_table = f"{CATALOG}.valid_transition_segments{suffix}"
    endpoints_table = f"{CATALOG}.glycemic_endpoints_transition{suffix}"
    guardrails_table = f"{CATALOG}.valid_transition_guardrails{suffix}"

    endpoints = spark.table(endpoints_table).toPandas()

    # Coerce object columns to numeric
    non_numeric_cols = {"_userId", "segment", "tb_to_ab_seg1_start"}
    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in non_numeric_cols:
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    _record_funnel(
        funnel, "Glycemic endpoints computed",
        "Segments with glycemic endpoints computed for ≥1 of the two 14-day "
        "halves (≥1 CGM reading in the half)",
        endpoints,
    )

    # Cohort filter: Loop-version predicate + age ≥6 (per indication). Version
    # takes precedence — if known, keep iff < MAX_LOOP_VERSION_INT; if unknown,
    # fall back to seg2_end < MAX_SEG2_END_DATE. Users with unknown DOB
    # (tb_to_ab_age_years IS NULL) are kept.
    allowed = (
        spark.table(segments_table)
        .where(COHORT_WHERE)
        .select("_userId", "tb_to_ab_seg1_start")
        .toPandas()
    )
    allowed_keys = pd.MultiIndex.from_frame(allowed)
    seg_keys = pd.MultiIndex.from_frame(endpoints[SEGMENT_KEY])
    endpoints = endpoints[seg_keys.isin(allowed_keys)].copy()
    print(f"  Cohort filter kept {len(allowed_keys)} segments")

    _record_funnel(
        funnel, "Analysis cohort gate",
        f"Known Loop version < {_MAX_LOOP_VERSION_STR}, or unknown version and "
        f"segment ending before {MAX_SEG2_END_DATE}; age ≥ {MIN_AGE} y at "
        f"segment start or unknown",
        endpoints,
    )

    # Diagnosis gate: restrict to confirmed type-1 users (FDA Loop indication).
    type1_ids = load_type1_user_ids(spark)
    endpoints = endpoints[endpoints["_userId"].isin(type1_ids)].copy()
    print(f"  Type-1 filter kept {endpoints['_userId'].nunique()} users")

    _record_funnel(
        funnel, "Type 1 diabetes",
        "Diagnosis resolved to type-1 in user_diagnosis_type (JAEB→type1, else "
        "patients/seagull); non-type1, unresolved, or absent users excluded",
        endpoints,
    )

    # Per-segment-half CBG coverage filter. Both halves must pass; the inner join
    # below enforces that by dropping any (user, seg1_start) with only one half.
    endpoints = endpoints.loc[endpoints["cbg_count"] >= MIN_CBG_COUNT].copy()

    _record_funnel(
        funnel, "CGM coverage filter",
        f"≥ {MIN_COVERAGE:.0%} of expected 5-minute CGM samples in the 14-day "
        f"half (≥ {MIN_CBG_COUNT} readings); segments keep counting here while "
        f"≥1 half survives",
        endpoints,
    )

    # Per-segment guardrail exclusion.
    guardrails = (
        spark.table(guardrails_table)
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

    _record_funnel(
        funnel, "Guardrail exclusion",
        "No pump-settings guardrail violation during the segment",
        endpoints,
    )

    # Pivot per-segment; inner join ensures both halves survived.
    seg1 = endpoints[endpoints["segment"] == SEG1].set_index(SEGMENT_KEY).add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index(SEGMENT_KEY).add_suffix("_seg2")
    wide = seg1.join(seg2, how="inner")
    wide = wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")

    # Best surviving segment per user: lowest segment_rank.
    wide = wide.sort_values("segment_rank_seg1").reset_index()

    _record_funnel(
        funnel, "Paired TB/AB halves",
        "Both the temp-basal and autobolus 14-day halves pass all per-half "
        "filters",
        wide,
    )

    wide = wide.drop_duplicates(subset="_userId", keep="first")

    _record_funnel(
        funnel, "Final transition cohort",
        "One segment per user — lowest segment_rank among survivors",
        wide,
    )

    return wide


def load_override_endpoints(spark, suffix: str = "") -> pd.DataFrame:
    """
    Load per-activation glycemic endpoints for Analysis 8-2 with cohort,
    guardrail, and starting-glucose filters applied.

    `suffix` selects the source tables: "" (default) reads the production
    glycemic_endpoints_override / valid_transition_segments /
    valid_transition_guardrails; "_box080" reads the parallel 0.80-box build
    (see exploratory/run_transition_variant.py).

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
    endpoints_table = f"{CATALOG}.glycemic_endpoints_override{suffix}"
    segments_table = f"{CATALOG}.valid_transition_segments{suffix}"
    guardrails_table = f"{CATALOG}.valid_transition_guardrails{suffix}"

    endpoints = spark.table(endpoints_table).toPandas()

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
        spark.table(segments_table)
        .where("segment_rank = 1")
        .where(COHORT_WHERE)
        .select("_userId", "tb_to_ab_seg1_start")
        .toPandas()
    )
    pre_cohort = endpoints["_userId"].nunique()
    endpoints = endpoints.merge(allowed, on="_userId", how="inner")
    print(f"  Cohort filter kept {endpoints['_userId'].nunique()}/{pre_cohort} users")

    # Diagnosis gate: confirmed type-1 users only (FDA Loop indication).
    type1_ids = load_type1_user_ids(spark)
    pre_dx = endpoints["_userId"].nunique()
    endpoints = endpoints[endpoints["_userId"].isin(type1_ids)].copy()
    print(f"  Type-1 filter kept {endpoints['_userId'].nunique()}/{pre_dx} users")

    # Guardrail-violation exclusion.
    guardrails = (
        spark.table(guardrails_table)
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
