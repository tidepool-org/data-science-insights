"""Classify preset activations and users against the TL 2.0 guardrails
(PLN IR-1002 §7.2/§7.3; staging S3).

Two outputs:

- `override_guardrail_flags` — every overrides_all row plus:
    needs_exceeds_mitigation  insulin needs (basalRateScaleFactor) > 170%
    is_p_violation            preset guardrail: own target outside [67, 250]
                              mg/dL, or needs outside [15%, 200%]
    is_m_violation            mitigation: needs > 170% with effective target
                              lower bound < 110 mg/dL at any point during the
                              activation — the preset's own target low when it
                              has one, else the scheduled correction range
                              (correction_range_history slot intersection)
    is_m_indeterminate        needs > 170%, no own target, and NO settings
                              interval covers any part of the window
    is_after_first_ab         override_day on/after the user's first eligible
                              AB day
    is_qualifying             is_version_eligible AND is_after_first_ab — the
                              PLN §7.3 exposure-classification activation set
    is_all_days_ab            every day the activation window touches
                              (override_day..end_day) is an eligible AB day —
                              the PLN §7.5 / IR-3 inclusion rule
- `user_guardrail_groups` — one row per cohort user (>= 1 eligible AB day),
  zero-filled: n_qualifying_activations, ever_p, ever_m, guardrail_group in
  {never_preset, compliant, p_only, m_only, both}, depends_on_indeterminate
  (>= 1 qualifying indeterminate activation and not already ever_m — counting
  indeterminates as M would change the group).

Mitigation fallback semantics (slot intersection):
- The activation window is [override_time, override_time + duration); a
  zero-duration activation is evaluated at its activation instant.
- For each correction_range_history interval overlapping the window, a slot
  triggers when its daily time-of-day span intersects the overlap — if the
  overlap is >= 24 h every slot applies; otherwise a circular time-of-day
  test (slots recur daily; the last slot wraps past midnight).
- Partial settings coverage resolves from the covered part alone: a hit
  anywhere is a violation; covered-but-no-hit is not a violation.
  Indeterminate means ZERO coverage.
- The fallback set is small (Phase 0: 1,629 activations / 39 users), so it is
  computed driver-side in plain Python; timestamps travel as strings both ways
  (tz-naive pandas Timestamps trip Spark Connect's arrow conversion).
"""

import argparse

import pandas as pd
from pyspark.sql import types as T


CATALOG = "dev.fda_510k_rwd"

# TL 2.0 preset guardrail (PLN IR-1002 §7.2): target range within
# [GUARDRAIL_TARGET_LOW, GUARDRAIL_TARGET_HIGH] mg/dL, insulin needs within
# [GUARDRAIL_NEEDS_MIN, GUARDRAIL_NEEDS_MAX].
GUARDRAIL_TARGET_LOW = 67.0
GUARDRAIL_TARGET_HIGH = 250.0
GUARDRAIL_NEEDS_MIN = 0.15
GUARDRAIL_NEEDS_MAX = 2.0

# TL 2.0 high-insulin-needs mitigation: needs above MITIGATION_NEEDS_THRESHOLD
# force the effective target lower bound to >= MITIGATION_TARGET_LB mg/dL.
MITIGATION_NEEDS_THRESHOLD = 1.7
MITIGATION_TARGET_LB = 110.0

SECONDS_PER_DAY = 86_400

_FALLBACK_SCHEMA = T.StructType([
    T.StructField("_userId", T.StringType()),
    T.StructField("override_time_str", T.StringType()),
    T.StructField("is_m_fallback", T.BooleanType()),
    T.StructField("is_m_indeterminate", T.BooleanType()),
])


def _slot_spans(slots):
    """[(slot_start_seconds, low_mgdl)] -> [(span_start, span_end, low_mgdl)]
    time-of-day spans: each slot runs to the next slot's start; the last slot
    wraps past midnight to the first slot's start (span_end may exceed 86400)."""
    ordered = sorted(slots)
    spans = []
    for i, (start, low) in enumerate(ordered):
        if i + 1 < len(ordered):
            end = ordered[i + 1][0]
        else:
            end = ordered[0][0] + SECONDS_PER_DAY
        spans.append((start, end, low))
    return spans


def _window_hits_span(window_tod_start, window_seconds, span_start, span_end):
    """Circular overlap between the daily-recurring span [span_start, span_end)
    and a (< 24 h) window starting at window_tod_start seconds-of-day."""
    for w in (
        window_tod_start - SECONDS_PER_DAY,
        window_tod_start,
        window_tod_start + SECONDS_PER_DAY,
    ):
        if max(span_start, w) < min(span_end, w + window_seconds):
            return True
    return False


def _fallback_m_status(start, duration_seconds, records):
    """Resolve the mitigation fallback for one activation.

    start: pd.Timestamp; records: [(valid_from, valid_to_or_None, hot_spans)]
    for the user, SORTED by valid_from, with hot_spans precomputed once per
    record ([(span_start, span_end)] for slots whose low < the mitigation LB).
    Returns (is_m_fallback, is_m_indeterminate)."""
    eval_end = start + pd.Timedelta(seconds=max(int(duration_seconds), 1))
    covered = False
    for valid_from, valid_to, hot in records:
        if valid_from >= eval_end:
            break  # sorted: no later record can overlap this window
        if valid_to is not None and valid_to <= start:
            continue
        overlap_start = start if valid_from <= start else valid_from
        overlap_end = eval_end if valid_to is None else min(eval_end, valid_to)
        if overlap_end <= overlap_start:
            continue
        covered = True
        if not hot:
            continue
        overlap_seconds = (overlap_end - overlap_start).total_seconds()
        if overlap_seconds >= SECONDS_PER_DAY:
            return True, False
        tod = (overlap_start - overlap_start.normalize()).total_seconds()
        if any(_window_hits_span(tod, overlap_seconds, s, e) for s, e in hot):
            return True, False
    return False, (not covered)


def _resolve_fallback(spark, overrides_table, correction_range_table):
    """Compute (is_m_fallback, is_m_indeterminate) for every activation with
    needs > threshold and no own target; register as temp view
    ir1002_m_fallback keyed on (_userId, override_time_str)."""
    activations = spark.sql(f"""
        --begin-sql
        SELECT
          _userId,
          CAST(override_time AS STRING) AS override_time_str,
          duration
        FROM {overrides_table}
        WHERE basalRateScaleFactor IS NOT NULL
          AND basalRateScaleFactor > {MITIGATION_NEEDS_THRESHOLD}
          AND bg_target_low IS NULL
        ;
    """).toPandas()

    corrections = spark.sql(f"""
        --begin-sql
        SELECT
          c._userId,
          CAST(c.valid_from AS STRING) AS valid_from,
          CAST(c.valid_to AS STRING) AS valid_to,
          c.slot_start_seconds,
          c.target_low_mgdl
        FROM {correction_range_table} c
        WHERE c._userId IN (
          SELECT DISTINCT _userId
          FROM {overrides_table}
          WHERE basalRateScaleFactor IS NOT NULL
            AND basalRateScaleFactor > {MITIGATION_NEEDS_THRESHOLD}
            AND bg_target_low IS NULL
        )
        ;
    """).toPandas()

    # Per-user settings records, built in ONE pass over the sorted rows (Loop
    # users upload settings constantly — hundreds of records per user — so
    # per-group pandas iteration and per-activation span recomputation are the
    # slow path). Hot spans (low < mitigation LB) are precomputed once per
    # record. format="mixed" parses each timestamp element independently:
    # CAST(ts AS STRING) renders fractional seconds only when present
    # ("...:00" vs "...:00.8"), and pandas' default Series parsing locks the
    # format from the first element and then rejects the rest.
    records_by_user = {}
    if len(corrections):
        corrections["valid_from_ts"] = pd.to_datetime(corrections["valid_from"], format="mixed")
        corrections["valid_to_ts"] = pd.to_datetime(corrections["valid_to"], format="mixed")
        ordered = corrections.sort_values(["_userId", "valid_from_ts", "slot_start_seconds"])
        raw_records = []  # (user_id, valid_from, valid_to_or_None, [(start_s, low)])
        current_key = None
        slots = []
        for user_id, valid_from_ts, valid_to_ts, start_s, low in ordered[
            ["_userId", "valid_from_ts", "valid_to_ts", "slot_start_seconds", "target_low_mgdl"]
        ].itertuples(index=False, name=None):
            if (user_id, valid_from_ts) != current_key:
                slots = []
                raw_records.append((
                    user_id,
                    valid_from_ts,
                    None if pd.isna(valid_to_ts) else valid_to_ts,
                    slots,
                ))
                current_key = (user_id, valid_from_ts)
            slots.append((int(start_s), float(low)))
        for user_id, valid_from_ts, valid_to_ts, slots in raw_records:
            hot = [
                (s, e) for s, e, low in _slot_spans(slots)
                if low < MITIGATION_TARGET_LB and e > s
            ]
            records_by_user.setdefault(user_id, []).append(
                (valid_from_ts, valid_to_ts, hot)
            )

    rows = []
    for user_id, time_str, duration in activations[
        ["_userId", "override_time_str", "duration"]
    ].itertuples(index=False, name=None):
        is_m, indet = _fallback_m_status(
            pd.to_datetime(time_str),
            0 if pd.isna(duration) else int(duration),
            records_by_user.get(user_id, []),
        )
        rows.append((user_id, time_str, is_m, indet))

    spark.createDataFrame(rows, schema=_FALLBACK_SCHEMA).createOrReplaceTempView(
        "ir1002_m_fallback"
    )


def run(
    spark,
    flags_table=f"{CATALOG}.override_guardrail_flags",
    groups_table=f"{CATALOG}.user_guardrail_groups",
    overrides_table=f"{CATALOG}.overrides_all",
    correction_range_table=f"{CATALOG}.correction_range_history",
    ab_day_cohort_table=f"{CATALOG}.ab_day_cohort",
):
    _resolve_fallback(spark, overrides_table, correction_range_table)

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {flags_table} AS

    WITH

    user_first_ab AS (
      SELECT _userId, MIN(first_eligible_ab_day) AS first_eligible_ab_day
      FROM {ab_day_cohort_table}
      WHERE is_eligible_ab_day
      GROUP BY _userId
    ),

    eligible_days AS (
      SELECT _userId, day
      FROM {ab_day_cohort_table}
      WHERE is_eligible_ab_day
    ),

    -- Eligible-AB-day count inside each activation's spanned day range; the
    -- all-days-AB rule compares it to the span length. Each activation is
    -- exploded into its spanned days and equi-joined — a BETWEEN range join
    -- here makes Spark enumerate every (activation x eligible-day) pair per
    -- user before filtering, which is quadratic for heavy users. Total
    -- exploded rows are bounded: effective durations are capped by the gap to
    -- the next override, so spans cannot overlap.
    span_days AS (
      SELECT
        o._userId,
        o.override_time,
        explode(sequence(o.override_day, o.end_day)) AS span_day
      FROM {overrides_table} o
    ),

    span_counts AS (
      SELECT
        s._userId,
        s.override_time,
        COUNT(c.day) AS n_ab_days_in_span
      FROM span_days s
      LEFT JOIN eligible_days c
        ON c._userId = s._userId
       AND c.day = s.span_day
      GROUP BY s._userId, s.override_time
    ),

    base AS (
      SELECT
        o.*,
        (o.basalRateScaleFactor IS NOT NULL
         AND o.basalRateScaleFactor > {MITIGATION_NEEDS_THRESHOLD})
          AS needs_exceeds_mitigation
      FROM {overrides_table} o
    )

    SELECT
      b.*,
      -- Preset guardrail: own target outside [{GUARDRAIL_TARGET_LOW},
      -- {GUARDRAIL_TARGET_HIGH}] or needs outside [{GUARDRAIL_NEEDS_MIN},
      -- {GUARDRAIL_NEEDS_MAX}]. Missing parameters are not violations.
      (   (b.bg_target_low  IS NOT NULL AND b.bg_target_low  < {GUARDRAIL_TARGET_LOW})
       OR (b.bg_target_high IS NOT NULL AND b.bg_target_high > {GUARDRAIL_TARGET_HIGH})
       OR (b.basalRateScaleFactor IS NOT NULL
           AND (b.basalRateScaleFactor < {GUARDRAIL_NEEDS_MIN}
                OR b.basalRateScaleFactor > {GUARDRAIL_NEEDS_MAX}))
      ) AS is_p_violation,
      CASE
        WHEN b.needs_exceeds_mitigation AND b.has_own_target
          THEN b.bg_target_low < {MITIGATION_TARGET_LB}
        WHEN b.needs_exceeds_mitigation
          THEN COALESCE(f.is_m_fallback, FALSE)
        ELSE FALSE
      END AS is_m_violation,
      CASE
        WHEN b.needs_exceeds_mitigation AND NOT b.has_own_target
          THEN COALESCE(f.is_m_indeterminate, TRUE)
        ELSE FALSE
      END AS is_m_indeterminate,
      (u.first_eligible_ab_day IS NOT NULL
       AND b.override_day >= u.first_eligible_ab_day) AS is_after_first_ab,
      (b.is_version_eligible
       AND u.first_eligible_ab_day IS NOT NULL
       AND b.override_day >= u.first_eligible_ab_day) AS is_qualifying,
      (s.n_ab_days_in_span = DATEDIFF(b.end_day, b.override_day) + 1)
        AS is_all_days_ab
    FROM base b
    LEFT JOIN user_first_ab u
      ON b._userId = u._userId
    LEFT JOIN span_counts s
      ON s._userId = b._userId AND s.override_time = b.override_time
    LEFT JOIN ir1002_m_fallback f
      ON f._userId = b._userId
     AND f.override_time_str = CAST(b.override_time AS STRING)
    ;
    """)

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {groups_table} AS

    WITH

    cohort_users AS (
      SELECT _userId, MIN(first_eligible_ab_day) AS first_eligible_ab_day
      FROM {ab_day_cohort_table}
      WHERE is_eligible_ab_day
      GROUP BY _userId
    ),

    rollup AS (
      SELECT
        _userId,
        SUM(CASE WHEN is_qualifying THEN 1 ELSE 0 END) AS n_qualifying,
        MAX(CASE WHEN is_qualifying AND is_p_violation THEN 1 ELSE 0 END) AS ever_p_int,
        MAX(CASE WHEN is_qualifying AND is_m_violation THEN 1 ELSE 0 END) AS ever_m_int,
        MAX(CASE WHEN is_qualifying AND is_m_indeterminate THEN 1 ELSE 0 END) AS any_indet_int
      FROM {flags_table}
      GROUP BY _userId
    )

    SELECT
      c._userId,
      c.first_eligible_ab_day,
      COALESCE(r.n_qualifying, 0) AS n_qualifying_activations,
      COALESCE(r.ever_p_int, 0) = 1 AS ever_p,
      COALESCE(r.ever_m_int, 0) = 1 AS ever_m,
      CASE
        WHEN COALESCE(r.n_qualifying, 0) = 0 THEN 'never_preset'
        WHEN COALESCE(r.ever_p_int, 0) = 0 AND COALESCE(r.ever_m_int, 0) = 0 THEN 'compliant'
        WHEN COALESCE(r.ever_p_int, 0) = 1 AND COALESCE(r.ever_m_int, 0) = 0 THEN 'p_only'
        WHEN COALESCE(r.ever_p_int, 0) = 0 AND COALESCE(r.ever_m_int, 0) = 1 THEN 'm_only'
        ELSE 'both'
      END AS guardrail_group,
      (COALESCE(r.any_indet_int, 0) = 1 AND COALESCE(r.ever_m_int, 0) = 0)
        AS depends_on_indeterminate
    FROM cohort_users c
    LEFT JOIN rollup r
      ON c._userId = r._userId
    ;
    """)


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--flags_table", default=f"{CATALOG}.override_guardrail_flags")
    _parser.add_argument("--groups_table", default=f"{CATALOG}.user_guardrail_groups")
    _args, _ = _parser.parse_known_args()

    run(spark, flags_table=_args.flags_table, groups_table=_args.groups_table)
