"""Per-user-day total daily insulin (TDD) = delivered basal + delivered bolus.

Resolved from BDDP exploration (see exploratory/tdd_explore.sql). Loop uploads basal via two
paths that MUST NOT be summed (they are duplicates of the same deliveries):
  * HealthKit (origin.payload.sourceRevision.source.name = 'Loop'): deliveryType='temp', and
    `rate` is the DELIVERED rate, so rate x duration = actual delivered units. Present on
    ~86% of cohort user-days, and its `duration` is the actual segment length (tiles the day).
  * Loop-direct (origin.name = 'com.loopkit.Loop'): deliveryType='automated', `rate` is the
    COMMANDED temp rate (integrates to ~1.7x delivered); the actual delivered amount is
    `payload.deliveredUnits`. Present on ~27% of days; ~14% of days have ONLY this stream.

Delivered basal per user-day:
    HealthKit days   : SUM(rate * effective_duration), effective_duration = LEAST(gap_to_next, duration)
    Loop-direct days : SUM(payload.deliveredUnits)
  Prefer HealthKit; fall back to Loop-direct for days lacking it. Both yield delivered units;
  the commanded Loop `rate x duration` is never used.

Boluses are mirrored under both origins; `normal` is the delivered bolus amount, so take it
from ONE origin (prefer HealthKit) to avoid the 2x double-count.

DEDUP: BDDP re-ingests the same logical record many times (observed up to ~14,000x copies of
one bolus). On top of that, Loop's intermittent dual-sync writes the SAME bolus twice within
the HealthKit stream at offsets of ~2.5s and ~15s (one with milliseconds, one rounded to the
whole second). Every stream is first deduped to one row per
(_userId, round-to-nearest-minute(time_string), value) keeping the latest created_timestamp;
this collapses exact re-ingests AND both dual-sync offset patterns (which an exact-time_string
key misses and a 10s gap misses the ~15s pattern). Without this, TDD is over-tallied.

TRY_CAST is used for the day key because a tiny fraction of rows store time_string as Mongo
JSON ({"$numberLong": ...}); those yield NULL and drop out.

The per-user overall-mean and rolling-30-day TDD references (PLN-1008 §7.5, computed over
ELIGIBLE user-days) are deferred to a downstream step where day eligibility is known.

Inputs:
    dev.default.bddp_sample_all_2   (basal + bolus rows)

Outputs:
    nma_user_day_tdd
        (_userId, local_day, basal_units, bolus_units, tdd_units, basal_source)

Maps to PLN-1008:
    §6   Total daily insulin delivered.
    §7.5 / §8.3 TDD (per-day; per-user reference + ratio computed downstream).
"""

import argparse

HK_LOOP_PREDICATE = "get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'"
LOOP_DIRECT_PREDICATE = "get_json_object(origin, '$.name') = 'com.loopkit.Loop'"


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    output_table="dev.fda_510k_rwd.nma_user_day_tdd",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH
-- HealthKit (source=Loop) basal: `rate` is the DELIVERED rate. Dedup re-ingests first, then
-- clip each segment to the next one (within the user's HealthKit timeline) so any overlap or
-- data-gap is bounded by `duration`.
hk_basal_dedup AS (
  SELECT
    _userId,
    TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    TRY_CAST(time_string AS TIMESTAMP) AS ts,
    TRY_CAST(rate AS DOUBLE) AS rate,
    TRY_CAST(duration AS DOUBLE) AS dur_ms
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, CAST(ROUND(unix_timestamp(TRY_CAST(time_string AS TIMESTAMP)) / 60.0) AS BIGINT), rate, duration
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM {input_table}
    WHERE type = 'basal'
      AND {HK_LOOP_PREDICATE}
      AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  )
  WHERE rn = 1
),

hk_basal_clipped AS (
  SELECT
    _userId,
    local_day,
    COALESCE(rate, 0) * LEAST(
      COALESCE(
        (CAST(LEAD(ts) OVER (PARTITION BY _userId ORDER BY ts) AS DOUBLE) - CAST(ts AS DOUBLE)) * 1000,
        dur_ms
      ),
      dur_ms
    ) / 3600000.0 AS seg_units
  FROM hk_basal_dedup
),

hk_basal_day AS (
  SELECT _userId, local_day, SUM(seg_units) AS basal_units
  FROM hk_basal_clipped
  GROUP BY _userId, local_day
),

-- Loop-direct basal: `payload.deliveredUnits` is the exact delivered amount per segment.
loop_basal_dedup AS (
  SELECT
    _userId,
    TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
    TRY_CAST(get_json_object(payload, '$.deliveredUnits') AS DOUBLE) AS delivered_units
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, CAST(ROUND(unix_timestamp(TRY_CAST(time_string AS TIMESTAMP)) / 60.0) AS BIGINT), rate, duration
        ORDER BY created_timestamp DESC
      ) AS rn
    FROM {input_table}
    WHERE type = 'basal'
      AND {LOOP_DIRECT_PREDICATE}
      AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
  )
  WHERE rn = 1
),

loop_basal_day AS (
  SELECT _userId, local_day, SUM(delivered_units) AS basal_units
  FROM loop_basal_dedup
  GROUP BY _userId, local_day
),

-- Prefer HealthKit delivered basal; fall back to Loop-direct deliveredUnits.
basal_day AS (
  SELECT
    COALESCE(hk._userId, lp._userId) AS _userId,
    COALESCE(hk.local_day, lp.local_day) AS local_day,
    COALESCE(hk.basal_units, lp.basal_units) AS basal_units,
    CASE WHEN hk._userId IS NOT NULL THEN 'healthkit' ELSE 'loop_direct' END AS basal_source
  FROM hk_basal_day hk
  FULL OUTER JOIN loop_basal_day lp
    ON hk._userId = lp._userId
    AND hk.local_day = lp.local_day
),

-- Boluses: `normal` = delivered amount. Dedup re-ingests, then take ONE origin (prefer HealthKit).
hk_bolus_day AS (
  SELECT _userId, local_day, SUM(units) AS bolus_units
  FROM (
    SELECT
      _userId,
      TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
      TRY_CAST(normal AS DOUBLE) AS units
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY _userId, CAST(ROUND(unix_timestamp(TRY_CAST(time_string AS TIMESTAMP)) / 60.0) AS BIGINT), normal
          ORDER BY created_timestamp DESC
        ) AS rn
      FROM {input_table}
      WHERE type = 'bolus'
        AND {HK_LOOP_PREDICATE}
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    )
    WHERE rn = 1
  )
  GROUP BY _userId, local_day
),

loop_bolus_day AS (
  SELECT _userId, local_day, SUM(units) AS bolus_units
  FROM (
    SELECT
      _userId,
      TRY_CAST(LEFT(time_string, 10) AS DATE) AS local_day,
      TRY_CAST(normal AS DOUBLE) AS units
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY _userId, CAST(ROUND(unix_timestamp(TRY_CAST(time_string AS TIMESTAMP)) / 60.0) AS BIGINT), normal
          ORDER BY created_timestamp DESC
        ) AS rn
      FROM {input_table}
      WHERE type = 'bolus'
        AND {LOOP_DIRECT_PREDICATE}
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    )
    WHERE rn = 1
  )
  GROUP BY _userId, local_day
),

bolus_day AS (
  SELECT
    COALESCE(hk._userId, lp._userId) AS _userId,
    COALESCE(hk.local_day, lp.local_day) AS local_day,
    COALESCE(hk.bolus_units, lp.bolus_units) AS bolus_units
  FROM hk_bolus_day hk
  FULL OUTER JOIN loop_bolus_day lp
    ON hk._userId = lp._userId
    AND hk.local_day = lp.local_day
)

SELECT
  COALESCE(b._userId, x._userId) AS _userId,
  COALESCE(b.local_day, x.local_day) AS local_day,
  COALESCE(b.basal_units, 0.0) AS basal_units,
  COALESCE(x.bolus_units, 0.0) AS bolus_units,
  COALESCE(b.basal_units, 0.0) + COALESCE(x.bolus_units, 0.0) AS tdd_units,
  b.basal_source
FROM basal_day b
FULL OUTER JOIN bolus_day x
  ON b._userId = x._userId
  AND b.local_day = x.local_day
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    _parser.add_argument("--output_table", default="dev.fda_510k_rwd.nma_user_day_tdd")
    _args, _ = _parser.parse_known_args()

    run(spark, _args.input_table, _args.output_table)
