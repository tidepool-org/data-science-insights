"""Classify each user-day as autobolus or temp_basal.

Two independent classification methods are used and combined:

1. dosingDecision: Matches bolus/basal records to the most recent
   dosingDecision with reason='loop' in the 5 seconds before the record.
   Excludes boluses that have a reason='normalBolus' dosingDecision
   within ±15 seconds (user-initiated correction bolus, not an autobolus).
2. HealthKit: Uses MetadataKeyAutomaticallyIssued on insulin delivery records
   where the HealthKit source is Loop.

A day is 'autobolus' if any automated bolus is detected by either method;
'temp_basal' if only automated basals are detected.
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    output_table="dev.fda_510k_rwd.loop_recommendations",
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

WITH
-- Method 1: dosingDecision-based classification

-- Loop-initiated dosing decisions
loop_decisions AS (
  SELECT
    _userId,
    time_string,
    TRY_CAST(time_string AS TIMESTAMP) AS dd_ts,
    get_json_object(origin, '$.version') AS loop_version
  FROM {input_table}
  WHERE type = 'dosingDecision'
    AND reason = 'loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

-- User-initiated bolus decisions (used to exclude false-positive autoboluses)
normal_bolus_decisions AS (
  SELECT
    _userId,
    TRY_CAST(time_string AS TIMESTAMP) AS nb_ts
  FROM {input_table}
  WHERE type = 'dosingDecision'
    AND reason = 'normalBolus'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

-- Match each bolus/basal to all loop DDs in the prior 5 seconds, ranked most recent first
dd_matched AS (
  SELECT
    b._userId,
    b.time_string AS b_time_string,
    TRY_CAST(b.time_string AS TIMESTAMP) AS b_ts,
    b.type AS b_type,
    b.subType AS b_subType,
    dd.dd_ts,
    ROW_NUMBER() OVER (
      PARTITION BY b._userId, b.time_string
      ORDER BY dd.dd_ts DESC
    ) AS rn
  FROM {input_table} b
  INNER JOIN loop_decisions dd
    ON b._userId = dd._userId
    AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
    AND TIMESTAMPDIFF(SECOND, dd.dd_ts, TRY_CAST(b.time_string AS TIMESTAMP))
        BETWEEN 0 AND 5
  WHERE b.type IN ('bolus', 'basal')
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

-- Keep only the most recent DD match per bolus/basal
dd_most_recent AS (
  SELECT *
  FROM dd_matched
  WHERE rn = 1
),

-- Exclude boluses with a normalBolus DD within ±15s (user-initiated, not autobolus)
dd_filtered AS (
  SELECT *
  FROM dd_most_recent m
  WHERE NOT (
    m.b_type = 'bolus'
    AND EXISTS (
      SELECT 1
      FROM normal_bolus_decisions nb
      WHERE nb._userId = m._userId
        AND ABS(TIMESTAMPDIFF(SECOND, nb.nb_ts, m.b_ts)) <= 15
    )
  )
),

dd_autobolus_days AS (
  SELECT
    _userId,
    CAST(LEFT(b_time_string, 10) AS DATE) AS day,
    COUNT(DISTINCT b_time_string) AS autobolus_count
  FROM dd_filtered
  WHERE b_type = 'bolus'
    AND b_subType != 'normal'
  GROUP BY
    _userId,
    CAST(LEFT(b_time_string, 10) AS DATE)
),

dd_temp_basal_days AS (
  SELECT
    _userId,
    CAST(LEFT(b_time_string, 10) AS DATE) AS day,
    COUNT(DISTINCT b_time_string) AS temp_basal_count
  FROM dd_filtered
  WHERE b_type = 'basal'
  GROUP BY
    _userId,
    CAST(LEFT(b_time_string, 10) AS DATE)
),

-- Method 2: HealthKit-based classification
hk_automated AS (
  SELECT
    _userId,
    type,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    get_json_object(origin, '$.version') AS loop_version
  FROM {input_table}
  WHERE get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND CAST(get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS DOUBLE) = 1
    AND type IN ('bolus', 'basal')
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

hk_autobolus_days AS (
  SELECT
    _userId,
    day,
    COUNT(*) AS autobolus_count
  FROM hk_automated
  WHERE type = 'bolus'
  GROUP BY
    _userId,
    day
),

hk_temp_basal_days AS (
  SELECT
    _userId,
    day,
    COUNT(*) AS temp_basal_count
  FROM hk_automated
  WHERE type = 'basal'
  GROUP BY
    _userId,
    day
),

-- Combine both methods
all_autobolus_days AS (
  SELECT
    _userId,
    day,
    MAX(dd_autobolus_count) AS dd_autobolus_count,
    MAX(hk_autobolus_count) AS hk_autobolus_count
  FROM (
    SELECT _userId, day, autobolus_count AS dd_autobolus_count, NULL AS hk_autobolus_count
    FROM dd_autobolus_days
    UNION ALL
    SELECT _userId, day, NULL AS dd_autobolus_count, autobolus_count AS hk_autobolus_count
    FROM hk_autobolus_days
  )
  GROUP BY _userId, day
),

all_temp_basal_days AS (
  SELECT
    _userId,
    day,
    MAX(dd_temp_basal_count) AS dd_temp_basal_count,
    MAX(hk_temp_basal_count) AS hk_temp_basal_count
  FROM (
    SELECT _userId, day, temp_basal_count AS dd_temp_basal_count, NULL AS hk_temp_basal_count
    FROM dd_temp_basal_days
    UNION ALL
    SELECT _userId, day, NULL AS dd_temp_basal_count, temp_basal_count AS hk_temp_basal_count
    FROM hk_temp_basal_days
  )
  GROUP BY _userId, day
),

-- Convert version string (e.g. '3.10.1') to a sortable int (3*1000000 + 10*1000 + 1 = 3010001)
-- so MAX picks the true highest version, not the lexicographic max
day_versions AS (
  SELECT
    _userId,
    day,
    MAX_BY(loop_version, version_int) AS loop_version
  FROM (
    SELECT
      _userId,
      CAST(LEFT(time_string, 10) AS DATE) AS day,
      loop_version,
      COALESCE(CAST(SPLIT(loop_version, '\\.')[0] AS INT), 0) * 1000000
        + COALESCE(CAST(SPLIT(loop_version, '\\.')[1] AS INT), 0) * 1000
        + COALESCE(CAST(SPLIT(loop_version, '\\.')[2] AS INT), 0) AS version_int
    FROM loop_decisions
    UNION ALL
    SELECT
      _userId,
      day,
      loop_version,
      COALESCE(CAST(SPLIT(loop_version, '\\.')[0] AS INT), 0) * 1000000
        + COALESCE(CAST(SPLIT(loop_version, '\\.')[1] AS INT), 0) * 1000
        + COALESCE(CAST(SPLIT(loop_version, '\\.')[2] AS INT), 0) AS version_int
    FROM hk_automated
  )
  GROUP BY _userId, day
),

classified AS (
  SELECT
    _userId,
    day,
    'autobolus' AS day_type,
    dd_autobolus_count,
    hk_autobolus_count,
    NULL AS dd_temp_basal_count,
    NULL AS hk_temp_basal_count
  FROM all_autobolus_days

  UNION ALL

  SELECT
    tb._userId,
    tb.day,
    'temp_basal' AS day_type,
    NULL AS dd_autobolus_count,
    NULL AS hk_autobolus_count,
    tb.dd_temp_basal_count,
    tb.hk_temp_basal_count
  FROM all_temp_basal_days tb
  LEFT JOIN all_autobolus_days ab
    ON tb._userId = ab._userId AND tb.day = ab.day
  WHERE ab._userId IS NULL
)

SELECT
  c._userId,
  c.day,
  c.day_type,
  c.dd_autobolus_count,
  c.hk_autobolus_count,
  c.dd_temp_basal_count,
  c.hk_temp_basal_count,
  dv.loop_version
FROM classified c
LEFT JOIN day_versions dv
  ON c._userId = dv._userId
  AND c.day = dv.day
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    args, _ = parser.parse_known_args()

    run(spark, args.input_table)
