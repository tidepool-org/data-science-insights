"""Classify each user-day as autobolus or temp_basal.

Two independent classification methods are used and combined:

1. dosingDecision: Matches bolus/basal records to dosingDecision records
   with reason='loop' within a ±5-second window.
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

dd_autobolus_days AS (
  SELECT DISTINCT
    b._userId,
    CAST(LEFT(b.time_string, 10) AS DATE) AS day
  FROM {input_table} b
  INNER JOIN loop_decisions dd
    ON b._userId = dd._userId
    AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
    AND ABS(TIMESTAMPDIFF(SECOND, dd.dd_ts, TRY_CAST(b.time_string AS TIMESTAMP)))
        <= 5
  WHERE b.type = 'bolus'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
),

dd_temp_basal_days AS (
  SELECT DISTINCT
    b._userId,
    CAST(LEFT(b.time_string, 10) AS DATE) AS day
  FROM {input_table} b
  INNER JOIN loop_decisions dd
    ON b._userId = dd._userId
    AND LEFT(b.time_string, 10) = LEFT(dd.time_string, 10)
    AND ABS(TIMESTAMPDIFF(SECOND, dd.dd_ts, TRY_CAST(b.time_string AS TIMESTAMP)))
        <= 5
  WHERE b.type = 'basal'
    AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
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
  SELECT DISTINCT _userId, day
  FROM hk_automated
  WHERE type = 'bolus'
),

hk_temp_basal_days AS (
  SELECT DISTINCT _userId, day
  FROM hk_automated
  WHERE type = 'basal'
),

-- Combine both methods
all_autobolus_days AS (
  SELECT _userId, day FROM dd_autobolus_days
  UNION
  SELECT _userId, day FROM hk_autobolus_days
),

all_temp_basal_days AS (
  SELECT _userId, day FROM dd_temp_basal_days
  UNION
  SELECT _userId, day FROM hk_temp_basal_days
),

day_versions AS (
  SELECT _userId, day, MAX(loop_version) AS loop_version
  FROM (
    SELECT _userId, CAST(LEFT(time_string, 10) AS DATE) AS day, loop_version
    FROM loop_decisions
    UNION ALL
    SELECT _userId, day, loop_version
    FROM hk_automated
  )
  GROUP BY _userId, day
),

classified AS (
  SELECT _userId, day, 'autobolus' AS day_type
  FROM all_autobolus_days

  UNION ALL

  SELECT tb._userId, tb.day, 'temp_basal' AS day_type
  FROM all_temp_basal_days tb
  LEFT JOIN all_autobolus_days ab
    ON tb._userId = ab._userId AND tb.day = ab.day
  WHERE ab._userId IS NULL
)

SELECT
  c._userId,
  c.day,
  c.day_type,
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
