"""Classify each user-day as autobolus or temp_basal.

Matches bolus/basal records to dosingDecision records with reason='loop'
within a ±5-second window. A day is 'autobolus' if any bolus matches;
'temp_basal' if only basal records match.
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    output_table="dev.fda_510k_rwd.loop_recommendation_day",
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

WITH
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

day_versions AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    MAX(loop_version) AS loop_version
  FROM loop_decisions
  GROUP BY _userId, CAST(LEFT(time_string, 10) AS DATE)
),

autobolus_days AS (
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

temp_basal_days AS (
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

classified AS (
  SELECT _userId, day, 'autobolus' AS day_type
  FROM autobolus_days

  UNION ALL

  SELECT tb._userId, tb.day, 'temp_basal' AS day_type
  FROM temp_basal_days tb
  LEFT JOIN autobolus_days ab
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
