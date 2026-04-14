"""Classify each user-day as autobolus or temp_basal using HealthKit metadata.

Uses the MetadataKeyAutomaticallyIssued field on insulin delivery records
where the HealthKit source is Loop. A day is 'autobolus' if any record has
automatically_issued = 1; 'temp_basal' otherwise.
"""

import argparse


def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    output_table="dev.fda_510k_rwd.loop_recommendation_healthkit_day",
):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

WITH
loop_automated AS (
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

autobolus_days AS (
  SELECT DISTINCT _userId, day
  FROM loop_automated
  WHERE type = 'bolus'
),

temp_basal_days AS (
  SELECT DISTINCT _userId, day
  FROM loop_automated
  WHERE type = 'basal'
),

day_versions AS (
  SELECT
    _userId,
    day,
    MAX(loop_version) AS loop_version
  FROM loop_automated
  GROUP BY _userId, day
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
