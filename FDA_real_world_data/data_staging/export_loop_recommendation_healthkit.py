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
loop_insulin AS (
  SELECT
    _userId,
    CAST(LEFT(time_string, 10) AS DATE) AS day,
    get_json_object(payload, '$["com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued"]') AS automatically_issued,
    get_json_object(origin, '$.payload.sourceRevision.source.name') AS source_name,
    get_json_object(origin, '$.version') AS loop_version
  FROM {input_table}
  WHERE payload LIKE '%MetadataKeyAutomaticallyIssued%'
    AND get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
    AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
),

classified AS (
  SELECT
    _userId,
    day,
    CASE
      WHEN MAX(CAST(automatically_issued AS DOUBLE)) = 1 THEN 'autobolus'
      ELSE 'temp_basal'
    END AS day_type,
    MAX(loop_version) AS loop_version
  FROM loop_insulin
  GROUP BY _userId, day
)

SELECT
  _userId,
  day,
  day_type,
  loop_version
FROM classified
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    args, _ = parser.parse_known_args()

    run(spark, args.input_table)
