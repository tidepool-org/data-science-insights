import argparse


def run(spark, input_table, output_table="dev.fda_510k_rwd.loop_recommendations"):
    spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} AS

SELECT
  _userId,
  TRY_CAST(created_timestamp AS TIMESTAMP) AS settings_time,
  CAST(TRY_CAST(created_timestamp AS TIMESTAMP) AS DATE) AS day,
  CASE
    WHEN recommendedBasal IS NULL AND recommendedBolus IS NULL THEN NULL
    WHEN recommendedBolus IS NOT NULL THEN 1
    ELSE 0
  END AS is_autobolus
FROM IDENTIFIER(:input_table)
WHERE reason = 'loop'
  AND TRY_CAST(created_timestamp AS TIMESTAMP) IS NOT NULL
  AND CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
    + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
    < 34
""", args={"input_table": input_table})


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", default="dev.default.bddp_sample_all_2")
    args, _ = parser.parse_known_args()

    run(spark, args.input_table)
