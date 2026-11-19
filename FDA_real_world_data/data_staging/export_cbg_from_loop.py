def run(
    spark,
    input_table="dev.default.bddp_sample_all_2",
    output_table="dev.fda_510k_rwd.loop_cbg",
    loop_recommendations_table="dev.fda_510k_rwd.loop_recommendations",
):
    spark.sql(f"""
--begin-sql
CREATE OR REPLACE TABLE {output_table} AS

WITH
loop_users AS (
  SELECT DISTINCT _userId
  FROM {loop_recommendations_table}
),

cbg_raw AS (
  SELECT
    s._userId,
    TRY_CAST(s.time_string AS TIMESTAMP) AS cbg_timestamp,
    (s.value * 18.018) AS cbg_mg_dl,
    s.created_timestamp
  FROM {input_table} s
  INNER JOIN loop_users u ON s._userId = u._userId
  WHERE s.type = 'cbg'
    AND s.value IS NOT NULL
    AND TRY_CAST(s.time_string AS TIMESTAMP) IS NOT NULL
),

cbg_bucketed AS (
  SELECT
    *,
    cbg_mg_dl BETWEEN 38 AND 500 AS is_plausible,
    DATE_TRUNC('hour', cbg_timestamp)
      + INTERVAL '5' MINUTE * FLOOR(MINUTE(cbg_timestamp) / 5) AS cbg_bucket
  FROM cbg_raw
),

cbg_deduped AS (
  SELECT * FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, cbg_bucket
        -- One reading per five-minute slot: the latest by timestamp, then the most
        -- recently created record, then the lower value (MC, 2026-11-19). The last two
        -- keys settle readings that share a timestamp; ordered by timestamp alone, two
        -- stagings of the same data kept different readings (2026-11-17 against 11-18).
        ORDER BY cbg_timestamp DESC, created_timestamp DESC NULLS LAST, cbg_mg_dl ASC
      ) AS rn
    FROM cbg_bucketed
  )
  WHERE rn = 1
)

SELECT _userId, cbg_timestamp, cbg_mg_dl, is_plausible
FROM cbg_deduped
;
""")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
