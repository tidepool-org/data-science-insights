CREATE OR REPLACE TABLE dev.fda_510k_rwd.stable_autobolus_cbg AS

WITH
stable_users AS (
  SELECT DISTINCT _userId
  FROM dev.fda_510k_rwd.stable_autobolus_segments
),

cbg_raw AS (
  SELECT
    s._userId,
    TRY_CAST(s.time:`$date` AS TIMESTAMP) AS cbg_timestamp,
    (s.value * 18.018) AS cbg_mg_dl
  FROM dev.default.bddp_sample_all s
  INNER JOIN stable_users u ON s._userId = u._userId
  WHERE s.type = 'cbg'
    AND s.value IS NOT NULL
),

cbg_bucketed AS (
  SELECT
    *,
    DATE_TRUNC('hour', cbg_timestamp)
      + INTERVAL '5' MINUTE * FLOOR(MINUTE(cbg_timestamp) / 5) AS cbg_bucket
  FROM cbg_raw
  WHERE cbg_mg_dl BETWEEN 38 AND 500
),

cbg_deduped AS (
  SELECT * FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY _userId, cbg_bucket
        ORDER BY cbg_timestamp DESC
      ) AS rn
    FROM cbg_bucketed
  )
  WHERE rn = 1
)

SELECT _userId, cbg_timestamp, cbg_mg_dl
FROM cbg_deduped;
