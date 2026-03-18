spark = spark  # type: ignore[name-defined]  # noqa: F841
dbutils = dbutils  # type: ignore[name-defined]  # noqa: F841

DEFAULT_INPUT_TABLE = "dev.default.bddp_sample_all_2"

dbutils.widgets.text("input_table", DEFAULT_INPUT_TABLE)
INPUT_TABLE = dbutils.widgets.get("input_table")

if not INPUT_TABLE.strip():
    raise ValueError("input_table parameter is required")

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.loop_recommendations AS

WITH merged AS (
  -- Dev table (TBDDP)
  SELECT
    _userId,
    TRY_CAST(created_timestamp AS TIMESTAMP) AS settings_time,
    origin,
    recommendedBasal,
    recommendedBolus
  FROM IDENTIFIER(:input_table)
  WHERE reason = 'loop'
    AND TRY_CAST(created_timestamp AS TIMESTAMP) IS NOT NULL

  UNION ALL

  -- Prod table (TDP)
  SELECT
    _userId,
    from_unixtime(
      CAST(get_json_object(`time`, '$.$date.$numberLong') AS BIGINT) / 1000
    ) AS settings_time,
    origin,
    recommendedBasal,
    recommendedBolus
  FROM prod.default.device_data
  WHERE reason = 'loop'
    AND get_json_object(`time`, '$.$date.$numberLong') IS NOT NULL
)

SELECT
  _userId,
  settings_time,
  CAST(settings_time AS DATE) AS day,
  CASE
    WHEN recommendedBasal IS NULL AND recommendedBolus IS NULL THEN NULL
    WHEN recommendedBolus IS NOT NULL THEN 1
    ELSE 0
  END AS is_autobolus
FROM merged
WHERE CAST(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 1) AS INT) * 10
    + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(get_json_object(origin, '$.version'), '.', 2), '.', -1) AS INT)
    < 34
""", args={"input_table": INPUT_TABLE})
