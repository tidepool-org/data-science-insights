spark = spark  # type: ignore[name-defined]  # noqa: F841

spark.sql("""
CREATE OR REPLACE TABLE dev.fda_510k_rwd.valid_transition_carbs AS

WITH carb_entries AS (
  SELECT
    _userId,
    TRY_CAST(created_timestamp AS TIMESTAMP) AS carb_timestamp,
    TRY_CAST(
      get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE
    ) AS carb_grams
  FROM dev.default.bddp_sample_all_2
  WHERE type = 'food'
    AND TRY_CAST(created_timestamp AS TIMESTAMP) IS NOT NULL
    AND nutrition IS NOT NULL
)

SELECT
    c._userId,
    c.carb_grams,
    c.carb_timestamp,
    CASE
        WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
    END AS segment
FROM carb_entries c
INNER JOIN dev.fda_510k_rwd.valid_transition_segments t ON c._userId = t._userId
WHERE CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg2_end
  AND CASE
        WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg1_start AND t.tb_to_ab_seg1_end THEN 'tb_to_ab_seg1'
        WHEN CAST(c.carb_timestamp AS DATE) BETWEEN t.tb_to_ab_seg2_start AND t.tb_to_ab_seg2_end THEN 'tb_to_ab_seg2'
      END IS NOT NULL
""")
